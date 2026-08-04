//! The derivation cycle. SPEC.md §9, §16.4, §16.5.
//!
//! Invalidation is driven by **layer commit**, not by buffer instrumentation: a committed layer *is*
//! the changeset, so one pass over it answers both trigger questions — cell writes dirty existing
//! invocations, and object creations produce new ones.
//!
//! The scheduler is **stateless**. Work is derived from the gap between a producer's watermark and
//! the branch head, rather than queued, which bounds memory and makes crash recovery free.
//!
//! ## A round is a transaction
//!
//! Settling a source layer forks the branch **at that layer**, runs every producer on the fork, and
//! merges when it settles (§16.5). A producer's read path is therefore `[(round branch, its head),
//! (trunk, the source layer)]`: it sees its siblings' output because that output is on the round's
//! own branch, and a client merge landing on the trunk mid-round is above the fork point and simply
//! is not in the path. There is no high-water mark to maintain, and `reflects` is the fork point by
//! construction rather than by bookkeeping.
//!
//! ## A round runs in waves
//!
//! The invocations discovered from one layer are independent of each other by construction: the
//! single-writer rule (§8) means no two of them can target the same cell, so their layers may commit
//! in any order (§16.3.1). They are therefore run **concurrently**, bounded by
//! [`DerivationEngine::with_parallelism`].
//!
//! What is *not* concurrent is one wave with the next, and the barrier survived the deletion of the
//! ceiling because it never was about the ceiling. Scheduling is sequential and cheap — it runs no
//! user code — and each wave joins before the layers it produced are turned into the next wave's
//! work. A producer records its read-set *before* its layer commits, so a run that read an input its
//! upstream had not written yet is already subscribed to that cell by the time any later wave scans
//! the layer supplying it. Without the barrier a run could commit after the layer it needed had
//! already been scanned, and the correction would never be triggered.

use crate::branch::RoundOutcome;
use crate::defs::DefView;
use crate::index::{DependencyIndexProvider, Invocation};
use crate::log::LayerManager;
use crate::resolve::{FrontierTracker, InlineDerivation};
use crate::seams::WorkGap;
use crate::values::Values;
use crate::write::WriteSession;
use async_trait::async_trait;
use borg_core::{
    BorgError, BranchId, CellAt, CellRef, ClientVersion, DefVersion, Derivation, LayerAuthor,
    LayerId, Pid, ProducerDef, ProducerId, ReadPath, Result, Value, ValueInput, Writer,
};
use borg_exec::{ExecutionProvider, ProducerCtx, ProducerRef};
use borg_storage::StorageProvider;
use futures_util::StreamExt;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

/// How many times one invocation may re-run at a fixed branch head before it is judged to be
/// cycling. SPEC.md §16.5.
const CYCLE_RERUN_LIMIT: u32 = 8;

/// How many invocations run at once when nothing says otherwise.
///
/// One per core. Producer work is arbitrary user code — a native producer is CPU-bound in this
/// process and a subprocess worker is a pipe round-trip away — so the core count is the one bound
/// that is right for the first case and not badly wrong for the second. Anything larger only helps
/// when producers block, and that is the executor's business to oversubscribe (see
/// `borg-exec-process`), not the scheduler's.
fn default_parallelism() -> usize {
    std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get)
}

/// Where one producer run writes, and what its output claims.
///
/// The two branches coincide for an inline computation (§10.5) and differ inside a round, where the
/// output goes to the round's own branch and everything *about* the invocation — its dependency
/// edges, its producer's poison state — belongs to the branch the round is settling. Naming them
/// separately is what lets one code path serve both without either lying about the other.
#[derive(Clone, Copy)]
struct RunAt {
    /// The branch this run's derived layer is committed on: a round's branch, or the trunk for an
    /// inline run.
    on: BranchId,
    /// The branch the dependency index and the poison table are keyed on. Always the branch that
    /// owns the data — a round branch is where events land on the way through, never what the
    /// dependency graph is a graph *of*, or the round after this one would find nothing.
    home: BranchId,
    /// What the output *claims*: the source layer through which its inputs have been incorporated,
    /// written into every cell's `fresh_as_of` (SPEC.md §10.1).
    fresh_as_of: LayerId,
    /// What the derived *layer* is labelled with, and therefore what a restart folds back into the
    /// producer's watermark. Equal to `fresh_as_of` inside a round; deliberately behind it for an
    /// inline run, which speaks for one cell and must not advance a whole producer's frontier.
    reflects: LayerId,
}

/// The mediated view of the world handed to producer code.
///
/// Every access flows through here, which is what makes dependency capture automatic and exact —
/// there is nothing for a producer author to declare or mis-declare (SPEC.md §9.4).
struct RecordingCtx<'a> {
    storage: &'a dyn StorageProvider,
    /// Interning and content resolution, shared with every other surface that accepts or emits
    /// value text — see `crate::values` for why they are one implementation and not two.
    values: &'a Values,
    /// This round's ancestry, resolved once rather than per read.
    path: ReadPath,
    /// The layer this run is bringing the world up to. This is the *label* on the output, not where
    /// its inputs are read from: a producer consuming another producer's output must see that
    /// output, which lives in a derived layer with a higher id than the source layer they both
    /// reflect.
    fresh_as_of: LayerId,
    producer: ProducerId,
    /// The def-version this producer takes its **input** at, where that is not simply its own view.
    /// `None` for a pipeline, whose input version *is* its ClientVersion; `Some` for a migration,
    /// which reads the other end of the step it bridges (SPEC.md §9.3).
    input_version: Option<DefVersion>,
    /// Output goes through the same validated write path as everything else, so a producer writing
    /// a field it does not own is rejected against the *declaration* rather than against whatever
    /// happened to write there first (SPEC.md §8).
    session: WriteSession,
    /// What this run read and wrote, as `CellAt` — the record key (§16.3). Two uses, and the round
    /// takes them by value at the end of the run rather than copying: the dependency index's edges,
    /// and the round's guard set (SPEC.md §16.5).
    read_set: Vec<CellAt>,
    write_set: Vec<CellAt>,
}

impl RecordingCtx<'_> {
    /// Where this producer's own def-view puts this field (SPEC.md §5.3).
    ///
    /// Taken from the write session, so a producer reads a cell at exactly the version it would
    /// write one — the property that makes a read-set entry and the client write it depends on name
    /// the same record.
    fn version_of(&self, cell: &CellRef) -> DefVersion {
        self.session.defs().version_of(cell)
    }

    /// Note an accepted write.
    fn wrote(&mut self, cell: &CellRef) {
        let written = CellAt::new(cell.clone(), self.version_of(cell));
        if !self.write_set.contains(&written) {
            self.write_set.push(written);
        }
    }
}

#[async_trait]
impl ProducerCtx for RecordingCtx<'_> {
    async fn get(&mut self, cell: &CellRef) -> Result<Option<Value>> {
        let version = self.version_of(cell);
        self.get_at(cell, version).await
    }

    async fn get_at(&mut self, cell: &CellRef, version: DefVersion) -> Result<Option<Value>> {
        // Recorded before the lookup, so that a read finding *nothing* is still a dependency.
        // Absence is a legitimate input, and a later write to that cell must invalidate this run.
        //
        // Recorded *at the version read*, so that a migration writing `C@v9` does not appear to have
        // disturbed its own read of `C@v1`.
        let read = CellAt::new(cell.clone(), version);
        if !self.read_set.contains(&read) {
            self.read_set.push(read);
        }
        let found = self.storage.get_cell(&self.path, cell, version).await?;
        Ok(found.map(|found| found.event.value))
    }

    async fn get_input(&mut self, cell: &CellRef) -> Result<Option<Value>> {
        let version = self.input_version.unwrap_or_else(|| self.version_of(cell));
        self.get_at(cell, version).await
    }

    async fn set(&mut self, cell: &CellRef, value: Value) -> Result<()> {
        let derivation = Derivation {
            producer: self.producer,
            fresh_as_of: self.fresh_as_of,
            read_set: self.read_set.clone(),
        };
        self.session.set_derived(cell, value, derivation).await?;
        // Recorded only after the write is accepted: a rejected write is not output, and claiming
        // it in the index would leave the producer owning a cell it never produced.
        self.wrote(cell);
        Ok(())
    }

    async fn set_text(&mut self, cell: &CellRef, text: &str) -> Result<()> {
        let derivation = Derivation {
            producer: self.producer,
            fresh_as_of: self.fresh_as_of,
            read_set: self.read_set.clone(),
        };
        // The session parses against the field's declared type, so a worker sending `true` into a
        // `String` field writes four characters and one sending `acme` into an `Int` field is told
        // so — the same rule the CLI gets, from the same place (§3.4).
        self.session
            .set_text_derived(cell, text, derivation)
            .await?;
        self.wrote(cell);
        Ok(())
    }

    async fn intern(&mut self, input: ValueInput) -> Result<Value> {
        self.values.intern(input).await
    }

    async fn render(&mut self, value: &Value) -> Result<String> {
        self.values.render(value).await
    }
}

/// Drives the cycle. SPEC.md §16.
pub struct DerivationEngine {
    storage: Arc<dyn StorageProvider>,
    layers: Arc<LayerManager>,
    index: Arc<dyn DependencyIndexProvider>,
    executor: Arc<dyn ExecutionProvider>,
    frontier: Arc<FrontierTracker>,
    defs: Arc<crate::defs::DefRegistry>,
    branches: Arc<crate::branch::BranchManager>,
    values: Values,
    producers: Mutex<HashMap<ProducerId, ProducerDef>>,
    /// Producers poisoned by a runtime failure. Scoped to the producer, never the branch — which is
    /// why main never breaks because someone merged a bad pipeline (SPEC.md §14).
    broken: Mutex<HashMap<(BranchId, ProducerId), String>>,
    /// How many invocations of one wave run at once. Deployment configuration, not a semantic —
    /// see [`DerivationEngine::set_parallelism`].
    parallelism: AtomicUsize,
}

impl DerivationEngine {
    pub fn new(
        storage: Arc<dyn StorageProvider>,
        layers: Arc<LayerManager>,
        index: Arc<dyn DependencyIndexProvider>,
        executor: Arc<dyn ExecutionProvider>,
        frontier: Arc<FrontierTracker>,
        defs: Arc<crate::defs::DefRegistry>,
        branches: Arc<crate::branch::BranchManager>,
    ) -> Self {
        Self {
            // Built here rather than passed in: `Values` is a pure function of the store, and the
            // engine already holds it. One type, not one instance — there is no state to share.
            values: Values::new(Arc::clone(&storage)),
            storage,
            layers,
            index,
            executor,
            frontier,
            defs,
            branches,
            producers: Mutex::new(HashMap::new()),
            broken: Mutex::new(HashMap::new()),
            parallelism: AtomicUsize::new(default_parallelism()),
        }
    }

    /// How many invocations may run at once while a round settles.
    ///
    /// Scheduling policy cannot affect correctness, only latency (SPEC.md §9.6), so this is a knob
    /// and not a semantic: `1` is the sequential engine, and any larger value must settle on the
    /// same result. Exposed because the right number is a property of the deployment — how many
    /// cores there are, how much a producer blocks — and of a test that wants to force contention.
    ///
    /// Clamped to at least one: zero would be a scheduler that never ran anything.
    pub fn set_parallelism(&self, invocations: usize) {
        self.parallelism
            .store(invocations.max(1), std::sync::atomic::Ordering::Relaxed);
    }

    /// [`set_parallelism`](Self::set_parallelism) as a builder, for callers holding the engine
    /// before it goes behind an `Arc`.
    #[must_use]
    pub fn with_parallelism(self, invocations: usize) -> Self {
        self.set_parallelism(invocations);
        self
    }

    #[must_use]
    pub fn parallelism(&self) -> usize {
        self.parallelism.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn register(&self, def: ProducerDef) {
        self.producers.lock().unwrap().insert(def.id, def);
    }

    pub fn is_broken(&self, branch: BranchId, producer: ProducerId) -> Option<String> {
        self.broken
            .lock()
            .unwrap()
            .get(&(branch, producer))
            .cloned()
    }

    fn producer_ids(&self) -> Vec<ProducerId> {
        self.producers.lock().unwrap().keys().copied().collect()
    }

    /// The gap a producer must close: everything from its watermark to head. SPEC.md §16.4.
    ///
    /// Note there is no queue anywhere — pending work is *implied* by this gap plus the dependency
    /// index, so it is recomputed rather than remembered.
    pub fn pending(&self, branch: BranchId, producer: ProducerId) -> Option<WorkGap> {
        let head = self.layers.head(branch)?;
        let gap = WorkGap {
            producer,
            from: self.frontier.watermark(branch, producer),
            to: head,
        };
        (!gap.is_empty()).then_some(gap)
    }

    /// Recompute this branch's derived data from source, ignoring everything already derived.
    ///
    /// §6.3 calls derived layers **droppable** — "a cache that happens to live in the log", whose
    /// fallback is always recompute, because source is separate. Nothing could invoke that fallback,
    /// and one thing needs it badly: a watermark claims *"replay the world at layer W and you get
    /// exactly this value"* (§10.1), and the only way to check a claim of that shape is to do the
    /// replay and compare. Fork at W, recompute there, read both.
    ///
    /// **A fork is where this is cheap and where it is honest.** A fork inherits its parent's
    /// derived layers by ancestry (§7.4), so recomputing on one costs nothing until it writes, and
    /// what it does write shadows what it inherited — the fork's own segment is the first the read
    /// path walks (§7.2). Meanwhile its ancestors stay bounded at the fork point however far the
    /// round's ceiling climbs, so the producers re-run against exactly the world W names and not
    /// against whatever has landed on the parent since.
    ///
    /// Rewinding the frontier is the whole difference from [`catch_up`](Self::catch_up). A producer
    /// standing at W correctly has nothing left to do; saying it has incorporated *nothing* is what
    /// puts its entire source buffer back in front of it, through the same [`backfill`](Self::backfill)
    /// path a producer newer than its data takes.
    ///
    /// This is not free — it is `O(everything derivable)`, which is what "rebuild the cache" costs
    /// anywhere.
    pub async fn recompute(self: &Arc<Self>, branch: BranchId) -> Result<usize> {
        let path = self.branches.read_path(branch, None)?;
        // A round settles one *source* layer and labels its output `reflects: that layer` (§6.3), so
        // it is opened at the highest source layer under the branch's ceiling rather than at the
        // ceiling itself — the same world, named in the stream watermarks actually point into. The
        // two differ whenever the last thing to commit was derived, which on a settled branch is
        // almost always.
        let Some(at) = self.layers.highest_source_layer(&path) else {
            return Ok(0);
        };
        for producer in self.producer_ids() {
            self.frontier.rewind(branch, producer);
        }
        Ok(self.settle(branch, at).await?.executed)
    }

    /// Run every producer forward to head. Returns the number of invocations executed.
    ///
    /// Work is discovered, never queued: the layers between a producer's watermark and head are the
    /// complete statement of what remains to do (SPEC.md §16.4).
    pub async fn catch_up(self: &Arc<Self>, branch: BranchId) -> Result<usize> {
        let Some(head) = self.layers.head(branch) else {
            return Ok(0);
        };
        let from = self
            .producer_ids()
            .into_iter()
            .map(|p| self.frontier.watermark(branch, p))
            .min()
            .unwrap_or(LayerId(0));

        let mut executed = 0;
        for raw in (from.0 + 1)..=head.0 {
            let source_layer = LayerId(raw);
            let Some(layer) = self.layers.layer(source_layer) else {
                continue;
            };
            if layer.branch != branch {
                continue;
            }
            // Only *source* layers open a round. Derived layers are consequences, and are picked up
            // inside the closure below rather than driving one of their own.
            if !matches!(layer.author, LayerAuthor::Source) {
                continue;
            }
            // **Stop at a source layer that is still open, rather than settling it or stepping over
            // it.** A layer becomes the changeset at commit (§6.2, §9.6) and means nothing before
            // then; settling it would derive from writes that may yet be abandoned, and skipping it
            // would advance every watermark past a layer nothing had incorporated. Neither is
            // available once a client can be writing while derivation runs, which is the whole point
            // of this being concurrent. An *aborted* layer is different — it will never commit, so
            // there is nothing to wait for.
            //
            // The wait is scoped to source layers deliberately. A derived layer is skipped above
            // whatever state it is in, so an abandoned one — a round whose task was cancelled by a
            // panicking peer — costs a layer id and nothing else. Waiting on those would let one
            // panic stall every future round in the process.
            match layer.state {
                borg_core::LayerState::Committed => {}
                borg_core::LayerState::Aborted => continue,
                borg_core::LayerState::Open | borg_core::LayerState::Sealed => break,
            }
            executed += self.settle(branch, source_layer).await?.executed;
        }
        Ok(executed)
    }

    /// Run one source layer's consequences to a fixpoint, as a transaction. SPEC.md §16.5.
    ///
    /// Fork at the layer, run every producer on the fork, merge what settled. A committed layer
    /// triggers producers; their output commits further layers, which trigger more. All of it
    /// carries the same `reflects`, because it is all the consequence of one source layer, and the
    /// fork point is what makes that claim true rather than merely asserted. The watermark advances
    /// only once this settles — which is precisely what makes the watermark mean *"replay the world
    /// at this layer and you get exactly this."*
    ///
    /// **Public because a round is a verb** (§16.2), and because the interleavings worth testing —
    /// two rounds settling different source layers, a client landing mid-round — are statements
    /// about *which* layer a round settles, which `catch_up` chooses for itself.
    ///
    /// Alternating *schedule the whole wave, then run the whole wave* is what makes this safe to
    /// parallelise; see the module header. It also removes an order-dependence the sequential
    /// version had and nobody had noticed: work used to be discovered one producer at a time, in
    /// `HashMap` iteration order, so which producer got to run first was already unspecified.
    pub async fn settle(
        self: &Arc<Self>,
        branch: BranchId,
        source_layer: LayerId,
    ) -> Result<RoundOutcome> {
        // **The fork is the whole of what replaced the ceiling.** Taken at the source layer being
        // settled, so the round's reads resolve through `[(round, head), (trunk, source layer)]`:
        // siblings' output is visible because it is on this branch, and anything a client lands on
        // the trunk meanwhile is above the bound and is not in the path at all. `reflects` cannot
        // drift from what the round saw, because the fork point *is* what the round can see.
        //
        // Forked from whichever branch owns the layer rather than from `branch`, because the two
        // differ in the ordinary case of a fork that has written nothing of its own: the highest
        // layer it can see belongs to an ancestor, while the merge must still land on the branch
        // being settled. That is the same split a client transaction carries (§12.2).
        let owner = self
            .layers
            .layer(source_layer)
            .ok_or_else(|| BorgError::Storage(format!("unknown layer {source_layer}")))?
            .branch;
        let round_branch = self.branches.fork(owner, source_layer, None).await?;
        let round = Arc::new(Mutex::new(borg_core::Round::new(
            round_branch,
            branch,
            source_layer,
        )));

        // Folded once per round, from the **trunk** and not from the round's fork point. A round
        // isolates data, not definitions: a layer holds value events xor def events (§6.2), so
        // nothing a round commits can move a definition, and folding at the fork point would hide
        // exactly the def-mutation that appoints a migration from the round that has to run it.
        //
        // Behind an `Arc` because every invocation in the wave needs it and it is read-only.
        let defs = Arc::new(
            self.defs
                .view(&self.branches.read_path(branch, None)?)
                .await?,
        );
        let permits = Arc::new(Semaphore::new(self.parallelism()));

        let mut executed = 0;
        let mut wave = vec![source_layer];
        // Scoped to this round: a cycle is an invocation that keeps re-running while the branch head
        // is otherwise fixed (SPEC.md §16.5).
        let mut reruns: HashMap<Invocation, u32> = HashMap::new();
        // Producers already given their whole source buffer as work in this round — see `backfill`.
        let mut seeded: HashSet<ProducerId> = HashSet::new();

        while !wave.is_empty() {
            let work = self
                .schedule(
                    branch,
                    round_branch,
                    &wave,
                    source_layer,
                    &defs,
                    &mut reruns,
                    &mut seeded,
                )
                .await?;
            wave.clear();

            let mut running = JoinSet::new();
            for (def, invocation) in work {
                let engine = Arc::clone(self);
                let defs = Arc::clone(&defs);
                let round = Arc::clone(&round);
                let permits = Arc::clone(&permits);
                running.spawn(async move {
                    // The permit is taken inside the task rather than before spawning, so the whole
                    // wave is described up front and the bound applies to what is *running* rather
                    // than to what is known.
                    let _permit = permits.acquire_owned().await;
                    let at = RunAt {
                        on: round_branch,
                        home: branch,
                        fresh_as_of: source_layer,
                        reflects: source_layer,
                    };
                    let outcome = engine.run(&def, &defs, invocation.input, at).await;
                    // The round takes the accesses by value: they are already the two vectors the
                    // dependency index was fed, and copying them per invocation is the difference
                    // this shows up as on the fan-out benchmark.
                    let derived = match outcome {
                        Ok((layer, reads, writes)) => {
                            round.lock().unwrap().ran(layer, reads, writes);
                            Ok(layer)
                        }
                        Err(err) => Err(err),
                    };
                    (def.id, derived)
                });
            }

            while let Some(joined) = running.join_next().await {
                // A panicking producer is a failure of that producer, not of the round: the same
                // shape as an error, poisoned the same way. Which one panicked is unknowable from
                // the `JoinError`, so the round reports it rather than mislabelling a producer.
                let (producer, outcome) = joined.map_err(|err| {
                    BorgError::Execution(format!("a producer run did not finish: {err}"))
                })?;
                match outcome {
                    Ok(derived) => {
                        executed += 1;
                        wave.push(derived);
                    }
                    Err(err) => self.poison(branch, producer, err),
                }
            }
        }

        // Taken out rather than unwrapped from the `Arc`: every invocation has joined, so this is the
        // only holder — but "so it must be" is the kind of reasoning that becomes a panic in
        // production when a later refactor keeps a clone alive.
        let round = std::mem::replace(
            &mut *round.lock().unwrap(),
            borg_core::Round::new(round_branch, branch, source_layer),
        );
        let mut outcome = self.branches.merge_round(&round).await?;
        outcome.executed = executed;

        // **The watermark advances whether or not every invocation landed.** A dropped invocation's
        // cells are still dirty in the dependency index — its edges were recorded when it ran, on
        // this branch and not on the round's — and the layer that failed its guard is itself a
        // source layer a later round settles, which rediscovers it through the cell that moved.
        // Holding the watermark back instead would stall every *other* producer on one contended
        // cell, which is the failure partial application exists to avoid.
        for producer in self.producer_ids() {
            self.frontier.advance(branch, producer, source_layer);
        }
        Ok(outcome)
    }

    /// Turn one wave of committed layers into the invocations they dirty.
    ///
    /// Runs no user code and commits nothing, which is why it stays sequential: it is a walk over
    /// changesets and index lookups, and making it concurrent would buy contention on the one index
    /// mutex in exchange for nothing.
    #[allow(clippy::too_many_arguments)]
    async fn schedule(
        &self,
        branch: BranchId,
        round_branch: BranchId,
        wave: &[LayerId],
        source_layer: LayerId,
        defs: &DefView,
        reruns: &mut HashMap<Invocation, u32>,
        seeded: &mut HashSet<ProducerId>,
    ) -> Result<Vec<(ProducerDef, Invocation)>> {
        let mut work = Vec::new();
        // One run per invocation per wave. Two layers in one wave can dirty the same invocation, and
        // running it twice concurrently would put two layers in the same round writing the same
        // cell and two `record` calls racing to describe one run. Deferring the duplicate to the
        // next wave is free: this wave's own output re-triggers it if it is still dirty.
        let mut scheduled: HashSet<Invocation> = HashSet::new();

        for layer in wave {
            let cells = self.cells_of(*layer).await?;

            for def in self.producer_defs() {
                if self.is_broken(branch, def.id).is_some() {
                    continue;
                }
                // A producer already caught up past this layer has no business in this round —
                // otherwise a newly-registered producer replaying history would drag every
                // up-to-date producer back through it.
                if self.frontier.watermark(branch, def.id).0 >= source_layer.0 {
                    continue;
                }
                // A migration whose step this branch does not record has nothing to bridge here —
                // running it would write at whatever version its own definition happened to mention,
                // which is precisely the coupling `migration_role` exists to remove.
                if matches!(def.kind, borg_core::ProducerKind::Migration { .. })
                    && defs.migration_role(&def).is_none()
                {
                    continue;
                }
                // Migrations materialize only for versions that have live clients (SPEC.md §5.5).
                if !self.is_materialized_version(defs, &def) {
                    continue;
                }
                let mut candidates = self.invalidated_by(branch, &cells, &def, defs)?;
                if seeded.insert(def.id) {
                    for invocation in self.backfill(branch, round_branch, &def, defs).await? {
                        if !candidates.contains(&invocation) {
                            candidates.push(invocation);
                        }
                    }
                }
                for invocation in candidates {
                    if !scheduled.insert(invocation.clone()) {
                        continue;
                    }
                    let runs = reruns.entry(invocation.clone()).or_insert(0);
                    *runs += 1;
                    if *runs > CYCLE_RERUN_LIMIT {
                        self.poison(
                            branch,
                            def.id,
                            BorgError::ProducerCycle {
                                producer: def.id,
                                runs: *runs,
                            },
                        );
                        break;
                    }
                    work.push((def.clone(), invocation));
                }
            }
        }
        Ok(work)
    }

    /// A committed layer's changeset: each cell at the version it was written at, and which
    /// producer wrote it (`None` for source data).
    async fn cells_of(&self, layer: LayerId) -> Result<Vec<(CellAt, Option<ProducerId>)>> {
        let mut stream = self.storage.read_layer(layer).await?;
        let mut cells = Vec::new();
        while let Some(row) = stream.next().await {
            let event = row?;
            let by = event.derivation.as_ref().map(|d| d.producer);
            cells.push((CellAt::new(event.cell, event.version), by));
        }
        Ok(cells)
    }

    /// Whether this producer's output is worth materializing. SPEC.md §5.5.
    fn is_materialized_version(&self, defs: &DefView, def: &ProducerDef) -> bool {
        let Some(role) = defs.migration_role(def) else {
            return true;
        };
        // The set holds ClientVersions and `role.output` is one field's def-version (§5.3). They
        // are compared as the def-layers they both are, which is exact when a client's view was
        // built from the very push that moved this field and approximate otherwise. Nothing
        // registers a client in v1, so the set is empty and the filter is switched off (§5.5);
        // making this precise is part of the deferred reduction policies, which need a folded view
        // per live version to answer it properly.
        let live = self.defs.live_versions();
        live.is_empty() || live.contains(&ClientVersion(role.output.0))
    }

    /// The whole of a producer's source buffer, as work, for a producer that has never run here.
    ///
    /// A committed layer is the changeset (§9.6), which answers "what moved" perfectly and "what was
    /// already there" not at all. That gap opens the moment a producer is *newer than the data*:
    /// a migration pushed on a fork owes the parent's inherited values, and none of those values
    /// were written in a layer belonging to this branch. §9.6 reserves buffer enumeration for exactly
    /// this — discovering entities the layer stream cannot mention.
    ///
    /// Read through the **round's** ancestry, so a fork enumerates what it can see rather than what
    /// it wrote — and so that a round enumerates the world at its fork point rather than whatever
    /// has landed on the trunk since.
    ///
    /// A migration's source is not a buffer but **one version of one field** (§9.3), and a buffer
    /// scan cannot express that, so the candidates are filtered afterwards. Both halves of the filter
    /// matter. Requiring a value at the input version keeps a migration from being invoked over
    /// entities it has nothing to say about; requiring that value not to have come from the other
    /// half of its own step is what makes the round *order-independent* — `up` and `down` seed in
    /// whichever order the engine happens to walk its producers, and `down` backfilling over the
    /// value `up` had just derived would overwrite the source value `up` derived it from.
    async fn backfill(
        &self,
        branch: BranchId,
        round_branch: BranchId,
        def: &ProducerDef,
        defs: &DefView,
    ) -> Result<Vec<Invocation>> {
        if self.frontier.watermark(branch, def.id) != LayerId(0) {
            return Ok(Vec::new());
        }
        let role = defs.migration_role(def);
        let path = self.branches.read_path(round_branch, None)?;
        let mut stream = self.storage.scan_buffer(&path, &def.source).await?;
        let mut candidates = Vec::new();
        while let Some(row) = stream.next().await {
            let cell = row?.cell;
            if !candidates.contains(&cell) {
                candidates.push(cell);
            }
        }

        let mut found = Vec::new();
        for cell in candidates {
            if let Some(role) = &role {
                let found = self.storage.get_cell(&path, &cell, role.input).await?;
                let usable = found.is_some_and(|found| {
                    found
                        .event
                        .derivation
                        .is_none_or(|by| !role.step.contains(&by.producer))
                });
                if !usable {
                    continue;
                }
            }
            found.push(Invocation {
                producer: def.id,
                input: *cell.pid(),
            });
        }
        Ok(found)
    }

    fn producer_defs(&self) -> Vec<ProducerDef> {
        self.producers.lock().unwrap().values().cloned().collect()
    }

    /// One pass over a committed layer's cells answers both trigger questions (SPEC.md §9.6).
    fn invalidated_by(
        &self,
        branch: BranchId,
        cells: &[(CellAt, Option<ProducerId>)],
        def: &ProducerDef,
        defs: &DefView,
    ) -> Result<Vec<Invocation>> {
        let role = defs.migration_role(def);
        // **A migration is not triggered by the other half of its own step.** `up` and `down` are two
        // projections of one value: each writes into the buffer the other reads from, at exactly the
        // version the other reads it at, so left unfiltered they chase each other until the cycle
        // detector fires (§16.6) on a configuration that is not a cycle but the normal case. This
        // covers a producer's own output too, which is the same statement with a one-element step.
        //
        // It filters *both* trigger paths, including the read-set one. §9.3's rule that the read-set
        // trigger is never filtered by author exists so that a producer disturbing a cell it reads is
        // caught; a migration re-expressing its own input in the other direction is not that.
        let peer = |by: &Option<ProducerId>| match (by, &role) {
            (Some(by), Some(role)) => role.step.contains(by),
            _ => false,
        };

        // (a) cell writes -> existing invocations that read those cells go dirty.
        let written: Vec<CellAt> = cells
            .iter()
            .filter(|(_, by)| !peer(by))
            .map(|(cell, _)| cell.clone())
            .collect();
        // A set, because a large source layer can name a large number of new entities and a
        // membership scan per candidate would be quadratic in the layer's size.
        let mut invocations: std::collections::HashSet<Invocation> = self
            .index
            .dependents(branch, &written)?
            .into_iter()
            .filter(|i| i.producer == def.id)
            .collect();

        // (b) writes into this producer's source buffer -> new invocations.
        //
        // For a migration the buffer is not enough: it consumes one *version* of a field and
        // produces another, so a write at any other version — including the one it writes — is
        // somebody else's business (§9.3).
        for (cell, by) in cells {
            if peer(by) || *by == Some(def.id) {
                continue;
            }
            if cell.cell.buffer != def.source {
                continue;
            }
            if role.as_ref().is_some_and(|role| cell.version != role.input) {
                continue;
            }
            invocations.insert(Invocation {
                producer: def.id,
                input: *cell.cell.pid(),
            });
        }
        Ok(invocations.into_iter().collect())
    }

    /// Execute one invocation into its own layer.
    ///
    /// One layer per invocation, because that is the unit a round's merge decides on: partial
    /// application (§16.5) applies the invocations whose guards held, and a guard is a fact about
    /// one invocation. The layers are regrouped on the way across, so the trunk gains one layer per
    /// producer rather than one per invocation.
    async fn run(
        &self,
        def: &ProducerDef,
        defs: &DefView,
        input: Pid,
        at: RunAt,
    ) -> Result<(LayerId, Vec<CellAt>, Vec<CellAt>)> {
        // A migration's ClientVersion is the version it produces: it reads the world at that view
        // and writes that version. Its own source cell is the one exception, reached through
        // `ProducerCtx::get_input` (SPEC.md §9.3). Which two versions those are is a fact about the
        // branch's version chain, not about the producer's definition — see `migration_role`.
        let (version, input_version) = match defs.migration_role(def) {
            // A migration's ClientVersion is the version it produces (§5.4) — and that version is a
            // def-version of one field, which is a def-layer like any other and so names a view.
            // The conversion is explicit because it is the one place the two meanings meet.
            Some(role) => (ClientVersion(role.output.0), Some(role.input)),
            None => (ClientVersion(def.version), None),
        };
        // No layer bound anywhere: the bound is now the branch. Reads resolve through the round
        // branch's own head and its ancestors at the fork point, which is exactly §16.5's filter
        // rather than the prefix that used to approximate it. Definitions come from `home`, because
        // a round isolates data and not schema — see `WriteSession`.
        let session = WriteSession::open_on(
            &self.layers,
            &self.defs,
            at.on,
            at.home,
            version,
            Writer::Producer(def.id),
            LayerAuthor::Derived {
                producer: def.id,
                reflects: at.reflects,
            },
        )
        .await?;

        let producer_ref = ProducerRef {
            id: def.id,
            version,
        };
        let mut ctx = RecordingCtx {
            storage: self.storage.as_ref(),
            values: &self.values,
            path: self.branches.read_path(at.on, None)?,
            fresh_as_of: at.fresh_as_of,
            producer: def.id,
            input_version,
            session,
            read_set: Vec::new(),
            write_set: Vec::new(),
        };

        let outcome = self.executor.run(&producer_ref, input, &mut ctx).await;
        let (read_set, write_set, session) = (ctx.read_set, ctx.write_set, ctx.session);

        match outcome {
            Ok(()) => {
                // Recorded *before* the layer commits, and that ordering is what a concurrent round
                // rests on. A peer scanning this layer in the next wave must find this run already
                // subscribed to everything it read, or a write it missed would go unnoticed.
                //
                // Recorded against `home` and never against the round's own branch. The dependency
                // graph is a fact about the data, which lives on the trunk; keyed on the round
                // branch it would be discarded with the round, and the invocation a merge dropped
                // would never be rediscovered.
                let invocation = Invocation {
                    producer: def.id,
                    input,
                };
                self.index
                    .record(at.home, &invocation, &read_set, &write_set)?;
                Ok((session.commit().await?, read_set, write_set))
            }
            Err(err) => {
                // The layer never becomes visible, so a failed run leaves no trace.
                session.abort().await?;
                Err(err)
            }
        }
    }

    fn poison(&self, branch: BranchId, producer: ProducerId, err: BorgError) {
        self.broken
            .lock()
            .unwrap()
            .insert((branch, producer), err.to_string());
    }

    /// Bring one cell up to date, recursing into whatever it was computed from.
    ///
    /// `computing` is the cells already being brought up to date further up this stack, and it is
    /// what makes the recursion safe. §16.5's re-run counter cannot help here: it is scoped to a
    /// settling round, and there is no round — an inline computation is one client's request, not a
    /// consequence of a source layer. A chain that loops back on itself instead hits a cell it is
    /// already computing and stops there, leaving that cell as it stands. The read that follows
    /// reports it honestly, which is the same outcome the round-based detector arrives at by a
    /// different route.
    fn refresh<'a>(
        &'a self,
        branch: BranchId,
        cell: CellRef,
        version: DefVersion,
        computing: &'a mut HashSet<CellAt>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if !computing.insert(CellAt::new(cell.clone(), version)) {
                return Ok(());
            }

            // Re-resolved on every step rather than hoisted: each producer this runs commits a
            // layer, so the world a later hop reads is not the world the first one saw.
            let path = self.branches.read_path(branch, None)?;
            let defs = self.defs.view(&path).await?;

            let Some(found) = self.storage.get_cell(&path, &cell, version).await? else {
                // Nothing is materialized at this version. What is owed is a migration, and the
                // hops that owe it are exactly the ones the reader's reachability check walks
                // (SPEC.md §10.4) — the resolver computes that path already and, until now, never
                // invoked it.
                let available = self.storage.cell_versions(&path, &cell).await?;
                let Some((from, hops)) = available.iter().find_map(|from| {
                    defs.path(&cell.buffer, *from, version)
                        .map(|hops| (*from, hops))
                }) else {
                    // No path here from any materialized version: this reader's ClientVersion is
                    // unreachable, and no amount of computing changes that (SPEC.md §9.3).
                    return Ok(());
                };
                // The far end of the chain may itself be behind — a migration reads a value some
                // pipeline owes.
                self.refresh(branch, cell.clone(), from, computing).await?;
                for hop in hops {
                    self.run_now(branch, hop.producer, *cell.pid(), &defs)
                        .await?;
                }
                return Ok(());
            };

            // Source data is ground truth; there is nothing to compute and nothing behind it.
            let Some(derivation) = found.event.derivation else {
                return Ok(());
            };
            // Inputs first, so that the run below reads a settled world and can honestly claim to
            // reflect head. Doing it the other way round would compute this cell from stale inputs
            // and label the result current, which is the one thing §10 does not permit.
            for dependency in derivation.read_set {
                self.refresh(branch, dependency.cell, dependency.version, &mut *computing)
                    .await?;
            }
            self.run_now(branch, derivation.producer, *cell.pid(), &defs)
                .await
        })
    }

    /// One producer run, outside any round.
    ///
    /// **It does not advance the producer's watermark**, and the derived layer it commits is
    /// labelled with the watermark the producer already had. A watermark is a claim about *all* of a
    /// producer's output (§10.1); one entity computed on demand says nothing about the other
    /// hundred thousand, and letting it advance the frontier would make a single `current` read
    /// declare a whole branch caught up.
    ///
    /// That is also what makes this self-healing. The work stays outstanding, so the next round
    /// redoes it in the ordinary way and the consequences the round-based invalidator would have
    /// propagated — downstream producers, chained migrations — still get propagated.
    async fn run_now(
        &self,
        branch: BranchId,
        producer: ProducerId,
        input: Pid,
        defs: &DefView,
    ) -> Result<()> {
        let Some(def) = self.producers.lock().unwrap().get(&producer).cloned() else {
            // The definition is on the branch but no implementation is resolvable in this process.
            // Not an error: the read simply reports the lag it already had.
            return Ok(());
        };
        if self.is_broken(branch, producer).is_some() {
            return Ok(());
        }
        // Read at head, claim head. The two coincide here precisely because there is no round: what
        // a round forks to get *is* the head once every input has been settled above.
        //
        // **No fork either.** A round forks because it is `N` computations that must land or not
        // land together with respect to the world they read; this is one cell, computed because one
        // client asked, and it advances no watermark. Forking it would buy a branch and a merge to
        // isolate a single invocation from a snapshot it has no claim on.
        let head = self.layers.head(branch).unwrap_or(LayerId(0));
        let at = RunAt {
            on: branch,
            home: branch,
            fresh_as_of: head,
            reflects: self.frontier.watermark(branch, producer),
        };
        // Errors propagate rather than poisoning. Poisoning is a judgement about a producer, made
        // while settling a round; a failure here is the answer to one client's request, and the
        // client that asked to pay for a fresh value is the one entitled to hear why it could not
        // have one (SPEC.md §14).
        self.run(&def, defs, input, at).await?;
        Ok(())
    }
}

#[async_trait]
impl InlineDerivation for DerivationEngine {
    async fn compute_now(
        &self,
        branch: BranchId,
        cell: &CellRef,
        version: DefVersion,
    ) -> Result<()> {
        let mut computing = HashSet::new();
        self.refresh(branch, cell.clone(), version, &mut computing)
            .await
    }
}
