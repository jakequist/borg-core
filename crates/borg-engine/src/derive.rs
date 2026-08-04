//! The derivation cycle. SPEC.md §9, §16.4, §16.5.
//!
//! Invalidation is driven by **layer commit**, not by buffer instrumentation: a committed layer *is*
//! the changeset, so one pass over it answers both trigger questions — cell writes dirty existing
//! invocations, and object creations produce new ones.
//!
//! The scheduler is **stateless**. Work is derived from the gap between a producer's watermark and
//! the branch head, rather than queued, which bounds memory and makes crash recovery free.

use crate::defs::DefView;
use crate::index::{DependencyIndexProvider, Invocation};
use crate::log::LayerManager;
use crate::resolve::{FrontierTracker, InlineDerivation};
use crate::seams::WorkGap;
use crate::values::Values;
use crate::write::WriteSession;
use async_trait::async_trait;
use borg_core::{
    BorgError, BranchId, CellAt, CellRef, ClientVersion, Derivation, LayerAuthor, LayerId, Pid,
    ProducerDef, ProducerId, ReadPath, Result, Value, ValueInput, Writer,
};
use borg_exec::{ExecutionProvider, ProducerCtx, ProducerRef};
use borg_storage::StorageProvider;
use futures_util::StreamExt;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

/// How many times one invocation may re-run at a fixed branch head before it is judged to be
/// cycling. SPEC.md §16.5.
const CYCLE_RERUN_LIMIT: u32 = 8;

/// Which layers one producer run is pinned to.
///
/// Three layer ids that coincide inside a settling round and must not be assumed to. Naming them
/// separately is what lets an inline computation (§10.5) reuse this code path without lying about a
/// producer's progress.
#[derive(Clone, Copy)]
struct RunAt {
    /// What the output *claims*: the source layer through which its inputs have been incorporated,
    /// written into every cell's `fresh_as_of` (SPEC.md §10.1).
    fresh_as_of: LayerId,
    /// What the derived *layer* is labelled with, and therefore what a restart folds back into the
    /// producer's watermark. Equal to `fresh_as_of` inside a round; deliberately behind it for an
    /// inline run, which speaks for one cell and must not advance a whole producer's frontier.
    reflects: LayerId,
    /// The ancestry bound the run's reads resolve through — the round's ceiling (SPEC.md §16.5).
    read_at: LayerId,
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
    /// The def-view this producer's code was authored against. Reads resolve here and writes are
    /// labelled with it. For a migration it is the version it *produces*: a migration is the lens
    /// for its output version and sees the rest of the world as a client on that version does.
    version: ClientVersion,
    /// The version this producer takes its input at. Equal to `version` for a pipeline; for a
    /// migration it is the other end of the step it bridges (SPEC.md §9.3).
    input_version: ClientVersion,
    /// Output goes through the same validated write path as everything else, so a producer writing
    /// a field it does not own is rejected against the *declaration* rather than against whatever
    /// happened to write there first (SPEC.md §8).
    session: WriteSession,
    read_set: Vec<CellAt>,
    write_set: Vec<CellAt>,
}

#[async_trait]
impl ProducerCtx for RecordingCtx<'_> {
    async fn get(&mut self, cell: &CellRef) -> Result<Option<Value>> {
        let version = self.version;
        self.get_at(cell, version).await
    }

    async fn get_at(&mut self, cell: &CellRef, version: ClientVersion) -> Result<Option<Value>> {
        // Recorded before the lookup, so that a read finding *nothing* is still a dependency.
        // Absence is a legitimate input, and a later write to that cell must invalidate this run.
        //
        // Recorded *at the version read*, so that a migration writing `C@v9` does not appear to have
        // disturbed its own read of `C@v1`.
        let read = CellAt::new(cell.clone(), version);
        if !self.read_set.contains(&read) {
            self.read_set.push(read);
        }
        let record = self.storage.get_cell(&self.path, cell, version).await?;
        Ok(record.map(|r| r.value))
    }

    async fn get_input(&mut self, cell: &CellRef) -> Result<Option<Value>> {
        let version = self.input_version;
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
        let written = CellAt::new(cell.clone(), self.version);
        if !self.write_set.contains(&written) {
            self.write_set.push(written);
        }
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
        let written = CellAt::new(cell.clone(), self.version);
        if !self.write_set.contains(&written) {
            self.write_set.push(written);
        }
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
        }
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

    /// Run every producer forward to head. Returns the number of invocations executed.
    ///
    /// Work is discovered, never queued: the layers between a producer's watermark and head are the
    /// complete statement of what remains to do (SPEC.md §16.4).
    pub async fn catch_up(&self, branch: BranchId) -> Result<usize> {
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
            // Only *source* layers open a round. Derived layers are consequences, and are picked up
            // inside the closure below rather than driving one of their own.
            if layer.branch != branch || !matches!(layer.author, LayerAuthor::Source) {
                continue;
            }
            executed += self.settle(branch, source_layer).await?;
        }
        Ok(executed)
    }

    /// Run one source layer's consequences to a fixpoint.
    ///
    /// A committed layer triggers producers; their output commits further layers, which trigger
    /// more. All of it carries the same `reflects`, because it is all the consequence of one source
    /// layer. The watermark advances only once this settles — which is precisely what makes the
    /// watermark mean *"replay the world at this layer and you get exactly this."*
    async fn settle(&self, branch: BranchId, source_layer: LayerId) -> Result<usize> {
        // Folded once per round. Definitions cannot move inside one: a layer holds value events xor
        // def events (§6.2) and derivation only ever commits value layers.
        let defs = self
            .defs
            .view(&self.branches.read_path(branch, None)?)
            .await?;

        let mut executed = 0;
        let mut frontier_of_change = vec![source_layer];
        // Everything committed so far as a consequence of this source layer. Producers read here, so
        // that one can consume another's output within the round. Safe because derivation is the
        // only writer while a round is settling.
        let mut ceiling = source_layer;
        // Scoped to this round: a cycle is an invocation that keeps re-running while the branch head
        // is otherwise fixed (SPEC.md §16.5).
        let mut reruns: HashMap<Invocation, u32> = HashMap::new();
        // Producers already given their whole source buffer as work in this round — see `backfill`.
        let mut seeded: std::collections::HashSet<ProducerId> = std::collections::HashSet::new();

        while let Some(layer) = frontier_of_change.pop() {
            let cells = self.cells_of(layer).await?;

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
                if !self.is_materialized_version(&defs, &def) {
                    continue;
                }
                let mut work = self.invalidated_by(branch, &cells, &def, &defs)?;
                if seeded.insert(def.id) {
                    for invocation in self.backfill(branch, &def, &defs, ceiling).await? {
                        if !work.contains(&invocation) {
                            work.push(invocation);
                        }
                    }
                }
                for invocation in work {
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

                    let at = RunAt {
                        fresh_as_of: source_layer,
                        reflects: source_layer,
                        read_at: ceiling,
                    };
                    match self.run(branch, &def, &defs, invocation.input, at).await {
                        Ok(derived) => {
                            executed += 1;
                            if derived.0 > ceiling.0 {
                                ceiling = derived;
                            }
                            frontier_of_change.push(derived);
                        }
                        Err(err) => {
                            self.poison(branch, def.id, err);
                            break;
                        }
                    }
                }
            }
        }

        for producer in self.producer_ids() {
            self.frontier.advance(branch, producer, source_layer);
        }
        Ok(executed)
    }

    /// A committed layer's changeset: each cell at the version it was written at, and which
    /// producer wrote it (`None` for source data).
    async fn cells_of(&self, layer: LayerId) -> Result<Vec<(CellAt, Option<ProducerId>)>> {
        let mut stream = self.storage.read_layer(layer).await?;
        let mut cells = Vec::new();
        while let Some(row) = stream.next().await {
            let (cell, record) = row?;
            let by = record.derivation.as_ref().map(|d| d.producer);
            cells.push((CellAt::new(cell, record.version), by));
        }
        Ok(cells)
    }

    /// Whether this producer's output is worth materializing. SPEC.md §5.5.
    fn is_materialized_version(&self, defs: &DefView, def: &ProducerDef) -> bool {
        let Some(role) = defs.migration_role(def) else {
            return true;
        };
        let live = self.defs.live_versions();
        live.is_empty() || live.contains(&role.output)
    }

    /// The whole of a producer's source buffer, as work, for a producer that has never run here.
    ///
    /// A committed layer is the changeset (§9.6), which answers "what moved" perfectly and "what was
    /// already there" not at all. That gap opens the moment a producer is *newer than the data*:
    /// a migration pushed on a fork owes the parent's inherited values, and none of those values
    /// were written in a layer belonging to this branch. §9.6 reserves buffer enumeration for exactly
    /// this — discovering entities the layer stream cannot mention.
    ///
    /// Bounded by the round's ceiling and read through the branch's ancestry, so a fork enumerates
    /// what it can see rather than what it wrote.
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
        def: &ProducerDef,
        defs: &DefView,
        ceiling: LayerId,
    ) -> Result<Vec<Invocation>> {
        if self.frontier.watermark(branch, def.id) != LayerId(0) {
            return Ok(Vec::new());
        }
        let role = defs.migration_role(def);
        let path = self.branches.read_path(branch, Some(ceiling))?;
        let mut stream = self.storage.scan_buffer(&path, &def.source).await?;
        let mut candidates = Vec::new();
        while let Some(row) = stream.next().await {
            let (cell, _) = row?;
            if !candidates.contains(&cell) {
                candidates.push(cell);
            }
        }

        let mut found = Vec::new();
        for cell in candidates {
            if let Some(role) = &role {
                let record = self.storage.get_cell(&path, &cell, role.input).await?;
                let usable = record.is_some_and(|record| {
                    record
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
    async fn run(
        &self,
        branch: BranchId,
        def: &ProducerDef,
        defs: &DefView,
        input: Pid,
        at: RunAt,
    ) -> Result<LayerId> {
        // A migration's ClientVersion is the version it produces: it reads the world at that view
        // and writes that version. Its own source cell is the one exception, reached through
        // `ProducerCtx::get_input` (SPEC.md §9.3). Which two versions those are is a fact about the
        // branch's version chain, not about the producer's definition — see `migration_role`.
        let (version, input_version) = match defs.migration_role(def) {
            Some(role) => (role.output, role.input),
            None => {
                let version = ClientVersion(def.version);
                (version, version)
            }
        };
        // This round's ceiling — the source layer plus every derived layer already committed as a
        // consequence of it — bounds the ancestry the reads resolve through. Distinct from
        // `reflects` on purpose (SPEC.md §16.5), and deliberately *not* applied to the def-view the
        // writes are checked against: see `WriteSession`.
        let session = WriteSession::open(
            &self.layers,
            &self.defs,
            branch,
            Some(at.read_at),
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
            path: self.branches.read_path(branch, Some(at.read_at))?,
            fresh_as_of: at.fresh_as_of,
            producer: def.id,
            version,
            input_version,
            session,
            read_set: Vec::new(),
            write_set: Vec::new(),
        };

        let outcome = self.executor.run(&producer_ref, input, &mut ctx).await;
        let (read_set, write_set, session) = (ctx.read_set, ctx.write_set, ctx.session);

        match outcome {
            Ok(()) => {
                let invocation = Invocation {
                    producer: def.id,
                    input,
                };
                self.index
                    .record(branch, &invocation, &read_set, &write_set)?;
                session.commit().await
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
        version: ClientVersion,
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

            let Some(record) = self.storage.get_cell(&path, &cell, version).await? else {
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
            let Some(derivation) = record.derivation else {
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
        // Read at head, claim head. The two coincide here precisely because there is no round: the
        // ceiling a round exists to compute *is* the head once every input has been settled above.
        let head = self.layers.head(branch).unwrap_or(LayerId(0));
        let at = RunAt {
            fresh_as_of: head,
            reflects: self.frontier.watermark(branch, producer),
            read_at: head,
        };
        // Errors propagate rather than poisoning. Poisoning is a judgement about a producer, made
        // while settling a round; a failure here is the answer to one client's request, and the
        // client that asked to pay for a fresh value is the one entitled to hear why it could not
        // have one (SPEC.md §14).
        self.run(branch, &def, defs, input, at).await?;
        Ok(())
    }
}

#[async_trait]
impl InlineDerivation for DerivationEngine {
    async fn compute_now(
        &self,
        branch: BranchId,
        cell: &CellRef,
        version: ClientVersion,
    ) -> Result<()> {
        let mut computing = HashSet::new();
        self.refresh(branch, cell.clone(), version, &mut computing)
            .await
    }
}
