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
//! Settling forks the branch **at the top of the range it is settling**, runs every producer on the
//! fork, and merges when it settles (§16.5). A producer's read path is therefore
//! `[(round branch, its head), (trunk, the top of the range)]`: it sees its siblings' output because
//! that output is on the round's own branch, and a client merge landing on the trunk mid-round is
//! above the fork point and simply is not in the path. There is no high-water mark to maintain, and
//! `reflects` is the fork point by construction rather than by bookkeeping.
//!
//! ## A round settles a range, not a layer
//!
//! One round covers `[watermark+1 … head]` (§6.3, §16.5). The alternative — a round per source layer
//! — makes a backlog a treadmill: with `L10`, `L11`, `L12` all committed before anything settles, the
//! `L10` round computes from the world at `L10` and is rejected at merge by its own guard, because
//! `L11` moved its input while it ran. The guards were right; the *schedule* had guaranteed the work
//! was stale before it ran.
//!
//! Two things follow from the range and both are load-bearing:
//!
//! * **The invalidation walk covers every layer in the range, derived layers included.** A cell
//!   written by a previous round's merged output counts as a trigger for the producers that read it,
//!   which is the only way a chained migration or a pipeline pushed over already-derived data is ever
//!   discovered — a derived layer opens no round of its own. A layer is skipped for a producer that
//!   has already incorporated it, and *"already incorporated"* is the layer's position in the source
//!   stream ([`DerivationEngine::position`]) against that producer's watermark. Without that test a
//!   settled branch would re-derive itself for ever: the round's own merged output is in the next
//!   round's range.
//! * **The fork is at the top layer, which may be a derived one, while `reflects` is the top
//!   *source* layer.** A watermark points into the source stream (§6.3), so what the output claims
//!   has to be a source position; but the world at that position *includes* the consequences of the
//!   layers below it, and those sit above it in the log. Forking at the top source layer would hide
//!   exactly the derived output an earlier round merged above it — §16.5's backlog residue. Forking
//!   at the top layer keeps `reflects` true by construction all the same, because every derived layer
//!   in between reflects a source layer at or below it.
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
use crate::poison::{MemoryPoison, PoisonProvider, Poisoning};
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

/// The stretch of log one round settles: everything between a watermark and head. SPEC.md §16.5.
///
/// Two layer ids, and keeping them apart is the whole of what makes a range honest. `fork_at` is
/// where the round can see from and is the top of the range whatever authored it; `reflects` is what
/// its output claims and must be a *source* position, because that is what a watermark points into
/// (§6.3).
struct Span {
    /// Where the round forks — the top of the range, source layer or derived.
    fork_at: LayerId,
    /// The highest source layer in the range. What every cell this round writes claims, and where
    /// every producer's watermark lands when it settles.
    reflects: LayerId,
    /// Every committed layer in the range, oldest first. Derived layers are in here on purpose: a
    /// previous round's merged output is a trigger like any other write.
    layers: Vec<LayerId>,
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
    ///
    /// Behind a provider because it has to **outlive the process that discovered it**: the CLI is
    /// process-per-command, and a poisoning kept in this struct died with the command that recorded
    /// it. See `crate::poison` for why it lives beside the store rather than in the log.
    poison: Arc<dyn PoisonProvider>,
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
            poison: Arc::new(MemoryPoison::new()),
            parallelism: AtomicUsize::new(default_parallelism()),
        }
    }

    /// Keep poisonings somewhere they survive this process. SPEC.md §14.
    ///
    /// A builder rather than a constructor argument because the in-process default is right for
    /// every caller that *is* the process — a test, a server — and wrong only for a client that
    /// exits between commands. The one caller that needs this is the one that also has somewhere to
    /// put it.
    #[must_use]
    pub fn with_poison(mut self, poison: Arc<dyn PoisonProvider>) -> Self {
        self.poison = poison;
        self
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

    /// Whether this producer is poisoned on this branch **as the branch's definitions now stand**.
    /// SPEC.md §14.
    ///
    /// A record naming a ClientVersion the producer has since moved off has expired, not been
    /// forgiven: pushing fixed code *is* §14's recovery, and the log is what says it happened. That
    /// is the whole reason a durable record is safe to keep outside the log — see `crate::poison`.
    ///
    /// A producer with no definition resolvable in this process cannot be shown to have moved, so
    /// its record stands. An unproven recovery is not a recovery.
    pub fn is_broken(&self, branch: BranchId, producer: ProducerId) -> Result<Option<Poisoning>> {
        let version = self.version_of(producer);
        Ok(self
            .poison
            .poisoned(branch)?
            .into_iter()
            .find(|poisoning| poisoning.producer == producer)
            .filter(|poisoning| version.is_none_or(|version| poisoning.applies_to(version))))
    }

    /// Every producer poisoned on this branch whose record has not expired — one per producer, the
    /// failure that last happened. Whoever has to *report* a poisoning is rarely the process that
    /// discovered it, which is the whole reason this is public.
    pub fn broken(&self, branch: BranchId) -> Result<Vec<Poisoning>> {
        Ok(self
            .poison
            .poisoned(branch)?
            .into_iter()
            .filter(|poisoning| {
                self.version_of(poisoning.producer)
                    .is_none_or(|version| poisoning.applies_to(version))
            })
            .collect())
    }

    /// Forget every live poisoning on this branch and put the work it skipped back in front of the
    /// producer. Returns how many were cleared.
    ///
    /// The escape hatch for a fix that is **not** a def push — a worker's environment was repaired,
    /// a service it calls came back — where nothing in the log moved and so nothing could expire the
    /// record. Deliberately explicit: retrying by default is what turns one bad deploy into the same
    /// failure repeated by every command, with whatever partial effects it had repeated too.
    pub fn retry_broken(&self, branch: BranchId) -> Result<usize> {
        let broken = self.broken(branch)?;
        for poisoning in &broken {
            self.revive(branch, poisoning.producer)?;
        }
        Ok(broken.len())
    }

    /// Expire the poisonings whose producer has been pushed again since. SPEC.md §14's recovery.
    ///
    /// Called where work is *discovered* rather than where a record is read, because reviving a
    /// producer rewinds its frontier and that must not happen underneath a round already choosing
    /// what to settle.
    fn recover(&self, branch: BranchId) -> Result<()> {
        for poisoning in self.poison.poisoned(branch)? {
            let moved = self
                .version_of(poisoning.producer)
                .is_some_and(|version| !poisoning.applies_to(version));
            if moved {
                self.revive(branch, poisoning.producer)?;
            }
        }
        Ok(())
    }

    /// Clear a poisoning and hand the producer back the work it missed.
    ///
    /// **The frontier goes back to nothing**, because a round advances every producer's watermark
    /// whether or not it ran (§16.5) — so a producer skipped while broken is standing at head
    /// claiming to have incorporated everything it never saw. Rewinding is what makes §14's
    /// *invalidates and recomputes its output* true rather than a promise the next write happens to
    /// keep, and it is the same rewind [`recompute`](Self::recompute) uses for the same reason.
    fn revive(&self, branch: BranchId, producer: ProducerId) -> Result<()> {
        self.poison.clear(branch, producer)?;
        self.frontier.rewind(branch, producer);
        Ok(())
    }

    /// The ClientVersion this process resolves for a producer, if it can resolve one at all.
    fn version_of(&self, producer: ProducerId) -> Option<LayerId> {
        self.producers
            .lock()
            .unwrap()
            .get(&producer)
            .map(|def| def.version)
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
        // **Deliberately not a range**, where [`catch_up`](Self::catch_up) is. A range forks at the
        // top layer so that a round can see what earlier rounds derived; a rebuild is the one
        // operation that must *not* see it, or a producer re-running would read its own previous
        // output as an input and the replay would confirm itself. Forking at the highest source
        // layer leaves the derived output for that layer above the fork point and out of the world —
        // which is what `scenarios/100-watermark-truth` depends on, and why it forks and rebuilds
        // rather than forking and reading.
        //
        // It is also the only fork point available: a fresh fork has no head of its own, so the top
        // of a range is a question about the ancestor's log rather than about this branch.
        let Some(at) = self.layers.highest_source_layer(&path) else {
            return Ok(0);
        };
        // Spent poisonings are dropped here too, though the rewind below would have covered the
        // frontier half on its own: a record naming a ClientVersion nothing appoints any more is
        // dead weight, and leaving it for a reader to filter out forever is how it starts being
        // believed.
        self.recover(branch)?;
        for producer in self.producer_ids() {
            self.frontier.rewind(branch, producer);
        }
        Ok(self.settle(branch, at).await?.executed)
    }

    /// Run every producer forward to head, in **one** round. Returns the number of invocations
    /// executed.
    ///
    /// Work is discovered, never queued: the layers between a producer's watermark and head are the
    /// complete statement of what remains to do (SPEC.md §16.4). All of them, as one range — see the
    /// module header for why a round per source layer made a backlog run work it had already
    /// guaranteed would be rejected.
    pub async fn catch_up(self: &Arc<Self>, branch: BranchId) -> Result<usize> {
        // **Before the gap is measured, not after.** A producer whose code has been pushed again is
        // no longer the producer that failed (SPEC.md §14), and reviving it rewinds its watermark —
        // which is the work this call is about to go and find.
        self.recover(branch)?;
        let from = self
            .producer_ids()
            .into_iter()
            .map(|p| self.frontier.watermark(branch, p))
            .min()
            .unwrap_or(LayerId(0));
        let Some(span) = self.span(branch, from)? else {
            return Ok(0);
        };
        Ok(self.settle_span(branch, span).await?.executed)
    }

    /// The range a round would settle: everything on this branch above `from`, up to head.
    ///
    /// `None` where there is nothing to do, and *nothing to do* is a narrower claim than *nothing in
    /// the range*. A settled branch's head is a derived layer this branch's own last round merged,
    /// and it is above every watermark by construction — so "the range is non-empty" would be true
    /// for ever and a catch-up would chase its own output round again on every call. What makes it
    /// work is a layer's **position** in the source stream: the range holds work only if something in
    /// it stands above `from`.
    fn span(&self, branch: BranchId, from: LayerId) -> Result<Option<Span>> {
        let Some(head) = self.layers.head(branch) else {
            return Ok(None);
        };
        let mut layers = Vec::new();
        let mut fork_at = None;
        let mut top_source = None;
        let mut has_work = false;

        for raw in (from.0 + 1)..=head.0 {
            let id = LayerId(raw);
            let Some(layer) = self.layers.layer(id) else {
                continue;
            };
            if layer.branch != branch {
                continue;
            }
            match layer.author {
                LayerAuthor::Source => {
                    // **Stop at a source layer that is still open, rather than settling it or
                    // stepping over it.** A layer becomes the changeset at commit (§6.2, §9.6) and
                    // means nothing before then; settling it would derive from writes that may yet
                    // be abandoned, and skipping it would advance every watermark past a layer
                    // nothing had incorporated. Neither is available once a client can be writing
                    // while derivation runs, which is the whole point of this being concurrent. An
                    // *aborted* layer is different — it will never commit, so there is nothing to
                    // wait for.
                    //
                    // Stopping matters more than it did, because the range's top is now the fork
                    // point: a layer that committed after the round forked *below* that fork point
                    // would appear in the round's read path halfway through. Breaking here is what
                    // keeps the fork point a snapshot.
                    match layer.state {
                        borg_core::LayerState::Committed => {}
                        borg_core::LayerState::Aborted => continue,
                        borg_core::LayerState::Open | borg_core::LayerState::Sealed => break,
                    }
                    layers.push(id);
                    fork_at = Some(id);
                    top_source = Some(id);
                    has_work = true;
                }
                LayerAuthor::Derived { reflects, .. } => {
                    // A derived layer is skipped above whatever state it is in, so an abandoned one
                    // — a round whose task was cancelled by a panicking peer — costs a layer id and
                    // nothing else. Waiting on those would let one panic stall every future round.
                    if layer.state != borg_core::LayerState::Committed {
                        continue;
                    }
                    layers.push(id);
                    fork_at = Some(id);
                    // It is in the fork's world either way; it is *work* only if some producer here
                    // has yet to incorporate the source layer it speaks for.
                    has_work |= reflects.0 > from.0;
                }
            }
        }

        let Some(fork_at) = fork_at.filter(|_| has_work) else {
            return Ok(None);
        };
        let reflects = match top_source {
            Some(id) => id,
            // Every layer in the range is derived — a fork that has settled what it inherited and
            // written no source layer of its own. The source position it stands at belongs to an
            // ancestor, and the read path is what knows where.
            None => {
                let path = self.branches.read_path(branch, Some(fork_at))?;
                match self.layers.highest_source_layer(&path) {
                    Some(id) => id,
                    // No source layer anywhere below: nothing to derive from, and nothing a
                    // watermark could honestly name.
                    None => return Ok(None),
                }
            }
        };
        Ok(Some(Span {
            fork_at,
            reflects,
            layers,
        }))
    }

    /// Where a layer stands in the **source** stream: its own id if it is one, and the source layer
    /// it is a consequence of if it is derived (SPEC.md §6.3).
    ///
    /// This is the comparison a watermark is for. A producer standing at `W` has incorporated every
    /// layer whose position is at or below `W` — including derived layers with ids far above `W`,
    /// because a derived layer reflecting `W` *is* part of the world at `W`.
    fn position(&self, layer: LayerId) -> LayerId {
        match self.layers.author_of(layer) {
            Some(LayerAuthor::Derived { reflects, .. }) => reflects,
            _ => layer,
        }
    }

    /// Run one source layer's consequences to a fixpoint, as a transaction. SPEC.md §16.5.
    ///
    /// The degenerate range: a round whose fork point and `reflects` are both this one layer, and
    /// whose opening wave is this one layer. **Public because a round is a verb** (§16.2), and
    /// because the interleavings worth testing — two rounds settling different source layers, a
    /// client landing mid-round — are statements about *which* layer a round settles, which
    /// `catch_up` chooses for itself. A test asking for a deliberately stale round needs to name the
    /// layer it is stale about, and a range would helpfully take the staleness away.
    pub async fn settle(
        self: &Arc<Self>,
        branch: BranchId,
        source_layer: LayerId,
    ) -> Result<RoundOutcome> {
        self.settle_span(
            branch,
            Span {
                fork_at: source_layer,
                reflects: source_layer,
                layers: vec![source_layer],
            },
        )
        .await
    }

    /// Run a range's consequences to a fixpoint, as a transaction. SPEC.md §16.5.
    ///
    /// Fork at the top of the range, run every producer on the fork, merge what settled. The layers
    /// in the range trigger producers; their output commits further layers, which trigger more. All
    /// of it carries the same `reflects` — the top *source* layer of the range — and the fork point
    /// is what makes that claim true rather than merely asserted. The watermark advances only once
    /// this settles, which is precisely what makes the watermark mean *"replay the world at this
    /// layer and you get exactly this."*
    ///
    /// Alternating *schedule the whole wave, then run the whole wave* is what makes this safe to
    /// parallelise; see the module header. It also removes an order-dependence the sequential
    /// version had and nobody had noticed: work used to be discovered one producer at a time, in
    /// `HashMap` iteration order, so which producer got to run first was already unspecified.
    async fn settle_span(
        self: &Arc<Self>,
        branch: BranchId,
        mut span: Span,
    ) -> Result<RoundOutcome> {
        // **The fork is the whole of what replaced the ceiling.** Taken at the top of the range, so
        // the round's reads resolve through `[(round, head), (trunk, top of range)]`: siblings'
        // output is visible because it is on this branch, and anything a client lands on the trunk
        // meanwhile is above the bound and is not in the path at all. `reflects` cannot drift from
        // what the round saw, because the fork point *is* what the round can see — and everything
        // between `reflects` and the fork point is derived output reflecting `reflects` or lower,
        // which is part of the world `reflects` names rather than something above it.
        //
        // Forked from whichever branch owns the layer rather than from `branch`, because the two
        // differ in the ordinary case of a fork that has written nothing of its own: the highest
        // layer it can see belongs to an ancestor, while the merge must still land on the branch
        // being settled. That is the same split a client transaction carries (§12.2).
        let owner = self
            .layers
            .layer(span.fork_at)
            .ok_or_else(|| BorgError::Storage(format!("unknown layer {}", span.fork_at)))?
            .branch;
        let round_branch = self.branches.fork(owner, span.fork_at, None).await?;
        let round = Arc::new(Mutex::new(borg_core::Round::new(
            round_branch,
            branch,
            span.fork_at,
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
        // Taken rather than cloned: a long backlog's range is as long as the backlog.
        let mut wave = std::mem::take(&mut span.layers);
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
                    span.reflects,
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
                        fresh_as_of: span.reflects,
                        reflects: span.reflects,
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
                    Err(err) => self.record_poisoning(branch, producer, span.reflects, &err)?,
                }
            }
        }

        // Taken out rather than unwrapped from the `Arc`: every invocation has joined, so this is the
        // only holder — but "so it must be" is the kind of reasoning that becomes a panic in
        // production when a later refactor keeps a clone alive.
        let round = std::mem::replace(
            &mut *round.lock().unwrap(),
            borg_core::Round::new(round_branch, branch, span.fork_at),
        );
        let mut outcome = self.branches.merge_round(&round).await?;
        outcome.executed = executed;

        // **The watermark advances whether or not every invocation landed** — and whether or not a
        // producer was skipped as broken. Holding a poisoned producer's watermark back would stall
        // the settled frontier (§10.5) and every `frontier reaches` on the branch behind one bad
        // pipeline, which is precisely the branch-wide poisoning §14 exists to avoid. What a client
        // needs to know is carried by the read envelope instead, as `broken` rather than `stale`,
        // and the work skipped here is handed back by [`revive`](Self::revive) on recovery.
        //
        // The same holds for an invocation the merge dropped, for a different reason. Its
        // cells are still dirty in the dependency index — its edges were recorded when it ran, on
        // this branch and not on the round's — and the layer that failed its guard is itself a
        // source layer a later round settles, which rediscovers it through the cell that moved.
        // Holding the watermark back instead would stall every *other* producer on one contended
        // cell, which is the failure partial application exists to avoid.
        for producer in self.producer_ids() {
            self.frontier.advance(branch, producer, span.reflects);
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
        reflects: LayerId,
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
            // Where this layer stands in the source stream, which is what a watermark is comparable
            // with. For a layer the round produced itself this is the round's own `reflects`, so an
            // intra-round wave behaves exactly as it always did.
            let position = self.position(*layer);
            let cells = self.cells_of(*layer).await?;

            for def in self.producer_defs() {
                // A poisoned producer is not scheduled at all — not even for the invocations that
                // have nothing to do with whatever failed. §14 scopes `IllegalState` to the
                // producer, and the producer is the unit that is judged broken (§9.2).
                if self.is_broken(branch, def.id)?.is_some() {
                    continue;
                }
                // A producer that has already incorporated this layer has no business being
                // dirtied by it — otherwise a newly-registered producer replaying history would drag
                // every up-to-date producer back through it, and a settled branch would re-derive
                // itself off its own merged output for ever.
                //
                // Asked **per layer** rather than once per round, because a range holds layers
                // standing at different source positions: a derived layer a previous round merged
                // and a source layer that arrived afterwards are both in front of a new producer,
                // and only the second of them is in front of a producer that was already caught up.
                if self.frontier.watermark(branch, def.id).0 >= position.0 {
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
                    let seeds = self.backfill(branch, round_branch, &def, defs).await?;
                    if !seeds.is_empty() {
                        // **Merged through a set, never by `Vec::contains`** (invariant 5). Both
                        // sides can be the producer's whole buffer at once, and they now routinely
                        // are: the seeding round used to fork below the data and find nothing, so
                        // this loop was only ever exercised at one or two candidates. Settling a
                        // range puts the fork where the buffer is full, and a linear membership test
                        // turned a 128k backfill into 88 seconds against 2.6.
                        let mut merged: HashSet<Invocation> = candidates.into_iter().collect();
                        merged.extend(seeds);
                        candidates = merged.into_iter().collect();
                    }
                }
                for invocation in candidates {
                    if !scheduled.insert(invocation.clone()) {
                        continue;
                    }
                    let runs = reruns.entry(invocation.clone()).or_insert(0);
                    *runs += 1;
                    if *runs > CYCLE_RERUN_LIMIT {
                        self.record_poisoning(
                            branch,
                            def.id,
                            reflects,
                            &BorgError::ProducerCycle {
                                producer: def.id,
                                runs: *runs,
                            },
                        )?;
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
        // A set, not a `Vec` with a membership scan: a buffer holds one row per entity and there may
        // be millions of them (invariant 5, §16.3). This is a hot enough path to have been measured
        // — see the merge in `schedule`.
        let mut candidates: HashSet<CellRef> = HashSet::new();
        while let Some(row) = stream.next().await {
            candidates.insert(row?.cell);
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

    /// Judge a producer broken, against the ClientVersion it was running at.
    ///
    /// Recording the version is what makes the record self-expiring, and it is read back from the
    /// same registration the run was dispatched through — so the version blamed is the one that
    /// actually failed, not whatever the branch has moved to by the time anybody looks.
    fn record_poisoning(
        &self,
        branch: BranchId,
        producer: ProducerId,
        since: LayerId,
        err: &BorgError,
    ) -> Result<()> {
        self.poison.poison(
            branch,
            Poisoning {
                producer,
                version: self.version_of(producer).unwrap_or(LayerId(0)),
                error: err.to_string(),
                since,
            },
        )
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
        // A poisoned producer does not run for a `current` read either. §10.5's contract is that the
        // read pays for the computation, not that the computation is guaranteed — and the envelope
        // it gets back says `broken`, which is the answer.
        if self.is_broken(branch, producer)?.is_some() {
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
