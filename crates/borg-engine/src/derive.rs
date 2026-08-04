//! The derivation cycle. SPEC.md §9, §16.4, §16.5.
//!
//! Invalidation is driven by **layer commit**, not by buffer instrumentation: a committed layer *is*
//! the changeset, so one pass over it answers both trigger questions — cell writes dirty existing
//! invocations, and object creations produce new ones.
//!
//! The scheduler is **stateless**. Work is derived from the gap between a producer's watermark and
//! the branch head, rather than queued, which bounds memory and makes crash recovery free.

use crate::index::{DependencyIndexProvider, Invocation};
use crate::log::LayerManager;
use crate::resolve::FrontierTracker;
use crate::seams::WorkGap;
use crate::values::Values;
use crate::write::WriteSession;
use async_trait::async_trait;
use borg_core::{
    BorgError, BranchId, CellAt, CellRef, ClientVersion, Derivation, LayerAuthor, LayerId, Pid,
    ProducerDef, ProducerId, ProducerKind, ReadPath, Result, Value, ValueInput, Writer,
};
use borg_exec::{ExecutionProvider, ProducerCtx, ProducerRef};
use borg_storage::StorageProvider;
use futures_util::StreamExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// How many times one invocation may re-run at a fixed branch head before it is judged to be
/// cycling. SPEC.md §16.5.
const CYCLE_RERUN_LIMIT: u32 = 8;

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
    /// The source layer this run is bringing the world up to. This is the *label* on the output, not
    /// where its inputs are read from: a producer consuming another producer's output must see that
    /// output, which lives in a derived layer with a higher id than the source layer they both
    /// reflect.
    reflects: LayerId,
    producer: ProducerId,
    /// The def-view this producer's code was authored against. Reads resolve here and writes are
    /// labelled with it.
    version: ClientVersion,
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

    async fn set(&mut self, cell: &CellRef, value: Value) -> Result<()> {
        let derivation = Derivation {
            producer: self.producer,
            fresh_as_of: self.reflects,
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
            fresh_as_of: self.reflects,
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
        let mut executed = 0;
        let mut frontier_of_change = vec![source_layer];
        // Everything committed so far as a consequence of this source layer. Producers read here, so
        // that one can consume another's output within the round. Safe because derivation is the
        // only writer while a round is settling.
        let mut ceiling = source_layer;
        // Scoped to this round: a cycle is an invocation that keeps re-running while the branch head
        // is otherwise fixed (SPEC.md §16.5).
        let mut reruns: HashMap<Invocation, u32> = HashMap::new();

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
                // Migrations materialize only for versions that have live clients (SPEC.md §5.5).
                if !self.is_materialized_version(&def) {
                    continue;
                }
                for invocation in self.invalidated_by(branch, &cells, &def)? {
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

                    match self
                        .run(branch, &def, invocation.input, source_layer, ceiling)
                        .await
                    {
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
    fn is_materialized_version(&self, def: &ProducerDef) -> bool {
        let ProducerKind::Migration { to, .. } = def.kind else {
            return true;
        };
        let live = self.defs.live_versions();
        live.is_empty() || live.contains(&ClientVersion(to))
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
    ) -> Result<Vec<Invocation>> {
        // (a) cell writes -> existing invocations that read those cells go dirty. Deliberately *not*
        // filtered by author: a producer disturbing a cell it reads is exactly a cycle, and must be
        // caught rather than hidden.
        let written: Vec<CellAt> = cells.iter().map(|(cell, _)| cell.clone()).collect();
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
        // Skipping this producer's own output matters: a migration writes `C@v9` into the very
        // buffer it consumes `C@v1` from, and would otherwise re-trigger itself forever. Its own
        // output appearing in its source buffer is not a new entity.
        for (cell, by) in cells {
            if *by == Some(def.id) {
                continue;
            }
            if cell.cell.buffer == def.source {
                invocations.insert(Invocation {
                    producer: def.id,
                    input: *cell.cell.pid(),
                });
            }
        }
        Ok(invocations.into_iter().collect())
    }

    /// Execute one invocation into its own layer.
    async fn run(
        &self,
        branch: BranchId,
        def: &ProducerDef,
        input: Pid,
        reflects: LayerId,
        read_at: LayerId,
    ) -> Result<LayerId> {
        // A migration's ClientVersion is the def-layer that introduced its target: it reads the
        // world at the target view and writes the target version. Its own source cell is the one
        // exception, reached through `ProducerCtx::get_at` (SPEC.md §9.3).
        let version = match def.kind {
            ProducerKind::Migration { to, .. } => ClientVersion(to),
            ProducerKind::Pipeline => ClientVersion(def.version),
        };
        // This round's ceiling — the source layer plus every derived layer already committed as a
        // consequence of it — bounds both the def-view the writes are checked against and the
        // ancestry the reads resolve through. Distinct from `reflects` on purpose (SPEC.md §16.5).
        let session = WriteSession::open(
            &self.layers,
            &self.defs,
            branch,
            Some(read_at),
            version,
            Writer::Producer(def.id),
            LayerAuthor::Derived {
                producer: def.id,
                reflects,
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
            path: self.branches.read_path(branch, Some(read_at))?,
            reflects,
            producer: def.id,
            version,
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
}
