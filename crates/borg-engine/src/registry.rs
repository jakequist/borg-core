//! The registry: everything wired together, opened from a store.
//!
//! ## What is durable and what is rebuilt
//!
//! Only two things are stored in their own right — **layers and branches**, the shape of the log.
//! The dependency index, the cell-touch index and the per-producer watermarks are all **caches**,
//! and every one of them is recovered by replaying committed layers.
//!
//! That is not a shortcut, it is a property worth having: a derived cell already records the exact
//! read-set it was computed from (§4.3), and a derived layer already records the source layer it
//! reflects (§6.3). The indexes are those facts turned around for lookup, so losing them costs
//! nothing but time.
//!
//! ## Opening is not the same as replaying
//!
//! Those caches are [`Projection`](crate::projection::Projection)s, and open is *"bring each one to
//! head"* rather than *"replay the log"*. The two are the same call only when the projections start
//! empty:
//!
//! * A **process-per-command CLI** builds them fresh every time, so open folds the whole log and is
//!   `O(log)`. That is the honest cost of exiting between commands, and it is unchanged.
//! * A **process that stays up** — `borg serve` — opens once and keeps the registry. Its projections
//!   are maintained by the commits flowing through it, so they are already at head and there is no
//!   replay to pay for. Before this, serve opened per request and multiplied the `O(log)` open by
//!   the number of reads (`examples/personal-crm/FRICTION.md` #9).
//!
//! What is *not* a projection is the layer and branch table this open reads first. §17.1 puts it
//! plainly: those are the structure of the log rather than a fold over it, so they are read from
//! storage and not rebuilt from anything.

use crate::branch::BranchManager;
use crate::defs::DefRegistry;
use crate::derive::DerivationEngine;
use crate::index::{DependencyIndexProvider, MemoryDependencyIndex};
use crate::log::LayerManager;
use crate::poison::{MemoryPoison, PoisonProvider};
use crate::projection::{DependencyProjection, FrontierProjection, Projection, Projections};
use crate::resolve::{FrontierTracker, InlineDerivation, Resolver};
use crate::seams::InProcessSequencer;
use crate::touch::CellTouchIndex;
use crate::values::Values;
use crate::write::WriteSession;
use borg_core::{BranchId, ClientVersion, LayerAuthor, LayerId, Result, Writer};
use borg_exec::ExecutionProvider;
use borg_storage::StorageProvider;
use futures_util::StreamExt;
use std::sync::Arc;

pub struct Registry {
    pub storage: Arc<dyn StorageProvider>,
    pub layers: Arc<LayerManager>,
    pub branches: Arc<BranchManager>,
    pub defs: Arc<DefRegistry>,
    pub engine: Arc<DerivationEngine>,
    pub resolver: Resolver,
    pub frontier: Arc<FrontierTracker>,
    /// Which producers are broken here, and why. Public because the client that has to *report* a
    /// poisoning is not the one that discovered it (SPEC.md §14).
    pub poison: Arc<dyn PoisonProvider>,
    /// Text ↔ value, for the surfaces that speak text. Every client-facing write goes through
    /// `intern` and every client-facing read through `render`, so a string is a string on the way in
    /// and on the way out (§3.4).
    pub values: Arc<Values>,
    /// The dependency graph. Public for the same reason `poison` is: `explain` is answered from it,
    /// and the rebuild-and-diff tests have to be able to ask it questions.
    pub index: Arc<dyn DependencyIndexProvider>,
    /// `cell -> layers that wrote it`, which guard validation is checked against (§12.4).
    pub touches: Arc<CellTouchIndex>,
    /// The caches above, as the folds over the log that they are. See [`crate::projection`] — this
    /// is the seam that makes "held open across requests" a lifecycle choice rather than a risk.
    pub projections: Arc<Projections>,
}

impl Registry {
    /// Open a store and restore everything derivable from it, keeping poisonings in this process.
    ///
    /// Right for anything that *is* the process — a test, a server. A client that exits between
    /// commands wants [`open_with_poison`](Self::open_with_poison), or §14's judgement dies with the
    /// command that made it.
    pub async fn open(
        storage: Arc<dyn StorageProvider>,
        executor: Arc<dyn ExecutionProvider>,
    ) -> Result<Self> {
        Self::open_with_poison(storage, executor, Arc::new(MemoryPoison::new())).await
    }

    /// Open a store against a poison table that outlives this process. SPEC.md §14.
    ///
    /// Not a `StorageProvider` concern and deliberately not one: nothing above the provider line
    /// teaches storage what derivation is, and a poisoned producer is derivation through and
    /// through. See `crate::poison` for where it belongs instead and why.
    pub async fn open_with_poison(
        storage: Arc<dyn StorageProvider>,
        executor: Arc<dyn ExecutionProvider>,
        poison: Arc<dyn PoisonProvider>,
    ) -> Result<Self> {
        let touches = Arc::new(CellTouchIndex::new());
        let index: Arc<dyn DependencyIndexProvider> = Arc::new(MemoryDependencyIndex::new());
        let frontier = Arc::new(FrontierTracker::new());

        // Three folds over the log, named as such. Built before the log manager, because the log is
        // what feeds them and it has to be handed the set.
        let dependencies = Arc::new(DependencyProjection::new(Arc::clone(&index)));
        let watermarks = Arc::new(FrontierProjection::new(Arc::clone(&frontier)));
        let projections = Arc::new(Projections::new([
            Arc::clone(&touches) as Arc<dyn Projection>,
            Arc::clone(&dependencies) as Arc<dyn Projection>,
            Arc::clone(&watermarks) as Arc<dyn Projection>,
        ]));

        let known = storage.read_layers().await?;
        let highest = known.iter().map(|layer| layer.id.0).max().unwrap_or(0);
        let layers = Arc::new(
            LayerManager::new(
                Arc::clone(&storage),
                // Resume the sequence rather than restarting it, or a second CLI invocation would
                // try to reuse layer ids that already exist.
                Arc::new(InProcessSequencer::resuming_after(LayerId(highest))),
                Arc::clone(&touches),
            )
            .with_projections(Arc::clone(&projections)),
        );

        for branch in storage.read_branches().await? {
            layers.register_branch(branch);
        }
        for layer in &known {
            layers.restore(layer.clone());
        }

        let branches = Arc::new(BranchManager::resuming(
            Arc::clone(&layers),
            highest_branch(&known) + 1,
        ));
        let defs = Arc::new(DefRegistry::new(Arc::clone(&layers), Arc::clone(&storage)));
        let values = Arc::new(Values::new(Arc::clone(&storage)));

        let engine = Arc::new(
            DerivationEngine::new(
                Arc::clone(&storage),
                Arc::clone(&layers),
                Arc::clone(&index),
                executor,
                Arc::clone(&frontier),
                Arc::clone(&defs),
                Arc::clone(&branches),
            )
            .with_poison(Arc::clone(&poison)),
        );

        let registry = Self {
            resolver: Resolver::new(
                Arc::clone(&storage),
                Arc::clone(&index),
                Arc::clone(&defs),
                Arc::clone(&branches),
                // The read path's one link to derivation, and deliberately the narrowest one there
                // is: `InlineDerivation` says "bring this cell up to date" and nothing else
                // (SPEC.md §10.5).
                Arc::clone(&engine) as Arc<dyn InlineDerivation>,
            )
            // The same table the engine writes to. Two views of one fact — the scheduler stops
            // running a poisoned producer, the reader is told that is why (SPEC.md §14).
            .with_poison(Arc::clone(&poison)),
            storage,
            layers,
            branches,
            defs,
            engine,
            frontier,
            poison,
            values,
            index,
            touches,
            projections,
        };
        // **Not "replay the log" — "bring the projections to head."** For this registry they are
        // empty, so the two are the same call and it is `O(log)`; for one that has been maintained
        // live there is nothing to fold. See `crate::projection`.
        registry
            .projections
            .bring_to_head(registry.storage.as_ref(), &known)
            .await?;
        Ok(registry)
    }

    /// The branch a bare command operates on: the first root, by convention named `main`.
    pub fn default_branch(&self) -> Option<BranchId> {
        self.branches.roots().into_iter().min_by_key(|id| id.0)
    }

    /// The producers whose definitions are in force on a branch.
    ///
    /// Definitions travel the log, so this is branch-scoped like everything else: a pipeline pushed
    /// on a fork is not a producer main is waiting for.
    pub async fn producers_of(&self, branch: BranchId) -> Result<Vec<borg_core::ProducerId>> {
        let path = self.branches.read_path(branch, None)?;
        let view = self.defs.view(&path).await?;
        Ok(view.producers().map(|def| def.id).collect())
    }

    /// Tell the derivation engine which producers this branch defines.
    ///
    /// Definitions travel the log and implementations do not (§9.2), so this is half of joining the
    /// two — and it is what puts a **ClientVersion** in front of the engine, which is what a
    /// poisoning is checked against (§14). An engine that has not been told cannot tell a producer
    /// that has been fixed from one that has not.
    pub async fn register_producers(&self, branch: BranchId) -> Result<()> {
        let path = self.branches.read_path(branch, None)?;
        for def in self.defs.view(&path).await?.producers() {
            self.engine.register(def.clone());
        }
        Ok(())
    }

    /// Every object of one struct on this branch, by PID. SPEC.md §9.6, §17.5.
    ///
    /// The struct's existence buffer *is* the set of its instances (§4.2), so this is the same
    /// `scan_buffer` the scheduler uses to discover entities — read through the branch's ancestry,
    /// so a fork enumerates what it can see rather than only what it wrote.
    ///
    /// **A tombstoned existence cell is not one of the objects** (§8.1). The scan answers records
    /// rather than a set, which is what the scheduler wants; deciding that a deletion means absence
    /// is this caller's question and is answered here.
    ///
    /// Sorted by PID, which for one allocator is `(branch, allocator, counter)` and therefore
    /// allocation order. Sorted at all so that two identical reads answer identically; nothing
    /// promises the order means anything more than that.
    ///
    /// Materialized, like `scan_buffer` itself (`CLAUDE.md`, things left undone): a struct with
    /// millions of instances is exactly the case that wants a cursor, and a cursor is the query
    /// layer's shape rather than a parameter that could be bolted on here.
    pub async fn object_ids(
        &self,
        branch: BranchId,
        struct_name: &borg_core::ObjectTypeName,
    ) -> Result<Vec<borg_core::Pid>> {
        let path = self.branches.read_path(branch, None)?;
        let buffer = borg_core::BufferId::Object(struct_name.clone());
        let mut stream = self.storage.scan_buffer(&path, &buffer).await?;
        let mut ids = Vec::new();
        while let Some(row) = stream.next().await {
            let event = row?;
            if event.value.is_tombstone() {
                continue;
            }
            ids.push(*event.cell.pid());
        }
        // One record per existence cell — they are unversioned (§5.2), so there is one version of
        // each to answer — but the answer is a set of objects, and saying so here costs one pass.
        ids.sort_unstable();
        ids.dedup();
        Ok(ids)
    }

    /// The layer to read at for a coherent snapshot of this branch. SPEC.md §10.5.
    ///
    /// Everything visible here was computed from everything else visible here — the alternative to
    /// the ragged head, where the latest of every field is served with per-field freshness. A branch
    /// with no producers is settled at its head, because nothing derives and so nothing can be
    /// behind.
    pub async fn settled(&self, branch: BranchId) -> Result<LayerId> {
        let head = self.layers.head(branch).unwrap_or(LayerId(0));
        let producers = self.producers_of(branch).await?;
        Ok(match self.frontier.settled(branch, &producers) {
            Some(watermark) => self.layers.settled_ceiling(branch, watermark),
            None => head,
        })
    }

    /// Open the sanctioned write path: a layer plus the definitions it will be checked against
    /// (SPEC.md §5.1, §8). This is the only way to write a cell from outside the engine.
    pub async fn begin_write(
        &self,
        branch: BranchId,
        version: ClientVersion,
        writer: Writer,
    ) -> Result<WriteSession> {
        WriteSession::open(
            &self.layers,
            &self.defs,
            branch,
            version,
            writer,
            LayerAuthor::Source,
        )
        .await
    }
}

fn highest_branch(layers: &[borg_core::Layer]) -> u64 {
    layers.iter().map(|layer| layer.branch.0).max().unwrap_or(0)
}
