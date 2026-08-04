//! The registry: everything wired together, opened from a store.
//!
//! ## What is durable and what is rebuilt
//!
//! Only two things are stored in their own right — **layers and branches**, the shape of the log.
//! The dependency index, the cell-touch index and the per-producer watermarks are all **caches**,
//! and every one of them is recovered by replaying committed layers on open.
//!
//! That is not a shortcut, it is a property worth having: a derived cell already records the exact
//! read-set it was computed from (§4.3), and a derived layer already records the source layer it
//! reflects (§6.3). The indexes are those facts turned around for lookup, so losing them costs
//! nothing but time.
//!
//! It does mean open is `O(log)`. Fine for a CLI over a development store, and the fix — materialize
//! the indexes alongside the log — needs no interface change, because they already sit behind
//! providers.

use crate::branch::BranchManager;
use crate::defs::DefRegistry;
use crate::derive::DerivationEngine;
use crate::index::{DependencyIndexProvider, Invocation, MemoryDependencyIndex};
use crate::log::LayerManager;
use crate::resolve::{FrontierTracker, InlineDerivation, Resolver};
use crate::seams::InProcessSequencer;
use crate::touch::CellTouchIndex;
use crate::values::Values;
use crate::write::WriteSession;
use borg_core::{
    BranchId, CellAt, ClientVersion, LayerAuthor, LayerId, LayerState, Result, Writer,
};
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
    /// Text ↔ value, for the surfaces that speak text. Every client-facing write goes through
    /// `intern` and every client-facing read through `render`, so a string is a string on the way in
    /// and on the way out (§3.4).
    pub values: Arc<Values>,
}

impl Registry {
    /// Open a store and restore everything derivable from it.
    pub async fn open(
        storage: Arc<dyn StorageProvider>,
        executor: Arc<dyn ExecutionProvider>,
    ) -> Result<Self> {
        let touches = Arc::new(CellTouchIndex::new());
        let index: Arc<dyn DependencyIndexProvider> = Arc::new(MemoryDependencyIndex::new());
        let frontier = Arc::new(FrontierTracker::new());

        let known = storage.read_layers().await?;
        let highest = known.iter().map(|layer| layer.id.0).max().unwrap_or(0);
        let layers = Arc::new(LayerManager::new(
            Arc::clone(&storage),
            // Resume the sequence rather than restarting it, or a second CLI invocation would try to
            // reuse layer ids that already exist.
            Arc::new(InProcessSequencer::resuming_after(LayerId(highest))),
            Arc::clone(&touches),
        ));

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

        let engine = Arc::new(DerivationEngine::new(
            Arc::clone(&storage),
            Arc::clone(&layers),
            Arc::clone(&index),
            executor,
            Arc::clone(&frontier),
            Arc::clone(&defs),
            Arc::clone(&branches),
        ));

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
            ),
            storage,
            layers,
            branches,
            defs,
            engine,
            frontier,
            values,
        };
        registry
            .rebuild_caches(&known, index.as_ref(), &touches)
            .await?;
        Ok(registry)
    }

    /// Replay committed layers to restore the indexes.
    ///
    /// Layer ids are registry-unique and monotonic, so id order is replay order across every branch.
    async fn rebuild_caches(
        &self,
        known: &[borg_core::Layer],
        index: &dyn DependencyIndexProvider,
        touches: &CellTouchIndex,
    ) -> Result<()> {
        let mut ordered: Vec<_> = known
            .iter()
            .filter(|layer| layer.state == LayerState::Committed)
            .collect();
        ordered.sort_by_key(|layer| layer.id.0);

        for layer in ordered {
            // A layer's *membership*, which for a merge layer is the child's events — so the touch
            // index learns that those cells were touched on the parent at the merge layer, which is
            // where a guard re-evaluated on the parent must see them.
            let mut stream = self.storage.read_layer(layer.id).await?;
            let mut events = Vec::new();
            while let Some(row) = stream.next().await {
                events.push(row?);
            }

            match layer.author {
                // Guards may name source cells only, so the touch index only ever needed these.
                LayerAuthor::Source => {
                    let refs: Vec<_> = events.into_iter().map(|event| event.cell).collect();
                    touches.record(layer.branch, layer.id, &refs)?;
                }
                LayerAuthor::Derived { producer, reflects } => {
                    for event in events {
                        let Some(derivation) = event.derivation else {
                            continue;
                        };
                        let invocation = Invocation {
                            producer,
                            input: *event.cell.pid(),
                        };
                        let written = [CellAt::new(event.cell, event.version)];
                        index.record(layer.branch, &invocation, &derivation.read_set, &written)?;
                    }
                    self.frontier.advance(layer.branch, producer, reflects);
                }
            }
        }
        Ok(())
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
            None,
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
