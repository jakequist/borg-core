//! The log: layer lifecycle and the state machine. SPEC.md §6.
//!
//! A layer is the universal unit of atomicity — client transactions and producer runs follow the
//! same `open → sealed → committed | aborted` path, through one code path.
//!
//! Two constraints shape everything here:
//!
//! * **Commit streams.** A layer may hold millions of mutations and can never be buffered whole.
//! * **Locks are per-layer, never per-branch.** A branch-wide lock would serialize derivation.

use crate::seams::LayerSequencer;
use crate::touch::CellTouchIndex;
use borg_core::{
    BorgError, Branch, BranchId, CellRecord, CellRef, Guard, Layer, LayerAuthor, LayerId,
    LayerKind, LayerState, Origin, ReadPath, Result,
};
use borg_storage::{OpenLayer, StorageProvider};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// An open layer, exclusive to its owner. Writes stream through it and are invisible to every reader
/// until commit.
pub struct LayerHandle {
    layer: Layer,
    open: Box<dyn OpenLayer>,
}

impl LayerHandle {
    pub const fn id(&self) -> LayerId {
        self.layer.id
    }

    /// Make this layer contingent on a condition. Validated at seal (SPEC.md §6.2).
    pub fn guard(&mut self, guard: Guard) {
        self.layer.guards.push(guard);
    }

    /// Append one cell write. Called an unbounded number of times — a producer run writes *all* of
    /// its output into this one layer.
    pub async fn put(&mut self, cell: &CellRef, record: CellRecord) -> Result<()> {
        self.open.put_cell(cell, record).await
    }

    /// Append a def mutation. A layer holds value events *xor* def events (SPEC.md §6.2), so a
    /// layer taking these takes no cells.
    pub async fn put_def(&mut self, event: borg_core::DefEvent) -> Result<()> {
        if self.layer.kind != LayerKind::Value {
            return self.open.put_def(event).await;
        }
        Err(BorgError::MixedLayerKind)
    }
}

pub struct LayerManager {
    storage: Arc<dyn StorageProvider>,
    sequencer: Arc<dyn LayerSequencer>,
    touches: Arc<CellTouchIndex>,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    layers: HashMap<LayerId, Layer>,
    heads: HashMap<BranchId, LayerId>,
    /// Branches live here rather than in `BranchManager` so that guard validation, which needs the
    /// ancestry, does not require the log to depend on the branch layer above it.
    branches: HashMap<BranchId, Branch>,
}

impl LayerManager {
    pub fn new(
        storage: Arc<dyn StorageProvider>,
        sequencer: Arc<dyn LayerSequencer>,
        touches: Arc<CellTouchIndex>,
    ) -> Self {
        Self {
            storage,
            sequencer,
            touches,
            state: Mutex::new(State::default()),
        }
    }

    pub fn register_branch(&self, branch: Branch) {
        self.state
            .lock()
            .unwrap()
            .branches
            .insert(branch.id, branch);
    }

    pub fn branch(&self, id: BranchId) -> Option<Branch> {
        self.state.lock().unwrap().branches.get(&id).cloned()
    }

    /// The ancestry a read resolves through. SPEC.md §7.2.
    pub fn read_path(&self, branch: BranchId, layer: Option<LayerId>) -> Result<ReadPath> {
        let mut current = branch;
        let mut bound = layer.or_else(|| self.head(branch)).unwrap_or(LayerId(0));
        let mut segments = Vec::new();

        loop {
            segments.push((current, bound));
            let Some(origin) = self.branch(current).and_then(|b| b.origin) else {
                break;
            };
            let Some(parent_layer) = self.layer(origin) else {
                break;
            };
            // An ancestor is bounded at the fork point — and *only* further clamped when the caller
            // asked for an explicit layer, which may sit below the fork point. Clamping by the
            // child's own bound instead would leave a fork that has not written anything yet unable
            // to see its parent at all, since its head is still nothing.
            bound = match layer {
                Some(requested) => LayerId(requested.0.min(origin.0)),
                None => origin,
            };
            current = parent_layer.branch;
        }
        Ok(ReadPath::new(segments))
    }

    /// Check a set of guards against a branch's history. SPEC.md §12.
    ///
    /// Used both at seal, with each guard's own `since`, and by merge, with the fork point — which
    /// is what makes guards the merge-conflict detector (SPEC.md §13).
    pub async fn check_guards(
        &self,
        branch: BranchId,
        guards: &[Guard],
        since: Option<LayerId>,
    ) -> Result<()> {
        if guards.is_empty() {
            return Ok(());
        }
        let path = self.read_path(branch, None)?;
        for guard in guards {
            let since = since.unwrap_or(guard.since);
            for cell in &guard.cells {
                // Guarding derived data would be checking a shadow: its value is a function of
                // source data with a lag (SPEC.md §12).
                let derived = self
                    .storage
                    .get_cell(&path, cell, borg_core::ClientVersion(LayerId(0)))
                    .await?
                    .is_some_and(|record| record.origin == Origin::Derived);
                if derived || self.is_derived_anywhere(&path, cell).await? {
                    return Err(BorgError::GuardOnDerivedCell { cell: cell.clone() });
                }
                if let Some(mutated_at) = self.touches.touched_since(&path, cell, since)? {
                    return Err(BorgError::GuardViolated {
                        cell: cell.clone(),
                        since,
                        mutated_at,
                    });
                }
            }
        }
        Ok(())
    }

    async fn is_derived_anywhere(&self, path: &ReadPath, cell: &CellRef) -> Result<bool> {
        for version in self.storage.cell_versions(path, cell).await? {
            if self
                .storage
                .get_cell(path, cell, version)
                .await?
                .is_some_and(|record| record.origin == Origin::Derived)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The most recently committed layer on a branch.
    pub fn head(&self, branch: BranchId) -> Option<LayerId> {
        self.state.lock().unwrap().heads.get(&branch).copied()
    }

    pub fn layer(&self, id: LayerId) -> Option<Layer> {
        self.state.lock().unwrap().layers.get(&id).cloned()
    }

    /// Every committed layer belonging to a branch. A layer belongs to exactly one branch
    /// (SPEC.md §6.2), so this needs no ancestry walk.
    pub fn layers_of(&self, branch: BranchId) -> Vec<Layer> {
        self.state
            .lock()
            .unwrap()
            .layers
            .values()
            .filter(|layer| layer.branch == branch && layer.state == LayerState::Committed)
            .cloned()
            .collect()
    }

    pub fn storage(&self) -> Arc<dyn StorageProvider> {
        Arc::clone(&self.storage)
    }

    /// Open a layer. Order within a branch is established at *commit*, not here — many layers may be
    /// open on a branch simultaneously (SPEC.md §7.3).
    pub async fn open(
        &self,
        branch: BranchId,
        kind: LayerKind,
        author: LayerAuthor,
    ) -> Result<LayerHandle> {
        let id = self.sequencer.next_layer_id(branch);
        let parent = self.head(branch);
        let layer = Layer {
            id,
            branch,
            kind,
            author,
            state: LayerState::Open,
            parent,
            guards: Vec::new(),
        };
        self.state.lock().unwrap().layers.insert(id, layer.clone());
        let open = self.storage.open_layer(branch, id).await?;
        Ok(LayerHandle { layer, open })
    }

    /// Seal, then commit. **The commit edge is what triggers dependent producers** — callers hand
    /// the returned id to the invalidator.
    pub async fn commit(&self, handle: LayerHandle) -> Result<LayerId> {
        let LayerHandle { layer, open } = handle;
        self.transition(layer.id, LayerState::Open, LayerState::Sealed)?;

        // Guards are validated at seal — before anything becomes visible, so a rejected transaction
        // leaves no trace (SPEC.md §6.2).
        if let Err(violation) = self.check_guards(layer.branch, &layer.guards, None).await {
            open.abort().await?;
            self.transition(layer.id, LayerState::Sealed, LayerState::Aborted)?;
            return Err(violation);
        }
        // Copy the guards onto the stored layer so merge can re-evaluate them later. Only the
        // guards — writing the whole handle back would clobber the state machine with the handle's
        // stale `Open`.
        if let Some(stored) = self.state.lock().unwrap().layers.get_mut(&layer.id) {
            stored.guards = layer.guards.clone();
        }

        let sealed = open.seal().await?;
        self.storage.commit_layer(sealed).await?;
        self.transition(layer.id, LayerState::Sealed, LayerState::Committed)?;
        self.state
            .lock()
            .unwrap()
            .heads
            .insert(layer.branch, layer.id);

        // Feed the touch index by streaming the committed layer. Source layers only: guards may name
        // source cells only, and derived layers are the enormous ones.
        if matches!(layer.author, LayerAuthor::Source) {
            let mut stream = self.storage.read_layer(layer.id).await?;
            let mut cells = Vec::new();
            while let Some(row) = stream.next().await {
                cells.push(row?.0);
            }
            self.touches.record(layer.branch, layer.id, &cells)?;
        }
        Ok(layer.id)
    }

    pub fn touches(&self) -> Arc<CellTouchIndex> {
        Arc::clone(&self.touches)
    }

    /// Discard. The layer never becomes visible — which is what lets a failed producer run leave no
    /// trace (SPEC.md §14).
    pub async fn abort(&self, handle: LayerHandle) -> Result<()> {
        let LayerHandle { layer, open } = handle;
        open.abort().await?;
        self.transition(layer.id, LayerState::Open, LayerState::Aborted)
    }

    fn transition(&self, id: LayerId, expected: LayerState, next: LayerState) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        let layer = state
            .layers
            .get_mut(&id)
            .ok_or(BorgError::Storage(format!("unknown layer {id}")))?;
        if layer.state != expected {
            return Err(BorgError::LayerStateViolation {
                layer: id,
                expected,
                actual: layer.state,
            });
        }
        layer.state = next;
        Ok(())
    }
}
