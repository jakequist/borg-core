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
use borg_core::{
    BorgError, BranchId, CellRecord, CellRef, Layer, LayerAuthor, LayerId, LayerKind, LayerState,
    Result,
};
use borg_storage::{OpenLayer, StorageProvider};
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

    /// Append one cell write. Called an unbounded number of times — a producer run writes *all* of
    /// its output into this one layer.
    pub async fn put(&mut self, cell: &CellRef, record: CellRecord) -> Result<()> {
        self.open.put_cell(cell, record).await
    }
}

pub struct LayerManager {
    storage: Arc<dyn StorageProvider>,
    sequencer: Arc<dyn LayerSequencer>,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    layers: HashMap<LayerId, Layer>,
    heads: HashMap<BranchId, LayerId>,
}

impl LayerManager {
    pub fn new(storage: Arc<dyn StorageProvider>, sequencer: Arc<dyn LayerSequencer>) -> Self {
        Self {
            storage,
            sequencer,
            state: Mutex::new(State::default()),
        }
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
        let sealed = open.seal().await?;

        self.storage.commit_layer(sealed).await?;
        self.transition(layer.id, LayerState::Sealed, LayerState::Committed)?;

        self.state
            .lock()
            .unwrap()
            .heads
            .insert(layer.branch, layer.id);
        Ok(layer.id)
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
