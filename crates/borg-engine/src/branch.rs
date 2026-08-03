//! Branches, forking, and merge. SPEC.md §7, §13.
//!
//! A fork is O(1) even under eager derivation: a new branch inherits its parent's layers — source
//! and derived alike — by ancestry, and diverges only where it writes.
//!
//! Merge replays the child's **source** layers onto the parent as new layers. Derived layers are
//! skipped, because the child's derived values are wrong on the parent by construction: they were
//! computed from different data.

use crate::log::LayerManager;
use borg_core::{
    BorgError, Branch, BranchId, CellRecord, CellRef, LayerAuthor, LayerId, LayerKind, MergeMode,
    MergeRejection, ReadPath, Result,
};
use borg_storage::StorageProvider;
use futures_util::StreamExt;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub struct BranchManager {
    layers: Arc<LayerManager>,
    storage: Arc<dyn StorageProvider>,
    branches: Mutex<HashMap<BranchId, Branch>>,
    next_id: AtomicU64,
}

impl BranchManager {
    pub fn new(layers: Arc<LayerManager>) -> Self {
        let storage = layers.storage();
        Self {
            layers,
            storage,
            branches: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    /// The root of the tree: a branch with no origin.
    pub fn create_root(&self, name: Option<String>) -> BranchId {
        let id = BranchId(self.next_id.fetch_add(1, Ordering::Relaxed));
        self.branches.lock().unwrap().insert(
            id,
            Branch {
                id,
                name,
                origin: None,
            },
        );
        id
    }

    /// Fork at a layer. O(1) — nothing is copied.
    pub fn fork(&self, parent: BranchId, at: LayerId, name: Option<String>) -> Result<BranchId> {
        let layer = self
            .layers
            .layer(at)
            .ok_or(BorgError::Storage(format!("unknown layer {at}")))?;
        if layer.branch != parent {
            return Err(BorgError::LayerNotOnBranch {
                layer: at,
                branch: parent,
            });
        }
        let id = BranchId(self.next_id.fetch_add(1, Ordering::Relaxed));
        self.branches.lock().unwrap().insert(
            id,
            Branch {
                id,
                name,
                origin: Some(at),
            },
        );
        Ok(id)
    }

    pub fn branch(&self, id: BranchId) -> Option<Branch> {
        self.branches.lock().unwrap().get(&id).cloned()
    }

    /// The parent branch, inferred from the origin layer. There is deliberately no parent pointer
    /// (SPEC.md §7.1).
    pub fn parent_of(&self, id: BranchId) -> Option<BranchId> {
        let origin = self.branch(id)?.origin?;
        self.layers.layer(origin).map(|layer| layer.branch)
    }

    /// The ancestry a read resolves through: this branch bounded at `layer` (or its head), then each
    /// ancestor bounded at the fork point below it.
    pub fn read_path(&self, branch: BranchId, layer: Option<LayerId>) -> Result<ReadPath> {
        let mut current = branch;
        let mut bound = layer
            .or_else(|| self.layers.head(branch))
            .unwrap_or(LayerId(0));
        let mut segments = Vec::new();

        loop {
            segments.push((current, bound));
            let Some(origin) = self.branch(current).and_then(|b| b.origin) else {
                break;
            };
            let Some(parent_layer) = self.layers.layer(origin) else {
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

    /// Replay a child's source layers onto its parent. SPEC.md §13.
    ///
    /// Returns the new parent layers. v1 rejects a whole merge rather than applying it partially,
    /// and validates everything *before* writing anything, so a rejected merge leaves no trace.
    pub async fn merge(&self, child: BranchId, mode: MergeMode) -> Result<Vec<LayerId>> {
        let origin = self
            .branch(child)
            .and_then(|b| b.origin)
            .ok_or(BorgError::Storage("cannot merge the root branch".into()))?;
        let parent = self
            .parent_of(child)
            .ok_or(BorgError::Storage("child has no parent".into()))?;

        let replayable = self.replayable_layers(child, mode);

        // Validate the whole merge first. Nothing is written until every layer has passed.
        let mut staged = Vec::new();
        for layer in &replayable {
            let writes = self.contents_of(*layer).await?;
            self.check_dangling(parent, origin, &writes).await?;
            staged.push(writes);
        }

        let mut replayed = Vec::new();
        for (source, writes) in replayable.iter().zip(staged) {
            let kind = self
                .layers
                .layer(*source)
                .map_or(LayerKind::Value, |layer| layer.kind);
            let mut layer = self.layers.open(parent, kind, LayerAuthor::Source).await?;
            for (cell, mut record) in writes {
                // Each event keeps the ClientVersion it was authored at, so the parent's readers
                // migrate rather than anything being coerced (SPEC.md §13).
                record.written_at = layer.id();
                layer.put(&cell, record).await?;
            }
            replayed.push(self.layers.commit(layer).await?);
        }
        Ok(replayed)
    }

    /// The child's own **source** layers, oldest first.
    ///
    /// Derived layers are skipped: the child's derived values were computed from the child's data
    /// and are wrong on the parent by construction. The parent re-derives (SPEC.md §13).
    fn replayable_layers(&self, child: BranchId, mode: MergeMode) -> Vec<LayerId> {
        let mut layers: Vec<_> = self
            .layers
            .layers_of(child)
            .into_iter()
            .filter(|layer| matches!(layer.author, LayerAuthor::Source))
            .filter(|layer| match mode {
                MergeMode::DefOnly => layer.kind == LayerKind::Def,
                MergeMode::DefAndData => true,
            })
            .collect();
        layers.sort_by_key(|layer| layer.id.0);
        layers.into_iter().map(|layer| layer.id).collect()
    }

    async fn contents_of(&self, layer: LayerId) -> Result<Vec<(CellRef, CellRecord)>> {
        let mut stream = self.storage.read_layer(layer).await?;
        let mut writes = Vec::new();
        while let Some(row) = stream.next().await {
            writes.push(row?);
        }
        Ok(writes)
    }

    /// Reject if the child wrote to an object the parent has since deleted. SPEC.md §13.
    async fn check_dangling(
        &self,
        parent: BranchId,
        fork_point: LayerId,
        writes: &[(CellRef, CellRecord)],
    ) -> Result<()> {
        let path = self.read_path(parent, None)?;
        for (cell, record) in writes {
            let existence = CellRef::existence_of(cell);
            if existence == *cell {
                continue;
            }
            let deleted = self
                .storage
                .get_cell(&path, &existence, record.version)
                .await?
                .filter(|found| found.value.is_tombstone())
                .filter(|found| found.written_at.0 > fork_point.0);
            if let Some(found) = deleted {
                return Err(BorgError::MergeRejected(MergeRejection::DanglingWrite {
                    cell: cell.clone(),
                    deleted_at: found.written_at,
                }));
            }
        }
        Ok(())
    }
}

// TODO(v1): def divergence — reject when the parent moved the same def since the fork point.
// Needs def-pushes to be real DefEvents on a branch first; today `DefRegistry` is populated directly.
//
// TODO(v1): guard conflicts — re-evaluating the child's guards against the parent's history since
// the fork point *is* the merge-conflict detector (SPEC.md §13). Waits on object transactions.
//
// Until both land, merge is last-write-wins per cell, which is the documented default.
