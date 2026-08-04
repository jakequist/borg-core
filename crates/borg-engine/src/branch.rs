//! Branches, forking, and merge. SPEC.md §7, §13.
//!
//! A fork is O(1) even under eager derivation: a new branch inherits its parent's layers — source
//! and derived alike — by ancestry, and diverges only where it writes.
//!
//! Merge replays the child's **source** layers onto the parent as new layers. Derived layers are
//! skipped, because the child's derived values are wrong on the parent by construction: they were
//! computed from different data.
//!
//! **Merge does not copy events.** A parent layer *names* the events of the child layer it replays
//! (§13). The events are not rewritten, so `authored` still says where they were written and the
//! parent layer says where they landed — the two facts the old model collapsed into one. What this
//! costs is `n` membership rows and `n` read-index entries rather than `n` full records carrying
//! value, version, origin, derivation and read-set: a large constant factor, and deliberately not
//! an asymptotic one. Genuine `O(1)` needs a parent layer to reference a child layer's event *set*
//! rather than enumerate it, which grows the read path per merge and needs compaction to pay for
//! itself. Deferred.

use crate::log::LayerManager;
use borg_core::{
    BorgError, Branch, BranchId, CellRef, Event, FieldName, LayerAuthor, LayerId, LayerKind,
    MergeMode, MergeRejection, ObjectTypeName, ReadPath, Result,
};
use borg_storage::StorageProvider;
use futures_util::StreamExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct BranchManager {
    layers: Arc<LayerManager>,
    storage: Arc<dyn StorageProvider>,
    next_id: AtomicU64,
}

impl BranchManager {
    pub fn new(layers: Arc<LayerManager>) -> Self {
        Self::resuming(layers, 1)
    }

    /// Resume id allocation after reopening a store, so a second process cannot mint a branch id
    /// that already exists.
    pub fn resuming(layers: Arc<LayerManager>, next_id: u64) -> Self {
        let storage = layers.storage();
        let next = layers
            .branches()
            .iter()
            .map(|b| b.id.0 + 1)
            .chain(std::iter::once(next_id))
            .max()
            .unwrap_or(1);
        Self {
            layers,
            storage,
            next_id: AtomicU64::new(next),
        }
    }

    /// Branches with no origin — the roots of the tree.
    pub fn roots(&self) -> Vec<BranchId> {
        self.layers
            .branches()
            .into_iter()
            .filter(|b| b.origin.is_none())
            .map(|b| b.id)
            .collect()
    }

    pub fn all(&self) -> Vec<Branch> {
        self.layers.branches()
    }

    async fn persist(&self, branch: &Branch) -> Result<()> {
        self.layers.register_branch(branch.clone());
        self.storage.put_branch(branch).await
    }

    /// The root of the tree: a branch with no origin.
    pub async fn create_root(&self, name: Option<String>) -> Result<BranchId> {
        let id = BranchId(self.next_id.fetch_add(1, Ordering::Relaxed));
        self.persist(&Branch {
            id,
            name,
            origin: None,
        })
        .await?;
        Ok(id)
    }

    /// Fork at a layer. O(1) — nothing is copied.
    pub async fn fork(
        &self,
        parent: BranchId,
        at: LayerId,
        name: Option<String>,
    ) -> Result<BranchId> {
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
        self.persist(&Branch {
            id,
            name,
            origin: Some(at),
        })
        .await?;
        Ok(id)
    }

    pub fn branch(&self, id: BranchId) -> Option<Branch> {
        self.layers.branch(id)
    }

    /// The parent branch, inferred from the origin layer. There is deliberately no parent pointer
    /// (SPEC.md §7.1).
    pub fn parent_of(&self, id: BranchId) -> Option<BranchId> {
        let origin = self.branch(id)?.origin?;
        self.layers.layer(origin).map(|layer| layer.branch)
    }

    /// The ancestry a read resolves through. SPEC.md §7.2.
    pub fn read_path(&self, branch: BranchId, layer: Option<LayerId>) -> Result<ReadPath> {
        self.layers.read_path(branch, layer)
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
        let moved_on_parent = self.defs_touched_since(parent, origin).await?;

        let mut staged = Vec::new();
        for layer in &replayable {
            // The child authored its def-mutations against the def-view at the fork point. If the
            // parent has moved the same def since, they cannot cleanly rebase — re-fork from head
            // and redo (SPEC.md §13).
            for event in self.storage.read_def_layer(*layer).await? {
                if let Some(touched) = event.touches()
                    && moved_on_parent.contains(&touched)
                {
                    return Err(BorgError::MergeRejected(MergeRejection::DefDiverged {
                        struct_name: touched.0,
                    }));
                }
            }
            let events = self.contents_of(*layer).await?;
            self.check_dangling(parent, origin, &events).await?;

            // Re-evaluate the child's guards against the *parent*, since the fork point. "Has the
            // parent touched this while I was working?" is exactly the definition of a merge
            // conflict, so guards double as the conflict detector for free (SPEC.md §13).
            let guards = self
                .layers
                .layer(*layer)
                .map(|l| l.guards)
                .unwrap_or_default();
            if let Err(violation) = self
                .layers
                .check_guards(parent, &guards, Some(origin))
                .await
            {
                return Err(match violation {
                    BorgError::GuardViolated { cell, .. } => {
                        BorgError::MergeRejected(MergeRejection::GuardConflict { cell })
                    }
                    other => other,
                });
            }
            staged.push(events);
        }

        let mut replayed = Vec::new();
        for (source, events) in replayable.iter().zip(staged) {
            let kind = self
                .layers
                .layer(*source)
                .map_or(LayerKind::Value, |layer| layer.kind);
            let mut layer = self.layers.open(parent, kind, LayerAuthor::Source).await?;
            for event in self.storage.read_def_layer(*source).await? {
                layer.put_def(event).await?;
            }
            for event in events {
                // **The whole change, in one line.** The parent layer names the child's event; it
                // does not rewrite it. The event keeps its identity, the ClientVersion it was
                // authored at — so the parent's readers migrate and nothing is coerced (§13) — and
                // the layer that authored it, which is what makes lineage survive the merge.
                //
                // Membership is also the reason this needs no revalidation. These events were
                // validated once, on the child, against the def-view they were authored under, and
                // the def layers that made them legal are replayed in this same merge. Re-checking
                // them against a def-view that is itself mid-replay would reject a `DefOnly`
                // merge's own data half and turn "the merge is atomic" into "the merge is atomic if
                // you ordered your layers correctly".
                layer.include(event.id).await?;
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

    /// Which definitions the parent has moved since the fork point.
    async fn defs_touched_since(
        &self,
        parent: BranchId,
        fork_point: LayerId,
    ) -> Result<Vec<(ObjectTypeName, FieldName)>> {
        let mut touched = Vec::new();
        for layer in self.layers.layers_of(parent) {
            if layer.kind != LayerKind::Def || layer.id.0 <= fork_point.0 {
                continue;
            }
            for event in self.storage.read_def_layer(layer.id).await? {
                if let Some(key) = event.touches()
                    && !touched.contains(&key)
                {
                    touched.push(key);
                }
            }
        }
        Ok(touched)
    }

    /// A layer's membership, oldest first.
    async fn contents_of(&self, layer: LayerId) -> Result<Vec<Event>> {
        let mut stream = self.storage.read_layer(layer).await?;
        let mut events = Vec::new();
        while let Some(row) = stream.next().await {
            events.push(row?);
        }
        Ok(events)
    }

    /// Reject if the child wrote to an object the parent has since deleted. SPEC.md §13.
    async fn check_dangling(
        &self,
        parent: BranchId,
        fork_point: LayerId,
        events: &[Event],
    ) -> Result<()> {
        let path = self.read_path(parent, None)?;
        for event in events {
            let existence = CellRef::existence_of(&event.cell);
            if existence == event.cell {
                continue;
            }
            // *Landed*, not authored: the question is whether the deletion became visible on the
            // parent after the fork, and a tombstone the parent inherited by merge was authored
            // somewhere else entirely.
            //
            // Read unversioned, not at `event.version`: that is the def-version of the *property*
            // this event wrote, and an existence cell sits on no chain of its own (§5.2).
            let deleted = self
                .storage
                .get_cell(&path, &existence, borg_core::DefVersion::UNVERSIONED)
                .await?
                .filter(|found| found.event.value.is_tombstone())
                .filter(|found| found.landed_at.0 > fork_point.0);
            if let Some(found) = deleted {
                return Err(BorgError::MergeRejected(MergeRejection::DanglingWrite {
                    cell: event.cell.clone(),
                    deleted_at: found.landed_at,
                }));
            }
        }
        Ok(())
    }
}

//
//
// Everything else is last-write-wins per cell, which is the documented default: guards are the
// opt-in to safety, not the baseline.
