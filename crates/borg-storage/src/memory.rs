//! An in-memory `StorageProvider`.
//!
//! Exists to get the derivation cycle running end-to-end on the thinnest possible substrate — that
//! loop is the only genuinely unproven part of the design, and everything else can wait behind it.
//! Not durable, not sharded, not efficient.
//!
//! It does honour the two constraints that actually shape the interface: writes stream into an
//! invisible open layer, and nothing becomes visible until commit.

use crate::{CellStream, OpenLayer, SealedLayer, StorageProvider};
use async_trait::async_trait;
use borg_core::{BorgError, BranchId, BufferId, CellRecord, CellRef, LayerId, Result};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

type Committed = HashMap<BranchId, HashMap<CellRef, Vec<(LayerId, CellRecord)>>>;

#[derive(Default)]
struct Inner {
    /// Per cell, the history of writes in layer order. Time travel is a binary search over this.
    committed: Committed,
    /// Open and sealed layers, invisible to readers until commit.
    staged: HashMap<LayerId, StagedLayer>,
    /// A committed layer's contents — the changeset that drives invalidation (SPEC.md §9.6).
    layer_contents: HashMap<LayerId, Vec<(CellRef, CellRecord)>>,
}

struct StagedLayer {
    branch: BranchId,
    writes: Vec<(CellRef, CellRecord)>,
    sealed: bool,
}

#[derive(Clone, Default)]
pub struct MemoryStorage {
    inner: Arc<Mutex<Inner>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

/// A handle to one open layer. Writes accumulate here and are invisible to every reader until the
/// layer commits (SPEC.md §6.2).
pub struct MemoryOpenLayer {
    id: LayerId,
    inner: Arc<Mutex<Inner>>,
}

#[async_trait]
impl OpenLayer for MemoryOpenLayer {
    fn id(&self) -> LayerId {
        self.id
    }

    async fn put_cell(&mut self, cell: &CellRef, record: CellRecord) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let staged = inner
            .staged
            .get_mut(&self.id)
            .ok_or_else(|| BorgError::Storage(format!("layer {} is not open", self.id)))?;
        if staged.sealed {
            return Err(BorgError::Storage(format!("layer {} is sealed", self.id)));
        }
        staged.writes.push((cell.clone(), record));
        Ok(())
    }

    async fn seal(self: Box<Self>) -> Result<SealedLayer> {
        let mut inner = self.inner.lock().unwrap();
        let staged = inner
            .staged
            .get_mut(&self.id)
            .ok_or_else(|| BorgError::Storage(format!("layer {} is not open", self.id)))?;
        staged.sealed = true;
        Ok(SealedLayer { id: self.id })
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        self.inner.lock().unwrap().staged.remove(&self.id);
        Ok(())
    }
}

#[async_trait]
impl StorageProvider for MemoryStorage {
    async fn get_cell(
        &self,
        branch: BranchId,
        cell: &CellRef,
        layer: LayerId,
    ) -> Result<Option<CellRecord>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .committed
            .get(&branch)
            .and_then(|cells| cells.get(cell))
            .and_then(|history| {
                history
                    .iter()
                    .rev()
                    .find(|(written_at, _)| written_at.0 <= layer.0)
                    .map(|(_, record)| record.clone())
            }))
    }

    async fn scan_buffer(
        &self,
        branch: BranchId,
        buffer: &BufferId,
        layer: LayerId,
    ) -> Result<CellStream> {
        let inner = self.inner.lock().unwrap();
        let rows: Vec<_> = inner
            .committed
            .get(&branch)
            .into_iter()
            .flatten()
            .filter(|(cell, _)| &cell.buffer == buffer)
            .filter_map(|(cell, history)| {
                history
                    .iter()
                    .rev()
                    .find(|(written_at, _)| written_at.0 <= layer.0)
                    .map(|(_, record)| Ok((cell.clone(), record.clone())))
            })
            .collect();
        Ok(Box::pin(futures_util::stream::iter(rows)))
    }

    async fn read_layer(&self, layer: LayerId) -> Result<CellStream> {
        let inner = self.inner.lock().unwrap();
        let rows: Vec<_> = inner
            .layer_contents
            .get(&layer)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(Ok)
            .collect();
        Ok(Box::pin(futures_util::stream::iter(rows)))
    }

    async fn open_layer(&self, branch: BranchId, id: LayerId) -> Result<Box<dyn OpenLayer>> {
        let mut inner = self.inner.lock().unwrap();
        if inner.staged.contains_key(&id) || inner.layer_contents.contains_key(&id) {
            return Err(BorgError::Storage(format!("layer {id} already exists")));
        }
        inner.staged.insert(
            id,
            StagedLayer {
                branch,
                writes: Vec::new(),
                sealed: false,
            },
        );
        Ok(Box::new(MemoryOpenLayer {
            id,
            inner: Arc::clone(&self.inner),
        }))
    }

    async fn commit_layer(&self, layer: SealedLayer) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let staged = inner
            .staged
            .remove(&layer.id)
            .ok_or_else(|| BorgError::Storage(format!("layer {} is not staged", layer.id)))?;
        if !staged.sealed {
            return Err(BorgError::Storage(format!(
                "layer {} is not sealed",
                layer.id
            )));
        }
        let branch_cells = inner.committed.entry(staged.branch).or_default();
        for (cell, record) in &staged.writes {
            branch_cells
                .entry(cell.clone())
                .or_default()
                .push((layer.id, record.clone()));
        }
        inner.layer_contents.insert(layer.id, staged.writes);
        Ok(())
    }
}
