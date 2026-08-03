//! The cell-touch index. SPEC.md §12, §16.1.
//!
//! `cell -> layers that wrote it`, which is what a guard is checked against: *has anything touched
//! these cells since that layer?*
//!
//! **Only source layers are recorded.** Guards may reference source cells only, so a derived write
//! can never appear in a guard — and derived layers are the enormous ones. Skipping them bounds the
//! index by authored data rather than by everything the derivation engine produces.

use borg_core::{BranchId, CellRef, LayerId, ReadPath, Result};
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Default)]
pub struct CellTouchIndex {
    inner: Mutex<HashMap<(BranchId, CellRef), Vec<LayerId>>>,
}

impl CellTouchIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that a layer wrote these cells. Fed by streaming a committed layer, never by
    /// buffering one — a layer may hold millions of mutations (SPEC.md §6.2).
    pub fn record(&self, branch: BranchId, layer: LayerId, cells: &[CellRef]) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        for cell in cells {
            let history = inner.entry((branch, cell.clone())).or_default();
            if history.last() != Some(&layer) {
                history.push(layer);
            }
        }
        Ok(())
    }

    /// The first layer strictly after `since` that touched this cell, anywhere along the path.
    ///
    /// Walking the whole path rather than one branch is what lets a child's guard be re-evaluated
    /// against its parent at merge time (SPEC.md §13).
    pub fn touched_since(
        &self,
        path: &ReadPath,
        cell: &CellRef,
        since: LayerId,
    ) -> Result<Option<LayerId>> {
        let inner = self.inner.lock().unwrap();
        let mut earliest: Option<LayerId> = None;
        for (branch, bound) in &path.segments {
            let Some(history) = inner.get(&(*branch, cell.clone())) else {
                continue;
            };
            for layer in history {
                if layer.0 > since.0
                    && layer.0 <= bound.0
                    && earliest.is_none_or(|found| layer.0 < found.0)
                {
                    earliest = Some(*layer);
                }
            }
        }
        Ok(earliest)
    }
}
