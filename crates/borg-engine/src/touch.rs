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

/// Nested by branch rather than keyed on `(BranchId, CellRef)`.
///
/// A flat tuple key cannot be looked up from a borrowed cell — `HashMap::get` wants the whole key,
/// so every probe had to clone a `CellRef`, and a `CellRef` owns two or three `String`s. A round
/// checks one guard per cell it read, which at a 128k fan-out is close to a million probes in one
/// merge; the clone was most of what they cost.
#[derive(Default)]
pub struct CellTouchIndex {
    inner: Mutex<Touches>,
}

#[derive(Default)]
struct Touches {
    cells: HashMap<BranchId, HashMap<CellRef, Vec<LayerId>>>,
    /// The highest layer recorded on each branch — the whole index summarised down to the one
    /// question a guard set can be answered *collectively* by. See [`CellTouchIndex::moved_since`].
    highest: HashMap<BranchId, LayerId>,
}

impl CellTouchIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that a layer wrote these cells. Fed by streaming a committed layer, never by
    /// buffering one — a layer may hold millions of mutations (SPEC.md §6.2).
    pub fn record(&self, branch: BranchId, layer: LayerId, cells: &[CellRef]) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let highest = inner.highest.entry(branch).or_insert(layer);
        if layer.0 > highest.0 {
            *highest = layer;
        }
        let branch = inner.cells.entry(branch).or_default();
        for cell in cells {
            let history = branch.entry(cell.clone()).or_default();
            if history.last() != Some(&layer) {
                history.push(layer);
            }
        }
        Ok(())
    }

    /// Has **anything** been written along this path since that layer?
    ///
    /// The cheap negative that makes an uncontended merge free. A guard fails only if some *source*
    /// layer touched the cell it names, so if no source layer has landed anywhere on the path since
    /// the fork point, no guard on it can trip — however many cells the guard set holds. A round's
    /// guard set is the sum of its producers' read-sets and is unbounded (§7.7); on a branch with no
    /// concurrent writer this answers all of it in a couple of map lookups, and the guard set never
    /// has to be built.
    ///
    /// A `true` says only *check properly*. It never stands in for a guard failure.
    pub fn moved_since(&self, path: &ReadPath, since: LayerId) -> Result<bool> {
        let inner = self.inner.lock().unwrap();
        Ok(path.segments.iter().any(|(branch, bound)| {
            inner
                .highest
                .get(branch)
                .is_some_and(|layer| layer.0 > since.0 && layer.0 <= bound.0)
        }))
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
        Ok(Self::lookup(&inner, path, cell, since))
    }

    /// The first cell in the set that has been touched since that layer, with the layer that did it.
    ///
    /// The batched form, because a round's guard set is one probe per cell it read and taking the
    /// lock per probe is the cost that shows up (§7.7 of the transactional draft: read-sets are
    /// unbounded). Same question as [`touched_since`](Self::touched_since), asked once.
    pub fn first_touched_since<'a>(
        &self,
        path: &ReadPath,
        cells: impl IntoIterator<Item = &'a CellRef>,
        since: LayerId,
    ) -> Result<Option<(&'a CellRef, LayerId)>> {
        let inner = self.inner.lock().unwrap();
        for cell in cells {
            if let Some(layer) = Self::lookup(&inner, path, cell, since) {
                return Ok(Some((cell, layer)));
            }
        }
        Ok(None)
    }

    fn lookup(inner: &Touches, path: &ReadPath, cell: &CellRef, since: LayerId) -> Option<LayerId> {
        let mut earliest: Option<LayerId> = None;
        for (branch, bound) in &path.segments {
            let Some(history) = inner.cells.get(branch).and_then(|cells| cells.get(cell)) else {
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
        earliest
    }
}
