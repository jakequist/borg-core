//! The cell-touch index. SPEC.md §12, §16.1.
//!
//! `cell -> layers that wrote it`, which is what a guard is checked against: *has anything touched
//! these cells since that layer?*
//!
//! **Only source layers are recorded.** Guards may reference source cells only, so a derived write
//! can never appear in a guard — and derived layers are the enormous ones. Skipping them bounds the
//! index by authored data rather than by everything the derivation engine produces.

use crate::projection::{Position, Projection};
use borg_core::{BranchId, CellRef, Event, Layer, LayerAuthor, LayerId, ReadPath, Result};
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
    /// How far this index has been folded. See [`crate::projection`]: the touch index is a
    /// projection of the log, and this is the field that lets a live one skip a replay.
    position: Position,
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
        self.position.reached(layer);
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

    /// The first cell in the set that has been touched since that layer, with the layer that did it.
    ///
    /// **Batched, and only batched.** A round's guard set is one probe per cell it read, and taking
    /// the lock per probe is the cost that shows up (§7.7: read-sets are unbounded) — so the
    /// per-cell form this replaced is gone rather than kept beside it, and there is no cheap-looking
    /// call left to reach for in a loop.
    ///
    /// Walking the whole path rather than one branch is what lets a child's guard be re-evaluated
    /// against its parent at merge time (SPEC.md §13).
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

/// `cell -> layers that wrote it`, folded from the source layers of the log.
///
/// The live feed is `LayerManager::commit`, which is the only witness a client write has. See
/// [`crate::projection`] for what the two lifecycles are and why they must agree.
impl Projection for CellTouchIndex {
    fn name(&self) -> &'static str {
        "touch index"
    }

    fn position(&self) -> LayerId {
        self.position.get()
    }

    fn wants(&self, layer: &Layer) -> bool {
        matches!(layer.author, LayerAuthor::Source)
    }

    fn apply(&self, layer: &Layer, events: &[Event]) -> Result<()> {
        if matches!(layer.author, LayerAuthor::Source) {
            // A layer's *membership*, which for a merge layer is the child's events — so the touch
            // index learns that those cells were touched on the parent at the merge layer, which is
            // where a guard re-evaluated on the parent must see them (§13).
            let cells: Vec<CellRef> = events.iter().map(|event| event.cell.clone()).collect();
            self.record(layer.branch, layer.id, &cells)?;
        }
        self.position.reached(layer.id);
        Ok(())
    }
}
