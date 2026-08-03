//! The read path and frontier tracking. SPEC.md §10, §11.

use borg_core::{BranchId, LayerId, ProducerId};
use std::collections::HashMap;
use std::sync::Mutex;

/// Per-producer watermarks and the settled frontier. SPEC.md §10.3.
///
/// A watermark is the source layer through which a producer has incorporated every input — *"replay
/// the world at this layer and you get exactly this output."*
#[derive(Default)]
pub struct FrontierTracker {
    watermarks: Mutex<HashMap<(BranchId, ProducerId), LayerId>>,
}

impl FrontierTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// How far this producer has caught up. `LayerId(0)` means "nothing incorporated yet", which is
    /// correct for a producer that has never run.
    pub fn watermark(&self, branch: BranchId, producer: ProducerId) -> LayerId {
        self.watermarks
            .lock()
            .unwrap()
            .get(&(branch, producer))
            .copied()
            .unwrap_or(LayerId(0))
    }

    /// Advance monotonically. Never moves backwards, so a late-arriving run cannot un-catch-up a
    /// producer.
    pub fn advance(&self, branch: BranchId, producer: ProducerId, to: LayerId) {
        let mut marks = self.watermarks.lock().unwrap();
        let entry = marks.entry((branch, producer)).or_insert(LayerId(0));
        if to.0 > entry.0 {
            *entry = to;
        }
    }

    /// The layer through which *all* derived data on this branch is caught up — the minimum over
    /// every producer. Reading here gives a fully coherent snapshot, slightly in the past, as
    /// opposed to the ragged head (SPEC.md §10.5).
    pub fn settled(&self, branch: BranchId, producers: &[ProducerId]) -> LayerId {
        producers
            .iter()
            .map(|p| self.watermark(branch, *p))
            .min()
            .unwrap_or(LayerId(0))
    }
}

// TODO(v1): Resolver — resolve + explain, migration path composition, validate-before-report.
