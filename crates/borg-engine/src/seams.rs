//! Distribution seams. SPEC.md §17.2.
//!
//! Borg is **designed to be distributable and implemented single-node.** Building a distributed
//! system from day one reliably produces coordination overhead everywhere, distributed code paths no
//! test exercises, and a system that is both slower and still not distributed.
//!
//! So every point that would require coordination sits behind a trait here, with a naive in-process
//! implementation. Distribution later is a handful of swaps, not a rewrite.

use borg_core::{BranchId, LayerId, ProducerId, Result};
use std::time::Duration;

/// Assigns commit order within a branch. SPEC.md §7.3.
///
/// Layers are totally ordered within a branch, but that order is established **at commit**, not at
/// open — many layers may be open on a branch simultaneously.
///
/// v1: an in-process atomic counter. Later: consensus, or partition by branch.
pub trait LayerSequencer: Send + Sync {
    /// Reserve an id for a layer about to be opened.
    fn next_layer_id(&self, branch: BranchId) -> LayerId;
}

/// Grants exclusive ownership of an open layer. SPEC.md §6.2.
///
/// Locks are held **per layer, never per branch** — a branch-wide write lock would serialize
/// derivation and defeat the whole design.
///
/// Leases rather than mutexes, because a producer run may hold a layer open across 100k mutations
/// and the holder can die. An expired lease must be abortable by someone else.
pub trait LockManager: Send + Sync {
    fn acquire(&self, layer: LayerId, ttl: Duration) -> Result<LayerLease>;
    fn renew(&self, lease: &LayerLease, ttl: Duration) -> Result<()>;
    fn release(&self, lease: LayerLease) -> Result<()>;
}

#[derive(Debug)]
pub struct LayerLease {
    pub layer: LayerId,
    pub token: u64,
}

/// Supplies pending work. SPEC.md §16.4.
///
/// **There is no queue.** Pending work is fully implied by the gap between a producer's watermark
/// and the branch head, plus the dependency index — so this derives what to run by streaming the
/// layers in that gap rather than materializing a list of invocations.
///
/// Three properties fall out: bounded memory (work is streamed, never accumulated), free crash
/// recovery (restart and recompute the gap; there is no queue to lose), and distributability
/// (workers derive their own work from shared state instead of contending on a shared queue).
///
/// This trait is identical single-node and distributed, which is why the "Later" column of
/// SPEC.md §17.2 reads *unchanged*.
pub trait WorkSource: Send + Sync {
    /// The gap this producer must close on this branch: everything from its watermark to head.
    fn pending(&self, branch: BranchId, producer: ProducerId) -> Result<WorkGap>;
}

/// A half-open range of source layers a producer has yet to incorporate.
#[derive(Clone, Copy, Debug)]
pub struct WorkGap {
    pub producer: ProducerId,
    /// Exclusive lower bound — the producer's current watermark.
    pub from: LayerId,
    /// Inclusive upper bound — where the branch head stands in the **source** stream at the time of
    /// asking. A source position and not the head itself, because a watermark points into the source
    /// stream (§6.3) and the two ends of a range have to be commensurable: a settled branch's head is
    /// a derived layer above every watermark, and comparing against it would report every settled
    /// branch as permanently behind.
    pub to: LayerId,
}

impl WorkGap {
    pub const fn is_empty(&self) -> bool {
        self.from.0 >= self.to.0
    }
}

// --- Naive in-process implementations (SPEC.md §17.2) ---

/// v1 `LayerSequencer`: a process-local counter.
///
/// Layer ids are registry-unique rather than per-branch, so one counter serves every branch.
#[derive(Default)]
pub struct InProcessSequencer {
    next: std::sync::atomic::AtomicU64,
}

impl InProcessSequencer {
    pub fn new() -> Self {
        Self::resuming_after(LayerId(0))
    }

    /// Continue after the highest layer a store already holds. Layer ids are registry-unique, so
    /// restarting the count would collide with history.
    pub fn resuming_after(highest: LayerId) -> Self {
        Self {
            next: std::sync::atomic::AtomicU64::new(highest.0 + 1),
        }
    }
}

impl LayerSequencer for InProcessSequencer {
    fn next_layer_id(&self, _branch: BranchId) -> LayerId {
        LayerId(self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
    }
}
