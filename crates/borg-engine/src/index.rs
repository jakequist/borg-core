//! The dependency index. SPEC.md §9.4, §11, §16.3.
//!
//! One structure, read two ways: **forward** (cell → dependents) drives invalidation, **backward**
//! (cell → dependencies) drives lineage. Lineage therefore requires no storage of its own.
//!
//! This is where essentially all of the engineering difficulty in Borg lives. Identity makes
//! normalization free, normalization concentrates fan-out, and the index is the bill for that:
//! flipping one widely-depended-on field can touch 100k dependents.

use borg_core::{BranchId, CellAt, ProducerId, Result};

/// One unit of producer work: a producer applied to one entity. SPEC.md §9.2.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Invocation {
    pub producer: ProducerId,
    pub input: borg_core::Pid,
}

/// v1 tracking is deliberately verbose: the fully-enumerated edge set, no compression.
///
/// For a run of `is_top_company` reading `Company#100.description` and `Person#300.title` and
/// writing `Company#100.is_top_company`, v1 stores exactly:
///
/// ```text
/// [Company#100.description, Person#300.title] -> [Company#100.is_top_company]
/// ```
///
/// Compressed and probabilistic policies — bloom filters, graded lineage from per-record to
/// whole-buffer — are deferred to v2+.
pub trait DependencyIndexProvider: Send + Sync {
    /// Record what an invocation read and wrote. Called once per successful producer run.
    fn record(
        &self,
        branch: BranchId,
        invocation: &Invocation,
        read_set: &[CellAt],
        write_set: &[CellAt],
    ) -> Result<()>;

    /// **Forward** — which invocations depend on these cells. Drives invalidation.
    ///
    /// Takes a slice rather than a single cell because the caller is always walking a committed
    /// layer, which may hold millions of writes.
    fn dependents(&self, branch: BranchId, cells: &[CellAt]) -> Result<Vec<Invocation>>;

    /// **Backward** — what this cell was computed from. Drives `explain()` (SPEC.md §11).
    fn dependencies(&self, branch: BranchId, cell: &CellAt) -> Result<Vec<CellAt>>;

    /// Which producer claimed this cell. Every field has exactly one writer (SPEC.md §8), and in v1
    /// ownership is discovered at runtime rather than declared.
    fn writer_of(&self, branch: BranchId, cell: &CellAt) -> Result<Option<ProducerId>>;

    /// Drop everything a producer recorded. Used when a producer's `ClientVersion` moves, which
    /// invalidates all of its prior output (SPEC.md §9.2).
    fn forget_producer(&self, branch: BranchId, producer: ProducerId) -> Result<()>;
}

// Note the shape of every method above: each takes an explicit key or key-slice and returns a
// bounded result. **Nothing iterates the whole index.** That constraint is what keeps the interface
// identical when the implementation shards by cell key (SPEC.md §17.2) — it is cheap to honor now
// and impossible to retrofit onto an API that hands out a whole map.

// --- Naive in-memory implementation ---

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

/// v1 `DependencyIndexProvider`: the fully-enumerated edge set, held in memory.
///
/// Deliberately dumb. Every method still takes an explicit key or key-slice and returns a bounded
/// result — nothing here iterates the whole index — so the interface survives being sharded by cell
/// key later (SPEC.md §17.2).
#[derive(Default)]
pub struct MemoryDependencyIndex {
    inner: Mutex<IndexInner>,
}

#[derive(Default)]
struct IndexInner {
    /// Forward: cell -> invocations that read it. Drives invalidation.
    ///
    /// A `HashSet`, not a `Vec`. A widely-shared upstream cell accumulates one entry per dependent,
    /// and every one of those dependents retracts itself on re-run — linear removal from a vector
    /// would make a fan-out of `n` cost `O(n²)`.
    dependents: HashMap<(BranchId, CellAt), HashSet<Invocation>>,
    /// Backward: cell -> what it was computed from. Drives lineage.
    dependencies: HashMap<(BranchId, CellAt), Vec<CellAt>>,
    /// Discovered field ownership. This lives here rather than in the def because discovery happens
    /// during derivation, and defs may only be mutated by def-layers (SPEC.md §8).
    writers: HashMap<(BranchId, CellAt), ProducerId>,
    /// What each invocation read, so a re-run can retract its stale forward edges.
    read_sets: HashMap<(BranchId, Invocation), Vec<CellAt>>,
}

impl MemoryDependencyIndex {
    pub fn new() -> Self {
        Self::default()
    }
}

impl DependencyIndexProvider for MemoryDependencyIndex {
    fn record(
        &self,
        branch: BranchId,
        invocation: &Invocation,
        read_set: &[CellAt],
        write_set: &[CellAt],
    ) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();

        // Retract the previous run's forward edges before adding this run's, or a producer whose
        // dependencies shrink would stay subscribed to cells it no longer reads.
        if let Some(previous) = inner.read_sets.remove(&(branch, invocation.clone())) {
            for cell in previous {
                if let Some(deps) = inner.dependents.get_mut(&(branch, cell)) {
                    deps.remove(invocation);
                }
            }
        }

        for cell in read_set {
            inner
                .dependents
                .entry((branch, cell.clone()))
                .or_default()
                .insert(invocation.clone());
        }
        inner
            .read_sets
            .insert((branch, invocation.clone()), read_set.to_vec());

        for cell in write_set {
            inner
                .dependencies
                .insert((branch, cell.clone()), read_set.to_vec());
            inner
                .writers
                .insert((branch, cell.clone()), invocation.producer);
        }
        Ok(())
    }

    fn dependents(&self, branch: BranchId, cells: &[CellAt]) -> Result<Vec<Invocation>> {
        let inner = self.inner.lock().unwrap();
        // Deduplicated through a set: one hot cell can name a very large number of dependents, and
        // a membership scan per candidate is the same `O(n²)` trap in a different place.
        let mut found: HashSet<Invocation> = HashSet::new();
        for cell in cells {
            if let Some(invocations) = inner.dependents.get(&(branch, cell.clone())) {
                found.extend(invocations.iter().cloned());
            }
        }
        Ok(found.into_iter().collect())
    }

    fn dependencies(&self, branch: BranchId, cell: &CellAt) -> Result<Vec<CellAt>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .dependencies
            .get(&(branch, cell.clone()))
            .cloned()
            .unwrap_or_default())
    }

    fn writer_of(&self, branch: BranchId, cell: &CellAt) -> Result<Option<ProducerId>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.writers.get(&(branch, cell.clone())).copied())
    }

    fn forget_producer(&self, branch: BranchId, producer: ProducerId) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.dependents.retain(|(b, _), v| {
            v.retain(|i| i.producer != producer);
            *b != branch || !v.is_empty()
        });
        inner
            .writers
            .retain(|(b, _), p| *b != branch || *p != producer);
        inner
            .read_sets
            .retain(|(b, i), _| *b != branch || i.producer != producer);
        Ok(())
    }
}
