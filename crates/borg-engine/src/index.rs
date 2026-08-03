//! The dependency index. SPEC.md §9.4, §11, §16.3.
//!
//! One structure, read two ways: **forward** (cell → dependents) drives invalidation, **backward**
//! (cell → dependencies) drives lineage. Lineage therefore requires no storage of its own.
//!
//! This is where essentially all of the engineering difficulty in Borg lives. Identity makes
//! normalization free, normalization concentrates fan-out, and the index is the bill for that:
//! flipping one widely-depended-on field can touch 100k dependents.

use borg_core::{BranchId, CellRef, ProducerId, Result};

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
        read_set: &[CellRef],
        write_set: &[CellRef],
    ) -> Result<()>;

    /// **Forward** — which invocations depend on these cells. Drives invalidation.
    ///
    /// Takes a slice rather than a single cell because the caller is always walking a committed
    /// layer, which may hold millions of writes.
    fn dependents(&self, branch: BranchId, cells: &[CellRef]) -> Result<Vec<Invocation>>;

    /// **Backward** — what this cell was computed from. Drives `explain()` (SPEC.md §11).
    fn dependencies(&self, branch: BranchId, cell: &CellRef) -> Result<Vec<CellRef>>;

    /// Which producer claimed this cell. Every field has exactly one writer (SPEC.md §8), and in v1
    /// ownership is discovered at runtime rather than declared.
    fn writer_of(&self, branch: BranchId, cell: &CellRef) -> Result<Option<ProducerId>>;

    /// Drop everything a producer recorded. Used when a producer's `ClientVersion` moves, which
    /// invalidates all of its prior output (SPEC.md §9.2).
    fn forget_producer(&self, branch: BranchId, producer: ProducerId) -> Result<()>;
}

// Note the shape of every method above: each takes an explicit key or key-slice and returns a
// bounded result. **Nothing iterates the whole index.** That constraint is what keeps the interface
// identical when the implementation shards by cell key (SPEC.md §17.2) — it is cheap to honor now
// and impossible to retrofit onto an API that hands out a whole map.
