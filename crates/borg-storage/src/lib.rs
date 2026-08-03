//! # borg-storage
//!
//! The `StorageProvider` seam. SPEC.md §17.1.
//!
//! Deliberately minimal, so that a plain KV store and Postgres remain equally viable. **Nothing
//! about derivation, dependency tracking, or watermarks appears here** — all of that lives above the
//! provider line, so that swapping backends never means reimplementing the engine.

pub mod memory;
pub use memory::MemoryStorage;

use async_trait::async_trait;
use borg_core::{BranchId, BufferId, CellRecord, CellRef, LayerId, Result};

/// A handle to an open, invisible layer that is accepting writes. SPEC.md §6.2.
///
/// **Streaming commit is the binding constraint.** One producer run writes all of its output into
/// exactly one layer, so a flip of a widely-depended-on field may produce a single layer containing
/// 100k mutations. A layer can never be assembled in memory and flushed; any provider that cannot
/// accept an unbounded write stream into an uncommitted layer is disqualified.
#[async_trait]
pub trait OpenLayer: Send {
    fn id(&self) -> LayerId;

    /// Append one cell write. Called an unbounded number of times.
    async fn put_cell(&mut self, cell: &CellRef, record: CellRecord) -> Result<()>;

    /// Close to writes. Validation and durability happen here.
    async fn seal(self: Box<Self>) -> Result<SealedLayer>;

    /// Discard. The layer never becomes visible.
    async fn abort(self: Box<Self>) -> Result<()>;
}

/// A sealed layer, not yet visible.
pub struct SealedLayer {
    pub id: LayerId,
}

#[async_trait]
pub trait StorageProvider: Send + Sync {
    /// Resolve one cell as of a layer. The engine layers migration, validation and provenance on
    /// top; this returns only what is physically stored.
    async fn get_cell(
        &self,
        branch: BranchId,
        cell: &CellRef,
        layer: LayerId,
    ) -> Result<Option<CellRecord>>;

    /// Enumerate a buffer. **Engine-internal only** — used by the scheduler to discover new
    /// entities (SPEC.md §9.6). Enumeration is not exposed as a user-facing query in v1.
    async fn scan_buffer(
        &self,
        branch: BranchId,
        buffer: &BufferId,
        layer: LayerId,
    ) -> Result<CellStream>;

    /// Read a committed layer's contents. This is what drives invalidation: the committed layer *is*
    /// the changeset, so buffers need no observation machinery (SPEC.md §9.6).
    async fn read_layer(&self, layer: LayerId) -> Result<CellStream>;

    async fn open_layer(&self, branch: BranchId, id: LayerId) -> Result<Box<dyn OpenLayer>>;

    /// Make a sealed layer visible. *This edge is what triggers dependent producers.*
    async fn commit_layer(&self, layer: SealedLayer) -> Result<()>;
}

/// A stream of cells. Boxed rather than returning a concrete iterator so that a remote provider can
/// page without the engine noticing.
pub type CellStream =
    std::pin::Pin<Box<dyn futures_core::Stream<Item = Result<(CellRef, CellRecord)>> + Send>>;
