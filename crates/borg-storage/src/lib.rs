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
use borg_core::{
    Branch, BranchId, BufferId, CellRecord, CellRef, ClientVersion, DefEvent, Layer, LayerId, Pid,
    PidKind, ReadPath, Result,
};

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

    /// Append one def mutation. A layer holds value events *xor* def events, never both
    /// (SPEC.md §6.2), so a layer taking these takes no cells.
    async fn put_def(&mut self, event: DefEvent) -> Result<()>;

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
    /// Resolve one cell as of a layer, **at one def-version**.
    ///
    /// Version is part of the key, not a filter. Writes are never coerced (SPEC.md §5.4), so one
    /// cell may be materialized at several versions simultaneously — the value a v1 client wrote and
    /// the migrated view a v5 client reads coexist. Were version merely a tag, a migration would
    /// overwrite the very value it migrated from.
    ///
    /// Returns only what is physically stored; the engine layers migration, validation and
    /// provenance on top.
    /// Reads resolve through a [`ReadPath`] rather than a bare branch, so that a fork inherits its
    /// parent by ancestry instead of by copying (SPEC.md §7.2). Storage never has to know what a
    /// branch *is* — only how to walk the segments it is handed.
    async fn get_cell(
        &self,
        path: &ReadPath,
        cell: &CellRef,
        version: ClientVersion,
    ) -> Result<Option<CellRecord>>;

    /// Which def-versions this cell is materialized at, as of a layer. Used by the resolver to find
    /// a migration path when the requested version is not yet materialized.
    async fn cell_versions(&self, path: &ReadPath, cell: &CellRef) -> Result<Vec<ClientVersion>>;

    /// Enumerate a buffer. **Engine-internal only** — used by the scheduler to discover new
    /// entities (SPEC.md §9.6). Enumeration is not exposed as a user-facing query in v1.
    async fn scan_buffer(&self, path: &ReadPath, buffer: &BufferId) -> Result<CellStream>;

    /// Read a committed layer's contents. This is what drives invalidation: the committed layer *is*
    /// the changeset, so buffers need no observation machinery (SPEC.md §9.6).
    async fn read_layer(&self, layer: LayerId) -> Result<CellStream>;

    /// A committed def layer's events, in order.
    async fn read_def_layer(&self, layer: LayerId) -> Result<Vec<DefEvent>>;

    async fn open_layer(&self, branch: BranchId, id: LayerId) -> Result<Box<dyn OpenLayer>>;

    // --- Interned values ---
    //
    // Content-addressed values are branch-independent and eternal (SPEC.md §3.1), so these take no
    // [`ReadPath`], no layer and no def-version: there is nothing here for two branches to disagree
    // about, and nothing to time-travel through. That absence is the feature — it is what makes a
    // string write unable to conflict across branches and what stores equal strings exactly once
    // registry-wide.
    //
    // The buffer is not a parameter either. A PID encodes its own kind, so dispatch to the
    // `String`/`Binary`/`BigInt` buffer requires no lookup (SPEC.md §3.1, §4.2); those three
    // buffers are singular precisely because interned values have no def to partition by.

    /// Store a value by content and return its PID.
    ///
    /// Idempotent by construction: the PID is a pure function of `(kind, bytes)`, so interning the
    /// same value twice — on one branch, on two branches, or on two machines that never spoke —
    /// yields the same PID and one stored copy.
    ///
    /// Interning is **not layered**. It takes effect immediately rather than inside an open layer,
    /// because an interned value nobody references is garbage rather than corruption: an aborted
    /// layer may strand one, and all that costs is space. Routing it through a layer would buy
    /// nothing and would reintroduce the branch scoping the whole scheme exists to avoid.
    async fn intern(&self, kind: PidKind, bytes: &[u8]) -> Result<Pid>;

    /// The bytes behind a content-addressed PID, or `None` if this store has never seen them.
    ///
    /// `None` is a legitimate answer, not a failure: PIDs travel — through layers, across branches,
    /// between stores — so holding one is no promise the content is local. An *allocated* PID is a
    /// different matter and errors, because there is no interned row it could ever name.
    async fn read_interned(&self, pid: &Pid) -> Result<Option<Vec<u8>>>;

    // --- Log structure ---
    //
    // Layers and branches are the shape of the log, not derived from it, so they have to be durable
    // in their own right. Everything else the engine holds in memory — the dependency index, the
    // touch index, watermarks — is a *cache* that can be rebuilt by replaying committed layers, so
    // none of it appears here.

    /// Record or update a layer's metadata.
    async fn put_layer_meta(&self, layer: &Layer) -> Result<()>;

    /// Every layer known to the store, in no particular order.
    async fn read_layers(&self) -> Result<Vec<Layer>>;

    async fn put_branch(&self, branch: &Branch) -> Result<()>;

    async fn read_branches(&self) -> Result<Vec<Branch>>;

    /// Make a sealed layer visible. *This edge is what triggers dependent producers.*
    async fn commit_layer(&self, layer: SealedLayer) -> Result<()>;
}

/// A stream of cells. Boxed rather than returning a concrete iterator so that a remote provider can
/// page without the engine noticing.
pub type CellStream =
    std::pin::Pin<Box<dyn futures_core::Stream<Item = Result<(CellRef, CellRecord)>> + Send>>;
