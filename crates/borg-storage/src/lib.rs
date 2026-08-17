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
    Branch, BranchId, BufferId, CellRef, DefEvent, DefVersion, Event, EventDraft, EventId, Landed,
    Layer, LayerId, Pid, PidKind, ReadPath, Result,
};

/// A handle to an open, invisible layer that is accepting writes. SPEC.md §6.2.
///
/// **Streaming commit is the binding constraint.** One producer run writes all of its output into
/// exactly one layer, so a flip of a widely-depended-on field may produce a single layer containing
/// 100k mutations. A layer can never be assembled in memory and flushed; any provider that cannot
/// accept an unbounded write stream into an uncommitted layer is disqualified.
///
/// A layer acquires members two ways, and the difference is the whole of §13's cost argument:
/// [`author_event`](OpenLayer::author_event) creates one, and
/// [`include_event`](OpenLayer::include_event) names one that already exists.
#[async_trait]
pub trait OpenLayer: Send {
    fn id(&self) -> LayerId;

    /// Create an event in this layer, and return the identity the log gave it. Called an unbounded
    /// number of times.
    ///
    /// The draft names no layer: `authored` is this layer, by definition, so a writer cannot claim
    /// otherwise.
    async fn author_event(&mut self, cell: &CellRef, draft: EventDraft) -> Result<EventId>;

    /// Author an event that **already has an identity**. The import half of §19, and its only caller.
    ///
    /// [`author_event`](OpenLayer::author_event) takes a draft precisely because a writer must not be
    /// able to name an id or a layer. An import is the one writer for which that is backwards: an
    /// event id is *referenced* — by every membership row in the stream and by every read-set inside
    /// it — so re-minting one would rewrite the lineage the export exists to preserve, and `authored`
    /// is what tells a merged event apart from a copied one (§13). The stream carries both because
    /// they are data.
    ///
    /// **`event.authored` must be this layer**, and a provider is required to refuse otherwise. That
    /// is what keeps `author_event`'s property intact rather than merely mostly intact: an event
    /// still cannot claim to have been written somewhere it was not — it can only be replayed into
    /// the layer it says it came from.
    ///
    /// A provider must also advance whatever mints ids past this one, or the next ordinary write
    /// would reissue an id the import has just adopted.
    async fn adopt_event(&mut self, event: Event) -> Result<()>;

    /// Name an existing event as a member of this layer. SPEC.md §13.
    ///
    /// **This is what makes a merge not copy.** The event keeps its identity and its `authored`
    /// layer; this layer merely records that it landed here too. The membership row is
    /// `(layer, event)` rather than a whole record carrying value, version, origin, derivation and
    /// read-set — a large constant-factor saving, not an asymptotic one: there are still `n` rows,
    /// and the provider's read index has to gain a row for each.
    async fn include_event(&mut self, event: EventId) -> Result<()>;

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
    /// The version asked for is a [`DefVersion`] — this *field's* version, not the caller's
    /// whole-schema view (§5.3). Translating one into the other needs the definitions, which is
    /// exactly the knowledge a provider does not have and must not need.
    ///
    /// Returns only what is physically stored; the engine layers migration, validation and
    /// provenance on top.
    /// Reads resolve through a [`ReadPath`] rather than a bare branch, so that a fork inherits its
    /// parent by ancestry instead of by copying (SPEC.md §7.2). Storage never has to know what a
    /// branch *is* — only how to walk the segments it is handed.
    ///
    /// Answers *"the latest write to this cell visible on this branch at layer ≤ N"*, which is a
    /// question about **membership**: the layers of the branch's chain up to N, their events, those
    /// touching this cell, the latest. A provider is expected to keep that a single indexed lookup
    /// by materializing `(branch, cell, version) -> (layer, event)` as events are put into a layer.
    /// Like every other index in the system that one is a projection of the log, and
    /// [`rebuild_read_index`](StorageProvider::rebuild_read_index) is the proof.
    async fn get_cell(
        &self,
        path: &ReadPath,
        cell: &CellRef,
        version: DefVersion,
    ) -> Result<Option<Landed>>;

    /// Which def-versions this cell is materialized at, as of a layer. Used by the resolver to find
    /// a migration path when the requested version is not yet materialized.
    async fn cell_versions(&self, path: &ReadPath, cell: &CellRef) -> Result<Vec<DefVersion>>;

    /// Enumerate a buffer. The scheduler's way of discovering entities a layer changeset cannot
    /// mention (SPEC.md §9.6) — and, since `list`, the one read behind a client asking which objects
    /// of a struct there are (§17.5). It was engine-internal until an application needed the second,
    /// which changed who calls it and nothing about what it answers.
    ///
    /// It answers **records**: one row per `(cell, def-version)` visible on the path, a child's
    /// shadowing its ancestors'. Tombstones come back like any other value, because whether a
    /// deletion is interesting is the caller's question — the scheduler wants the entity, `list`
    /// does not.
    async fn scan_buffer(&self, path: &ReadPath, buffer: &BufferId) -> Result<EventStream>;

    /// A committed layer's membership, in order. This is what drives invalidation: the committed
    /// layer *is* the changeset, so buffers need no observation machinery (SPEC.md §9.6).
    ///
    /// Membership, not authorship: a merge layer yields the events it named, which still report the
    /// layer that authored them. Two writes to one cell in one layer are two events and both appear
    /// — a layer is an *ordered group of events* (§6.2), and it is the read index, not the group,
    /// that decides which of them resolves.
    async fn read_layer(&self, layer: LayerId) -> Result<EventStream>;

    /// The same membership, as identities only.
    ///
    /// A merge names a child layer's events on the parent (§13) and needs nothing about them but
    /// their ids — and a round's output is `n` events each carrying the read-set it was computed
    /// from, so reading them as whole events to throw everything but the id away is the difference
    /// between a merge that costs a pointer per event and one that costs a deep copy per event.
    /// Measured on the fan-out benchmark, where it was most of the merge.
    async fn read_membership(&self, layer: LayerId) -> Result<Vec<EventId>>;

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

    /// Discard the read index and rebuild it from layer membership.
    ///
    /// Nothing on the read or write path calls this. It exists so that *"the index is a projection
    /// of the log, not a second source of truth"* is a property a test can check rather than a
    /// claim a comment makes — the same standing the dependency and touch indexes have, which
    /// `Registry::open` rebuilds by replaying committed layers. It is deliberately not called at
    /// open: unlike those two, this one is durable, and rebuilding it per process would turn an
    /// `O(log)` read into an `O(log)` write.
    async fn rebuild_read_index(&self) -> Result<()>;
}

/// A stream of events. Boxed rather than returning a concrete iterator so that a remote provider can
/// page without the engine noticing.
pub type EventStream = std::pin::Pin<Box<dyn futures_core::Stream<Item = Result<Event>> + Send>>;
