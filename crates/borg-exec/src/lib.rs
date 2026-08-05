//! # borg-exec
//!
//! The `ExecutionProvider` seam. SPEC.md §17.3.
//!
//! The contract is *"run this code, **mediating every cell access through me**"* — not merely "run
//! this code." That mediation is the whole design: it is what makes dependency capture automatic and
//! exact (SPEC.md §9.4), with nothing for a producer author to declare or mis-declare, and it is
//! equally satisfiable by an in-process call and by a socket round-trip to a container.

use async_trait::async_trait;
use borg_core::{CellRef, ClientVersion, DefVersion, Pid, ProducerId, Result, Value, ValueInput};

/// Which producer to run, and the def-view its code was authored against.
///
/// Names a *definition*. Resolving it to an implementation is this provider's entire job — a static
/// registry of Rust functions in v1, a container image reference later (SPEC.md §9.2).
#[derive(Clone, Copy, Debug)]
pub struct ProducerRef {
    pub id: ProducerId,
    pub version: ClientVersion,
}

/// Text ↔ value, for a runtime that holds no store handle.
///
/// **Deliberately not part of [`ProducerCtx`]'s own method set**, though every `ProducerCtx` is one.
/// Everything on `ProducerCtx` is *cell* access: scoped to a branch, a layer and a def-version, and
/// recorded into the read-set as it happens (SPEC.md §9.4). Nothing here is any of those things.
/// Interning is unscoped — no branch, no layer, no version (§17.1) — so there is nothing that could
/// later change and invalidate anything, and rendering is a pure function of content already
/// resolved. Two kinds of thing sat side by side in one list, and the only warning that they were
/// different was a sentence in a doc comment.
///
/// Mediated all the same, and for the reason cell access is: a producer runtime holds no store
/// handle, and content-addressed values have no identity until a store has seen them (§3.1). A
/// runtime that reached around this to intern for itself would be a second writer into the store,
/// which is precisely what the `ExecutionProvider` contract exists to prevent.
///
/// Separating it means a caller that only needs the codec can say so: `borg-exec-process` renders
/// cell values for the wire and takes this rather than the whole context, so it cannot reach a
/// dependency-recording method by accident.
#[async_trait]
pub trait ValueCodec: Send {
    /// Turn a parsed value into a storable one, interning `String`, `Binary` and `BigInt` content.
    ///
    /// **Not recorded as a dependency** — see the trait header.
    async fn intern(&mut self, input: ValueInput) -> Result<Value>;

    /// Render a value as the text a worker reads, resolving interned content back to its bytes.
    ///
    /// This is what makes interning invisible to producers (§3.4): a pipeline asking for
    /// `company.website` is handed `acme.ai`, never `@s-1a2b3c` plus a second round trip to resolve
    /// it. Nothing above this line needs to know that strings are content-addressed at all.
    async fn render(&mut self, value: &Value) -> Result<String>;
}

/// The mediated view of the world given to running producer code.
///
/// Every method here is **cell access**, and every one of them is recorded — which is what makes
/// dependency capture automatic and exact, with nothing for a producer author to declare or
/// mis-declare (SPEC.md §9.4). The value codec a producer also needs is [`ValueCodec`], a supertrait
/// rather than more methods, because it answers a different kind of question; see there.
///
/// **Async from day one**, even though the v1 in-process implementation only ever returns ready
/// futures. A socket-backed provider performs a round-trip per cell read, and retrofitting async
/// through the derivation engine afterwards is a far larger change than paying for it now.
#[async_trait]
pub trait ProducerCtx: ValueCodec + Send {
    /// Read a cell through this producer's own def-view — the schema its code was authored against
    /// (SPEC.md §5.4). The record it resolves to is the one at the *field's* def-version as that
    /// view names it (§5.3). Recorded into the read-set, including reads that find nothing, since
    /// absence is a legitimate dependency (SPEC.md §9.4).
    async fn get(&mut self, cell: &CellRef) -> Result<Option<Value>>;

    /// Read a cell at an explicitly chosen def-version.
    ///
    /// Exists for exactly one case: a migration reading its own source cell. `up_v1→v2` runs at
    /// ClientVersion v2, so an ordinary `get` of the cell it is migrating would resolve at v2 and
    /// recurse into itself. Every *other* read a migration makes is an ordinary `get` and correctly
    /// sees the target view (SPEC.md §9.3).
    ///
    /// A [`DefVersion`] and not a ClientVersion: this names a position on *one field's* chain, and
    /// the only chains that exist are per-field (SPEC.md §5.3).
    async fn get_at(&mut self, cell: &CellRef, version: DefVersion) -> Result<Option<Value>>;

    /// Read a cell at the version this producer takes its **input** at.
    ///
    /// [`get_at`](Self::get_at) with the one version a migration author should never have to name.
    /// For `up` that is the older version and for `down` the newer, but a migration script says
    /// `get_input` either way — the direction is the log's business, and a worker that had to do
    /// arithmetic on layer ids to read its own input would be a worker nobody writes in bash.
    ///
    /// For a pipeline this is an ordinary [`get`](Self::get): its input version *is* its
    /// ClientVersion.
    async fn get_input(&mut self, cell: &CellRef) -> Result<Option<Value>>;

    /// Write a cell. Validated against the branch's definitions: the field must be declared, the
    /// value must fit its declared type, and the field must be one this producer *owns* — ownership
    /// is declared, not discovered (SPEC.md §5.1, §8). A violation poisons this producer rather than
    /// the branch (SPEC.md §14).
    async fn set(&mut self, cell: &CellRef, value: Value) -> Result<()>;

    /// Write a cell from its text form, parsed against the field's **declared type** (SPEC.md §3.4).
    ///
    /// Exists because a worker speaking the wire protocol sends text (§17.4) and only the engine
    /// knows the declared type. Parsing worker-side would be parsing without one — which is exactly
    /// the guessing that type-directed parsing removes, and would make `true` unstorable in a
    /// `String` field over the wire while the CLI stored it fine.
    async fn set_text(&mut self, cell: &CellRef, text: &str) -> Result<()>;
}

#[async_trait]
pub trait ExecutionProvider: Send + Sync {
    /// Run one invocation. v1 producers are per-entity maps, so `input` is the entity being mapped
    /// (SPEC.md §9.2).
    ///
    /// The implementation owns the inversion of control: in-process, it calls a registered function
    /// directly; over a socket, it sends a message and services `ctx` callbacks as they arrive. The
    /// engine never learns which it got.
    async fn run(
        &self,
        producer: &ProducerRef,
        input: Pid,
        ctx: &mut dyn ProducerCtx,
    ) -> Result<()>;
}
