//! # borg-exec
//!
//! The `ExecutionProvider` seam. SPEC.md §17.3.
//!
//! The contract is *"run this code, **mediating every cell access through me**"* — not merely "run
//! this code." That mediation is the whole design: it is what makes dependency capture automatic and
//! exact (SPEC.md §9.4), with nothing for a producer author to declare or mis-declare, and it is
//! equally satisfiable by an in-process call and by a socket round-trip to a container.

use async_trait::async_trait;
use borg_core::{CellRef, ClientVersion, Pid, ProducerId, Result, Value, ValueInput};

/// Which producer to run, and the def-view its code was authored against.
///
/// Names a *definition*. Resolving it to an implementation is this provider's entire job — a static
/// registry of Rust functions in v1, a container image reference later (SPEC.md §9.2).
#[derive(Clone, Copy, Debug)]
pub struct ProducerRef {
    pub id: ProducerId,
    pub version: ClientVersion,
}

/// The mediated view of the world given to running producer code.
///
/// **Async from day one**, even though the v1 in-process implementation only ever returns ready
/// futures. A socket-backed provider performs a round-trip per cell read, and retrofitting async
/// through the derivation engine afterwards is a far larger change than paying for it now.
#[async_trait]
pub trait ProducerCtx: Send {
    /// Read a cell at this producer's own ClientVersion — the def-view its code was authored
    /// against (SPEC.md §5.4). Recorded into the read-set, including reads that find nothing, since
    /// absence is a legitimate dependency (SPEC.md §9.4).
    async fn get(&mut self, cell: &CellRef) -> Result<Option<Value>>;

    /// Read a cell at an explicitly chosen def-version.
    ///
    /// Exists for exactly one case: a migration reading its own source cell. `up_v1→v2` runs at
    /// ClientVersion v2, so an ordinary `get` of the cell it is migrating would resolve at v2 and
    /// recurse into itself. Every *other* read a migration makes is an ordinary `get` and correctly
    /// sees the target view (SPEC.md §9.3).
    async fn get_at(&mut self, cell: &CellRef, version: ClientVersion) -> Result<Option<Value>>;

    /// Write a cell. Checked against field ownership: every field has exactly one writer, and a
    /// violation poisons this producer rather than the branch (SPEC.md §8, §14).
    async fn set(&mut self, cell: &CellRef, value: Value) -> Result<()>;

    /// Turn a parsed value into a storable one, interning `String`, `Binary` and `BigInt` content.
    ///
    /// Mediated for the same reason cell access is: a producer runtime holds no store handle, and
    /// content-addressed values have no identity until a store has seen them (SPEC.md §3.1). A
    /// runtime that reached around this to intern for itself would be a second writer into the
    /// store, which is precisely what the `ExecutionProvider` contract exists to prevent.
    ///
    /// **Not recorded as a dependency.** Interning is unscoped — no branch, no layer, no version
    /// (§17.1) — so there is nothing here that could later change and invalidate anything.
    async fn intern(&mut self, input: ValueInput) -> Result<Value>;

    /// Render a value as the text a worker reads, resolving interned content back to its bytes.
    ///
    /// This is what makes interning invisible to producers (§3.4): a pipeline asking for
    /// `company.website` is handed `acme.ai`, never `@s-1a2b3c` plus a second round trip to resolve
    /// it. Nothing above this line needs to know that strings are content-addressed at all.
    async fn render(&mut self, value: &Value) -> Result<String>;
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
