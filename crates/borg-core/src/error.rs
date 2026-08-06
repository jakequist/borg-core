//! Errors.
//!
//! Most interesting failures in Borg are only detectable at runtime — cycles, throwing migrations,
//! field-ownership violations. They attach to the *producer*, not the branch (SPEC.md §14).

use crate::cell::CellRef;
use crate::ids::{BranchId, LayerId, ProducerId};
use crate::value::ObjectTypeName;

pub type Result<T> = std::result::Result<T, BorgError>;

#[derive(Debug, thiserror::Error)]
pub enum BorgError {
    // --- Definition errors, caught at push time ---
    /// Two repos declared the same field on the same struct. SPEC.md §5.2.
    #[error("field `{struct_name}.{field}` is already declared by repo {existing:?}")]
    FieldCollision {
        struct_name: ObjectTypeName,
        field: String,
        existing: crate::ids::RepoId,
    },

    #[error("repo {repo:?} may not mutate `{struct_name}.{field}`, declared by repo {owner:?}")]
    NotDeclaringRepo {
        repo: crate::ids::RepoId,
        owner: crate::ids::RepoId,
        struct_name: ObjectTypeName,
        field: String,
    },

    #[error("def-mutation of `{struct_name}.{field}` requires a migration")]
    MissingMigration {
        struct_name: ObjectTypeName,
        field: String,
    },

    /// A `MutateField` naming migrations for a field a producer owns. SPEC.md §8, §9.3.
    ///
    /// Rejected at push rather than at run time, which is where it used to surface: ownership is
    /// checked against the declaration naming the field's writer *before* the migration exemption is
    /// reached, so the appointed migration would have been forbidden to write the very field it was
    /// appointed for — an `OwnershipViolation` from a producer nobody had done anything wrong with,
    /// arriving whenever the round that ran it happened to run.
    #[error(
        "`{struct_name}.{field}` is derived by {owner}, so no migration can be appointed for it: a \
         derived field's shape is its producer's business, and a producer that changes its output \
         re-derives rather than migrating"
    )]
    MigrationOnDerivedField {
        struct_name: ObjectTypeName,
        field: String,
        owner: ProducerId,
    },

    /// A write that the definitions in force do not permit. SPEC.md §5.1, §8.
    ///
    /// Boxed because these carry a lot of context on purpose — a rejected write should tell you
    /// what to do next — and every other variant would otherwise pay for it in size.
    #[error(transparent)]
    WriteRejected(#[from] Box<WriteRejection>),

    // --- Runtime producer failures. These poison the producer, never the branch. ---
    /// A producer transitively depends on a field it writes. Under a stateless scheduler this
    /// livelocks rather than re-entering, so it is caught by a re-run counter (SPEC.md §16.5).
    #[error("producer {producer:?} is cycling: invocation re-ran {runs} times at a fixed head")]
    ProducerCycle { producer: ProducerId, runs: u32 },

    #[error("producer {producer:?} failed: {message}")]
    ProducerFailed {
        producer: ProducerId,
        message: String,
    },

    // --- Transactions and merge ---
    /// SPEC.md §12. Guards may reference source cells only.
    #[error("guard on {cell:?} failed: mutated at {mutated_at} (guard held since {since})")]
    GuardViolated {
        cell: CellRef,
        since: LayerId,
        mutated_at: LayerId,
    },

    #[error("guard on {cell:?} is invalid: guards may reference source cells only")]
    GuardOnDerivedCell { cell: CellRef },

    /// SPEC.md §13. v1 rejects whole merges rather than applying partially.
    #[error("merge rejected: {0}")]
    MergeRejected(MergeRejection),

    // --- Log and branch ---
    #[error("layer {layer} is {actual:?}, expected {expected:?}")]
    LayerStateViolation {
        layer: LayerId,
        expected: crate::layer::LayerState,
        actual: crate::layer::LayerState,
    },

    #[error("layer {layer} is not on branch {branch:?} or any of its ancestors")]
    LayerNotOnBranch { layer: LayerId, branch: BranchId },

    #[error("a layer may contain value events xor def events, never both")]
    MixedLayerKind,

    // --- Values ---
    /// SPEC.md §3.1: only `String`, `Binary` and `BigInt` are content-addressed. Asking to intern
    /// anything else, or to read interned bytes behind an allocated PID, is a caller bug rather
    /// than a miss.
    #[error("{kind:?} is not content-addressed; only String, Binary and BigInt are interned")]
    NotContentAddressed { kind: crate::pid::PidKind },

    #[error("storage: {0}")]
    Storage(String),

    /// **Nothing is listening where a client was told to connect.** SPEC.md §17.7.
    ///
    /// Its own variant, and printed with no prefix, because it is the one error whose whole value
    /// is the sentence: *no borg server at <addr> — start one with: `borg-server start`*. A
    /// `storage:` in front of that is noise in front of the only words a reader needs, and the
    /// variant is also what lets a caller tell "the server is down" from "the server said no" —
    /// which is the distinction `examples/personal-crm/FRICTION.md` #11 is about. Built by
    /// `borg_protocol::url::unreachable`, which is where the sentence lives.
    #[error("{0}")]
    Unreachable(String),

    #[error(transparent)]
    Parse(#[from] crate::parse::ParseError),

    #[error("execution: {0}")]
    Execution(String),
}

/// Why a write was refused. SPEC.md §5.1, §8.
///
/// Every cell write is checked against the definitions in force on its branch, and these are the
/// four ways it can fail. They are worded to be read by whoever typed the write: each one names the
/// cell, what the schema says, and — where there is one — the fix.
#[derive(Debug, thiserror::Error)]
pub enum WriteRejection {
    /// Nothing has declared a field on this struct, so the struct does not exist (SPEC.md §5.2).
    #[error("`{cell}`: no struct named `{struct_name}` is declared on this branch")]
    UndeclaredStruct {
        cell: CellRef,
        struct_name: ObjectTypeName,
    },

    #[error(
        "`{cell}`: `{struct_name}` has no field `{field}` declared on this branch — it has: {known}"
    )]
    UndeclaredField {
        cell: CellRef,
        struct_name: ObjectTypeName,
        field: String,
        /// The fields that *are* declared. A rejection naming the alternatives turns a typo into a
        /// one-line fix rather than a second command.
        known: String,
    },

    #[error("`{cell}` is declared {expected}, and `{actual}` is not a {expected}")]
    TypeMismatch {
        cell: CellRef,
        expected: crate::value::ValueType,
        actual: String,
    },

    /// Every field has exactly one writer, and the declaration says which (SPEC.md §8).
    #[error("{attempted} may not write `{cell}`: it is declared {ownership}")]
    OwnershipViolation {
        cell: CellRef,
        ownership: crate::def::Ownership,
        attempted: crate::cell::Writer,
    },
}

impl From<WriteRejection> for BorgError {
    fn from(rejection: WriteRejection) -> Self {
        Self::WriteRejected(Box::new(rejection))
    }
}

/// Why a merge was rejected. SPEC.md §13.
#[derive(Debug, thiserror::Error)]
pub enum MergeRejection {
    #[error(
        "the parent moved def `{struct_name}` since the fork point; re-fork from head and redo"
    )]
    DefDiverged { struct_name: ObjectTypeName },

    #[error("the child wrote {cell:?}, which the parent deleted at {deleted_at}")]
    DanglingWrite { cell: CellRef, deleted_at: LayerId },

    /// Re-evaluating the child's guards against the parent's history since the fork point *is* the
    /// definition of a merge conflict — so guards double as the conflict detector.
    #[error("guard on {cell:?} no longer holds against the parent")]
    GuardConflict { cell: CellRef },
}
