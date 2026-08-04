//! Errors.
//!
//! Most interesting failures in Borg are only detectable at runtime — cycles, throwing migrations,
//! field-ownership violations. They attach to the *producer*, not the branch (SPEC.md §14).

use crate::cell::{CellAt, CellRef};
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

    // --- Runtime producer failures. These poison the producer, never the branch. ---
    /// SPEC.md §8: every field has exactly one writer.
    #[error("producer {attempted:?} wrote {cell:?}, which is owned by producer {owner:?}")]
    FieldOwnershipViolation {
        cell: CellAt,
        owner: Option<ProducerId>,
        attempted: ProducerId,
    },

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

    #[error("storage: {0}")]
    Storage(String),

    #[error(transparent)]
    Parse(#[from] crate::parse::ParseError),

    #[error("execution: {0}")]
    Execution(String),
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
