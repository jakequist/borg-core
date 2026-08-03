//! Definitions. SPEC.md §5.
//!
//! The namespace is flat and there is no explicit `extends`. If one repo declares `Company.name` and
//! another declares `Company.website`, they simply merge: a struct's definition is the union of all
//! field declarations across all repos. Two repos declaring the *same* field is a hard error.
//!
//! The consequence worth internalizing: a struct has no owner. Only its fields do. `Company` exists
//! because somebody declared a field on it.

use crate::cell::{BufferId, FieldName, Origin};
use crate::ids::{LayerId, ProducerId, RepoId};
use crate::value::{ObjectTypeName, ValueType};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A struct definition — the union of every repo's field declarations.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObjectDef {
    pub name: ObjectTypeName,
    pub fields: BTreeMap<FieldName, FieldDef>,
}

/// A single field.
///
/// **There is deliberately no `writer` here.** Every field has exactly one writer, but that
/// ownership is *discovered* at runtime, and defs are mutated only by DefEvents in def-layers.
/// Recording a discovered owner into the def would mean the derivation engine emitting
/// def-mutations — violating the value-xor-def rule and letting a producer's first run silently
/// rewrite the schema. Ownership is discovered state and lives in the dependency index with the rest
/// of it (SPEC.md §8).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FieldDef {
    pub name: FieldName,
    pub ty: ValueType,
    /// The repo that declared this field. Only that repo may mutate or delete it.
    pub declaring_repo: RepoId,
    pub origin: Origin,
    /// The def-layer that last mutated this field — i.e. its def-version. There is no separate
    /// versioning scheme (SPEC.md §5.3).
    pub version: LayerId,
}

/// A producer *definition*. The log records this; the `ExecutionProvider` resolves the id to an
/// implementation (SPEC.md §9.2). In v1 that resolution is a static registry of Rust functions;
/// later it is a container image reached over a socket.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProducerDef {
    pub id: ProducerId,
    pub kind: ProducerKind,
    /// The buffer this producer maps over — i.e. the set of instances of one struct (SPEC.md
    /// §4.2). v1 producers are per-entity maps: one invocation per entity (SPEC.md §9.2).
    pub source: BufferId,
    /// The def-view this producer's code was authored against. All of its reads resolve here.
    pub version: LayerId,
    pub declaring_repo: RepoId,
}

/// Pipelines and migrations are the same mechanism with different triggers (SPEC.md §9.1).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum ProducerKind {
    /// Triggered by a source data write. Bounded fan-out.
    Pipeline,
    /// Triggered by a def-mutation. Produces the same cell at a different def-version, as a set of
    /// per-output-field functions rather than a whole-object transform (SPEC.md §9.3).
    Migration {
        from: LayerId,
        to: LayerId,
        direction: MigrationDirection,
    },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum MigrationDirection {
    Up,
    /// Kept working so that old clients keep reading. v1 trusts these (SPEC.md §9.3).
    Down,
}
