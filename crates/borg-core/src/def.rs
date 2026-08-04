//! Definitions. SPEC.md §5.
//!
//! The namespace is flat and there is no explicit `extends`. If one repo declares `Company.name` and
//! another declares `Company.website`, they simply merge: a struct's definition is the union of all
//! field declarations across all repos. Two repos declaring the *same* field is a hard error.
//!
//! The consequence worth internalizing: a struct has no owner. Only its fields do. `Company` exists
//! because somebody declared a field on it.

use crate::cell::{BufferId, FieldName};
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

/// Who writes a field. SPEC.md §5.1, §8.
///
/// **One enum, not an `origin` beside an optional `writer`.** Every field has exactly one writer, so
/// "derived but unowned" and "source but owned by P1" are not states the system has an answer for —
/// and a pair of loose fields is exactly how they become spellable.
///
/// Ownership is **declared, not discovered**. The author of a repo knows which producer computes
/// which field, so a violation is caught on the *first* wrong write rather than on the second
/// producer's collision with the first. Runtime enforcement then checks a write against the
/// declaration instead of inventing one.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Ownership {
    /// Ground truth, pushed by external clients.
    Source,
    /// Computed by exactly one producer. No client may write it.
    Derived(ProducerId),
}

impl Ownership {
    /// The producer that owns this field, if one does.
    ///
    /// There is deliberately no `Origin` conversion here. A *record's* `Origin` follows the actor
    /// that wrote it ([`Writer::origin`](crate::cell::Writer::origin)), not the declaration, because
    /// an `up` migration is a producer writing a field declared `Source` and the record it leaves is
    /// still derived (SPEC.md §9.3). Deriving one from the other would be wrong in exactly that case.
    pub const fn producer(&self) -> Option<ProducerId> {
        match self {
            Self::Source => None,
            Self::Derived(producer) => Some(*producer),
        }
    }
}

impl std::fmt::Display for Ownership {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Source => f.write_str("source data, written by clients"),
            Self::Derived(producer) => write!(f, "derived by producer {producer}"),
        }
    }
}

/// A single field.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FieldDef {
    pub name: FieldName,
    pub ty: ValueType,
    /// The repo that declared this field. Only that repo may mutate or delete it.
    pub declaring_repo: RepoId,
    /// Who may write this field, and therefore whether its cells are source or derived (SPEC.md §8).
    pub ownership: Ownership,
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
    ///
    /// **Stamped by the fold, not by the author.** A producer's ClientVersion *is* the def-layer it
    /// was pushed at (SPEC.md §9.2), and that layer id does not exist until the layer opens — nor is
    /// it the same id after a merge replays the event onto another branch. Whatever an event carries
    /// here is therefore overwritten by [`DefView`](crate::DefEvent) as it folds, and only migrations
    /// ignore it entirely (see [`ProducerKind::Migration`]).
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
    ///
    /// **Which two versions it bridges is deliberately not recorded here.** It is a fact about the
    /// field's version chain (SPEC.md §5.3), which is folded per branch from the `MutateField` that
    /// named this producer as `up` or `down`. Baking the pair into the definition would freeze the
    /// layer ids of the branch it was pushed on, and a def-only merge replays that event onto the
    /// parent as a *different* layer — leaving a migration writing at a version no reader on that
    /// branch will ever ask for. Direction is the one half the author declares; the log supplies the
    /// rest.
    Migration { direction: MigrationDirection },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum MigrationDirection {
    Up,
    /// Kept working so that old clients keep reading. v1 trusts these (SPEC.md §9.3).
    Down,
}

/// A mutation of the data model. SPEC.md §6.1.
///
/// Note there is deliberately no `CreateObjectDef`: a struct has no owner and exists because someone
/// declared a field on it (SPEC.md §5.2). Creation falls out of declaration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DefEvent {
    /// Add a field. The declaring repo becomes its owner — the only repo permitted to mutate or
    /// delete it. Two repos declaring the same field is a hard error.
    ///
    /// `ownership` is what makes a derived field declarable at all: before it existed every field
    /// was implicitly source, so no producer could legally write anything (SPEC.md §8).
    DeclareField {
        struct_name: ObjectTypeName,
        field: FieldName,
        ty: ValueType,
        repo: RepoId,
        ownership: Ownership,
    },
    /// Change a field's shape. **Must** supply migrations: `up` carries existing values forward, and
    /// `down` — optional — keeps older clients reading (SPEC.md §9.3).
    MutateField {
        struct_name: ObjectTypeName,
        field: FieldName,
        ty: ValueType,
        repo: RepoId,
        up: ProducerId,
        down: Option<ProducerId>,
    },
    DeleteField {
        struct_name: ObjectTypeName,
        field: FieldName,
        repo: RepoId,
    },
    /// Register a producer *definition*. The implementation is resolved separately by the
    /// `ExecutionProvider` (SPEC.md §9.2).
    PushProducer(ProducerDef),
}

impl DefEvent {
    /// Which definition this event touches, if any. Used to detect two branches moving the same def
    /// (SPEC.md §13).
    pub fn touches(&self) -> Option<(ObjectTypeName, FieldName)> {
        match self {
            DefEvent::DeclareField {
                struct_name, field, ..
            }
            | DefEvent::MutateField {
                struct_name, field, ..
            }
            | DefEvent::DeleteField {
                struct_name, field, ..
            } => Some((struct_name.clone(), field.clone())),
            DefEvent::PushProducer(_) => None,
        }
    }

    pub const fn repo(&self) -> Option<RepoId> {
        match self {
            DefEvent::DeclareField { repo, .. }
            | DefEvent::MutateField { repo, .. }
            | DefEvent::DeleteField { repo, .. } => Some(*repo),
            DefEvent::PushProducer(def) => Some(def.declaring_repo),
        }
    }
}
