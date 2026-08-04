//! Identifier types.
//!
//! Every id in Borg is a plain value type: `Copy`, ordered, and serializable. Nothing here
//! coordinates, allocates, or touches storage — see SPEC.md §17.2 for why that matters.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Identifies a branch. SPEC.md §7.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BranchId(pub u64);

/// Identifies a layer. Registry-unique, not per-branch. SPEC.md §6.2.
///
/// A `LayerId` also underlies both version types below: a def-version *is* the def-layer that last
/// mutated that definition, and a ClientVersion *is* the def-layer a view was folded to, so no
/// separate versioning scheme exists (SPEC.md §5.3, §5.4).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LayerId(pub u64);

/// Identifies an event — one mutation, with an identity of its own. SPEC.md §4.3, §6.1.
///
/// Registry-unique rather than layer-scoped or branch-scoped, and for the same reason `LayerId` is:
/// a merge makes one event a member of layers on two branches (§13), so an id that meant something
/// only within one of them would stop naming the same event the moment it was shared.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EventId(pub u64);

/// The def-view an actor's code was authored against — a **whole-schema** position. SPEC.md §5.4.
///
/// A distinct type from [`LayerId`] despite the identical representation, because conflating "which
/// layer am I reading at" with "which schema am I reading through" is the single easiest mistake to
/// make in this system. They vary independently.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ClientVersion(pub LayerId);

/// The version of **one definition**: the def-layer that last mutated it. SPEC.md §5.3.
///
/// ## Why this is not a `ClientVersion`
///
/// The two are both def-layer ids and they are not the same thing. A ClientVersion is a *whole
/// schema* — every definition as one actor sees them — and it advances on every def push. A
/// def-version belongs to one field, and advances only when that field is mutated. They coincide
/// exactly when every push touches every field, which is to say almost never.
///
/// A stored record is keyed by the **def-version** of its field, and so are read-sets, the
/// dependency index and the migration chains of §5.3. Keying them by the writer's ClientVersion
/// instead made an unrelated declaration move every subsequent write to a version no reader was
/// looking for and no migration led to — data reported `broken`, and, worse, dependencies that
/// silently stopped matching, so invalidation stopped with nothing to show for it.
///
/// **The only way from a ClientVersion to a `DefVersion` is to ask a def-view**
/// (`DefView::version_of`), because the answer is a fact about the schema and not arithmetic on
/// ids. That is the whole reason this is a type rather than a convention.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DefVersion(pub LayerId);

impl DefVersion {
    /// The version of a cell that has no definition to version.
    ///
    /// Existence cells, lists and the untyped containers have no `FieldDef` — a struct exists
    /// because someone declared a field *on* it (§5.2), and there is no `ListDef` event to declare
    /// a list with (§8). Nothing about them can change shape, so nothing can migrate them and there
    /// is no chain for them to sit on. One fixed version keeps them findable across every def push,
    /// which is what "unversioned" has to mean in a store whose record key includes a version.
    pub const UNVERSIONED: Self = Self(LayerId(0));
}

/// Identifies a PID-allocating authority. SPEC.md §3.1, §17.2.
///
/// Exists so that any node may allocate PIDs without coordinating. One per process in v1.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AllocatorId(pub u32);

/// Identifies a producer — a pipeline or a migration. SPEC.md §9.1.
///
/// Names a *definition*, not an implementation. The log records this id; the `ExecutionProvider`
/// resolves it to code (SPEC.md §9.2).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ProducerId(pub u64);

/// Identifies a repo — a contribution unit, not an isolation boundary. SPEC.md §5.2.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RepoId(pub u32);

macro_rules! debug_as {
    ($t:ty, $prefix:literal) => {
        impl fmt::Debug for $t {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!($prefix, "{}"), self.0)
            }
        }
        impl fmt::Display for $t {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Debug::fmt(self, f)
            }
        }
    };
}

debug_as!(BranchId, "b");
debug_as!(LayerId, "L");
debug_as!(EventId, "e");
debug_as!(AllocatorId, "a");
debug_as!(ProducerId, "P");
debug_as!(RepoId, "r");

impl fmt::Debug for ClientVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cv@{}", self.0)
    }
}

impl fmt::Display for ClientVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl fmt::Debug for DefVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v@{}", self.0)
    }
}

impl fmt::Display for DefVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
