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
/// A `LayerId` also serves as a def-version and as a `ClientVersion`: a def-version *is* the
/// def-layer that last mutated that definition, so no separate versioning scheme exists
/// (SPEC.md §5.3, §5.4).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LayerId(pub u64);

/// The def-view an actor's code was authored against. SPEC.md §5.4.
///
/// A distinct type from [`LayerId`] despite the identical representation, because conflating "which
/// layer am I reading at" with "which schema am I reading through" is the single easiest mistake to
/// make in this system. They vary independently.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ClientVersion(pub LayerId);

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
