//! Point IDs — the universal identifier for every non-primitive value. SPEC.md §3.1.
//!
//! Two flavors, split by mutability:
//!
//! * **Allocated** — `(branch, allocator, counter)`. Survives mutation, so it is identity.
//!   Objects, lists, and the `Any*` family.
//! * **Content-addressed** — `hash(bytes)`. Immutable, branch-independent, eternal. Strings,
//!   binary, bigints.
//!
//! The consequence that matters most: two nodes independently interning `"hello"` produce the same
//! PID with no coordination, so string writes can never conflict across branches.

use crate::ids::{AllocatorId, BranchId};
use serde::{Deserialize, Serialize};
use std::fmt;

/// The kind of value a PID points at. Encoded in the PID itself so that dispatching to the correct
/// buffer requires no lookup.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum PidKind {
    // Allocated identity — mutable.
    Object = 0,
    List = 1,
    Any = 2,
    AnyObject = 3,
    AnyArray = 4,
    AnyNumber = 5,

    // Content-addressed — immutable.
    String = 64,
    Binary = 65,
    BigInt = 66,
    // Set/Map are deferred (SPEC.md §3.3).
}

impl PidKind {
    /// Content-addressed kinds are immutable and their PIDs are eternal.
    pub const fn is_content_addressed(self) -> bool {
        (self as u8) >= 64
    }

    /// Allocated kinds are mutable in place; the PID survives mutation.
    pub const fn is_mutable(self) -> bool {
        !self.is_content_addressed()
    }
}

/// A Point ID.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Pid {
    /// Identity, allocated without coordination. SPEC.md §3.1, §17.2.
    Allocated {
        kind: PidKind,
        branch: BranchId,
        allocator: AllocatorId,
        counter: u64,
    },
    /// Content address. Equal content always yields an equal PID, on every branch, forever.
    Content { kind: PidKind, hash: [u8; 32] },
}

impl Pid {
    pub const fn kind(&self) -> PidKind {
        match self {
            Pid::Allocated { kind, .. } | Pid::Content { kind, .. } => *kind,
        }
    }

    pub const fn is_mutable(&self) -> bool {
        self.kind().is_mutable()
    }
}

impl fmt::Debug for Pid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Pid::Allocated {
                kind,
                branch,
                allocator,
                counter,
            } => write!(f, "{kind:?}#{branch}.{allocator}.{counter}"),
            Pid::Content { kind, hash } => {
                write!(f, "{kind:?}#")?;
                for byte in &hash[..6] {
                    write!(f, "{byte:02x}")?;
                }
                Ok(())
            }
        }
    }
}

/// Allocates identity PIDs. A distribution seam (SPEC.md §17.2): v1 uses one allocator per process,
/// and adding more requires no coordination because `AllocatorId` disambiguates.
pub trait PidAllocator: Send + Sync {
    fn allocate(&self, kind: PidKind, branch: BranchId) -> Pid;
}
