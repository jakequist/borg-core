//! Layers and branches. SPEC.md §6, §7.

use crate::cell::CellRef;
use crate::ids::{BranchId, LayerId, ProducerId};
use serde::{Deserialize, Serialize};

/// A layer's lifecycle. SPEC.md §6.2.
///
/// The same state machine governs client transactions and producer runs alike — layers are the
/// universal unit of atomicity, and there is one code path.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum LayerState {
    /// Exclusive to its owner; writes accumulate; invisible to every reader.
    Open,
    /// Writes closed; durability and validation happen here. Source layers validate their guards at
    /// seal (SPEC.md §12).
    Sealed,
    /// Visible to readers. *This edge is what triggers dependent producers.*
    Committed,
    /// Discarded; never visible.
    Aborted,
}

/// What a layer carries. A layer holds ValueEvents **xor** DefEvents, never both — which is what
/// makes "the def-version as of layer L" well-defined (SPEC.md §6.2).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum LayerKind {
    Value,
    Def,
}

/// Who authored a layer, and what it reflects. SPEC.md §6.3.
///
/// The log is two interleaved streams: source layers pushed by clients, and derived layers chasing
/// them. A watermark is literally a pointer into the source stream.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum LayerAuthor {
    /// Authored externally. Ground truth.
    Source,
    /// Emitted by the derivation engine. Skipped by merge — the child's derived values are wrong on
    /// the parent by construction — and droppable, because they are a cache that happens to live in
    /// the log (SPEC.md §6.3, §13).
    Derived {
        producer: ProducerId,
        /// The source layer this brings the world up to. Derived data is always addressed by this,
        /// never by derived `LayerId`, which is what makes the ordering of concurrent independent
        /// producers unobservable (SPEC.md §16.3).
        reflects: LayerId,
    },
}

/// A condition a transaction is contingent on: *nothing has touched these cells since this layer*.
/// SPEC.md §12.
///
/// **Guards may reference source cells only.** Guarding derived data is meaningless — its value is a
/// function of source data with a lag, so the guard would be checking a shadow. Guard the sources
/// instead.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Guard {
    pub cells: Vec<CellRef>,
    pub since: LayerId,
}

/// A layer's metadata.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Layer {
    pub id: LayerId,
    pub branch: BranchId,
    pub kind: LayerKind,
    pub author: LayerAuthor,
    pub state: LayerState,
    /// The layer this one follows on its branch. `None` for a branch's first layer.
    pub parent: Option<LayerId>,
    /// Validated at seal. Retained afterwards because merge re-evaluates them against the parent —
    /// which is what makes guards the merge-conflict detector (SPEC.md §13).
    pub guards: Vec<Guard>,
}

/// A branch. SPEC.md §7.1.
///
/// The parent branch is inferred from the origin layer; no explicit parent pointer exists. Branches
/// are registry-scoped — one global tree spanning all repos — which is what makes cross-repo
/// def-mutations atomic.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Branch {
    pub id: BranchId,
    pub name: Option<String>,
    /// `None` marks the root of the tree.
    pub origin: Option<LayerId>,
}

/// The ancestry a read resolves through. SPEC.md §7.2.
///
/// A fork inherits its parent by *ancestry*, never by copying, which is what keeps forking O(1) even
/// under eager derivation (SPEC.md §7.4). Resolving a cell walks the segments outward: the first one
/// holding any record for that cell wins.
///
/// "Holding any record" rather than "holding a value" is the important part — a tombstone on a child
/// must stop the walk, or a deletion would fall through and resurrect the parent's value.
///
/// The engine computes this and hands it to storage, so that `StorageProvider` never has to know
/// what a branch is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadPath {
    /// Innermost first. Each segment is a branch and the highest layer visible on it — head for the
    /// branch being read, and the fork point for every ancestor.
    pub segments: Vec<(BranchId, LayerId)>,
}

impl ReadPath {
    pub fn new(segments: Vec<(BranchId, LayerId)>) -> Self {
        Self { segments }
    }

    /// The branch being read, before ancestry.
    pub fn branch(&self) -> Option<BranchId> {
        self.segments.first().map(|(branch, _)| *branch)
    }

    /// The highest layer visible anywhere along the path.
    ///
    /// This, not the branch's own head, is what "read at HEAD" means: a fork that has not written
    /// anything yet has no head of its own, and its effective ceiling is the fork point it inherits
    /// from.
    pub fn ceiling(&self) -> LayerId {
        self.segments
            .iter()
            .map(|(_, bound)| *bound)
            .max()
            .unwrap_or(LayerId(0))
    }
}

/// What a merge carries across. SPEC.md §13.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MergeMode {
    /// Definition mutations only — the common case. The parent's existing values are then read
    /// through the new lens.
    DefOnly,
    /// Definitions and data both.
    DefAndData,
}
