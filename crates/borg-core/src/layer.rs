//! Layers and branches. SPEC.md §6, §7.

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
