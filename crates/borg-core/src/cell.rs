//! Cells — the universal addressable unit. SPEC.md §4.
//!
//! Every mechanism in Borg is field-granular: transaction guards, producer dependencies, field
//! ownership, migration staleness, and merge conflict resolution all key on the same primitive. So
//! the physical unit of storage is the cell, not the object.

use crate::ids::{ClientVersion, LayerId, ProducerId};
use crate::pid::Pid;
use crate::value::Value;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

/// A field name. `Arc`-shared because field names repeat across every cell of a type and are
/// compared constantly during dependency-index lookups.
pub type FieldName = Arc<str>;

/// Addresses one cell.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum CellRef {
    /// An object property. SPEC.md §4.1.
    Prop { pid: Pid, field: FieldName },
    /// A list element. Lists are append-only in v1, so indices are stable (SPEC.md §4.4).
    Elem { pid: Pid, index: u64 },
    /// An object's existence. A cell like any other — it has to be, or it could not appear in a
    /// read-set, and then deletion could not invalidate anything (SPEC.md §8.1).
    ///
    /// **Every cell read implicitly depends on its object's existence cell.** Otherwise a
    /// `DeleteObject` writes only this cell, and everything depending on `company#100.name` never
    /// observes a write to *that* cell and is never invalidated. Costs one extra read-set entry per
    /// object touched, and keeps deletion `O(1)` rather than `O(fields)`.
    Existence { pid: Pid },
}

impl CellRef {
    pub const fn pid(&self) -> &Pid {
        match self {
            CellRef::Prop { pid, .. } | CellRef::Elem { pid, .. } | CellRef::Existence { pid } => {
                pid
            }
        }
    }
}

impl fmt::Debug for CellRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CellRef::Prop { pid, field } => write!(f, "{pid:?}.{field}"),
            CellRef::Elem { pid, index } => write!(f, "{pid:?}[{index}]"),
            CellRef::Existence { pid } => write!(f, "{pid:?}.<exists>"),
        }
    }
}

/// Whether a cell is ground truth or computed. SPEC.md §8.
///
/// Origin is a property of the `(struct, field)` pair, not of the object: one `Company` may carry
/// source cells and derived cells side by side.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub enum Origin {
    Source,
    Derived,
}

/// What is physically stored for a cell. SPEC.md §4.3.
///
/// Source cells carry only the first three fields. The heavy metadata — watermark, read-set,
/// producer — attaches to derived cells only, which in a normalized model are the minority.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CellRecord {
    pub value: Value,
    /// The `ClientVersion` this value was *written* at. Never coerced or rewritten; readers at other
    /// versions migrate on the read path (SPEC.md §5.4).
    pub version: ClientVersion,
    /// The layer that produced this value.
    pub written_at: LayerId,
    pub origin: Origin,
    /// Present only when `origin == Derived`.
    pub derivation: Option<Derivation>,
}

/// Derived-cell metadata. SPEC.md §4.3, §10.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Derivation {
    pub producer: ProducerId,
    /// The source layer through which every input has been incorporated — i.e. *"replay the world at
    /// this layer and you get exactly this value."* SPEC.md §10.1.
    pub fresh_as_of: LayerId,
    /// Exactly the cells this invocation read, captured automatically via `ProducerCtx`
    /// (SPEC.md §9.4). Read forwards it drives invalidation; read backwards, lineage.
    pub read_set: Vec<CellRef>,
}

/// Identifies a buffer — a physical partition of cells. SPEC.md §4.2.
///
/// Buffers partition **by def where a def exists, and globally where one does not.** Objects get one
/// buffer per struct, which is what makes "a producer maps over a source buffer" precise: the buffer
/// *is* the set of instances of one struct. Interning buffers must be global, since registry-wide
/// deduplication is their entire purpose.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub enum BufferId {
    /// One per struct.
    Object(crate::value::ObjectTypeName),
    /// One per `ListDef`.
    List(crate::value::ObjectTypeName),
    /// Global — interning is registry-wide by definition.
    String,
    Binary,
    BigInt,
    /// Global — the `Any*` family has no def to partition by.
    AnyObject,
    AnyArray,
}
