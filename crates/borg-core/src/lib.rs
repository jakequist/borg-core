//! # borg-core
//!
//! Pure types. No I/O, no coordination, no derivation. Everything else in the workspace depends on
//! this crate; this crate depends on nothing of ours.
//!
//! See `SPEC.md` at the repository root — every type here cites the section it implements.

pub mod cell;
pub mod def;
pub mod error;
pub mod freshness;
pub mod ids;
pub mod layer;
pub mod pid;
pub mod value;

pub use cell::{BufferId, CellKey, CellRecord, CellRef, Derivation, FieldName, Origin};
pub use def::{FieldDef, MigrationDirection, ObjectDef, ProducerDef, ProducerKind};
pub use error::{BorgError, Result};
pub use freshness::{Freshness, FreshnessRequirement, Resolved, Watermark};
pub use ids::{AllocatorId, BranchId, ClientVersion, LayerId, ProducerId, RepoId};
pub use layer::{Branch, Layer, LayerAuthor, LayerKind, LayerState};
pub use pid::{Pid, PidAllocator, PidKind};
pub use value::{ObjectTypeName, Value, ValueType};
