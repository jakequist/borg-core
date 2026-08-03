//! Values. SPEC.md §3.
//!
//! A cell holds either an inline primitive or a PID. Primitives have no PID because the identifier
//! would cost more than the payload; everything else is referenced by PID and lives in a buffer.

use crate::pid::Pid;
use serde::{Deserialize, Serialize};

/// What a cell can hold.
#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
pub enum Value {
    // Primitives — stored inline, no PID (SPEC.md §3.2).
    Int(i64),
    Bool(bool),
    Double(f64),

    /// Everything else. Strings, binary, bigints, objects, lists, and the `Any*` family are all
    /// reached by PID.
    Ref(Pid),

    /// A tombstone (SPEC.md §8.1). Distinct from "absent": absence is a legitimate tracked read,
    /// and a producer observing either must be able to tell them apart.
    Deleted,
}

impl Value {
    pub const fn as_ref_pid(&self) -> Option<&Pid> {
        match self {
            Value::Ref(pid) => Some(pid),
            _ => None,
        }
    }

    pub const fn is_deleted(&self) -> bool {
        matches!(self, Value::Deleted)
    }
}

/// The static type of a field or element. SPEC.md §5.1.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub enum ValueType {
    Int,
    Bool,
    Double,
    String,
    Binary,
    BigInt,
    Object(ObjectTypeName),
    List(Box<ValueType>),
    Any,
    AnyObject,
    AnyArray,
    AnyNumber,
    // Set/Map deferred (SPEC.md §3.3).
}

/// A struct name. The namespace is flat and registry-wide: a struct has no owner, only its fields do
/// (SPEC.md §5.2).
pub type ObjectTypeName = std::sync::Arc<str>;
