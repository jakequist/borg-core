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

    /// Explicitly removed (SPEC.md §8.1).
    ///
    /// Distinct from *absent*, which is the `None` of an enclosing `Option<Value>`: absence means
    /// never written, a tombstone means removed. Both are legitimate tracked reads and a producer
    /// must be able to tell them apart.
    ///
    /// A tombstone is cell-valued, so **the cell it occupies determines what was removed**:
    /// in a property cell it is an unset field, in an existence cell a deleted object, and in a
    /// future set-member or map-entry cell one removed element. One concept, every granularity.
    Tombstone,
}

impl Value {
    pub const fn as_ref_pid(&self) -> Option<&Pid> {
        match self {
            Value::Ref(pid) => Some(pid),
            _ => None,
        }
    }

    pub const fn is_tombstone(&self) -> bool {
        matches!(self, Value::Tombstone)
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
