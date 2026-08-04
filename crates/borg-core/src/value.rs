//! Values. SPEC.md §3.
//!
//! A cell holds either an inline primitive or a PID. Primitives have no PID because the identifier
//! would cost more than the payload; everything else is referenced by PID and lives in a buffer.

use crate::pid::{Pid, PidKind};
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

/// A value on its way in, before the store has seen it. SPEC.md §3.1, §3.4.
///
/// `String`, `Binary` and `BigInt` are content-addressed: their PID *is* the hash of their content,
/// so text cannot become a [`Value`] without a store to intern into. Parsing therefore stops one
/// step short and hands back this; the engine turns it into a `Value`.
///
/// **This is why interning is invisible to clients.** A worker or a CLI user writes the text of a
/// string and is done — it never learns that a PID was allocated, never makes a second round trip to
/// create one, and never sees `@s-…` come back. Interning is a runtime concern in exactly the way
/// batching is.
#[derive(Clone, PartialEq, Debug)]
pub enum ValueInput {
    /// Already a value: a primitive, a reference, or a tombstone.
    Immediate(Value),
    /// Content that must be interned before it can be stored.
    Content { kind: PidKind, bytes: Vec<u8> },
}

impl ValueInput {
    pub fn string(text: &str) -> Self {
        Self::Content {
            kind: PidKind::String,
            bytes: text.as_bytes().to_vec(),
        }
    }

    pub const fn binary(bytes: Vec<u8>) -> Self {
        Self::Content {
            kind: PidKind::Binary,
            bytes,
        }
    }

    /// The value, when this needed no interning.
    pub const fn immediate(&self) -> Option<Value> {
        match self {
            Self::Immediate(value) => Some(*value),
            Self::Content { .. } => None,
        }
    }
}

impl From<Value> for ValueInput {
    fn from(value: Value) -> Self {
        Self::Immediate(value)
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

impl ValueType {
    /// Whether a value may be stored in a cell declared this type. SPEC.md §5.1, §8.
    ///
    /// **A tombstone satisfies every type.** It means *explicitly removed* (§8.1) rather than "a
    /// value of the wrong shape", and deletion has to be expressible for every field or
    /// `borg delete` would only work on `Any`.
    ///
    /// **What this cannot check.** A `Ref` carries a PID, and a PID records a *kind*, not a struct
    /// (§3.1). So a field declared `Object(Company)` is checked for "is this an object at all" and
    /// no further — nothing in the value says which struct the object belongs to, and finding out
    /// would mean a read of its existence cell on the write path. This is stated rather than faked:
    /// a check that looks total and is not is worse than one that admits its edge.
    pub fn accepts(&self, value: &Value) -> bool {
        if value.is_tombstone() {
            return true;
        }
        match (self, value) {
            (Self::Any, _)
            | (Self::Int, Value::Int(_))
            | (Self::Bool, Value::Bool(_))
            | (Self::Double, Value::Double(_))
            | (Self::AnyNumber, Value::Int(_) | Value::Double(_)) => true,
            (Self::String, Value::Ref(pid)) => pid.kind() == PidKind::String,
            (Self::Binary, Value::Ref(pid)) => pid.kind() == PidKind::Binary,
            (Self::BigInt | Self::AnyNumber, Value::Ref(pid)) => pid.kind() == PidKind::BigInt,
            (Self::Object(_) | Self::AnyObject, Value::Ref(pid)) => {
                matches!(pid.kind(), PidKind::Object | PidKind::AnyObject)
            }
            (Self::List(_) | Self::AnyArray, Value::Ref(pid)) => {
                matches!(pid.kind(), PidKind::List | PidKind::AnyArray)
            }
            _ => false,
        }
    }
}

/// The name a type wears in a def file, an error message and `borg def show` — one spelling, so a
/// type named in a rejection is a type the reader can paste back into a declaration.
impl std::fmt::Display for ValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Int => f.write_str("Int"),
            Self::Bool => f.write_str("Bool"),
            Self::Double => f.write_str("Double"),
            Self::String => f.write_str("String"),
            Self::Binary => f.write_str("Binary"),
            Self::BigInt => f.write_str("BigInt"),
            Self::Object(name) => f.write_str(name),
            Self::List(element) => write!(f, "{element}[]"),
            Self::Any => f.write_str("Any"),
            Self::AnyObject => f.write_str("AnyObject"),
            Self::AnyArray => f.write_str("AnyArray"),
            Self::AnyNumber => f.write_str("AnyNumber"),
        }
    }
}

/// A struct name. The namespace is flat and registry-wide: a struct has no owner, only its fields do
/// (SPEC.md §5.2).
pub type ObjectTypeName = std::sync::Arc<str>;
