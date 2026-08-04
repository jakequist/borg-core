//! Parsing the text forms of cell addresses and values.
//!
//! These are the forms a human types into the CLI and a shell pipeline writes into the wire
//! protocol. They are deliberately the *same* forms, parsed here and rendered by the matching
//! `Display` impls, so the CLI, the protocol and error messages cannot drift into three dialects.

use crate::cell::{BufferId, CellKey, CellRef};
use crate::ids::{AllocatorId, BranchId};
use crate::pid::{Pid, PidKind};
use crate::value::Value;
use std::fmt;

#[derive(Debug, thiserror::Error)]
#[error("cannot parse {kind} from `{input}`: {reason}")]
pub struct ParseError {
    pub kind: &'static str,
    pub input: String,
    pub reason: String,
}

fn err(kind: &'static str, input: &str, reason: impl fmt::Display) -> ParseError {
    ParseError {
        kind,
        input: input.to_string(),
        reason: reason.to_string(),
    }
}

/// Parse a cell address written in the canonical text form (see [`CellRef`]'s `Display`).
///
/// `branch` and `allocator` are supplied by the caller rather than written out, because a human
/// naming a cell means "on the branch I am working on". The full PID still exists underneath; this
/// is shorthand, not a different addressing scheme.
pub fn cell_ref(
    input: &str,
    branch: BranchId,
    allocator: AllocatorId,
) -> Result<CellRef, ParseError> {
    let (head, field) = match input.split_once('.') {
        Some((head, field)) if !field.is_empty() => (head, Some(field)),
        Some(_) => return Err(err("cell", input, "trailing `.` with no field")),
        None => (input, None),
    };

    // `Founder[]#500` and `Founder[]#500[0]` — a list, or one of its elements.
    if let Some((element, rest)) = head.split_once("[]#") {
        if element.is_empty() {
            return Err(err("cell", input, "missing element type before `[]`"));
        }
        if field.is_some() {
            return Err(err("cell", input, "list cells have no fields"));
        }
        let (counter, index) = match rest.split_once('[') {
            Some((counter, idx)) => {
                let idx = idx
                    .strip_suffix(']')
                    .ok_or_else(|| err("cell", input, "unclosed `[`"))?;
                let idx: u64 = idx
                    .parse()
                    .map_err(|_| err("cell", input, format!("`{idx}` is not an index")))?;
                (counter, Some(idx))
            }
            None => (rest, None),
        };
        let pid = allocated(PidKind::List, counter, branch, allocator, input)?;
        return Ok(match index {
            Some(index) => CellRef::elem(element.into(), pid, index),
            None => CellRef::list(element.into(), pid),
        });
    }

    // `Company#100` and `Company#100.website` — an object, or one of its properties.
    let (struct_name, counter) = head
        .split_once('#')
        .ok_or_else(|| err("cell", input, "expected `Struct#id` or `Element[]#id`"))?;
    if struct_name.is_empty() {
        return Err(err("cell", input, "missing struct name"));
    }
    let pid = allocated(PidKind::Object, counter, branch, allocator, input)?;
    Ok(match field {
        Some(field) => CellRef::prop(struct_name.into(), field.into(), pid),
        None => CellRef::existence(struct_name.into(), pid),
    })
}

fn allocated(
    kind: PidKind,
    counter: &str,
    branch: BranchId,
    allocator: AllocatorId,
    input: &str,
) -> Result<Pid, ParseError> {
    let counter: u64 = counter
        .parse()
        .map_err(|_| err("cell", input, format!("`{counter}` is not an id")))?;
    Ok(Pid::Allocated {
        kind,
        branch,
        allocator,
        counter,
    })
}

/// Parse a value in the CLI's shorthand.
///
/// ```text
/// 42            Int
/// 1.5           Double
/// true / false  Bool
/// @Company#101  a reference to another object
/// ~             a tombstone
/// ```
///
/// Strings are deliberately absent: they are content-addressed and interned (§3.1), and pretending
/// otherwise with a quoted literal would hide that they are a different kind of thing.
pub fn value(input: &str, branch: BranchId, allocator: AllocatorId) -> Result<Value, ParseError> {
    match input {
        "true" => return Ok(Value::Bool(true)),
        "false" => return Ok(Value::Bool(false)),
        "~" => return Ok(Value::Tombstone),
        _ => {}
    }
    if let Some(target) = input.strip_prefix('@') {
        let cell = cell_ref(target, branch, allocator)?;
        return Ok(Value::Ref(*cell.pid()));
    }
    if let Ok(n) = input.parse::<i64>() {
        return Ok(Value::Int(n));
    }
    if let Ok(n) = input.parse::<f64>() {
        return Ok(Value::Double(n));
    }
    Err(err(
        "value",
        input,
        "expected an integer, a decimal, true, false, ~, or @Struct#id",
    ))
}

/// Render a value in the same shorthand `value` parses.
pub fn render(value: &Value) -> String {
    match value {
        Value::Int(n) => n.to_string(),
        Value::Double(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Tombstone => "~".to_string(),
        Value::Ref(pid) => format!("@{pid:?}"),
    }
}

/// A struct name parsed out of a cell address, for commands that name a type rather than a cell.
pub fn buffer_of(cell: &CellRef) -> &BufferId {
    &cell.buffer
}

/// The element index of a list cell, if it has one.
pub const fn index_of(cell: &CellRef) -> Option<u64> {
    match cell.key {
        CellKey::Elem(_, index) => Some(index),
        CellKey::Pid(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const B: BranchId = BranchId(1);
    const A: AllocatorId = AllocatorId(0);

    /// The text form is canonical, so anything we print must parse back to itself.
    #[test]
    fn cell_addresses_round_trip() {
        for input in [
            "Company#100",
            "Company#100.website",
            "Founder[]#500",
            "Founder[]#500[7]",
        ] {
            let parsed = cell_ref(input, B, A).expect(input);
            assert_eq!(parsed.to_string(), input, "round trip of {input}");
        }
    }

    #[test]
    fn malformed_addresses_are_rejected_with_the_offending_input() {
        for input in ["Company", "#100", "Company#abc", "[]#1", "Founder[]#1[x]"] {
            let error = cell_ref(input, B, A).unwrap_err();
            assert!(
                error.to_string().contains(input),
                "error for {input} should quote it back: {error}"
            );
        }
    }

    #[test]
    fn values_round_trip_through_their_shorthand() {
        for input in ["42", "-1", "true", "false", "~"] {
            let parsed = value(input, B, A).expect(input);
            assert_eq!(render(&parsed), input, "round trip of {input}");
        }
        assert!(matches!(value("@Company#7", B, A), Ok(Value::Ref(_))));
        assert!(value("nonsense", B, A).is_err());
    }
}
