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

pub(crate) fn err(kind: &'static str, input: &str, reason: impl fmt::Display) -> ParseError {
    ParseError {
        kind,
        input: input.to_string(),
        reason: reason.to_string(),
    }
}

/// Parse a cell address.
///
/// Two forms are accepted:
///
/// ```text
/// Company:o-1234abcd.website    canonical — the whole PID, and what everything renders
/// Company#100.website           shorthand — `branch`, `allocator` and that counter
/// ```
///
/// **The shorthand is input-only, permanently.** It exists because hand-authored scenario data and
/// `borg set Company#1.name` read far better with a small number than with a base32 blob, and it
/// means exactly "the caller's branch, the caller's allocator, this counter" — which is why the
/// caller supplies the first two. Nothing in the system ever *renders* it: `CellRef`'s `Display` is
/// always canonical, so a shorthand address that leaves a human's hands is immediately replaced by
/// one that names the PID in full. Do not "unify" the two by teaching `Display` to emit `#`; that
/// is the lossiness this form was built to remove.
pub fn cell_ref(
    input: &str,
    branch: BranchId,
    allocator: AllocatorId,
) -> Result<CellRef, ParseError> {
    // A PID's text is base32 and a field name has no dots, so the first `.` is always the field
    // separator.
    let (head, field) = match input.split_once('.') {
        Some((head, field)) if !field.is_empty() => (head, Some(field)),
        Some(_) => return Err(err("cell", input, "trailing `.` with no field")),
        None => (input, None),
    };

    let at = head
        .find([':', '#'])
        .ok_or_else(|| err("cell", input, "expected `Struct:id` or `Element[]:id`"))?;
    let (name, rest) = head.split_at(at);
    let shorthand = rest.starts_with('#');
    let id = &rest[1..];

    // `Founder[]:l-5678wxyz` and `Founder[]:l-5678wxyz[0]` — a list, or one of its elements.
    if let Some(element) = name.strip_suffix("[]") {
        if element.is_empty() {
            return Err(err("cell", input, "missing element type before `[]`"));
        }
        if field.is_some() {
            return Err(err("cell", input, "list cells have no fields"));
        }
        let (id, index) = match id.split_once('[') {
            Some((id, index)) => {
                let index = index
                    .strip_suffix(']')
                    .ok_or_else(|| err("cell", input, "unclosed `[`"))?;
                let index: u64 = index
                    .parse()
                    .map_err(|_| err("cell", input, format!("`{index}` is not an index")))?;
                (id, Some(index))
            }
            None => (id, None),
        };
        let pid = pid(PidKind::List, id, shorthand, branch, allocator, input)?;
        return Ok(match index {
            Some(index) => CellRef::elem(element.into(), pid, index),
            None => CellRef::list(element.into(), pid),
        });
    }

    // `Company:o-1234abcd` and `Company:o-1234abcd.website` — an object, or one of its properties.
    if name.is_empty() {
        return Err(err("cell", input, "missing struct name"));
    }
    let pid = pid(PidKind::Object, id, shorthand, branch, allocator, input)?;
    Ok(match field {
        Some(field) => CellRef::prop(name.into(), field.into(), pid),
        None => CellRef::existence(name.into(), pid),
    })
}

/// The id half of a cell address, in either form.
///
/// A canonical PID carries its own kind, and is taken at its word: `kind` is the kind the *shape*
/// of the address implies, and only the shorthand — which says nothing about kind — needs it.
fn pid(
    kind: PidKind,
    id: &str,
    shorthand: bool,
    branch: BranchId,
    allocator: AllocatorId,
    input: &str,
) -> Result<Pid, ParseError> {
    if !shorthand {
        return id
            .parse::<Pid>()
            .map_err(|inner| err("cell", input, inner.reason));
    }
    let counter: u64 = id
        .parse()
        .map_err(|_| err("cell", input, format!("`{id}` is not an id")))?;
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
/// 42                     Int
/// 1.5                    Double
/// true / false           Bool
/// @o-1234abcd            a reference — a bare PID, and what `render` emits
/// @Company:o-1234abcd    the same reference, named through a cell it identifies
/// @Company#101           the same again, in the input-only shorthand
/// ~                      a tombstone
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
        // A reference *is* a PID — the struct name adds nothing the PID does not already carry, so
        // a bare one is accepted and is what comes back out.
        if let Ok(pid) = target.parse::<Pid>() {
            return Ok(Value::Ref(pid));
        }
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
        "expected an integer, a decimal, true, false, ~, or @<pid>",
    ))
}

/// Render a value in the same shorthand `value` parses.
pub fn render(value: &Value) -> String {
    match value {
        Value::Int(n) => n.to_string(),
        Value::Double(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Tombstone => "~".to_string(),
        Value::Ref(pid) => format!("@{pid}"),
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

    fn object(counter: u64) -> Pid {
        Pid::Allocated {
            kind: PidKind::Object,
            branch: BranchId(9),
            allocator: AllocatorId(3),
            counter,
        }
    }

    /// The text form is canonical, so anything we print must parse back to itself.
    #[test]
    fn cell_addresses_round_trip() {
        for input in [
            "Company:o-04068",
            "Company:o-04068.website",
            "Founder[]:l-040f80r",
            "Founder[]:l-040f80r[7]",
        ] {
            let parsed = cell_ref(input, B, A).expect(input);
            assert_eq!(parsed.to_string(), input, "round trip of {input}");
        }
    }

    /// The other direction, over PIDs no shorthand could name: a foreign branch, a second
    /// allocator, a counter at the top of the range, and a content address.
    #[test]
    fn every_cell_shape_survives_render_and_reparse() {
        let list = Pid::Allocated {
            kind: PidKind::List,
            branch: BranchId(u64::MAX),
            allocator: AllocatorId(u32::MAX),
            counter: u64::MAX,
        };
        let interned = Pid::Content {
            kind: PidKind::String,
            hash: [7u8; 32],
        };
        for cell in [
            CellRef::existence("Company".into(), object(u64::MAX)),
            CellRef::prop("Company".into(), "website".into(), object(0)),
            CellRef::prop("Company".into(), "website".into(), interned),
            CellRef::list("Founder".into(), list),
            CellRef::elem("Founder".into(), list, u64::MAX),
        ] {
            let text = cell.to_string();
            assert_eq!(
                cell_ref(&text, B, A).expect(&text),
                cell,
                "round trip of {text}"
            );
        }
    }

    /// Shorthand names the root branch, allocator 0, and that counter — nothing else.
    #[test]
    fn shorthand_names_the_callers_branch_and_allocator() {
        assert_eq!(
            *cell_ref("Company#7", B, A).unwrap().pid(),
            Pid::Allocated {
                kind: PidKind::Object,
                branch: B,
                allocator: A,
                counter: 7,
            }
        );
        assert_eq!(
            cell_ref("Founder[]#7", B, A).unwrap().pid().kind(),
            PidKind::List,
            "a list shorthand allocates a list PID, not an object one"
        );
    }

    /// Shorthand is accepted on input and never produced. Rendering it back canonically is what
    /// makes the lossy form impossible to reintroduce by copy-paste.
    #[test]
    fn shorthand_is_never_echoed_back() {
        for input in [
            "Company#100",
            "Company#100.website",
            "Founder[]#500",
            "Founder[]#500[7]",
        ] {
            let cell = cell_ref(input, B, A).expect(input);
            let text = cell.to_string();
            assert!(!text.contains('#'), "{input} rendered as {text}");
            assert_eq!(
                cell_ref(&text, B, A).expect(&text),
                cell,
                "{text} means {input}"
            );
        }
    }

    #[test]
    fn malformed_addresses_are_rejected_with_the_offending_input() {
        for input in [
            "Company",
            "#100",
            "Company#abc",
            "[]#1",
            "Founder[]#1[x]",
            ":o-04068",
            "Company:",
            "Company:q-04068",
            "Company:o-04o68.",
            "Company:o-04068[0]",
            "Founder[]:l-040f80r[7",
        ] {
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
        assert!(value("nonsense", B, A).is_err());
    }

    /// A reference carries a whole PID, so what `render` prints must name the same object when it
    /// is read back — including the branch it was allocated on.
    #[test]
    fn a_reference_round_trips_without_losing_its_pid() {
        let reference = Value::Ref(object(100));
        assert_eq!(
            value(&render(&reference), B, A).unwrap(),
            reference,
            "rendered as {}",
            render(&reference)
        );
        assert_eq!(
            value("@Company#7", B, A).unwrap(),
            Value::Ref(Pid::Allocated {
                kind: PidKind::Object,
                branch: B,
                allocator: A,
                counter: 7,
            }),
            "the cell shorthand still names an object"
        );
    }
}
