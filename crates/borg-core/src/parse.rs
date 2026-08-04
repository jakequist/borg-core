//! Parsing the text forms of cell addresses and values.
//!
//! These are the forms a human types into the CLI and a shell pipeline writes into the wire
//! protocol. They are deliberately the *same* forms, parsed here and rendered by the matching
//! `Display` impls, so the CLI, the protocol and error messages cannot drift into three dialects.

use crate::bigint;
use crate::cell::{BufferId, CellKey, CellRef};
use crate::ids::{AllocatorId, BranchId};
use crate::pid::{Pid, PidKind};
use crate::value::{Value, ValueInput};
use std::fmt::{self, Write as _};

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
        // An untyped array has no def and so no name of its own; the kind letter in the PID is what
        // says so, which is the same dispatch §4.2 relies on to route without a schema lookup.
        if pid.kind() == PidKind::AnyArray {
            return Ok(CellRef {
                buffer: BufferId::AnyArray,
                key: match index {
                    Some(index) => CellKey::Elem(pid, index),
                    None => CellKey::Pid(pid),
                },
            });
        }
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
    if pid.kind() == PidKind::AnyObject {
        // One buffer for every untyped object, so there is nowhere to put a field name.
        if field.is_some() {
            return Err(err("cell", input, "an untyped object has no named fields"));
        }
        return Ok(CellRef {
            buffer: BufferId::AnyObject,
            key: CellKey::Pid(pid),
        });
    }
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

/// Parse a value in the CLI's shorthand. SPEC.md §3.4.
///
/// ```text
/// 42                     Int
/// 1.5                    Double
/// true / false           Bool
/// ~                      a tombstone
/// @o-1234abcd            a reference — a bare PID, and what `render` emits
/// @Company:o-1234abcd    the same reference, named through a cell it identifies
/// @Company#101           the same again, in the input-only shorthand
/// 0xdeadbeef             Binary
/// -170141183460469n      BigInt
/// acme.ai                String — anything that matched none of the above
/// ```
///
/// **A bare word is a string**, and that is why this cannot fail: every input names some value.
/// Quoting was the alternative and it is worse — a shell worker is the target audience (§17.4), and
/// a form needing quotes is one that will eventually be typed unquoted.
///
/// ## The ambiguity, stated plainly
///
/// The forms above win, so the *strings* spelling them are unwritable today: `true` is a `Bool`, so
/// no string field can hold the text "true"; `42` is an `Int`; `0xff` is `Binary`; `@nonsense` is a
/// String only because it failed to parse as a PID, which turns a typo'd reference into data.
///
/// This is not a hole to paper over — it is what untyped parsing costs. Milestone B makes parsing
/// **type-directed** against the field's declared type, at which point `Company.name` being declared
/// `String` means `true` is the four-character string and nothing else, and a malformed `@…` against
/// a reference field is an error rather than a silent string. Until then the shorthand guesses, and
/// this doc is the record of what it guesses.
///
/// The result is a [`ValueInput`] rather than a [`Value`] because content-addressed kinds have no
/// identity until they are interned (§3.1) — see that type.
pub fn value(
    input: &str,
    branch: BranchId,
    allocator: AllocatorId,
) -> Result<ValueInput, ParseError> {
    match input {
        "true" => return Ok(Value::Bool(true).into()),
        "false" => return Ok(Value::Bool(false).into()),
        "~" => return Ok(Value::Tombstone.into()),
        _ => {}
    }

    // `@` and `0x` are *reserved sigils*: having introduced one, a malformed remainder is an error
    // rather than a string. Falling through would turn a mistyped reference into data that looks
    // almost right — the worst possible failure, because nothing complains and the value is wrong.
    //
    // The cost is that a string genuinely starting with `@` cannot be written yet. Type-directed
    // parsing relaxes this: once the field's declared type says `String`, `@jake` is simply that
    // string (ROADMAP, milestone B).
    if let Some(target) = input.strip_prefix('@') {
        // A reference *is* a PID — the struct name adds nothing the PID does not already carry, so
        // a bare one is accepted and is what comes back out.
        if let Ok(pid) = target.parse::<Pid>() {
            return Ok(Value::Ref(pid).into());
        }
        if let Ok(cell) = cell_ref(target, branch, allocator) {
            return Ok(Value::Ref(*cell.pid()).into());
        }
        return Err(err(
            "value",
            input,
            "`@` introduces a reference, and this is neither a PID nor a cell address",
        ));
    }
    if let Some(rest) = input.strip_prefix("0x") {
        return unhex(rest).map(ValueInput::binary).ok_or_else(|| {
            err(
                "value",
                input,
                "`0x` introduces binary, and this is not an even-length run of hex digits",
            )
        });
    }

    if let Some(bytes) = input.strip_suffix('n').and_then(bigint::encode) {
        return Ok(ValueInput::Content {
            kind: PidKind::BigInt,
            bytes,
        });
    }
    if let Ok(n) = input.parse::<i64>() {
        return Ok(Value::Int(n).into());
    }
    // An integer literal too large for `Int` must not quietly land in `Double`. `1e23` is not the
    // number that was typed, and BigInt is one character away.
    if is_integer_literal(input) {
        return Err(err(
            "value",
            input,
            "too large for Int — append `n` to store it as a BigInt",
        ));
    }
    if looks_numeric(input)
        && let Ok(n) = input.parse::<f64>()
    {
        return Ok(Value::Double(n).into());
    }
    Ok(ValueInput::string(input))
}

/// Digits with an optional sign, and nothing else — no point, no exponent.
fn is_integer_literal(input: &str) -> bool {
    let digits = input.strip_prefix(['+', '-']).unwrap_or(input);
    !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
}

/// Whether this could be a decimal at all.
///
/// Guards `f64::from_str`, which also accepts `nan`, `inf`, `+inf` and `infinity` — words a person
/// writing a string field would reasonably type, and which have no round-tripping text form anyway.
/// Requiring a digit is what rules them out; requiring the *first* character to be a digit or a sign
/// is what stops `3rd` and `1a` from being read as numbers with trailing junk.
fn looks_numeric(input: &str) -> bool {
    input.starts_with(|c: char| c.is_ascii_digit() || matches!(c, '+' | '-' | '.'))
        && input.bytes().any(|b| b.is_ascii_digit())
}

/// Whole octets only. An odd number of digits has no canonical reading, and guessing one would mean
/// two spellings of one blob — two hashes, two interned copies (§3.1).
fn unhex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    text.as_bytes()
        .chunks(2)
        .map(|pair| {
            let digit = |b: u8| char::from(b).to_digit(16).map(|d| d as u8);
            Some(digit(pair[0])? << 4 | digit(pair[1])?)
        })
        .collect()
}

/// Render a value in the same shorthand `value` parses.
///
/// A reference to a content-addressed PID comes out as `@s-…` here, which is the *honest* answer for
/// a caller holding nothing but the value: the content lives in the store, and only something with a
/// store handle can resolve it. Client-facing surfaces resolve it first — see
/// `borg_engine::Values::render` and [`render_interned`].
pub fn render(value: &Value) -> String {
    match value {
        Value::Int(n) => n.to_string(),
        Value::Double(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Tombstone => "~".to_string(),
        Value::Ref(pid) => format!("@{pid}"),
    }
}

/// Render interned content back into the text `value` parses. SPEC.md §3.4.
///
/// This is the half of the value model that makes interning invisible: a cell holding a `Ref` to a
/// content-addressed PID reads back as its content, so a pipeline asking for `company.website`
/// receives `acme.ai` and never `@s-1a2b3c`.
pub fn render_interned(pid: &Pid, bytes: &[u8]) -> String {
    match pid.kind() {
        // Interned strings are UTF-8 by construction — nothing can intern a `String` except from
        // one — so `lossy` is a total function's default, not a repair.
        PidKind::String => String::from_utf8_lossy(bytes).into_owned(),
        PidKind::Binary => {
            let mut text = String::with_capacity(2 + bytes.len() * 2);
            text.push_str("0x");
            for byte in bytes {
                let _ = write!(text, "{byte:02x}");
            }
            text
        }
        PidKind::BigInt => format!("{}n", bigint::decode(bytes)),
        // Not content-addressed, so there are no bytes behind it and the PID is the value's text.
        _ => format!("@{pid}"),
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
            let parsed = value(input, B, A).expect(input).immediate().expect(input);
            assert_eq!(render(&parsed), input, "round trip of {input}");
        }
    }

    /// The three content-addressed kinds have no identity until a store interns them, so parsing
    /// stops one step short and names the kind plus the canonical bytes (§3.1).
    #[test]
    fn content_addressed_values_parse_to_their_canonical_bytes() {
        assert_eq!(
            value("acme.ai", B, A).unwrap(),
            ValueInput::Content {
                kind: PidKind::String,
                bytes: b"acme.ai".to_vec()
            }
        );
        assert_eq!(
            value("0xdeadbeef", B, A).unwrap(),
            ValueInput::Content {
                kind: PidKind::Binary,
                bytes: vec![0xde, 0xad, 0xbe, 0xef]
            }
        );
        assert_eq!(
            value("-129n", B, A).unwrap(),
            ValueInput::Content {
                kind: PidKind::BigInt,
                bytes: vec![0xff, 0x7f]
            }
        );
    }

    /// The round trip that matters for a client: whatever `borg get` prints must mean the same value
    /// when it is handed back to `borg set`.
    #[test]
    fn interned_content_round_trips_through_its_text() {
        for input in [
            "acme.ai",
            "we make things",
            "a string with: colons, @ and #",
            "",
            "0x",
            "0xdeadbeef",
            "0n",
            "-129n",
            "170141183460469231731687303715884105728n",
        ] {
            let ValueInput::Content { kind, bytes } = value(input, B, A).unwrap() else {
                panic!("{input} should be content-addressed");
            };
            let pid = crate::content::pid(kind, &bytes).expect(input);
            assert_eq!(
                render_interned(&pid, &bytes),
                input,
                "round trip of {input}"
            );
        }
    }

    /// The documented ambiguity, asserted rather than described: the older forms win, so their
    /// spellings are not strings. Milestone B's type-directed parsing is what resolves this.
    #[test]
    fn a_form_that_already_means_something_is_not_a_string() {
        for input in ["42", "1.5", "true", "false", "~", "0xff", "7n"] {
            assert!(
                !matches!(
                    value(input, B, A).unwrap(),
                    ValueInput::Content {
                        kind: PidKind::String,
                        ..
                    }
                ),
                "{input} should keep its existing meaning"
            );
        }
        // …and everything else is, including text that would trip a permissive float parser.
        for input in [
            "acme.ai", "nan", "inf", "+inf", "-inf", "infinity", "12a3n", "-", "n", "3rd",
        ] {
            assert_eq!(
                value(input, B, A).unwrap(),
                ValueInput::string(input),
                "{input} should be a string"
            );
        }
    }

    /// `@` and `0x` are reserved. Having introduced one, a malformed remainder is an error rather
    /// than a string — a mistyped reference silently becoming data that looks almost right is the
    /// worst available outcome, because nothing complains and the value is wrong.
    #[test]
    fn a_malformed_reserved_sigil_is_an_error_not_a_string() {
        for input in ["@oops", "@o-", "0xzz", "0xf"] {
            let error = value(input, B, A).unwrap_err();
            assert!(
                error.to_string().contains(input),
                "error for {input} should quote it back: {error}"
            );
        }
    }

    /// An integer past `Int` must not land quietly in `Double`: `1e23` is not the number that was
    /// typed, and BigInt is one character away.
    #[test]
    fn an_integer_too_large_for_int_is_an_error_pointing_at_bigint() {
        let error = value("99999999999999999999999", B, A).unwrap_err();
        assert!(error.to_string().contains('n'), "{error}");

        // The same digits with the suffix are a BigInt, not an error.
        assert!(matches!(
            value("99999999999999999999999n", B, A).unwrap(),
            ValueInput::Content {
                kind: PidKind::BigInt,
                ..
            }
        ));
    }

    /// A reference carries a whole PID, so what `render` prints must name the same object when it
    /// is read back — including the branch it was allocated on.
    #[test]
    fn a_reference_round_trips_without_losing_its_pid() {
        let reference = Value::Ref(object(100));
        assert_eq!(
            value(&render(&reference), B, A).unwrap().immediate(),
            Some(reference),
            "rendered as {}",
            render(&reference)
        );
        assert_eq!(
            value("@Company#7", B, A).unwrap().immediate(),
            Some(Value::Ref(Pid::Allocated {
                kind: PidKind::Object,
                branch: B,
                allocator: A,
                counter: 7,
            })),
            "the cell shorthand still names an object"
        );
    }

    /// The untyped buffers have no name of their own, so they render as `Any` and are recognised by
    /// their PID kind. Every buffer must render as something that parses back — a `{:?}` escape
    /// hatch would be a second dialect in exactly the places a readable address matters.
    #[test]
    fn every_buffer_renders_as_an_address_that_parses_back() {
        let untyped_object = Pid::Content {
            kind: PidKind::AnyObject,
            hash: [3u8; 32],
        };
        let untyped_array = Pid::Content {
            kind: PidKind::AnyArray,
            hash: [4u8; 32],
        };
        for (cell, expected) in [
            (
                CellRef {
                    buffer: BufferId::AnyObject,
                    key: CellKey::Pid(untyped_object),
                },
                "Any:j-",
            ),
            (
                CellRef {
                    buffer: BufferId::AnyArray,
                    key: CellKey::Pid(untyped_array),
                },
                "Any[]:y-",
            ),
            (
                CellRef {
                    buffer: BufferId::AnyArray,
                    key: CellKey::Elem(untyped_array, 2),
                },
                "Any[]:y-",
            ),
        ] {
            let text = cell.to_string();
            assert!(text.starts_with(expected), "{text} should name {expected}");
            assert_eq!(
                cell_ref(&text, B, A).expect(&text),
                cell,
                "round trip {text}"
            );
        }
    }
}
