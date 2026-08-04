//! Point IDs — the universal identifier for every non-primitive value. SPEC.md §3.1.
//!
//! Two flavors, split by mutability:
//!
//! * **Allocated** — `(branch, allocator, counter)`. Survives mutation, so it is identity.
//!   Objects, lists, and the `Any*` family.
//! * **Content-addressed** — `hash(bytes)`. Immutable, branch-independent, eternal. Strings,
//!   binary, bigints.
//!
//! The consequence that matters most: two nodes independently interning `"hello"` produce the same
//! PID with no coordination, so string writes can never conflict across branches.
//!
//! ## The text form
//!
//! `o-1234abcd` — a kind letter, a hyphen, and the rest of the PID in Crockford base32. It is
//! **lossless**: `Display` and `FromStr` are inverses for every value of this type, which is what
//! lets a PID travel through a shell pipeline, a scenario file or an error message and come back
//! meaning the same object. The form it replaced carried only the counter, so `Company#100` named a
//! different object depending on which branch and allocator you assumed — a defect that reached
//! production behaviour in the CLI and cost a real bug.

use crate::ids::{AllocatorId, BranchId};
use crate::parse::{ParseError, err};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Write as _};
use std::str::FromStr;

/// The kind of value a PID points at. Encoded in the PID itself so that dispatching to the correct
/// buffer requires no lookup.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum PidKind {
    // Allocated identity — mutable.
    Object = 0,
    List = 1,
    Any = 2,
    AnyObject = 3,
    AnyArray = 4,
    AnyNumber = 5,

    // Content-addressed — immutable.
    String = 64,
    Binary = 65,
    BigInt = 66,
    // Set/Map are deferred (SPEC.md §3.3).
}

impl PidKind {
    /// Content-addressed kinds are immutable and their PIDs are eternal.
    pub const fn is_content_addressed(self) -> bool {
        (self as u8) >= 64
    }

    /// Allocated kinds are mutable in place; the PID survives mutation.
    pub const fn is_mutable(self) -> bool {
        !self.is_content_addressed()
    }

    /// The letter this kind wears in the text form.
    ///
    /// The obvious initials go to the kinds a human types most (`o`, `l`, `s`, `b`); `n` is BigInt
    /// (*number*) and `a` is `Any`. The untyped family then takes the next distinctive letter of
    /// its own name — `j` for an*yObj*ect, `y` for arra*y*, `m` for nu*m*ber — because their
    /// initials are already spoken for. One letter, not a word, because these appear in every cell
    /// address a worker prints.
    pub const fn letter(self) -> char {
        match self {
            Self::Object => 'o',
            Self::List => 'l',
            Self::Any => 'a',
            Self::AnyObject => 'j',
            Self::AnyArray => 'y',
            Self::AnyNumber => 'm',
            Self::String => 's',
            Self::Binary => 'b',
            Self::BigInt => 'n',
        }
    }

    /// The inverse of [`PidKind::letter`], case-insensitively.
    pub const fn from_letter(letter: char) -> Option<Self> {
        Some(match letter.to_ascii_lowercase() {
            'o' => Self::Object,
            'l' => Self::List,
            'a' => Self::Any,
            'j' => Self::AnyObject,
            'y' => Self::AnyArray,
            'm' => Self::AnyNumber,
            's' => Self::String,
            'b' => Self::Binary,
            'n' => Self::BigInt,
            _ => return None,
        })
    }
}

/// A Point ID.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Pid {
    /// Identity, allocated without coordination. SPEC.md §3.1, §17.2.
    Allocated {
        kind: PidKind,
        branch: BranchId,
        allocator: AllocatorId,
        counter: u64,
    },
    /// Content address. Equal content always yields an equal PID, on every branch, forever.
    Content { kind: PidKind, hash: [u8; 32] },
}

impl Pid {
    pub const fn kind(&self) -> PidKind {
        match self {
            Pid::Allocated { kind, .. } | Pid::Content { kind, .. } => *kind,
        }
    }

    pub const fn is_mutable(&self) -> bool {
        self.kind().is_mutable()
    }
}

/// A content hash, and the upper bound on any payload.
const HASH_LEN: usize = 32;

/// Three varints: 10 bytes for a `u64` branch, 5 for a `u32` allocator, 10 for a `u64` counter.
///
/// This being *below* [`HASH_LEN`] is load-bearing. The two flavours are told apart on decode by
/// payload length alone, so no discriminator byte is needed and a mis-typed kind letter cannot
/// silently reinterpret one flavour as the other.
const MAX_ALLOCATED: usize = 25;

/// Crockford base32: no `i`, `l`, `o` or `u`, so a PID read aloud or retyped survives the trip.
const ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";

/// The canonical text form. See the module docs — this is lossless, and deliberately so.
impl fmt::Display for Pid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut buf = [0u8; HASH_LEN];
        let payload: &[u8] = match self {
            Pid::Allocated {
                branch,
                allocator,
                counter,
                ..
            } => {
                let mut at = 0;
                put_varint(branch.0, &mut buf, &mut at);
                put_varint(u64::from(allocator.0), &mut buf, &mut at);
                put_varint(*counter, &mut buf, &mut at);
                &buf[..at]
            }
            // The whole hash, never a prefix. A truncated content address would make two distinct
            // strings share a name in every place a human or a shell script handles one, and the
            // cheap fix — a longer prefix — only moves the birthday bound rather than removing it.
            Pid::Content { hash, .. } => hash.as_slice(),
        };
        write!(f, "{}-", self.kind().letter())?;
        encode_base32(payload, f)
    }
}

impl FromStr for Pid {
    type Err = ParseError;

    fn from_str(text: &str) -> Result<Self, ParseError> {
        let (letter, payload) = text
            .split_once('-')
            .ok_or_else(|| err("pid", text, "expected `<kind>-<id>`"))?;

        let mut letters = letter.chars();
        let kind = match (letters.next(), letters.next()) {
            (Some(letter), None) => PidKind::from_letter(letter),
            _ => None,
        }
        .ok_or_else(|| err("pid", text, format!("`{letter}` is not a kind letter")))?;

        let mut bytes = [0u8; HASH_LEN];
        let len = decode_base32(payload, &mut bytes).map_err(|reason| err("pid", text, reason))?;

        if len == HASH_LEN {
            return Ok(Pid::Content { kind, hash: bytes });
        }
        allocated(kind, &bytes[..len])
            .ok_or_else(|| err("pid", text, format!("`{payload}` is not a well-formed id")))
    }
}

/// Rebuild an allocated PID from its varint payload. `None` on anything malformed — a truncated
/// varint, an allocator that does not fit, or trailing bytes nobody claimed.
fn allocated(kind: PidKind, mut bytes: &[u8]) -> Option<Pid> {
    let branch = take_varint(&mut bytes)?;
    let allocator = take_varint(&mut bytes)?;
    let counter = take_varint(&mut bytes)?;
    if !bytes.is_empty() {
        return None;
    }
    Some(Pid::Allocated {
        kind,
        branch: BranchId(branch),
        allocator: AllocatorId(u32::try_from(allocator).ok()?),
        counter,
    })
}

fn put_varint(mut value: u64, out: &mut [u8], at: &mut usize) {
    loop {
        let byte = (value as u8) & 0x7f;
        value >>= 7;
        out[*at] = if value == 0 { byte } else { byte | 0x80 };
        *at += 1;
        if value == 0 {
            return;
        }
    }
}

fn take_varint(bytes: &mut &[u8]) -> Option<u64> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let (byte, rest) = bytes.split_first()?;
        *bytes = rest;
        let part = u64::from(byte & 0x7f);
        // Reject a payload whose varint claims more bits than a u64 holds, rather than wrapping it
        // into some other perfectly plausible PID.
        value |= part.checked_shl(shift).filter(|v| v >> shift == part)?;
        if byte & 0x80 == 0 {
            return Some(value);
        }
        shift += 7;
    }
}

fn encode_base32(bytes: &[u8], f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let mut acc = 0u32;
    let mut bits = 0u32;
    for byte in bytes {
        acc = (acc << 8) | u32::from(*byte);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            f.write_char(digit_at((acc >> bits) & 31))?;
        }
    }
    // Whatever the last byte left over, zero-padded up to one final digit.
    if bits > 0 {
        f.write_char(digit_at((acc << (5 - bits)) & 31))?;
    }
    Ok(())
}

fn digit_at(value: u32) -> char {
    char::from(ALPHABET[value as usize])
}

/// Decode into `out`, returning how many bytes were written.
fn decode_base32(text: &str, out: &mut [u8]) -> Result<usize, String> {
    let mut acc = 0u32;
    let mut bits = 0u32;
    let mut len = 0;
    for c in text.chars() {
        let value = digit(c).ok_or_else(|| format!("`{c}` is not a base32 digit"))?;
        acc = (acc << 5) | u32::from(value);
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            if len == out.len() {
                return Err("id is longer than any PID".to_string());
            }
            out[len] = (acc >> bits) as u8;
            len += 1;
        }
    }
    // Leftover bits are the encoder's zero padding. Insisting they are zero keeps the mapping
    // injective, so there is exactly one spelling of each PID.
    if acc & ((1 << bits) - 1) != 0 {
        return Err("id has non-zero padding bits".to_string());
    }
    if len > MAX_ALLOCATED && len != HASH_LEN {
        return Err("id is neither an allocated PID nor a content hash".to_string());
    }
    Ok(len)
}

/// Crockford's decoding table: case-insensitive, with `i`/`l` read as `1` and `o` as `0` so a
/// transcription slip is corrected rather than rejected. Hyphens are *not* accepted as visual
/// separators, unlike Crockford's own suggestion, because the hyphen delimits the kind letter.
fn digit(c: char) -> Option<u8> {
    Some(match c.to_ascii_lowercase() {
        '0' | 'o' => 0,
        '1' | 'i' | 'l' => 1,
        c @ '2'..='9' => c as u8 - b'0',
        c @ 'a'..='h' => c as u8 - b'a' + 10,
        'j' => 18,
        'k' => 19,
        'm' => 20,
        'n' => 21,
        'p' => 22,
        'q' => 23,
        'r' => 24,
        's' => 25,
        't' => 26,
        c @ 'v'..='z' => c as u8 - b'v' + 27,
        _ => return None,
    })
}

/// Debug is the canonical text form too. One dialect, so a PID in a panic message is a PID you can
/// paste straight back into the CLI.
impl fmt::Debug for Pid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// Allocates identity PIDs. A distribution seam (SPEC.md §17.2): v1 uses one allocator per process,
/// and adding more requires no coordination because `AllocatorId` disambiguates.
pub trait PidAllocator: Send + Sync {
    fn allocate(&self, kind: PidKind, branch: BranchId) -> Pid;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    const KINDS: [PidKind; 9] = [
        PidKind::Object,
        PidKind::List,
        PidKind::Any,
        PidKind::AnyObject,
        PidKind::AnyArray,
        PidKind::AnyNumber,
        PidKind::String,
        PidKind::Binary,
        PidKind::BigInt,
    ];

    fn allocated(kind: PidKind, branch: u64, allocator: u32, counter: u64) -> Pid {
        Pid::Allocated {
            kind,
            branch: BranchId(branch),
            allocator: AllocatorId(allocator),
            counter,
        }
    }

    fn content(kind: PidKind, seed: u8) -> Pid {
        let mut hash = [0u8; 32];
        for (i, byte) in hash.iter_mut().enumerate() {
            *byte = seed.wrapping_mul(31).wrapping_add(i as u8);
        }
        Pid::Content { kind, hash }
    }

    /// The point of the whole exercise: the text form loses nothing. Every component of every
    /// flavour must survive a round trip, for every kind.
    #[test]
    fn every_pid_round_trips_through_its_text_form() {
        for kind in KINDS {
            for pid in [
                allocated(kind, 0, 0, 0),
                allocated(kind, 1, 0, 1),
                allocated(kind, 7, 3, 100),
                allocated(kind, u64::MAX, u32::MAX, u64::MAX),
                content(kind, 0),
                content(kind, 250),
            ] {
                let text = pid.to_string();
                assert_eq!(
                    Pid::from_str(&text).expect(&text),
                    pid,
                    "round trip of {text}"
                );
            }
        }
    }

    /// The old form carried only the counter, so these four PIDs were indistinguishable. This is
    /// the bug the canonical form exists to fix.
    #[test]
    fn pids_differing_only_outside_the_counter_have_different_text() {
        let texts = [
            allocated(PidKind::Object, 1, 0, 100),
            allocated(PidKind::Object, 2, 0, 100),
            allocated(PidKind::Object, 1, 1, 100),
            allocated(PidKind::List, 1, 0, 100),
        ]
        .map(|pid| pid.to_string());
        for (i, text) in texts.iter().enumerate() {
            assert!(
                !texts[..i].contains(text),
                "{text} collides with an earlier PID"
            );
        }
    }

    /// A content PID carries all 32 bytes, so two hashes differing in the last byte differ in text.
    #[test]
    fn a_content_pid_encodes_its_whole_hash() {
        let a = Pid::Content {
            kind: PidKind::String,
            hash: [0u8; 32],
        };
        let mut tail = [0u8; 32];
        tail[31] = 1;
        let b = Pid::Content {
            kind: PidKind::String,
            hash: tail,
        };
        assert_ne!(a.to_string(), b.to_string());
        assert_eq!(Pid::from_str(&b.to_string()).unwrap(), b);
    }

    /// Crockford base32 is case-insensitive and treats `i`/`l` as `1` and `o` as `0`, so a PID
    /// survives being shouted, typed by hand, or passed through a case-mangling shell.
    #[test]
    fn the_text_form_is_case_insensitive() {
        let pid = allocated(PidKind::Object, 12, 3, 987_654_321);
        let text = pid.to_string();
        assert_eq!(Pid::from_str(&text.to_uppercase()).unwrap(), pid);
    }

    #[test]
    fn malformed_pids_are_rejected_with_the_offending_text() {
        for input in ["", "o", "o-", "-1", "z9-1", "oo-1", "o-1u", "o-!", "o-1-2"] {
            let error = Pid::from_str(input).unwrap_err();
            assert!(
                error.to_string().contains(input),
                "error for `{input}` should quote it back: {error}"
            );
        }
    }
}
