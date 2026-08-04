//! Content addressing — the identity of interned values. SPEC.md §3.1.
//!
//! `String`, `Binary` and `BigInt` have no allocated identity: their PID *is* the hash of their
//! content. Two consequences do a great deal of work elsewhere in the system:
//!
//! * **No coordination.** Any node computes the same PID for the same bytes, so interning needs no
//!   allocator, no lock and no round trip (SPEC.md §17.2).
//! * **Branch-independent and eternal.** `"hello"` has the same PID on every branch, forever — so a
//!   string write can never conflict across branches, and interning storage has no branch column at
//!   all.
//!
//! The hash is SHA-256 of the value's bytes and nothing else — no length prefix, no kind tag. The
//! kind is a field of `Pid::Content` in its own right, so `String("x")` and `Binary("x")` are
//! already distinct PIDs without paying for domain separation in the preimage. Keeping the preimage
//! exactly the value's bytes means the PID of a file is `sha256sum` of that file, which is worth
//! more than a prefix no reader can see anyway.
//!
//! This is a **persisted format**, fixed now rather than retrofitted (SPEC.md §3.1): changing the
//! hash function or the preimage renames every interned value that was ever stored.

use crate::error::{BorgError, Result};
use crate::pid::{Pid, PidKind};
use sha2::{Digest, Sha256};

/// The content hash of a value's canonical bytes.
pub fn hash(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

/// The content-addressed PID of a value, given its canonical byte encoding.
///
/// The encoding is the caller's — UTF-8 for `String`, the octets themselves for `Binary` — but it
/// must be *canonical*. Two encodings of one value would intern as two values and defeat the
/// registry-wide deduplication that is interning's entire purpose.
pub fn pid(kind: PidKind, bytes: &[u8]) -> Result<Pid> {
    if !kind.is_content_addressed() {
        return Err(BorgError::NotContentAddressed { kind });
    }
    Ok(Pid::Content {
        kind,
        hash: hash(bytes),
    })
}

/// The hash inside a content-addressed PID.
///
/// An allocated PID is rejected rather than answered with a miss: it is a caller bug, and an
/// interning store has no row it could even look for.
pub fn hash_of(pid: &Pid) -> Result<[u8; 32]> {
    match pid {
        Pid::Content { hash, .. } => Ok(*hash),
        Pid::Allocated { kind, .. } => Err(BorgError::NotContentAddressed { kind: *kind }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{AllocatorId, BranchId};

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn equal_content_has_one_pid_however_it_was_produced() {
        let once = pid(PidKind::String, b"hello").unwrap();
        let again = pid(PidKind::String, "hello".to_string().as_bytes()).unwrap();
        assert_eq!(once, again);
    }

    #[test]
    fn different_content_has_different_pids() {
        assert_ne!(
            pid(PidKind::String, b"hello").unwrap(),
            pid(PidKind::String, b"world").unwrap()
        );
    }

    #[test]
    fn one_preimage_under_two_kinds_is_two_values() {
        // The kind lives in the PID rather than in the preimage, which is what makes the hash
        // reproducible outside Borg while still keeping these apart.
        let text = pid(PidKind::String, b"x").unwrap();
        let blob = pid(PidKind::Binary, b"x").unwrap();
        assert_ne!(text, blob);
        assert_eq!(hash_of(&text).unwrap(), hash_of(&blob).unwrap());
    }

    #[test]
    fn the_hash_is_plain_sha256_of_the_bytes() {
        // Pins the persisted format. `printf 'hello' | sha256sum` must agree with this, forever.
        assert_eq!(
            hex(&hash(b"hello")),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        assert_eq!(
            hex(&hash(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn an_allocated_kind_has_no_content_address() {
        assert!(matches!(
            pid(PidKind::Object, b"hello"),
            Err(BorgError::NotContentAddressed { .. })
        ));
        assert!(matches!(
            hash_of(&Pid::Allocated {
                kind: PidKind::Object,
                branch: BranchId(1),
                allocator: AllocatorId(0),
                counter: 7,
            }),
            Err(BorgError::NotContentAddressed { .. })
        ));
    }
}
