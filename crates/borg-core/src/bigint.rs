//! Arbitrary-precision integers and their canonical byte encoding. SPEC.md §3.1.
//!
//! A `BigInt` is content-addressed, so its bytes **are** its identity. Two encodings of one number
//! would hash to two PIDs, store twice, and compare unequal — which would defeat the deduplication
//! that is interning's entire purpose. The encoding is therefore pinned, and this module is the only
//! place that produces or consumes it:
//!
//! > **Two's-complement, big-endian, minimal length. The empty slice is zero.**
//!
//! Minimal length is the part that does the work: without it `1` could be spelled `01`, `0001` or
//! any longer run of leading zeros, and every spelling would be a different value.
//!
//! There is no bignum dependency here on purpose. Borg does no arithmetic on bigints — it stores
//! them and hands them back — so the whole requirement is decimal→binary and binary→decimal, two
//! schoolbook loops. A numeric library would be a dependency taken for a pair of conversions.

/// Encode decimal text as the canonical bytes, or `None` if it is not an integer.
///
/// Accepts an optional leading `-`. `0` and `-0` both encode to the empty slice, so zero has exactly
/// one PID.
pub fn encode(decimal: &str) -> Option<Vec<u8>> {
    let (negative, digits) = match decimal.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, decimal),
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }

    // magnitude = magnitude * 10 + digit, big-endian. The carry never exceeds nine, because
    // `255 * 10 + 9` still fits in nine bits above the byte it leaves behind.
    let mut magnitude: Vec<u8> = Vec::new();
    for digit in digits.bytes().map(|b| u32::from(b - b'0')) {
        let mut carry = digit;
        for byte in magnitude.iter_mut().rev() {
            let next = u32::from(*byte) * 10 + carry;
            *byte = next as u8;
            carry = next >> 8;
        }
        if carry > 0 {
            magnitude.insert(0, carry as u8);
        }
    }
    while magnitude.first() == Some(&0) {
        magnitude.remove(0);
    }

    if magnitude.is_empty() {
        return Some(Vec::new());
    }
    if !negative {
        // A leading byte with its top bit set would read back as a negative number.
        if magnitude[0] & 0x80 != 0 {
            magnitude.insert(0, 0);
        }
        return Some(magnitude);
    }

    // Negate over one byte more than the magnitude needs, then drop the sign bytes that carry no
    // information. Padding *first* is what stops -255 collapsing: complementing `ff` alone gives
    // `01`, which is +1, whereas complementing `00 ff` gives `ff 01`, which is right.
    magnitude.insert(0, 0);
    for byte in &mut magnitude {
        *byte = !*byte;
    }
    increment(&mut magnitude);
    while magnitude.len() > 1 && magnitude[0] == 0xff && magnitude[1] & 0x80 != 0 {
        magnitude.remove(0);
    }
    Some(magnitude)
}

/// Decode canonical bytes back to decimal text. Total: any byte string names some integer.
pub fn decode(bytes: &[u8]) -> String {
    if bytes.iter().all(|byte| *byte == 0) {
        return "0".to_string();
    }
    let negative = bytes[0] & 0x80 != 0;
    let mut value = bytes.to_vec();
    if negative {
        for byte in &mut value {
            *byte = !*byte;
        }
        increment(&mut value);
    }

    // Repeated division by ten, most significant byte first, emitting one digit per pass.
    let mut digits = Vec::new();
    while value.iter().any(|byte| *byte != 0) {
        let mut remainder = 0u32;
        for byte in &mut value {
            let current = (remainder << 8) | u32::from(*byte);
            *byte = (current / 10) as u8;
            remainder = current % 10;
        }
        digits.push(b'0' + remainder as u8);
    }
    if negative {
        digits.push(b'-');
    }
    digits.reverse();
    String::from_utf8(digits).expect("digits and a sign are ASCII")
}

/// Add one, big-endian, discarding a carry out of the top. The caller has already sized the buffer
/// so that no carry escapes.
fn increment(bytes: &mut [u8]) {
    for byte in bytes.iter_mut().rev() {
        *byte = byte.wrapping_add(1);
        if *byte != 0 {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The encoding is persisted and content-addressed, so every value has to survive the round trip
    /// unchanged — including the boundaries where the sign byte appears and disappears.
    #[test]
    fn every_integer_round_trips_through_its_canonical_bytes() {
        for decimal in [
            "0",
            "1",
            "-1",
            "127",
            "128",
            "255",
            "256",
            "-127",
            "-128",
            "-129",
            "-255",
            "-256",
            "9223372036854775807",
            "-9223372036854775808",
            "170141183460469231731687303715884105728",
            "-170141183460469231731687303715884105728",
            "99999999999999999999999999999999999999999999999999",
        ] {
            let bytes = encode(decimal).unwrap_or_else(|| panic!("{decimal} should encode"));
            assert_eq!(decode(&bytes), decimal, "round trip of {decimal}");
        }
    }

    /// Interning deduplicates by bytes, so one number must have exactly one encoding — otherwise
    /// `-0` and `0` would be two values with two PIDs.
    #[test]
    fn one_number_has_exactly_one_encoding() {
        assert_eq!(encode("0").unwrap(), Vec::<u8>::new());
        assert_eq!(encode("-0").unwrap(), Vec::<u8>::new());
        assert_eq!(encode("1").unwrap(), vec![1]);
        assert_eq!(encode("-1").unwrap(), vec![0xff]);
        // Minimal length: no leading `00` on a positive value that does not need one, and no
        // leading `ff` on a negative one.
        assert_eq!(encode("128").unwrap(), vec![0x00, 0x80]);
        assert_eq!(encode("-128").unwrap(), vec![0x80]);
        assert_eq!(encode("-255").unwrap(), vec![0xff, 0x01]);
    }

    /// Two's complement, so the sign is the top bit of the first byte and nothing else.
    #[test]
    fn the_sign_is_the_top_bit_of_the_first_byte() {
        assert!(encode("255").unwrap()[0] & 0x80 == 0);
        assert!(encode("-1").unwrap()[0] & 0x80 != 0);
        assert_eq!(decode(&[]), "0");
        assert_eq!(decode(&[0xff]), "-1");
    }

    #[test]
    fn text_that_is_not_an_integer_does_not_encode() {
        for input in ["", "-", "1.5", "1n", "abc", "1 2", "+1", " 1"] {
            assert!(encode(input).is_none(), "{input} should not encode");
        }
    }
}
