//! Fuzzing entry points.
//!
//! These are `#[doc(hidden)]` and not part of the advertised API. They live in
//! the library (rather than inside the `fuzz/` crate) so the fuzzed logic is
//! compiled and unit-tested by the normal stable build — the cargo-fuzz target
//! under `crates/ratatosk-resp/fuzz/` is only a thin libFuzzer shell that calls
//! [`drain_and_roundtrip`].

use bytes::BytesMut;

use crate::{encode_to_vec, encoded_len, parse};

/// Drive arbitrary bytes through the incremental RESP parser, asserting the
/// invariants the parser/encoder must always uphold:
///
/// 1. Parsing never panics.
/// 2. A successful frame always consumes input — the parser cannot make a
///    `Ok(Some(_))` without advancing, so draining can never infinite-loop.
/// 3. Every parsed frame re-encodes and re-parses to an identical frame — the
///    encoder is a faithful inverse of the parser on the frames it produces.
///
/// On incomplete input (`Ok(None)`) or a protocol error (`Err`) it simply stops
/// draining, exactly as a connection read loop would.
pub fn drain_and_roundtrip(data: &[u8]) {
    let mut buf = BytesMut::with_capacity(data.len());
    buf.extend_from_slice(data);

    loop {
        let before = buf.len();
        match parse(&mut buf) {
            Ok(Some(frame)) => {
                assert!(
                    buf.len() < before,
                    "parser produced a frame without consuming input"
                );

                let mut encoded = Vec::with_capacity(encoded_len(&frame));
                encode_to_vec(&frame, &mut encoded);

                let mut roundtrip = BytesMut::with_capacity(encoded.len());
                roundtrip.extend_from_slice(&encoded);
                match parse(&mut roundtrip) {
                    Ok(Some(reparsed)) => assert_eq!(
                        reparsed, frame,
                        "re-encoding a parsed frame did not round-trip"
                    ),
                    other => panic!("a canonically-encoded frame failed to re-parse: {other:?}"),
                }
            }
            Ok(None) | Err(_) => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::drain_and_roundtrip;

    #[test]
    fn round_trips_canonical_frames_and_drains_multiple() {
        // Canonical RESP2/RESP3 encodings: each must drain + round-trip cleanly.
        for input in [
            "+OK\r\n".as_bytes(),
            "-ERR something\r\n".as_bytes(),
            ":12345\r\n".as_bytes(),
            "$3\r\nfoo\r\n".as_bytes(),
            "$-1\r\n".as_bytes(),
            "*-1\r\n".as_bytes(),
            "*2\r\n$3\r\nfoo\r\n:7\r\n".as_bytes(),
            // Two frames back-to-back exercises the drain loop.
            "+OK\r\n:1\r\n".as_bytes(),
        ] {
            drain_and_roundtrip(input);
        }
    }

    #[test]
    fn malformed_and_partial_input_never_panics() {
        for input in [
            b"".as_slice(),
            b"not resp at all".as_slice(),
            b"$5\r\nab".as_slice(),     // truncated bulk
            b"*3\r\n:1\r\n".as_slice(), // array header promising more than present
            b"\r\n\r\n\r\n".as_slice(),
            b":\r\n".as_slice(), // empty integer
            &[0xff, 0x00, 0x0d, 0x0a],
        ] {
            drain_and_roundtrip(input);
        }
    }
}
