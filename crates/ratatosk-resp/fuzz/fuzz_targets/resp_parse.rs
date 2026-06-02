#![no_main]

//! libFuzzer target for the RESP parser/encoder.
//!
//! The real logic and its invariants live in (and are unit-tested by)
//! `ratatosk_resp::fuzz_support::drain_and_roundtrip`, so this target is just a
//! thin shell that feeds the fuzzer's bytes into it.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    ratatosk_resp::fuzz_support::drain_and_roundtrip(data);
});
