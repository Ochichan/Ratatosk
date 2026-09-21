# RESP parser/encoder fuzzing

cargo-fuzz (libFuzzer) target for `ratatosk-resp`. Operator-run — it needs a
nightly toolchain and is intentionally a **separate workspace** so the normal
stable gates (`cargo build/test --workspace` at the repo root) never touch it.

## What it checks

The target (`resp_parse`) is a thin shell over
`ratatosk_resp::fuzz_support::drain_and_roundtrip`, which asserts, for arbitrary
input bytes:

1. parsing never panics;
2. a successful frame always consumes input (the drain loop cannot hang); and
3. every parsed frame re-encodes and re-parses to an identical frame (the
   encoder is a faithful inverse of the parser).

The parser bounds aggregate nesting (128 levels) and inline line length
(64 KiB), so deeply nested or oversized inputs are expected to stop at a
protocol error rather than exhaust the stack.

The same invariants are unit-tested under the stable build
(`cargo test -p ratatosk-resp fuzz_support`), so this crate only adds the
libFuzzer campaign harness.

## One-time setup

```bash
rustup toolchain install nightly
cargo install cargo-fuzz                 # not installed by default
```

## Run

`cargo fuzz` looks for a `fuzz/` directory next to the package it runs in, so
run it from `crates/ratatosk-resp`, not from the repository root:

```bash
cd crates/ratatosk-resp
cargo +nightly fuzz run resp_parse                       # run until a crash / Ctrl-C
cargo +nightly fuzz run resp_parse -- -max_total_time=600 # bounded 10-minute campaign
```

A finding is written to `fuzz/artifacts/resp_parse/`. Reproduce it with:

```bash
cargo +nightly fuzz run resp_parse fuzz/artifacts/resp_parse/<crash-input>
```

## CI

This is not wired into the stable CI gates by design (libFuzzer needs nightly and
a campaign needs wall-clock time). To add a periodic short campaign, run the
bounded form above on a nightly-toolchain runner on a schedule. See
`docs/RELEASE_ROADMAP.md` Phase 5.
