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

The same invariants are unit-tested under the stable build
(`cargo test -p ratatosk-resp fuzz_support`), so this crate only adds the
libFuzzer campaign harness.

## One-time setup

```bash
rustup toolchain install nightly        # already present on this host
cargo install cargo-fuzz                 # not installed by default
```

## Run

```bash
# from the repo root
cargo +nightly fuzz run resp_parse                       # run until a crash / Ctrl-C
cargo +nightly fuzz run resp_parse -- -max_total_time=600 # bounded 10-minute campaign
```

A finding is written to `crates/ratatosk-resp/fuzz/artifacts/resp_parse/`.
Reproduce it with:

```bash
cargo +nightly fuzz run resp_parse crates/ratatosk-resp/fuzz/artifacts/resp_parse/<crash-input>
```

## CI

This is not wired into the stable CI gates by design (libFuzzer needs nightly and
a campaign needs wall-clock time). To add a periodic short campaign, run the
bounded form above on a nightly-toolchain runner on a schedule. See
`docs/RELEASE_ROADMAP.md` Phase 5 and `docs/GA_MANUAL_RUNBOOK.md`.
