# Shared-memory transport (`ratatosk-shm`) — experimental

Status: **experimental, opt-in, Unix only.** Ratatosk's supported transports are
TCP and Unix domain sockets. The shared-memory transport exists to answer one
question with measurements rather than claims: how much latency does removing the
kernel from the data path actually buy on a given host, and at what CPU cost. It
is not part of the product contract until the IPC benchmark (see
[ipc-benchmark.md](ipc-benchmark.md)) shows a need and the gate below stays green
on both Linux and macOS.

## What it is

A byte stream (`AsyncRead + AsyncWrite`) carrying ordinary RESP. The server's
session loop is transport-generic, so commands, ACL, Pub/Sub, MONITOR and
`CLIENT LIST` behave identically over TCP, UDS and shared memory. There is no
separate protocol and no zero-copy of values between processes: bytes are copied
into and out of two single-producer/single-consumer rings.

```text
client process                                server process
  connect(shm-socket)  ── HELLO ──────────▶  accept, peer-cred check
                       ◀── READY + fd ───    memfd / shm_open(+unlink) segment
  mmap(fd)                                    mmap(fd)
  ┌────── c2s ring ──────▶ (RESP request bytes)
  ◀────── s2c ring ────── (RESP reply bytes)
  control socket: 1-byte doorbells both ways; EOF = peer gone
```

## Trust model

The shared region is treated as hostile at all times:

- each side keeps a **private copy** of the index it owns and never re-reads it
  from shared memory;
- the peer-owned index is validated on every observation
  (`peer_index - own_index > capacity` ⇒ corrupt ⇒ connection closed);
- payload bytes are copied out with atomic loads into private memory **before**
  parsing; nothing is parsed in place;
- ring indices are always masked, so no value from shared memory can index
  outside the mapping;
- the server refuses peers whose uid differs from its own (`require_same_uid`);
- the segment is anonymous (Linux `memfd_create` + size seals, macOS `shm_open`
  followed immediately by `shm_unlink`), so a crash on either side leaves nothing
  in a global namespace; the kernel reclaims it when the last mapping goes away.

`unsafe` is confined to `segment.rs` (mmap and atomic views) and `fdpass.rs`
(`sendmsg`/`recvmsg` with `SCM_RIGHTS`), plus one `send(2)` for the doorbell.
Every block carries a `// SAFETY:` comment. The workspace rule that only
`ratatosk-server` may use `unsafe` is amended by this crate; see
`docs/ai/CONVENTIONS.md`.

## Wake-ups

Waiting is hybrid: spin `spin_iters` times on the ring index, then arm a parked
flag and sleep on the control socket. The parked handshake is Dekker-style
(`SeqCst` fences on both sides) so a wake-up can be coalesced but never lost;
`tests/loom.rs` model-checks this exhaustively:

```bash
RUSTFLAGS="--cfg loom" cargo test -p ratatosk-shm --release --test loom
```

The server always uses a small spin budget (default 2000 iterations) because a
spinning `poll_*` occupies a tokio worker shared with cron and AOF fsync. Spin-only
behaviour is a client-side choice and shows up as CPU in the benchmark.

A `ShmStream` must be driven from one task: both directions wait on the same
readable control socket, and tokio keeps a single reader waker per socket. The
server session loop already satisfies this; do not `split` the stream.

## Doorbell implementation note

Doorbells are sent with a direct non-blocking `send(2)` rather than tokio's
`try_write`. `try_write` consults tokio's cached writable-readiness and returns
`WouldBlock` without a syscall if the socket has never been polled for
writability, which would silently drop the first wake-up and leave the peer
parked forever. The direct syscall has no such dependency.

## Tests

| Test | What it proves |
| --- | --- |
| `ring::tests` | wrap-around, full/empty parking, hostile indices rejected, masked indices |
| `segment::tests` | create/reopen aliasing, corrupted header rejected on reopen |
| `fdpass::tests` | a descriptor crosses a socketpair; plain messages carry none |
| `stream::tests` | round trip, 200 KiB through a 4 KiB ring (backpressure), peer drop ⇒ EOF then `BrokenPipe`, corrupt index ⇒ `InvalidData`, `peer_closed` ignores doorbells |
| `handshake::tests` | end-to-end over a real socket; bad HELLO rejected |
| `tests/hostile.rs` | 40 rounds of random scribbling over control words and data while an echo server runs: server never panics or hangs |
| `tests/loom.rs` | no lost wake-up / deadlock in the parking handshake |

## Limits

- Unix only (Linux, macOS). No Windows.
- One producer and one consumer per direction; the transport is per connection.
- Byte-wise atomic copies: roughly 1 byte per cycle, so a 1 KiB reply costs on
  the order of 0.3 µs of copy time per side. Acceptable for an experiment; a
  word-wise copy is the obvious next optimisation if the benchmark justifies it.
- No zero-copy value sharing and no change to Pub/Sub's at-most-once delivery.
