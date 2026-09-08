//! Shared-memory transport for Ratatosk.
//!
//! The transport carries plain RESP bytes through two single-producer /
//! single-consumer byte rings that live in an anonymous shared-memory segment
//! (Linux `memfd`, macOS `shm_open` + immediate `shm_unlink`). The segment file
//! descriptor is handed to the peer over a Unix domain socket with
//! `SCM_RIGHTS`; that same socket afterwards serves as the doorbell and as the
//! peer-death signal (EOF).
//!
//! # Trust model
//!
//! The shared region is treated as hostile input at all times:
//!
//! * each side keeps a private copy of the index it owns and never re-reads it
//!   from shared memory;
//! * the index owned by the peer is validated on every observation
//!   (`peer_index.wrapping_sub(own_index) > capacity` ⇒ corrupt ⇒ close);
//! * payload bytes are copied out with atomic byte loads into a private buffer
//!   before anything parses them — nothing is ever parsed in place;
//! * ring indices are always masked before use, so no arithmetic performed on
//!   peer-controlled values can produce an out-of-bounds access.
//!
//! `unsafe` is confined to [`segment`] (mapping / atomic views) and
//! [`fdpass`] (`sendmsg` / `recvmsg` with ancillary data).
//!
//! The ring algorithm itself ([`ring`]) is sans-IO and generic over the atomic
//! storage, which is what allows it to be model-checked with `loom`
//! (`RUSTFLAGS="--cfg loom" cargo test -p ratatosk-shm --release`).

#![cfg_attr(not(unix), allow(dead_code))]

pub mod layout;
pub mod ring;

#[cfg(all(unix, not(loom)))]
pub mod fdpass;
#[cfg(all(unix, not(loom)))]
pub mod handshake;
#[cfg(all(unix, not(loom)))]
pub mod segment;
#[cfg(all(unix, not(loom)))]
pub mod stream;

#[cfg(all(unix, not(loom)))]
pub use handshake::{ClientConfig, ServerConfig, accept_shm_session, connect_shm};
#[cfg(all(unix, not(loom)))]
pub use stream::ShmStream;

/// Atomic primitives, swapped for `loom` models under `--cfg loom`.
pub(crate) mod sync {
    #[cfg(loom)]
    pub(crate) use loom::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering, fence};
    #[cfg(not(loom))]
    pub(crate) use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering, fence};
}
