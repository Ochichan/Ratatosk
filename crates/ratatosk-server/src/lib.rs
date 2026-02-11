//! # ratatosk-server
//!
//! TCP server and event loop — the "nervous system" of Ratatosk.
//!
//! - [`event_loop`] — Main `tokio::select!` loop: TCP accept, server_cron timer
//!   (active expiry + eviction), signal handling (SIGINT/SIGTERM/SIGUSR1),
//!   and lazy-free background thread management.
//! - [`client`] — Per-client I/O task: read → parse → execute → write pipeline
//!   with query buffer limits, output buffer limits, and pub/sub message drain.
//! - [`config`] — Server configuration from environment variables.
//! - [`io_thread`] — I/O thread pool (placeholder for future parallel I/O).

pub mod client;
pub mod config;
pub mod event_loop;
pub mod io_thread;
