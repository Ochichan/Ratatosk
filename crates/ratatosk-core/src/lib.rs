//! # ratatosk-core
//!
//! Leaf crate for primitives shared by every Ratatosk layer.
//!
//! It currently provides the clocks ([`now_ms`], [`now_sec`],
//! [`time::monotonic_ms`]) and [`time::with_command_time`], which pins the
//! wall clock seen by one synchronous command execution so AOF replay reruns a
//! command at its recorded time.
//!
//! This crate has no dependencies on any other Ratatosk crate and
//! sits at the bottom of the dependency graph.

#![forbid(unsafe_code)]

pub mod time;

pub use time::{now_ms, now_sec};
