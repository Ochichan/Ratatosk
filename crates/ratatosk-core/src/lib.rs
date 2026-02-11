//! # ratatosk-core
//!
//! Leaf crate for Ratatosk domain types and shared primitives.
//!
//! Provides newtype wrappers ([`ClientId`], [`DbIndex`], [`SlotId`]),
//! bitmask flags ([`CommandFlags`], [`AclCategory`]),
//! a domain error type ([`RedisError`]), and time utilities ([`now_ms`], [`now_sec`]).
//!
//! This crate has no dependencies on any other Ratatosk crate and
//! sits at the bottom of the dependency graph.

#![forbid(unsafe_code)]

pub mod error;
pub mod flags;
pub mod time;
pub mod types;

pub use error::RedisError;
pub use flags::{AclCategory, CommandFlags};
pub use time::{now_ms, now_sec};
pub use types::{ClientId, DbIndex, SlotId};
