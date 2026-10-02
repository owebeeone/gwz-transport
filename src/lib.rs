#![doc = include_str!("../README.md")]
//! Shared taut protocol and bounded streams over host-delivered discrete messages.
//! No physical I/O, network adapter or executor is implemented here.
extern crate alloc;

#[allow(clippy::collapsible_if)] // Pinned upstream codec; do not hand-edit.
pub mod cbor;
#[allow(clippy::needless_question_mark)] // Pinned taut 0.10.0 output.
pub mod protocol;

/// The canonical authored schema, shipped with the package for other consumers.
pub const SCHEMA_SOURCE: &str = include_str!("../protocol/transport.taut.py");

mod admission;
pub mod binding;
mod budget;
pub mod codec;
mod policy;
pub mod pool;
// Only with the unstable-sequenced feature: the module's own `#![cfg]` bounds it.
pub mod sequenced;
pub mod stream;

pub mod mux;
