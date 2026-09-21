//! Shared taut protocol and bounded streams over host-delivered discrete messages.
//! No physical I/O, network adapter or executor is implemented here.
extern crate alloc;

#[allow(clippy::collapsible_if)] // Pinned upstream codec; do not hand-edit.
pub mod cbor;
#[allow(clippy::needless_question_mark)] // Pinned taut 0.9.1 output.
pub mod protocol;

/// The canonical authored schema, shipped with the package for other consumers.
pub const SCHEMA_SOURCE: &str = include_str!("../protocol/transport.taut.py");

mod admission;
pub mod binding;
mod budget;
pub mod codec;
mod policy;
pub mod pool;
pub mod stream;

pub mod mux;
