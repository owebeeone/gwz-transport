//! Admission before allocating the generic CBOR tree or copying typed values.
use crate::{cbor, protocol::Envelope};
use std::fmt;

mod preflight;
mod validate;

pub const MAX_FRAME: usize = 128 * 1024;
pub const MAX_BOOTSTRAP: usize = 64 * 1024;
pub const MAX_DATA: usize = 64 * 1024;
pub const MAX_METADATA: usize = 16 * 1024;
pub const MAX_DEPTH: usize = 16;
pub const MAX_ENTRIES: usize = 256;
pub const MAX_ALLOCATION: usize = 512 * 1024;
pub const MAX_BOOTSTRAP_ALLOCATION: usize = 256 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    FrameTooLarge,
    Bounds,
    InvalidMessage,
    UnsupportedVersion,
    Malformed,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "transport protocol: {self:?}")
    }
}
impl std::error::Error for Error {}

/// Local admission must run before generated to_cbor clones fields.
pub fn admit(message: &Envelope) -> Result<(), Error> {
    admit_limited(message, &crate::binding::default_limits())
}

pub fn admit_limited(message: &Envelope, limits: &crate::protocol::Limits) -> Result<(), Error> {
    validate_limits(limits)?;
    validate::envelope(message)?;
    crate::admission::envelope(message, limits)
}

pub(crate) use validate::limits as validate_limits;

pub fn encode(message: &Envelope) -> Result<Vec<u8>, Error> {
    admit(message)?;
    let bytes = cbor::encode(&message.to_cbor());
    preflight::check(&bytes, &crate::binding::default_limits())?;
    check_bootstrap(message, bytes.len())?;
    Ok(bytes)
}

/// Check lengths, recursion and allocation budgets before the allocating decoder.
pub fn decode(bytes: &[u8]) -> Result<Envelope, Error> {
    decode_limited(bytes, &crate::binding::default_limits())
}

pub fn decode_limited(bytes: &[u8], limits: &crate::protocol::Limits) -> Result<Envelope, Error> {
    validate_limits(limits)?;
    preflight::check(bytes, limits)?;
    let tree = cbor::try_decode(bytes).map_err(|_| Error::Malformed)?;
    let message = Envelope::from_cbor(&tree).map_err(|_| Error::Malformed)?;
    validate::envelope(&message)?;
    check_bootstrap(&message, bytes.len())?;
    Ok(message)
}

fn check_bootstrap(message: &Envelope, length: usize) -> Result<(), Error> {
    if message.stream_id == 0 && length > MAX_BOOTSTRAP {
        return Err(Error::FrameTooLarge);
    }
    Ok(())
}

pub fn encode_limited(
    message: &Envelope,
    limits: &crate::protocol::Limits,
) -> Result<Vec<u8>, Error> {
    validate_limits(limits)?;
    admit_limited(message, limits)?;
    let bytes = encode(message)?;
    preflight::check(&bytes, limits)?;
    Ok(bytes)
}
