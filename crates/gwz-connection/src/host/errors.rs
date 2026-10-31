//! The host's build, closing and attach errors.
use super::ConfigReason;
use crate::FactoryError;
use std::fmt;

/// Why `build` or `cleanup_domain` refused. (`MissingPort` of contract section 11 cannot arise: `HostPorts::new` takes
/// all four.)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum BuildError {
    InvalidConfig(ConfigReason),
    UnavailableProgress,
    DomainClosing,
}
impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(reason) => write!(f, "connection host limits invalid: {reason:?}"),
            Self::UnavailableProgress => f.write_str("the host's progress executor is unavailable"),
            Self::DomainClosing => f.write_str("the cleanup domain is closing"),
        }
    }
}
impl std::error::Error for BuildError {}

/// A handle reached a host that is closing or closed (`HostClosing` of contract section 11).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HostClosing;
impl fmt::Display for HostClosing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("connection host is closing or closed")
    }
}
impl std::error::Error for HostClosing {}

/// Why an instance could not be attached.
#[derive(Debug)]
#[non_exhaustive]
pub enum AttachError {
    Closing(HostClosing),
    InstanceLimit,
    Factory(FactoryError),
}
