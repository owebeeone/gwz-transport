//! Endpoint-owned capacity and exclusive leases. Hosts execute commands outside
//! the pool and acknowledge actual connector/cleanup completion. No physical I/O.
use crate::protocol::{Effect, ErrorCode, Scheme};
use std::fmt;

mod allocation;
mod asynchronous;
mod clock;
mod lifecycle;
mod machine;
pub use asynchronous::{Checkout, Lease, Pool, PoolDriver};
pub use machine::PoolMachine;

#[derive(Clone, Debug)]
pub struct Config {
    pub per_key: usize,
    pub per_host: usize,
    pub total: usize,
    pub max_requests: usize,
    pub idle_timeout_ms: u64,
    pub allocation_timeout_ms: u64,
    pub connect_timeout_ms: u64,
    pub interaction_timeout_ms: u64,
    pub cleanup_timeout_ms: u64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            per_key: 8,
            per_host: 8,
            total: 256,
            max_requests: 1024,
            idle_timeout_ms: 60_000,
            allocation_timeout_ms: 30_000,
            connect_timeout_ms: 10_000,
            interaction_timeout_ms: 120_000,
            cleanup_timeout_ms: 5_000,
        }
    }
}
impl Config {
    fn validate(&self) -> Result<(), Error> {
        if [self.per_key, self.per_host, self.total]
            .iter()
            .any(|value| !(1..=4096).contains(value))
            || !(1..=16384).contains(&self.max_requests)
            || [
                self.idle_timeout_ms,
                self.allocation_timeout_ms,
                self.connect_timeout_ms,
                self.interaction_timeout_ms,
                self.cleanup_timeout_ms,
            ]
            .iter()
            .any(|value| !(1..=86_400_000).contains(value))
        {
            return Err(Error::InvalidConfig);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Key {
    pub scheme: Scheme,
    pub username: Option<String>,
    pub host: String,
    pub port: u16,
}
impl Key {
    pub fn ssh(username: impl Into<String>, host: impl Into<String>, port: u16) -> Self {
        Self {
            scheme: Scheme::Ssh,
            username: Some(username.into()),
            host: host.into(),
            port,
        }
    }
    pub fn https(host: impl Into<String>, port: u16) -> Self {
        Self {
            scheme: Scheme::Https,
            username: None,
            host: host.into(),
            port,
        }
    }
    fn valid(&self) -> bool {
        !self.host.is_empty()
            && self.host.len() <= 255
            && self.port != 0
            && !self
                .host
                .chars()
                .any(|c| c.is_control() || c.is_whitespace() || "@/?#\\".contains(c))
            && match self.scheme {
                Scheme::Ssh => self.username.as_ref().is_some_and(|s| bounded_text(s, 128)),
                Scheme::Https => self.username.is_none(),
            }
    }
}
fn bounded_text(text: &str, max: usize) -> bool {
    !text.is_empty() && text.len() <= max && !text.chars().any(char::is_control)
}

/// Explicit proofs must be resolved and validated by the endpoint for each
/// request. Never put a private key or its filename here. HTTPS reuse carries
/// no account claim: authentication is independently applied to every request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Identity {
    Ambient,
    Explicit(String),
    Https,
}
impl Identity {
    fn valid_for(&self, key: &Key) -> bool {
        match (key.scheme, self) {
            (Scheme::Ssh, Self::Ambient) | (Scheme::Https, Self::Https) => true,
            (Scheme::Ssh, Self::Explicit(proof)) => bounded_text(proof, 1024),
            _ => false,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Request {
    pub key: Key,
    pub identity: Identity,
    /// Operation/carrier ownership; losing it must not invalidate idle entries.
    pub owner: String,
    pub allocation_timeout_ms: Option<u64>,
    pub connect_timeout_ms: Option<u64>,
    pub interaction_timeout_ms: Option<u64>,
}
impl Request {
    pub fn new(key: Key, identity: Identity, owner: impl Into<String>) -> Self {
        Self {
            key,
            identity,
            owner: owner.into(),
            allocation_timeout_ms: None,
            connect_timeout_ms: None,
            interaction_timeout_ms: None,
        }
    }
    fn valid(&self, config: &Config) -> bool {
        self.key.valid()
            && self.identity.valid_for(&self.key)
            && bounded_text(&self.owner, 128)
            && [
                (self.allocation_timeout_ms, config.allocation_timeout_ms),
                (self.connect_timeout_ms, config.connect_timeout_ms),
                (self.interaction_timeout_ms, config.interaction_timeout_ms),
            ]
            .iter()
            .all(|(value, max)| value.is_none_or(|v| v > 0 && v <= *max))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConnectionId {
    pool: u64,
    serial: u64,
}
impl ConnectionId {
    /// For diagnostics/replay only; routing and comparisons must use the full ID.
    pub fn sequence(self) -> u64 {
        self.serial
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequestId {
    pool: u64,
    serial: u64,
}
impl RequestId {
    pub fn sequence(self) -> u64 {
        self.serial
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LeaseId {
    connection: ConnectionId,
    generation: u64,
}
impl LeaseId {
    pub fn connection(self) -> ConnectionId {
        self.connection
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Error {
    InvalidConfig,
    InvalidRequest,
    Capacity,
    WouldBlock,
    Cancelled,
    Shutdown,
    DriverLost,
    Stale,
    WrongState,
    AllocationTimeout,
    ConnectTimeout,
    InteractionTimeout,
    IdentityMismatch,
    ConnectFailed { code: ErrorCode, effect: Effect },
}
impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "connection pool: {self:?}")
    }
}
impl std::error::Error for Error {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CloseReason {
    IdleExpired,
    Evicted,
    Discarded,
    Unproven,
    Cancelled,
    Shutdown,
    IdentityMismatch,
    DriverLost,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    Connect {
        connection: ConnectionId,
        key: Key,
        identity: Identity,
        network_deadline: u64,
    },
    CancelConnect {
        connection: ConnectionId,
        deadline: u64,
    },
    AbortConnect {
        connection: ConnectionId,
    },
    Close {
        connection: ConnectionId,
        reason: CloseReason,
        deadline: u64,
    },
    Abort {
        connection: ConnectionId,
    },
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    pub opening: usize,
    pub idle: usize,
    pub leased: usize,
    pub closing: usize,
}
impl Counts {
    pub fn total(self) -> usize {
        self.opening + self.idle + self.leased + self.closing
    }
}
