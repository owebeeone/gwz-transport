//! The frozen inputs of an operation and the vocabulary of its life (contract sections 4, 5, 7 and 11).
//!
//! `PreparedOperation` is what the composition builds at the caller's entry and hands to `admit`: an explicit route, a
//! validated profile, an opaque bounded authentication context, the operation's policy. This module defines the types
//! and their validation; capturing them (the environment, the home, the agent) is core's, in step 1.5.
use crate::{BindingId, CleanupStatus, FactoryError};
use gwz_transport::{
    binding,
    mux::{self, Mux},
    pool::{Identity, Key},
    protocol::{AuthPolicy, Effect, EndpointRole, Scheme},
};
use std::{any::Any, fmt, sync::Arc};

/// An input was refused at construction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum InvalidInput {
    Operation,
    Route,
    Profile,
    Policy,
    Spec,
}
impl fmt::Display for InvalidInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid operation input: {self:?}")
    }
}
impl std::error::Error for InvalidInput {}

/// Which schemes and authentication policies the operation's endpoint side offers.
#[derive(Clone, Debug, PartialEq)]
pub struct EndpointRoute {
    pub(crate) schemes: Vec<Scheme>,
    pub(crate) policies: Vec<AuthPolicy>,
}
impl EndpointRoute {
    /// A route the transport accepts: it is tried against a throwaway endpoint session.
    pub fn new(schemes: Vec<Scheme>, policies: Vec<AuthPolicy>) -> Result<Self, InvalidInput> {
        let route = Self { schemes, policies };
        Mux::endpoint(
            "probe",
            route.endpoint_config("probe"),
            mux::Config::default(),
        )
        .map_err(|_| InvalidInput::Route)?;
        Ok(route)
    }
    /// Both schemes with every policy a Git endpoint offers.
    pub fn ssh_and_https() -> Self {
        Self {
            schemes: vec![Scheme::Ssh, Scheme::Https],
            policies: vec![
                AuthPolicy::SshAmbient,
                AuthPolicy::SshExplicit,
                AuthPolicy::Anonymous,
                AuthPolicy::Gh,
                AuthPolicy::WindowsConfigured,
                AuthPolicy::WindowsDefault,
            ],
        }
    }
    pub(crate) fn endpoint_config(&self, name: &str) -> binding::EndpointConfig {
        binding::EndpointConfig {
            endpoint_id: format!("endpoint-{name}"),
            trust_owner: format!("endpoint-{name}"),
            role: EndpointRole::Driver,
            schemes: self.schemes.clone(),
            policies: self.policies.clone(),
            limits: crate::binding::limits(),
        }
    }
}

/// The nonsecret canonical bytes of an endpoint profile and their digest. Equality compares the bytes after the digest
/// matches: a digest collision is not equality. It is never logged or serialized, and its `Debug` shows neither.
#[derive(Clone, PartialEq, Eq)]
pub struct EndpointProfile {
    digest: [u8; 32],
    canonical: Vec<u8>,
}
/// The longest canonical profile accepted.
const MAX_PROFILE_BYTES: usize = 16 * 1024;
impl EndpointProfile {
    pub fn new(canonical: Vec<u8>, digest: [u8; 32]) -> Result<Self, InvalidInput> {
        if canonical.is_empty() || canonical.len() > MAX_PROFILE_BYTES {
            return Err(InvalidInput::Profile);
        }
        Ok(Self { digest, canonical })
    }
    pub fn digest(&self) -> &[u8; 32] {
        &self.digest
    }
    /// The same profile: digest first, then the bytes.
    pub fn same_as(&self, other: &Self) -> bool {
        self.digest == other.digest && self.canonical == other.canonical
    }
}
impl fmt::Debug for EndpointProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("EndpointProfile(..)")
    }
}

/// An opaque, bounded capability captured on the original caller entry. The host never looks inside it: it counts its
/// charge against the admission budget and passes it to the adapters.
#[derive(Clone)]
pub struct AuthContext {
    charge: usize,
    provider: Arc<dyn Any + Send + Sync>,
}
impl AuthContext {
    /// `charge` is the most the provider retains for as long as this context lives, in bytes.
    pub fn new(charge: usize, provider: Arc<dyn Any + Send + Sync>) -> Self {
        Self { charge, provider }
    }
    pub fn charge(&self) -> usize {
        self.charge
    }
    pub fn provider(&self) -> &Arc<dyn Any + Send + Sync> {
        &self.provider
    }
}
impl fmt::Debug for AuthContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AuthContext(..)")
    }
}

/// Capacity and retry inputs of one operation (carried now, applied per binding in step 1.4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OperationPolicy {
    pub concurrency: usize,
    pub max_connections_per_host: usize,
    pub max_retries: u32,
}
impl OperationPolicy {
    pub fn new(
        concurrency: usize,
        max_connections_per_host: usize,
        max_retries: u32,
    ) -> Result<Self, InvalidInput> {
        if concurrency == 0
            || concurrency > 4096
            || max_connections_per_host == 0
            || max_connections_per_host > 4096
        {
            return Err(InvalidInput::Policy);
        }
        Ok(Self {
            concurrency,
            max_connections_per_host,
            max_retries,
        })
    }
}

/// Everything an operation is, frozen: no other application state reaches the host.
#[derive(Clone, Debug)]
pub struct PreparedOperation {
    pub(crate) operation: String,
    pub(crate) route: EndpointRoute,
    pub(crate) profile: EndpointProfile,
    pub(crate) auth: AuthContext,
    #[allow(dead_code, reason = "carried now; the binding applies it in step 1.4")]
    pub(crate) policy: OperationPolicy,
}
impl PreparedOperation {
    /// `operation` names the operation to its binding: non-empty, at most 128 characters, no control character.
    pub fn new(
        operation: &str,
        route: EndpointRoute,
        profile: EndpointProfile,
        auth: AuthContext,
        policy: OperationPolicy,
    ) -> Result<Self, InvalidInput> {
        if operation.is_empty() || operation.len() > 96 || operation.chars().any(char::is_control) {
            return Err(InvalidInput::Operation);
        }
        Ok(Self {
            operation: operation.into(),
            route,
            profile,
            auth,
            policy,
        })
    }
}

/// What an exchange connects to: the pool's key and identity.
#[derive(Clone, Debug)]
pub struct ExchangeSpec {
    pub(crate) key: Key,
    pub(crate) identity: Identity,
}
impl ExchangeSpec {
    pub fn new(key: Key, identity: Identity) -> Self {
        Self { key, identity }
    }
}

/// Why a terminal (never registered) operation, or a closing host, refuses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum TerminalReason {
    HostClosing,
    HostFault,
    DomainClosing,
}
/// Whether an operation became a binding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Registration {
    Registered(BindingId),
    NeverRegistered(TerminalReason),
}

/// Why `admit` or `acquire` refused.
#[derive(Debug)]
#[non_exhaustive]
pub enum AdmissionError {
    QueueFull,
    AdmissionTimeout,
    Closing(TerminalReason),
    IdentifierExhausted,
    UnavailableAdapter(FactoryError),
    /// The host holds its limit of endpoint instances and none matches this profile.
    InstanceLimit,
    InputBudget,
}
impl fmt::Display for AdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "admission refused: {self:?}")
    }
}
impl std::error::Error for AdmissionError {}

/// Why `Operation::open` refused at once.
#[derive(Debug, PartialEq)]
#[non_exhaustive]
pub enum OpenError {
    /// The operation never became a binding: the reason, and that nothing was done.
    Terminal {
        reason: TerminalReason,
        effect: Effect,
    },
    Cancelled,
    Sealed,
    /// The binding's own registration limit.
    BindingLimit,
}
/// Why an opening ended without an exchange.
#[derive(Debug)]
#[non_exhaustive]
pub enum OpenFailure {
    Cancelled,
    Sealed,
    Closing(TerminalReason),
    Pool(gwz_transport::pool::Error),
    Io(std::io::Error),
}

/// What `OperationCancel::cancel` found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CancelState {
    Cancelled,
    AlreadyCancelled,
    /// The operation is finished or gone.
    Gone,
}
/// An operation's terminal receipt: what it was and what became of its cleanup.
#[derive(Clone, Debug)]
pub struct OperationEnd {
    pub registration: Registration,
    pub cleanup: CleanupStatus,
    pub exchanges_opened: u64,
    pub cancelled: bool,
}
/// An exchange's terminal receipt.
#[derive(Clone, Debug)]
pub struct ExchangeEnd {
    pub disposition: gwz_transport::protocol::Disposition,
    pub cleanup: CleanupStatus,
}
