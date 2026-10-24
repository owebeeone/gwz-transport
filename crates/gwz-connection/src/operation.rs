//! The frozen inputs of an operation and the vocabulary of its life (contract sections 4, 5, 7 and 11).
//!
//! `PreparedOperation` is what the composition builds at the caller's entry and hands to `admit`: an explicit route, a
//! validated profile, an opaque bounded authentication context, the operation's policy. This module defines the types
//! and their validation; capturing them (the environment, the home, the agent) is core's
//! (`transport_host/endpoint_environment.rs`, step 1.5).
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

/// The nonsecret canonical bytes of an endpoint profile and their digest, and the validated inputs an endpoint
/// adapter is made from (contract sections 4.1 and 7). Equality compares the bytes after the digest matches: a digest
/// collision is not equality, and the inputs are not compared (the canonical bytes are what makes two inputs one
/// endpoint). It is never logged or serialized, and its `Debug` shows nothing of it.
///
/// What the canonical bytes hold is the composition's (core captures HOME, the agent, the trust and proxy settings
/// and the timeouts, the reuse design's section 3); what they must not hold is the operation's: its capacity, its
/// repositories, its helper environment, its account tokens and its identity selection, which travel in the
/// [`AuthContext`] and the exchange.
#[derive(Clone)]
pub struct EndpointProfile {
    digest: [u8; 32],
    canonical: Vec<u8>,
    inputs: Option<Arc<dyn Any + Send + Sync>>,
}
/// The longest canonical profile accepted: 64 KiB (16 KiB before step 1.5's remediation). A composition's encoding must
/// fit any capture its grammars accept (core holds the sum of its field maxima under this bound statically).
pub const MAX_PROFILE_BYTES: usize = 64 * 1024;
impl EndpointProfile {
    pub fn new(canonical: Vec<u8>, digest: [u8; 32]) -> Result<Self, InvalidInput> {
        if canonical.is_empty() || canonical.len() > MAX_PROFILE_BYTES {
            return Err(InvalidInput::Profile);
        }
        Ok(Self {
            digest,
            canonical,
            inputs: None,
        })
    }
    /// A profile that also carries the validated, immutable inputs its endpoint factory makes the adapter from. The
    /// host never looks inside them; the factory that knows their type does.
    pub fn with_inputs(
        canonical: Vec<u8>,
        digest: [u8; 32],
        inputs: Arc<dyn Any + Send + Sync>,
    ) -> Result<Self, InvalidInput> {
        let mut profile = Self::new(canonical, digest)?;
        profile.inputs = Some(inputs);
        Ok(profile)
    }
    pub fn digest(&self) -> &[u8; 32] {
        &self.digest
    }
    /// The validated inputs, when the composition supplied them.
    pub fn inputs(&self) -> Option<&Arc<dyn Any + Send + Sync>> {
        self.inputs.as_ref()
    }
    /// The same profile: digest first, then the bytes.
    pub fn same_as(&self, other: &Self) -> bool {
        self.digest == other.digest && self.canonical == other.canonical
    }
}
impl PartialEq for EndpointProfile {
    fn eq(&self, other: &Self) -> bool {
        self.same_as(other)
    }
}
impl Eq for EndpointProfile {}
impl fmt::Debug for EndpointProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("EndpointProfile(..)")
    }
}

/// Which authentication context a connection was set up under, as a fixed-size value the provider derives from the
/// context's secret-bearing view (core: a digest of the helper's executable and environment). Two contexts with equal
/// scopes may share a connection; any other pair may not. It names no account and carries no secret, and its `Debug`
/// shows nothing of it.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct AuthScope([u8; 32]);
impl AuthScope {
    pub fn new(value: [u8; 32]) -> Self {
        Self(value)
    }
}
impl fmt::Debug for AuthScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AuthScope(..)")
    }
}

/// An opaque, bounded capability captured on the original caller entry: the operation's own view for its
/// authentication (core: the helper's executable and environment, so the account tokens it reads). The host never
/// looks inside it: it counts its charge against the admission budget, keeps it in the operation's binding (never in
/// an endpoint instance or a profile) and hands it to the adapter with each connect and each exchange it starts for
/// that operation. A connection serves only operations whose context has the scope of the one it was set up under.
#[derive(Clone)]
pub struct AuthContext {
    charge: usize,
    scope: AuthScope,
    provider: Arc<dyn Any + Send + Sync>,
}
impl AuthContext {
    /// `charge` is the most the provider retains for as long as this context lives, in bytes.
    pub fn new(charge: usize, scope: AuthScope, provider: Arc<dyn Any + Send + Sync>) -> Self {
        Self {
            charge,
            scope,
            provider,
        }
    }
    pub fn charge(&self) -> usize {
        self.charge
    }
    pub fn scope(&self) -> AuthScope {
        self.scope
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

/// The Git service of one exchange (contract section 9): a service's advertisement or its exchange, for upload-pack
/// (fetch) and receive-pack (push). It is the crate's own type: the public API exports no gwz-transport Git `Open`, and
/// it names nothing outside the Git smart protocol (no method, header or generic URL).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GitService {
    UploadPackAdvertisement,
    UploadPackExchange,
    ReceivePackAdvertisement,
    ReceivePackExchange,
}
impl GitService {
    /// Whether the service writes to the remote repository (receive-pack).
    pub fn is_receive_pack(self) -> bool {
        matches!(
            self,
            Self::ReceivePackAdvertisement | Self::ReceivePackExchange
        )
    }
    /// Whether this is the service's advertisement rather than its exchange.
    pub fn is_advertisement(self) -> bool {
        matches!(
            self,
            Self::UploadPackAdvertisement | Self::ReceivePackAdvertisement
        )
    }
}
/// The longest repository path an exchange accepts, in bytes (the SSH command's own bound).
const MAX_PATH_BYTES: usize = 16 * 1024;

/// What an exchange connects to and runs: the pool's key and identity, the Git service, and the repository path on
/// the remote. The path is validated here, before any effect: non-empty, at most 16 KiB, no NUL, and not starting
/// with `-` (so that no adapter can pass it to a remote command as an option).
#[derive(Clone, Debug)]
pub struct ExchangeSpec {
    pub(crate) key: Key,
    pub(crate) identity: Identity,
    service: GitService,
    path: String,
}
impl ExchangeSpec {
    pub fn git(
        key: Key,
        identity: Identity,
        service: GitService,
        path: &str,
    ) -> Result<Self, InvalidInput> {
        if path.is_empty()
            || path.len() > MAX_PATH_BYTES
            || path.contains('\0')
            || path.starts_with('-')
        {
            return Err(InvalidInput::Spec);
        }
        Ok(Self {
            key,
            identity,
            service,
            path: path.into(),
        })
    }
    pub fn key(&self) -> &Key {
        &self.key
    }
    pub fn identity(&self) -> &Identity {
        &self.identity
    }
    pub fn service(&self) -> GitService {
        self.service
    }
    pub fn path(&self) -> &str {
        &self.path
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
    /// The opening was offered more connections set up under another authentication context than its key can hold. A
    /// conforming opening misfits at most once (its fresh resubmission opens a connection under its own context), so
    /// this names a broken scope record, never load: nothing was started for this opening.
    NoFittingConnection,
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
