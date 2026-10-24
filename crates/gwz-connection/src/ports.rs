//! The four ports of contract section 4.1, supplied together to `build`: `Clock`, `OwnerSeed`, `Progress` and
//! `EndpointFactory`. None gives the library a socket, a file, a thread or a process; the composition implements them
//! (core's adapters over today's endpoints in step 1.3, fakes in this crate's tests).
use crate::{
    BindingId, CleanupDomain, CleanupStatus, EndpointProfile, ExchangeSpec,
    safety::SharedProcessSafety,
};
use gwz_transport::{
    pool::{ConnectionId, Identity, Key},
    protocol::{Disposition, Failure},
};
use std::{
    fmt,
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

/// A point on a host's clock, in milliseconds from the clock's own origin.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Tick(u64);
impl Tick {
    pub fn new(ms: u64) -> Self {
        Self(ms)
    }
    pub fn get(self) -> u64 {
        self.0
    }
    pub fn plus(self, by: Duration) -> Self {
        Self(
            self.0
                .saturating_add(u64::try_from(by.as_millis()).unwrap_or(u64::MAX)),
        )
    }
}
/// A wait on the clock.
pub type Sleep = Pin<Box<dyn Future<Output = ()> + Send>>;

/// One time origin for every comparison within a host (contract section 10).
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> Tick;
    /// Resolves when `now` reaches `deadline`. Dropping it cancels the wait.
    fn sleep_until(&self, deadline: Tick) -> Sleep;
    /// Changes whenever the clock saw a suspend or a wall-clock jump, so a deadline computed before it can be
    /// recomputed. A clock that cannot see either returns a constant.
    fn epoch(&self) -> u64;
}

/// The nonsecret namespace of one host instance. Identifiers a host mints are unique within it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HostNamespace(u128);
impl HostNamespace {
    pub fn new(value: u128) -> Self {
        Self(value)
    }
    pub fn get(self) -> u128 {
        self.0
    }
}
/// The composition root's identifier source. There is no global counter behind it.
pub trait OwnerSeed: Send + Sync + 'static {
    /// A namespace no other host of this composition has.
    fn namespace(&self) -> HostNamespace;
}

/// Work the composition runs for the host, independently of any application call.
pub type DriverWork = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
/// The executor cannot run work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProgressUnavailable;
impl fmt::Display for ProgressUnavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the host's progress executor is unavailable")
    }
}
impl std::error::Error for ProgressUnavailable {}
/// Executes the host's bounded work: its driver (`pump.rs`, one task per host, spawned when the first endpoint instance
/// attaches) and its close's disposal. It has no network primitive.
pub trait Progress: Send + Sync + 'static {
    /// Whether work can run now. `build` asks once.
    fn check(&self) -> Result<(), ProgressUnavailable>;
    /// Runs `work` to completion, whether or not anything awaits its result. A refusal drops the work unrun.
    fn spawn(&self, work: DriverWork) -> Result<(), ProgressUnavailable>;
}

/// The validated, immutable inputs an endpoint adapter is made from: the operation's profile (contract section 7), which
/// is also what coalesces equal endpoints onto one instance.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct EndpointSpec {
    pub profile: EndpointProfile,
}
impl EndpointSpec {
    pub fn new(profile: EndpointProfile) -> Self {
        Self { profile }
    }
}
/// What a factory is given besides the spec: the domain that owns its cleanup budgets and the process's one gate.
pub struct FactoryEnv<'a> {
    domain: &'a CleanupDomain,
    safety: &'a SharedProcessSafety,
}
impl<'a> FactoryEnv<'a> {
    pub(crate) fn new(domain: &'a CleanupDomain, safety: &'a SharedProcessSafety) -> Self {
        Self { domain, safety }
    }
    pub fn domain(&self) -> &CleanupDomain {
        self.domain
    }
    pub fn safety(&self) -> &SharedProcessSafety {
        self.safety
    }
}
/// Makes endpoint adapters. Making one allocates metadata only: no network, key, agent, helper or authentication
/// (contract section 5). Its cleanup budgets come from `env.domain()`, never from a private pool.
pub trait EndpointFactory: Send + Sync + 'static {
    fn create(
        &self,
        spec: &EndpointSpec,
        env: &FactoryEnv<'_>,
    ) -> Result<Box<dyn EndpointAdapter>, FactoryError>;
}
/// Start connecting `connection`, which the instance's pool named: the adapter reports the result with
/// `Completion::Connected`, and never starts it twice.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ConnectCommand {
    pub connection: ConnectionId,
    pub key: Key,
    pub identity: Identity,
}
/// What an adapter reports about work it was given. Connection identifiers are the pool's and are never reused.
#[derive(Debug)]
#[non_exhaustive]
pub enum Completion {
    /// The connect ended. `Err` means the adapter holds nothing for it.
    Connected {
        connection: ConnectionId,
        result: Result<Option<Identity>, Failure>,
    },
    /// The physical connection is disposed: the pool may free its capacity.
    Closed { connection: ConnectionId },
    /// An idle connection was lost (the peer closed or reset it, or sent bytes no exchange asked for) and the adapter
    /// has disposed of it: the pool forgets it without a close (contract section 8, idle liveness). If a lease won
    /// the race, the lease keeps it and its exchange fails; that lease's release and close end it.
    IdleClosed { connection: ConnectionId },
    /// The proof `EndpointAdapter::authorize_reuse` started for `connection` ended (contract section 7.1).
    Reuse {
        connection: ConnectionId,
        verdict: ReuseVerdict,
    },
}
/// Prove `connection`, an idle connection that another binding last used, for `binding` before the host leases it to
/// `binding` (contract sections 4.1 and 7.1): an agent's fresh possession signature for the key the connection
/// authenticated with, an explicit key's equality, a fresh trust check of its host key. While the proof runs the
/// connection is `binding`'s lease.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReuseCommand {
    pub connection: ConnectionId,
    pub binding: BindingId,
}
/// How a reuse proof ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReuseVerdict {
    /// The binding proved the connection's authority now: the lease serves it.
    Authorized,
    /// The proof failed (the key is gone, the signature does not verify, the host key is no longer trusted): the
    /// connection is discarded, and the adapter reports matching idle connections it retires as `IdleClosed`. The
    /// opening asks for a new connection.
    Revoked,
    /// Nothing this binding can prove (a password's connection, a key the adapter cannot check), or the proof could
    /// not finish: the connection goes back to idle for the binding that used it, and the opening asks for a new one.
    Unprovable,
}
/// One exchange on a leased connection, in the adapter's own terms. Each call returns at once: a call that cannot
/// proceed returns `WouldBlock`, never blocks the caller.
pub trait ExchangeIo: Send {
    fn write(&mut self, data: &[u8]) -> io::Result<usize>;
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize>;
    /// The orderly end of the exchange. The first poll starts it; `Ready(Reusable)` asserts that the native protocol
    /// confirmed complete request/response cleanup and the connection can serve another exchange, and anything
    /// unproven is `Ready(Discarded)`. The host's driver polls it (not the application), so a `Pending` must wake
    /// `cx` when it can go on.
    fn poll_finish(&mut self, cx: &mut Context<'_>) -> Poll<Disposition>;
    /// The exchange ends abnormally; its connection is discarded.
    fn abort(&mut self);
}
/// One endpoint instance, at the Connector/Resource level: it makes and disposes of physical connections and runs one
/// exchange at a time on each, and it owns no pool and no reuse policy (contract sections 4.1 and 8: the host's pool
/// chooses what to connect, lease, keep idle and close). Every method returns at once (the commands only start work,
/// and the adapter does its I/O on its own threads or tasks, outside any library lock) and completions come back
/// through `poll_completion`, which the host's driver polls whenever the adapter wakes the waker it was given.
/// Dropping an adapter must still release what it holds, as a last resort and without a guarantee of confirmation.
/// `cancel_work` is `cancel_connect` and `close` here.
pub trait EndpointAdapter: Send {
    fn connect(&mut self, command: ConnectCommand);
    /// The connect is no longer wanted; its `Connected` completion still arrives.
    fn cancel_connect(&mut self, connection: ConnectionId);
    /// Dispose of `connection`; `Completion::Closed` confirms it.
    fn close(&mut self, connection: ConnectionId);
    /// Starts the exchange `spec` names (its Git service and path) on `connection`, which the caller leases.
    fn start_exchange(
        &mut self,
        connection: ConnectionId,
        spec: &ExchangeSpec,
    ) -> io::Result<Box<dyn ExchangeIo>>;
    /// Contract section 4.1's `authorize_reuse` (section 7.1; step 1.6): starts the proof `command` names and reports
    /// it as `Completion::Reuse`, or returns `false` when it starts none, which the host takes as `Unprovable`. The
    /// default starts none: an adapter that proves nothing never serves one binding's connection to another.
    fn authorize_reuse(&mut self, command: ReuseCommand) -> bool {
        let _ = command;
        false
    }
    /// The next completion, or `Pending` with the waker registered.
    fn poll_completion(&mut self, cx: &mut Context<'_>) -> Poll<Completion>;
    /// The first poll starts disposal; `Ready` means disposal is confirmed. Polling again after `Ready` is `Ready`.
    /// It wakes the waker it was last polled with.
    fn poll_dispose(&mut self, cx: &mut Context<'_>) -> Poll<()>;
}

/// Why a factory made nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum FactoryReason {
    Unavailable,
    InvalidSpec,
}
/// A factory error says what became of anything it started.
#[derive(Clone, Debug)]
pub struct FactoryError {
    reason: FactoryReason,
    cleanup: CleanupStatus,
}
impl FactoryError {
    pub fn new(reason: FactoryReason, cleanup: CleanupStatus) -> Self {
        Self { reason, cleanup }
    }
    pub fn reason(&self) -> FactoryReason {
        self.reason
    }
    pub fn cleanup(&self) -> &CleanupStatus {
        &self.cleanup
    }
}
impl fmt::Display for FactoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "endpoint factory refused: {:?}", self.reason)
    }
}
impl std::error::Error for FactoryError {}

/// The four ports, supplied together. No `Default`.
pub struct HostPorts {
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) seed: Arc<dyn OwnerSeed>,
    pub(crate) progress: Arc<dyn Progress>,
    pub(crate) factory: Arc<dyn EndpointFactory>,
}
impl HostPorts {
    pub fn new(
        clock: Arc<dyn Clock>,
        seed: Arc<dyn OwnerSeed>,
        progress: Arc<dyn Progress>,
        factory: Arc<dyn EndpointFactory>,
    ) -> Self {
        Self {
            clock,
            seed,
            progress,
            factory,
        }
    }
}
