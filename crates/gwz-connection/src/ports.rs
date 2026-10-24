//! The four ports of contract section 4.1, supplied together to `build`: `Clock`, `OwnerSeed`, `Progress` and
//! `EndpointFactory`. None gives the library a socket, a file, a thread or a process; the composition implements them
//! (core's adapters over today's endpoints in step 1.3, fakes in this crate's tests).
use crate::{CleanupDomain, CleanupStatus, EndpointProfile, safety::SharedProcessSafety};
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
/// Executes the host's bounded work (its close's disposal now, its driver and timers later). It has no network
/// primitive.
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
}
/// One exchange on a leased connection, in the adapter's own terms. Each call returns at once: a call that cannot
/// proceed returns `WouldBlock`, never blocks the caller.
pub trait ExchangeIo: Send {
    fn write(&mut self, data: &[u8]) -> io::Result<usize>;
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize>;
    /// The exchange ended in order. `Reusable` asserts that the protocol is cleanly complete and the connection can
    /// serve another exchange; anything unproven is `Discarded`.
    fn finish(&mut self) -> Disposition;
    /// The exchange ends abnormally; its connection is discarded.
    fn abort(&mut self);
}
/// One endpoint instance. Every method returns at once (the commands only start work, and the adapter does its I/O on
/// its own threads or tasks, outside any library lock) and completions come back through `poll_completion`.
/// Dropping an adapter must still release what it holds, as a last resort and without a guarantee of confirmation.
/// Contract 4.1's `authorize_reuse` and `cancel_work` arrive with steps 1.6 and 1.3.
pub trait EndpointAdapter: Send {
    fn connect(&mut self, command: ConnectCommand);
    /// The connect is no longer wanted; its `Connected` completion still arrives.
    fn cancel_connect(&mut self, connection: ConnectionId);
    /// Dispose of `connection`; `Completion::Closed` confirms it.
    fn close(&mut self, connection: ConnectionId);
    /// Starts an exchange on `connection`, which the caller leases.
    fn start_exchange(&mut self, connection: ConnectionId) -> io::Result<Box<dyn ExchangeIo>>;
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
