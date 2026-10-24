//! A registered operation and its exchanges (contract sections 4, 5 and 8).
//!
//! An `Operation` owns one binding. `open` registers a request in the binding's two sessions (so the binding's own limit
//! applies) and returns an `Opening`, which leases a connection from the host's pool for the operation's profile and
//! yields an `Exchange`: one exclusive lease, one stream. Finishing an exchange returns its connection to idle only if
//! the adapter proves the protocol clean; anything else discards it. Neither type has a public constructor.
use crate::{
    CancelState, ExchangeEnd, ExchangeIo, ExchangeSpec, OpenError, OpenFailure, OperationEnd,
    Registration,
    binding::{BindingShared, closing_reason},
    cell::Cell,
    host::HostShared,
    instance::Instance,
    operation::TerminalReason,
};
use gwz_transport::{
    pool::{LeaseId, Owner, Request, RequestId},
    protocol::{Disposition, Effect},
};
use std::{
    fmt,
    future::Future,
    io,
    pin::Pin,
    sync::{
        Arc, Weak,
        atomic::Ordering::{AcqRel, Acquire, Release},
    },
    task::{Context, Poll},
};

enum Kind {
    Live(Arc<BindingShared>),
    Terminal(TerminalReason),
}
/// One registered operation, or the terminal stand-in for one that never registered. `Send`, not `Clone`.
pub struct Operation {
    host: Arc<HostShared>,
    kind: Kind,
    finished: bool,
}
impl Operation {
    pub(crate) fn live(host: Arc<HostShared>, binding: Arc<BindingShared>) -> Self {
        Self {
            host,
            kind: Kind::Live(binding),
            finished: false,
        }
    }
    pub(crate) fn terminal(host: Arc<HostShared>, reason: TerminalReason) -> Self {
        Self {
            host,
            kind: Kind::Terminal(reason),
            finished: false,
        }
    }
    pub fn registration(&self) -> Registration {
        match &self.kind {
            Kind::Live(binding) => Registration::Registered(binding.id),
            Kind::Terminal(reason) => Registration::NeverRegistered(*reason),
        }
    }
    /// Cancels this operation only; the handle is weak and `Send + Sync + Clone`.
    pub fn cancel_handle(&self) -> OperationCancel {
        OperationCancel {
            binding: match &self.kind {
                Kind::Live(binding) => Arc::downgrade(binding),
                Kind::Terminal(_) => Weak::new(),
            },
        }
    }
    /// Registers a request in the binding's sessions and begins to lease a connection for it. No effect happens
    /// until the returned opening is polled.
    pub fn open(&self, spec: ExchangeSpec) -> Result<Opening, OpenError> {
        let binding = match &self.kind {
            Kind::Live(binding) => binding,
            Kind::Terminal(reason) => {
                return Err(OpenError::Terminal {
                    reason: *reason,
                    effect: Effect::None,
                });
            }
        };
        if binding.cancelled.load(Acquire) {
            return Err(OpenError::Cancelled);
        }
        if binding.sealed.load(Acquire) {
            return Err(OpenError::Sealed);
        }
        let number = binding.next_exchange.fetch_add(1, AcqRel);
        let request = format!("x{}-{number}", binding.id.serial);
        let registered = binding
            .pair
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .register(&request, &binding.operation);
        match registered {
            Ok(()) => {}
            Err(crate::binding::Fault::Limit) => return Err(OpenError::BindingLimit),
            Err(crate::binding::Fault::Protocol) => {
                return Err(OpenError::Terminal {
                    reason: TerminalReason::HostFault,
                    effect: Effect::None,
                });
            }
        }
        binding.opened.fetch_add(1, AcqRel);
        Ok(Opening {
            live: Live::new(self.host.clone(), binding.clone()),
            spec,
        })
    }
    /// Seals the operation now (no new opens), and resolves when nothing of it is live: its exchanges and openings
    /// have ended. It never closes the host or another operation's connections. Dropping the future changes nothing.
    pub fn finish(mut self) -> OperationFinish {
        self.finished = true;
        match &self.kind {
            Kind::Live(binding) => {
                binding.sealed.store(true, Release);
                self.host.retire_if_done(binding);
                OperationFinish {
                    cell: binding.done.clone(),
                }
            }
            Kind::Terminal(reason) => {
                let cell = Cell::new();
                cell.set(OperationEnd {
                    registration: Registration::NeverRegistered(*reason),
                    cleanup: crate::CleanupStatus::Complete,
                    exchanges_opened: 0,
                    cancelled: false,
                });
                OperationFinish { cell }
            }
        }
    }
    #[allow(
        dead_code,
        reason = "frames are routed to a binding by its session; the tests and the carrier ask for it"
    )]
    pub(crate) fn binding_session(&self) -> String {
        match &self.kind {
            Kind::Live(binding) => binding.session.clone(),
            Kind::Terminal(_) => String::new(),
        }
    }
    /// Streams the binding's two sessions hold (driver, endpoint).
    #[allow(
        dead_code,
        reason = "the tests use it to show that a discarded frame left no state"
    )]
    pub(crate) fn binding_streams(&self) -> (usize, usize) {
        match &self.kind {
            Kind::Live(binding) => {
                let pair = binding.pair.lock().unwrap_or_else(|e| e.into_inner());
                (pair.driver.active_streams(), pair.endpoint.active_streams())
            }
            Kind::Terminal(_) => (0, 0),
        }
    }
}
impl Drop for Operation {
    /// An operation dropped without `finish` abandons its binding: it is cancelled and sealed.
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        if let Kind::Live(binding) = &self.kind {
            cancel_binding(binding);
            binding.sealed.store(true, Release);
            self.host.retire_if_done(binding);
        }
    }
}
impl fmt::Debug for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Operation")
            .field("registration", &self.registration())
            .finish()
    }
}

/// Marks the binding cancelled and has its pool cancel what it owns: pending requests fail and its leased connections
/// are closed. Other bindings' connections are untouched.
fn cancel_binding(binding: &Arc<BindingShared>) -> bool {
    if binding.cancelled.swap(true, AcqRel) {
        return false;
    }
    {
        let mut state = binding.instance.lock();
        state.pool.cancel_operation(&Owner::new(
            binding.session.clone(),
            binding.operation.clone(),
        ));
        binding.instance.pump(&mut state);
    }
    binding.instance.wake_waiters();
    true
}

/// Cancels one operation. `Send + Sync + Clone`; it holds the binding weakly, so it keeps nothing alive.
#[derive(Clone)]
pub struct OperationCancel {
    binding: Weak<BindingShared>,
}
impl OperationCancel {
    pub fn cancel(&self) -> CancelState {
        match self.binding.upgrade() {
            Some(binding) if !binding.retired.load(Acquire) => {
                if cancel_binding(&binding) {
                    CancelState::Cancelled
                } else {
                    CancelState::AlreadyCancelled
                }
            }
            _ => CancelState::Gone,
        }
    }
}

/// The wait for an operation's end. Sealing happened when it was built.
pub struct OperationFinish {
    cell: Cell<OperationEnd>,
}
impl Future for OperationFinish {
    type Output = OperationEnd;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<OperationEnd> {
        self.cell.poll(cx)
    }
}

/// Holds a binding open while an opening or an exchange of it is live; the last to go lets it retire.
struct Live {
    host: Arc<HostShared>,
    binding: Arc<BindingShared>,
}
impl Live {
    fn new(host: Arc<HostShared>, binding: Arc<BindingShared>) -> Self {
        binding.live.fetch_add(1, AcqRel);
        Self { host, binding }
    }
}
impl Drop for Live {
    fn drop(&mut self) {
        self.binding.live.fetch_sub(1, AcqRel);
        self.host.retire_if_done(&self.binding);
    }
}

/// A request for an exchange: a registered request, no connection yet.
pub struct Opening {
    live: Live,
    spec: ExchangeSpec,
}
impl Opening {
    /// Leases a connection (reusing an idle one the host holds for this profile) and starts the exchange. Dropping the
    /// future cancels this opening only.
    pub fn wait(self) -> OpenWait {
        OpenWait {
            live: Some(self.live),
            spec: self.spec,
            request: None,
        }
    }
}
/// See [`Opening::wait`].
pub struct OpenWait {
    live: Option<Live>,
    spec: ExchangeSpec,
    request: Option<RequestId>,
}
impl Future for OpenWait {
    type Output = Result<Exchange, OpenFailure>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let Some(live) = &this.live else {
            return Poll::Ready(Err(OpenFailure::Cancelled));
        };
        let binding = live.binding.clone();
        if let Some(reason) = closing_reason(binding.closing.load(Acquire)) {
            return Poll::Ready(Err(OpenFailure::Closing(reason)));
        }
        if binding.cancelled.load(Acquire) {
            return Poll::Ready(Err(OpenFailure::Cancelled));
        }
        if binding.sealed.load(Acquire) {
            return Poll::Ready(Err(OpenFailure::Sealed));
        }
        let instance = binding.instance.clone();
        instance.wait(cx.waker());
        let mut state = instance.lock();
        let request = match this.request {
            Some(request) => request,
            None => {
                let request = Request::new(
                    this.spec.key.clone(),
                    this.spec.identity.clone(),
                    Owner::new(binding.session.clone(), binding.operation.clone()),
                );
                match state.pool.request(request) {
                    Ok(id) => id,
                    Err(error) => return Poll::Ready(Err(OpenFailure::Pool(error))),
                }
            }
        };
        this.request = Some(request);
        instance.pump(&mut state);
        match state.pool.take(request) {
            Ok(lease) => {
                this.request = None;
                let started = match state.adapter.as_mut() {
                    Some(adapter) => adapter.start_exchange(lease.connection()),
                    None => Err(io::Error::other("endpoint instance is closed")),
                };
                match started {
                    Ok(io) => {
                        drop(state);
                        let live = this.live.take().expect("live checked above");
                        Poll::Ready(Ok(Exchange {
                            stream: MessageStream { io, binding },
                            lease: Some(LeaseGuard {
                                instance,
                                lease: Some(lease),
                            }),
                            _live: Some(live),
                        }))
                    }
                    Err(error) => {
                        let _ = state.pool.release(lease, Disposition::Discarded);
                        instance.pump(&mut state);
                        Poll::Ready(Err(OpenFailure::Io(error)))
                    }
                }
            }
            Err(gwz_transport::pool::Error::WouldBlock) => Poll::Pending,
            Err(error) => {
                this.request = None;
                Poll::Ready(Err(OpenFailure::Pool(error)))
            }
        }
    }
}
impl Drop for OpenWait {
    fn drop(&mut self) {
        if let (Some(request), Some(live)) = (self.request.take(), &self.live) {
            live.binding.instance.abandon(request);
        }
    }
}

/// A leased connection, given back (discarded unless told otherwise) when it goes.
struct LeaseGuard {
    instance: Arc<Instance>,
    lease: Option<LeaseId>,
}
impl LeaseGuard {
    fn release(mut self, disposition: Disposition) {
        if let Some(lease) = self.lease.take() {
            self.instance.release(lease, disposition);
        }
    }
}
impl Drop for LeaseGuard {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.take() {
            self.instance.release(lease, Disposition::Discarded);
        }
    }
}

/// Why a stream call failed.
#[derive(Debug)]
pub enum StreamError {
    /// The operation was cancelled (or its host closed): the exchange is over.
    Cancelled,
    Io(io::Error),
}
/// One exchange's byte stream, over the adapter's own I/O. Reads and writes are serialized by `&mut`.
pub struct MessageStream {
    io: Box<dyn ExchangeIo>,
    binding: Arc<BindingShared>,
}
impl MessageStream {
    pub fn write(&mut self, data: &[u8]) -> Result<usize, StreamError> {
        self.check()?;
        self.io.write(data).map_err(StreamError::Io)
    }
    pub fn read(&mut self, out: &mut [u8]) -> Result<usize, StreamError> {
        self.check()?;
        self.io.read(out).map_err(StreamError::Io)
    }
    fn check(&self) -> Result<(), StreamError> {
        if self.binding.cancelled.load(Acquire) {
            Err(StreamError::Cancelled)
        } else {
            Ok(())
        }
    }
}

/// One exclusive lease and its stream. `Send`, not `Clone`; dropping it discards the lease.
pub struct Exchange {
    stream: MessageStream,
    lease: Option<LeaseGuard>,
    _live: Option<Live>,
}
impl Exchange {
    pub fn stream(&mut self) -> &mut MessageStream {
        &mut self.stream
    }
    /// Ends the exchange: its connection goes back to idle only if the adapter proves the protocol clean and the
    /// operation was not cancelled. The lease is settled when this is built.
    pub fn finish(mut self) -> ExchangeFinish {
        let disposition = if self.stream.binding.cancelled.load(Acquire) {
            self.stream.io.abort();
            Disposition::Discarded
        } else {
            self.stream.io.finish()
        };
        if let Some(lease) = self.lease.take() {
            lease.release(disposition);
        }
        ExchangeFinish(Some(ExchangeEnd {
            disposition,
            cleanup: crate::CleanupStatus::Complete,
        }))
    }
    /// Ends the exchange abnormally: its connection is discarded.
    pub fn abort(mut self) {
        self.stream.io.abort();
        if let Some(lease) = self.lease.take() {
            lease.release(Disposition::Discarded);
        }
    }
}
impl Drop for Exchange {
    fn drop(&mut self) {
        if self.lease.is_some() {
            self.stream.io.abort();
        }
    }
}
/// The receipt of a finished exchange (already settled).
pub struct ExchangeFinish(Option<ExchangeEnd>);
impl Future for ExchangeFinish {
    type Output = ExchangeEnd;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<ExchangeEnd> {
        match self.0.take() {
            Some(end) => Poll::Ready(end),
            None => Poll::Pending,
        }
    }
}
