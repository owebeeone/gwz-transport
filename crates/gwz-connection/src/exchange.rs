//! A registered operation and its exchanges (contract sections 4, 5 and 8).
//!
//! An `Operation` owns one binding. `open` registers a request in the binding's two sessions (so the binding's own limit
//! applies) and returns an `Opening`, which leases a connection from the host's pool for the operation's profile and
//! yields an `Exchange`: one exclusive lease, one stream. Finishing an exchange returns its connection to idle only if
//! the adapter proves the protocol clean; anything else discards it. Neither type has a public constructor.
//!
//! A lease serves the operation only when its connection was set up under a context of the operation's scope (step
//! 1.5): a connection set up under another context is retired, never handed over, and the opening asks again.
use crate::{
    CancelState, ExchangeEnd, ExchangeIo, ExchangeSpec, OpenError, OpenFailure, OperationEnd,
    Registration,
    binding::{BindingShared, closing_reason},
    cell::{Cell, Slot},
    host::HostShared,
    instance::{Instance, Opened},
    operation::TerminalReason,
};
use gwz_transport::{
    pool::{LeaseId, Owner, Request, RequestId},
    protocol::{Disposition, Effect},
};
use std::{
    fmt,
    future::Future,
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
            tag: request,
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
cfg_if::cfg_if! {
    if #[cfg(test)] {
        impl Operation {
            /// The endpoint instance this operation's binding leases from.
            pub(crate) fn instance_for_test(&self) -> Arc<Instance> {
                match &self.kind {
                    Kind::Live(binding) => binding.instance.clone(),
                    Kind::Terminal(_) => panic!("a terminal operation has no instance"),
                }
            }
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
    /// The request's name in the binding, which tags its pool request.
    tag: String,
}
impl Opening {
    /// Submits the opening to the host: from here its lease, its connection and the start of its exchange are the
    /// host's driver's to progress, whether or not the returned future is polled (contract section 6: progress is
    /// independent of the application's calls). The future yields the exchange once the driver has started it, on a
    /// connection of this operation's profile set up under a context of its scope. Dropping the future cancels this
    /// opening only; an exchange already started for it is aborted and its connection discarded.
    pub fn wait(self) -> OpenWait {
        let binding = self.live.binding.clone();
        let refused = closing_reason(binding.closing.load(Acquire))
            .map(OpenFailure::Closing)
            .or_else(|| {
                binding
                    .cancelled
                    .load(Acquire)
                    .then_some(OpenFailure::Cancelled)
            })
            .or_else(|| binding.sealed.load(Acquire).then_some(OpenFailure::Sealed));
        let submitted = match refused {
            Some(failure) => Err(failure),
            None => {
                let mut request = Request::new(
                    self.spec.key.clone(),
                    self.spec.identity.clone(),
                    Owner::new(binding.session.clone(), binding.operation.clone()),
                );
                // The tag is how a connect the pool starts for this request finds the operation's context.
                request.tag = Some(self.tag);
                binding
                    .instance
                    .submit(request, self.spec, binding.auth.clone())
                    .map_err(OpenFailure::Pool)
            }
        };
        let (submitted, early) = match submitted {
            Ok(submitted) => (Some(submitted), None),
            Err(failure) => (None, Some(failure)),
        };
        OpenWait {
            live: Some(self.live),
            submitted,
            early,
        }
    }
}
/// See [`Opening::wait`].
pub struct OpenWait {
    live: Option<Live>,
    submitted: Option<(RequestId, Arc<Slot<Opened>>)>,
    early: Option<OpenFailure>,
}
impl OpenWait {
    /// Whether the host's driver has already started this opening's exchange (or ended it): the next poll is ready.
    pub fn is_ready(&self) -> bool {
        self.early.is_some()
            || self
                .submitted
                .as_ref()
                .is_some_and(|(_, slot)| slot.is_filled())
    }
}
impl Future for OpenWait {
    type Output = Result<Exchange, OpenFailure>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        if let Some(failure) = this.early.take() {
            return Poll::Ready(Err(failure));
        }
        let (Some(live), Some((_, slot))) = (&this.live, &this.submitted) else {
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
        // A cancellation or a seal wakes the instance's waiters; the driver fills the slot.
        binding.instance.wait(cx.waker());
        match slot.poll_take(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(failure)) => {
                this.submitted = None;
                Poll::Ready(Err(failure))
            }
            Poll::Ready(Ok((io, lease, reused))) => {
                this.submitted = None;
                let live = this.live.take().expect("live checked above");
                Poll::Ready(Ok(Exchange {
                    stream: MessageStream {
                        io: Some(io),
                        binding: binding.clone(),
                    },
                    lease: Some(LeaseGuard {
                        instance: binding.instance.clone(),
                        lease: Some(lease),
                    }),
                    reused,
                    _live: Some(live),
                }))
            }
        }
    }
}
impl Drop for OpenWait {
    fn drop(&mut self) {
        if let (Some((request, slot)), Some(live)) = (self.submitted.take(), &self.live) {
            live.binding.instance.abandon(request, &slot);
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
    Io(std::io::Error),
}
/// One exchange's byte stream, over the adapter's own I/O. Reads and writes are serialized by `&mut`.
pub struct MessageStream {
    /// Taken only by the exchange's end, which consumes the exchange.
    io: Option<Box<dyn ExchangeIo>>,
    binding: Arc<BindingShared>,
}
impl MessageStream {
    pub fn write(&mut self, data: &[u8]) -> Result<usize, StreamError> {
        self.io()?.write(data).map_err(StreamError::Io)
    }
    pub fn read(&mut self, out: &mut [u8]) -> Result<usize, StreamError> {
        self.io()?.read(out).map_err(StreamError::Io)
    }
    fn io(&mut self) -> Result<&mut Box<dyn ExchangeIo>, StreamError> {
        if self.binding.cancelled.load(Acquire) {
            return Err(StreamError::Cancelled);
        }
        self.io.as_mut().ok_or(StreamError::Cancelled)
    }
}

/// One exclusive lease and its stream. `Send`, not `Clone`; dropping it discards the lease.
pub struct Exchange {
    stream: MessageStream,
    lease: Option<LeaseGuard>,
    reused: bool,
    _live: Option<Live>,
}
impl Exchange {
    pub fn stream(&mut self) -> &mut MessageStream {
        &mut self.stream
    }
    /// Whether the connection served an exchange before this one (the observation's `reused`): it was idle, of this
    /// operation's profile, and set up under a context of this operation's scope.
    pub fn reused(&self) -> bool {
        self.reused
    }
    /// Ends the exchange: its connection goes back to idle only if the adapter proves the protocol clean and the
    /// operation was not cancelled. The orderly end starts when this is built, and the host's driver settles the
    /// lease when the adapter confirms it (`ExchangeIo::poll_finish`), whether or not the future is polled.
    pub fn finish(mut self) -> ExchangeFinish {
        let (Some(mut io), Some(mut lease)) = (self.stream.io.take(), self.lease.take()) else {
            return ExchangeFinish(Cell::ready(discarded()));
        };
        if self.stream.binding.cancelled.load(Acquire) {
            io.abort();
            lease.release(Disposition::Discarded);
            return ExchangeFinish(Cell::ready(discarded()));
        }
        let id = lease.lease.take().expect("a live lease");
        ExchangeFinish(lease.instance.settle(io, id))
    }
    /// Ends the exchange abnormally: its connection is discarded.
    pub fn abort(mut self) {
        if let Some(mut io) = self.stream.io.take() {
            io.abort();
        }
        if let Some(lease) = self.lease.take() {
            lease.release(Disposition::Discarded);
        }
    }
}
fn discarded() -> ExchangeEnd {
    ExchangeEnd {
        disposition: Disposition::Discarded,
        cleanup: crate::CleanupStatus::Complete,
    }
}
impl Drop for Exchange {
    fn drop(&mut self) {
        if self.lease.is_some()
            && let Some(io) = self.stream.io.as_mut()
        {
            io.abort();
        }
    }
}
/// The receipt of a finished exchange, set by the host's driver once the adapter confirmed the exchange's end.
pub struct ExchangeFinish(Cell<ExchangeEnd>);
impl Future for ExchangeFinish {
    type Output = ExchangeEnd;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<ExchangeEnd> {
        self.0.poll(cx)
    }
}
