//! A registered operation and its exchanges (contract sections 4, 5 and 8).
//!
//! `Operation`, `Opening`, `OpenWait`, `Exchange`, `MessageStream` and their futures are the one set of types for a local
//! host and a carrier-backed handle (GwzSplitCommandChannel 3.4): each holds the local case below, or the carrier-backed
//! one of `remote`, behind a private representation, and a caller cannot tell one from the other by type. What follows
//! describes the local case.
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
    instance::{Instance, Opened, cancel_authorizing},
    operation::TerminalReason,
    remote::{
        RemoteCancel, RemoteFinish, RemoteLive, RemoteOp, RemoteOpenWait, RemoteOpening,
        RemoteStream,
    },
};
use gwz_transport::{
    pool::{LeaseId, Owner, Request},
    protocol::{Disposition, Effect, ErrorCode},
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
    Live {
        host: Arc<HostShared>,
        binding: Arc<BindingShared>,
    },
    Terminal(TerminalReason),
    Remote(RemoteOp),
}
/// One registered operation, or the terminal stand-in for one that never registered. `Send`, not `Clone`.
pub struct Operation {
    kind: Kind,
    finished: bool,
}
impl Operation {
    pub(crate) fn live(host: Arc<HostShared>, binding: Arc<BindingShared>) -> Self {
        Self {
            kind: Kind::Live { host, binding },
            finished: false,
        }
    }
    pub(crate) fn terminal(reason: TerminalReason) -> Self {
        Self {
            kind: Kind::Terminal(reason),
            finished: false,
        }
    }
    pub(crate) fn remote(op: RemoteOp) -> Self {
        Self {
            kind: Kind::Remote(op),
            finished: false,
        }
    }
    /// Where the registration stands. A local operation is registered or never was; a carrier-backed one is
    /// `Pending` until the client answers `bound` or `bind_rejected` or the carrier ends.
    pub fn registration(&self) -> Registration {
        match &self.kind {
            Kind::Live { binding, .. } => Registration::Registered(binding.id),
            Kind::Terminal(reason) => Registration::NeverRegistered(*reason),
            Kind::Remote(op) => op.registration(),
        }
    }
    /// Resolves once the registration is no longer `Pending`: at once for a local operation.
    pub fn registered(&self) -> Registered<'_> {
        Registered { operation: self }
    }
    /// Cancels this operation only; the handle is weak and `Send + Sync + Clone`.
    pub fn cancel_handle(&self) -> OperationCancel {
        OperationCancel(match &self.kind {
            Kind::Live { binding, .. } => CancelKind::Local(Arc::downgrade(binding)),
            Kind::Terminal(_) => CancelKind::Local(Weak::new()),
            Kind::Remote(op) => CancelKind::Remote(op.cancel_handle()),
        })
    }
    /// Registers a request in the binding's sessions and begins to lease a connection for it. No effect happens
    /// until the returned opening is polled.
    pub fn open(&self, spec: ExchangeSpec) -> Result<Opening, OpenError> {
        let (host, binding) = match &self.kind {
            Kind::Live { host, binding } => (host, binding),
            Kind::Terminal(reason) => {
                return Err(OpenError::Terminal {
                    reason: *reason,
                    effect: Effect::None,
                });
            }
            Kind::Remote(op) => {
                return op
                    .open(spec)
                    .map(|opening| Opening(OpeningKind::Remote(opening)));
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
        Ok(Opening(OpeningKind::Local(LocalOpening {
            live: Live::new(host.clone(), binding.clone()),
            spec,
            tag: request,
        })))
    }
    /// Seals the operation now (no new opens), and resolves when nothing of it is live: its exchanges and openings
    /// have ended. It never closes the host or another operation's connections. Dropping the future changes nothing.
    pub fn finish(mut self) -> OperationFinish {
        self.finished = true;
        match &self.kind {
            Kind::Live { host, binding } => {
                binding.sealed.store(true, Release);
                host.retire_if_done(binding);
                OperationFinish {
                    cell: binding.done.clone(),
                }
            }
            Kind::Remote(op) => OperationFinish { cell: op.finish() },
            Kind::Terminal(reason) => {
                let cell = Cell::new();
                cell.set(OperationEnd {
                    registration: Registration::NeverRegistered(*reason),
                    cleanup: crate::CleanupStatus::Complete,
                    exchanges_opened: 0,
                    cancelled: false,
                    revoked: None,
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
            Kind::Live { binding, .. } => binding.session.clone(),
            Kind::Terminal(_) | Kind::Remote(_) => String::new(),
        }
    }
    /// Streams the binding's two sessions hold (driver, endpoint).
    #[allow(
        dead_code,
        reason = "the tests use it to show that a discarded frame left no state"
    )]
    pub(crate) fn binding_streams(&self) -> (usize, usize) {
        match &self.kind {
            Kind::Live { binding, .. } => {
                let pair = binding.pair.lock().unwrap_or_else(|e| e.into_inner());
                (pair.driver.active_streams(), pair.endpoint.active_streams())
            }
            Kind::Terminal(_) | Kind::Remote(_) => (0, 0),
        }
    }
}
cfg_if::cfg_if! {
    if #[cfg(test)] {
        impl Operation {
            /// The endpoint instance this operation's binding leases from.
            pub(crate) fn instance_for_test(&self) -> Arc<Instance> {
                match &self.kind {
                    Kind::Live { binding, .. } => binding.instance.clone(),
                    Kind::Terminal(_) | Kind::Remote(_) => panic!("only a local operation has an instance"),
                }
            }
            /// A weak reference to the binding: once the operation and everything of it has ended it upgrades to nothing,
            /// which is how a row shows that no table, receipt or lease keeps a retired binding (or its muxes) alive.
            pub(crate) fn binding_weak_for_test(&self) -> std::sync::Weak<BindingShared> {
                match &self.kind {
                    Kind::Live { binding, .. } => Arc::downgrade(binding),
                    Kind::Terminal(_) | Kind::Remote(_) => panic!("only a local operation has a binding"),
                }
            }
            /// Sets the binding's cancelled flag as a rejected frame does (`host.rs` `route`), without the sweep that
            /// `cancel_binding` makes: what a row needs to show that a retry is refused for a cancelled binding.
            pub(crate) fn mark_cancelled_for_test(&self) {
                match &self.kind {
                    Kind::Live { binding, .. } => binding.cancelled.store(true, Release),
                    Kind::Terminal(_) | Kind::Remote(_) => panic!("only a local operation has a binding"),
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
        match &self.kind {
            Kind::Live { host, binding } => {
                cancel_binding(binding);
                binding.sealed.store(true, Release);
                host.retire_if_done(binding);
            }
            Kind::Remote(op) => op.abandon(),
            Kind::Terminal(_) => {}
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
pub(crate) fn cancel_binding(binding: &Arc<BindingShared>) -> bool {
    if binding.cancelled.swap(true, AcqRel) {
        return false;
    }
    {
        let mut state = binding.instance.lock();
        // An opening that holds a lease for its proof goes first: the lease is another binding's idle connection, still
        // clean, and the pool's cancellation would close it (contract section 7.1).
        cancel_authorizing(&mut state, binding.id);
        state.pool.cancel_operation(&Owner::new(
            binding.session.clone(),
            binding.operation.clone(),
        ));
        binding.instance.pump(&mut state);
    }
    binding.instance.pumped();
    true
}

/// Cancels one operation. `Send + Sync + Clone`; it holds the binding weakly, so it keeps nothing alive.
#[derive(Clone)]
pub struct OperationCancel(CancelKind);
#[derive(Clone)]
enum CancelKind {
    Local(Weak<BindingShared>),
    Remote(RemoteCancel),
}
impl OperationCancel {
    pub fn cancel(&self) -> CancelState {
        match &self.0 {
            CancelKind::Local(binding) => match binding.upgrade() {
                Some(binding) if !binding.retired.load(Acquire) => {
                    if cancel_binding(&binding) {
                        CancelState::Cancelled
                    } else {
                        CancelState::AlreadyCancelled
                    }
                }
                _ => CancelState::Gone,
            },
            CancelKind::Remote(cancel) => cancel.cancel(),
        }
    }
}

/// See [`Operation::registered`].
pub struct Registered<'a> {
    operation: &'a Operation,
}
impl Future for Registered<'_> {
    type Output = Registration;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Registration> {
        match &self.operation.kind {
            Kind::Remote(op) => op.poll_registered(cx),
            _ => Poll::Ready(self.operation.registration()),
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
pub struct Opening(OpeningKind);
enum OpeningKind {
    Local(LocalOpening),
    Remote(RemoteOpening),
}
impl Opening {
    /// Submits the opening: from here its lease, its connection and the start of its exchange are the host's
    /// driver's to progress, whether or not the returned future is polled (contract section 6: progress is
    /// independent of the application's calls). The future yields the exchange once the driver has started it, on a
    /// connection of this operation's profile set up under a context of its scope. Dropping the future cancels this
    /// opening only; an exchange already started for it is aborted and its connection discarded. A carrier-backed
    /// opening sends its `open` when the binding is ready and yields the exchange when the client answers `opened`.
    pub fn wait(self) -> OpenWait {
        OpenWait(match self.0 {
            OpeningKind::Local(opening) => OpenWaitKind::Local(opening.wait()),
            OpeningKind::Remote(opening) => OpenWaitKind::Remote(RemoteOpenWait::new(opening)),
        })
    }
}
struct LocalOpening {
    live: Live,
    spec: ExchangeSpec,
    /// The request's name in the binding, which tags its pool request.
    tag: String,
}
impl LocalOpening {
    fn wait(self) -> LocalOpenWait {
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
                // The binding's limits travel with every request it makes; the pool holds the binding to them.
                request.limits = Some(binding.limits);
                binding
                    .instance
                    .submit(
                        request,
                        binding.id,
                        self.spec,
                        binding.auth.clone(),
                        binding.cancelled.clone(),
                    )
                    .map_err(OpenFailure::Pool)
            }
        };
        let (submitted, early) = match submitted {
            Ok(submitted) => (Some(submitted), None),
            Err(failure) => (None, Some(failure)),
        };
        LocalOpenWait {
            live: Some(self.live),
            submitted,
            early,
        }
    }
}
/// See [`Opening::wait`].
pub struct OpenWait(OpenWaitKind);
enum OpenWaitKind {
    Local(LocalOpenWait),
    Remote(RemoteOpenWait),
}
impl OpenWait {
    /// Whether the opening is already decided: the next poll is ready. A local opening's exchange has been started by
    /// the host's driver (or has ended); a carrier-backed one has been answered or has failed.
    pub fn is_ready(&self) -> bool {
        match &self.0 {
            OpenWaitKind::Local(wait) => wait.is_ready(),
            OpenWaitKind::Remote(wait) => wait.is_ready(),
        }
    }
}
impl Future for OpenWait {
    type Output = Result<Exchange, OpenFailure>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.0 {
            OpenWaitKind::Local(wait) => Pin::new(wait).poll(cx),
            OpenWaitKind::Remote(wait) => wait.poll_open(cx).map_ok(|started| Exchange {
                stream: MessageStream(StreamKind::Remote(started.stream)),
                back: Back::Remote {
                    reused: started.reused,
                    live: Some(started.live),
                },
            }),
        }
    }
}
struct LocalOpenWait {
    live: Option<Live>,
    submitted: Option<Arc<Slot<Opened>>>,
    early: Option<OpenFailure>,
}
impl LocalOpenWait {
    fn is_ready(&self) -> bool {
        self.early.is_some() || self.submitted.as_ref().is_some_and(|slot| slot.is_filled())
    }
}
impl Future for LocalOpenWait {
    type Output = Result<Exchange, OpenFailure>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        if let Some(failure) = this.early.take() {
            return Poll::Ready(Err(failure));
        }
        let (Some(live), Some(slot)) = (&this.live, &this.submitted) else {
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
                    stream: MessageStream(StreamKind::Local {
                        io: Some(io),
                        binding: binding.clone(),
                    }),
                    back: Back::Local {
                        lease: Some(LeaseGuard {
                            instance: binding.instance.clone(),
                            lease: Some(lease),
                        }),
                        reused,
                        _live: Some(live),
                    },
                }))
            }
        }
    }
}
impl Drop for LocalOpenWait {
    fn drop(&mut self) {
        if let (Some(slot), Some(live)) = (self.submitted.take(), &self.live) {
            live.binding.instance.abandon(&slot);
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
#[non_exhaustive]
pub enum StreamError {
    /// The operation was cancelled (or its host closed): the exchange is over.
    Cancelled,
    /// The adapter's I/O failed; `ErrorKind::WouldBlock` means not yet, on either side of a carrier.
    Io(std::io::Error),
    /// A carrier-backed exchange: the carrier ended under it. A `receive_pack_exchange` that had started reports
    /// `Effect::Possible`.
    CarrierLost { effect: Effect },
    /// A carrier-backed exchange: the client's exchange failed with this code and effect.
    Failed { code: ErrorCode, effect: Effect },
    /// A carrier-backed exchange: its write side has ended, or the exchange is closed.
    Closed,
    /// A carrier-backed exchange: the stream broke a rule of the transport.
    Protocol,
}
/// One exchange's byte stream, over the adapter's own I/O (or, carrier-backed, over the lane). Reads and writes are
/// serialized by `&mut`, and a call that cannot proceed returns `Io(WouldBlock)`, never blocks.
pub struct MessageStream(StreamKind);
enum StreamKind {
    Local {
        /// Taken only by the exchange's end, which consumes the exchange.
        io: Option<Box<dyn ExchangeIo>>,
        binding: Arc<BindingShared>,
    },
    Remote(RemoteStream),
}
impl MessageStream {
    pub fn write(&mut self, data: &[u8]) -> Result<usize, StreamError> {
        match &mut self.0 {
            StreamKind::Remote(stream) => stream.write(data),
            StreamKind::Local { .. } => self.io()?.write(data).map_err(StreamError::Io),
        }
    }
    pub fn read(&mut self, out: &mut [u8]) -> Result<usize, StreamError> {
        match &mut self.0 {
            StreamKind::Remote(stream) => stream.read(out),
            StreamKind::Local { .. } => self.io()?.read(out).map_err(StreamError::Io),
        }
    }
    fn io(&mut self) -> Result<&mut Box<dyn ExchangeIo>, StreamError> {
        let StreamKind::Local { io, binding } = &mut self.0 else {
            return Err(StreamError::Closed);
        };
        if binding.cancelled.load(Acquire) {
            return Err(StreamError::Cancelled);
        }
        io.as_mut().ok_or(StreamError::Cancelled)
    }
    /// The local exchange's adapter I/O, taken by the exchange's end.
    fn take_io(&mut self) -> Option<Box<dyn ExchangeIo>> {
        match &mut self.0 {
            StreamKind::Local { io, .. } => io.take(),
            StreamKind::Remote(_) => None,
        }
    }
    fn local_binding(&self) -> Option<&Arc<BindingShared>> {
        match &self.0 {
            StreamKind::Local { binding, .. } => Some(binding),
            StreamKind::Remote(_) => None,
        }
    }
}

/// One exclusive lease and its stream. `Send`, not `Clone`; dropping it discards the lease (or, carrier-backed,
/// cancels the exchange).
pub struct Exchange {
    stream: MessageStream,
    back: Back,
}
enum Back {
    Local {
        lease: Option<LeaseGuard>,
        reused: bool,
        _live: Option<Live>,
    },
    Remote {
        reused: bool,
        live: Option<RemoteLive>,
    },
}
impl Exchange {
    pub fn stream(&mut self) -> &mut MessageStream {
        &mut self.stream
    }
    /// Whether the connection served an exchange before this one (the observation's `reused`): it was idle, of this
    /// operation's profile, and set up under a context of this operation's scope. Carrier-backed, the client's
    /// connection.
    pub fn reused(&self) -> bool {
        match &self.back {
            Back::Local { reused, .. } | Back::Remote { reused, .. } => *reused,
        }
    }
    /// Ends the exchange: its connection goes back to idle only if the adapter proves the protocol clean and the
    /// operation was not cancelled. The orderly end starts when this is built, and the host's driver settles the
    /// lease when the adapter confirms it (`ExchangeIo::poll_finish`), whether or not the future is polled.
    pub fn finish(mut self) -> ExchangeFinish {
        let cancelled = self
            .stream
            .local_binding()
            .is_some_and(|binding| binding.cancelled.load(Acquire));
        match &mut self.back {
            Back::Remote { live, .. } => ExchangeFinish(match (live.take(), &self.stream.0) {
                (Some(live), StreamKind::Remote(stream)) => FinishKind::Remote(stream.finish(live)),
                _ => FinishKind::Local(Cell::ready(discarded())),
            }),
            Back::Local { lease, .. } => {
                let (Some(mut io), Some(mut lease)) = (self.stream.take_io(), lease.take()) else {
                    return ExchangeFinish(FinishKind::Local(Cell::ready(discarded())));
                };
                if cancelled {
                    io.abort();
                    lease.release(Disposition::Discarded);
                    return ExchangeFinish(FinishKind::Local(Cell::ready(discarded())));
                }
                let id = lease.lease.take().expect("a live lease");
                ExchangeFinish(FinishKind::Local(lease.instance.settle(io, id)))
            }
        }
    }
    /// Ends the exchange abnormally: its connection is discarded.
    pub fn abort(mut self) {
        if let Some(mut io) = self.stream.take_io() {
            io.abort();
        }
        if let Back::Local { lease, .. } = &mut self.back
            && let Some(lease) = lease.take()
        {
            lease.release(Disposition::Discarded);
        }
        // A carrier-backed exchange: dropping its `Live` cancels the exchange and the client discards its connection.
    }
}
fn discarded() -> ExchangeEnd {
    ExchangeEnd {
        disposition: Disposition::Discarded,
        cleanup: crate::CleanupStatus::Complete,
        failure: None,
    }
}
impl Drop for Exchange {
    fn drop(&mut self) {
        if let Back::Local { lease: Some(_), .. } = &self.back
            && let StreamKind::Local { io: Some(io), .. } = &mut self.stream.0
        {
            io.abort();
        }
    }
}
/// The receipt of a finished exchange, set by the host's driver once the adapter confirmed the exchange's end.
pub struct ExchangeFinish(FinishKind);
enum FinishKind {
    Local(Cell<ExchangeEnd>),
    Remote(RemoteFinish),
}
impl Future for ExchangeFinish {
    type Output = ExchangeEnd;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<ExchangeEnd> {
        match &mut self.0 {
            FinishKind::Local(cell) => cell.poll(cx),
            FinishKind::Remote(finish) => Pin::new(finish).poll(cx),
        }
    }
}
impl fmt::Debug for Exchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Exchange").finish_non_exhaustive()
    }
}
