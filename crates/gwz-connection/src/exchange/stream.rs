//! One exchange's byte stream and its futures (movement from `exchange.rs`, HTTPS model step H2b).
use crate::{
    ExchangeIo,
    binding::{AffinityKey, BindingShared},
    remote::RemoteStream,
};
use gwz_transport::{
    pool::ConnectionId,
    protocol::{Effect, ErrorCode},
};
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, atomic::Ordering::Acquire},
    task::{Context, Poll, Waker},
};

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
    /// `Exchange::response_head` on an exchange whose scheme has no response head (SSH).
    WrongScheme,
    /// `Exchange::response_head` after it already resolved.
    WrongState,
}
/// How much of a stream's bytes have moved: fixed-size and monotonic, so a caller whose `write_all` future was dropped
/// resolves it by reading `accepted_write`, never by resending the original buffer (contract section 9).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct StreamProgress {
    /// Bytes the stream accepted for the peer, a prefix of everything written, which a dropped future cannot retract.
    pub accepted_write: u64,
    /// Bytes the caller has read.
    pub consumed_read: u64,
    /// The highest written offset the peer is known to have received (not an application acknowledgment); `None` when
    /// the adapter cannot say.
    pub peer_confirmed: Option<u64>,
}

/// One exchange's byte stream, over the adapter's own I/O (or, carrier-backed, over the lane). Reads and writes are
/// serialized by `&mut`. `read`, `write`, `write_all`, `flush` and `shutdown_write` are futures that wait on the
/// adapter's waker and never sleep-poll; `try_read` and `try_write` are the non-waiting calls, which return
/// `Io(WouldBlock)` when they cannot proceed.
///
/// A local exchange implements all of them over `ExchangeIo`. A carrier-backed stream's data path gives no wake yet
/// (Phase 2, step 2.3g): its `read`, `write` and `write_all` re-poll their task in a busy loop until the lane moves,
/// and its `flush` and `shutdown_write` return `Io(Unsupported)`.
pub struct MessageStream {
    pub(super) kind: StreamKind,
    accepted: u64,
    consumed: u64,
}
pub(super) enum StreamKind {
    Local {
        /// Taken only by the exchange's end, which consumes the exchange.
        io: Option<Box<dyn ExchangeIo>>,
        binding: Arc<BindingShared>,
        /// Where the exchange runs, for its binding's affinity table until it is noted there (HTTPS model 3.1);
        /// `None` once noted, or for a path the table does not record.
        sample: Option<(AffinityKey, ConnectionId)>,
    },
    Remote(RemoteStream),
}
impl MessageStream {
    pub(super) fn new(kind: StreamKind) -> Self {
        Self {
            kind,
            accepted: 0,
            consumed: 0,
        }
    }
    /// Accepts a nonzero prefix of `data`, or waits for room. An empty `data` is accepted at once, as a no-op.
    pub fn write<'a>(&'a mut self, data: &'a [u8]) -> StreamWrite<'a> {
        StreamWrite { stream: self, data }
    }
    /// Accepts all of `data`. When the future is dropped or fails, `progress().accepted_write` says how much of it
    /// the stream took; the rest was not sent.
    pub fn write_all<'a>(&'a mut self, data: &'a [u8]) -> StreamWriteAll<'a> {
        StreamWriteAll {
            stream: self,
            data,
            taken: 0,
        }
    }
    /// Reads into `out`: `Ok(0)` is the validated end of the response, or an empty `out`. A dropped future consumed no
    /// bytes.
    pub fn read<'a>(&'a mut self, out: &'a mut [u8]) -> StreamRead<'a> {
        StreamRead { stream: self, out }
    }
    /// Resolves when the adapter has pushed out what it buffered.
    pub fn flush(&mut self) -> StreamFlush<'_> {
        StreamFlush { stream: self }
    }
    /// Ends the request's writes and leaves the response readable (a POST's body end). Later writes fail.
    pub fn shutdown_write(&mut self) -> StreamShutdown<'_> {
        StreamShutdown { stream: self }
    }
    /// The bytes moved so far.
    pub fn progress(&self) -> StreamProgress {
        StreamProgress {
            accepted_write: self.accepted,
            consumed_read: self.consumed,
            peer_confirmed: match &self.kind {
                StreamKind::Local { io, .. } => io.as_ref().and_then(|io| io.peer_confirmed()),
                StreamKind::Remote(_) => None,
            },
        }
    }
    /// A write that does not wait: one poll with a waker that does nothing, `Io(WouldBlock)` when it is pending.
    pub fn try_write(&mut self, data: &[u8]) -> Result<usize, StreamError> {
        match self.poll_write(&mut Context::from_waker(Waker::noop()), data) {
            Poll::Ready(result) => result,
            Poll::Pending => Err(StreamError::Io(io::ErrorKind::WouldBlock.into())),
        }
    }
    /// A read that does not wait: one poll with a waker that does nothing, `Io(WouldBlock)` when it is pending.
    pub fn try_read(&mut self, out: &mut [u8]) -> Result<usize, StreamError> {
        match self.poll_read(&mut Context::from_waker(Waker::noop()), out) {
            Poll::Ready(result) => result,
            Poll::Pending => Err(StreamError::Io(io::ErrorKind::WouldBlock.into())),
        }
    }
    fn io(&mut self) -> Result<&mut Box<dyn ExchangeIo>, StreamError> {
        let StreamKind::Local { io, binding, .. } = &mut self.kind else {
            return Err(StreamError::Closed);
        };
        if binding.cancelled.load(Acquire) {
            return Err(StreamError::Cancelled);
        }
        io.as_mut().ok_or(StreamError::Cancelled)
    }
    /// Runs one adapter poll for a local stream, after registering `cx` with the instance so a cancellation wakes it.
    /// The waker is registered before the cancelled flag is read, so a cancellation cannot fall between the two.
    pub(super) fn poll_local<R>(
        &mut self,
        cx: &mut Context<'_>,
        poll: impl FnOnce(&mut dyn ExchangeIo, &mut Context<'_>) -> Poll<io::Result<R>>,
    ) -> Poll<Result<R, StreamError>> {
        if let StreamKind::Local { binding, .. } = &self.kind {
            binding.instance.wait(cx.waker());
        }
        match self.io() {
            Ok(io) => poll(io.as_mut(), cx).map(|result| result.map_err(StreamError::Io)),
            Err(error) => Poll::Ready(Err(error)),
        }
    }
    /// A carrier-backed call as a poll: "not yet" re-polls the task (the lane gives no wake yet, step 2.3g).
    fn poll_remote<R>(
        cx: &mut Context<'_>,
        call: Result<R, StreamError>,
    ) -> Poll<Result<R, StreamError>> {
        match call {
            Err(StreamError::Io(error)) if error.kind() == io::ErrorKind::WouldBlock => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            ready => Poll::Ready(ready),
        }
    }
    fn poll_write(
        &mut self,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<Result<usize, StreamError>> {
        if data.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let polled = match &mut self.kind {
            StreamKind::Remote(stream) => Self::poll_remote(cx, stream.write(data)),
            StreamKind::Local { .. } => self.poll_local(cx, |io, cx| io.poll_write(cx, data)),
        };
        if let Poll::Ready(Ok(count)) = &polled {
            self.accepted += *count as u64;
        }
        polled
    }
    fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        out: &mut [u8],
    ) -> Poll<Result<usize, StreamError>> {
        if out.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let polled = match &mut self.kind {
            StreamKind::Remote(stream) => Self::poll_remote(cx, stream.read(out)),
            StreamKind::Local { .. } => self.poll_local(cx, |io, cx| io.poll_read(cx, out)),
        };
        if let Poll::Ready(Ok(count)) = &polled {
            if self.consumed == 0 && *count > 0 {
                self.sample_affinity();
            }
            self.consumed += *count as u64;
        }
        polled
    }
    /// Notes the exchange's connection in its binding's affinity table if the adapter says that the connection is
    /// authenticated by a mechanism that lives on it (`ExchangeIo::connection_bound`): read when the first response byte
    /// is delivered and once more when the exchange finishes (`Exchange::finish`), so a client that read the first byte
    /// finds the hint, and a response without a body is noted too (HTTPS model 3.1, Consistency P3-4). At most once.
    pub(super) fn sample_affinity(&mut self) {
        if let StreamKind::Local {
            io: Some(io),
            binding,
            sample,
        } = &mut self.kind
            && sample.is_some()
            && io.connection_bound()
            && let Some((at, connection)) = sample.take()
        {
            binding.affinity.note(at, connection);
        }
    }
    /// The local exchange's adapter I/O, taken by the exchange's end.
    pub(super) fn take_io(&mut self) -> Option<Box<dyn ExchangeIo>> {
        match &mut self.kind {
            StreamKind::Local { io, .. } => io.take(),
            StreamKind::Remote(_) => None,
        }
    }
    pub(super) fn local_binding(&self) -> Option<&Arc<BindingShared>> {
        match &self.kind {
            StreamKind::Local { binding, .. } => Some(binding),
            StreamKind::Remote(_) => None,
        }
    }
}

/// See [`MessageStream::write`].
pub struct StreamWrite<'a> {
    stream: &'a mut MessageStream,
    data: &'a [u8],
}
impl Future for StreamWrite<'_> {
    type Output = Result<usize, StreamError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        this.stream.poll_write(cx, this.data)
    }
}
/// See [`MessageStream::write_all`].
pub struct StreamWriteAll<'a> {
    stream: &'a mut MessageStream,
    data: &'a [u8],
    taken: usize,
}
impl Future for StreamWriteAll<'_> {
    type Output = Result<(), StreamError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        while this.taken < this.data.len() {
            match this.stream.poll_write(cx, &this.data[this.taken..]) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                // An adapter that accepts nothing and does not fail would loop forever.
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(StreamError::Io(io::ErrorKind::WriteZero.into())));
                }
                Poll::Ready(Ok(count)) => this.taken += count,
            }
        }
        Poll::Ready(Ok(()))
    }
}
/// See [`MessageStream::read`].
pub struct StreamRead<'a> {
    stream: &'a mut MessageStream,
    out: &'a mut [u8],
}
impl Future for StreamRead<'_> {
    type Output = Result<usize, StreamError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        this.stream.poll_read(cx, this.out)
    }
}
/// See [`MessageStream::flush`].
pub struct StreamFlush<'a> {
    stream: &'a mut MessageStream,
}
impl Future for StreamFlush<'_> {
    type Output = Result<(), StreamError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &self.stream.kind {
            StreamKind::Remote(_) => {
                Poll::Ready(Err(StreamError::Io(io::ErrorKind::Unsupported.into())))
            }
            StreamKind::Local { .. } => self.stream.poll_local(cx, |io, cx| io.poll_flush(cx)),
        }
    }
}
/// See [`MessageStream::shutdown_write`].
pub struct StreamShutdown<'a> {
    stream: &'a mut MessageStream,
}
impl Future for StreamShutdown<'_> {
    type Output = Result<(), StreamError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &self.stream.kind {
            StreamKind::Remote(_) => {
                Poll::Ready(Err(StreamError::Io(io::ErrorKind::Unsupported.into())))
            }
            StreamKind::Local { .. } => self
                .stream
                .poll_local(cx, |io, cx| io.poll_shutdown_write(cx)),
        }
    }
}
