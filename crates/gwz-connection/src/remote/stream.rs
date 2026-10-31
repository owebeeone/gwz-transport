//! A carrier-backed opening, exchange and byte stream (GwzSplitCommandChannel section 3.3 and 3.4), the parts the crate's
//! `Opening`, `Exchange` and `MessageStream` hold when the connection is carrier-backed. A stream is a `StreamMachine`
//! (the transport's flow-controlled byte stream, initiator side) whose `data`, `window`, `flush`, `end_write` and
//! `close` envelopes the pump carries. Nothing here is public.
use super::{
    backend::{RemoteOpening, Shared},
    core::{ExchEnd, Stage, Wait},
};
use crate::{ExchangeEnd, OpenFailure, StreamError};
use gwz_transport::{
    protocol::{Disposition, Effect, ErrorCode},
    stream,
};
use std::{
    future::Future,
    io::ErrorKind,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

/// Holds a binding open while an opening or an exchange of it is live (the last to go lets it retire), and cancels
/// the exchange if its handle goes before the exchange ended.
pub(crate) struct Live {
    shared: Arc<Shared>,
    serial: u64,
    token: u64,
}
impl Live {
    pub(super) fn new(shared: Arc<Shared>, serial: u64, token: u64) -> Self {
        Self {
            shared,
            serial,
            token,
        }
    }
}
impl Drop for Live {
    fn drop(&mut self) {
        let (serial, token) = (self.serial, self.token);
        self.shared.with(|state| {
            let Some(entry) = state.bindings.get_mut(&serial) else {
                return;
            };
            entry.live = entry.live.saturating_sub(1);
            if let Some(exch) = entry.exchanges.get_mut(&token) {
                exch.dropped = true;
                match exch.stage {
                    Stage::Queued => {
                        exch.spec = None;
                        exch.stage = Stage::Ended;
                    }
                    Stage::Open | Stage::Closing => {
                        if let Some(machine) = exch.machine.as_mut() {
                            machine.cancel();
                        }
                    }
                    Stage::Opening | Stage::Ended => {}
                }
                if exch.stage == Stage::Ended {
                    entry.exchanges.remove(&token);
                }
            }
        });
    }
}

/// See `Opening::wait`.
pub(crate) struct RemoteOpenWait {
    live: Option<Live>,
}
impl RemoteOpenWait {
    pub(crate) fn new(opening: RemoteOpening) -> Self {
        Self {
            live: Some(opening.live),
        }
    }
    /// Whether the next poll is ready: the opening is decided, or its record is gone (which polls as `Cancelled`).
    pub(crate) fn is_ready(&self) -> bool {
        let Some(live) = &self.live else {
            return true;
        };
        live.shared.with(|state| {
            state
                .bindings
                .get(&live.serial)
                .and_then(|b| b.exchanges.get(&live.token))
                .is_none_or(|e| e.outcome.is_some())
        })
    }
    pub(crate) fn poll_open(&mut self, cx: &mut Context<'_>) -> Poll<Result<Started, OpenFailure>> {
        let Some(live) = self.live.as_ref() else {
            return Poll::Ready(Err(OpenFailure::Cancelled));
        };
        let (shared, serial, token) = (live.shared.clone(), live.serial, live.token);
        let waker = cx.waker();
        let decided = shared.with(|state| {
            // No binding or exchange record is a terminal answer, never a wait that nothing wakes.
            let Some(exch) = state
                .bindings
                .get_mut(&serial)
                .and_then(|entry| entry.exchanges.get_mut(&token))
            else {
                return Some(Err(OpenFailure::Cancelled));
            };
            match exch.outcome.take() {
                Some(Ok(())) => Some(Ok(exch.reused)),
                Some(Err(failure)) => {
                    exch.dropped = true;
                    Some(Err(failure))
                }
                None => {
                    state.wait_for(Wait::Opening(serial, token), waker);
                    None
                }
            }
        });
        match decided {
            None => Poll::Pending,
            Some(Ok(reused)) => {
                let live = self.live.take().expect("checked above");
                Poll::Ready(Ok(Started {
                    stream: RemoteStream {
                        shared,
                        serial,
                        token,
                    },
                    reused,
                    live,
                }))
            }
            Some(Err(failure)) => Poll::Ready(Err(failure)),
        }
    }
}
/// An opening that was answered `opened`.
pub(crate) struct Started {
    pub(crate) stream: RemoteStream,
    pub(crate) reused: bool,
    pub(crate) live: Live,
}

fn from_stream(error: stream::Error, effect: Effect) -> StreamError {
    match error {
        stream::Error::WouldBlock => StreamError::Io(ErrorKind::WouldBlock.into()),
        stream::Error::Cancelled => StreamError::Cancelled,
        stream::Error::CarrierLost => StreamError::CarrierLost { effect },
        stream::Error::PeerFailed { code, effect } => StreamError::Failed { code, effect },
        stream::Error::WriteClosed | stream::Error::Closed => StreamError::Closed,
        _ => StreamError::Protocol,
    }
}
fn from_end(end: &ExchEnd) -> StreamError {
    match end.failure {
        Some((ErrorCode::CarrierLost, effect)) => StreamError::CarrierLost { effect },
        Some((ErrorCode::Cancelled, _)) => StreamError::Cancelled,
        Some((code, effect)) => StreamError::Failed { code, effect },
        None => StreamError::Closed,
    }
}

/// One exchange's byte stream, over the lane. A call that cannot proceed returns `Io(WouldBlock)`, never blocks (the
/// same rule as the local `ExchangeIo`).
pub(crate) struct RemoteStream {
    shared: Arc<Shared>,
    serial: u64,
    token: u64,
}
impl RemoteStream {
    fn call<R>(
        &self,
        f: impl FnOnce(&mut stream::StreamMachine) -> Result<R, stream::Error>,
    ) -> Result<R, StreamError> {
        let (serial, token) = (self.serial, self.token);
        self.shared.with(|state| {
            let exch = state
                .bindings
                .get_mut(&serial)
                .and_then(|b| b.exchanges.get_mut(&token))
                .ok_or(StreamError::Closed)?;
            if let Some(end) = &exch.end {
                return Err(from_end(end));
            }
            let effect = exch.effect_on_loss();
            let machine = exch.machine.as_mut().ok_or(StreamError::Closed)?;
            f(machine).map_err(|error| from_stream(error, effect))
        })
    }
    pub(crate) fn write(&mut self, data: &[u8]) -> Result<usize, StreamError> {
        self.call(|machine| machine.write(data))
    }
    /// Bytes the peer sent; `Ok(0)` once the peer has ended its writes and everything was read.
    pub(crate) fn read(&mut self, out: &mut [u8]) -> Result<usize, StreamError> {
        self.call(|machine| machine.read(out))
    }
    /// Ends the exchange in order: the writes end, `close` is sent, and the future resolves with the client's `closed`.
    /// The orderly end starts when this is called, whether or not the future is polled.
    pub(crate) fn finish(&self, live: Live) -> RemoteFinish {
        let (serial, token) = (self.serial, self.token);
        let shared = self.shared.clone();
        shared.with(|state| {
            if let Some(exch) = state
                .bindings
                .get_mut(&serial)
                .and_then(|b| b.exchanges.get_mut(&token))
                && exch.end.is_none()
                && let Some(machine) = exch.machine.as_mut()
                && machine.start_close().is_ok()
            {
                exch.stage = Stage::Closing;
                exch.finish_requested = true;
            }
        });
        RemoteFinish {
            _live: Some(live),
            shared,
            serial,
            token,
        }
    }
}

/// See `Exchange::finish`.
pub(crate) struct RemoteFinish {
    _live: Option<Live>,
    shared: Arc<Shared>,
    serial: u64,
    token: u64,
}
impl Future for RemoteFinish {
    type Output = ExchangeEnd;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<ExchangeEnd> {
        let waker = cx.waker();
        let (serial, token) = (self.serial, self.token);
        let end = self.shared.with(|state| {
            let Some(exch) = state
                .bindings
                .get(&serial)
                .and_then(|b| b.exchanges.get(&token))
            else {
                // No record: nothing is left to wait for. A record leaves the table only with its handle, so this is not
                // reached while this future holds its `Live`; if it is, the push is reported as possibly applied.
                return Some(ExchEnd {
                    disposition: Disposition::Discarded,
                    failure: Some((ErrorCode::Cancelled, Effect::Possible)),
                });
            };
            let end = exch.end.clone();
            if end.is_none() {
                state.wait_for(Wait::Ending(serial, token), waker);
            }
            end
        });
        match end {
            Some(end) => Poll::Ready(ExchangeEnd {
                disposition: end.disposition,
                cleanup: crate::CleanupStatus::Complete,
                effect: end.failure.map_or(Effect::None, |(_, effect)| effect),
                failure: end.failure,
            }),
            None => Poll::Pending,
        }
    }
}
