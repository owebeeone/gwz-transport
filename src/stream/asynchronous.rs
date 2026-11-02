//! Executor-independent futures; the host delivers messages and clock ticks.
use super::*;
use crate::protocol::{Disposition, Effect, Envelope, ErrorCode, Facts};
use std::{
    collections::BTreeMap,
    future::poll_fn,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};

struct Runtime {
    machine: StreamMachine,
    waiters: BTreeMap<u64, Registration>,
    serial: u64,
}
struct Registration {
    waker: Waker,
    dispatcher: bool,
}
struct Shared {
    runtime: Mutex<Runtime>,
    owners: AtomicUsize,
}
impl Shared {
    fn change<T>(&self, action: impl FnOnce(&mut StreamMachine) -> T) -> T {
        let (result, wakes) = {
            let mut state = self.runtime.lock().expect("stream lock poisoned");
            let before = state.machine.revision;
            let result = action(&mut state.machine);
            let wakes: Vec<_> = if before != state.machine.revision {
                state
                    .waiters
                    .values()
                    .map(|entry| entry.waker.clone())
                    .collect()
            } else {
                Vec::new()
            };
            (result, wakes)
        };
        for waker in wakes {
            waker.wake();
        }
        result
    }
}

// A cancelled future unregisters itself, including when it never becomes ready.
struct Waiter {
    shared: Arc<Shared>,
    id: Option<u64>,
    dispatcher: bool,
}
impl Waiter {
    fn new(shared: &Arc<Shared>) -> Self {
        Self {
            shared: shared.clone(),
            id: None,
            dispatcher: false,
        }
    }
    fn poll<T>(
        &mut self,
        cx: &Context<'_>,
        action: impl FnOnce(&mut StreamMachine) -> Result<T, Error>,
    ) -> Poll<Result<T, Error>> {
        let (result, wakes) = {
            let mut state = self.shared.runtime.lock().expect("stream lock poisoned");
            let before = state.machine.revision;
            let mut result = action(&mut state.machine);
            if result
                .as_ref()
                .is_err_and(|error| *error == Error::WouldBlock)
            {
                if self.id.is_none() {
                    let occupied = state
                        .waiters
                        .values()
                        .filter(|entry| entry.dispatcher == self.dispatcher)
                        .count();
                    let limit = if self.dispatcher {
                        1
                    } else {
                        state.machine.config.max_waiters
                    };
                    if occupied >= limit || state.serial == u64::MAX {
                        result = Err(Error::WaiterCapacity);
                    } else {
                        state.serial += 1;
                        self.id = Some(state.serial);
                    }
                }
                if let Some(id) = self.id {
                    state.waiters.insert(
                        id,
                        Registration {
                            waker: cx.waker().clone(),
                            dispatcher: self.dispatcher,
                        },
                    );
                }
            } else if let Some(id) = self.id.take() {
                state.waiters.remove(&id);
            }
            let wakes: Vec<_> = if before != state.machine.revision {
                state
                    .waiters
                    .iter()
                    .filter(|(id, _)| Some(**id) != self.id)
                    .map(|(_, entry)| entry.waker.clone())
                    .collect()
            } else {
                Vec::new()
            };
            let result = match result {
                Err(Error::WouldBlock) => Poll::Pending,
                result => Poll::Ready(result),
            };
            (result, wakes)
        };
        for waker in wakes {
            waker.wake();
        }
        result
    }
}
impl Drop for Waiter {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            self.shared
                .runtime
                .lock()
                .expect("stream lock poisoned")
                .waiters
                .remove(&id);
        }
    }
}

/// A caller-held wait registration for [`Stream::poll_read`] and [`Stream::poll_write`]. A `Pending` poll leaves it
/// registered with the latest waker, holding one place against `max_waiters` however often it is polled, until a poll
/// returns `Ready` or the slot drops. Hold one slot per direction a task waits on. A slot polled on another stream
/// leaves its earlier stream and registers on the new one.
#[derive(Default)]
pub struct StreamWaiter {
    waiter: Option<Waiter>,
}
impl StreamWaiter {
    pub fn new() -> Self {
        Self::default()
    }
    fn bind(&mut self, shared: &Arc<Shared>) -> &mut Waiter {
        let bound = self
            .waiter
            .as_ref()
            .is_some_and(|waiter| Arc::ptr_eq(&waiter.shared, shared));
        if !bound {
            // Unregister a waiter of another stream before registering here, so it never holds a second place.
            self.waiter = None;
        }
        self.waiter.get_or_insert_with(|| Waiter::new(shared))
    }
}

/// File-like asynchronous access to one exchange. Clones share byte positions;
/// concurrent reads divide the data, they do not create a broadcast subscription.
pub struct Stream {
    shared: Arc<Shared>,
}
/// Host-owned ordered message boundary. It performs no physical I/O. Drop means
/// delivery has been lost; all unfinished operations are woken with an error.
pub struct MessageEndpoint {
    shared: Arc<Shared>,
}

impl Stream {
    pub fn new(config: Config) -> Result<(Self, MessageEndpoint), Error> {
        let shared = Arc::new(Shared {
            runtime: Mutex::new(Runtime {
                machine: StreamMachine::new(config)?,
                waiters: BTreeMap::new(),
                serial: 0,
            }),
            owners: AtomicUsize::new(1),
        });
        Ok((
            Self {
                shared: shared.clone(),
            },
            MessageEndpoint { shared },
        ))
    }
    pub async fn read(&self, output: &mut [u8]) -> Result<usize, Error> {
        let mut slot = StreamWaiter::new();
        poll_fn(|cx| self.poll_read(cx, &mut slot, output)).await
    }
    pub async fn write(&self, input: &[u8]) -> Result<usize, Error> {
        let mut slot = StreamWaiter::new();
        poll_fn(|cx| self.poll_write(cx, &mut slot, input)).await
    }
    /// Reads into `output`, or registers `cx` in `slot` and returns `Pending`; a `Pending` poll consumed nothing.
    /// The slot stays registered between polls, so the delivery or end that lets the read proceed wakes `cx`.
    pub fn poll_read(
        &self,
        cx: &mut Context<'_>,
        slot: &mut StreamWaiter,
        output: &mut [u8],
    ) -> Poll<Result<usize, Error>> {
        slot.bind(&self.shared)
            .poll(cx, |machine| machine.read(output))
    }
    /// Accepts a prefix of `input`, or registers `cx` in `slot` and returns `Pending` having accepted nothing. The
    /// slot stays registered between polls, so the credit or the dispatcher's take that makes room wakes `cx`.
    pub fn poll_write(
        &self,
        cx: &mut Context<'_>,
        slot: &mut StreamWaiter,
        input: &[u8],
    ) -> Poll<Result<usize, Error>> {
        slot.bind(&self.shared)
            .poll(cx, |machine| machine.write(input))
    }
    pub async fn write_all(&self, mut input: &[u8]) -> Result<(), Error> {
        while !input.is_empty() {
            let count = self.write(input).await?;
            input = &input[count..];
        }
        Ok(())
    }
    pub async fn flush(&self) -> Result<(), Error> {
        let ticket = self.shared.change(StreamMachine::start_flush)?;
        let mut waiter = Waiter::new(&self.shared);
        poll_fn(|cx| {
            waiter.poll(cx, |machine| {
                if machine.flush_complete(ticket)? {
                    Ok(())
                } else {
                    Err(Error::WouldBlock)
                }
            })
        })
        .await
    }
    pub async fn end_write(&self) -> Result<(), Error> {
        self.shared.change(StreamMachine::end_write)?;
        let mut waiter = Waiter::new(&self.shared);
        poll_fn(|cx| {
            waiter.poll(cx, |machine| {
                machine.live()?;
                if machine.end_sent {
                    Ok(())
                } else {
                    Err(Error::WouldBlock)
                }
            })
        })
        .await
    }
    pub async fn close(&self) -> Result<CloseResult, Error> {
        self.shared.change(StreamMachine::start_close)?;
        let mut waiter = Waiter::new(&self.shared);
        poll_fn(|cx| waiter.poll(cx, |machine| machine.close_result())).await
    }
    pub fn cancel(&self) {
        self.shared.change(StreamMachine::cancel);
    }
    pub fn retained_failure(&self) -> Option<crate::protocol::Failure> {
        self.shared
            .change(|machine| machine.retained_failure().cloned())
    }
    pub fn retained_failure_facts(&self) -> Option<Facts> {
        self.shared
            .change(|machine| machine.retained_failure_facts().cloned())
    }
}
impl Clone for Stream {
    fn clone(&self) -> Self {
        self.shared.owners.fetch_add(1, Ordering::Relaxed);
        Self {
            shared: self.shared.clone(),
        }
    }
}
impl Drop for Stream {
    fn drop(&mut self) {
        if self.shared.owners.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.shared.change(StreamMachine::cancel);
        }
    }
}
impl MessageEndpoint {
    pub async fn next_message(&self) -> Result<Option<Envelope>, Error> {
        let mut waiter = Waiter::new(&self.shared);
        waiter.dispatcher = true;
        poll_fn(|cx| {
            waiter.poll(cx, |machine| {
                if let Some(message) = machine.next_message() {
                    return Ok(Some(message));
                }
                if machine.stats().terminal {
                    Ok(None)
                } else {
                    Err(Error::WouldBlock)
                }
            })
        })
        .await
    }
    pub fn deliver(&self, message: Envelope) -> Result<(), Error> {
        self.shared.change(|machine| machine.receive(message))
    }
    pub fn advance(&self, now_ms: u64) {
        self.shared.change(|machine| machine.advance(now_ms));
    }
    pub fn set_io_state(&self, state: IoState) -> Result<(), Error> {
        self.shared.change(|machine| machine.set_io_state(state))
    }
    pub fn record_io_progress(&self, bytes: usize) -> Result<(), Error> {
        self.shared
            .change(|machine| machine.record_io_progress(bytes))
    }
    pub fn io_status(&self) -> IoStatus {
        self.shared.change(|machine| machine.io_status())
    }
    pub fn next_deadline(&self) -> Option<u64> {
        self.shared.change(|machine| machine.next_deadline())
    }
    pub fn stats(&self) -> Snapshot {
        self.shared.change(|machine| machine.stats())
    }
    pub fn complete_close(&self, disposition: Disposition, facts: Facts) -> Result<(), Error> {
        self.shared
            .change(|machine| machine.complete_close(disposition, facts))
    }
    pub fn complete_close_failure(
        &self,
        disposition: Disposition,
        facts: Facts,
        code: ErrorCode,
        effect: Effect,
    ) -> Result<(), Error> {
        self.shared
            .change(|machine| machine.complete_close_failure(disposition, facts, code, effect))
    }
    pub fn fail_terminal(&self, failure: crate::protocol::Failure) -> Result<(), Error> {
        self.shared.change(|machine| machine.fail_terminal(failure))
    }
    pub fn retained_failure_facts(&self) -> Option<Facts> {
        self.shared
            .change(|machine| machine.retained_failure_facts().cloned())
    }
    pub fn disconnect(&self) {
        self.shared.change(StreamMachine::disconnect);
    }
    pub fn waiter_count(&self) -> usize {
        self.shared
            .runtime
            .lock()
            .expect("stream lock poisoned")
            .waiters
            .len()
    }
}
impl Drop for MessageEndpoint {
    fn drop(&mut self) {
        self.disconnect();
    }
}
