//! Executor-independent ownership and async application ports.
use super::*;
use std::{
    future::poll_fn,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Poll, Waker},
};
struct Shared {
    inner: Mutex<State>,
    owners: AtomicUsize,
    ports: AtomicUsize,
}
struct State {
    mux: Mux,
    waiters: BTreeMap<u64, Option<Waker>>,
    serial: u64,
}
impl Shared {
    fn change<T>(&self, f: impl FnOnce(&mut Mux) -> T) -> T {
        let (result, wakes) = {
            let mut state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            let result = f(&mut state.mux);
            let wakes: Vec<_> = state
                .waiters
                .values_mut()
                .filter_map(Option::take)
                .collect();
            (result, wakes)
        };
        for wake in wakes {
            wake.wake();
        }
        result
    }
}
/// Runtime-side owner. Dropping the last owner closes its application port.
pub struct Owner(Arc<Shared>);
/// Communication-side owner. This object never opens or frames a connection.
pub struct Port(Arc<Shared>);
impl Owner {
    pub fn new(mux: Mux) -> (Self, Port) {
        let shared = Arc::new(Shared {
            inner: Mutex::new(State {
                mux,
                waiters: BTreeMap::new(),
                serial: 0,
            }),
            owners: AtomicUsize::new(1),
            ports: AtomicUsize::new(1),
        });
        (Self(shared.clone()), Port(shared))
    }
    pub fn register(&self, request: &str, operation: Option<String>) -> Result<(), Error> {
        self.0.change(|m| m.register(request, operation))
    }
    pub fn begin(&self, request: &str) -> Result<(), Error> {
        self.0.change(|m| m.begin(request))
    }
    pub fn open(&self, request: &str, open: Open) -> Result<i64, Error> {
        self.0.change(|m| m.open(request, open))
    }
    pub fn check_identity(
        &self,
        request: &str,
        identity: Identity,
        timeout_ms: u64,
    ) -> Result<i64, Error> {
        self.0
            .change(|m| m.check_identity(request, identity, timeout_ms))
    }
    pub fn send(&self, request: &str, message: &Envelope) -> Result<(), Error> {
        self.0.change(|m| m.send(request, message))
    }
    pub fn cancel(&self, request: &str) -> Result<(), Error> {
        self.0.change(|m| m.cancel(request))
    }
    pub fn finish(&self, request: &str) -> Result<(), Error> {
        self.0.change(|m| m.finish(request))
    }
    pub fn advance(&self, now_ms: u64) {
        self.0.change(|m| m.advance(now_ms));
    }
    pub fn phase(&self) -> Phase {
        self.0.change(|m| m.phase())
    }
    /// Settings snapshot, not a permit to bypass mux admission or closure.
    pub fn binding(&self) -> Option<Binding> {
        self.0.change(|m| m.binding().cloned())
    }
    pub fn bootstrap_failure(&self) -> Option<Failure> {
        self.0.change(|m| m.bootstrap_failure().cloned())
    }
    pub async fn ready(&self) -> Result<(), Error> {
        wait(&self.0, |m| match m.phase {
            Phase::Ready => Ok(Some(())),
            Phase::Rejecting => Err(Error::Rejected),
            Phase::Closed if m.bootstrap_failure().is_some() => Err(Error::Rejected),
            Phase::Closed => Err(Error::Closed),
            _ => Ok(None),
        })
        .await
    }
    pub async fn next_action(&self) -> Result<Option<Attachment>, Error> {
        wait(&self.0, |m| {
            if m.phase == Phase::Closed {
                Ok(Some(None))
            } else {
                Ok(m.next_action().map(Some))
            }
        })
        .await
    }
}
impl Port {
    pub async fn next_message(&self) -> Result<Option<Attachment>, Error> {
        wait(&self.0, |m| {
            if m.phase == Phase::Closed {
                Ok(Some(None))
            } else {
                Ok(m.next_message().map(Some))
            }
        })
        .await
    }
    pub async fn deliver(&self, item: Attachment) -> Result<(), Error> {
        wait(&self.0, |m| match m.receive(&item) {
            Ok(()) => Ok(Some(())),
            Err(Error::WouldBlock) => Ok(None),
            Err(e) => Err(e),
        })
        .await
    }
    pub fn disconnect(&self) {
        self.0.change(Mux::disconnect);
    }
}
impl Clone for Owner {
    fn clone(&self) -> Self {
        self.0.owners.fetch_add(1, Ordering::Relaxed);
        Self(self.0.clone())
    }
}
impl Clone for Port {
    fn clone(&self) -> Self {
        self.0.ports.fetch_add(1, Ordering::Relaxed);
        Self(self.0.clone())
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        if self.0.owners.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.change(Mux::disconnect);
        }
    }
}
impl Drop for Port {
    fn drop(&mut self) {
        if self.0.ports.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.change(Mux::disconnect);
        }
    }
}
struct Registration {
    shared: Arc<Shared>,
    id: Option<u64>,
}
impl Drop for Registration {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            self.shared
                .inner
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .waiters
                .remove(&id);
        }
    }
}
async fn wait<T>(
    shared: &Arc<Shared>,
    mut action: impl FnMut(&mut Mux) -> Result<Option<T>, Error>,
) -> Result<T, Error> {
    let mut registration = Registration {
        shared: shared.clone(),
        id: None,
    };
    poll_fn(|cx| {
        let (result, wakes) = {
            let mut state = shared.inner.lock().unwrap_or_else(|e| e.into_inner());
            let mut result = action(&mut state.mux);
            if matches!(result, Ok(None)) {
                let id = registration.id.or_else(|| {
                    if state.waiters.len() >= 128 {
                        None
                    } else {
                        state.serial.checked_add(1)
                    }
                });
                if let Some(id) = id {
                    state.serial = state.serial.max(id);
                    registration.id = Some(id);
                    state.waiters.insert(id, Some(cx.waker().clone()));
                    return Poll::Pending;
                }
                // Capacity failure is a port error, not a stranded wait. Close
                // the generation and wake every previously admitted caller.
                state.mux.disconnect();
                result = Err(Error::Capacity);
            }
            let wakes: Vec<_> = state
                .waiters
                .iter_mut()
                .filter(|(id, _)| Some(**id) != registration.id)
                .filter_map(|(_, w)| w.take())
                .collect();
            (result, wakes)
        };
        for w in wakes {
            w.wake();
        }
        Poll::Ready(result.map(|value| value.expect("nonpending result")))
    })
    .await
}
