//! A value set once and awaited by many (`Cell`), and a value handed once to one waiter (`Slot`): the seams between work
//! that runs on the composition's executor (the host's driver, a close job) and the future its caller holds. The
//! caller may drop its future; the work still sets the value.
use std::{
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

struct State<T> {
    value: Option<T>,
    wakers: Vec<Waker>,
}
pub(crate) struct Cell<T> {
    inner: Arc<Mutex<State<T>>>,
}
impl<T> Clone for Cell<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
impl<T: Clone> Cell<T> {
    /// A cell whose value is already set.
    pub(crate) fn ready(value: T) -> Self {
        let cell = Self::new();
        cell.set(value);
        cell
    }
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(State {
                value: None,
                wakers: Vec::new(),
            })),
        }
    }
    /// Sets the value if none is set, and wakes every waiter. The first value wins.
    pub(crate) fn set(&self, value: T) {
        let wakers = {
            let mut state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            if state.value.is_some() {
                return;
            }
            state.value = Some(value);
            std::mem::take(&mut state.wakers)
        };
        for waker in wakers {
            waker.wake();
        }
    }
    /// The value, or `Pending` with the caller's waker registered once.
    pub(crate) fn poll(&self, cx: &mut Context<'_>) -> Poll<T> {
        let mut state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(value) = &state.value {
            return Poll::Ready(value.clone());
        }
        if !state.wakers.iter().any(|waker| waker.will_wake(cx.waker())) {
            state.wakers.push(cx.waker().clone());
        }
        Poll::Pending
    }
}

/// A value handed once from the host's driver to the one future that waits for it (an opening's exchange). Unlike
/// [`Cell`] the value is moved out, not cloned; a waiter that goes first abandons the slot, and whatever is handed in
/// after that comes back to the giver to dispose of.
pub(crate) struct Slot<T> {
    inner: Mutex<SlotState<T>>,
}
struct SlotState<T> {
    value: Option<T>,
    waker: Option<Waker>,
    abandoned: bool,
}
impl<T> Slot<T> {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(SlotState {
                value: None,
                waker: None,
                abandoned: false,
            }),
        })
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, SlotState<T>> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
    /// Hands `value` to the waiter and wakes it, or gives it back if the waiter has gone.
    pub(crate) fn fill(&self, value: T) -> Result<(), T> {
        let waker = {
            let mut state = self.lock();
            if state.abandoned || state.value.is_some() {
                return Err(value);
            }
            state.value = Some(value);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }
    /// The value, or `Pending` with the caller's waker kept.
    pub(crate) fn poll_take(&self, cx: &mut Context<'_>) -> Poll<T> {
        let mut state = self.lock();
        if let Some(value) = state.value.take() {
            return Poll::Ready(value);
        }
        state.waker = Some(cx.waker().clone());
        Poll::Pending
    }
    /// Whether a value waits to be taken.
    pub(crate) fn is_filled(&self) -> bool {
        self.lock().value.is_some()
    }
    /// The waiter goes: no value is handed in from now on, and one already handed in comes back.
    pub(crate) fn abandon(&self) -> Option<T> {
        let mut state = self.lock();
        state.abandoned = true;
        state.waker = None;
        state.value.take()
    }
}
