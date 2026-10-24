//! A value set once and awaited by many: the seam between a job that runs on the composition's executor and the
//! future its caller holds. The caller may drop its future; the job still sets the value.
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
