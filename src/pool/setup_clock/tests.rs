// Custom wake/drop callbacks must never execute under the authority lock.
use super::*;
use std::{
    future::Future,
    pin::pin,
    sync::{
        Weak,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Wake},
};

struct Probe {
    inner: Weak<Inner>,
    callbacks: Arc<AtomicUsize>,
}
impl Probe {
    fn check(&self) {
        if let Some(inner) = self.inner.upgrade() {
            assert!(inner.state.try_lock().is_ok(), "callback under clock lock");
        }
        self.callbacks.fetch_add(1, Ordering::SeqCst);
    }
}
impl Wake for Probe {
    fn wake(self: Arc<Self>) {
        self.check();
    }
}
impl Drop for Probe {
    fn drop(&mut self) {
        self.check();
    }
}

#[test]
fn wake_and_drop_callbacks_stay_outside_clock_lock() {
    let clock = SetupClock::new(ConnectionId { pool: 1, serial: 1 }, Arc::new(|| 0), None, 0);
    let callbacks = Arc::new(AtomicUsize::new(0));
    let waker = Waker::from(Arc::new(Probe {
        inner: Arc::downgrade(&clock.inner),
        callbacks: callbacks.clone(),
    }));
    clock.register_driver(Arc::new(waker.clone())).deliver();
    clock.register_driver(Arc::new(waker.clone())).deliver();
    let token = clock
        .prepare_local(LocalPhase::Admission, 100)
        .deliver()
        .unwrap();
    let receipt = clock.publish_local(token).deliver().unwrap();
    {
        let mut wait = pin!(clock.wait_acknowledged(receipt));
        assert!(
            wait.as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert!(
            wait.as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        clock.acknowledge(receipt).deliver().unwrap();
        assert!(
            wait.as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_ready()
        );
    }
    clock.terminate(SetupCause::Cancelled).deliver();
    drop(waker);
    assert_eq!(callbacks.load(Ordering::SeqCst), 4);
}
