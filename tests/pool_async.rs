use gwz_transport::{pool::*, protocol::Disposition};
use std::{
    future::Future,
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake},
};
struct WakeCount(AtomicUsize);
impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn request() -> Request {
    Request::new(
        Key::ssh("git", "host", 22),
        Identity::Ambient,
        Owner::new("session", "owner"),
    )
}

#[test]
fn opening_correlation_never_exposes_waiting_ready_or_consumed_leases() {
    let (pool, mut driver) = Pool::new(Config {
        per_user_host: 1,
        ..Default::default()
    })
    .unwrap();
    let mut cx = Context::from_waker(std::task::Waker::noop());
    let mut first = pool.checkout(request()).unwrap();
    let waiting = pool.checkout(request()).unwrap();
    let Poll::Ready(Some(Action::Connect { connection, .. })) =
        pin!(driver.next_action()).poll(&mut cx)
    else {
        panic!("connect");
    };
    assert_eq!(first.opening_connection(), Some(connection));
    assert_eq!(waiting.opening_connection(), None);
    driver
        .connected(connection, Ok(Some(Identity::Ambient)))
        .unwrap();
    assert_eq!(first.opening_connection(), None);
    let Poll::Ready(Ok(lease)) = pin!(&mut first).poll(&mut cx) else {
        panic!("lease");
    };
    assert_eq!(first.opening_connection(), None);
    lease.release(Disposition::Reusable).unwrap();
    assert_eq!(waiting.opening_connection(), None);
}

#[test]
fn clones_share_capacity_and_lease_drop_discards_while_checkout_drop_cancels() {
    let (pool, mut driver) = Pool::new(Config {
        per_user_host: 1,
        max_requests: 1,
        ..Config::default()
    })
    .unwrap();
    let clone = pool.clone();
    let wake = Arc::new(WakeCount(AtomicUsize::new(0)));
    let waker = wake.clone().into();
    let mut cx = Context::from_waker(&waker);
    let mut first = pool.checkout(request()).unwrap();
    assert!(pin!(&mut first).poll(&mut cx).is_pending());
    assert!(matches!(clone.checkout(request()), Err(Error::Capacity)));
    let Poll::Ready(Some(Action::Connect { connection, .. })) =
        pin!(driver.next_action()).poll(&mut cx)
    else {
        panic!("connect");
    };
    driver
        .connected(connection, Ok(Some(Identity::Ambient)))
        .unwrap();
    assert!(wake.0.load(Ordering::SeqCst) > 0);
    let Poll::Ready(Ok(lease)) = pin!(&mut first).poll(&mut cx) else {
        panic!("lease");
    };
    assert_eq!(lease.connection().unwrap(), connection);
    let mut second = clone.checkout(request()).unwrap();
    assert!(pin!(&mut second).poll(&mut cx).is_pending());
    drop(second);
    assert_eq!(pool.outstanding_requests(), 0);
    drop(lease);
    let Poll::Ready(Some(Action::Close { connection: id, .. })) =
        pin!(driver.next_action()).poll(&mut cx)
    else {
        panic!("close");
    };
    assert_eq!(id, connection);
    driver.closed(id).unwrap();
    assert_eq!(pool.counts().total(), 0);
}

#[test]
fn ready_checkout_drop_discards_and_driver_loss_wakes_pending_callers() {
    let (pool, mut driver) = Pool::new(Config::default()).unwrap();
    let waker = Arc::new(WakeCount(AtomicUsize::new(0))).into();
    let mut cx = Context::from_waker(&waker);
    let checkout = pool.checkout(request()).unwrap();
    let Poll::Ready(Some(Action::Connect { connection, .. })) =
        pin!(driver.next_action()).poll(&mut cx)
    else {
        panic!("connect");
    };
    driver
        .connected(connection, Ok(Some(Identity::Ambient)))
        .unwrap();
    drop(checkout);
    assert_eq!(pool.counts().closing, 1);
    let mut pending = pool.checkout(request()).unwrap();
    assert!(pin!(&mut pending).poll(&mut cx).is_pending());
    drop(driver);
    assert!(matches!(
        pin!(&mut pending).poll(&mut cx),
        Poll::Ready(Err(Error::DriverLost))
    ));
    assert!(matches!(pool.checkout(request()), Err(Error::DriverLost)));
}

#[test]
fn dropping_last_pool_owner_shuts_down_even_with_a_live_lease() {
    let (pool, mut driver) = Pool::new(Config::default()).unwrap();
    let clone = pool.clone();
    let waker = Arc::new(WakeCount(AtomicUsize::new(0))).into();
    let mut cx = Context::from_waker(&waker);
    let mut checkout = pool.checkout(request()).unwrap();
    let Poll::Ready(Some(Action::Connect { connection, .. })) =
        pin!(driver.next_action()).poll(&mut cx)
    else {
        panic!("connect");
    };
    driver
        .connected(connection, Ok(Some(Identity::Ambient)))
        .unwrap();
    let Poll::Ready(Ok(lease)) = pin!(&mut checkout).poll(&mut cx) else {
        panic!("lease");
    };
    drop(pool);
    assert!(lease.connection().is_ok());
    drop(clone);
    assert_eq!(lease.connection(), Err(Error::Stale));
    assert_eq!(lease.release(Disposition::Reusable), Err(Error::Stale));
    assert!(matches!(
        pin!(driver.next_action()).poll(&mut cx),
        Poll::Ready(Some(Action::Close { .. }))
    ));
    driver.closed(connection).unwrap();
    assert!(matches!(
        pin!(driver.next_action()).poll(&mut cx),
        Poll::Ready(None)
    ));
}

#[test]
fn full_request_capacity_does_not_consume_the_driver_wake_slot() {
    let (pool, mut driver) = Pool::new(Config {
        per_user_host: 1,
        max_requests: 1,
        ..Config::default()
    })
    .unwrap();
    let wake = Arc::new(WakeCount(AtomicUsize::new(0)));
    let waker = wake.clone().into();
    let mut cx = Context::from_waker(&waker);
    let mut checkout = pool.checkout(request()).unwrap();
    assert!(pin!(&mut checkout).poll(&mut cx).is_pending());
    let Poll::Ready(Some(Action::Connect { connection, .. })) =
        pin!(driver.next_action()).poll(&mut cx)
    else {
        panic!("connect");
    };
    {
        let mut action = pin!(driver.next_action());
        assert!(action.as_mut().poll(&mut cx).is_pending());
        let before = wake.0.load(Ordering::SeqCst);
        drop(checkout);
        assert!(wake.0.load(Ordering::SeqCst) > before);
        assert!(matches!(
            action.as_mut().poll(&mut cx),
            Poll::Ready(Some(Action::CancelConnect { .. }))
        ));
    }
    driver
        .connected(connection, Ok(Some(Identity::Ambient)))
        .unwrap();
    assert!(matches!(
        pin!(driver.next_action()).poll(&mut cx),
        Poll::Ready(Some(Action::Close { .. }))
    ));
    driver.closed(connection).unwrap();
    assert_eq!(pool.counts().total(), 0);
}

#[test]
fn callbacks_can_reenter_pool_without_holding_its_lock() {
    struct Observe(Pool);
    impl Wake for Observe {
        fn wake(self: Arc<Self>) {
            let _ = self.0.counts();
        }
    }
    let (pool, mut driver) = Pool::new(Config::default()).unwrap();
    let waker = Arc::new(Observe(pool.clone())).into();
    let mut cx = Context::from_waker(&waker);
    let mut checkout = pool.checkout(request()).unwrap();
    assert!(pin!(&mut checkout).poll(&mut cx).is_pending());
    let Poll::Ready(Some(Action::Connect { connection, .. })) =
        pin!(driver.next_action()).poll(&mut cx)
    else {
        panic!("connect");
    };
    driver
        .connected(connection, Ok(Some(Identity::Ambient)))
        .unwrap();
    let Poll::Ready(Ok(lease)) = pin!(&mut checkout).poll(&mut cx) else {
        panic!("lease");
    };
    lease.release(Disposition::Reusable).unwrap();
    assert_eq!(pool.counts().idle, 1);
}

#[test]
fn spontaneous_idle_disposal_wakes_queued_checkout_and_schedules_replacement() {
    let (pool, mut driver) = Pool::new(Config {
        total: 1,
        ..Default::default()
    })
    .unwrap();
    let wake = Arc::new(WakeCount(AtomicUsize::new(0)));
    let waker = wake.clone().into();
    let mut cx = Context::from_waker(&waker);
    let mut checkout = pool.checkout(request()).unwrap();
    let Poll::Ready(Some(Action::Connect { connection, .. })) =
        pin!(driver.next_action()).poll(&mut cx)
    else {
        panic!("connect");
    };
    driver
        .connected(connection, Ok(Some(Identity::Ambient)))
        .unwrap();
    let Poll::Ready(Ok(lease)) = pin!(&mut checkout).poll(&mut cx) else {
        panic!("lease");
    };
    lease.release(Disposition::Reusable).unwrap();
    let mut different = request();
    different.key.host = "other-host".into();
    let mut waiting = pool.checkout(different).unwrap();
    assert!(pin!(&mut waiting).poll(&mut cx).is_pending());
    let before = wake.0.load(Ordering::SeqCst);
    driver.idle_closed(connection).unwrap();
    assert!(wake.0.load(Ordering::SeqCst) > before);
    let Poll::Ready(Some(Action::Connect {
        connection: new, ..
    })) = pin!(driver.next_action()).poll(&mut cx)
    else {
        panic!("replacement");
    };
    assert_ne!(new, connection);
    driver.connected(new, Ok(Some(Identity::Ambient))).unwrap();
    assert!(matches!(
        pin!(&mut waiting).poll(&mut cx),
        Poll::Ready(Ok(_))
    ));
}
