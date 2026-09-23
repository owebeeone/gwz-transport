use gwz_transport::{
    pool::{Config as PoolConfig, Identity, Key, Owner, Pool, Request},
    protocol::{Data, Envelope, MessageKind},
    stream::{Config as StreamConfig, IoState, Side, Stream},
};
use std::{
    future::Future,
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

#[derive(Default)]
struct Signal(AtomicUsize);
impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

fn request() -> Request {
    Request::new(
        Key::ssh("git", "wake-host", 22),
        Identity::Ambient,
        Owner::new("session", "operation"),
    )
}

#[test]
fn quiet_pool_tick_does_not_wake_pending_checkout_or_driver() {
    let mut config = PoolConfig::default();
    config.connect_timeout_ms = 100;
    let (pool, mut driver) = Pool::new(config).unwrap();
    let mut checkout = pool.checkout(request()).unwrap();
    let signal = Arc::new(Signal::default());
    let waker = Waker::from(signal.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(pin!(&mut checkout).poll(&mut cx), Poll::Pending));
    let Poll::Ready(Some(gwz_transport::pool::Action::Connect { connection, .. })) =
        pin!(driver.next_action()).poll(&mut cx)
    else {
        panic!("connect must be runnable");
    };
    let before = signal.0.load(Ordering::Relaxed);
    for now_ms in [10, 20, 50, 99] {
        driver.advance(now_ms);
        assert_eq!(signal.0.load(Ordering::Relaxed), before);
    }

    driver.begin_interaction(connection).unwrap();
    driver.end_interaction(connection).unwrap();
    assert_eq!(signal.0.load(Ordering::Relaxed), before);
    assert_eq!(driver.next_deadline(), Some(100));

    driver.advance(100);
    assert!(signal.0.load(Ordering::Relaxed) > before);
    assert!(matches!(
        pin!(&mut checkout).poll(&mut cx),
        Poll::Ready(Err(gwz_transport::pool::Error::ConnectTimeout))
    ));
    assert!(matches!(
        pin!(driver.next_action()).poll(&mut cx),
        Poll::Ready(Some(gwz_transport::pool::Action::CancelConnect { .. }))
    ));

    let (pool, mut driver) = Pool::new(PoolConfig::default()).unwrap();
    let driver_signal = Arc::new(Signal::default());
    let driver_waker = Waker::from(driver_signal.clone());
    let mut driver_cx = Context::from_waker(&driver_waker);
    let mut pending = pin!(driver.next_action());
    assert!(pending.as_mut().poll(&mut driver_cx).is_pending());
    let _checkout = pool.checkout(request()).unwrap();
    assert!(driver_signal.0.load(Ordering::Relaxed) > 0);
}

#[test]
fn stream_clock_bookkeeping_does_not_wake_unrelated_waiters_but_delivery_does() {
    let mut config = StreamConfig::new("wake-stream", 1, Side::Endpoint);
    config.io_timeout_ms = 100;
    let (stream, endpoint) = Stream::new(config).unwrap();
    endpoint.set_io_state(IoState::Network).unwrap();
    let signal = Arc::new(Signal::default());
    let read_waker = Waker::from(signal.clone());
    let dispatch_signal = Arc::new(Signal::default());
    let dispatch_waker = Waker::from(dispatch_signal.clone());
    let mut read_cx = Context::from_waker(&read_waker);
    let mut dispatch_cx = Context::from_waker(&dispatch_waker);
    let mut output = [0; 1];
    let mut read = pin!(stream.read(&mut output));
    let mut dispatch = pin!(endpoint.next_message());
    assert!(read.as_mut().poll(&mut read_cx).is_pending());
    assert!(dispatch.as_mut().poll(&mut dispatch_cx).is_pending());
    endpoint.advance(10);
    let before_read = signal.0.load(Ordering::Relaxed);
    let before_dispatch = dispatch_signal.0.load(Ordering::Relaxed);

    endpoint.record_io_progress(1).unwrap();
    assert_eq!(endpoint.next_deadline(), Some(110));
    endpoint.set_io_state(IoState::Backpressure).unwrap();
    assert_eq!(endpoint.next_deadline(), None);
    assert_eq!(signal.0.load(Ordering::Relaxed), before_read);
    assert_eq!(dispatch_signal.0.load(Ordering::Relaxed), before_dispatch);

    endpoint
        .deliver(Envelope {
            version: 1,
            session_id: "wake-stream".into(),
            stream_id: 1,
            kind: MessageKind::Data,
            data: Some(Data {
                offset: 0,
                payload: vec![7],
            }),
            ..Default::default()
        })
        .unwrap();
    assert!(signal.0.load(Ordering::Relaxed) > before_read);
    assert!(dispatch_signal.0.load(Ordering::Relaxed) > before_dispatch);
    assert_eq!(read.as_mut().poll(&mut read_cx), Poll::Ready(Ok(1)));
}

#[test]
fn cleanup_abort_and_abort_connect_become_runnable_once_at_deadline() {
    let mut pool = gwz_transport::pool::PoolMachine::new(PoolConfig::default()).unwrap();
    let request_id = pool.request(request()).unwrap();
    let Some(gwz_transport::pool::Action::Connect { connection, .. }) = pool.next_action() else {
        panic!("connect");
    };
    pool.cancel(request_id).unwrap();
    let Some(gwz_transport::pool::Action::CancelConnect { deadline, .. }) = pool.next_action()
    else {
        panic!("cancel connect");
    };
    pool.advance(deadline - 1);
    assert!(pool.next_action().is_none());
    pool.advance(deadline);
    assert!(matches!(
        pool.next_action(),
        Some(gwz_transport::pool::Action::AbortConnect { connection: id }) if id == connection
    ));
    pool.connected(
        connection,
        Err(gwz_transport::protocol::Failure {
            setup_cause: None,
            code: gwz_transport::protocol::ErrorCode::Io,
            effect: gwz_transport::protocol::Effect::Possible,
            facts: None,
        }),
    )
    .unwrap();

    let mut pool = gwz_transport::pool::PoolMachine::new(PoolConfig::default()).unwrap();
    let request_id = pool.request(request()).unwrap();
    let Some(gwz_transport::pool::Action::Connect { connection, .. }) = pool.next_action() else {
        panic!("connect");
    };
    pool.connected(connection, Ok(Some(Identity::Ambient)))
        .unwrap();
    let lease = pool.take(request_id).unwrap();
    pool.shutdown();
    let Some(gwz_transport::pool::Action::Close { deadline, .. }) = pool.next_action() else {
        panic!("close");
    };
    pool.advance(deadline - 1);
    assert!(pool.next_action().is_none());
    pool.advance(deadline);
    assert!(matches!(
        pool.next_action(),
        Some(gwz_transport::pool::Action::Abort { connection: id }) if id == lease.connection()
    ));
    pool.closed(connection).unwrap();
}
