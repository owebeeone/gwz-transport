//! Host timer service wakes asynchronous users without inferring backend activity.
use gwz_transport::{
    protocol::{Effect, ErrorCode, MessageKind},
    stream::{Config, Error, IoState, Stream},
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

#[test]
fn independently_driven_expiry_wakes_reader_and_dispatcher_with_first_cause() {
    for state in [IoState::Network, IoState::Interaction] {
        let mut config = Config::new("timer", 1, gwz_transport::stream::Side::Endpoint);
        config.io_timeout_ms = 10;
        config.interaction_budget_ms = 10;
        let (stream, port) = Stream::new(config).unwrap();
        port.advance(4_000_000);
        port.set_io_state(state).unwrap();
        let signal = Arc::new(Signal::default());
        let waker = Waker::from(signal.clone());
        let mut cx = Context::from_waker(&waker);
        let mut bytes = [0];
        let mut read = pin!(stream.read(&mut bytes));
        let mut dispatch = pin!(port.next_message());
        assert!(read.as_mut().poll(&mut cx).is_pending());
        assert!(dispatch.as_mut().poll(&mut cx).is_pending());
        assert_eq!(port.next_deadline(), Some(4_000_010));
        port.advance(4_000_009);
        assert_eq!(signal.0.load(Ordering::Relaxed), 0);
        port.advance(4_000_010);
        assert!(signal.0.load(Ordering::Relaxed) > 0);
        stream.cancel();
        assert_eq!(port.record_io_progress(1), Err(Error::Timeout));
        assert_eq!(port.set_io_state(IoState::Idle), Err(Error::Timeout));
        assert_eq!(
            read.as_mut().poll(&mut cx),
            Poll::Ready(Err(Error::Timeout))
        );
        let Poll::Ready(Ok(Some(message))) = dispatch.as_mut().poll(&mut cx) else {
            panic!("timer must wake the dispatcher with the terminal message");
        };
        assert_eq!(message.kind, MessageKind::Failed);
        let failure = message.failed.unwrap();
        assert_eq!(failure.code, ErrorCode::Timeout);
        assert_eq!(failure.effect, Effect::Possible);
        assert_eq!(port.next_deadline(), None);
        assert_eq!(port.waiter_count(), 0);
    }
}

#[test]
fn batching_and_io_deadlines_remain_independent() {
    let mut config = Config::new("timer", 1, gwz_transport::stream::Side::Endpoint);
    config.coalesce_delay_ms = 5;
    config.io_timeout_ms = 20;
    let (stream, port) = Stream::new(config).unwrap();
    port.advance(100);
    port.set_io_state(IoState::Network).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(pin!(stream.write(b"a")).poll(&mut cx), Poll::Ready(Ok(1)));
    assert_eq!(port.next_deadline(), Some(105));
    assert!(pin!(port.next_message()).poll(&mut cx).is_pending());
    port.advance(105);
    let Poll::Ready(Ok(Some(message))) = pin!(port.next_message()).poll(&mut cx) else {
        panic!("partial batch must become ready independently of I/O");
    };
    assert_eq!(message.data.unwrap().payload, b"a");
    assert_eq!(port.next_deadline(), Some(120));
    assert_eq!(port.io_status().remaining_network_ms, 15);
    port.advance(120);
    assert!(
        port.stats().terminal,
        "message delivery must not reset peer progress"
    );
}

#[test]
fn elapsed_batch_does_not_wake_repeatedly_while_credit_is_exhausted() {
    let mut config = Config::new("timer", 1, gwz_transport::stream::Side::Endpoint);
    config.peer_receive_window = 1;
    config.max_payload = 1;
    let (stream, port) = Stream::new(config).unwrap();
    port.set_io_state(IoState::Backpressure).unwrap();
    let signal = Arc::new(Signal::default());
    let waker = Waker::from(signal.clone());
    let mut cx = Context::from_waker(&waker);
    assert_eq!(pin!(stream.write(b"ab")).poll(&mut cx), Poll::Ready(Ok(2)));
    assert!(matches!(
        pin!(port.next_message()).poll(&mut cx),
        Poll::Ready(Ok(Some(_)))
    ));
    let mut dispatch = pin!(port.next_message());
    assert!(dispatch.as_mut().poll(&mut cx).is_pending());
    port.advance(100);
    assert!(dispatch.as_mut().poll(&mut cx).is_pending());
    let before = signal.0.load(Ordering::Relaxed);
    port.advance(101);
    port.advance(100_000);
    assert_eq!(signal.0.load(Ordering::Relaxed), before);
    assert!(!port.stats().terminal);
    assert_eq!(port.io_status().remaining_network_ms, 3000);
}
