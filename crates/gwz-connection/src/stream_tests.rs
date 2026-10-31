//! The asynchronous stream of contract section 9 for a local exchange (Phase 1 remediation R2a-1): a pending read is
//! woken by its data and never sleep-polls, `write_all` and `progress()` stay truthful after a dropped future, a POST
//! half-closes with `shutdown_write`, and the transitional `ExchangeIo` defaults do what their documentation says.
use crate::fakes::{Rig, exchange_spec, now, prepared};
use crate::{CancelState, Exchange, ExchangeIo, HostOwner, Operation, StreamError, StreamProgress};
use std::{
    io,
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

/// A waker that counts its wakes.
struct Count(AtomicUsize);
impl Wake for Count {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn counting() -> (Arc<Count>, Waker) {
    let count = Arc::new(Count(AtomicUsize::new(0)));
    let waker = Waker::from(count.clone());
    (count, waker)
}
fn wakes(count: &Count) -> usize {
    count.0.load(Ordering::SeqCst)
}

/// A host whose exchanges park their reads, and one open exchange on it.
/// Bind as `(rig, owner, operation, exchange)`: the exchange then drops first.
fn parked() -> (Rig, HostOwner, Operation, Exchange) {
    let rig = Rig::new();
    rig.world.park_streams.store(true, Ordering::SeqCst);
    let (owner, handle) = rig.host(1);
    let operation = now(handle.admit(prepared("op-stream", 1)).unwrap().acquire())
        .unwrap()
        .register();
    let exchange = now(operation.open(exchange_spec()).unwrap().wait()).unwrap();
    (rig, owner, operation, exchange)
}

#[test]
fn a_pending_read_wakes_on_data_with_no_sleep() {
    let (rig, _owner, _operation, mut exchange) = parked();
    let (count, waker) = counting();
    let mut cx = Context::from_waker(&waker);
    let mut buffer = [0u8; 8];
    let mut read = pin!(exchange.stream().read(&mut buffer));
    assert!(read.as_mut().poll(&mut cx).is_pending());
    assert!(read.as_mut().poll(&mut cx).is_pending());
    assert_eq!(
        wakes(&count),
        0,
        "a parked read is not re-polled: nothing wakes it until the data arrives"
    );
    rig.world.peer(0).feed(b"hello");
    assert_eq!(wakes(&count), 1, "the data woke the reader once");
    assert!(matches!(read.as_mut().poll(&mut cx), Poll::Ready(Ok(5))));
    assert_eq!(&buffer[..5], b"hello");
    assert_eq!(exchange.stream().progress().consumed_read, 5);
}

#[test]
fn a_dropped_read_consumed_nothing_and_the_next_read_gets_the_bytes() {
    let (rig, _owner, _operation, mut exchange) = parked();
    let (_count, waker) = counting();
    let mut cx = Context::from_waker(&waker);
    let mut buffer = [0u8; 8];
    {
        let mut read = pin!(exchange.stream().read(&mut buffer));
        assert!(read.as_mut().poll(&mut cx).is_pending());
    }
    assert_eq!(exchange.stream().progress().consumed_read, 0);
    rig.world.peer(0).feed(b"abc");
    let mut again = pin!(exchange.stream().read(&mut buffer));
    assert!(matches!(again.as_mut().poll(&mut cx), Poll::Ready(Ok(3))));
    assert_eq!(&buffer[..3], b"abc");
    assert_eq!(exchange.stream().progress().consumed_read, 3);
}

#[test]
fn a_read_ends_with_zero_once_the_peer_ended_the_response() {
    let (rig, _owner, _operation, mut exchange) = parked();
    let mut buffer = [0u8; 4];
    rig.world.peer(0).feed(b"x");
    rig.world.peer(0).end();
    assert_eq!(now(exchange.stream().read(&mut buffer)).unwrap(), 1);
    assert_eq!(now(exchange.stream().read(&mut buffer)).unwrap(), 0);
}

#[test]
fn zero_length_reads_and_writes_are_no_ops() {
    let (rig, _owner, _operation, mut exchange) = parked();
    // The read of an empty buffer would park on the empty exchange; it is a no-op instead.
    assert_eq!(now(exchange.stream().read(&mut [])).unwrap(), 0);
    assert_eq!(now(exchange.stream().write(&[])).unwrap(), 0);
    now(exchange.stream().write_all(&[])).unwrap();
    assert_eq!(
        exchange.stream().progress(),
        StreamProgress::default(),
        "no-ops move no offset"
    );
    assert_eq!(rig.world.peer(0).written(), 0, "the adapter was not called");
}

#[test]
fn write_all_and_progress_stay_truthful_after_a_dropped_future() {
    let (rig, _owner, _operation, mut exchange) = parked();
    let peer = rig.world.peer(0);
    peer.set_room(4);
    let body = b"0123456789";
    let (count, waker) = counting();
    let mut cx = Context::from_waker(&waker);
    {
        let mut write = pin!(exchange.stream().write_all(body));
        assert!(
            write.as_mut().poll(&mut cx).is_pending(),
            "the exchange took 4 bytes and has no room for more"
        );
        assert_eq!(wakes(&count), 0, "a write with no room waits to be woken");
    }
    // The future was dropped mid-way: the accepted prefix is a fact, and progress says so.
    let progress = exchange.stream().progress();
    assert_eq!(progress.accepted_write, 4);
    assert_eq!(peer.written(), 4, "the adapter agrees with progress");
    // Resolve by progress, never by resending the original buffer.
    let rest = &body[usize::try_from(progress.accepted_write).unwrap()..];
    assert_eq!(peer.take(4), b"0123");
    let mut write = pin!(exchange.stream().write_all(rest));
    assert!(write.as_mut().poll(&mut cx).is_pending());
    let before = wakes(&count);
    assert_eq!(peer.take(4), b"4567");
    assert!(
        wakes(&count) > before,
        "making room woke the writer that was waiting"
    );
    assert!(matches!(write.as_mut().poll(&mut cx), Poll::Ready(Ok(()))));
    assert_eq!(exchange.stream().progress().accepted_write, 10);
    assert_eq!(peer.written(), 10);
    assert_eq!(peer.take(4), b"89");
}

#[test]
fn write_may_accept_a_prefix_and_write_all_continues_to_the_end() {
    let (rig, _owner, _operation, mut exchange) = parked();
    rig.world.peer(0).set_chunk(3);
    assert_eq!(now(exchange.stream().write(b"abcdefgh")).unwrap(), 3);
    now(exchange.stream().write_all(b"defgh")).unwrap();
    assert_eq!(exchange.stream().progress().accepted_write, 8);
}

#[test]
fn a_post_half_closes_with_shutdown_write_and_still_reads_the_response() {
    let (rig, _owner, _operation, mut exchange) = parked();
    let peer = rig.world.peer(0);
    now(exchange.stream().write_all(b"0000")).unwrap();
    assert!(!peer.half_closed());
    now(exchange.stream().flush()).unwrap();
    now(exchange.stream().shutdown_write()).unwrap();
    assert!(peer.half_closed(), "the adapter was told the request ended");
    match now(exchange.stream().write(b"late")) {
        Err(StreamError::Io(error)) => assert_eq!(error.kind(), io::ErrorKind::BrokenPipe),
        other => panic!("a write after the half-close must fail: {other:?}"),
    }
    assert_eq!(exchange.stream().progress().accepted_write, 4);
    // The response is still readable: the half-close ended the request only.
    let mut buffer = [0u8; 16];
    let mut read = pin!(exchange.stream().read(&mut buffer));
    let (_count, waker) = counting();
    let mut cx = Context::from_waker(&waker);
    // The loopback hands the request back to the peer, which consumes it before it answers.
    assert_eq!(peer.take(4), b"0000");
    assert!(read.as_mut().poll(&mut cx).is_pending());
    peer.feed(b"200 OK");
    assert!(matches!(read.as_mut().poll(&mut cx), Poll::Ready(Ok(6))));
}

#[test]
fn a_cancel_wakes_a_pending_read_with_cancelled() {
    let (_rig, _owner, operation, mut exchange) = parked();
    let (count, waker) = counting();
    let mut cx = Context::from_waker(&waker);
    let mut buffer = [0u8; 4];
    let mut read = pin!(exchange.stream().read(&mut buffer));
    assert!(read.as_mut().poll(&mut cx).is_pending());
    assert_eq!(operation.cancel_handle().cancel(), CancelState::Cancelled);
    assert!(wakes(&count) >= 1, "the cancellation woke the reader");
    assert!(matches!(
        read.as_mut().poll(&mut cx),
        Poll::Ready(Err(StreamError::Cancelled))
    ));
}

/// An adapter written for the earlier port: only the synchronous calls.
struct SyncOnly {
    ready: bool,
}
impl ExchangeIo for SyncOnly {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        Ok(data.len())
    }
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.ready {
            out[0] = 7;
            Ok(1)
        } else {
            Err(io::ErrorKind::WouldBlock.into())
        }
    }
    fn poll_finish(&mut self, _cx: &mut Context<'_>) -> Poll<gwz_transport::protocol::Disposition> {
        Poll::Ready(gwz_transport::protocol::Disposition::Discarded)
    }
    fn abort(&mut self) {}
}
/// An adapter written for the poll port: no synchronous calls.
struct PollOnly;
impl ExchangeIo for PollOnly {
    fn poll_finish(&mut self, _cx: &mut Context<'_>) -> Poll<gwz_transport::protocol::Disposition> {
        Poll::Ready(gwz_transport::protocol::Disposition::Discarded)
    }
    fn abort(&mut self) {}
}

#[test]
fn the_transitional_defaults_adapt_the_sync_port_and_say_unsupported_otherwise() {
    let (count, waker) = counting();
    let mut cx = Context::from_waker(&waker);
    let mut buffer = [0u8; 2];
    let mut old = SyncOnly { ready: false };
    // The shim over a sync adapter: not ready is Pending with the waker woken at once (a busy re-poll, retired by R2b).
    assert!(old.poll_read(&mut cx, &mut buffer).is_pending());
    assert_eq!(wakes(&count), 1);
    old.ready = true;
    assert!(matches!(
        old.poll_read(&mut cx, &mut buffer),
        Poll::Ready(Ok(1))
    ));
    assert!(matches!(old.poll_write(&mut cx, b"ab"), Poll::Ready(Ok(2))));
    // A sync-only adapter cannot half-close, and says so; nothing is buffered, and no peer offset is known.
    assert!(matches!(
        old.poll_shutdown_write(&mut cx),
        Poll::Ready(Err(ref e)) if e.kind() == io::ErrorKind::Unsupported
    ));
    assert!(matches!(old.poll_flush(&mut cx), Poll::Ready(Ok(()))));
    assert_eq!(old.peer_confirmed(), None);
    // A poll-only adapter's sync calls are unsupported.
    let mut new = PollOnly;
    assert_eq!(
        new.read(&mut buffer).unwrap_err().kind(),
        io::ErrorKind::Unsupported
    );
    assert_eq!(
        new.write(b"x").unwrap_err().kind(),
        io::ErrorKind::Unsupported
    );
}

#[test]
fn a_stream_with_no_peer_offset_reports_it_unknown() {
    let (_rig, _owner, _operation, mut exchange) = parked();
    assert_eq!(exchange.stream().progress().peer_confirmed, None);
}
