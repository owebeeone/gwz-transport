//! A request may name one preferred connection (`Request::prefer`) and, while that connection is leased, a bounded
//! wait for it (`Request::prefer_wait_ms`): the pool leases the preferred connection first when it is idle, of equal
//! key and identity, reusable, not excluded and its owner not full; while the lease it held when the request was made
//! runs and the wait has not lapsed the request opens, evicts and leases nothing; otherwise it decides as if none were
//! named (contract section 8 as amended by the HTTPS model's A10; rows H2a-1 to H2a-9 of `GwzHttpsAuthModel-DRAFT.md`
//! section 5).
use gwz_transport::{pool::*, protocol::Disposition};
use std::{
    future::Future,
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

const WAIT: u64 = 500;

fn ask(host: &str, identity: Identity) -> Request {
    Request::new(
        Key::ssh("git", host, 22),
        identity,
        Owner::new("session", "operation"),
    )
}
fn request() -> Request {
    ask("host", Identity::Ambient)
}
fn preferring(connection: ConnectionId, wait: Option<u64>) -> Request {
    let mut request = request();
    request.prefer = Some(connection);
    request.prefer_wait_ms = wait;
    request
}
fn pool(total: usize) -> PoolMachine {
    let mut pool = PoolMachine::new(Config {
        total,
        ..Config::default()
    })
    .unwrap();
    pool.advance(1_000);
    pool
}
fn connect(pool: &mut PoolMachine) -> ConnectionId {
    let Some(Action::Connect { connection, .. }) = pool.next_action() else {
        panic!("a connect action");
    };
    connection
}
/// A new connection for `request`, leased to it; `proved` decides whether it is reusable.
fn leased_as(pool: &mut PoolMachine, request: Request, proved: bool) -> LeaseId {
    let identity = request.identity.clone();
    let id = pool.request(request).unwrap();
    let connection = connect(pool);
    pool.connected(connection, Ok(proved.then_some(identity)))
        .unwrap();
    pool.take(id).unwrap()
}
fn leased(pool: &mut PoolMachine) -> LeaseId {
    leased_as(pool, request(), true)
}
fn idle(pool: &mut PoolMachine, lease: LeaseId) -> ConnectionId {
    pool.release(lease, Disposition::Reusable).unwrap();
    lease.connection()
}

#[test]
fn h2a_1_an_idle_preferred_connection_is_leased_over_a_lower_id() {
    let mut pool = pool(256);
    let (low, high) = (leased(&mut pool), leased(&mut pool));
    let (low, high) = (idle(&mut pool, low), idle(&mut pool, high));
    assert!(low < high);
    let id = pool.request(preferring(high, None)).unwrap();
    assert_eq!(pool.take(id).unwrap().connection(), high);
    // Without a hint the lowest id is leased.
    let id = pool.request(request()).unwrap();
    assert_eq!(pool.take(id).unwrap().connection(), low);
}

#[test]
fn h2a_2_a_stale_preferred_id_decides_as_absent() {
    let mut pool = pool(256);
    let (low, gone) = (leased(&mut pool), leased(&mut pool));
    let low = idle(&mut pool, low);
    pool.release(gone, Disposition::Discarded).unwrap();
    assert!(
        matches!(pool.next_action(), Some(Action::Close { connection, .. }) if connection == gone.connection())
    );
    pool.closed(gone.connection()).unwrap();
    let id = pool
        .request(preferring(gone.connection(), Some(WAIT)))
        .unwrap();
    let lease = pool.take(id).unwrap();
    assert_eq!(lease.connection(), low);
    // Another pool's connection id names nothing here.
    let low = idle(&mut pool, lease);
    let mut other = self::pool(256);
    let foreign = leased(&mut other).connection();
    let id = pool.request(preferring(foreign, Some(WAIT))).unwrap();
    assert_eq!(pool.take(id).unwrap().connection(), low);
}

#[test]
fn h2a_3_another_key_or_identity_an_excluded_or_unproven_preferred_is_not_taken() {
    // Another key, and another identity: the request opens a connection of its own.
    for (host, identity, wanted) in [
        ("other", Identity::Ambient, Identity::Ambient),
        (
            "host",
            Identity::Explicit("one".into()),
            Identity::Explicit("two".into()),
        ),
    ] {
        let mut pool = pool(256);
        let foreign = leased_as(&mut pool, ask(host, identity), true);
        let foreign = idle(&mut pool, foreign);
        let mut request = ask("host", wanted);
        request.prefer = Some(foreign);
        request.prefer_wait_ms = Some(WAIT);
        let id = pool.request(request).unwrap();
        assert_eq!(pool.take(id), Err(Error::WouldBlock));
        assert_ne!(connect(&mut pool), foreign);
    }
    // Excluded: the next compatible idle connection is leased.
    let mut pool = pool(256);
    let (low, high) = (leased(&mut pool), leased(&mut pool));
    let (low, high) = (idle(&mut pool, low), idle(&mut pool, high));
    let mut request = preferring(high, Some(WAIT));
    request.exclude = vec![high];
    let id = pool.request(request).unwrap();
    assert_eq!(pool.take(id).unwrap().connection(), low);
    // Unproven (never reusable): no wait for it; a connection is opened at once.
    let mut pool = self::pool(256);
    let unproven = leased_as(&mut pool, self::request(), false).connection();
    let id = pool.request(preferring(unproven, Some(WAIT))).unwrap();
    assert_eq!(pool.take(id), Err(Error::WouldBlock));
    assert_ne!(connect(&mut pool), unproven);
}

#[test]
fn h2a_4_a_leased_preferred_without_a_wait_decides_as_absent() {
    let mut pool = pool(256);
    let busy = leased(&mut pool).connection();
    let id = pool.request(preferring(busy, None)).unwrap();
    assert_eq!(pool.take(id), Err(Error::WouldBlock));
    assert_ne!(connect(&mut pool), busy);
}

#[test]
fn h2a_5_a_leased_preferred_with_a_wait_opens_and_evicts_nothing_and_is_leased_on_release() {
    // Room to open, and a compatible idle connection of a lower id: neither is used while the wait runs.
    let mut pool = pool(256);
    let (low, busy) = (leased(&mut pool), leased(&mut pool));
    let low = idle(&mut pool, low);
    let id = pool
        .request(preferring(busy.connection(), Some(WAIT)))
        .unwrap();
    assert_eq!(pool.take(id), Err(Error::WouldBlock));
    assert_eq!(pool.next_action(), None);
    assert_eq!(pool.counts().idle, 1);
    pool.release(busy, Disposition::Reusable).unwrap();
    assert_eq!(pool.take(id).unwrap().connection(), busy.connection());
    assert!(!pool.is_closing(low) && pool.counts().idle == 1);
    // At capacity with another key's idle connection: it is not evicted.
    let mut pool = self::pool(2);
    let other = leased_as(&mut pool, ask("other", Identity::Ambient), true);
    let busy = leased(&mut pool);
    idle(&mut pool, other);
    let id = pool
        .request(preferring(busy.connection(), Some(WAIT)))
        .unwrap();
    assert_eq!(pool.next_action(), None);
    assert_eq!(pool.counts().idle, 1);
    // A discarded preferred connection ends the wait at once: the request decides as absent and evicts for room.
    pool.release(busy, Disposition::Discarded).unwrap();
    let mut closed = Vec::new();
    while let Some(Action::Close { connection, .. }) = pool.next_action() {
        closed.push(connection);
    }
    assert!(closed.contains(&other.connection()) && closed.contains(&busy.connection()));
    assert_eq!(pool.take(id), Err(Error::WouldBlock));
}

#[derive(Default)]
struct Signal(AtomicUsize);
impl Signal {
    fn count(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}
impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn h2a_6_the_wait_lapses_at_its_bound_with_the_driver_parked_first() {
    const NOW: u64 = 1_000;
    let (pool, mut driver) = Pool::new(Config::default()).unwrap();
    driver.advance(NOW);
    let signal = Arc::new(Signal::default());
    let waker = Waker::from(signal.clone());
    let mut cx = Context::from_waker(&waker);
    let mut first = pool.checkout(request()).unwrap();
    let Poll::Ready(Some(Action::Connect { connection, .. })) =
        pin!(driver.next_action()).poll(&mut cx)
    else {
        panic!("a connect action");
    };
    driver
        .connected(connection, Ok(Some(Identity::Ambient)))
        .unwrap();
    let Poll::Ready(Ok(lease)) = pin!(&mut first).poll(&mut cx) else {
        panic!("the first lease");
    };
    // The driver is parked before the preferring request arrives; its arrival wakes the driver, which then finds
    // the lapse in `next_deadline`.
    let before = signal.count();
    let mut second = {
        let mut parked = pin!(driver.next_action());
        assert!(parked.as_mut().poll(&mut cx).is_pending());
        pool.checkout(preferring(connection, Some(WAIT))).unwrap()
    };
    assert!(
        signal.count() > before,
        "the request wakes the parked driver"
    );
    assert!(pin!(&mut second).poll(&mut cx).is_pending());
    assert_eq!(driver.next_deadline(), Some(NOW + WAIT));
    driver.advance(NOW + WAIT - 1);
    assert!(pin!(driver.next_action()).poll(&mut cx).is_pending());
    let before = signal.count();
    driver.advance(NOW + WAIT);
    assert!(signal.count() > before, "the lapse wakes the waiter");
    let Poll::Ready(Some(Action::Connect {
        connection: own, ..
    })) = pin!(driver.next_action()).poll(&mut cx)
    else {
        panic!("the lapsed request decides as absent and opens");
    };
    assert_ne!(own, connection);
    driver.connected(own, Ok(Some(Identity::Ambient))).unwrap();
    let Poll::Ready(Ok(second)) = pin!(&mut second).poll(&mut cx) else {
        panic!("the second lease");
    };
    assert_eq!(second.connection(), Ok(own));
    assert_eq!(lease.connection(), Ok(connection));
}

#[test]
fn h2a_7_the_wait_never_runs_past_the_request_deadline() {
    // Its own allocation timeout.
    let mut pool = pool(256);
    let busy = leased(&mut pool).connection();
    let mut request = preferring(busy, Some(WAIT));
    request.allocation_timeout_ms = Some(200);
    let id = pool.request(request).unwrap();
    assert_eq!(pool.next_deadline(), Some(1_200));
    pool.advance(1_200);
    assert_eq!(pool.take(id), Err(Error::AllocationTimeout));
    assert_eq!(pool.next_action(), None);
    // An absolute deadline.
    let id = pool
        .request_until(preferring(busy, Some(WAIT)), Some(1_300))
        .unwrap();
    assert_eq!(pool.next_deadline(), Some(1_300));
    pool.advance(1_300);
    assert_eq!(pool.take(id), Err(Error::AllocationTimeout));
    assert_eq!(pool.next_action(), None);
}

#[test]
fn h2a_8_a_wait_without_a_preferred_connection_or_past_the_cleanup_bound_is_invalid() {
    let mut pool = pool(256);
    let busy = leased(&mut pool).connection();
    let mut request = self::request();
    request.prefer_wait_ms = Some(WAIT);
    assert_eq!(pool.request(request), Err(Error::InvalidRequest));
    let cleanup = Config::default().cleanup_timeout_ms;
    for wait in [0, cleanup + 1] {
        assert_eq!(
            pool.request(preferring(busy, Some(wait))),
            Err(Error::InvalidRequest)
        );
    }
    assert!(pool.request(preferring(busy, Some(cleanup))).is_ok());
    assert!(pool.request(preferring(busy, None)).is_ok());
}

#[test]
fn h2a_9_fifo_an_earlier_waiter_without_a_hint_may_take_the_preferred_connection() {
    let mut pool = pool(1);
    let busy = leased(&mut pool);
    let earlier = pool.request(request()).unwrap();
    let later = pool
        .request(preferring(busy.connection(), Some(WAIT)))
        .unwrap();
    pool.release(busy, Disposition::Reusable).unwrap();
    let taken = pool.take(earlier).unwrap();
    assert_eq!(taken.connection(), busy.connection());
    assert_eq!(pool.take(later), Err(Error::WouldBlock));
    // Now the preferring request is the earliest waiter, and a later one without a hint does not take it first.
    let plain = pool.request(request()).unwrap();
    pool.release(taken, Disposition::Reusable).unwrap();
    assert_eq!(pool.take(later).unwrap().connection(), busy.connection());
    assert_eq!(pool.take(plain), Err(Error::WouldBlock));
}

#[test]
fn h2a_9_once_another_request_takes_the_preferred_connection_the_wait_ends() {
    // The wait is for the lease that was finishing when the request was made: once an earlier waiter of another
    // owner is leased the preferred connection, the preferring request waits on another binding's exchange no
    // longer. A compatible connection gone idle is leased at once, and so is room to open.
    for discard in [false, true] {
        let mut pool = pool(2);
        let owned = |session: &str| {
            let mut request = request();
            request.owner = Owner::new(session, "operation");
            request
        };
        let preferred = leased_as(&mut pool, owned("a"), true);
        let other = leased_as(&mut pool, owned("b"), true);
        let earlier = pool.request(owned("c")).unwrap();
        let mut later = owned("a");
        later.prefer = Some(preferred.connection());
        later.prefer_wait_ms = Some(Config::default().cleanup_timeout_ms);
        let later = pool.request(later).unwrap();
        pool.release(preferred, Disposition::Reusable).unwrap();
        assert_eq!(
            pool.take(earlier).unwrap().connection(),
            preferred.connection()
        );
        assert_eq!(pool.take(later), Err(Error::WouldBlock));
        // The next deadline is the request's own allocation deadline, not the lapse of a wait that has ended.
        assert_eq!(pool.next_deadline(), Some(1_000 + 30_000));
        if discard {
            pool.release(other, Disposition::Discarded).unwrap();
            let Some(Action::Close { connection, .. }) = pool.next_action() else {
                panic!("the discarded connection closes");
            };
            pool.closed(connection).unwrap();
            let own = connect(&mut pool);
            assert!(own != preferred.connection() && own != other.connection());
        } else {
            pool.release(other, Disposition::Reusable).unwrap();
            assert_eq!(pool.take(later).unwrap().connection(), other.connection());
        }
    }
}
