//! A slot the host has just disposed of stays held for a settle time, so the
//! replacement cannot start while the server may still count the old
//! connection (gwz-core's adaptive concurrency design, sections 2.5, 4.5 and
//! 4.9; case 42). A hold carries the request that evicted its connection,
//! which is served first when the hold lapses.
use gwz_transport::{
    pool::*,
    protocol::{Disposition, Effect, ErrorCode, Failure},
};

const SETTLE: u64 = 250;

fn request(key: Key, identity: Identity) -> Request {
    Request::new(key, identity, Owner::new("session", "operation"))
}
fn ssh() -> Key {
    Key::ssh("git", "host", 22)
}
fn site() -> Site {
    ssh().site()
}
fn explicit(name: &str) -> Identity {
    Identity::Explicit(name.into())
}
fn next_connect(pool: &mut PoolMachine) -> ConnectionId {
    let Some(Action::Connect { connection, .. }) = pool.next_action() else {
        panic!("a connect action");
    };
    connection
}
fn next_close(pool: &mut PoolMachine) -> (ConnectionId, CloseReason) {
    let Some(Action::Close {
        connection, reason, ..
    }) = pool.next_action()
    else {
        panic!("a close action");
    };
    (connection, reason)
}
fn leased(pool: &mut PoolMachine, identity: Identity) -> LeaseId {
    let mut fresh = request(ssh(), identity.clone());
    fresh.fresh = true;
    let id = pool.request(fresh).unwrap();
    let connection = next_connect(pool);
    pool.connected(connection, Ok(Some(identity))).unwrap();
    pool.take(id).unwrap()
}
/// A pool at a limit of `limit` with `leased` leased and the rest idle of
/// identity A, all opened at time 0 with a settle time of 250 ms.
fn full(limit: usize, leased_count: usize) -> (PoolMachine, Vec<ConnectionId>) {
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    pool.advance(0);
    pool.set_limit(&site(), limit).unwrap();
    pool.set_settle(&site(), SETTLE).unwrap();
    for _ in 0..leased_count {
        leased(&mut pool, explicit("a"));
    }
    let mut idle = Vec::new();
    for _ in leased_count..limit {
        let lease = leased(&mut pool, explicit("a"));
        idle.push(lease.connection());
        pool.release(lease, Disposition::Reusable).unwrap();
    }
    (pool, idle)
}
fn refusal() -> Failure {
    Failure {
        detail: None,
        setup_cause: None,
        code: ErrorCode::Io,
        effect: Effect::None,
        facts: None,
    }
}

#[test]
fn an_evicted_slot_is_held_for_the_settle_time_and_then_serves_its_evictor() {
    // Case 42a.
    let (mut pool, idle) = full(8, 2);
    pool.advance(1_000);
    let b = pool.request(request(ssh(), explicit("b"))).unwrap();
    let (victim, reason) = next_close(&mut pool);
    assert_eq!(reason, CloseReason::Evicted);
    assert_eq!(victim, idle[0]);
    assert!(pool.next_action().is_none());
    pool.closed(victim).unwrap();
    // The slot is held: no connect, five idle survive, the deadline is the
    // hold's expiry.
    assert!(pool.next_action().is_none());
    assert_eq!(pool.take(b), Err(Error::WouldBlock));
    assert_eq!(pool.counts_for_key(&ssh()).idle, 5);
    assert_eq!(pool.settling(&site()), 1);
    let expiry = 1_000 + SETTLE;
    assert_eq!(pool.next_deadline(), Some(expiry));
    pool.advance(expiry - 1);
    assert!(pool.next_action().is_none());
    pool.advance(expiry);
    assert_eq!(pool.settling(&site()), 0);
    let connection = next_connect(&mut pool);
    pool.connected(connection, Ok(Some(explicit("b")))).unwrap();
    assert!(pool.take(b).is_ok());
    // Exactly one eviction in all.
    assert!(pool.next_action().is_none());
    assert_eq!(pool.counts_for_key(&ssh()).idle, 5);
}

#[test]
fn a_hold_does_not_block_installing_a_new_operations_capacity() {
    let (mut pool, idle) = full(2, 0);
    pool.advance(10);
    let b = pool.request(request(ssh(), explicit("b"))).unwrap();
    let (victim, _) = next_close(&mut pool);
    assert_eq!(victim, idle[0]);
    pool.closed(victim).unwrap();
    pool.abandon(b);
    assert_eq!(pool.settling(&site()), 1);
    pool.can_install_capacity(pool.capacity()).unwrap();
    pool.install_capacity(pool.capacity()).unwrap();
    assert_eq!(pool.settling(&site()), 0);
}

#[test]
fn two_evictors_each_wait_for_their_own_hold() {
    // Case 42b, with the closes acknowledged in either order.
    for reversed in [false, true] {
        let (mut pool, idle) = full(8, 2);
        pool.advance(1_000);
        let first = pool.request(request(ssh(), explicit("b"))).unwrap();
        let second = pool.request(request(ssh(), explicit("c"))).unwrap();
        let (one, one_reason) = next_close(&mut pool);
        let (two, two_reason) = next_close(&mut pool);
        assert_eq!(
            (one_reason, two_reason),
            (CloseReason::Evicted, CloseReason::Evicted)
        );
        assert_eq!([one, two], [idle[0], idle[1]]);
        assert!(pool.next_action().is_none());
        if reversed {
            pool.closed(two).unwrap();
            pool.closed(one).unwrap();
        } else {
            pool.closed(one).unwrap();
            pool.closed(two).unwrap();
        }
        assert_eq!(pool.settling(&site()), 2);
        assert!(pool.next_action().is_none());
        assert_eq!(pool.counts_for_key(&ssh()).idle, 4);
        pool.advance(1_000 + SETTLE);
        let one = next_connect(&mut pool);
        let two = next_connect(&mut pool);
        pool.connected(one, Ok(Some(explicit("b")))).unwrap();
        pool.connected(two, Ok(Some(explicit("c")))).unwrap();
        assert!(pool.take(first).is_ok());
        assert!(pool.take(second).is_ok());
        assert!(pool.next_action().is_none());
    }
}

#[test]
fn a_lapsed_hold_serves_its_own_evictor_before_an_earlier_waiter() {
    // The later request evicted the connection whose hold lapses first; the
    // earlier request must not take the slot and orphan the evictor.
    let (mut pool, idle) = full(2, 0);
    pool.advance(1_000);
    let earlier = pool.request(request(ssh(), explicit("b"))).unwrap();
    let later = pool.request(request(ssh(), explicit("c"))).unwrap();
    let (one, _) = next_close(&mut pool);
    let (two, _) = next_close(&mut pool);
    assert_eq!([one, two], [idle[0], idle[1]]);
    // `earlier` evicted `one`, `later` evicted `two`. `two` is disposed
    // first, so its hold lapses first.
    pool.closed(two).unwrap();
    pool.advance(1_100);
    pool.closed(one).unwrap();
    pool.advance(1_000 + SETTLE);
    let connection = next_connect(&mut pool);
    pool.connected(connection, Ok(Some(explicit("c")))).unwrap();
    assert!(pool.take(later).is_ok());
    assert_eq!(pool.take(earlier), Err(Error::WouldBlock));
    pool.advance(1_100 + SETTLE);
    let connection = next_connect(&mut pool);
    pool.connected(connection, Ok(Some(explicit("b")))).unwrap();
    assert!(pool.take(earlier).is_ok());
}

#[test]
fn a_hold_whose_evictor_is_gone_just_lapses() {
    let (mut pool, idle) = full(2, 1);
    pool.advance(1_000);
    let b = pool.request(request(ssh(), explicit("b"))).unwrap();
    let (victim, _) = next_close(&mut pool);
    assert_eq!(victim, idle[0]);
    pool.closed(victim).unwrap();
    pool.cancel(b).unwrap();
    // Another request may take the slot once the hold lapses.
    let c = pool.request(request(ssh(), explicit("c"))).unwrap();
    assert!(pool.next_action().is_none());
    pool.advance(1_000 + SETTLE);
    let connection = next_connect(&mut pool);
    pool.connected(connection, Ok(Some(explicit("c")))).unwrap();
    assert!(pool.take(c).is_ok());
}

#[test]
fn a_hold_counts_in_the_host_and_user_totals() {
    // The caps, not only the limit, count a hold until it lapses.
    let mut pool = PoolMachine::new(Config {
        per_host: 2,
        per_user_host: 2,
        ..Config::default()
    })
    .unwrap();
    pool.advance(0);
    pool.set_settle(&site(), SETTLE).unwrap();
    let lease = leased(&mut pool, Identity::Ambient);
    let held = lease.connection();
    leased(&mut pool, Identity::Ambient);
    pool.release(lease, Disposition::Discarded).unwrap();
    let (closing, _) = next_close(&mut pool);
    assert_eq!(closing, held);
    pool.closed(closing).unwrap();
    let next = pool.request(request(ssh(), Identity::Ambient)).unwrap();
    assert!(pool.next_action().is_none());
    assert_eq!(pool.take(next), Err(Error::WouldBlock));
    pool.advance(SETTLE);
    assert!(matches!(pool.next_action(), Some(Action::Connect { .. })));
}

#[test]
fn without_a_settle_time_a_freed_slot_is_free_at_once() {
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    pool.advance(0);
    pool.set_limit(&site(), 1).unwrap();
    let lease = leased(&mut pool, Identity::Ambient);
    let connection = lease.connection();
    pool.release(lease, Disposition::Discarded).unwrap();
    let _next = pool.request(request(ssh(), Identity::Ambient)).unwrap();
    assert_eq!(next_close(&mut pool).0, connection);
    pool.closed(connection).unwrap();
    assert_eq!(pool.settling(&site()), 0);
    assert!(matches!(pool.next_action(), Some(Action::Connect { .. })));
    // A settle time of zero clears it.
    pool.set_settle(&site(), SETTLE).unwrap();
    pool.set_settle(&site(), 0).unwrap();
    assert_eq!(pool.settle(&site()), 0);
}

#[test]
fn a_server_refusal_during_setup_leaves_no_hold() {
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    pool.advance(0);
    pool.set_limit(&site(), 1).unwrap();
    pool.set_settle(&site(), SETTLE).unwrap();
    let _a = pool.request(request(ssh(), Identity::Ambient)).unwrap();
    let connection = next_connect(&mut pool);
    pool.connected(connection, Err(refusal())).unwrap();
    assert_eq!(pool.settling(&site()), 0);
    let _b = pool.request(request(ssh(), Identity::Ambient)).unwrap();
    assert!(matches!(pool.next_action(), Some(Action::Connect { .. })));
}

#[test]
fn a_connect_the_client_cancelled_leaves_a_hold() {
    // Section 4.1: the server may still count it (F15).
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    pool.advance(0);
    pool.set_limit(&site(), 1).unwrap();
    pool.set_settle(&site(), SETTLE).unwrap();
    let a = pool.request(request(ssh(), Identity::Ambient)).unwrap();
    let connection = next_connect(&mut pool);
    pool.cancel(a).unwrap();
    assert!(matches!(
        pool.next_action(),
        Some(Action::CancelConnect { .. })
    ));
    pool.connected(connection, Err(refusal())).unwrap();
    assert_eq!(pool.settling(&site()), 1);
    let b = pool.request(request(ssh(), Identity::Ambient)).unwrap();
    assert!(pool.next_action().is_none());
    pool.advance(SETTLE);
    assert!(matches!(pool.next_action(), Some(Action::Connect { .. })));
    let _ = b;
}

#[test]
fn spontaneous_idle_loss_leaves_no_hold() {
    let (mut pool, idle) = full(1, 0);
    pool.idle_closed(idle[0]).unwrap();
    assert_eq!(pool.settling(&site()), 0);
}

#[test]
fn a_lapsed_hold_without_a_waiter_changes_nothing_but_the_count() {
    // The pool's own idle expiry is a client close too: it leaves a hold.
    let (mut pool, _) = full(1, 0);
    pool.advance(60_000);
    let (connection, reason) = next_close(&mut pool);
    assert_eq!(reason, CloseReason::IdleExpired);
    pool.closed(connection).unwrap();
    assert_eq!(pool.settling(&site()), 1);
    assert_eq!(pool.next_deadline(), Some(60_000 + SETTLE));
    pool.advance(60_000 + SETTLE);
    assert_eq!(pool.settling(&site()), 0);
    assert_eq!(pool.next_deadline(), None);
    assert!(pool.next_action().is_none());
}
