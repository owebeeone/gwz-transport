//! Per-owner limits (reuse design section 14, T1 and T2): a request carries its operation's limits, the machine counts the
//! entries each `Owner.session` has opening or leased and admits below them, and `Config`'s `total`, `per_host`,
//! `per_user_host` and `max_requests` are instance ceilings applied as max(ceiling, the request's limit): a lone operation
//! keeps its own limits, whatever they are, and operations that overlap share the ceilings.
use gwz_transport::{pool::*, protocol::Disposition};

fn ceilings(host: usize, total: usize) -> Config {
    Config {
        per_user_host: host,
        per_host: host,
        total,
        max_requests: 64,
        ..Config::default()
    }
}
fn limits(per_host: usize, total: usize, max_requests: usize) -> Capacity {
    Capacity {
        per_user_host: per_host,
        per_host,
        total,
        max_requests,
    }
}
fn ask(session: &str, host: &str, limits: Capacity) -> Request {
    let mut request = Request::new(
        Key::ssh("git", host, 22),
        Identity::Ambient,
        Owner::new(session, "operation"),
    );
    request.limits = Some(limits);
    request
}
/// Connects whatever the pool asked to connect.
fn connect_all(pool: &mut PoolMachine) {
    while let Some(action) = pool.next_action() {
        if let Action::Connect { connection, .. } = action {
            pool.connected(connection, Ok(Some(Identity::Ambient)))
                .unwrap();
        }
    }
}
/// Asks `count` times for `session` and returns how many leases the pool granted.
fn lease_up_to(
    pool: &mut PoolMachine,
    session: &str,
    host: &str,
    own: Capacity,
    count: usize,
    held: &mut Vec<LeaseId>,
) -> usize {
    let ids: Vec<_> = (0..count)
        .map(|_| pool.request(ask(session, host, own)).unwrap())
        .collect();
    connect_all(pool);
    let mut granted = 0;
    for id in ids {
        if let Ok(lease) = pool.take(id) {
            held.push(lease);
            granted += 1;
        }
    }
    granted
}

#[test]
fn a_lone_operation_above_the_ceilings_gets_its_own_limit() {
    // Design verification item 7: test ceilings of 8, a lone operation at 16 gets 16.
    let mut pool = PoolMachine::new(ceilings(8, 8)).unwrap();
    let mut held = Vec::new();
    let own = limits(16, 16, 64);
    assert_eq!(lease_up_to(&mut pool, "lone", "h", own, 20, &mut held), 16);
    assert_eq!(pool.counts().leased, 16);
}

#[test]
fn operations_that_overlap_share_the_ceilings() {
    // Item 7 again: two operations at 8 share the 8 the ceilings give.
    let mut pool = PoolMachine::new(ceilings(8, 8)).unwrap();
    let own = limits(8, 8, 64);
    let (mut a, mut b) = (Vec::new(), Vec::new());
    assert_eq!(lease_up_to(&mut pool, "a", "h", own, 6, &mut a), 6);
    assert_eq!(lease_up_to(&mut pool, "b", "h", own, 6, &mut b), 2);
    assert_eq!(pool.counts().leased, 8);
}

#[test]
fn bindings_with_different_limits_are_each_held_to_their_own() {
    // Plan 1.4's red row: --max-per-host 4 and 32 overlap, and neither is refused or resized.
    let mut pool = PoolMachine::new(ceilings(64, 64)).unwrap();
    let (small, large) = (limits(4, 256, 1024), limits(32, 256, 1024));
    let (mut a, mut b) = (Vec::new(), Vec::new());
    assert_eq!(lease_up_to(&mut pool, "four", "h", small, 8, &mut a), 4);
    assert_eq!(
        lease_up_to(&mut pool, "thirty-two", "h", large, 40, &mut b),
        32
    );
    assert_eq!(pool.counts().leased, 36);
    assert_eq!(pool.capacity(), Capacity::from(&ceilings(64, 64)));
}

#[test]
fn releasing_one_owner_changes_neither_the_other_s_limit_nor_its_connections() {
    let mut pool = PoolMachine::new(ceilings(64, 64)).unwrap();
    let (small, large) = (limits(4, 256, 1024), limits(32, 256, 1024));
    let (mut a, mut b) = (Vec::new(), Vec::new());
    lease_up_to(&mut pool, "four", "h", small, 4, &mut a);
    lease_up_to(&mut pool, "thirty-two", "h", large, 32, &mut b);
    for lease in a.drain(..) {
        pool.release(lease, Disposition::Reusable).unwrap();
    }
    pool.cancel_operation(&Owner::new("four", "operation"));
    // The released four stay idle for anyone; the other owner's 32 are untouched and it is still held to 32.
    assert_eq!(pool.counts().idle, 4);
    assert_eq!(pool.counts().leased, 32);
    assert_eq!(
        lease_up_to(&mut pool, "thirty-two", "h", large, 1, &mut b),
        0
    );
    // Idle entries belong to nobody: the first owner leases them again, up to its own limit.
    assert_eq!(lease_up_to(&mut pool, "four", "h", small, 5, &mut a), 4);
}

#[test]
fn an_owner_at_its_limit_evicts_nothing_for_it() {
    // Idle entries have no owner, so evicting one cannot give an owner at its own limit room.
    let mut pool = PoolMachine::new(ceilings(8, 64)).unwrap();
    let own = limits(1, 64, 64);
    let mut other = Vec::new();
    assert_eq!(
        lease_up_to(&mut pool, "other", "h2", limits(8, 64, 64), 1, &mut other),
        1
    );
    pool.release(other.pop().unwrap(), Disposition::Reusable)
        .unwrap();
    let mut mine = Vec::new();
    assert_eq!(lease_up_to(&mut pool, "mine", "h", own, 1, &mut mine), 1);
    let waiting = pool.request(ask("mine", "h", own)).unwrap();
    connect_all(&mut pool);
    assert_eq!(pool.take(waiting), Err(Error::WouldBlock));
    assert_eq!(
        pool.counts().idle,
        1,
        "the other owner's idle connection stays"
    );
    assert_eq!(pool.counts().closing, 0);
}

#[test]
fn an_owner_s_outstanding_requests_stay_below_its_own_max_requests() {
    let mut pool = PoolMachine::new(ceilings(8, 64)).unwrap();
    let own = limits(8, 64, 2);
    pool.request(ask("a", "h", own)).unwrap();
    pool.request(ask("a", "h", own)).unwrap();
    assert_eq!(pool.request(ask("a", "h", own)), Err(Error::Capacity));
    // Another owner is unaffected.
    assert!(pool.request(ask("b", "h", own)).is_ok());
}

#[test]
fn a_request_without_limits_is_held_to_the_pool_s_own_capacity() {
    let mut pool = PoolMachine::new(ceilings(2, 8)).unwrap();
    let mut request = ask("a", "h", limits(2, 8, 64));
    request.limits = None;
    let ids: Vec<_> = (0..3)
        .map(|_| pool.request(request.clone()).unwrap())
        .collect();
    connect_all(&mut pool);
    let granted = ids.into_iter().filter(|id| pool.take(*id).is_ok()).count();
    assert_eq!(granted, 2);
}

#[test]
fn invalid_limits_are_refused() {
    let mut pool = PoolMachine::new(ceilings(8, 8)).unwrap();
    let mut bad = limits(8, 8, 8);
    bad.total = 0;
    assert_eq!(pool.request(ask("a", "h", bad)), Err(Error::InvalidRequest));
}

#[test]
fn https_requests_are_counted_per_owner_too() {
    let mut pool = PoolMachine::new(ceilings(8, 64)).unwrap();
    let own = limits(1, 64, 64);
    let mut request = Request::new(
        Key::https("h", 443),
        Identity::Https,
        Owner::new("a", "operation"),
    );
    request.limits = Some(own);
    let first = pool.request(request.clone()).unwrap();
    let second = pool.request(request).unwrap();
    while let Some(action) = pool.next_action() {
        if let Action::Connect { connection, .. } = action {
            pool.connected(connection, Ok(Some(Identity::Https)))
                .unwrap();
        }
    }
    assert!(pool.take(first).is_ok());
    assert_eq!(pool.take(second), Err(Error::WouldBlock));
}

#[test]
fn operations_that_share_a_session_share_one_count_so_callers_key_one_per_request() {
    // State review P2-1: the machine counts entries per `Owner.session`, so two operations with differing limits under
    // ONE session judge each other's entries against their own limit. The caller that wants each held to its own limit
    // gives each limit-bearing request its own session (gwz-core's SSH and HTTPS paths do).
    let mut pool = PoolMachine::new(ceilings(64, 64)).unwrap();
    let (large, small) = (limits(32, 256, 1024), limits(4, 256, 1024));
    let (mut a, mut b) = (Vec::new(), Vec::new());
    assert_eq!(lease_up_to(&mut pool, "shared", "h", large, 10, &mut a), 10);
    assert_eq!(
        lease_up_to(&mut pool, "shared", "h", small, 4, &mut b),
        0,
        "the small limit is judged against the shared session's ten"
    );
    // With a session each, the same sequence holds each to its own.
    let mut pool = PoolMachine::new(ceilings(64, 64)).unwrap();
    let (mut a, mut b) = (Vec::new(), Vec::new());
    assert_eq!(lease_up_to(&mut pool, "one", "h", large, 10, &mut a), 10);
    assert_eq!(lease_up_to(&mut pool, "two", "h", small, 4, &mut b), 4);
    // Reverse order, one session: the small limit's four leave the large one 28 of its 32.
    let mut pool = PoolMachine::new(ceilings(64, 64)).unwrap();
    let (mut a, mut b) = (Vec::new(), Vec::new());
    assert_eq!(lease_up_to(&mut pool, "shared", "h", small, 4, &mut b), 4);
    assert_eq!(lease_up_to(&mut pool, "shared", "h", large, 40, &mut a), 28);
}
