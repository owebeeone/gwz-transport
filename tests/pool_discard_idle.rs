//! `discard_idle(site)` closes every idle connection of a site in one step,
//! through no waiting request: gwz-core calls it when a long `Retry-After`
//! hold ends, because the server's keep-alive may have closed them during the
//! hold (adaptive concurrency design sections 4.5 and 4.9; case 51).
use gwz_transport::{pool::*, protocol::Disposition};

fn request(key: Key, identity: Identity) -> Request {
    let mut request = Request::new(key, identity, Owner::new("session", "operation"));
    request.fresh = true;
    request
}
fn https() -> Key {
    Key::https("host", 443)
}
fn site() -> Site {
    https().site()
}
fn next_connect(pool: &mut PoolMachine) -> ConnectionId {
    let Some(Action::Connect { connection, .. }) = pool.next_action() else {
        panic!("a connect action");
    };
    connection
}
fn lease(pool: &mut PoolMachine, key: Key, identity: Identity) -> LeaseId {
    let id = pool.request(request(key, identity.clone())).unwrap();
    let connection = next_connect(pool);
    pool.connected(connection, Ok(Some(identity))).unwrap();
    pool.take(id).unwrap()
}
fn idle(pool: &mut PoolMachine, key: Key, identity: Identity) -> ConnectionId {
    let lease = lease(pool, key, identity);
    let connection = lease.connection();
    pool.release(lease, Disposition::Reusable).unwrap();
    connection
}
fn closes(pool: &mut PoolMachine) -> Vec<(ConnectionId, CloseReason)> {
    let mut found = Vec::new();
    while let Some(action) = pool.next_action() {
        let Action::Close {
            connection, reason, ..
        } = action
        else {
            panic!("only closes: {action:?}");
        };
        found.push((connection, reason));
    }
    found
}

#[test]
fn every_idle_connection_of_the_site_is_discarded_in_one_step() {
    // Case 51: ten idle connections and five leased.
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    let held: Vec<_> = (0..5)
        .map(|_| lease(&mut pool, https(), Identity::Https))
        .collect();
    let idle_ids: Vec<_> = (0..10)
        .map(|_| idle(&mut pool, https(), Identity::Https))
        .collect();
    assert_eq!(pool.discard_idle(&site()), 10);
    let found = closes(&mut pool);
    assert_eq!(found.len(), 10);
    assert!(
        found
            .iter()
            .all(|(_, reason)| *reason == CloseReason::Discarded)
    );
    let mut closed: Vec<_> = found.iter().map(|(connection, _)| *connection).collect();
    closed.sort();
    assert_eq!(closed, idle_ids);
    // The leased connections are untouched.
    assert!(held.iter().all(|lease| pool.is_live(*lease)));
    assert_eq!(pool.counts_for_key(&https()).leased, 5);
    assert_eq!(pool.counts_for_key(&https()).closing, 10);
    // A second discard finds nothing.
    assert_eq!(pool.discard_idle(&site()), 0);
}

#[test]
fn a_discard_takes_every_identity_and_username_on_the_site_and_no_other_site() {
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    let alice = idle(
        &mut pool,
        Key::ssh("alice", "host", 22),
        Identity::Explicit("a".into()),
    );
    let bob = idle(&mut pool, Key::ssh("bob", "host", 22), Identity::Ambient);
    let other_port = idle(
        &mut pool,
        Key::ssh("alice", "host", 2222),
        Identity::Ambient,
    );
    let other_host = idle(
        &mut pool,
        Key::ssh("alice", "elsewhere", 22),
        Identity::Ambient,
    );
    let other_scheme = idle(&mut pool, https(), Identity::Https);
    let ssh_site = Key::ssh("anyone", "host", 22).site();
    assert_eq!(pool.discard_idle(&ssh_site), 2);
    let mut closed: Vec<_> = closes(&mut pool).into_iter().map(|(id, _)| id).collect();
    closed.sort();
    assert_eq!(closed, [alice, bob]);
    assert_eq!(pool.counts().idle, 3);
    assert!(
        pool.discard_idle(&Key::ssh("alice", "host", 2222).site()) == 1
            && pool.discard_idle(&Key::ssh("alice", "elsewhere", 22).site()) == 1
            && pool.discard_idle(&site()) == 1
    );
    let mut rest: Vec<_> = closes(&mut pool).into_iter().map(|(id, _)| id).collect();
    rest.sort();
    assert_eq!(rest, [other_port, other_host, other_scheme]);
}

#[test]
fn a_discard_is_not_an_eviction_and_serves_no_waiting_request() {
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    pool.set_limit(&site(), 2).unwrap();
    let old = idle(&mut pool, https(), Identity::Https);
    lease(&mut pool, https(), Identity::Https);
    let waiting = pool.request(request(https(), Identity::Https)).unwrap();
    // The waiting request already evicted the idle connection for room; a
    // discard now finds nothing idle and starts no second close.
    let first = closes(&mut pool);
    assert_eq!(first, [(old, CloseReason::Evicted)]);
    assert_eq!(pool.discard_idle(&site()), 0);
    assert!(pool.next_action().is_none());
    pool.closed(old).unwrap();
    assert!(matches!(pool.next_action(), Some(Action::Connect { .. })));
    let _ = waiting;
}

#[test]
fn discarded_connections_leave_evictorless_holds_that_lapse() {
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    pool.advance(0);
    pool.set_settle(&site(), 250).unwrap();
    for _ in 0..3 {
        idle(&mut pool, https(), Identity::Https);
    }
    assert_eq!(pool.discard_idle(&site()), 3);
    for (connection, _) in closes(&mut pool) {
        pool.closed(connection).unwrap();
    }
    assert_eq!(pool.settling(&site()), 3);
    assert_eq!(pool.next_deadline(), Some(250));
    // A request waits behind the holds only when they fill a cap.
    pool.advance(250);
    assert_eq!(pool.settling(&site()), 0);
    assert_eq!(pool.counts().total(), 0);
}

#[test]
fn the_async_pool_offers_the_discard_and_the_settle_time() {
    let (pool, _driver) = Pool::new(Config::default()).unwrap();
    pool.set_settle(&site(), 250).unwrap();
    assert_eq!(pool.discard_idle(&site()), 0);
}
