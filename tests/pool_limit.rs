//! A numeric limit per site (scheme, host, port), set by the host to its
//! admission target. The pool compares it with the connections on the site,
//! whatever their username, and learns nothing else about why the number is
//! what it is (gwz-core's adaptive concurrency design, section 4.9).
use gwz_transport::{pool::*, protocol::Disposition};

fn request(key: Key, identity: Identity) -> Request {
    Request::new(key, identity, Owner::new("session", "operation"))
}
fn ssh(port: u16) -> Key {
    Key::ssh("git", "host", port)
}
fn next_connect(pool: &mut PoolMachine) -> ConnectionId {
    let Some(Action::Connect { connection, .. }) = pool.next_action() else {
        panic!("a connect action");
    };
    connection
}
/// A connected, leased connection of `key`: always a new one, never a reused
/// idle connection.
fn leased(pool: &mut PoolMachine, key: Key) -> LeaseId {
    let mut fresh = request(key, Identity::Ambient);
    fresh.fresh = true;
    let id = pool.request(fresh).unwrap();
    let connection = next_connect(pool);
    pool.connected(connection, Ok(Some(Identity::Ambient)))
        .unwrap();
    pool.take(id).unwrap()
}
/// An idle connection of `key`.
fn idle(pool: &mut PoolMachine, key: Key) -> ConnectionId {
    let lease = leased(pool, key);
    let connection = lease.connection();
    pool.release(lease, Disposition::Reusable).unwrap();
    connection
}
fn site(port: u16) -> Site {
    ssh(port).site()
}

#[test]
fn a_site_without_a_limit_is_bounded_only_by_the_caps() {
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    assert_eq!(pool.limit(&site(22)), None);
    for _ in 0..5 {
        leased(&mut pool, ssh(22));
    }
    assert_eq!(pool.counts_for_key(&ssh(22)).leased, 5);
}

#[test]
fn a_limit_holds_a_request_until_the_limit_is_raised() {
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    pool.set_limit(&site(22), 2).unwrap();
    leased(&mut pool, ssh(22));
    leased(&mut pool, ssh(22));
    let third = pool.request(request(ssh(22), Identity::Ambient)).unwrap();
    assert_eq!(pool.take(third), Err(Error::WouldBlock));
    assert!(pool.next_action().is_none());
    pool.set_limit(&site(22), 3).unwrap();
    let connection = next_connect(&mut pool);
    pool.connected(connection, Ok(Some(Identity::Ambient)))
        .unwrap();
    assert!(pool.take(third).is_ok());
}

#[test]
fn a_limit_counts_every_username_on_the_site() {
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    pool.set_limit(&site(22), 2).unwrap();
    leased(&mut pool, Key::ssh("alice", "host", 22));
    leased(&mut pool, Key::ssh("bob", "host", 22));
    let carol = pool
        .request(request(Key::ssh("carol", "host", 22), Identity::Ambient))
        .unwrap();
    assert_eq!(pool.take(carol), Err(Error::WouldBlock));
    assert!(pool.next_action().is_none());
}

#[test]
fn a_limit_belongs_to_its_port_though_the_host_cap_does_not() {
    // F16: per_host counts a host across ports, so the limit must not.
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    pool.set_limit(&site(22), 1).unwrap();
    leased(&mut pool, ssh(22));
    leased(&mut pool, ssh(2222));
    let again = pool.request(request(ssh(22), Identity::Ambient)).unwrap();
    assert_eq!(pool.take(again), Err(Error::WouldBlock));
    assert!(pool.next_action().is_none());
    // The host cap still counts both ports.
    assert_eq!(pool.counts_for_host("host").leased, 2);
}

#[test]
fn a_limit_belongs_to_its_scheme() {
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    pool.set_limit(&site(443), 1).unwrap();
    let id = pool
        .request(request(Key::https("host", 443), Identity::Https))
        .unwrap();
    let connection = next_connect(&mut pool);
    pool.connected(connection, Ok(Some(Identity::Https)))
        .unwrap();
    assert!(pool.take(id).is_ok());
    // SSH on the same host and number is another site.
    leased(&mut pool, ssh(443));
}

#[test]
fn a_limit_does_not_apply_to_another_host() {
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    pool.set_limit(&site(22), 1).unwrap();
    leased(&mut pool, ssh(22));
    leased(&mut pool, Key::ssh("git", "other", 22));
}

#[test]
fn closing_connections_count_against_the_limit() {
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    pool.set_limit(&site(22), 1).unwrap();
    let lease = leased(&mut pool, ssh(22));
    let connection = lease.connection();
    pool.release(lease, Disposition::Discarded).unwrap();
    let next = pool.request(request(ssh(22), Identity::Ambient)).unwrap();
    assert!(matches!(
        pool.next_action(),
        Some(Action::Close { connection: closing, .. }) if closing == connection
    ));
    assert_eq!(pool.take(next), Err(Error::WouldBlock));
    assert!(pool.next_action().is_none());
    pool.closed(connection).unwrap();
    assert!(matches!(pool.next_action(), Some(Action::Connect { .. })));
}

#[test]
fn a_request_at_the_limit_evicts_an_idle_connection_of_its_own_site() {
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    pool.set_limit(&site(22), 2).unwrap();
    let other = idle(&mut pool, ssh(2222));
    let old = idle(&mut pool, ssh(22));
    leased(&mut pool, ssh(22));
    let id = pool
        .request(request(ssh(22), Identity::Explicit("key".into())))
        .unwrap();
    // The oldest idle connection is on another site: it is not the victim.
    assert!(matches!(
        pool.next_action(),
        Some(Action::Close { connection, reason: CloseReason::Evicted, .. }) if connection == old
    ));
    pool.closed(old).unwrap();
    let fresh = next_connect(&mut pool);
    pool.connected(fresh, Ok(Some(Identity::Explicit("key".into()))))
        .unwrap();
    assert!(pool.take(id).is_ok());
    assert_eq!(pool.counts_for_key(&ssh(2222)).idle, 1);
    assert!(pool.next_action().is_none());
    let _ = other;
}

#[test]
fn a_limit_below_the_count_closes_nothing_leased_and_evicts_idle_one_at_a_time() {
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    let first = idle(&mut pool, ssh(22));
    let second = idle(&mut pool, ssh(22));
    leased(&mut pool, ssh(22));
    pool.set_limit(&site(22), 1).unwrap();
    // Lowering the limit closes nothing by itself.
    assert!(pool.next_action().is_none());
    let id = pool
        .request(request(ssh(22), Identity::Explicit("key".into())))
        .unwrap();
    assert!(matches!(
        pool.next_action(),
        Some(Action::Close { connection, .. }) if connection == first
    ));
    pool.closed(first).unwrap();
    // Still above the limit: the request evicts the next idle connection.
    assert!(matches!(
        pool.next_action(),
        Some(Action::Close { connection, .. }) if connection == second
    ));
    assert_eq!(pool.take(id), Err(Error::WouldBlock));
    pool.closed(second).unwrap();
    // One leased connection remains: at the limit, so it waits for it.
    assert!(pool.next_action().is_none());
    assert_eq!(pool.counts_for_key(&ssh(22)).leased, 1);
}

#[test]
fn a_test_request_creates_beside_an_idle_connection_when_the_limit_has_room() {
    // Section 10.2 case 41: STABLE at 8 with 7 leased and 1 idle of another
    // identity; the limit rises to 9 for the test, whose request is fresh.
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    pool.set_limit(&site(22), 8).unwrap();
    for _ in 0..7 {
        leased(&mut pool, ssh(22));
    }
    let old = idle(&mut pool, ssh(22));
    pool.set_limit(&site(22), 9).unwrap();
    let mut test = request(ssh(22), Identity::Ambient);
    test.fresh = true;
    let id = pool.request(test).unwrap();
    // No eviction, no wait: a ninth entry is created.
    let connection = next_connect(&mut pool);
    assert_ne!(connection, old);
    pool.connected(connection, Ok(Some(Identity::Ambient)))
        .unwrap();
    assert_eq!(pool.take(id).unwrap().connection(), connection);
    assert_eq!(pool.counts_for_key(&ssh(22)).total(), 9);
    assert!(pool.next_action().is_none());
    // The result lowers the limit again; nothing is closed by that.
    pool.set_limit(&site(22), 8).unwrap();
    assert!(pool.next_action().is_none());
    // The idle connection is reused again: the limit applies to creating.
    let next = pool.request(request(ssh(22), Identity::Ambient)).unwrap();
    assert_eq!(pool.take(next).map(|lease| lease.connection()), Ok(old));
}

#[test]
fn a_limit_is_cleared_and_validated() {
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    assert_eq!(pool.set_limit(&site(22), 0), Err(Error::InvalidRequest));
    let bad = Site {
        scheme: gwz_transport::protocol::Scheme::Ssh,
        host: String::new(),
        port: 22,
    };
    assert_eq!(pool.set_limit(&bad, 1), Err(Error::InvalidRequest));
    pool.set_limit(&site(22), 1).unwrap();
    assert_eq!(pool.limit(&site(22)), Some(1));
    pool.clear_limit(&site(22));
    assert_eq!(pool.limit(&site(22)), None);
    leased(&mut pool, ssh(22));
    leased(&mut pool, ssh(22));
}

#[test]
fn a_new_operations_capacity_drops_the_old_operations_limits() {
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    pool.set_limit(&site(22), 1).unwrap();
    pool.set_settle(&site(22), 250).unwrap();
    pool.install_capacity(pool.capacity()).unwrap();
    assert_eq!(pool.limit(&site(22)), None);
    leased(&mut pool, ssh(22));
    leased(&mut pool, ssh(22));
}

#[test]
fn the_async_pool_offers_the_same_operations() {
    let (pool, _driver) = Pool::new(Config::default()).unwrap();
    pool.set_limit(&site(22), 1).unwrap();
    assert_eq!(pool.limit(&site(22)), Some(1));
    assert_eq!(pool.set_limit(&site(22), 0), Err(Error::InvalidRequest));
    pool.clear_limit(&site(22));
    assert_eq!(pool.limit(&site(22)), None);
}

#[test]
fn a_control_handle_sets_the_numbers_but_is_not_an_owner() {
    // gwz-core's limit governor holds one beside the endpoint's own pool: it
    // must not keep the pool open when the endpoint drops its last Pool.
    let (pool, driver) = Pool::new(Config::default()).unwrap();
    let control = pool.control();
    control.set_limit(&site(22), 3).unwrap();
    control.set_settle(&site(22), 250).unwrap();
    assert_eq!(control.settle(&site(22)), 250);
    assert_eq!(pool.settle(&site(22)), 250);
    assert_eq!(control.limit(&site(22)), Some(3));
    assert_eq!(pool.limit(&site(22)), Some(3));
    assert_eq!(driver.control().limit(&site(22)), Some(3));
    assert_eq!(control.discard_idle(&site(22)), 0);
    control.clear_limit(&site(22));
    assert_eq!(pool.limit(&site(22)), None);
    // Dropping the last Pool shuts the pool down, whatever handles remain,
    // and a stopped pool takes no new numbers.
    drop(pool);
    assert_eq!(control.set_limit(&site(22), 3), Err(Error::Shutdown));
    assert_eq!(control.set_settle(&site(22), 1), Err(Error::Shutdown));
    drop(driver);
}
