//! A lease given back unused (its host could not authorize the reuse it offered, step 1.6) keeps its connection's idle
//! time, and a request can exclude the idle connections its host found unable to serve it, so the next compatible idle
//! connection, or a new one, is offered instead.
use gwz_transport::{pool::*, protocol::Disposition};
fn request() -> Request {
    Request::new(
        Key::ssh("git", "host", 22),
        Identity::Ambient,
        Owner::new("session", "operation"),
    )
}
fn connect(pool: &mut PoolMachine) -> ConnectionId {
    let Some(Action::Connect { connection, .. }) = pool.next_action() else {
        panic!("connect");
    };
    pool.connected(connection, Ok(Some(Identity::Ambient)))
        .unwrap();
    connection
}
/// Two idle connections, the first released first (both at time 0).
fn two_idle(pool: &mut PoolMachine) -> (ConnectionId, ConnectionId) {
    let first = pool.request(request()).unwrap();
    let a = connect(pool);
    let first = pool.take(first).unwrap();
    let second = pool.request(request()).unwrap();
    let b = connect(pool);
    let second = pool.take(second).unwrap();
    pool.release(first, Disposition::Reusable).unwrap();
    pool.release(second, Disposition::Reusable).unwrap();
    (a, b)
}
#[test]
fn an_excluded_idle_connection_is_not_leased_and_the_next_one_is() {
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    let (a, b) = two_idle(&mut pool);
    let id = pool.request(request()).unwrap();
    assert_eq!(
        pool.take(id).unwrap().connection(),
        a,
        "the lowest id first"
    );
    let mut excluding = request();
    excluding.exclude = vec![a];
    let id = pool.request(excluding).unwrap();
    assert_eq!(pool.take(id).unwrap().connection(), b);
}
#[test]
fn a_request_that_excludes_every_idle_connection_opens_a_new_one() {
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    let (a, b) = two_idle(&mut pool);
    let mut excluding = request();
    excluding.exclude = vec![a, b];
    let id = pool.request(excluding).unwrap();
    assert_eq!(pool.take(id), Err(Error::WouldBlock));
    let new = connect(&mut pool);
    assert!(new != a && new != b);
    assert_eq!(pool.take(id).unwrap().connection(), new);
    assert_eq!(pool.counts().idle, 2);
}
#[test]
fn more_exclusions_than_the_bound_are_an_invalid_request() {
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    let (a, _) = two_idle(&mut pool);
    let mut excluding = request();
    excluding.exclude = vec![a; MAX_EXCLUDED + 1];
    assert_eq!(pool.request(excluding), Err(Error::InvalidRequest));
}
#[test]
fn an_unused_release_keeps_the_idle_time_and_the_connection_expires_from_its_last_real_release() {
    let config = Config::default();
    let idle_ms = config.idle_timeout_ms;
    let mut pool = PoolMachine::new(config).unwrap();
    let (a, b) = two_idle(&mut pool);
    // Bounced at half the idle time: given back unused, it keeps the idle time it had.
    pool.advance(idle_ms / 2);
    let id = pool.request(request()).unwrap();
    let lease = pool.take(id).unwrap();
    assert_eq!(lease.connection(), a);
    pool.release_unused(lease).unwrap();
    // A real release of b restarts b's idle time.
    let id = pool
        .request({
            let mut excluding = request();
            excluding.exclude = vec![a];
            excluding
        })
        .unwrap();
    let lease = pool.take(id).unwrap();
    assert_eq!(lease.connection(), b);
    pool.release(lease, Disposition::Reusable).unwrap();
    pool.advance(idle_ms);
    let closed: Vec<_> = std::iter::from_fn(|| pool.next_action())
        .filter_map(|action| match action {
            Action::Close { connection, .. } => Some(connection),
            _ => None,
        })
        .collect();
    assert_eq!(
        closed,
        vec![a],
        "a expired from its last real release; b did not"
    );
}
#[test]
fn a_reusable_release_after_an_exchange_restarts_the_idle_time() {
    let config = Config::default();
    let idle_ms = config.idle_timeout_ms;
    let mut pool = PoolMachine::new(config).unwrap();
    let (a, _) = two_idle(&mut pool);
    pool.advance(idle_ms / 2);
    let id = pool.request(request()).unwrap();
    let lease = pool.take(id).unwrap();
    assert_eq!(lease.connection(), a);
    pool.release(lease, Disposition::Reusable).unwrap();
    pool.advance(idle_ms - 1);
    let closes = std::iter::from_fn(|| pool.next_action())
        .filter(|action| matches!(action, Action::Close { connection, .. } if *connection == a))
        .count();
    assert_eq!(closes, 0, "a served an exchange: its idle time restarted");
}
