//! A fresh request never takes an idle connection: the host asks for one when
//! a reused connection was found dead before its exchange wrote a byte.
use gwz_transport::{pool::*, protocol::Disposition};
fn request(fresh: bool) -> Request {
    let mut request = Request::new(
        Key::ssh("git", "host", 22),
        Identity::Ambient,
        Owner::new("session", "operation"),
    );
    request.fresh = fresh;
    request
}
fn connection(pool: &mut PoolMachine) -> ConnectionId {
    let Some(Action::Connect { connection, .. }) = pool.next_action() else {
        panic!("connect");
    };
    connection
}
fn idle(pool: &mut PoolMachine) -> ConnectionId {
    let id = pool.request(request(false)).unwrap();
    let connection = connection(pool);
    pool.connected(connection, Ok(Some(Identity::Ambient)))
        .unwrap();
    let lease = pool.take(id).unwrap();
    pool.release(lease, Disposition::Reusable).unwrap();
    connection
}
#[test]
fn a_new_request_is_not_fresh() {
    assert!(
        !Request::new(
            Key::ssh("git", "host", 22),
            Identity::Ambient,
            Owner::new("session", "operation"),
        )
        .fresh
    );
}
#[test]
fn a_plain_request_reuses_the_idle_connection() {
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    let old = idle(&mut pool);
    let id = pool.request(request(false)).unwrap();
    assert_eq!(pool.take(id).unwrap().connection(), old);
}
#[test]
fn a_fresh_request_opens_a_new_connection_beside_a_compatible_idle_one() {
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    let old = idle(&mut pool);
    let id = pool.request(request(true)).unwrap();
    assert_eq!(pool.take(id), Err(Error::WouldBlock));
    let new = connection(&mut pool);
    assert_ne!(new, old);
    pool.connected(new, Ok(Some(Identity::Ambient))).unwrap();
    assert_eq!(pool.take(id).unwrap().connection(), new);
    assert_eq!(pool.counts().idle, 1);
}
#[test]
fn a_fresh_request_at_capacity_evicts_the_idle_connection() {
    let mut pool = PoolMachine::new(Config {
        total: 1,
        ..Default::default()
    })
    .unwrap();
    let old = idle(&mut pool);
    let id = pool.request(request(true)).unwrap();
    assert_eq!(pool.take(id), Err(Error::WouldBlock));
    assert!(
        matches!(pool.next_action(), Some(Action::Close { connection, .. }) if connection == old)
    );
    pool.closed(old).unwrap();
    let new = connection(&mut pool);
    assert_ne!(new, old);
    pool.connected(new, Ok(Some(Identity::Ambient))).unwrap();
    assert_eq!(pool.take(id).unwrap().connection(), new);
}
