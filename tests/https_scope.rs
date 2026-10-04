use gwz_transport::{pool::*, protocol::Disposition};
fn request(scope: Option<&str>) -> Request {
    Request::new(
        Key::https("host", 443),
        scope.map_or(Identity::Https, |s| Identity::HttpsScoped(s.into())),
        Owner::new("session", "operation"),
    )
}
#[test]
fn credential_scope_promotes_once_isolates_anonymous_and_retires_physically() {
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    let first = pool.request(request(None)).unwrap();
    let Some(Action::Connect { connection, .. }) = pool.next_action() else {
        panic!("connect");
    };
    pool.connected(connection, Ok(Some(Identity::Https)))
        .unwrap();
    let lease = pool.take(first).unwrap();
    pool.scope_https(lease, "opaque-a").unwrap();
    pool.scope_https(lease, "opaque-a").unwrap();
    assert_eq!(pool.scope_https(lease, "opaque-b"), Err(Error::WrongState));
    pool.release(lease, Disposition::Reusable).unwrap();
    let same = pool.request(request(Some("opaque-a"))).unwrap();
    let lease = pool.take(same).unwrap();
    assert_eq!(lease.connection(), connection);
    pool.release(lease, Disposition::Reusable).unwrap();
    let anonymous = pool.request(request(None)).unwrap();
    let Some(Action::Connect {
        connection: fresh, ..
    }) = pool.next_action()
    else {
        panic!("anonymous fresh connect");
    };
    assert_ne!(fresh, connection);
    pool.connected(fresh, Ok(Some(Identity::Https))).unwrap();
    let anon = pool.take(anonymous).unwrap();
    pool.retire_https_scope("opaque-a");
    assert!(
        matches!(pool.next_action(), Some(Action::Close { connection: closed, .. }) if closed == connection)
    );
    assert_eq!(pool.counts().closing, 1);
    pool.closed(connection).unwrap();
    assert_eq!(pool.counts().closing, 0);
    pool.release(anon, Disposition::Reusable).unwrap();
}
