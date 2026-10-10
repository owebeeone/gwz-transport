//! An opaque tag a request carries to the connection it opens, and a per-site
//! switch that stops the pool evicting idle connections for a waiting request
//! (gwz-core's adaptive concurrency design, sections 4.5 and 4.9). The pool
//! reads neither for any decision but the switch; it learns no error codes.
use gwz_transport::{pool::*, protocol::Disposition};

fn request(key: Key, identity: Identity) -> Request {
    Request::new(key, identity, Owner::new("session", "operation"))
}
fn ssh(port: u16) -> Key {
    Key::ssh("git", "host", port)
}

#[test]
fn a_connect_carries_the_tag_of_the_request_it_serves() {
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    let mut tagged = request(ssh(22), Identity::Ambient);
    tagged.tag = Some("member-1".into());
    pool.request(tagged).unwrap();
    let Some(Action::Connect { tag, .. }) = pool.next_action() else {
        panic!("a connect action");
    };
    assert_eq!(tag.as_deref(), Some("member-1"));
    pool.request(request(ssh(22), Identity::Ambient)).unwrap();
    let Some(Action::Connect { tag, .. }) = pool.next_action() else {
        panic!("a connect action");
    };
    assert_eq!(tag, None, "an untagged request opens an untagged connect");
}

#[test]
fn no_evict_makes_a_waiting_request_wait_instead_of_closing_an_idle_connection() {
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    let site = ssh(22).site();
    pool.set_limit(&site, 1).unwrap();
    // One idle connection of identity A fills the site.
    let mut first = request(ssh(22), Identity::Ambient);
    first.fresh = true;
    let id = pool.request(first).unwrap();
    let Some(Action::Connect { connection, .. }) = pool.next_action() else {
        panic!("a connect action");
    };
    pool.connected(connection, Ok(Some(Identity::Ambient)))
        .unwrap();
    let lease = pool.take(id).unwrap();
    pool.release(lease, Disposition::Reusable).unwrap();
    // A request of another identity needs room. Evictions suppressed: it waits.
    pool.set_no_evict(&site, true).unwrap();
    assert!(pool.no_evict(&site));
    pool.request(request(ssh(22), Identity::Explicit("b".into())))
        .unwrap();
    assert!(pool.next_action().is_none(), "nothing is closed");
    assert_eq!(pool.counts_for_key(&ssh(22)).idle, 1);
    // Allowed again, it evicts as before.
    pool.set_no_evict(&site, false).unwrap();
    assert!(matches!(pool.next_action(), Some(Action::Close { .. })));
}

#[test]
fn a_capacity_install_clears_the_switch() {
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    let site = ssh(22).site();
    pool.set_no_evict(&site, true).unwrap();
    pool.install_capacity(pool.capacity()).unwrap();
    assert!(!pool.no_evict(&site));
}
