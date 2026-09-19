use gwz_transport::{pool::*, protocol::Disposition};
fn request(user: &str, port: u16) -> Request {
    Request::new(
        Key::ssh(user, "host", port),
        Identity::Ambient,
        Owner::new("session", "operation"),
    )
}
fn connection(pool: &mut PoolMachine) -> ConnectionId {
    let Some(Action::Connect { connection, .. }) = pool.next_action() else {
        panic!("connect");
    };
    connection
}
#[test]
fn user_host_capacity_spans_ports_in_every_physical_state() {
    for phase in 0..4 {
        let mut pool = PoolMachine::new(Config {
            per_user_host: 1,
            per_host: 4,
            total: 4,
            ..Default::default()
        })
        .unwrap();
        let first = pool.request(request("git", 22)).unwrap();
        let old = connection(&mut pool);
        let mut active = None;
        if phase > 0 {
            pool.connected(old, Ok(Some(Identity::Ambient))).unwrap();
            let lease = pool.take(first).unwrap();
            if phase == 1 {
                active = Some(lease);
            }
            if phase == 2 {
                pool.release(lease, Disposition::Reusable).unwrap();
            }
            if phase == 3 {
                pool.release(lease, Disposition::Discarded).unwrap();
            }
        }
        let alternate = pool.request(request("git", 2222)).unwrap();
        assert_eq!(pool.take(alternate), Err(Error::WouldBlock));
        if phase >= 2 {
            assert!(
                matches!(pool.next_action(), Some(Action::Close { connection, .. }) if connection == old)
            );
        }
        assert!(
            pool.next_action().is_none(),
            "port must not bypass user/host bound, phase={phase}"
        );
        let other = pool.request(request("other", 22)).unwrap();
        let other_connection = connection(&mut pool);
        pool.connected(other_connection, Ok(Some(Identity::Ambient)))
            .unwrap();
        let other_lease = pool.take(other).unwrap();
        assert!(pool.is_live(other_lease));
        if phase == 0 {
            pool.connected(old, Ok(Some(Identity::Ambient))).unwrap();
            let lease = pool.take(first).unwrap();
            pool.release(lease, Disposition::Discarded).unwrap();
        } else if phase == 1 {
            pool.release(active.unwrap(), Disposition::Discarded)
                .unwrap();
        }
        if phase < 2 {
            assert!(
                matches!(pool.next_action(), Some(Action::Close { connection, .. }) if connection == old)
            );
        }
        pool.closed(old).unwrap();
        let new = connection(&mut pool);
        assert_ne!(new, old);
    }
}

fn idle(pool: &mut PoolMachine) -> ConnectionId {
    let id = pool.request(request("git", 22)).unwrap();
    let connection = connection(pool);
    pool.connected(connection, Ok(Some(Identity::Ambient)))
        .unwrap();
    let lease = pool.take(id).unwrap();
    pool.release(lease, Disposition::Reusable).unwrap();
    connection
}
#[test]
fn spontaneous_idle_loss_prevents_reuse_and_validates_tokens() {
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    let mut other = PoolMachine::new(Config::default()).unwrap();
    let old = idle(&mut pool);
    let foreign = idle(&mut other);
    assert_eq!(pool.idle_closed(foreign), Err(Error::Stale));
    pool.idle_closed(old).unwrap();
    assert_eq!(pool.idle_closed(old), Err(Error::Stale));
    assert_eq!(pool.counts().total(), 0);
    let id = pool.request(request("git", 22)).unwrap();
    let new = connection(&mut pool);
    assert_ne!(new, old);
    assert_eq!(pool.take(id), Err(Error::WouldBlock));
    assert_eq!(pool.idle_closed(new), Err(Error::WrongState));
}
#[test]
fn checkout_winning_idle_loss_race_keeps_exclusive_lease_responsibility() {
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    let old = idle(&mut pool);
    let id = pool.request(request("git", 22)).unwrap();
    assert_eq!(pool.idle_closed(old), Err(Error::WrongState));
    let lease = pool.take(id).unwrap();
    assert_eq!(lease.connection(), old);
    assert!(pool.is_live(lease));
    assert_eq!(pool.idle_closed(old), Err(Error::WrongState));
    // The active exchange's host handles its own I/O failure and cleanup.
    pool.release(lease, Disposition::Discarded).unwrap();
    assert!(matches!(pool.next_action(), Some(Action::Close { .. })));
    pool.closed(old).unwrap();
}
#[test]
fn idle_loss_racing_eviction_wakes_capacity_waiter_without_an_extra_close() {
    let mut pool = PoolMachine::new(Config {
        total: 1,
        ..Default::default()
    })
    .unwrap();
    let old = idle(&mut pool);
    let waiting = pool.request(request("other", 22)).unwrap();
    assert_eq!(pool.take(waiting), Err(Error::WouldBlock));
    assert_eq!(pool.counts().closing, 1);
    pool.idle_closed(old).unwrap(); // Actual disposal arrived before Close dispatch.
    let new = connection(&mut pool);
    assert_ne!(old, new);
    assert_eq!(pool.closed(old), Err(Error::Stale));
}
#[test]
fn late_session_and_operation_cancellation_cannot_touch_a_fresh_session() {
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    let old_owner = Owner::new("session-one", "op-1");
    let new_owner = Owner::new("session-two", "op-1");
    let mut a = request("git", 22);
    a.owner = old_owner.clone();
    let a = pool.request(a).unwrap();
    let first = connection(&mut pool);
    pool.connected(first, Ok(Some(Identity::Ambient))).unwrap();
    let a = pool.take(a).unwrap();
    pool.release(a, Disposition::Reusable).unwrap();
    let mut b = request("git", 22);
    b.owner = new_owner.clone();
    let b = pool.request(b).unwrap();
    pool.cancel_operation(&old_owner);
    pool.cancel_session("session-one");
    let b = pool.take(b).unwrap();
    assert!(pool.is_live(b));
    pool.cancel_operation(&old_owner);
    pool.cancel_session("session-one");
    assert!(pool.is_live(b));
    assert_eq!(b.connection(), first);
    pool.cancel_operation(&new_owner);
    assert!(!pool.is_live(b));
}
#[test]
fn session_cancellation_covers_all_its_work_but_preserves_idle_and_other_sessions() {
    let mut pool = PoolMachine::new(Config {
        per_user_host: 1,
        ..Default::default()
    })
    .unwrap();
    let idle_connection = idle(&mut pool);
    let mut a = request("active", 22);
    a.owner = Owner::new("session-one", "op-1");
    let a = pool.request(a).unwrap();
    let active = connection(&mut pool);
    pool.connected(active, Ok(Some(Identity::Ambient))).unwrap();
    let active = pool.take(a).unwrap();
    let mut wait = request("active", 22);
    wait.owner = Owner::new("session-one", "op-2");
    let wait = pool.request(wait).unwrap();
    let mut opening = request("opening", 22);
    opening.owner = Owner::new("session-one", "op-3");
    let opening = pool.request(opening).unwrap();
    let connecting = connection(&mut pool);
    let mut ready = request("ready", 22);
    ready.owner = Owner::new("session-one", "op-4");
    let ready = pool.request(ready).unwrap();
    let ready_connection = connection(&mut pool);
    pool.connected(ready_connection, Ok(Some(Identity::Ambient)))
        .unwrap();
    let mut other = request("other", 22);
    other.owner = Owner::new("session-two", "op-1");
    let other = pool.request(other).unwrap();
    let other_connection = connection(&mut pool);
    pool.connected(other_connection, Ok(Some(Identity::Ambient)))
        .unwrap();
    let other = pool.take(other).unwrap();
    pool.cancel_session("session-one");
    assert!(!pool.is_live(active));
    assert!(pool.is_live(other));
    assert_eq!(pool.take(wait), Err(Error::Cancelled));
    assert_eq!(pool.take(opening), Err(Error::Cancelled));
    assert_eq!(pool.take(ready), Err(Error::Cancelled));
    assert_eq!(pool.counts().idle, 1);
    pool.idle_closed(idle_connection).unwrap();
    assert_eq!(pool.idle_closed(connecting), Err(Error::WrongState));
    for owner in [
        Owner::new("", "op"),
        Owner::new("session", ""),
        Owner::new("session\n", "op"),
    ] {
        let mut bad = request("git", 22);
        bad.owner = owner;
        assert_eq!(pool.request(bad), Err(Error::InvalidRequest));
    }
}
