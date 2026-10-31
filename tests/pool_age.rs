//! A connection's maximum age and the suspend rule (contract section 10; Phase 1 remediation R1b): an aged
//! connection is retired when it is released, never returned to idle and never leased past its age, and an epoch
//! change retires every idle one.
use gwz_transport::{pool::*, protocol::Disposition};

const AGE: u64 = 1_000;

fn pool() -> PoolMachine {
    PoolMachine::new(Config {
        max_age_ms: AGE,
        ..Config::default()
    })
    .unwrap()
}
fn request() -> Request {
    Request::new(
        Key::ssh("git", "host", 22),
        Identity::Ambient,
        Owner::new("session", "operation"),
    )
}
fn connect(pool: &mut PoolMachine) -> ConnectionId {
    let Some(Action::Connect { connection, .. }) = pool.next_action() else {
        panic!("a connect action");
    };
    connection
}
/// A lease on a new connection, established now.
fn open(pool: &mut PoolMachine) -> LeaseId {
    let mut fresh = request();
    fresh.fresh = true;
    let id = pool.request(fresh).unwrap();
    let connection = connect(pool);
    pool.connected(connection, Ok(Some(Identity::Ambient)))
        .unwrap();
    pool.take(id).unwrap()
}
/// A lease on the idle connection (reused).
fn reuse(pool: &mut PoolMachine) -> LeaseId {
    let id = pool.request(request()).unwrap();
    pool.take(id).unwrap()
}

#[test]
fn a_connection_reused_until_past_its_age_is_retired_at_release_not_returned_to_idle() {
    let mut pool = pool();
    let lease = open(&mut pool);
    let connection = lease.connection();
    pool.release(lease, Disposition::Reusable).unwrap();
    // Reused inside its age: back to idle.
    pool.advance(500);
    let lease = reuse(&mut pool);
    assert_eq!(lease.connection(), connection);
    pool.release(lease, Disposition::Reusable).unwrap();
    assert_eq!(pool.counts().idle, 1);
    // Leased at 900, still leased past 1000 (an active exchange is not cut off), released after its age.
    pool.advance(900);
    let lease = reuse(&mut pool);
    pool.advance(1_001);
    assert_eq!(pool.counts().leased, 1);
    pool.release(lease, Disposition::Reusable).unwrap();
    assert_eq!(pool.counts().idle, 0, "retired at release");
    assert_eq!(pool.counts().closing, 1);
    assert_eq!(
        pool.next_action(),
        Some(Action::Close {
            connection,
            reason: CloseReason::AgeExpired,
            deadline: 1_001 + Config::default().cleanup_timeout_ms,
        })
    );
    // The next opening gets a connection of its own.
    pool.closed(connection).unwrap();
    let lease = open(&mut pool);
    assert_ne!(lease.connection(), connection);
}

#[test]
fn an_idle_connection_that_reaches_its_age_is_retired_and_never_leased() {
    let mut pool = pool();
    let lease = open(&mut pool);
    let connection = lease.connection();
    pool.release(lease, Disposition::Reusable).unwrap();
    // Idle at 900 (within the idle expiry): at 1000 its age is up, so a request opens a new connection instead.
    pool.advance(999);
    assert_eq!(pool.counts().idle, 1);
    pool.advance(1_000);
    assert_eq!(pool.counts().idle, 0);
    let id = pool.request(request()).unwrap();
    assert!(matches!(
        pool.next_action(),
        Some(Action::Close {
            reason: CloseReason::AgeExpired,
            ..
        })
    ));
    let Some(Action::Connect {
        connection: fresh, ..
    }) = pool.next_action()
    else {
        panic!("a new connection is opened for the request");
    };
    assert_ne!(fresh, connection);
    let _ = id;
}

#[test]
fn the_age_is_a_wakeup_the_pool_reports() {
    let mut pool = pool();
    let lease = open(&mut pool);
    pool.release(lease, Disposition::Reusable).unwrap();
    // Idle expiry is 60 s from release; the age, 1 s from the connection's birth, comes first.
    assert_eq!(pool.next_deadline(), Some(AGE));
}

#[test]
fn an_unreusable_connection_is_not_blamed_on_age() {
    let mut pool = pool();
    let lease = open(&mut pool);
    let connection = lease.connection();
    pool.advance(2_000);
    pool.release(lease, Disposition::Discarded).unwrap();
    assert!(matches!(
        pool.next_action(),
        Some(Action::Close { connection: c, reason: CloseReason::Discarded, .. }) if c == connection
    ));
}

#[test]
fn expire_idle_retires_every_idle_connection_and_leaves_leased_ones() {
    let mut pool = PoolMachine::new(Config::default()).unwrap();
    let held = open(&mut pool);
    let idle: Vec<_> = (0..2)
        .map(|_| {
            let lease = open(&mut pool);
            let connection = lease.connection();
            pool.release(lease, Disposition::Reusable).unwrap();
            connection
        })
        .collect();
    assert_eq!(pool.expire_idle(), 2);
    assert_eq!(pool.counts().leased, 1);
    assert_eq!(pool.counts().closing, 2);
    assert!(pool.is_live(held));
    let mut closed = Vec::new();
    while let Some(Action::Close {
        connection, reason, ..
    }) = pool.next_action()
    {
        assert_eq!(reason, CloseReason::IdleExpired);
        closed.push(connection);
    }
    closed.sort();
    assert_eq!(closed, idle);
}
