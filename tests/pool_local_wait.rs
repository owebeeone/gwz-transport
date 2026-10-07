//! A connecting resource that waits on a local budget (a job place, a shared
//! reservation, a setup slot) pauses the pool's connect clock; only the
//! request's allocation deadline bounds the wait, and a wait that expires
//! fails the request with a local error, never a connect timeout. The pool is
//! told a state, not a code.
use gwz_transport::pool::{
    Action, Config, ConnectionId, Error, Identity, Key, Observation, Owner, PoolMachine, Request,
    RequestId, SetupCause,
};
use gwz_transport::protocol::Disposition;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

const CONNECT: u64 = 30_000;
const ALLOCATION: u64 = 10_000;

fn opened() -> (PoolMachine, RequestId, ConnectionId) {
    let mut pool = PoolMachine::new(Config {
        connect_timeout_ms: CONNECT,
        ..Config::default()
    })
    .unwrap();
    pool.advance(0);
    let mut request = Request::new(
        Key::https("host", 443),
        Identity::Https,
        Owner::new("session", "operation"),
    );
    request.allocation_timeout_ms = Some(ALLOCATION);
    let id = pool.request(request).unwrap();
    let Some(Action::Connect {
        connection,
        network_deadline,
        ..
    }) = pool.next_action()
    else {
        panic!("a connect action");
    };
    assert_eq!(network_deadline, Some(CONNECT));
    (pool, id, connection)
}

#[test]
fn a_local_wait_is_not_charged_to_the_connect_clock_and_ends_with_the_allocation() {
    let (mut pool, id, connection) = opened();
    pool.begin_local_wait(connection).unwrap();
    // Past the connect clock's deadline, inside the allocation: no timeout.
    pool.advance(ALLOCATION - 1);
    assert_eq!(pool.next_deadline(), Some(ALLOCATION));
    assert!(matches!(pool.take(id), Err(Error::WouldBlock)));
    // The allocation ends the wait, as a local failure.
    pool.advance(ALLOCATION);
    assert_eq!(pool.take(id), Err(Error::LocalWaitExpired));
}

#[test]
fn the_connect_clock_resumes_with_the_time_it_had_when_the_wait_ended() {
    let (mut pool, id, connection) = opened();
    pool.advance(1_000);
    pool.begin_local_wait(connection).unwrap();
    pool.advance(6_000);
    pool.end_local_wait(connection).unwrap();
    // 29 000 ms of network time were left at the pause.
    assert_eq!(pool.next_deadline(), Some(6_000 + 29_000));
    pool.advance(6_000 + 29_000 - 1);
    assert!(matches!(pool.take(id), Err(Error::WouldBlock)));
    pool.advance(6_000 + 29_000);
    assert_eq!(pool.take(id), Err(Error::ConnectTimeout));
}

#[test]
fn without_a_local_wait_the_connect_clock_expires_as_a_connect_timeout() {
    let (mut pool, id, _) = opened();
    pool.advance(CONNECT);
    assert_eq!(pool.take(id), Err(Error::ConnectTimeout));
}

#[test]
fn a_wait_cannot_begin_twice_or_end_unbegun() {
    let (mut pool, _, connection) = opened();
    assert_eq!(pool.end_local_wait(connection), Err(Error::WrongState));
    pool.begin_local_wait(connection).unwrap();
    pool.begin_local_wait(connection).unwrap(); // idempotent: the resource may repeat its report
    pool.end_local_wait(connection).unwrap();
    assert_eq!(pool.end_local_wait(connection), Err(Error::WrongState));
}

#[test]
fn a_shared_setup_clock_is_paused_the_same_way_and_ends_as_a_local_wait_cause() {
    let (mut pool, id, connection) = opened();
    let now = Arc::new(AtomicU64::new(0));
    let source = now.clone();
    let clock = pool
        .install_setup_clock(
            connection,
            Arc::new(move || source.load(Ordering::SeqCst)),
            5_000,
        )
        .unwrap();
    pool.begin_local_wait(connection).unwrap();
    // Neither the aggregate (30 000) nor the stall allowance (5 000) runs.
    now.store(ALLOCATION - 1, Ordering::SeqCst);
    pool.advance(ALLOCATION - 1);
    assert!(matches!(
        clock.observe().deliver(),
        Observation::Alive { .. }
    ));
    assert!(matches!(pool.take(id), Err(Error::WouldBlock)));
    now.store(ALLOCATION, Ordering::SeqCst);
    pool.advance(ALLOCATION);
    let Err(Error::SetupEnded(record)) = pool.take(id) else {
        panic!("the wait's end is the setup's end");
    };
    assert_eq!(record.cause, SetupCause::LocalWaitExpired);
}

#[test]
fn a_shared_setup_clock_resumes_its_aggregate_when_the_wait_ends() {
    let (mut pool, id, connection) = opened();
    let now = Arc::new(AtomicU64::new(0));
    let source = now.clone();
    let clock = pool
        .install_setup_clock(
            connection,
            Arc::new(move || source.load(Ordering::SeqCst)),
            0,
        )
        .unwrap();
    now.store(1_000, Ordering::SeqCst);
    pool.advance(1_000);
    pool.begin_local_wait(connection).unwrap();
    now.store(6_000, Ordering::SeqCst);
    pool.advance(6_000);
    pool.end_local_wait(connection).unwrap();
    assert!(matches!(
        clock.observe().deliver(),
        Observation::Alive {
            deadline: Some(35_000),
            local: None,
            ..
        }
    ));
    now.store(35_000, Ordering::SeqCst);
    pool.advance(35_000);
    let Err(Error::SetupEnded(record)) = pool.take(id) else {
        panic!("the aggregate ends the setup");
    };
    assert_eq!(record.cause, SetupCause::NetworkAggregate);
}

#[test]
fn a_fresh_request_beside_an_idle_connection_pauses_its_new_connections_clock_the_same_way() {
    // The retry after a reused connection was found dead (`Request::fresh`):
    // its new connection waits on a local budget while a compatible idle one
    // stays where it is.
    let mut pool = PoolMachine::new(Config {
        connect_timeout_ms: CONNECT,
        ..Config::default()
    })
    .unwrap();
    pool.advance(0);
    let request = |fresh: bool| {
        let mut request = Request::new(
            Key::https("host", 443),
            Identity::Https,
            Owner::new("session", "operation"),
        );
        request.allocation_timeout_ms = Some(ALLOCATION);
        request.fresh = fresh;
        request
    };
    let first = pool.request(request(false)).unwrap();
    let Some(Action::Connect {
        connection: old, ..
    }) = pool.next_action()
    else {
        panic!("a connect action");
    };
    pool.connected(old, Ok(Some(Identity::Https))).unwrap();
    let lease = pool.take(first).unwrap();
    pool.release(lease, Disposition::Reusable).unwrap();
    let id = pool.request(request(true)).unwrap();
    let Some(Action::Connect {
        connection: new, ..
    }) = pool.next_action()
    else {
        panic!("a fresh request opens a new connection");
    };
    assert_ne!(new, old);
    pool.begin_local_wait(new).unwrap();
    pool.advance(ALLOCATION - 1);
    assert!(matches!(pool.take(id), Err(Error::WouldBlock)));
    assert_eq!(pool.counts().idle, 1, "the idle connection was not taken");
    pool.advance(ALLOCATION);
    assert_eq!(pool.take(id), Err(Error::LocalWaitExpired));
}
