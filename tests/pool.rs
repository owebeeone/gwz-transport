use gwz_transport::{
    pool::*,
    protocol::{Disposition, Scheme},
};

fn config() -> Config {
    Config {
        per_user_host: 1,
        per_host: 2,
        total: 4,
        ..Config::default()
    }
}
fn request(user: &str, host: &str) -> Request {
    Request::new(
        Key::ssh(user, host, 22),
        Identity::Ambient,
        Owner::new("session", "owner"),
    )
}
fn connect(pool: &mut PoolMachine, identity: Identity) -> ConnectionId {
    let Some(Action::Connect { connection, .. }) = pool.next_action() else {
        panic!("expected connect");
    };
    pool.connected(connection, Ok(Some(identity))).unwrap();
    connection
}
fn close(pool: &mut PoolMachine) -> ConnectionId {
    let Some(Action::Close { connection, .. }) = pool.next_action() else {
        panic!("expected close");
    };
    pool.closed(connection).unwrap();
    connection
}

#[test]
fn healthy_release_reuses_one_connection_with_a_fresh_exclusive_lease() {
    let mut pool = PoolMachine::new(config()).unwrap();
    let first = pool.request(request("git", "host")).unwrap();
    let connection = connect(&mut pool, Identity::Ambient);
    let lease = pool.take(first).unwrap();
    let second = pool.request(request("git", "host")).unwrap();
    assert_eq!(pool.take(second), Err(Error::WouldBlock));
    pool.release(lease, Disposition::Reusable).unwrap();
    let next = pool.take(second).unwrap();
    assert_eq!(next.connection(), connection);
    assert_ne!(next, lease);
    assert_eq!(
        pool.release(lease, Disposition::Reusable),
        Err(Error::Stale)
    );
    assert!(pool.next_action().is_none());
    assert!(pool.is_live(next));
}

#[test]
fn opening_and_closing_count_against_key_and_host_limits() {
    let mut pool = PoolMachine::new(config()).unwrap();
    let a = pool.request(request("a", "host")).unwrap();
    let a_connection = connect(&mut pool, Identity::Ambient);
    let a = pool.take(a).unwrap();
    let b = pool.request(request("b", "host")).unwrap();
    let Some(Action::Connect {
        connection: b_connection,
        ..
    }) = pool.next_action()
    else {
        panic!("expected reservation");
    };
    let c = pool
        .request(Request::new(
            Key::https("host", 8443),
            Identity::Https,
            Owner::new("session", "owner"),
        ))
        .unwrap();
    assert_eq!(pool.take(c), Err(Error::WouldBlock));
    pool.release(a, Disposition::Discarded).unwrap();
    let Some(Action::Close { connection, .. }) = pool.next_action() else {
        panic!("expected close");
    };
    assert_eq!(connection, a_connection);
    assert_eq!(pool.counts_for_host("host").total(), 2);
    assert!(pool.next_action().is_none());
    pool.closed(connection).unwrap();
    assert!(
        matches!(pool.next_action(), Some(Action::Connect { key, .. }) if key.scheme == Scheme::Https)
    );
    pool.connected(b_connection, Ok(Some(Identity::Ambient)))
        .unwrap();
    assert!(pool.take(b).is_ok());
    assert_eq!(pool.counts_for_host("host").total(), 2);
}

#[test]
fn incompatible_idle_identity_is_retired_without_creating_repository_partitions() {
    let mut pool = PoolMachine::new(config()).unwrap();
    let a = pool.request(request("git", "host")).unwrap();
    let old = connect(&mut pool, Identity::Ambient);
    let lease = pool.take(a).unwrap();
    pool.release(lease, Disposition::Reusable).unwrap();
    let mut explicit = request("git", "host");
    explicit.identity = Identity::Explicit("current-key-proof".into());
    let b = pool.request(explicit).unwrap();
    assert_eq!(pool.take(b), Err(Error::WouldBlock));
    assert_eq!(close(&mut pool), old);
    let new = connect(&mut pool, Identity::Explicit("current-key-proof".into()));
    assert_ne!(old, new);
    assert_eq!(pool.take(b).unwrap().connection(), new);
}

#[test]
fn idle_expiry_runs_from_release_and_never_reclaims_a_quiet_lease() {
    let mut pool = PoolMachine::new(config()).unwrap();
    let a = pool.request(request("git", "host")).unwrap();
    let connection = connect(&mut pool, Identity::Ambient);
    let lease = pool.take(a).unwrap();
    pool.advance(100_000);
    assert!(pool.is_live(lease));
    assert!(pool.next_action().is_none());
    pool.release(lease, Disposition::Reusable).unwrap();
    assert_eq!(pool.next_deadline(), Some(160_000));
    pool.advance(159_999);
    let b = pool.request(request("git", "host")).unwrap();
    let lease = pool.take(b).unwrap();
    pool.advance(160_000);
    assert!(pool.is_live(lease));
    pool.release(lease, Disposition::Reusable).unwrap();
    pool.advance(219_999);
    assert!(pool.next_action().is_none());
    pool.advance(220_000);
    assert_eq!(close(&mut pool), connection);
    assert_eq!(pool.counts().total(), 0);
}

#[test]
fn cancelling_an_open_keeps_its_reservation_until_late_success_is_closed() {
    let mut pool = PoolMachine::new(config()).unwrap();
    let a = pool.request(request("git", "host")).unwrap();
    let Some(Action::Connect { connection, .. }) = pool.next_action() else {
        panic!("expected connect");
    };
    pool.cancel(a).unwrap();
    assert_eq!(pool.take(a), Err(Error::Cancelled));
    assert!(matches!(
        pool.next_action(),
        Some(Action::CancelConnect { .. })
    ));
    let b = pool.request(request("git", "host")).unwrap();
    assert!(pool.next_action().is_none());
    pool.connected(connection, Ok(Some(Identity::Ambient)))
        .unwrap();
    assert_eq!(pool.take(b), Err(Error::WouldBlock));
    assert_eq!(close(&mut pool), connection);
    assert!(matches!(pool.next_action(), Some(Action::Connect { .. })));
}

#[test]
fn shutdown_holds_capacity_until_abort_is_acknowledged() {
    let mut pool = PoolMachine::new(config()).unwrap();
    let a = pool.request(request("git", "host")).unwrap();
    let connection = connect(&mut pool, Identity::Ambient);
    let lease = pool.take(a).unwrap();
    let waiting = pool.request(request("git", "host")).unwrap();
    pool.shutdown();
    assert_eq!(pool.take(waiting), Err(Error::Shutdown));
    assert!(!pool.is_live(lease));
    assert_eq!(pool.request(request("b", "host")), Err(Error::Shutdown));
    assert!(matches!(pool.next_action(), Some(Action::Close { .. })));
    pool.advance(5000);
    assert!(
        matches!(pool.next_action(), Some(Action::Abort { connection: id }) if id == connection)
    );
    assert!(!pool.shutdown_complete());
    assert_eq!(pool.counts().total(), 1);
    pool.closed(connection).unwrap();
    assert!(pool.shutdown_complete());
}

#[test]
fn unsent_connect_cancellation_and_timeout_free_capacity_without_host_work() {
    let mut pool = PoolMachine::new(config()).unwrap();
    let a = pool.request(request("git", "host")).unwrap();
    pool.cancel(a).unwrap();
    assert_eq!(pool.counts().total(), 0);
    assert!(pool.next_action().is_none());
    assert_eq!(pool.take(a), Err(Error::Cancelled));
    let b = pool.request(request("git", "host")).unwrap();
    pool.advance(30_000);
    assert_eq!(pool.take(b), Err(Error::AllocationTimeout));
    assert_eq!(pool.counts().total(), 0);
    assert!(pool.next_action().is_none());
}

#[test]
fn queue_network_interaction_and_cleanup_use_independent_budgets() {
    let mut pool = PoolMachine::new(config()).unwrap();
    let a = pool.request(request("git", "host")).unwrap();
    pool.advance(29_000);
    let Some(Action::Connect {
        connection,
        network_deadline,
        ..
    }) = pool.next_action()
    else {
        panic!("connect");
    };
    assert_eq!(network_deadline, 39_000);
    pool.advance(30_000);
    pool.begin_interaction(connection).unwrap();
    assert_eq!(pool.next_deadline(), Some(150_000));
    pool.advance(130_000);
    assert_eq!(pool.take(a), Err(Error::WouldBlock));
    pool.end_interaction(connection).unwrap();
    assert_eq!(pool.next_deadline(), Some(139_000));
    pool.advance(131_000);
    pool.begin_interaction(connection).unwrap();
    assert_eq!(pool.next_deadline(), Some(151_000)); // Remaining 20s, not a new 120s.
    pool.advance(151_000);
    assert_eq!(pool.take(a), Err(Error::InteractionTimeout));
    assert!(matches!(
        pool.next_action(),
        Some(Action::CancelConnect {
            deadline: 156_000,
            ..
        })
    ));
    pool.advance(156_000);
    assert!(matches!(
        pool.next_action(),
        Some(Action::AbortConnect { .. })
    ));
    pool.connected(connection, Ok(Some(Identity::Ambient)))
        .unwrap();
    // Late success cannot restart cleanup's deadline or become an allocation.
    assert!(matches!(pool.next_action(), Some(Action::Abort { .. })));
    pool.closed(connection).unwrap();
    assert_eq!(pool.counts().total(), 0);
}

#[test]
fn connect_timeout_and_reported_failure_do_not_retry_the_request() {
    use gwz_transport::protocol::{Effect, ErrorCode, Failure};
    let mut pool = PoolMachine::new(config()).unwrap();
    let a = pool.request(request("git", "host")).unwrap();
    let Some(Action::Connect { connection, .. }) = pool.next_action() else {
        panic!("connect");
    };
    pool.advance(10_000);
    assert_eq!(pool.take(a), Err(Error::ConnectTimeout));
    pool.connected(
        connection,
        Err(Failure {
            code: ErrorCode::Io,
            effect: Effect::None,
        }),
    )
    .unwrap();
    assert!(pool.next_action().is_none());
    let b = pool.request(request("git", "host")).unwrap();
    let Some(Action::Connect { connection, .. }) = pool.next_action() else {
        panic!("connect");
    };
    pool.connected(
        connection,
        Err(Failure {
            code: ErrorCode::Io,
            effect: Effect::Possible,
        }),
    )
    .unwrap();
    assert_eq!(
        pool.take(b),
        Err(Error::ConnectFailed {
            code: ErrorCode::Io,
            effect: Effect::Possible
        })
    );
    assert!(pool.next_action().is_none());
}

#[test]
fn incompatible_head_does_not_block_eligible_fifo_reuse() {
    let mut pool = PoolMachine::new(config()).unwrap();
    let a = pool.request(request("git", "host")).unwrap();
    let connection = connect(&mut pool, Identity::Ambient);
    let lease = pool.take(a).unwrap();
    let mut explicit = request("git", "host");
    explicit.identity = Identity::Explicit("key".into());
    let incompatible = pool.request(explicit).unwrap();
    let first = pool.request(request("git", "host")).unwrap();
    let second = pool.request(request("git", "host")).unwrap();
    pool.release(lease, Disposition::Reusable).unwrap();
    let lease = pool.take(first).unwrap();
    assert_eq!(lease.connection(), connection);
    assert_eq!(pool.take(incompatible), Err(Error::WouldBlock));
    assert_eq!(pool.take(second), Err(Error::WouldBlock));
    pool.release(lease, Disposition::Reusable).unwrap();
    let lease = pool.take(second).unwrap();
    assert_eq!(lease.connection(), connection);
    pool.release(lease, Disposition::Reusable).unwrap();
    assert_eq!(close(&mut pool), connection);
    assert!(matches!(
        pool.next_action(),
        Some(Action::Connect {
            identity: Identity::Explicit(_),
            ..
        })
    ));
}

#[test]
fn unproven_connections_are_single_use_and_wrong_proofs_fail_closed() {
    let mut pool = PoolMachine::new(config()).unwrap();
    let a = pool.request(request("git", "host")).unwrap();
    let Some(Action::Connect { connection, .. }) = pool.next_action() else {
        panic!("connect");
    };
    pool.connected(connection, Ok(None)).unwrap();
    let lease = pool.take(a).unwrap();
    pool.release(lease, Disposition::Reusable).unwrap();
    assert!(matches!(
        pool.next_action(),
        Some(Action::Close {
            reason: CloseReason::Unproven,
            ..
        })
    ));
    pool.closed(connection).unwrap();
    let b = pool.request(request("git", "host")).unwrap();
    let Some(Action::Connect { connection, .. }) = pool.next_action() else {
        panic!("connect");
    };
    pool.connected(connection, Ok(Some(Identity::Explicit("other-key".into()))))
        .unwrap();
    assert_eq!(pool.take(b), Err(Error::IdentityMismatch));
    assert_eq!(close(&mut pool), connection);
    assert!(pool.next_action().is_none());
}

#[test]
fn owner_loss_preserves_idle_and_other_owners_but_closes_its_active_leases() {
    let mut pool = PoolMachine::new(config()).unwrap();
    let a = pool.request(request("a", "host")).unwrap();
    connect(&mut pool, Identity::Ambient);
    let a = pool.take(a).unwrap();
    pool.release(a, Disposition::Reusable).unwrap();
    let b = pool.request(request("b", "elsewhere")).unwrap();
    connect(&mut pool, Identity::Ambient);
    let b = pool.take(b).unwrap();
    let mut other = request("c", "elsewhere");
    other.owner = Owner::new("session", "other");
    let c = pool.request(other).unwrap();
    connect(&mut pool, Identity::Ambient);
    let c = pool.take(c).unwrap();
    let waiting = pool.request(request("b", "elsewhere")).unwrap();
    pool.cancel_operation(&Owner::new("session", "owner"));
    assert_eq!(pool.take(waiting), Err(Error::Cancelled));
    assert!(!pool.is_live(b));
    assert!(pool.is_live(c));
    assert_eq!(pool.counts().idle, 1);
    assert_eq!(pool.counts().closing, 1);
}

#[test]
fn result_slots_are_bounded_and_foreign_or_duplicate_tokens_cannot_change_state() {
    let mut one = PoolMachine::new(Config {
        max_requests: 1,
        ..config()
    })
    .unwrap();
    let mut two = PoolMachine::new(config()).unwrap();
    let a = one.request(request("git", "host")).unwrap();
    let connection = connect(&mut one, Identity::Ambient);
    assert_eq!(one.request(request("b", "host")), Err(Error::Capacity));
    let lease = one.take(a).unwrap();
    let b = two.request(request("git", "host")).unwrap();
    let other_connection = connect(&mut two, Identity::Ambient);
    let other = two.take(b).unwrap();
    assert_ne!(connection, other_connection);
    assert_eq!(one.release(other, Disposition::Reusable), Err(Error::Stale));
    assert_eq!(one.closed(other_connection), Err(Error::Stale));
    assert_eq!(one.closed(connection), Err(Error::WrongState));
    assert_eq!(
        one.connected(connection, Ok(Some(Identity::Ambient))),
        Err(Error::WrongState)
    );
    one.release(lease, Disposition::Discarded).unwrap();
    close(&mut one);
    assert_eq!(one.closed(connection), Err(Error::Stale));
    let a = one.request(request("git", "host")).unwrap();
    one.cancel(a).unwrap();
    assert_eq!(one.request(request("b", "host")), Err(Error::Capacity));
    one.abandon(a);
    assert!(one.request(request("b", "host")).is_ok());
}

#[test]
fn request_settings_cannot_resize_endpoint_limits_or_raise_timeouts() {
    assert!(matches!(
        PoolMachine::new(Config {
            total: 0,
            ..config()
        }),
        Err(Error::InvalidConfig)
    ));
    let mut pool = PoolMachine::new(config()).unwrap();
    let mut a = request("git", "host");
    a.connect_timeout_ms = Some(10_001);
    assert_eq!(pool.request(a), Err(Error::InvalidRequest));
    let mut a = request("git", "host");
    a.identity = Identity::Https;
    assert_eq!(pool.request(a), Err(Error::InvalidRequest));
    let a = Request::new(
        Key::ssh("git", "host", 0),
        Identity::Ambient,
        Owner::new("session", "owner"),
    );
    assert_eq!(pool.request(a), Err(Error::InvalidRequest));
}
