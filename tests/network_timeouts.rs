use gwz_transport::{
    pool::{
        Action, Config as PoolConfig, Error as PoolError, Identity, Key, Owner, PoolMachine,
        Request,
    },
    protocol::{Effect, ErrorCode, Failure},
    stream::{Config as StreamConfig, Error as StreamError, IoState, Side, StreamMachine},
};

fn request() -> Request {
    Request::new(
        Key::ssh("git", "timeout-host", 22),
        Identity::Ambient,
        Owner::new("session", "operation"),
    )
}

fn pool_config(connect_timeout_ms: u64) -> PoolConfig {
    PoolConfig {
        per_user_host: 1,
        per_host: 1,
        total: 1,
        connect_timeout_ms,
        ..PoolConfig::default()
    }
}

#[test]
fn absolute_pool_deadline_rejects_before_allocation_and_caps_connect() {
    let mut pool = PoolMachine::new(pool_config(100)).unwrap();
    assert_eq!(
        pool.request_until(request(), Some(0)),
        Err(PoolError::AllocationTimeout)
    );
    let id = pool.request_until(request(), Some(25)).unwrap();
    let Some(Action::Connect {
        network_deadline, ..
    }) = pool.next_action()
    else {
        panic!("expected connect");
    };
    assert_eq!(network_deadline, Some(25));
    pool.advance(25);
    assert_eq!(pool.take(id), Err(PoolError::ConnectTimeout));
}

#[test]
fn absolute_pool_deadline_survives_interaction_pause_and_resume() {
    let mut pool = PoolMachine::new(pool_config(100)).unwrap();
    let id = pool.request_until(request(), Some(25)).unwrap();
    let Some(Action::Connect {
        connection,
        network_deadline,
        ..
    }) = pool.next_action()
    else {
        panic!("expected connect");
    };
    assert_eq!(network_deadline, Some(25));
    pool.begin_interaction(connection).unwrap();
    pool.advance(5);
    pool.end_interaction(connection).unwrap();
    assert_eq!(pool.next_deadline(), Some(25));
    pool.advance(25);
    assert_eq!(pool.take(id), Err(PoolError::ConnectTimeout));
}

#[test]
fn absolute_pool_deadline_expires_during_interaction_without_resume() {
    let mut pool = PoolMachine::new(pool_config(100)).unwrap();
    let id = pool.request_until(request(), Some(25)).unwrap();
    let Some(Action::Connect { connection, .. }) = pool.next_action() else {
        panic!("expected connect");
    };
    pool.begin_interaction(connection).unwrap();
    pool.advance(24);
    assert_eq!(pool.next_deadline(), Some(25));
    assert_eq!(pool.take(id), Err(PoolError::WouldBlock));
    pool.advance(25);
    assert_eq!(pool.take(id), Err(PoolError::InteractionTimeout));
    assert!(matches!(
        pool.next_action(),
        Some(Action::CancelConnect { connection: id2, .. }) if id2 == connection
    ));
}

#[test]
fn absolute_pool_deadline_expires_ready_lease_before_take() {
    let mut pool = PoolMachine::new(pool_config(100)).unwrap();
    let id = pool.request_until(request(), Some(25)).unwrap();
    let Some(Action::Connect { connection, .. }) = pool.next_action() else {
        panic!("expected connect");
    };
    pool.connected(connection, Ok(None)).unwrap();
    pool.advance(25);
    assert_eq!(pool.take(id), Err(PoolError::AllocationTimeout));
    assert!(matches!(
        pool.next_action(),
        Some(Action::Close { connection: id2, .. }) if id2 == connection
    ));
}

#[test]
fn disabled_stream_network_has_no_deadline_or_expiry() {
    let mut config = StreamConfig::new("disabled-stream", 1, Side::Endpoint);
    config.io_timeout_ms = 0;
    let mut stream = StreamMachine::new(config).unwrap();
    stream.advance(9_000_000_000);
    stream.set_io_state(IoState::Network).unwrap();
    assert_eq!(stream.io_status().remaining_network_ms, 0);
    assert_eq!(stream.io_status().active_deadline, None);
    assert_eq!(stream.next_deadline(), None);
    stream.advance(u64::MAX);
    assert_eq!(stream.io_status().active_deadline, None);
    stream.record_io_progress(1).unwrap();
    assert_eq!(stream.io_status().active_deadline, None);
    assert_eq!(stream.read(&mut [0]), Err(StreamError::WouldBlock));
}

#[test]
fn stream_accepts_native_maximum_and_rejects_above_it() {
    let mut config = StreamConfig::new("native-max", 1, Side::Endpoint);
    config.io_timeout_ms = i32::MAX as u64;
    let mut stream = StreamMachine::new(config).unwrap();
    stream.set_io_state(IoState::Network).unwrap();
    assert_eq!(stream.io_status().active_deadline, Some(i32::MAX as u64));
    stream.advance(i32::MAX as u64 - 1);
    assert_eq!(stream.io_status().remaining_network_ms, 1);
    stream.advance(i32::MAX as u64);
    assert_eq!(stream.read(&mut [0]), Err(StreamError::Timeout));

    let mut invalid = StreamConfig::new("native-too-large", 1, Side::Endpoint);
    invalid.io_timeout_ms = i32::MAX as u64 + 1;
    assert!(matches!(
        StreamMachine::new(invalid),
        Err(StreamError::InvalidConfig)
    ));

    let max_pool = PoolMachine::new(pool_config(i32::MAX as u64)).unwrap();
    drop(max_pool);
    assert!(matches!(
        PoolMachine::new(pool_config(i32::MAX as u64 + 1)),
        Err(PoolError::InvalidConfig)
    ));

    let mut pool = PoolMachine::new(pool_config(i32::MAX as u64)).unwrap();
    let request = pool.request(request()).unwrap();
    let Some(Action::Connect {
        connection,
        network_deadline,
        ..
    }) = pool.next_action()
    else {
        panic!("expected maximum connect");
    };
    assert_eq!(network_deadline, Some(i32::MAX as u64));
    pool.advance(i32::MAX as u64 - 1);
    assert_eq!(pool.take(request), Err(PoolError::WouldBlock));
    pool.advance(i32::MAX as u64);
    assert_eq!(pool.take(request), Err(PoolError::ConnectTimeout));
    let Some(Action::CancelConnect { deadline, .. }) = pool.next_action() else {
        panic!("expected bounded maximum cleanup");
    };
    pool.advance(deadline);
    assert!(matches!(
        pool.next_action(),
        Some(Action::AbortConnect { connection: id }) if id == connection
    ));
    pool.connected(
        connection,
        Err(Failure {
            code: ErrorCode::Io,
            effect: Effect::Possible,
            facts: None,
        }),
    )
    .unwrap();
    assert_eq!(pool.counts().total(), 0);
}

#[test]
fn disabled_pool_connect_survives_ticks_and_cleanup_stays_bounded() {
    let mut pool = PoolMachine::new(pool_config(0)).unwrap();
    let request = pool.request(request()).unwrap();
    let Some(Action::Connect {
        connection,
        network_deadline,
        ..
    }) = pool.next_action()
    else {
        panic!("expected disabled connect");
    };
    assert_eq!(network_deadline, None);
    pool.advance(9_000_000_000);
    assert_eq!(pool.take(request), Err(PoolError::WouldBlock));
    assert_eq!(pool.next_deadline(), None);

    pool.cancel(request).unwrap();
    let Some(Action::CancelConnect { deadline, .. }) = pool.next_action() else {
        panic!("expected bounded cancellation cleanup");
    };
    assert_eq!(deadline, 9_000_000_000 + pool_config(0).cleanup_timeout_ms);
    pool.advance(deadline);
    assert!(matches!(
        pool.next_action(),
        Some(Action::AbortConnect { connection: id }) if id == connection
    ));
    pool.connected(
        connection,
        Err(Failure {
            code: ErrorCode::Io,
            effect: Effect::Possible,
            facts: None,
        }),
    )
    .unwrap();
    assert_eq!(pool.counts().total(), 0);
}

#[test]
fn positive_request_can_tighten_disabled_policy_but_zero_cannot_disable_finite() {
    let mut disabled = PoolMachine::new(pool_config(0)).unwrap();
    let mut bounded = request();
    bounded.connect_timeout_ms = Some(7);
    let bounded_id = disabled.request(bounded).unwrap();
    let Some(Action::Connect {
        network_deadline, ..
    }) = disabled.next_action()
    else {
        panic!("expected bounded request");
    };
    assert_eq!(network_deadline, Some(7));
    disabled.cancel(bounded_id).unwrap();

    let mut disabled_zero = PoolMachine::new(pool_config(0)).unwrap();
    let mut explicit_zero = request();
    explicit_zero.connect_timeout_ms = Some(0);
    assert!(disabled_zero.request(explicit_zero).is_ok());

    let mut finite = PoolMachine::new(pool_config(10)).unwrap();
    let mut disable = request();
    disable.connect_timeout_ms = Some(0);
    assert_eq!(finite.request(disable), Err(PoolError::InvalidRequest));
}

#[test]
fn helper_wait_expires_when_network_connect_timeout_is_disabled() {
    let mut pool = PoolMachine::new(pool_config(0)).unwrap();
    let request = pool.request(request()).unwrap();
    let Some(Action::Connect {
        connection,
        network_deadline,
        ..
    }) = pool.next_action()
    else {
        panic!("expected disabled connect");
    };
    assert_eq!(network_deadline, None);
    pool.begin_interaction(connection).unwrap();
    assert_eq!(pool.next_deadline(), Some(120_000));
    pool.advance(120_000);
    assert_eq!(pool.take(request), Err(PoolError::InteractionTimeout));
    let Some(Action::CancelConnect { deadline, .. }) = pool.next_action() else {
        panic!("expected bounded helper cleanup");
    };
    pool.advance(deadline);
    assert!(matches!(
        pool.next_action(),
        Some(Action::AbortConnect { connection: id }) if id == connection
    ));
    pool.connected(
        connection,
        Err(Failure {
            code: ErrorCode::Io,
            effect: Effect::Possible,
            facts: None,
        }),
    )
    .unwrap();
    assert_eq!(pool.counts().total(), 0);
}

#[test]
fn ending_helper_wait_resumes_disabled_network_without_arming_deadline() {
    let mut pool = PoolMachine::new(pool_config(0)).unwrap();
    let request = pool.request(request()).unwrap();
    let Some(Action::Connect { connection, .. }) = pool.next_action() else {
        panic!("expected disabled connect");
    };
    pool.begin_interaction(connection).unwrap();
    pool.advance(5);
    pool.end_interaction(connection).unwrap();
    assert_eq!(pool.next_deadline(), None);
    assert_eq!(pool.take(request), Err(PoolError::WouldBlock));

    let mut config = StreamConfig::new("helper-resume", 1, Side::Endpoint);
    config.io_timeout_ms = 0;
    config.interaction_budget_ms = 20;
    let mut stream = StreamMachine::new(config).unwrap();
    stream.set_io_state(IoState::Interaction).unwrap();
    stream.advance(5);
    stream.set_io_state(IoState::Network).unwrap();
    assert_eq!(stream.io_status().remaining_network_ms, 0);
    assert_eq!(stream.io_status().active_deadline, None);
    stream.advance(5_000_000);
    assert!(!stream.stats().terminal);
}

#[test]
fn shutdown_disposes_disabled_connect_with_bounded_cleanup() {
    let mut pool = PoolMachine::new(pool_config(0)).unwrap();
    let request = pool.request(request()).unwrap();
    let Some(Action::Connect {
        connection,
        network_deadline,
        ..
    }) = pool.next_action()
    else {
        panic!("expected disabled connect");
    };
    assert_eq!(network_deadline, None);
    pool.shutdown();
    assert_eq!(pool.take(request), Err(PoolError::Shutdown));
    let Some(Action::CancelConnect { deadline, .. }) = pool.next_action() else {
        panic!("expected shutdown cleanup");
    };
    pool.advance(deadline);
    assert!(matches!(
        pool.next_action(),
        Some(Action::AbortConnect { connection: id }) if id == connection
    ));
    pool.connected(
        connection,
        Err(Failure {
            code: ErrorCode::Io,
            effect: Effect::Possible,
            facts: None,
        }),
    )
    .unwrap();
    assert_eq!(pool.counts().total(), 0);
    assert!(pool.shutdown_complete());
}
