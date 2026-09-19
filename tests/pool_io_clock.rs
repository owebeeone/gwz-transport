use gwz_transport::{
    pool::{Action, Config as PoolConfig, Identity, Key, Owner, PoolMachine, Request},
    protocol::Disposition,
    stream::{Config as StreamConfig, IoState, Side, StreamMachine},
};

fn request() -> Request {
    Request::new(
        Key::ssh("git", "io-host", 22),
        Identity::Ambient,
        Owner::new("session", "operation"),
    )
}

fn pool_config() -> PoolConfig {
    PoolConfig {
        per_user_host: 1,
        per_host: 1,
        total: 1,
        ..PoolConfig::default()
    }
}

#[test]
fn remaining_connect_helper_allowance_transfers_to_active_stream() {
    let config = pool_config();
    let helper_budget_ms = config.interaction_timeout_ms;
    let mut pool = PoolMachine::new(config).unwrap();
    pool.advance(0);
    let request_id = pool.request(request()).unwrap();
    let Some(Action::Connect {
        connection,
        network_deadline,
        ..
    }) = pool.next_action()
    else {
        panic!("expected connect action");
    };
    assert_eq!(network_deadline, Some(10_000));
    let helper_start_ms = 0;
    pool.begin_interaction(connection).unwrap();
    let helper_end_ms = 20_000;
    pool.advance(helper_end_ms);
    pool.end_interaction(connection).unwrap();
    let remaining_helper_ms = helper_budget_ms.saturating_sub(helper_end_ms - helper_start_ms);
    assert_eq!(remaining_helper_ms, 100_000);
    assert_eq!(pool.next_deadline(), Some(30_000));
    pool.connected(connection, Ok(Some(Identity::Ambient)))
        .unwrap();
    let lease = pool.take(request_id).unwrap();

    let mut config = StreamConfig::new("io-stream", 1, Side::Endpoint);
    config.interaction_budget_ms = remaining_helper_ms;
    let mut stream = StreamMachine::new(config).unwrap();
    stream.advance(helper_end_ms);
    stream.set_io_state(IoState::Interaction).unwrap();
    assert_eq!(stream.io_status().active_deadline, Some(120_000));
    stream.advance(120_000);
    assert!(stream.stats().terminal);
    pool.advance(120_000);
    pool.release(lease, Disposition::Discarded).unwrap();
    assert!(matches!(
        pool.next_action(),
        Some(Action::Close { connection: id, .. }) if id == connection
    ));
    pool.closed(connection).unwrap();
    assert_eq!(pool.counts().total(), 0);
}

#[test]
fn discarded_timeout_lease_keeps_capacity_until_physical_disposal() {
    let mut pool = PoolMachine::new(pool_config()).unwrap();
    pool.advance(0);
    let first = pool.request(request()).unwrap();
    let Some(Action::Connect { connection, .. }) = pool.next_action() else {
        panic!("expected connect action");
    };
    pool.connected(connection, Ok(Some(Identity::Ambient)))
        .unwrap();
    let lease = pool.take(first).unwrap();

    let mut stream_config = StreamConfig::new("discarded", 1, Side::Endpoint);
    stream_config.io_timeout_ms = 5;
    let mut stream = StreamMachine::new(stream_config).unwrap();
    stream.advance(0);
    stream.set_io_state(IoState::Network).unwrap();
    stream.advance(5);
    assert_eq!(
        stream.read(&mut [0]),
        Err(gwz_transport::stream::Error::Timeout)
    );

    pool.advance(5);
    pool.release(lease, Disposition::Discarded).unwrap();
    assert!(
        matches!(pool.next_action(), Some(Action::Close { connection: id, .. }) if id == connection)
    );
    assert_eq!(pool.counts().closing, 1);

    let waiting = pool.request(request()).unwrap();
    assert_eq!(
        pool.take(waiting),
        Err(gwz_transport::pool::Error::WouldBlock)
    );
    pool.closed(connection).unwrap();
    assert_eq!(pool.counts().opening, 1);
    let Some(Action::Connect {
        connection: replacement,
        ..
    }) = pool.next_action()
    else {
        panic!("expected replacement connect action");
    };
    pool.connected(replacement, Ok(Some(Identity::Ambient)))
        .unwrap();
    let replacement_lease = pool.take(waiting).unwrap();
    pool.release(replacement_lease, Disposition::Discarded)
        .unwrap();
    assert!(matches!(
        pool.next_action(),
        Some(Action::Close { connection: id, .. }) if id == replacement
    ));
    pool.closed(replacement).unwrap();
    assert_eq!(pool.counts().total(), 0);
}
