use gwz_transport::pool::{
    Action, Config, Identity, Key, LocalPhase, Observation, Owner, PoolMachine, PublicationError,
    Request, SetupCause,
};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

fn fixture(
    network: u64,
    stall: u64,
) -> (PoolMachine, Arc<AtomicU64>, gwz_transport::pool::SetupClock) {
    let mut pool = PoolMachine::new(Config {
        connect_timeout_ms: network,
        ..Config::default()
    })
    .unwrap();
    pool.request(Request::new(
        Key::ssh("u", "host", 22),
        Identity::Ambient,
        Owner::new("s", "o"),
    ))
    .unwrap();
    let Action::Connect { connection, .. } = pool.next_action().unwrap() else {
        panic!("connect")
    };
    let now = Arc::new(AtomicU64::new(0));
    let source = now.clone();
    let clock = pool
        .install_setup_clock(
            connection,
            Arc::new(move || source.load(Ordering::SeqCst)),
            stall,
        )
        .unwrap();
    (pool, now, clock)
}

fn terminal(clock: &gwz_transport::pool::SetupClock) -> gwz_transport::pool::SetupTerminal {
    let Observation::Terminal(record) = clock.observe().deliver() else {
        panic!("terminal")
    };
    record
}

#[test]
fn publication_pauses_before_driver_acknowledgement() {
    let (mut pool, now, clock) = fixture(100, 10);
    now.store(99, Ordering::SeqCst);
    let prepared = clock
        .prepare_local(LocalPhase::Admission, 200)
        .deliver()
        .unwrap();
    let receipt = clock.publish_local(prepared).deliver().unwrap();
    now.store(101, Ordering::SeqCst);
    pool.advance(101);
    assert!(matches!(
        clock.observe().deliver(),
        Observation::Alive {
            acknowledged: false,
            deadline: Some(200),
            ..
        }
    ));
    clock.acknowledge(receipt).deliver().unwrap();
    assert!(matches!(
        clock.observe().deliver(),
        Observation::Alive {
            acknowledged: true,
            ..
        }
    ));
}

#[test]
fn equality_expires_before_local_publication() {
    let (_, now, clock) = fixture(100, 0);
    let prepared = clock
        .prepare_local(LocalPhase::Admission, 200)
        .deliver()
        .unwrap();
    now.store(100, Ordering::SeqCst);
    assert!(matches!(
        clock.publish_local(prepared).deliver(),
        Err(PublicationError::ActiveTerminal(_))
    ));
    assert_eq!(terminal(&clock).cause, SetupCause::NetworkAggregate);
}

#[test]
fn expired_preparation_while_alive_is_its_own_terminal() {
    for network in [0, 100] {
        let (_, now, clock) = fixture(network, 10);
        let prepared = clock
            .prepare_local(LocalPhase::Admission, 1)
            .deliver()
            .unwrap();
        let phase = prepared.phase();
        now.store(2, Ordering::SeqCst);
        assert!(matches!(
            clock.observe().deliver(),
            Observation::Alive { .. }
        ));
        let Err(PublicationError::PreparationExpired(record)) =
            clock.publish_local(prepared).deliver()
        else {
            panic!("prepared expiry")
        };
        assert_eq!(record.phase, phase);
        assert_eq!(record.cause, SetupCause::PreparationDeadline);
    }
}

#[test]
fn interaction_preparation_does_not_expire_live_admission() {
    let (_, now, clock) = fixture(100, 0);
    let prepared = clock
        .prepare_local(LocalPhase::Admission, 50)
        .deliver()
        .unwrap();
    let receipt = clock.publish_local(prepared).deliver().unwrap();
    clock.acknowledge(receipt).deliver().unwrap();
    let prepared = clock
        .prepare_local(LocalPhase::Interaction, 1)
        .deliver()
        .unwrap();
    now.store(2, Ordering::SeqCst);
    assert!(matches!(
        clock.publish_local(prepared).deliver(),
        Err(PublicationError::PreparationExpired(_))
    ));
}

#[test]
fn cancellation_and_expired_ack_never_admit_work() {
    for cancel in [true, false] {
        let (_, now, clock) = fixture(100, 0);
        let prepared = clock
            .prepare_local(LocalPhase::Admission, 20)
            .deliver()
            .unwrap();
        let receipt = clock.publish_local(prepared).deliver().unwrap();
        if cancel {
            clock.terminate(SetupCause::Cancelled).deliver();
        } else {
            now.store(20, Ordering::SeqCst);
        }
        assert!(clock.acknowledge(receipt).deliver().is_err());
        let first = terminal(&clock);
        clock.terminate(SetupCause::Completed).deliver();
        assert_eq!(terminal(&clock), first);
    }
}

#[test]
fn retained_stall_remainder_and_repeated_departures() {
    let (_, now, clock) = fixture(100, 10);
    clock.begin_network_wait().deliver().unwrap();
    now.store(4, Ordering::SeqCst);
    let token = clock
        .prepare_local(LocalPhase::Admission, 100)
        .deliver()
        .unwrap();
    let receipt = clock.publish_local(token).deliver().unwrap();
    clock.acknowledge(receipt).deliver().unwrap();
    assert!(clock.network_progress().deliver().is_err());
    now.store(40, Ordering::SeqCst);
    let receipt = clock.publish_network().deliver().unwrap();
    clock.acknowledge(receipt).deliver().unwrap();
    assert!(matches!(
        clock.observe().deliver(),
        Observation::Alive {
            deadline: Some(46),
            ..
        }
    ));
    now.store(42, Ordering::SeqCst);
    let token = clock
        .prepare_local(LocalPhase::Interaction, 200)
        .deliver()
        .unwrap();
    let receipt = clock.publish_local(token).deliver().unwrap();
    clock.acknowledge(receipt).deliver().unwrap();
    now.store(100, Ordering::SeqCst);
    let receipt = clock.publish_network().deliver().unwrap();
    clock.acknowledge(receipt).deliver().unwrap();
    now.store(104, Ordering::SeqCst);
    assert_eq!(terminal(&clock).cause, SetupCause::NetworkStall);
}

#[test]
fn inactive_stall_is_not_started_by_resume_or_ack() {
    let (_, now, clock) = fixture(0, 10);
    let token = clock
        .prepare_local(LocalPhase::Admission, 100)
        .deliver()
        .unwrap();
    let receipt = clock.publish_local(token).deliver().unwrap();
    clock.acknowledge(receipt).deliver().unwrap();
    now.store(50, Ordering::SeqCst);
    let receipt = clock.publish_network().deliver().unwrap();
    now.store(80, Ordering::SeqCst);
    clock.acknowledge(receipt).deliver().unwrap();
    assert!(matches!(
        clock.observe().deliver(),
        Observation::Alive { deadline: None, .. }
    ));
    clock.begin_network_wait().deliver().unwrap();
    now.store(90, Ordering::SeqCst);
    assert_eq!(terminal(&clock).cause, SetupCause::NetworkStall);
}

#[test]
fn foreign_token_cannot_change_another_clock() {
    let (_, _, first) = fixture(100, 0);
    let (_, _, second) = fixture(100, 0);
    let token = first
        .prepare_local(LocalPhase::Admission, 20)
        .deliver()
        .unwrap();
    assert_eq!(
        second.publish_local(token).deliver(),
        Err(PublicationError::InvalidToken)
    );
    assert!(matches!(
        second.observe().deliver(),
        Observation::Alive { .. }
    ));
    assert!(
        first
            .prepare_local(LocalPhase::Admission, 30)
            .deliver()
            .is_ok()
    );
}

#[test]
fn expiry_arbitrates_by_deadline_and_aggregate_wins_a_tie() {
    for (stall, expected) in [
        (10, SetupCause::NetworkStall),
        (100, SetupCause::NetworkAggregate),
    ] {
        let (_, now, clock) = fixture(100, stall);
        clock.begin_network_wait().deliver().unwrap();
        now.store(100, Ordering::SeqCst);
        assert_eq!(terminal(&clock).cause, expected);
    }
}

#[test]
fn only_genuine_network_progress_resets_resumed_stall() {
    let (_, now, clock) = fixture(100, 10);
    clock.begin_network_wait().deliver().unwrap();
    now.store(9, Ordering::SeqCst);
    let token = clock
        .prepare_local(LocalPhase::Admission, 200)
        .deliver()
        .unwrap();
    let receipt = clock.publish_local(token).deliver().unwrap();
    clock.acknowledge(receipt).deliver().unwrap();
    now.store(50, Ordering::SeqCst);
    let receipt = clock.publish_network().deliver().unwrap();
    clock.acknowledge(receipt).deliver().unwrap();
    clock.network_progress().deliver().unwrap();
    now.store(59, Ordering::SeqCst);
    assert!(matches!(
        clock.observe().deliver(),
        Observation::Alive { .. }
    ));
    now.store(60, Ordering::SeqCst);
    assert_eq!(terminal(&clock).cause, SetupCause::NetworkStall);
}

#[test]
fn delayed_network_ack_counts_network_time() {
    let (_, now, clock) = fixture(100, 0);
    now.store(99, Ordering::SeqCst);
    let token = clock
        .prepare_local(LocalPhase::Admission, 200)
        .deliver()
        .unwrap();
    let receipt = clock.publish_local(token).deliver().unwrap();
    clock.acknowledge(receipt).deliver().unwrap();
    now.store(150, Ordering::SeqCst);
    let receipt = clock.publish_network().deliver().unwrap();
    now.store(151, Ordering::SeqCst);
    assert!(clock.acknowledge(receipt).deliver().is_err());
    assert_eq!(terminal(&clock).cause, SetupCause::NetworkAggregate);
}

#[test]
fn dropping_a_pending_waiter_cancels_the_connection() {
    use std::{
        future::Future,
        pin::pin,
        task::{Context, Poll, Waker},
    };
    let (_, _, clock) = fixture(0, 0);
    let token = clock
        .prepare_local(LocalPhase::Admission, 200)
        .deliver()
        .unwrap();
    let receipt = clock.publish_local(token).deliver().unwrap();
    {
        let mut wait = pin!(clock.wait_acknowledged(receipt));
        assert!(matches!(
            wait.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
    }
    assert_eq!(terminal(&clock).cause, SetupCause::Cancelled);
}

#[test]
fn authoritative_clock_clamps_backward_samples_and_refuses_overflow() {
    let (_, now, clock) = fixture(0, 0);
    now.store(10, Ordering::SeqCst);
    assert_eq!(clock.deadline_after(5).deliver(), Ok(15));
    now.store(1, Ordering::SeqCst);
    assert_eq!(clock.deadline_after(5).deliver(), Ok(15));
    now.store(u64::MAX, Ordering::SeqCst);
    assert_eq!(
        clock.deadline_after(1).deliver(),
        Err(PublicationError::Overflow)
    );
}

#[test]
fn pool_final_admission_rejects_expiry_between_native_result_and_completion() {
    let (mut pool, now, clock) = fixture(100, 0);
    let connection = clock.connection();
    now.store(99, Ordering::SeqCst);
    assert!(matches!(
        clock.observe().deliver(),
        Observation::Alive { .. }
    ));
    now.store(100, Ordering::SeqCst);
    pool.connected(connection, Ok(Some(Identity::Ambient)))
        .unwrap();
    assert_eq!(pool.counts().leased, 0);
    assert_eq!(pool.counts().closing, 1);
    assert_eq!(terminal(&clock).cause, SetupCause::NetworkAggregate);
}

#[test]
fn pool_success_uses_resumed_clock_and_cannot_skip_phase_ack() {
    for acknowledged in [true, false] {
        let (mut pool, now, clock) = fixture(100, 0);
        now.store(90, Ordering::SeqCst);
        let token = clock
            .prepare_local(LocalPhase::Interaction, 500)
            .deliver()
            .unwrap();
        let receipt = clock.publish_local(token).deliver().unwrap();
        clock.acknowledge(receipt).deliver().unwrap();
        now.store(300, Ordering::SeqCst);
        let receipt = clock.publish_network().deliver().unwrap();
        if acknowledged {
            clock.acknowledge(receipt).deliver().unwrap();
        }
        pool.advance(300);
        pool.connected(clock.connection(), Ok(Some(Identity::Ambient)))
            .unwrap();
        assert_eq!(pool.counts().leased, usize::from(acknowledged));
        assert_eq!(
            terminal(&clock).cause == SetupCause::Completed,
            acknowledged
        );
    }
}

#[test]
fn driver_loss_wakes_pending_receipt_once_and_retains_capacity() {
    use gwz_transport::pool::Pool;
    use std::{
        future::Future,
        pin::pin,
        task::{Context, Poll, Wake, Waker},
    };
    struct WakeCount(AtomicU64);
    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let (pool, mut driver) = Pool::new(Config::default()).unwrap();
    let _checkout = pool
        .checkout(Request::new(
            Key::ssh("u", "h", 22),
            Identity::Ambient,
            Owner::new("s", "o"),
        ))
        .unwrap();
    let action = {
        let mut action = pin!(driver.next_action());
        let Poll::Ready(Some(action)) = action
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("connect")
        };
        action
    };
    let Action::Connect { connection, .. } = action else {
        panic!("connect")
    };
    let clock = driver
        .install_setup_clock(connection, Arc::new(|| 0), 0)
        .unwrap();
    let prepared = clock
        .prepare_local(LocalPhase::Admission, 100)
        .deliver()
        .unwrap();
    let receipt = clock.publish_local(prepared).deliver().unwrap();
    let counter = Arc::new(WakeCount(AtomicU64::new(0)));
    let waker = Waker::from(counter.clone());
    let mut wait = pin!(clock.wait_acknowledged(receipt));
    assert!(
        wait.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(driver);
    assert_eq!(counter.0.load(Ordering::SeqCst), 1);
    assert!(
        wait.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_ready()
    );
    assert_eq!(terminal(&clock).cause, SetupCause::DriverLost);
    assert_eq!(pool.counts().opening, 1);
}

#[test]
fn connect_dispatch_retains_allocation_wait_without_charging_network() {
    use gwz_transport::pool::Pool;
    use std::{
        future::Future,
        pin::pin,
        task::{Context, Poll, Waker},
    };
    let (pool, mut driver) = Pool::new(Config {
        allocation_timeout_ms: 100,
        ..Default::default()
    })
    .unwrap();
    let _checkout = pool
        .checkout(Request::new(
            Key::ssh("u", "h", 22),
            Identity::Ambient,
            Owner::new("s", "o"),
        ))
        .unwrap();
    driver.advance(40);
    let action = {
        let mut action = pin!(driver.next_action());
        let Poll::Ready(Some(action)) = action
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("connect")
        };
        action
    };
    let Action::Connect { connection, .. } = action else {
        panic!("connect")
    };
    let retained = driver.opening_allocation_remaining(connection).unwrap();
    assert_eq!(retained, 60);
    driver.advance(80);
    assert_eq!(
        retained, 60,
        "network work does not spend the captured allocation"
    );
}
