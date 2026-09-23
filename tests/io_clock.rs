use gwz_transport::{
    protocol::{Effect, Envelope, ErrorCode, Failure, MessageKind},
    stream::{Config, Error, IoState, Side, StreamMachine},
};

fn endpoint() -> StreamMachine {
    let mut config = Config::new("io-clock", 1, Side::Endpoint);
    config.io_timeout_ms = 10;
    config.interaction_budget_ms = 20;
    StreamMachine::new(config).unwrap()
}

#[test]
fn network_pause_resume_preserves_allowance_and_progress_resets_it() {
    let mut machine = endpoint();
    assert_eq!(machine.io_status().state, IoState::Idle);
    machine.set_io_state(IoState::Network).unwrap();
    assert_eq!(machine.io_status().active_deadline, Some(10));
    machine.advance(4);
    assert_eq!(machine.io_status().remaining_network_ms, 6);
    machine.set_io_state(IoState::Backpressure).unwrap();
    machine.advance(40);
    assert_eq!(machine.io_status().remaining_network_ms, 6);
    machine.set_io_state(IoState::Network).unwrap();
    assert_eq!(machine.io_status().active_deadline, Some(46));
    machine.record_io_progress(0).unwrap();
    assert_eq!(machine.io_status().active_deadline, Some(46));
    machine.record_io_progress(1).unwrap();
    assert_eq!(machine.io_status().active_deadline, Some(50));
}

#[test]
fn interaction_allowance_is_cumulative_and_zero_forbids_waiting() {
    let mut machine = endpoint();
    machine.set_io_state(IoState::Interaction).unwrap();
    machine.advance(7);
    assert_eq!(machine.io_status().remaining_interaction_ms, 13);
    machine.set_io_state(IoState::Idle).unwrap();
    machine.advance(100);
    machine.set_io_state(IoState::Interaction).unwrap();
    assert_eq!(machine.io_status().active_deadline, Some(113));
    machine.advance(113);
    assert_eq!(machine.io_status().state, IoState::Idle);
    assert_eq!(machine.next_message().unwrap().kind, MessageKind::Failed);

    let mut config = Config::new("io-clock-zero", 1, Side::Endpoint);
    config.interaction_budget_ms = 0;
    let mut zero = StreamMachine::new(config).unwrap();
    assert_eq!(zero.set_io_state(IoState::Interaction), Err(Error::Timeout));
}

#[test]
fn invalid_progress_and_initiator_mutation_are_explicit() {
    let mut machine = endpoint();
    assert_eq!(machine.record_io_progress(1), Err(Error::WrongState));
    machine.set_io_state(IoState::Backpressure).unwrap();
    assert_eq!(machine.record_io_progress(1), Err(Error::WrongState));

    let mut config = Config::new("io-clock-init", 1, Side::Initiator);
    config.io_timeout_ms = 10;
    let mut initiator = StreamMachine::new(config).unwrap();
    assert_eq!(
        initiator.set_io_state(IoState::Network),
        Err(Error::WrongSide)
    );
    assert_eq!(initiator.record_io_progress(1), Err(Error::WrongSide));
}

#[test]
fn exact_deadline_times_out_and_preserves_received_prefix() {
    let mut machine = endpoint();
    machine
        .receive(Envelope {
            version: 1,
            session_id: "io-clock".into(),
            stream_id: 1,
            kind: MessageKind::Data,
            data: Some(gwz_transport::protocol::Data {
                offset: 0,
                payload: b"prefix".to_vec(),
            }),
            ..Default::default()
        })
        .unwrap();
    machine.set_io_state(IoState::Network).unwrap();
    machine.advance(10);
    let mut output = [0; 8];
    assert_eq!(machine.read(&mut output), Ok(6));
    assert_eq!(&output[..6], b"prefix");
    assert_eq!(machine.read(&mut output), Err(Error::Timeout));
    assert_eq!(
        machine.next_message().unwrap(),
        Envelope {
            version: 1,
            session_id: "io-clock".into(),
            stream_id: 1,
            kind: MessageKind::Failed,
            failed: Some(Failure {
                setup_cause: None,
                code: ErrorCode::Timeout,
                effect: Effect::Possible,
                facts: None,
            }),
            ..Default::default()
        }
    );
}

#[test]
fn close_takes_over_clock_and_late_reports_cannot_rearm_it() {
    let mut machine = endpoint();
    machine.set_io_state(IoState::Network).unwrap();
    machine.advance(3);
    machine
        .receive(Envelope {
            version: 1,
            session_id: "io-clock".into(),
            stream_id: 1,
            kind: MessageKind::EndWrite,
            end_write: Some(gwz_transport::protocol::EndWrite { final_offset: 0 }),
            ..Default::default()
        })
        .unwrap();
    machine
        .receive(Envelope {
            version: 1,
            session_id: "io-clock".into(),
            stream_id: 1,
            kind: MessageKind::Close,
            close: Some(gwz_transport::protocol::Close { final_offset: 0 }),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(machine.io_status().state, IoState::Idle);
    assert_eq!(
        machine.set_io_state(IoState::Network),
        Err(Error::WrongState)
    );
    assert_eq!(machine.record_io_progress(1), Err(Error::WrongState));
    assert_eq!(machine.next_deadline(), Some(5003));
}

#[test]
fn quiet_idle_backward_ticks_and_config_boundaries_are_deterministic() {
    let mut quiet = endpoint();
    quiet.advance(100);
    let idle = quiet.io_status();
    quiet.advance(50);
    assert_eq!(quiet.io_status(), idle);
    quiet.set_io_state(IoState::Network).unwrap();
    quiet.advance(105);
    quiet.advance(90);
    assert_eq!(quiet.io_status().remaining_network_ms, 5);
    quiet.advance(110);
    assert_eq!(quiet.read(&mut [0]), Err(Error::Timeout));

    for (io_timeout_ms, interaction_budget_ms) in [(2_147_483_648, 1), (1, 86_400_001)] {
        let mut config = Config::new("io-clock-bound", 1, Side::Endpoint);
        config.io_timeout_ms = io_timeout_ms;
        config.interaction_budget_ms = interaction_budget_ms;
        assert!(matches!(
            StreamMachine::new(config),
            Err(Error::InvalidConfig)
        ));
    }
}
