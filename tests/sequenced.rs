use gwz_transport::sequenced::{Direction, Error as SequenceError, Outbound, Receiver, Status};
use gwz_transport::{binding, codec, protocol::*};

fn v3_data(sequence: Option<i64>) -> Envelope {
    Envelope {
        version: 3,
        session_id: "sequenced-test".into(),
        stream_id: 1,
        kind: MessageKind::Data,
        message_seq: sequence,
        data: Some(Data {
            offset: 0,
            payload: vec![1, 2, 3],
        }),
        ..Default::default()
    }
}

#[test]
fn profile_three_requires_a_positive_sequence_on_every_stream_frame() {
    let message = v3_data(Some(1));
    let encoded = codec::encode(&message).unwrap();
    assert_eq!(codec::decode(&encoded).unwrap(), message);
    assert_eq!(
        codec::admit(&v3_data(None)),
        Err(codec::Error::InvalidMessage)
    );
    assert_eq!(
        codec::admit(&v3_data(Some(0))),
        Err(codec::Error::InvalidMessage)
    );
    let mut old = v3_data(Some(1));
    old.version = 2;
    assert_eq!(codec::admit(&old), Err(codec::Error::InvalidMessage));
    let mut bootstrap = binding::offer("sequenced-test", EndpointRole::Driver);
    bootstrap.message_seq = Some(1);
    assert_eq!(codec::admit(&bootstrap), Err(codec::Error::InvalidMessage));
}

#[test]
fn profile_three_is_explicitly_negotiated_without_fallback() {
    let mut offer = binding::offer("sequenced-test", EndpointRole::Driver);
    offer.bind.as_mut().unwrap().versions = vec![3];
    let endpoint = binding::EndpointConfig {
        endpoint_id: "endpoint".into(),
        role: EndpointRole::Driver,
        schemes: vec![Scheme::Ssh],
        policies: vec![AuthPolicy::SshAmbient],
        limits: binding::default_limits(),
        trust_owner: "owner".into(),
    };
    let (reply, accepted) = endpoint.accept(&offer).unwrap();
    assert_eq!(reply.version, 1);
    assert_eq!(accepted.profile_version(), 3);
    assert_eq!(
        binding::verify(&offer, &reply).unwrap().profile_version(),
        3
    );
}

#[test]
fn profile_three_refuses_limits_without_a_missing_predecessor_reserve() {
    let mut offer = binding::offer("sequenced-test", EndpointRole::Driver);
    offer.bind.as_mut().unwrap().versions = vec![3];
    offer.bind.as_mut().unwrap().receive_limits.queued_bytes = 1_000_000;
    offer.bind.as_mut().unwrap().receive_limits.receive_window = 500_000;
    let endpoint = binding::EndpointConfig {
        endpoint_id: "endpoint".into(),
        role: EndpointRole::Driver,
        schemes: vec![Scheme::Ssh],
        policies: vec![AuthPolicy::SshAmbient],
        limits: binding::default_limits(),
        trust_owner: "owner".into(),
    };
    assert_eq!(
        endpoint.accept(&offer).unwrap_err().code,
        ErrorCode::UnsupportedOperation
    );
}

fn frame(stream_id: i64, sequence: i64, kind: MessageKind) -> Envelope {
    let mut message = Envelope {
        version: 3,
        session_id: "sequenced-test".into(),
        stream_id,
        kind,
        message_seq: Some(sequence),
        ..Default::default()
    };
    match kind {
        MessageKind::Open => {
            message.open = Some(Open {
                endpoint_id: "endpoint".into(),
                operation_id: format!("operation-{stream_id}"),
                destination: Destination {
                    scheme: Scheme::Ssh,
                    host: "example.test".into(),
                    port: 22,
                    path: "/repo".into(),
                    ssh_username: Some("git".into()),
                    https_username: None,
                },
                service: GitService::UploadPackExchange,
                identity: Identity {
                    mode: IdentityMode::Ambient,
                    key_path: None,
                    path_base: None,
                },
                policy: AuthPolicy::SshAmbient,
                deadlines: Deadlines {
                    allocation_ms: 1000,
                    connect_ms: 1000,
                    io_ms: 1000,
                    interaction_ms: 1000,
                    cleanup_ms: 1000,
                },
                receive_limits: binding::default_limits(),
            });
        }
        MessageKind::Data => {
            message.data = Some(Data {
                offset: 0,
                payload: vec![sequence as u8],
            });
        }
        MessageKind::Window => {
            message.window = Some(Window {
                max_offset: sequence,
            });
        }
        MessageKind::Flushed => {
            message.flushed = Some(Barrier {
                barrier_id: sequence,
                offset: 0,
            });
        }
        MessageKind::Failed => {
            message.failed = Some(Failure {
                code: ErrorCode::Io,
                effect: Effect::Possible,
                facts: None,
                detail: None,
                setup_cause: None,
            });
        }
        MessageKind::Opened => {
            message.opened = Some(Opened {
                connection_id: "connection".into(),
                endpoint_id: "endpoint".into(),
                trust_owner: "owner".into(),
                receive_limits: binding::default_limits(),
                ..Default::default()
            });
        }
        MessageKind::EndWrite => {
            message.end_write = Some(EndWrite { final_offset: 0 });
        }
        MessageKind::Closed => {
            message.closed = Some(Closed::default());
        }
        _ => panic!("unsupported test kind"),
    }
    message
}

#[test]
fn graceful_terminal_waits_for_its_opening_and_every_predecessor() {
    let mut receiver = Receiver::new(
        "sequenced-test",
        Direction::Endpoint,
        binding::default_limits(),
    )
    .unwrap();
    receiver.register(1).unwrap();
    let mut applied = Vec::new();
    for (sequence, kind) in [(3, MessageKind::Closed), (2, MessageKind::EndWrite)] {
        let report = receiver
            .submit(frame(1, sequence, kind), sequence as u64, |message| {
                applied.push(message.message_seq.unwrap());
                Ok(())
            })
            .unwrap();
        assert_eq!(report.submitted, Status::Buffered);
        assert!(report.resolved.is_empty());
        assert!(applied.is_empty());
    }
    let report = receiver
        .submit(frame(1, 1, MessageKind::Opened), 4, |message| {
            applied.push(message.message_seq.unwrap());
            Ok(())
        })
        .unwrap();
    assert_eq!(applied, [1, 2, 3]);
    assert_eq!(report.resolved.len(), 3);
    assert!(
        report
            .resolved
            .iter()
            .all(|item| item.status == Status::Applied)
    );
}

#[test]
fn abortive_successor_cannot_supersede_an_earlier_graceful_terminal() {
    let mut receiver = Receiver::new(
        "sequenced-test",
        Direction::Endpoint,
        binding::default_limits(),
    )
    .unwrap();
    receiver.register(1).unwrap();
    receiver
        .submit(frame(1, 1, MessageKind::Opened), 0, |_| Ok(()))
        .unwrap();
    assert_eq!(
        receiver
            .submit(frame(1, 3, MessageKind::Closed), 1, |_| Ok(()))
            .unwrap()
            .submitted,
        Status::Buffered
    );
    assert_eq!(
        receiver.submit(frame(1, 4, MessageKind::Failed), 2, |_| Ok(())),
        Err(SequenceError::Protocol)
    );
}

#[test]
fn terminal_record_still_rejects_a_conflicting_opening_replay() {
    let mut receiver = Receiver::new(
        "sequenced-test",
        Direction::Initiator,
        binding::default_limits(),
    )
    .unwrap();
    receiver.register(1).unwrap();
    receiver
        .submit(frame(1, 1, MessageKind::Open), 0, |_| Ok(()))
        .unwrap();
    receiver
        .submit(frame(1, 3, MessageKind::Failed), 1, |_| Ok(()))
        .unwrap();
    let mut conflicting = frame(1, 1, MessageKind::Open);
    conflicting.open.as_mut().unwrap().operation_id = "another-operation".into();
    assert_eq!(
        receiver.submit(conflicting, 2, |_| Ok(())),
        Err(SequenceError::Protocol)
    );
}

#[test]
fn outbound_sequence_is_committed_only_with_bounded_queue_ownership() {
    let mut sender = Outbound::new(
        "sequenced-test",
        Direction::Initiator,
        binding::default_limits(),
        2,
    )
    .unwrap();
    let mut opening = frame(1, 1, MessageKind::Open);
    opening.message_seq = None;
    assert_eq!(sender.enqueue(opening), Ok(1));
    let mut first_data = frame(1, 2, MessageKind::Data);
    first_data.message_seq = None;
    assert_eq!(sender.enqueue(first_data), Ok(2));
    let mut blocked = frame(1, 3, MessageKind::Data);
    blocked.message_seq = None;
    assert_eq!(
        sender.enqueue(blocked.clone()),
        Err(SequenceError::WouldBlock)
    );
    assert_eq!(sender.pop().unwrap().message_seq, Some(1));
    assert_eq!(sender.enqueue(blocked), Ok(3));
    assert_eq!(sender.pop().unwrap().message_seq, Some(2));
    assert_eq!(sender.pop().unwrap().message_seq, Some(3));
}

#[test]
fn outbound_control_reserve_admits_failure_after_data_queue_fills() {
    let mut sender = Outbound::new(
        "sequenced-test",
        Direction::Initiator,
        binding::default_limits(),
        2,
    )
    .unwrap();
    for (sequence, kind) in [(1, MessageKind::Open), (2, MessageKind::Data)] {
        let mut message = frame(1, sequence, kind);
        message.message_seq = None;
        assert_eq!(sender.enqueue(message), Ok(sequence));
    }
    let mut failed = frame(1, 3, MessageKind::Failed);
    failed.message_seq = None;
    assert_eq!(sender.enqueue(failed), Ok(3));
    assert_eq!(sender.pop().unwrap().message_seq, Some(1));
    assert_eq!(sender.pop().unwrap().message_seq, Some(2));
    assert_eq!(sender.pop().unwrap().message_seq, Some(3));
}

#[test]
fn receiver_applies_a_permuted_prefix_and_reports_buffered_versus_applied() {
    let mut receiver = Receiver::new(
        "sequenced-test",
        Direction::Initiator,
        binding::default_limits(),
    )
    .unwrap();
    receiver.register(1).unwrap();
    let mut applied = Vec::new();
    let callback = |message: &Envelope| {
        applied.push(message.message_seq.unwrap());
        Ok(())
    };
    let report = receiver
        .submit(frame(1, 3, MessageKind::Data), 0, callback)
        .unwrap();
    assert_eq!(report.submitted, Status::Buffered);
    assert!(applied.is_empty());
    let report = receiver
        .submit(frame(1, 2, MessageKind::Window), 1, |message| {
            applied.push(message.message_seq.unwrap());
            Ok(())
        })
        .unwrap();
    assert_eq!(report.submitted, Status::Buffered);
    let report = receiver
        .submit(frame(1, 1, MessageKind::Open), 2, |message| {
            applied.push(message.message_seq.unwrap());
            Ok(())
        })
        .unwrap();
    assert_eq!(report.submitted, Status::Applied);
    assert_eq!(applied, vec![1, 2, 3]);
    assert_eq!(report.resolved.len(), 3);
    assert!(report.resolved.iter().all(|r| r.status == Status::Applied));
}

#[test]
fn abortive_terminal_supersedes_late_lower_controls_without_reapplying() {
    let mut receiver = Receiver::new(
        "sequenced-test",
        Direction::Initiator,
        binding::default_limits(),
    )
    .unwrap();
    receiver.register(1).unwrap();
    let mut applied = Vec::new();
    receiver
        .submit(frame(1, 1, MessageKind::Open), 0, |message| {
            applied.push(message.kind);
            Ok(())
        })
        .unwrap();
    let report = receiver
        .submit(frame(1, 4, MessageKind::Failed), 1, |message| {
            applied.push(message.kind);
            Ok(())
        })
        .unwrap();
    assert_eq!(report.submitted, Status::Applied);
    for (sequence, kind) in [(2, MessageKind::Window), (3, MessageKind::Flushed)] {
        let report = receiver
            .submit(frame(1, sequence, kind), 2, |_| {
                panic!("late control applied")
            })
            .unwrap();
        assert_eq!(report.submitted, Status::Superseded);
    }
    assert_eq!(applied, vec![MessageKind::Open, MessageKind::Failed]);
}

#[test]
fn full_reorder_budget_still_admits_the_missing_predecessor() {
    let mut receiver = Receiver::new(
        "sequenced-test",
        Direction::Initiator,
        binding::default_limits(),
    )
    .unwrap();
    receiver.register(1).unwrap();
    receiver
        .submit(frame(1, 1, MessageKind::Open), 0, |_| Ok(()))
        .unwrap();
    for sequence in 3..=10 {
        assert_eq!(
            receiver
                .submit(frame(1, sequence, MessageKind::Window), 1, |_| Ok(()))
                .unwrap()
                .submitted,
            Status::Buffered
        );
    }
    assert_eq!(
        receiver.submit(frame(1, 11, MessageKind::Window), 1, |_| Ok(())),
        Err(SequenceError::WouldBlock)
    );
    let report = receiver
        .submit(frame(1, 2, MessageKind::Window), 2, |_| Ok(()))
        .unwrap();
    assert_eq!(report.resolved.len(), 9);
    assert_eq!(receiver.buffered_frames(), 0);
}

#[test]
fn gap_deadline_is_fixed_by_first_buffered_successor() {
    let mut receiver = Receiver::new(
        "sequenced-test",
        Direction::Initiator,
        binding::default_limits(),
    )
    .unwrap();
    receiver.register(1).unwrap();
    receiver
        .submit(frame(1, 2, MessageKind::Window), 100, |_| Ok(()))
        .unwrap();
    receiver
        .submit(frame(1, 3, MessageKind::Window), 29_999, |_| Ok(()))
        .unwrap();
    assert_eq!(receiver.advance(30_099), Ok(()));
    assert_eq!(receiver.advance(30_100), Err(SequenceError::GapTimeout));
}

#[test]
fn abortive_terminal_supersedes_already_buffered_predecessors() {
    let mut receiver = Receiver::new(
        "sequenced-test",
        Direction::Initiator,
        binding::default_limits(),
    )
    .unwrap();
    receiver.register(1).unwrap();
    receiver
        .submit(frame(1, 1, MessageKind::Open), 0, |_| Ok(()))
        .unwrap();
    receiver
        .submit(frame(1, 3, MessageKind::Flushed), 1, |_| {
            panic!("buffered control applied")
        })
        .unwrap();
    let report = receiver
        .submit(frame(1, 4, MessageKind::Failed), 2, |_| Ok(()))
        .unwrap();
    assert_eq!(receiver.buffered_frames(), 0);
    assert!(
        report
            .resolved
            .iter()
            .any(|r| { r.message_seq == 3 && r.status == Status::Superseded })
    );
    assert_eq!(
        receiver
            .submit(frame(1, 2, MessageKind::Window), 3, |_| panic!(
                "late frame applied"
            ))
            .unwrap()
            .submitted,
        Status::Superseded
    );
    assert_eq!(
        receiver
            .submit(frame(1, 1, MessageKind::Open), 3, |_| panic!(
                "opening replayed"
            ))
            .unwrap()
            .submitted,
        Status::AlreadyResolved
    );
}

#[test]
fn blocked_application_can_resume_without_waiting_for_another_message() {
    let mut receiver = Receiver::new(
        "sequenced-test",
        Direction::Initiator,
        binding::default_limits(),
    )
    .unwrap();
    receiver.register(1).unwrap();
    receiver
        .submit(frame(1, 1, MessageKind::Open), 0, |_| Ok(()))
        .unwrap();
    let report = receiver
        .submit(frame(1, 2, MessageKind::Data), 1, |_| {
            Err(gwz_transport::sequenced::ApplyError::WouldBlock)
        })
        .unwrap();
    assert_eq!(report.submitted, Status::Buffered);
    let report = receiver.retry(1, 2, |_| Ok(())).unwrap();
    assert_eq!(report.resolved.len(), 1);
    assert_eq!(report.resolved[0].message_seq, 2);
    assert_eq!(receiver.buffered_frames(), 0);
}

#[test]
fn other_stream_makes_progress_while_one_has_a_gap() {
    let mut receiver = Receiver::new(
        "sequenced-test",
        Direction::Initiator,
        binding::default_limits(),
    )
    .unwrap();
    receiver.register(1).unwrap();
    receiver.register(2).unwrap();
    receiver
        .submit(frame(1, 2, MessageKind::Window), 0, |_| {
            panic!("gap applied")
        })
        .unwrap();
    let report = receiver
        .submit(frame(2, 1, MessageKind::Open), 1, |_| Ok(()))
        .unwrap();
    assert_eq!(report.submitted, Status::Applied);
    assert_eq!(receiver.buffered_frames(), 1);
}

#[test]
fn close_does_not_end_the_initiator_message_sequence() {
    let mut sender = Outbound::new(
        "sequenced-test",
        Direction::Initiator,
        binding::default_limits(),
        8,
    )
    .unwrap();
    let mut opening = frame(1, 1, MessageKind::Open);
    opening.message_seq = None;
    assert_eq!(sender.enqueue(opening), Ok(1));
    let mut end = Envelope {
        version: 3,
        session_id: "sequenced-test".into(),
        stream_id: 1,
        kind: MessageKind::EndWrite,
        end_write: Some(EndWrite { final_offset: 0 }),
        ..Default::default()
    };
    assert_eq!(sender.enqueue(end.clone()), Ok(2));
    end.kind = MessageKind::Close;
    end.end_write = None;
    end.close = Some(Close { final_offset: 0 });
    assert_eq!(sender.enqueue(end), Ok(3));
    let mut window = frame(1, 4, MessageKind::Window);
    window.message_seq = None;
    assert_eq!(sender.enqueue(window), Ok(4));
    let mut flushed = frame(1, 5, MessageKind::Flushed);
    flushed.message_seq = None;
    assert_eq!(sender.enqueue(flushed), Ok(5));
    let mut invalid = frame(1, 6, MessageKind::Data);
    invalid.message_seq = None;
    assert_eq!(sender.enqueue(invalid), Err(SequenceError::Protocol));
}
