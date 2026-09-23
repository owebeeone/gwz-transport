use gwz_transport::{
    binding,
    mux::{Config, Error, Mux, Phase},
    protocol::*,
};

fn pair() -> (Mux, Mux) {
    let config = Config::default();
    let endpoint = binding::EndpointConfig {
        endpoint_id: "endpoint".into(),
        role: EndpointRole::Driver,
        schemes: vec![Scheme::Ssh],
        policies: vec![AuthPolicy::SshAmbient, AuthPolicy::SshExplicit],
        limits: binding::default_limits(),
        trust_owner: "account".into(),
    };
    (
        Mux::initiator("session", config.clone()).unwrap(),
        Mux::endpoint("session", endpoint, config).unwrap(),
    )
}
fn register(core: &mut Mux, cli: &mut Mux, id: &str) {
    core.register(id, Some(format!("op-{id}"))).unwrap();
    cli.register(id, None).unwrap();
}
fn bind(core: &mut Mux, cli: &mut Mux) {
    register(core, cli, "a");
    core.begin("a").unwrap();
    cli.receive(&core.next_message().unwrap()).unwrap();
    core.receive(&cli.next_message().unwrap()).unwrap();
    assert_eq!(core.phase(), Phase::Ready);
}
fn check(core: &mut Mux, req: &str) -> i64 {
    core.check_identity(
        req,
        Identity {
            mode: IdentityMode::ExplicitKey,
            key_path: Some("key".into()),
            path_base: Some("/endpoint".into()),
        },
        100,
    )
    .unwrap()
}
#[test]
fn cancellation_between_endpoint_ready_and_mux_install_retires_generation() {
    let (mut core, mut cli) = pair();
    register(&mut core, &mut cli, "a");
    register(&mut core, &mut cli, "b");
    core.begin("a").unwrap();
    core.begin("b").unwrap();
    cli.receive(&core.next_message().unwrap()).unwrap();
    let late = cli.next_message().unwrap();
    assert_eq!(cli.phase(), Phase::Ready);
    core.cancel("a").unwrap();
    assert_eq!(core.phase(), Phase::Closed);
    assert_eq!(core.begin("b"), Err(Error::Closed));
    assert_eq!(core.receive(&late), Err(Error::Closed));
    // The supplied host propagates port closure, not an invented wire frame.
    cli.disconnect();
    assert_eq!(cli.phase(), Phase::Closed);
}
#[test]
fn installed_binding_survives_owner_cancel_and_check_is_correlated() {
    let (mut core, mut cli) = pair();
    bind(&mut core, &mut cli);
    core.cancel("a").unwrap();
    register(&mut core, &mut cli, "b");
    core.begin("b").unwrap();
    assert_eq!(core.phase(), Phase::Ready);
    let id = check(&mut core, "b");
    cli.receive(&core.next_message().unwrap()).unwrap();
    let (req, request) = cli.next_action().unwrap();
    assert_eq!(request.check_identity.unwrap().operation_id, "op-b");
    let mut reply = cli.message(id, MessageKind::IdentityChecked).unwrap();
    reply.identity_checked = Some(IdentityChecked {});
    cli.send(&req, &reply).unwrap();
    core.receive(&cli.next_message().unwrap()).unwrap();
    let (req, reply) = core.next_action().unwrap();
    assert_eq!(req, "b");
    assert_eq!(reply.stream_id, id);
    assert_eq!(core.active_streams(), 0);
}
#[test]
fn wrong_request_cannot_capture_terminal_or_release_another_stream() {
    let (mut core, mut cli) = pair();
    bind(&mut core, &mut cli);
    register(&mut core, &mut cli, "b");
    let id = check(&mut core, "a");
    cli.receive(&core.next_message().unwrap()).unwrap();
    let mut reply = cli.message(id, MessageKind::IdentityChecked).unwrap();
    reply.identity_checked = Some(IdentityChecked {});
    assert_eq!(core.receive(&("b".into(), reply)), Err(Error::Protocol));
    assert_eq!(core.phase(), Phase::Closed);
}
#[test]
fn retired_request_ids_and_stream_ids_cannot_reopen_work() {
    let (mut core, mut cli) = pair();
    bind(&mut core, &mut cli);
    let id = check(&mut core, "a");
    let sent = core.next_message().unwrap();
    cli.receive(&sent).unwrap();
    cli.next_action();
    let mut reply = cli.message(id, MessageKind::IdentityChecked).unwrap();
    reply.identity_checked = Some(IdentityChecked {});
    cli.send("a", &reply).unwrap();
    assert!(cli.receive(&sent).is_ok()); // retired id is discarded
    assert!(cli.next_action().is_none());
    assert_eq!(cli.finish("a"), Err(Error::WouldBlock));
    assert_eq!(cli.next_message().unwrap().1, reply);
    cli.finish("a").unwrap();
    assert_eq!(cli.register("a", None), Err(Error::InvalidRequest));
}
#[test]
fn unknown_session_is_ignored_but_unregistered_request_cannot_open() {
    let (mut core, mut cli) = pair();
    bind(&mut core, &mut cli);
    check(&mut core, "a");
    let mut sent = core.next_message().unwrap();
    let session = sent.1.session_id.clone();
    sent.1.session_id = "old".into();
    cli.receive(&sent).unwrap();
    assert!(cli.next_action().is_none());
    sent.1.session_id = session;
    sent.0 = "forged".into();
    assert_eq!(cli.receive(&sent), Err(Error::Protocol));
}

#[test]
fn cancelling_endpoint_work_notifies_worker_and_keeps_binding() {
    let (mut core, mut cli) = pair();
    bind(&mut core, &mut cli);
    let id = check(&mut core, "a");
    cli.receive(&core.next_message().unwrap()).unwrap();
    cli.next_action();
    cli.cancel("a").unwrap();
    let (_, action) = cli.next_action().expect("cancel reaches endpoint worker");
    assert_eq!(action.kind, MessageKind::Cancel);
    assert_eq!(action.stream_id, id);
    let (_, reply) = cli.next_message().unwrap();
    assert_eq!(
        reply.identity_check_failed.unwrap().code,
        ErrorCode::Cancelled
    );
    assert_eq!(cli.phase(), Phase::Ready);
}
#[test]
fn endpoint_check_deadline_notifies_peer_and_local_worker() {
    let (mut core, mut cli) = pair();
    bind(&mut core, &mut cli);
    check(&mut core, "a");
    cli.receive(&core.next_message().unwrap()).unwrap();
    cli.next_action();
    cli.advance(100);
    assert_eq!(cli.next_action().unwrap().1.kind, MessageKind::Cancel);
    let reply = cli.next_message().expect("timeout reply");
    assert_eq!(
        reply.1.identity_check_failed.as_ref().unwrap().code,
        ErrorCode::Timeout
    );
    core.receive(&reply).unwrap();
    assert_eq!(core.active_streams(), 0);
}
#[test]
fn repeated_finish_does_not_extend_cleanup_deadline() {
    let (mut core, mut cli) = pair();
    bind(&mut core, &mut cli);
    check(&mut core, "a");
    cli.receive(&core.next_message().unwrap()).unwrap();
    assert_eq!(core.finish("a"), Err(Error::WouldBlock));
    core.advance(4999);
    assert_eq!(core.finish("a"), Err(Error::WouldBlock));
    core.advance(5000);
    assert_eq!(core.finish("a"), Err(Error::WouldBlock));
    core.next_action()
        .expect("local timeout must reach its owner");
    assert!(core.finish("a").is_ok());
}
#[test]
fn duplicate_bound_is_idempotent_after_bootstrap_request_finishes() {
    let (mut core, mut cli) = pair();
    register(&mut core, &mut cli, "a");
    core.begin("a").unwrap();
    cli.receive(&core.next_message().unwrap()).unwrap();
    let bound = cli.next_message().unwrap();
    core.receive(&bound).unwrap();
    core.finish("a").unwrap();
    assert!(core.receive(&bound).is_ok());
    assert_eq!(core.phase(), Phase::Ready);
}
fn begin_stream(core: &mut Mux, cli: &mut Mux) -> i64 {
    let id = core
        .open(
            "a",
            Open {
                endpoint_id: "endpoint".into(),
                operation_id: "op-a".into(),
                destination: Destination {
                    scheme: Scheme::Ssh,
                    host: "git.example".into(),
                    port: 22,
                    path: "repo".into(),
                    ssh_username: Some("git".into()),
                },
                service: GitService::UploadPackExchange,
                identity: Identity {
                    mode: IdentityMode::Ambient,
                    ..Default::default()
                },
                policy: AuthPolicy::SshAmbient,
                deadlines: Deadlines {
                    allocation_ms: 100,
                    connect_ms: 100,
                    io_ms: 100,
                    interaction_ms: 100,
                    cleanup_ms: 100,
                },
                receive_limits: binding::default_limits(),
            },
        )
        .unwrap();
    cli.receive(&core.next_message().unwrap()).unwrap();
    cli.next_action();
    id
}
fn open_stream(core: &mut Mux, cli: &mut Mux) -> i64 {
    let id = begin_stream(core, cli);
    let mut opened = cli.message(id, MessageKind::Opened).unwrap();
    opened.opened = Some(Opened {
        endpoint_id: "endpoint".into(),
        connection_id: "connection".into(),
        trust_owner: "account".into(),
        reused: false,
        facts: Facts::default(),
        receive_limits: binding::default_limits(),
    });
    cli.send("a", &opened).unwrap();
    core.receive(&cli.next_message().unwrap()).unwrap();
    core.next_action();
    id
}
#[test]
fn data_queue_capacity_preserves_control_admission() {
    let (mut core, mut cli) = pair();
    bind(&mut core, &mut cli);
    let id = open_stream(&mut core, &mut cli);
    let mut offset = 0;
    loop {
        let mut data = core.message(id, MessageKind::Data).unwrap();
        data.data = Some(Data {
            offset,
            payload: vec![7; 65536],
        });
        match core.send("a", &data) {
            Ok(()) => {
                offset += 65536;
            }
            Err(Error::WouldBlock) => {
                break;
            }
            other => panic!("unexpected {other:?}"),
        }
        assert!(offset < 4 * 1024 * 1024);
    }
    assert!(offset > 0);
    let mut window = core.message(id, MessageKind::Window).unwrap();
    window.window = Some(Window { max_offset: 1 });
    core.send("a", &window).expect("reserved control progress");
    let mut seen_window = false;
    while let Some((_, message)) = core.next_message() {
        seen_window |= message.kind == MessageKind::Window;
    }
    assert!(seen_window);
    assert_eq!(core.queued_bytes().0, 0);
}

mod support;
#[test]
fn randomized_chunks_reads_and_mux_routing_reassemble_exact_bytes() {
    use gwz_transport::stream::{
        Config as StreamConfig, Error as StreamError, Side, StreamMachine,
    };
    use support::{Random, case_seed, setting};
    let run_seed = setting("GWZ_MUX_SEED").unwrap_or(0x47575a_504c414345);
    let cases = setting("GWZ_MUX_CASES").unwrap_or(100) as usize;
    let mut blocked = 0;
    for case in 0..cases {
        let seed = case_seed(run_seed, case);
        let mut random = Random(seed);
        let (mut core, mut cli) = pair();
        bind(&mut core, &mut cli);
        let id = open_stream(&mut core, &mut cli);
        let window = 1 + random.below(31);
        let mut config = StreamConfig::new("session", id, Side::Initiator);
        config.profile_version = 2;
        config.receive_window = window;
        config.peer_receive_window = window;
        config.send_buffer = window;
        config.max_payload = window;
        config.coalesce_delay_ms = 0;
        let mut sender = StreamMachine::new(config.clone()).unwrap();
        config.side = Side::Endpoint;
        let mut receiver = StreamMachine::new(config).unwrap();
        let input: Vec<_> = (0..(256 + random.below(1024)))
            .map(|_| random.next() as u8)
            .collect();
        let mut output = Vec::new();
        let mut at = 0;
        let mut ended = false;
        let mut eof = false;
        for step in 0..100_000 {
            if at < input.len() {
                let end = input.len().min(at + 1 + random.below(129));
                match sender.write(&input[at..end]) {
                    Ok(n) => {
                        at += n;
                    }
                    Err(StreamError::WouldBlock) => {
                        blocked += 1;
                    }
                    other => panic!(
                        "seed={run_seed:#x} case={case} case_seed={seed:#x} step={step}: {other:?}"
                    ),
                }
            } else if !ended {
                sender.end_write().unwrap();
                ended = true;
            }
            if random.below(3) != 0 {
                if let Some(message) = sender.next_message() {
                    core.send("a", &message).unwrap();
                }
                if let Some(message) = core.next_message() {
                    cli.receive(&message).unwrap();
                }
                if let Some((_, message)) = cli.next_action() {
                    receiver.receive(message).unwrap();
                }
            }
            if random.below(3) != 0 {
                let mut buffer = vec![0; 1 + random.below(71)];
                match receiver.read(&mut buffer) {
                    Ok(0) => {
                        eof = true;
                    }
                    Ok(n) => {
                        output.extend_from_slice(&buffer[..n]);
                    }
                    Err(StreamError::WouldBlock) => {}
                    other => panic!("read {other:?}"),
                }
            }
            if let Some(message) = receiver.next_message() {
                cli.send("a", &message).unwrap();
            }
            if let Some(message) = cli.next_message() {
                core.receive(&message).unwrap();
            }
            if let Some((_, message)) = core.next_action() {
                sender.receive(message).unwrap();
            }
            sender.advance(step);
            receiver.advance(step);
            if eof {
                break;
            }
        }
        assert!(
            eof,
            "replay GWZ_MUX_SEED={run_seed:#x} GWZ_MUX_CASES={} (case_seed={seed:#x})",
            case + 1
        );
        assert_eq!(
            output, input,
            "run={run_seed:#x} case={case} case_seed={seed:#x}"
        );
    }
    assert!(blocked > 0, "campaign must exercise backpressure");
}

#[test]
fn opened_cannot_change_the_bound_trust_owner() {
    let (mut core, mut cli) = pair();
    bind(&mut core, &mut cli);
    let id = begin_stream(&mut core, &mut cli);
    let mut opened = cli.message(id, MessageKind::Opened).unwrap();
    opened.opened = Some(Opened {
        endpoint_id: "endpoint".into(),
        connection_id: "connection".into(),
        trust_owner: "different-account".into(),
        reused: false,
        facts: Facts::default(),
        receive_limits: binding::default_limits(),
    });
    assert_eq!(cli.send("a", &opened), Err(Error::Protocol));
    assert_eq!(core.receive(&("a".into(), opened)), Err(Error::Protocol));
    assert_eq!(core.phase(), Phase::Closed);
}

#[test]
fn cancellation_control_progresses_while_other_requests_keep_bulk_queue_busy() {
    let (mut core, mut cli) = pair();
    bind(&mut core, &mut cli);
    let bulk_id = open_stream(&mut core, &mut cli);
    register(&mut core, &mut cli, "b");
    let check_id = check(&mut core, "b");
    cli.receive(&core.next_message().unwrap()).unwrap();
    let mut data = core.message(bulk_id, MessageKind::Data).unwrap();
    data.data = Some(Data {
        offset: 0,
        payload: vec![1; 65536],
    });
    core.send("a", &data).unwrap();
    core.cancel("b").unwrap();
    let cancellation = core.next_message().unwrap();
    assert_eq!(cancellation.1.kind, MessageKind::Cancel);
    assert_eq!(cancellation.1.stream_id, check_id);
    assert_eq!(core.next_message().unwrap().1.kind, MessageKind::Data);
}

fn queued_terminal(kind: MessageKind) -> (Mux, Mux, gwz_transport::mux::Attachment) {
    let (mut core, mut cli) = pair();
    bind(&mut core, &mut cli);
    let id = match kind {
        MessageKind::IdentityChecked | MessageKind::IdentityCheckFailed => {
            let id = check(&mut core, "a");
            cli.receive(&core.next_message().unwrap()).unwrap();
            cli.next_action();
            id
        }
        MessageKind::OpenFailed => begin_stream(&mut core, &mut cli),
        _ => open_stream(&mut core, &mut cli),
    };
    let facts = Facts {
        method: AuthMethod::SshKey,
        credential_offered: true,
        authenticated: Some(true),
        ssh_exit_status: Some(128),
        ..Default::default()
    };
    let failure = Failure {
        setup_cause: None,
        code: ErrorCode::RepositoryRefused,
        effect: Effect::None,
        facts: Some(facts.clone()),
    };
    let mut message = cli.message(id, kind).unwrap();
    match kind {
        MessageKind::IdentityChecked => message.identity_checked = Some(IdentityChecked {}),
        MessageKind::IdentityCheckFailed => {
            message.identity_check_failed = Some(Failure {
                setup_cause: None,
                code: ErrorCode::Authentication,
                effect: Effect::None,
                facts: None,
            })
        }
        MessageKind::OpenFailed => message.open_failed = Some(failure),
        MessageKind::Failed => message.failed = Some(failure),
        MessageKind::Closed => {
            message.closed = Some(Closed {
                disposition: Disposition::Discarded,
                unread_response_discarded: false,
                facts,
                failure: Some(Failure {
                    setup_cause: None,
                    facts: None,
                    ..failure
                }),
            })
        }
        _ => panic!("terminal family"),
    }
    cli.send("a", &message).unwrap();
    (core, cli, ("a".into(), message))
}
const TERMINALS: [MessageKind; 5] = [
    MessageKind::OpenFailed,
    MessageKind::IdentityChecked,
    MessageKind::IdentityCheckFailed,
    MessageKind::Failed,
    MessageKind::Closed,
];
#[test]
fn finish_retains_every_terminal_until_direct_handoff_with_facts() {
    for kind in TERMINALS {
        let (mut core, mut cli, expected) = queued_terminal(kind);
        assert_eq!(cli.finish("a"), Err(Error::WouldBlock), "{kind:?}");
        cli.cancel("a").unwrap();
        assert_eq!(cli.finish("a"), Err(Error::WouldBlock));
        let terminal = cli.next_message().expect("owned terminal survives sealing");
        assert_eq!(terminal, expected);
        cli.finish("a").unwrap();
        core.receive(&terminal).unwrap();
        assert_eq!(core.active_streams(), 0);
        assert_eq!(core.next_action(), Some(expected));
        core.finish("a").unwrap();
    }
}
#[test]
fn finish_retains_every_terminal_until_async_handoff_with_facts() {
    use gwz_transport::mux::Owner;
    use std::{
        future::Future,
        pin::pin,
        task::{Context, Poll, Waker},
    };
    for kind in TERMINALS {
        let (core, cli, expected) = queued_terminal(kind);
        let (core, core_port) = Owner::new(core);
        let (cli, cli_port) = Owner::new(cli);
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(cli.finish("a"), Err(Error::WouldBlock), "{kind:?}");
        let Poll::Ready(Ok(Some(terminal))) = pin!(cli_port.next_message()).poll(&mut cx) else {
            panic!("terminal ready")
        };
        assert_eq!(terminal, expected);
        cli.finish("a").unwrap();
        assert_eq!(
            pin!(core_port.deliver(terminal)).poll(&mut cx),
            Poll::Ready(Ok(()))
        );
        assert_eq!(
            pin!(core.next_action()).poll(&mut cx),
            Poll::Ready(Ok(Some(expected)))
        );
        core.finish("a").unwrap();
    }
}
#[test]
fn stalled_terminal_handoff_expires_without_renewing_cleanup_deadline() {
    for kind in TERMINALS {
        let (mut core, mut cli, _) = queued_terminal(kind);
        assert_eq!(cli.finish("a"), Err(Error::WouldBlock));
        cli.advance(4999);
        assert_eq!(cli.finish("a"), Err(Error::WouldBlock));
        cli.advance(5000);
        assert_eq!(cli.phase(), Phase::Closed);
        // Host propagation releases the peer; never assert peer cleanup.
        core.disconnect();
        assert_eq!(core.finish("a"), Err(Error::Closed));
    }
}
#[test]
fn terminal_handoff_keeps_control_capacity_with_another_requests_bulk_queue() {
    let (mut core, mut cli) = pair();
    bind(&mut core, &mut cli);
    let bulk = open_stream(&mut core, &mut cli);
    register(&mut core, &mut cli, "b");
    let id = check(&mut core, "b");
    cli.receive(&core.next_message().unwrap()).unwrap();
    cli.next_action();
    let mut offset = 0;
    loop {
        let mut frame = cli.message(bulk, MessageKind::Data).unwrap();
        frame.data = Some(Data {
            offset,
            payload: vec![5; 65536],
        });
        match cli.send("a", &frame) {
            Ok(()) => offset += 65536,
            Err(Error::WouldBlock) => break,
            other => panic!("unexpected {other:?}"),
        }
    }
    let mut terminal = cli.message(id, MessageKind::IdentityChecked).unwrap();
    terminal.identity_checked = Some(IdentityChecked {});
    cli.send("b", &terminal).unwrap();
    assert_eq!(cli.finish("b"), Err(Error::WouldBlock));
    cli.cancel("b").unwrap();
    let mut delivered = false;
    for _ in 0..64 {
        let Some(item) = cli.next_message() else {
            break;
        };
        core.receive(&item).unwrap();
        core.next_action();
        if item.1.stream_id == id {
            assert_eq!(item.1, terminal);
            delivered = true;
            break;
        }
    }
    assert!(delivered);
    cli.finish("b").unwrap();
    assert_eq!(core.active_streams(), 1); // only unrelated bulk stream remains
}
#[test]
fn dropping_port_with_a_pending_terminal_closes_generation_and_peer_on_propagation() {
    use gwz_transport::mux::Owner;
    use std::{
        future::Future,
        pin::pin,
        task::{Context, Poll, Waker},
    };
    let (core, cli, _) = queued_terminal(MessageKind::OpenFailed);
    let (core, core_port) = Owner::new(core);
    let (cli, cli_port) = Owner::new(cli);
    assert_eq!(cli.finish("a"), Err(Error::WouldBlock));
    drop(cli_port);
    assert_eq!(cli.phase(), Phase::Closed);
    core_port.disconnect();
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(
        pin!(core.next_action()).poll(&mut cx),
        Poll::Ready(Ok(None))
    );
    assert_eq!(core.finish("a"), Err(Error::Closed));
}

#[test]
fn incompatible_binding_limits_send_the_exact_typed_rejection() {
    let (mut core, _) = pair();
    let mut limits = binding::default_limits();
    limits.metadata_bytes = 128;
    let mut cli = Mux::endpoint(
        "session",
        binding::EndpointConfig {
            endpoint_id: "endpoint".into(),
            role: EndpointRole::Driver,
            schemes: vec![Scheme::Ssh],
            policies: vec![AuthPolicy::SshAmbient],
            limits,
            trust_owner: "account".into(),
        },
        Config::default(),
    )
    .unwrap();
    register(&mut core, &mut cli, "a");
    core.begin("a").unwrap();
    assert_eq!(cli.receive(&core.next_message().unwrap()), Ok(()));
    assert_eq!(cli.active_streams(), 0);
    assert!(cli.binding().is_none());
    let terminal = cli
        .next_message()
        .expect("typed rejection before retirement");
    assert_eq!(terminal.1.kind, MessageKind::BindRejected);
    assert_eq!(
        terminal.1.bind_rejected,
        Some(Failure {
            setup_cause: None,
            code: ErrorCode::UnsupportedOperation,
            effect: Effect::None,
            facts: None,
        })
    );
    core.receive(&terminal).unwrap();
    assert_eq!(core.bootstrap_failure(), terminal.1.bind_rejected.as_ref());
    assert_eq!(core.phase(), Phase::Closed);
    assert!(core.binding().is_none());
    assert!(core.next_action().is_none());
}

#[test]
fn finish_preserves_timeout_terminal_and_peer_terminal_cleanup_action() {
    let (mut core, mut cli) = pair();
    bind(&mut core, &mut cli);
    check(&mut core, "a");
    cli.receive(&core.next_message().unwrap()).unwrap();
    cli.next_action();
    cli.advance(100);
    assert_eq!(cli.finish("a"), Err(Error::WouldBlock));
    let terminal = cli.next_message().unwrap();
    assert_eq!(
        terminal.1.identity_check_failed.as_ref().unwrap().code,
        ErrorCode::Timeout
    );
    core.receive(&terminal).unwrap();
    core.next_action();
    cli.finish("a").unwrap();
    core.finish("a").unwrap();

    let (mut core, mut cli) = pair();
    bind(&mut core, &mut cli);
    let id = open_stream(&mut core, &mut cli);
    let mut failed = core.message(id, MessageKind::Failed).unwrap();
    failed.failed = Some(Failure {
        setup_cause: None,
        code: ErrorCode::Io,
        effect: Effect::Possible,
        facts: None,
    });
    core.send("a", &failed).unwrap();
    cli.receive(&core.next_message().unwrap()).unwrap();
    assert_eq!(cli.finish("a"), Err(Error::WouldBlock));
    assert_eq!(cli.next_action(), Some(("a".into(), failed)));
    cli.finish("a").unwrap();
}

#[test]
fn async_bootstrap_rejection_retains_typed_reason_after_both_ports_retire() {
    use gwz_transport::mux::Owner;
    use std::{
        future::Future,
        pin::pin,
        task::{Context, Poll, Waker},
    };
    for code in [
        ErrorCode::UnsupportedVersion,
        ErrorCode::UnsupportedOperation,
    ] {
        let (mut core, mut cli) = pair();
        register(&mut core, &mut cli, "a");
        core.begin("a").unwrap();
        let mut request = core.next_message().unwrap();
        if code == ErrorCode::UnsupportedVersion {
            request.1.bind.as_mut().unwrap().versions = vec![1];
        } else {
            request
                .1
                .bind
                .as_mut()
                .unwrap()
                .receive_limits
                .metadata_bytes = 128;
        }
        cli.receive(&request).unwrap();
        assert_eq!(cli.phase(), Phase::Rejecting);
        assert!(cli.binding().is_none());
        assert_eq!(cli.register("new", None), Err(Error::Rejected));
        assert_eq!(cli.finish("a"), Err(Error::WouldBlock));
        let (core, core_port) = Owner::new(core);
        let (cli, cli_port) = Owner::new(cli);
        let mut cx = Context::from_waker(Waker::noop());
        let Poll::Ready(Ok(Some(frame))) = pin!(cli_port.next_message()).poll(&mut cx) else {
            panic!("rejection handoff")
        };
        assert_eq!(
            pin!(core_port.deliver(frame)).poll(&mut cx),
            Poll::Ready(Ok(()))
        );
        let expected = Some(Failure {
            setup_cause: None,
            code,
            effect: Effect::None,
            facts: None,
        });
        assert_eq!(core.bootstrap_failure(), expected);
        assert_eq!(cli.bootstrap_failure(), expected);
        assert_eq!(
            pin!(core.ready()).poll(&mut cx),
            Poll::Ready(Err(Error::Rejected))
        );
        assert_eq!(
            pin!(cli.ready()).poll(&mut cx),
            Poll::Ready(Err(Error::Rejected))
        );
        assert_eq!(
            pin!(cli_port.next_message()).poll(&mut cx),
            Poll::Ready(Ok(None))
        );
        core_port.disconnect();
        assert_eq!(core.bootstrap_failure(), expected); // no late closure erases first outcome
    }
}

#[test]
fn malformed_bootstrap_closes_without_claiming_a_negotiation_rejection() {
    let (mut core, mut cli) = pair();
    register(&mut core, &mut cli, "a");
    core.begin("a").unwrap();
    let mut invalid = core.next_message().unwrap();
    invalid
        .1
        .bind
        .as_mut()
        .unwrap()
        .receive_limits
        .queued_frames = 0;
    assert_eq!(cli.receive(&invalid), Err(Error::Protocol));
    assert_eq!(cli.phase(), Phase::Closed);
    assert!(cli.bootstrap_failure().is_none());
    assert!(cli.next_message().is_none());
    assert!(cli.next_action().is_none());
}

#[test]
fn stalled_bootstrap_rejection_handoff_has_a_bounded_deadline() {
    let (_, mut cli) = pair();
    cli.register("a", None).unwrap();
    let offer = binding::offer("session", EndpointRole::Driver);
    cli.receive(&("a".into(), offer)).unwrap();
    assert_eq!(cli.phase(), Phase::Rejecting);
    cli.advance(5000);
    assert_eq!(cli.phase(), Phase::Closed);
    assert_eq!(
        cli.bootstrap_failure().unwrap().code,
        ErrorCode::UnsupportedVersion
    );
    assert!(cli.next_message().is_none());
}

#[test]
fn bootstrap_rejection_rejects_operation_errors_without_retaining_them() {
    for code in [ErrorCode::Authentication, ErrorCode::Io] {
        let (mut core, mut cli) = pair();
        register(&mut core, &mut cli, "a");
        core.begin("a").unwrap();
        core.next_message();
        let reply = Envelope {
            version: 1,
            session_id: "session".into(),
            kind: MessageKind::BindRejected,
            bind_rejected: Some(Failure {
                setup_cause: None,
                code,
                effect: Effect::None,
                facts: None,
            }),
            ..Default::default()
        };
        assert_eq!(core.receive(&("a".into(), reply)), Err(Error::Protocol));
        assert_eq!(core.phase(), Phase::Closed);
        assert!(core.bootstrap_failure().is_none());
        assert!(core.binding().is_none());
        assert!(core.next_action().is_none());
    }
}

#[test]
fn bootstrap_rejection_codec_enforces_its_entire_failure_domain() {
    use gwz_transport::{cbor, codec};
    for code in 1..=13 {
        let code = ErrorCode::from_wire(code).unwrap();
        for effect in [Effect::None, Effect::Possible] {
            for facts in [None, Some(Facts::default())] {
                let reply = Envelope {
                    version: 1,
                    session_id: "session".into(),
                    kind: MessageKind::BindRejected,
                    bind_rejected: Some(Failure {
                        setup_cause: None,
                        code,
                        effect,
                        facts: facts.clone(),
                    }),
                    ..Default::default()
                };
                let allowed = matches!(
                    code,
                    ErrorCode::UnsupportedVersion | ErrorCode::UnsupportedOperation
                ) && effect == Effect::None
                    && facts.is_none();
                assert_eq!(
                    codec::admit(&reply).is_ok(),
                    allowed,
                    "{code:?}/{effect:?}/{facts:?}"
                );
                // Encode through the raw generated projection to exercise hostile bytes.
                let bytes = cbor::encode(&reply.to_cbor());
                assert_eq!(codec::decode(&bytes).is_ok(), allowed);
                assert_eq!(codec::encode(&reply).is_ok(), allowed);
            }
        }
    }
}

#[test]
fn invalid_endpoint_configuration_fails_locally_before_bootstrap() {
    for invalid in 0..4 {
        let mut config = binding::EndpointConfig {
            endpoint_id: "endpoint".into(),
            role: EndpointRole::Driver,
            schemes: vec![Scheme::Ssh],
            policies: vec![AuthPolicy::SshAmbient],
            limits: binding::default_limits(),
            trust_owner: "account".into(),
        };
        match invalid {
            0 => {
                config.limits.queued_frames = 0;
            }
            1 => {
                config.endpoint_id.clear();
            }
            2 => {
                config.trust_owner.clear();
            }
            _ => {
                config.policies.clear();
            }
        }
        assert!(matches!(
            Mux::endpoint("session", config, Config::default()),
            Err(Error::InvalidRequest)
        ));
    }
}
