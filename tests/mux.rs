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
