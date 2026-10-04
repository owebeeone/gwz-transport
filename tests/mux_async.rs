use gwz_transport::{
    binding,
    mux::{Attachment, Config, Error, Mux, Owner, Phase, Port},
    protocol::*,
};
use std::{
    future::Future,
    pin::pin,
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
    thread,
    time::Duration,
};
fn endpoints() -> (
    (Owner, gwz_transport::mux::Port),
    (Owner, gwz_transport::mux::Port),
) {
    let core = Mux::initiator("async", Config::default()).unwrap();
    let cli = Mux::endpoint(
        "async",
        binding::EndpointConfig {
            endpoint_id: "endpoint".into(),
            role: EndpointRole::Driver,
            schemes: vec![Scheme::Ssh],
            policies: vec![AuthPolicy::SshAmbient],
            limits: binding::default_limits(),
            trust_owner: "account".into(),
        },
        Config::default(),
    )
    .unwrap();
    (Owner::new(core), Owner::new(cli))
}
#[test]
fn ports_forward_both_directions_and_pending_receive_cancellation_consumes_nothing() {
    let ((core, core_port), (cli, cli_port)) = endpoints();
    core.register("r", Some("op".into())).unwrap();
    cli.register("r", None).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    {
        let mut cancelled = pin!(core_port.next_message());
        assert!(cancelled.as_mut().poll(&mut cx).is_pending());
    }
    core.begin("r").unwrap();
    // Explicit application direction: core.next_message -> client.deliver.
    let Poll::Ready(Ok(Some(bind))) = pin!(core_port.next_message()).poll(&mut cx) else {
        panic!("Bind")
    };
    assert_eq!(
        pin!(cli_port.deliver(bind)).poll(&mut cx),
        Poll::Ready(Ok(()))
    );
    // Reverse: client.next_message -> core.deliver. String is request_id.
    let Poll::Ready(Ok(Some(bound))) = pin!(cli_port.next_message()).poll(&mut cx) else {
        panic!("Bound")
    };
    assert_eq!(bound.0, "r");
    assert_eq!(
        pin!(core_port.deliver(bound)).poll(&mut cx),
        Poll::Ready(Ok(()))
    );
    assert_eq!(pin!(core.ready()).poll(&mut cx), Poll::Ready(Ok(())));
    assert_eq!(core.phase(), Phase::Ready);
    let settings = core.binding().unwrap();
    assert_eq!(settings.endpoint_id(), "endpoint");
    assert_eq!(settings.trust_owner(), "account");
    core_port.disconnect();
    assert!(core.binding().is_none());
}
#[test]
fn last_owner_drop_closes_port_and_last_port_drop_releases_bootstrap_waiters() {
    let ((core, core_port), _) = endpoints();
    let mut cx = Context::from_waker(Waker::noop());
    let mut waiting = pin!(core_port.next_message());
    assert!(waiting.as_mut().poll(&mut cx).is_pending());
    drop(core);
    assert_eq!(waiting.as_mut().poll(&mut cx), Poll::Ready(Ok(None)));
    let ((core, port), _) = endpoints();
    core.register("r", Some("op".into())).unwrap();
    core.begin("r").unwrap();
    let mut ready = pin!(core.ready());
    assert!(ready.as_mut().poll(&mut cx).is_pending());
    drop(port);
    assert_eq!(
        ready.as_mut().poll(&mut cx),
        Poll::Ready(Err(gwz_transport::mux::Error::Closed))
    );
}

#[test]
fn waiter_capacity_closes_binding_and_releases_existing_waiters() {
    let ((core, port), _) = endpoints();
    let mut cx = Context::from_waker(Waker::noop());
    let mut waiting: Vec<_> = (0..128).map(|_| Box::pin(core.ready())).collect();
    for future in &mut waiting {
        assert!(future.as_mut().poll(&mut cx).is_pending());
    }
    assert_eq!(
        pin!(port.next_message()).poll(&mut cx),
        Poll::Ready(Err(gwz_transport::mux::Error::Capacity))
    );
    assert_eq!(core.phase(), Phase::Closed);
    for future in &mut waiting {
        assert_eq!(
            future.as_mut().poll(&mut cx),
            Poll::Ready(Err(gwz_transport::mux::Error::Closed))
        );
    }
}

/// Moves the next message `from` has ready into `to`.
fn forward(from: &Port, to: &Port) {
    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(Ok(Some(item))) = pin!(from.next_message()).poll(&mut cx) else {
        panic!("a message is ready")
    };
    assert_eq!(pin!(to.deliver(item)).poll(&mut cx), Poll::Ready(Ok(())));
}
/// Every message `port` has ready.
fn drain(port: &Port) -> Vec<Attachment> {
    let mut cx = Context::from_waker(Waker::noop());
    let mut items = Vec::new();
    loop {
        let mut next = pin!(port.next_message());
        match next.as_mut().poll(&mut cx) {
            Poll::Ready(Ok(Some(item))) => items.push(item),
            _ => return items,
        }
    }
}
/// Bound owners whose endpoint has taken one Open on request "r", and the
/// Opened that answers it.
fn opening() -> ((Owner, Port), (Owner, Port), Envelope) {
    let ((core, core_port), (cli, cli_port)) = endpoints();
    core.register("r", Some("op".into())).unwrap();
    cli.register("r", None).unwrap();
    core.begin("r").unwrap();
    forward(&core_port, &cli_port);
    forward(&cli_port, &core_port);
    let id = core
        .open(
            "r",
            Open {
                endpoint_id: "endpoint".into(),
                operation_id: "op".into(),
                destination: Destination {
                    scheme: Scheme::Ssh,
                    host: "git.example".into(),
                    port: 22,
                    path: "repo".into(),
                    ssh_username: Some("git".into()),
                    https_username: None,
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
    forward(&core_port, &cli_port);
    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(Ok(Some((_, open)))) = pin!(cli.next_action()).poll(&mut cx) else {
        panic!("Open")
    };
    assert_eq!(open.stream_id, id);
    let opened = Envelope {
        version: 2,
        session_id: "async".into(),
        stream_id: id,
        kind: MessageKind::Opened,
        opened: Some(Opened {
            endpoint_id: "endpoint".into(),
            connection_id: "connection".into(),
            trust_owner: "account".into(),
            reused: false,
            facts: Facts::default(),
            receive_limits: binding::default_limits(),
        }),
        ..Default::default()
    };
    ((core, core_port), (cli, cli_port), opened)
}
#[test]
fn send_if_refusal_queues_nothing_and_leaves_the_route_opening() {
    let ((core, core_port), (cli, cli_port), opened) = opening();
    let mut calls = 0;
    let refused = cli.send_if("r", &opened, || {
        calls += 1;
        false
    });
    assert_eq!(refused, Ok(false));
    assert_eq!(calls, 1);
    assert!(drain(&cli_port).is_empty(), "a refusal queues nothing");
    // The route still waits for its Opened: the next send is admitted, and
    // only then does the route leave Opening.
    assert_eq!(cli.send("r", &opened), Ok(()));
    forward(&cli_port, &core_port);
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(
        pin!(core.next_action()).poll(&mut cx),
        Poll::Ready(Ok(Some(("r".into(), opened.clone()))))
    );
    assert_eq!(cli.send("r", &opened), Err(Error::Protocol));
}
#[test]
fn send_if_admitted_equals_send() {
    // Each case on two identical pairs: one sends, the other sends if admitted.
    type Case = fn(&Port, &Envelope) -> (String, Envelope);
    let cases: [Case; 4] = [
        |_, opened| ("r".into(), opened.clone()),
        |_, opened| ("other".into(), opened.clone()),
        |_, opened| {
            let mut unknown = opened.clone();
            unknown.stream_id += 1;
            ("r".into(), unknown)
        },
        |port, opened| {
            port.disconnect();
            ("r".into(), opened.clone())
        },
    ];
    for case in cases {
        let (_, (plain, plain_port), opened) = opening();
        let (_, (guarded, guarded_port), _) = opening();
        let (request, message) = case(&plain_port, &opened);
        let twin = case(&guarded_port, &opened);
        assert_eq!(twin, (request.clone(), message.clone()));
        let sent = plain.send(&request, &message);
        let mut calls = 0;
        let admitted = guarded.send_if(&request, &message, || {
            calls += 1;
            true
        });
        assert_eq!(calls, 1);
        assert_eq!(
            admitted,
            sent.map(|()| true),
            "{request} {}",
            message.stream_id
        );
        assert_eq!(drain(&guarded_port), drain(&plain_port));
        assert_eq!(guarded.phase(), plain.phase());
    }
}
#[test]
fn send_if_admits_once_under_the_mux_lock() {
    let (_, (cli, cli_port), opened) = opening();
    let started = Arc::new(Barrier::new(2));
    let finished = Arc::new(AtomicBool::new(false));
    let contender = thread::spawn({
        let (other, started, finished) = (cli.clone(), started.clone(), finished.clone());
        move || {
            started.wait();
            // Every owner call takes the mux mutex.
            other.phase();
            finished.store(true, Ordering::SeqCst);
        }
    });
    let mut calls = 0;
    let admitted = cli.send_if("r", &opened, || {
        calls += 1;
        started.wait();
        thread::sleep(Duration::from_millis(50));
        // The contender still waits for the mutex this check runs under.
        !finished.load(Ordering::SeqCst)
    });
    contender.join().unwrap();
    assert_eq!(calls, 1);
    assert_eq!(
        admitted,
        Ok(true),
        "another owner call ran while admit held"
    );
    assert!(finished.load(Ordering::SeqCst));
    // The send it admitted queued the message.
    assert_eq!(drain(&cli_port), vec![("r".into(), opened)]);
}
