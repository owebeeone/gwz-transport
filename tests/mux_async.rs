use gwz_transport::{
    binding,
    mux::{Config, Mux, Owner, Phase},
    protocol::*,
};
use std::{
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
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
