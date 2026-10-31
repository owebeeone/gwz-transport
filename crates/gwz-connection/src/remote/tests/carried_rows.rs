//! Rows for the three 2.2c P3s carried into step 2.3 (plan step 2.3b): a registration wakes every future waiting on it
//! (State P3-5); one config fixes the core's streams per binding and the client's ceiling (Code P3-7); an open right after
//! an abort at the stream bound waits and never answers `Fault(Capacity)` (Code P3-8).
use super::{
    super::{BindBounds, RemoteConfig, ServiceClass},
    core_rows::{FETCH, bound, open, register},
    world::{Count, World},
};
use crate::{
    OpenError, OpenFailure, Registration,
    fakes::{exchange_spec, poll_once},
};
use std::task::Poll;

// --- State P3-5: per-future waiter slots ---

#[test]
fn seventeen_futures_waiting_on_one_registration_are_each_woken_once() {
    let w = World::new();
    w.call("call-1", ServiceClass::Fetch, FETCH, &[]);
    let op = register(&w, "call-1");
    assert_eq!(op.registration(), Registration::Pending);
    let mut futures: Vec<_> = (0..17).map(|_| Box::pin(op.registered())).collect();
    let tasks: Vec<_> = (0..17).map(|_| Count::new()).collect();
    for (task, future) in tasks.iter().zip(futures.iter_mut()) {
        assert!(task.poll(future).is_pending());
    }
    w.settle();
    let wakes: Vec<usize> = tasks.iter().map(|task| task.wakes()).collect();
    assert_eq!(wakes, vec![1; 17], "wakes per waiter");
    for (task, future) in tasks.iter().zip(futures.iter_mut()) {
        assert!(matches!(
            task.poll(future),
            Poll::Ready(Registration::Registered(_))
        ));
    }
}

#[test]
fn a_dropped_future_leaves_no_waiter_behind() {
    let w = World::new();
    w.call("call-1", ServiceClass::Fetch, FETCH, &[]);
    let op = register(&w, "call-1");
    for _ in 0..40 {
        let mut registered = Box::pin(op.registered());
        assert!(Count::new().poll(&mut registered).is_pending());
        drop(registered);
    }
    assert_eq!(w.remote.with(|state| state.waiters.len()), 0);
}

// --- Code P3-7: one config, and the client's ceiling ---

fn wide() -> RemoteConfig {
    RemoteConfig {
        streams_per_binding: 128,
        ..Default::default()
    }
}

#[test]
fn the_core_bounds_never_exceed_what_the_client_carrier_holds_a_binding_to() {
    let client = BindBounds::client();
    for streams in [0, 1, 63, 64, 65, 128, 1024, 5000] {
        let core = BindBounds::core(&RemoteConfig {
            streams_per_binding: streams,
            ..Default::default()
        });
        assert!(
            core.streams <= client.streams,
            "streams_per_binding {streams} gives a core bound of {} against the client's {}",
            core.streams,
            client.streams
        );
    }
    assert_eq!(BindBounds::core(&wide()), client, "the ceiling is pinned");
    assert_eq!(client.streams, 64);
}

#[test]
fn a_core_at_one_hundred_and_twenty_eight_streams_opens_no_more_than_the_client_holds() {
    let w = World::with(wide());
    let op = bound(&w, "call-1");
    // The client lags: nothing is settled after the opens, so every open the core queued is still in flight.
    let mut kept = Vec::new();
    for _ in 0..64 {
        kept.push(
            op.open(exchange_spec())
                .expect("within the client's ceiling"),
        );
    }
    assert_eq!(
        op.open(exchange_spec()).map(drop),
        Err(OpenError::BindingLimit),
        "the sixty-fifth open is refused at once, as a local host refuses a full binding"
    );
    assert!(
        w.remote.streams_held() <= 64,
        "the core's mux holds {} streams",
        w.remote.streams_held()
    );
    drop(kept);
}

// --- Code P3-8: an open after an abort at the stream bound waits ---

#[test]
fn an_open_right_after_an_abort_at_the_stream_bound_waits_and_never_faults() {
    let w = World::with(RemoteConfig {
        streams_per_binding: 2,
        ..Default::default()
    });
    let op = bound(&w, "call-1");
    let aborted = open(&w, &op, exchange_spec());
    let _kept = open(&w, &op, exchange_spec());
    aborted.abort();
    // The aborted stream's route stays in the core's mux until the client's terminal returns.
    let mut wait = Box::pin(op.open(exchange_spec()).expect("a slot is free").wait());
    match poll_once(&mut wait) {
        Poll::Pending => {}
        Poll::Ready(Err(OpenFailure::Fault(code))) => {
            panic!("an open at the bound answered Fault({code:?})")
        }
        Poll::Ready(other) => panic!(
            "the open resolved before a route retired: {:?}",
            other.err()
        ),
    }
    w.settle();
    match poll_once(&mut wait) {
        Poll::Ready(Ok(_)) => {}
        other => panic!(
            "the open did not succeed once the route retired: {:?}",
            other.map(|r| r.err())
        ),
    }
}
