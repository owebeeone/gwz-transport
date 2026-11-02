//! The binding's connection affinity (HTTPS model step H2b, rows H2b-1 to H2b-9; GwzHttpsAuthModel-DRAFT.md r5
//! section 3.1, Consistency P3-4): an exchange whose connection is authenticated by a mechanism that lives on it
//! (`ExchangeIo::connection_bound`) is noted under its key and path when its first response byte is delivered (and
//! once more when it finishes), and the binding's next exchange of that key and path prefers that connection, waiting
//! for it only while the instance settles the exchange that finished on it. Against fakes only.
use crate::fakes::{Rig, auth, exchange_spec, now, prepared_with};
use crate::{ExchangeIo, ExchangeSpec, GitService, HostHandle, HostLimits, HostOwner, Operation};
use gwz_transport::pool::{ConnectionId, Identity, Key};
use gwz_transport::protocol::Disposition;
use std::{
    io,
    sync::atomic::Ordering,
    task::{Context, Poll},
};

/// A host whose bindings may register more requests than the rows below open.
fn host(rig: &Rig) -> (HostOwner, HostHandle) {
    let limits = HostLimits {
        binding_registrations: 1024,
        ..HostLimits::default()
    };
    crate::build(rig.config(limits), rig.ports(1)).unwrap()
}
fn registered(handle: &HostHandle, name: &str) -> Operation {
    now(handle
        .admit(prepared_with(name, 1, auth(1, "a")))
        .unwrap()
        .acquire())
    .unwrap()
    .register()
}
fn spec(service: GitService, path: &str) -> ExchangeSpec {
    ExchangeSpec::git(
        Key::https("example.invalid", 443),
        Identity::Https,
        service,
        path,
    )
    .unwrap()
}
fn advertisement(path: &str) -> ExchangeSpec {
    spec(GitService::UploadPackAdvertisement, path)
}
fn post(path: &str) -> ExchangeSpec {
    spec(GitService::UploadPackExchange, path)
}
/// The connection the `n`th exchange of the world ran on.
fn connection(rig: &Rig, n: usize) -> ConnectionId {
    rig.world.exchange_connections.lock().unwrap()[n]
}
fn exchanges(rig: &Rig) -> usize {
    rig.world.exchanges.load(Ordering::SeqCst)
}
/// The peer of the `n`th exchange answers one byte, `bound` or not, and the caller reads it.
fn first_byte(rig: &Rig, exchange: &mut crate::Exchange, n: usize, bound: bool) {
    let peer = rig.world.peer(n);
    peer.set_bound(bound);
    peer.feed(b"x");
    let mut out = [0u8; 1];
    assert_eq!(now(exchange.stream().read(&mut out)).unwrap(), 1);
}
/// Two connections of `operation`: an exchange of another path holds the lower one while an advertisement of `path`
/// is opened on the higher one, then ends, so the lower one is idle. The advertisement is returned with its index.
fn advertisement_on_the_higher(
    rig: &Rig,
    operation: &Operation,
    path: &str,
) -> (crate::Exchange, usize, ConnectionId, ConnectionId) {
    let other = now(operation.open(post("/other")).unwrap().wait()).unwrap();
    let low = connection(rig, exchanges(rig) - 1);
    let get = now(operation.open(advertisement(path)).unwrap().wait()).unwrap();
    let n = exchanges(rig) - 1;
    let high = connection(rig, n);
    assert_ne!(low, high);
    assert_eq!(now(other.finish()).disposition, Disposition::Reusable);
    (get, n, low, high)
}

#[test]
fn h2b_1_the_affinity_is_stored_at_the_first_delivered_byte_while_the_exchange_is_open() {
    let rig = Rig::new();
    let (_owner, handle) = host(&rig);
    let a = registered(&handle, "op-a");
    let mut get = now(a.open(advertisement("/r")).unwrap().wait()).unwrap();
    assert_eq!(a.affinity_for_test(&post("/r")), (None, 0));
    first_byte(&rig, &mut get, 0, true);
    // Still open: the next exchange of the key and path can read the hint now.
    assert_eq!(
        a.affinity_for_test(&post("/r")),
        (Some(connection(&rig, 0)), 1)
    );
    assert_eq!(a.affinity_for_test(&post("/s")).0, None, "path precision");
    now(get.finish());
    // A response without a body is sampled once more when it finishes.
    let get = now(a.open(advertisement("/empty")).unwrap().wait()).unwrap();
    rig.world.peer(1).set_bound(true);
    now(get.finish());
    assert_eq!(
        a.affinity_for_test(&post("/empty")),
        (Some(connection(&rig, 1)), 2)
    );
    now(a.finish());
}

#[test]
fn h2b_2_a_post_opened_as_its_advertisement_finishes_lands_on_that_connection_100_of_100() {
    let rig = Rig::new();
    let (_owner, handle) = host(&rig);
    let a = registered(&handle, "op-a");
    let mut landed = 0;
    for i in 0..100 {
        let path = format!("/r{i}");
        let (mut get, n, _low, high) = advertisement_on_the_higher(&rig, &a, &path);
        first_byte(&rig, &mut get, n, true);
        // The advertisement finishes and its lease is not yet released when the POST opens.
        rig.world.confirmed.store(false, Ordering::SeqCst);
        rig.world.hold_finish.store(true, Ordering::SeqCst);
        let get_end = get.finish();
        let wait = a.open(post(&path)).unwrap().wait();
        assert!(
            !wait.is_ready(),
            "iteration {i}: the POST waits for the finishing connection, not the idle lower one"
        );
        rig.world.confirm_finishes();
        rig.progress.run();
        assert!(
            wait.is_ready(),
            "iteration {i}: the settled lease serves it"
        );
        let exchange = now(wait).unwrap();
        if connection(&rig, exchanges(&rig) - 1) == high {
            landed += 1;
        }
        assert_eq!(now(get_end).disposition, Disposition::Reusable);
        rig.world.hold_finish.store(false, Ordering::SeqCst);
        now(exchange.finish());
    }
    assert_eq!(landed, 100);
    now(a.finish());
}

#[test]
fn h2b_3_a_connection_busy_with_a_reading_exchange_is_not_waited_for() {
    let rig = Rig::new();
    let (_owner, handle) = host(&rig);
    let a = registered(&handle, "op-a");
    let (mut get, n, low, high) = advertisement_on_the_higher(&rig, &a, "/r");
    first_byte(&rig, &mut get, n, true);
    assert_eq!(a.affinity_for_test(&post("/r")).0, Some(high));
    // The advertisement is still being read: its connection is leased, not finishing.
    let wait = a.open(post("/r")).unwrap().wait();
    assert!(wait.is_ready(), "no wait for a reading exchange");
    let exchange = now(wait).unwrap();
    assert_eq!(connection(&rig, exchanges(&rig) - 1), low);
    now(exchange.finish());
    now(get.finish());
    now(a.finish());
}

#[test]
fn h2b_4_another_binding_never_gets_the_hint() {
    let rig = Rig::new();
    let (_owner, handle) = host(&rig);
    let a = registered(&handle, "op-a");
    let b = registered(&handle, "op-b");
    let (mut get, n, _low, high) = advertisement_on_the_higher(&rig, &a, "/r");
    first_byte(&rig, &mut get, n, true);
    rig.world.confirmed.store(false, Ordering::SeqCst);
    rig.world.hold_finish.store(true, Ordering::SeqCst);
    let get_end = get.finish();
    assert_eq!(b.affinity_for_test(&post("/r")), (None, 0));
    // B's POST of the same key and path neither waits for A's finishing connection nor lands on it.
    let wait = b.open(post("/r")).unwrap().wait();
    assert!(wait.is_ready(), "B does not wait on A's hint");
    let exchange = now(wait).unwrap();
    assert_ne!(connection(&rig, exchanges(&rig) - 1), high);
    rig.world.confirm_finishes();
    rig.progress.run();
    now(get_end);
    rig.world.hold_finish.store(false, Ordering::SeqCst);
    now(exchange.finish());
    assert_eq!(b.affinity_for_test(&post("/r")), (None, 0));
    now(b.finish());
    now(a.finish());
}

#[test]
fn h2b_5_the_257th_entry_evicts_the_oldest() {
    let rig = Rig::new();
    let (_owner, handle) = host(&rig);
    let a = registered(&handle, "op-a");
    for i in 0..257 {
        let mut get = now(a.open(advertisement(&format!("/p{i}"))).unwrap().wait()).unwrap();
        first_byte(&rig, &mut get, i, true);
        now(get.finish());
    }
    assert_eq!(a.affinity_for_test(&post("/p0")), (None, 256));
    assert!(a.affinity_for_test(&post("/p1")).0.is_some());
    assert!(a.affinity_for_test(&post("/p256")).0.is_some());
    now(a.finish());
}

#[test]
fn h2b_6_a_path_over_1_kib_is_not_recorded() {
    let rig = Rig::new();
    let (_owner, handle) = host(&rig);
    let a = registered(&handle, "op-a");
    let long = format!("/{}", "a".repeat(1024));
    let edge = format!("/{}", "a".repeat(1023));
    for (n, path) in [&long, &edge].into_iter().enumerate() {
        let mut get = now(a.open(advertisement(path)).unwrap().wait()).unwrap();
        first_byte(&rig, &mut get, n, true);
        now(get.finish());
    }
    assert_eq!(a.affinity_for_test(&post(&long)).0, None, "1025 bytes");
    assert_eq!(
        a.affinity_for_test(&post(&edge)),
        (Some(connection(&rig, 1)), 1),
        "1024 bytes"
    );
    now(a.finish());
}

#[test]
fn h2b_7_a_closed_connections_hint_is_ignored() {
    let rig = Rig::new();
    let (_owner, handle) = host(&rig);
    let a = registered(&handle, "op-a");
    let (mut get, n, low, high) = advertisement_on_the_higher(&rig, &a, "/r");
    first_byte(&rig, &mut get, n, true);
    rig.world.discard_next.store(true, Ordering::SeqCst);
    assert_eq!(now(get.finish()).disposition, Disposition::Discarded);
    assert_eq!(a.affinity_for_test(&post("/r")).0, Some(high), "stale");
    let wait = a.open(post("/r")).unwrap().wait();
    assert!(wait.is_ready(), "a closed connection is not waited for");
    let exchange = now(wait).unwrap();
    assert_eq!(connection(&rig, exchanges(&rig) - 1), low);
    now(exchange.finish());
    // The same when it closes while the POST waits for it: the POST is served once its lease ends.
    let mut get = now(a.open(advertisement("/s")).unwrap().wait()).unwrap();
    let n = exchanges(&rig) - 1;
    first_byte(&rig, &mut get, n, true);
    let other = now(a.open(post("/other")).unwrap().wait()).unwrap();
    let held = connection(&rig, exchanges(&rig) - 1);
    rig.world.confirmed.store(false, Ordering::SeqCst);
    rig.world.hold_finish.store(true, Ordering::SeqCst);
    rig.world.discard_next.store(true, Ordering::SeqCst);
    let get_end = get.finish();
    let wait = a.open(post("/s")).unwrap().wait();
    assert!(!wait.is_ready(), "it waits for the finishing connection");
    rig.world.confirm_finishes();
    rig.progress.run();
    assert_eq!(now(get_end).disposition, Disposition::Discarded);
    assert!(
        wait.is_ready(),
        "the lease ended: it decides as if none were named"
    );
    let exchange = now(wait).unwrap();
    assert_ne!(connection(&rig, exchanges(&rig) - 1), connection(&rig, n));
    assert_ne!(connection(&rig, exchanges(&rig) - 1), held);
    rig.world.hold_finish.store(false, Ordering::SeqCst);
    now(exchange.finish());
    now(other.finish());
    now(a.finish());
}

#[test]
fn h2b_8_an_exchange_that_is_not_connection_bound_records_nothing() {
    let rig = Rig::new();
    let (_owner, handle) = host(&rig);
    let a = registered(&handle, "op-a");
    let mut get = now(a.open(advertisement("/r")).unwrap().wait()).unwrap();
    first_byte(&rig, &mut get, 0, false);
    now(get.finish());
    assert_eq!(a.affinity_for_test(&post("/r")), (None, 0));
    now(a.finish());
}

/// An exchange that overrides nothing it need not.
struct Plain;
impl ExchangeIo for Plain {
    fn poll_read(&mut self, _cx: &mut Context<'_>, _out: &mut [u8]) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(0))
    }
    fn poll_write(&mut self, _cx: &mut Context<'_>, data: &[u8]) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(data.len()))
    }
    fn poll_finish(&mut self, _cx: &mut Context<'_>) -> Poll<Disposition> {
        Poll::Ready(Disposition::Reusable)
    }
    fn abort(&mut self) {}
}

#[test]
fn h2b_9_connection_bound_defaults_to_false_for_ssh_and_the_fakes() {
    assert!(!Plain.connection_bound(), "the port's default");
    let rig = Rig::new();
    let (_owner, handle) = host(&rig);
    let a = registered(&handle, "op-a");
    // An SSH exchange on a fake that never says otherwise, read and finished.
    let mut exchange = now(a.open(exchange_spec()).unwrap().wait()).unwrap();
    rig.world.peer(0).feed(b"x");
    let mut out = [0u8; 1];
    assert_eq!(now(exchange.stream().read(&mut out)).unwrap(), 1);
    now(exchange.finish());
    assert_eq!(a.affinity_for_test(&exchange_spec()), (None, 0));
    now(a.finish());
}
