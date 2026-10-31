//! A connection's lifetime in the host (Phase 1 remediation R1b; contract sections 5 and 10): its maximum age, the
//! suspend epoch, and receipts that state a disposal the adapter has not confirmed. Fakes only.
use crate::fakes::{
    FakeFactory, FixedSeed, ManualClock, Rig, exchange_spec, now, poll_once, prepared,
};
use crate::{
    AdmissionPermit, CleanupStatus, Clock, HostLimits, HostPorts, Operation, Sleep, Tick,
    TicketStatus,
};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// The contract's maximum connection age and the pool's idle expiry, in the instance's pool.
const MAX_AGE_MS: u64 = 600_000;
const CLEANUP_MS: u64 = 5_000;

fn registered(handle: &crate::HostHandle, name: &str) -> Operation {
    let permit: AdmissionPermit = now(handle.admit(prepared(name, 1)).unwrap().acquire()).unwrap();
    permit.register()
}

/// The manual clock with an epoch a test moves, as a platform clock moves it across a suspend.
struct EpochClock {
    inner: Arc<ManualClock>,
    epoch: AtomicU64,
}
impl Clock for EpochClock {
    fn now(&self) -> Tick {
        self.inner.now()
    }
    fn sleep_until(&self, deadline: Tick) -> Sleep {
        self.inner.sleep_until(deadline)
    }
    fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }
}

#[test]
fn a_connection_reused_until_past_its_age_is_retired_at_release_and_the_next_opening_is_fresh() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let op = registered(&handle, "op-a");
    let first = now(op.open(exchange_spec()).unwrap().wait()).unwrap();
    assert!(!first.reused());
    now(first.finish());
    // Reused every 50 s, inside the 60 s idle expiry, for 550 s: the idle clock never retires it.
    for _ in 0..11 {
        rig.clock.advance(50_000);
        rig.progress.run();
        let exchange = now(op.open(exchange_spec()).unwrap().wait()).unwrap();
        assert!(exchange.reused());
        now(exchange.finish());
    }
    assert_eq!(rig.world.connects(), 1);
    // Leased at 599 s, and still leased past its age: the age is checked at release, an active exchange is not cut off.
    rig.clock.advance(49_000);
    rig.progress.run();
    let last = now(op.open(exchange_spec()).unwrap().wait()).unwrap();
    assert!(last.reused());
    rig.clock.advance(MAX_AGE_MS + 1 - 599_000);
    rig.progress.run();
    assert_eq!(handle.snapshot().connections.leased, 1);
    assert_eq!(rig.world.closes(), 0);
    now(last.finish());
    assert_eq!(handle.snapshot().connections.idle, 0, "retired, not idle");
    assert_eq!(rig.world.closes(), 1);
    let next = now(op.open(exchange_spec()).unwrap().wait()).unwrap();
    assert!(!next.reused());
    assert_eq!(
        rig.world.connects(),
        2,
        "the next opening got a fresh connection"
    );
    now(next.finish());
    now(op.finish());
}

fn epoch_rig() -> (Rig, Arc<EpochClock>, crate::HostOwner, crate::HostHandle) {
    let rig = Rig::new();
    let clock = Arc::new(EpochClock {
        inner: rig.clock.clone(),
        epoch: AtomicU64::new(0),
    });
    let ports = HostPorts::new(
        clock.clone(),
        Arc::new(FixedSeed(1)),
        rig.progress.clone(),
        Arc::new(FakeFactory(rig.world.clone())),
    );
    let (owner, handle) = crate::build(rig.config(HostLimits::default()), ports).unwrap();
    (rig, clock, owner, handle)
}

#[test]
fn an_epoch_change_retires_idle_entries_on_the_drivers_next_pass() {
    let (rig, clock, _owner, handle) = epoch_rig();
    let op = registered(&handle, "op-a");
    now(now(op.open(exchange_spec()).unwrap().wait())
        .unwrap()
        .finish());
    rig.progress.run();
    assert_eq!(handle.snapshot().connections.idle, 1);
    // A pass with no epoch change keeps it.
    rig.world.confirm_finishes();
    rig.progress.run();
    assert_eq!(handle.snapshot().connections.idle, 1);
    assert_eq!(rig.world.closes(), 0);
    // The clock saw a suspend: the next pass retires the idle entry, with no time having passed on the pool's clock.
    clock.epoch.store(1, Ordering::SeqCst);
    rig.world.confirm_finishes();
    rig.progress.run();
    assert_eq!(handle.snapshot().connections.idle, 0);
    assert_eq!(rig.world.closes(), 1);
    now(op.finish());
}

#[test]
fn an_epoch_change_is_seen_before_a_lease_so_no_stale_entry_is_offered() {
    let (rig, clock, _owner, handle) = epoch_rig();
    let op = registered(&handle, "op-a");
    now(now(op.open(exchange_spec()).unwrap().wait())
        .unwrap()
        .finish());
    clock.epoch.store(1, Ordering::SeqCst);
    let exchange = now(op.open(exchange_spec()).unwrap().wait()).unwrap();
    assert!(!exchange.reused(), "the idle entry was retired, not leased");
    assert_eq!(rig.world.connects(), 2);
    now(exchange.finish());
    now(op.finish());
}

/// An exchange whose finish ends `Discarded` while the adapter's `Closed` is held.
fn discarded_while_close_is_held(rig: &Rig, op: &Operation) -> crate::Exchange {
    let exchange = now(op.open(exchange_spec()).unwrap().wait()).unwrap();
    rig.progress.run();
    rig.world.discard_next.store(true, Ordering::SeqCst);
    rig.world.hold_close.store(true, Ordering::SeqCst);
    exchange
}

#[test]
fn a_receipt_waits_for_the_adapters_closed_then_states_pending_and_confirms_after() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let op = registered(&handle, "op-a");
    let exchange = discarded_while_close_is_held(&rig, &op);
    let connection = rig.world.connected_ids()[0];
    let mut finish = exchange.finish();
    // Within the cleanup bound the receipt is still waiting for the disposal.
    assert!(poll_once(&mut finish).is_pending());
    rig.clock.advance(CLEANUP_MS - 1);
    rig.progress.run();
    assert!(poll_once(&mut finish).is_pending());
    // At the bound it states the cleanup as pending, on a ticket that is not yet confirmed.
    rig.clock.advance(1);
    rig.progress.run();
    let end = now(finish);
    let CleanupStatus::Pending(ticket) = end.cleanup else {
        panic!("a pending receipt: {:?}", end.cleanup);
    };
    assert_eq!(ticket.status(), TicketStatus::Pending);
    // The operation's end states it too.
    let CleanupStatus::Pending(op_ticket) = now(op.finish()).cleanup else {
        panic!("a pending operation end");
    };
    assert_eq!(op_ticket.generation(), ticket.generation());
    // The adapter reports the disposal (the fake has no way to release a held `Closed`, so it reports the connection's
    // loss, which also ends a closing connection): the ticket confirms.
    rig.world.idle_closed_of(connection);
    rig.progress.run();
    assert_eq!(ticket.status(), TicketStatus::Confirmed);
}

#[test]
fn a_cancelled_exchanges_receipt_is_pending_until_its_disposal_is_confirmed() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let op = registered(&handle, "op-a");
    let exchange = discarded_while_close_is_held(&rig, &op);
    let connection = rig.world.connected_ids()[0];
    op.cancel_handle().cancel();
    let mut finish = exchange.finish();
    assert!(poll_once(&mut finish).is_pending());
    rig.clock.advance(CLEANUP_MS);
    rig.progress.run();
    let CleanupStatus::Pending(ticket) = now(finish).cleanup else {
        panic!("a pending receipt");
    };
    rig.world.idle_closed_of(connection);
    rig.progress.run();
    assert_eq!(ticket.status(), TicketStatus::Confirmed);
    now(op.finish());
}

#[test]
fn a_receipt_whose_disposal_is_confirmed_at_once_is_complete() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let op = registered(&handle, "op-a");
    let exchange = now(op.open(exchange_spec()).unwrap().wait()).unwrap();
    rig.world.discard_next.store(true, Ordering::SeqCst);
    let end = now(exchange.finish());
    assert!(matches!(end.cleanup, CleanupStatus::Complete));
    assert!(matches!(now(op.finish()).cleanup, CleanupStatus::Complete));
}

#[test]
fn a_seal_states_an_aborted_exchanges_cleanup_pending_until_the_disposal_is_confirmed() {
    let rig = Rig::new();
    let (owner, handle) = rig.host(1);
    let op = registered(&handle, "op-a");
    let exchange = now(op.open(exchange_spec()).unwrap().wait()).unwrap();
    rig.progress.run();
    // The adapter has not confirmed the exchange's end, so it is settling, and it will not confirm a close.
    rig.world.hold_finish.store(true, Ordering::SeqCst);
    rig.world.hold_close.store(true, Ordering::SeqCst);
    let mut finish = exchange.finish();
    assert!(poll_once(&mut finish).is_pending());
    let close = owner.close();
    // The seal aborts the exchange and states its receipt at once: the close was asked and not confirmed.
    let end = now(finish);
    let CleanupStatus::Pending(ticket) = end.cleanup else {
        panic!("a pending receipt: {:?}", end.cleanup);
    };
    assert_eq!(ticket.status(), TicketStatus::Pending);
    assert_eq!(rig.world.closes(), 1);
    // The sealed host's adapter is disposed (the close job runs): every connection it held is, and the ticket confirms.
    rig.progress.run();
    assert_eq!(ticket.status(), TicketStatus::Confirmed);
    drop(close);
    let _ = op;
}
