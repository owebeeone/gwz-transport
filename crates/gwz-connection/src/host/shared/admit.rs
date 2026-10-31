//! The admission and routing half of the host's shared methods: the retained-charge budget, the waiting queue and the
//! slots, and the bindings a host registers and routes to.
use super::{Claim, HostShared, Waiter};
use crate::{
    Admission, AdmissionError, Direction, PreparedOperation,
    binding::{BindingId, BindingShared, CarrierId, Discard, Routed},
    host::HostPhase,
    operation::{OperationEnd, Registration, TerminalReason},
};
use gwz_transport::mux::Attachment;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Waker,
    time::Duration,
};

/// The most one admission may charge, and the most a host's admissions and operations may charge together (contract 10).
const MAX_ADMISSION_CHARGE: usize = 16 * 1024 * 1024;
const MAX_HOST_CHARGE: usize = 64 * 1024 * 1024;

/// A retained-charge reservation; dropping it gives the charge back. Atomics only, so it is safe to drop anywhere.
pub(crate) struct ChargeGuard {
    counter: Arc<AtomicUsize>,
    amount: usize,
}
impl Drop for ChargeGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(self.amount, Ordering::AcqRel);
    }
}

impl HostShared {
    /// Reserves `charge` of the retained-input budget, or refuses.
    fn charge(&self, charge: usize) -> Result<ChargeGuard, AdmissionError> {
        if charge > MAX_ADMISSION_CHARGE {
            return Err(AdmissionError::InputBudget);
        }
        let before = self.charged.fetch_add(charge, Ordering::AcqRel);
        let guard = ChargeGuard {
            counter: self.charged.clone(),
            amount: charge,
        };
        if before + charge > MAX_HOST_CHARGE {
            // `guard` gives the charge back as it drops.
            return Err(AdmissionError::InputBudget);
        }
        Ok(guard)
    }
    /// Validates and queues one admission record. Synchronous, with no endpoint, thread or effect.
    pub(super) fn admit_input(
        self: Arc<Self>,
        input: PreparedOperation,
    ) -> Result<Admission, AdmissionError> {
        let charge = self.charge(input.auth.charge())?;
        let id = {
            let mut state = self.lock();
            if state.phase != HostPhase::Open {
                return Err(AdmissionError::Closing(
                    state.closing.unwrap_or(TerminalReason::HostClosing),
                ));
            }
            if state.queue.len() >= self.limits.waiting_admissions {
                return Err(AdmissionError::QueueFull);
            }
            let id = state.next_waiter;
            state.next_waiter = id.wrapping_add(1);
            state.queue.push_back(Waiter { id, waker: None });
            id
        };
        let deadline = self
            .clock
            .now()
            .plus(Duration::from_millis(self.limits.admission_bound_ms));
        let sleep = self.clock.sleep_until(deadline);
        Ok(Admission::local(self, id, input, charge, sleep))
    }
    /// What an admission found when it asked for its slot.
    pub(crate) fn take_slot(&self, id: u64, waker: &Waker) -> Slot {
        let mut state = self.lock();
        if state.phase != HostPhase::Open {
            state.queue.retain(|waiter| waiter.id != id);
            return Slot::Closing(state.closing.unwrap_or(TerminalReason::HostClosing));
        }
        if state.queue.front().is_some_and(|waiter| waiter.id == id)
            && state.active < self.limits.active_slots
        {
            let Some(serial) = state.next_binding.checked_add(1) else {
                state.queue.pop_front();
                return Slot::Exhausted;
            };
            state.queue.pop_front();
            let serial = std::mem::replace(&mut state.next_binding, serial);
            state.active += 1;
            let claim = state.next_claim;
            state.next_claim += 1;
            state.claims.insert(
                claim,
                Claim {
                    revoked: None,
                    released: false,
                },
            );
            return Slot::Granted { claim, serial };
        }
        if let Some(waiter) = state.queue.iter_mut().find(|waiter| waiter.id == id) {
            waiter.waker = Some(waker.clone());
        }
        Slot::Wait
    }
    /// Takes `id` out of the queue; the next waiter may now be at the front.
    pub(crate) fn dequeue(&self, id: u64) {
        let waker = {
            let mut state = self.lock();
            state.queue.retain(|waiter| waiter.id != id);
            state
                .queue
                .front_mut()
                .and_then(|waiter| waiter.waker.take())
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
    /// Gives a claim's slot back unless a revocation already did, and wakes the next waiter.
    pub(crate) fn drop_claim(&self, claim: u64) {
        let waker = {
            let mut state = self.lock();
            if let Some(claim) = state.claims.remove(&claim)
                && !claim.released
            {
                state.active -= 1;
            }
            state
                .queue
                .front_mut()
                .and_then(|waiter| waiter.waker.take())
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
    /// The one authority transition of registration: under the state lock the claim is either still good (and the
    /// binding is published) or it was revoked (and the caller gets the reason). It does no I/O and calls no port.
    pub(crate) fn publish(
        &self,
        claim: u64,
        binding: &Arc<BindingShared>,
    ) -> Result<(), TerminalReason> {
        let (outcome, waker) = {
            let mut state = self.lock();
            let claim = state.claims.remove(&claim);
            let revoked = claim
                .as_ref()
                .and_then(|claim| claim.revoked)
                .or(match state.phase {
                    HostPhase::Open => None,
                    _ => Some(state.closing.unwrap_or(TerminalReason::HostClosing)),
                });
            match (claim, revoked) {
                (Some(_), None) => {
                    state.bindings.insert(binding.id.serial, binding.clone());
                    (Ok(()), None)
                }
                (claim, reason) => {
                    if claim.is_some_and(|claim| !claim.released) {
                        state.active -= 1;
                    }
                    let waker = state
                        .queue
                        .front_mut()
                        .and_then(|waiter| waiter.waker.take());
                    (Err(reason.unwrap_or(TerminalReason::HostFault)), waker)
                }
            }
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        outcome
    }
    /// When a sealed binding has nothing live left, it leaves the table, gives its slot back and reports its end.
    pub(crate) fn retire_if_done(&self, binding: &Arc<BindingShared>) {
        use std::sync::atomic::Ordering::{AcqRel, Acquire};
        if !binding.sealed.load(Acquire)
            || binding.live.load(Acquire) != 0
            || binding.retired.swap(true, AcqRel)
        {
            return;
        }
        let waker = {
            let mut state = self.lock();
            if state.bindings.remove(&binding.id.serial).is_some() {
                state.active -= 1;
            }
            state
                .queue
                .front_mut()
                .and_then(|waiter| waiter.waker.take())
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        binding.done.set(OperationEnd {
            registration: Registration::Registered(binding.id),
            cleanup: binding.cleanups.status(),
            exchanges_opened: binding.opened.load(Acquire),
            cancelled: binding.cancelled.load(Acquire),
            revoked: None,
        });
    }
    pub(crate) fn closing_reason(&self) -> TerminalReason {
        self.lock().closing.unwrap_or(TerminalReason::HostClosing)
    }
    pub(crate) fn carrier(&self) -> CarrierId {
        CarrierId(self.namespace)
    }
    /// A frame for a binding, from a carrier connection. Only the binding that this carrier bound takes it; anything
    /// else is discarded and no record is made.
    pub(crate) fn route(
        &self,
        carrier: CarrierId,
        binding: BindingId,
        direction: Direction,
        item: &Attachment,
    ) -> Routed {
        if carrier != self.carrier() {
            return Routed::Discarded(Discard::ForeignCarrier);
        }
        if binding.host != self.namespace {
            return Routed::Discarded(Discard::ForeignHost);
        }
        let entry = self.lock().bindings.get(&binding.serial).cloned();
        let Some(entry) = entry else {
            return Routed::Discarded(Discard::Retired);
        };
        if entry.carrier != carrier {
            return Routed::Discarded(Discard::ForeignCarrier);
        }
        if item.1.session_id != entry.session {
            return Routed::Discarded(Discard::WrongSession);
        }
        let delivered = entry
            .pair
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .deliver(direction, item);
        match delivered {
            Ok(()) => Routed::Delivered,
            Err(_) => {
                // The binding's own frame was invalid: it, and only it, is cancelled, as `OperationCancel::cancel` cancels
                // it (its openings that wait for a proof end, then its pool owner is cancelled). No host or pool lock
                // is held here.
                crate::exchange::cancel_binding(&entry);
                Routed::Discarded(Discard::Rejected)
            }
        }
    }
}
/// What an admission's request for a slot returned.
pub(crate) enum Slot {
    Wait,
    Granted { claim: u64, serial: u64 },
    Closing(TerminalReason),
    Exhausted,
}
