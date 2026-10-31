//! The state an endpoint instance keeps under its lock: its openings, proofs, settling exchanges and connection
//! records, and the receipts of finished exchanges (movement from `instance.rs`).
use crate::{
    AuthContext, AuthScope, BindingId, CleanupStatus, CleanupTicket, Clock, EndpointAdapter,
    ExchangeEnd, ExchangeIo, ExchangeSpec, OpenFailure, ProofId, TicketStatus,
    binding::Cleanups,
    cell::{Cell, Slot},
    disposal::Disposing,
};
use gwz_transport::pool::{ConnectionId, Identity, LeaseId, PoolMachine, Request, RequestId};
use gwz_transport::protocol::Disposition;
use gwz_transport::protocol::Scheme;
use std::{
    collections::BTreeMap,
    sync::{Arc, atomic::AtomicBool},
    task::{Context, Poll},
};

/// What an opening's slot receives: the started exchange, its lease and whether its connection had served an exchange
/// before, or why there is none.
pub(crate) type Opened = Result<(Box<dyn ExchangeIo>, LeaseId, bool), OpenFailure>;
/// An opening whose request waits for a lease.
pub(super) struct Pending {
    pub(super) request: RequestId,
    /// The request as submitted (its tag included), to ask again after a lease of another scope, or, fresh, after a
    /// lease another binding last used could not serve it.
    pub(super) template: Request,
    pub(super) binding: BindingId,
    pub(super) spec: ExchangeSpec,
    /// The operation's context, lent to the connect the pool starts for this request and to the exchange.
    pub(super) auth: AuthContext,
    /// Leases of another scope this opening was offered and gave back.
    pub(super) misfits: usize,
    /// Idle connections another binding last used that could not serve this opening (step 1.6): it asks again with
    /// them excluded, so its own binding's idle connection, or a new one, is offered next.
    pub(super) excluded: Vec<ConnectionId>,
    /// The opening's absolute allocation deadline on the pool's clock, fixed at submit: a resubmission never extends it.
    pub(super) deadline: u64,
    /// The binding's cancelled flag: an opening that asks the pool again for a cancelled binding ends Cancelled instead.
    pub(super) cancelled: Arc<AtomicBool>,
    pub(super) slot: Arc<Slot<Opened>>,
}
/// A connection the pool has started and not yet disposed of: the scope its setup used (none when its opener had gone)
/// and whether an exchange has run on it.
pub(super) struct Setup {
    pub(super) scope: Option<AuthScope>,
    /// An HTTPS connection, the scheme the scope partitions.
    pub(super) https: bool,
    pub(super) served: bool,
    /// The binding that last leased it, once one has.
    pub(super) lessee: Option<BindingId>,
    /// The ticket of its disposal, once a receipt had to wait for it: confirmed when the adapter reports `Closed`.
    pub(super) closing: Option<CleanupTicket>,
}
/// An opening holding a fitting lease whose reuse proof for its binding is running: the proof the host minted for it,
/// which only a completion naming it can end.
pub(super) struct Authorizing {
    pub(super) pending: Pending,
    pub(super) lease: LeaseId,
    pub(super) proof: ProofId,
}
/// A finished exchange waiting for its adapter to confirm how it ended.
pub(super) struct Settling {
    pub(super) io: Box<dyn ExchangeIo>,
    pub(super) lease: LeaseId,
    pub(super) end: Cell<ExchangeEnd>,
    /// The exchange's operation's record of disposals left unconfirmed.
    pub(super) cleanups: Arc<Cleanups>,
}
/// A finished exchange whose connection is closing: its receipt is set when the adapter confirms the disposal
/// (`Complete`) or, if it has not by the cleanup bound, as `Pending` with the disposal's ticket.
pub(super) struct Ending {
    pub(super) ticket: CleanupTicket,
    pub(super) deadline: u64,
    pub(super) disposition: Disposition,
    pub(super) end: Cell<ExchangeEnd>,
}
pub(crate) struct InstanceState {
    pub(crate) adapter: Option<Box<dyn EndpointAdapter>>,
    pub(crate) pool: PoolMachine,
    pub(super) clock: Arc<dyn Clock>,
    /// The clock's epoch at the last pass: a change is a suspend or a wall-clock jump (contract section 10).
    pub(super) epoch: u64,
    /// The pool's cleanup bound, which a receipt waits for a disposal.
    pub(super) cleanup_ms: u64,
    pub(super) ending: Vec<Ending>,
    pub(super) openings: Vec<Pending>,
    pub(super) authorizing: Vec<Authorizing>,
    pub(super) settling: Vec<Settling>,
    pub(super) connections: BTreeMap<ConnectionId, Setup>,
    /// Leases of another scope the instance has offered and refused, over its life (an observation, never a limit).
    pub(super) misfits: u64,
    /// The pool identity token of each scope a live connection or opening uses (`scope-N`, `N` never reused), so the
    /// pool can match HTTPS idle connections by scope without holding the scope's bytes.
    pub(super) scope_tokens: Vec<(AuthScope, u64)>,
    pub(super) next_token: u64,
    /// The serial of the next reuse proof (`ProofId`), never reused within the instance's life.
    pub(super) next_proof: u64,
}
impl InstanceState {
    /// The adapter, for disposal elsewhere, which confirms the unconfirmed tickets of this instance's connections when
    /// it is done: from then on no pass of this instance reads the adapter's completions.
    pub(super) fn take_adapter(&mut self) -> Option<Box<dyn EndpointAdapter>> {
        let tickets = self
            .connections
            .values()
            .filter_map(|setup| setup.closing.clone())
            .filter(|ticket| ticket.status() == TicketStatus::Pending)
            .collect();
        self.adapter
            .take()
            .map(|adapter| Disposing::wrap(adapter, tickets))
    }
    /// The ticket for the disposal of `connection` if it is still closing, which the adapter confirms by `Closed`; none
    /// when the pool no longer holds it or the adapter has already disposed of it.
    pub(super) fn closing_ticket(&mut self, connection: ConnectionId) -> Option<CleanupTicket> {
        if !self.pool.is_closing(connection) {
            return None;
        }
        let setup = self.connections.get_mut(&connection)?;
        let clock = &self.clock;
        Some(
            setup
                .closing
                .get_or_insert_with(|| CleanupTicket::new(connection.sequence(), clock.clone()))
                .clone(),
        )
    }
    /// Sets the receipt of a finished exchange whose lease was just released: at once if its connection stays, else
    /// when its disposal is confirmed or the cleanup bound passes. `cleanups` is its operation's record.
    pub(super) fn report(
        &mut self,
        connection: ConnectionId,
        disposition: Disposition,
        end: Cell<ExchangeEnd>,
        cleanups: &Cleanups,
    ) {
        match self.closing_ticket(connection) {
            None => end.set(ExchangeEnd {
                disposition,
                cleanup: CleanupStatus::Complete,
                failure: None,
            }),
            Some(ticket) => {
                cleanups.note(ticket.clone());
                let deadline = self.clock.now().get().saturating_add(self.cleanup_ms);
                self.ending.push(Ending {
                    ticket,
                    deadline,
                    disposition,
                    end,
                });
            }
        }
    }
    /// Sets the receipts whose disposal is confirmed, and those past the cleanup bound (`all`: every one, as the
    /// instance stops) as pending on their ticket.
    pub(super) fn resolve_ending(&mut self, all: bool) {
        let now = self.clock.now().get();
        self.ending.retain(|item| {
            let confirmed = item.ticket.status() == TicketStatus::Confirmed;
            if !(confirmed || all || now >= item.deadline) {
                return true;
            }
            item.end.set(ExchangeEnd {
                disposition: item.disposition,
                cleanup: if confirmed {
                    CleanupStatus::Complete
                } else {
                    CleanupStatus::Pending(item.ticket.clone())
                },
                failure: None,
            });
            false
        });
    }
    /// The pool identity of a request for `key` and `identity` from an operation whose context has `scope`: HTTPS
    /// requests are scoped, everything else is unchanged.
    pub(super) fn pool_identity(
        &mut self,
        key: &gwz_transport::pool::Key,
        identity: Identity,
        scope: AuthScope,
    ) -> Identity {
        if key.scheme != Scheme::Https || identity != Identity::Https {
            return identity;
        }
        let token = match self.scope_tokens.iter().find(|(known, _)| *known == scope) {
            Some((_, token)) => *token,
            None => {
                let token = self.next_token;
                self.next_token += 1;
                self.scope_tokens.push((scope, token));
                token
            }
        };
        Identity::HttpsScoped(format!("scope-{token}"))
    }
    /// Forgets the tokens of scopes no live connection or opening uses. A forgotten scope gets a new token if it comes
    /// back, and a token is never given twice, so an orphaned connection's token matches nothing.
    pub(super) fn prune_scope_tokens(&mut self) {
        let InstanceState {
            scope_tokens,
            connections,
            openings,
            authorizing,
            ..
        } = self;
        scope_tokens.retain(|(scope, _)| {
            connections
                .values()
                .any(|setup| setup.scope == Some(*scope))
                || openings
                    .iter()
                    .any(|pending| pending.auth.scope() == *scope)
                || authorizing
                    .iter()
                    .any(|item| item.pending.auth.scope() == *scope)
        });
    }
}
/// Finished exchanges whose adapter now says how they ended: their leases go back to the pool with that disposition.
pub(super) fn settle(state: &mut InstanceState, cx: &mut Context<'_>) -> bool {
    let mut progressed = false;
    let mut index = 0;
    while index < state.settling.len() {
        let Poll::Ready(disposition) = state.settling[index].io.poll_finish(cx) else {
            index += 1;
            continue;
        };
        progressed = true;
        let item = state.settling.remove(index);
        let connection = item.lease.connection();
        let _ = state.pool.release(item.lease, disposition);
        state.report(connection, disposition, item.end, &item.cleanups);
    }
    progressed
}
