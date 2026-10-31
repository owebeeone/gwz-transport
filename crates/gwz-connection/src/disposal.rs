//! An adapter on its way to disposal, carrying the cleanup tickets that receipts stated while it was still the
//! instance's: once its disposal is confirmed every connection it held is disposed, so the tickets confirm.
use crate::{
    AuthContext, CleanupTicket, Completion, ConnectCommand, EndpointAdapter, ExchangeIo,
    ExchangeSpec, ProofId, ReuseCommand,
};
use gwz_transport::pool::ConnectionId;
use std::{
    io,
    task::{Context, Poll},
};

pub(crate) struct Disposing {
    inner: Box<dyn EndpointAdapter>,
    tickets: Vec<CleanupTicket>,
}
impl Disposing {
    pub(crate) fn wrap(
        inner: Box<dyn EndpointAdapter>,
        tickets: Vec<CleanupTicket>,
    ) -> Box<dyn EndpointAdapter> {
        if tickets.is_empty() {
            inner
        } else {
            Box::new(Self { inner, tickets })
        }
    }
}
impl EndpointAdapter for Disposing {
    fn connect(&mut self, command: ConnectCommand) {
        self.inner.connect(command);
    }
    fn cancel_connect(&mut self, connection: ConnectionId) {
        self.inner.cancel_connect(connection);
    }
    fn close(&mut self, connection: ConnectionId) {
        self.inner.close(connection);
    }
    fn start_exchange(
        &mut self,
        connection: ConnectionId,
        spec: &ExchangeSpec,
        auth: &AuthContext,
    ) -> io::Result<Box<dyn ExchangeIo>> {
        self.inner.start_exchange(connection, spec, auth)
    }
    fn authorize_reuse(&mut self, command: ReuseCommand) -> bool {
        self.inner.authorize_reuse(command)
    }
    fn cancel_reuse(&mut self, proof: ProofId) {
        self.inner.cancel_reuse(proof);
    }
    fn poll_completion(&mut self, cx: &mut Context<'_>) -> Poll<Completion> {
        self.inner.poll_completion(cx)
    }
    fn poll_dispose(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let polled = self.inner.poll_dispose(cx);
        if polled.is_ready() {
            for ticket in self.tickets.drain(..) {
                ticket.confirm();
            }
        }
        polled
    }
}
