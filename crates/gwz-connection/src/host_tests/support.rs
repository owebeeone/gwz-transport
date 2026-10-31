//! Helpers shared by the profile, partition and misfit rows.
use crate::{
    AuthContext, HostHandle, Operation,
    fakes::{exchange_spec, https_spec, now, prepared_with},
};
use gwz_transport::protocol::Disposition;

/// An admitted and registered operation over the profile of kind `profile`, with `auth` as its context.
pub(super) fn registered_with(
    handle: &HostHandle,
    name: &str,
    profile: u8,
    auth: AuthContext,
) -> Operation {
    now(handle
        .admit(prepared_with(name, profile, auth))
        .unwrap()
        .acquire())
    .unwrap()
    .register()
}

/// One operation with one `spec` exchange, finished cleanly: whether its exchange reused a connection.
pub(super) fn one_exchange_on(
    handle: &HostHandle,
    name: &str,
    profile: u8,
    auth: AuthContext,
    spec: crate::ExchangeSpec,
) -> bool {
    let operation = registered_with(handle, name, profile, auth);
    let exchange = now(operation.open(spec).unwrap().wait()).unwrap();
    let reused = exchange.reused();
    assert_eq!(now(exchange.finish()).disposition, Disposition::Reusable);
    now(operation.finish());
    reused
}
/// One operation with one SSH exchange.
pub(super) fn one_exchange(
    handle: &HostHandle,
    name: &str,
    profile: u8,
    auth: AuthContext,
) -> bool {
    one_exchange_on(handle, name, profile, auth, exchange_spec())
}
/// One operation with one HTTPS exchange: the scheme the authentication scope partitions.
pub(super) fn one_https(handle: &HostHandle, name: &str, profile: u8, auth: AuthContext) -> bool {
    one_exchange_on(handle, name, profile, auth, https_spec())
}
