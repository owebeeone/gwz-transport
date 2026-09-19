//! Fake endpoint ownership example: discrete messages and a lease, no wire.
use gwz_transport::{
    pool,
    protocol::{Disposition, Facts},
    stream::{self, Side, StreamMachine},
};
fn transfer(from: &mut StreamMachine, to: &mut StreamMachine) {
    while let Some(message) = from.next_message() {
        to.receive(message).unwrap();
    }
}
#[test]
fn endpoint_returns_lease_only_after_exchange_cleanup() {
    let mut pool = pool::PoolMachine::new(pool::Config {
        per_user_host: 1,
        ..Default::default()
    })
    .unwrap();
    let request = || {
        pool::Request::new(
            pool::Key::ssh("git", "host", 22),
            pool::Identity::Ambient,
            pool::Owner::new("carrier", "operation"),
        )
    };
    let first = pool.request(request()).unwrap();
    let Some(pool::Action::Connect { connection, .. }) = pool.next_action() else {
        panic!("connect");
    };
    pool.connected(connection, Ok(Some(pool::Identity::Ambient)))
        .unwrap();
    let lease = pool.take(first).unwrap();
    let next = pool.request(request()).unwrap();
    let mut sender =
        StreamMachine::new(stream::Config::new("carrier", 1, Side::Initiator)).unwrap();
    let mut endpoint =
        StreamMachine::new(stream::Config::new("carrier", 1, Side::Endpoint)).unwrap();
    sender.write(b"request").unwrap();
    sender.start_close().unwrap();
    transfer(&mut sender, &mut endpoint);
    endpoint.end_write().unwrap();
    transfer(&mut endpoint, &mut sender);
    assert_eq!(
        endpoint.complete_close(Disposition::Reusable, Facts::default()),
        Err(stream::Error::WouldBlock)
    );
    assert_eq!(pool.take(next), Err(pool::Error::WouldBlock));
    let mut buffer = [0; 7];
    assert_eq!(endpoint.read(&mut buffer).unwrap(), 7);
    assert_eq!(&buffer, b"request");
    // The fake host now proves cleanup. The two explicit calls below belong to
    // the endpoint host; the pool never guesses health from message delivery.
    endpoint
        .complete_close(Disposition::Reusable, Facts::default())
        .unwrap();
    pool.release(lease, Disposition::Reusable).unwrap();
    transfer(&mut endpoint, &mut sender);
    assert_eq!(
        sender.close_result().unwrap().disposition,
        Disposition::Reusable
    );
    let next_lease = pool.take(next).unwrap();
    assert_eq!(next_lease.connection(), connection);
    assert_ne!(next_lease, lease);
}
