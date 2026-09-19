# gwz-transport

Independent transport protocol package for GWZ. Phase 1 is under implementation;
the protocol is not frozen and SSH/HTTPS adapters are not enabled.

The package owns `protocol/transport.taut.py` and exports generated Rust types
from `protocol`. Consumers reuse these types. Other language consumers use the
packaged schema and exported IR. `src/cbor.rs` is the unmodified MIT-licensed
taut runtime (see LICENSE-taut); surrounding code uses GPL-2.0-only like GWZ.

Normal builds use checked-in generated code and do not require Python, a sibling
checkout or network schema discovery. Regeneration is explicit and version-pinned
by `protocol/generator.json`; see `python3 scripts/regen.py --help`.

Run `cargo test --locked`. The communication layer is supplied by the host;
this package does not define physical framing or change the CLI–core interface.
The stream runtime passes typed `protocol::Envelope` values directly. It never
encodes, frames or transports them. Schema codecs are separate utilities for a
host which needs them; the in-memory stream tests do not use them.

## Stream API

After the host establishes an exchange and agrees its limits, construct
`stream::Stream::new(Config)` to obtain a file-like `Stream` and its
`MessageEndpoint`. `read`, `write`, `write_all`, `flush`, `end_write` and `close`
return executor-independent futures. Writes can be partial. Clones share one
byte position and exchange; dropping the final handle requests cancellation.
Dropping the message endpoint reports delivery loss to pending operations.

The host takes `MessageEndpoint::next_message()` and passes each typed value to
the peer's `deliver()`, preserving order per direction. Delivery queues must be
bounded, with fair scheduling and control capacity across streams. Only one
host dispatcher should take outgoing messages for a given endpoint. There is
no host service, physical connection, timer task or executor inside this crate.
The host drives `advance(monotonic_milliseconds)` and services `next_deadline()`
even while application calls are blocked. Use the same clock origin throughout
a stream's lifetime and initialize it before the first write.

`StreamMachine` exposes the same deterministic transitions with `WouldBlock`
results for custom adapters and testing. The default limits are 64 KiB each for
send buffer and receive window, 16 KiB per payload, 100 ms coalescing from the
first byte, a 5 second close deadline, and 64 registered application waiters.
One additional wake slot is reserved for the outgoing message dispatcher, so
application concurrency cannot evict the control path. Set
`peer_receive_window` to the peer's advertised window; set `max_payload` to a
common negotiated cap. Supply `receive_limits` and `peer_limits` from negotiation;
construction refuses buffer/window/payload settings that expand those limits.
Typed ingress uses the receiver's policy, and caller-supplied close facts are
admitted against the peer's policy before the crate retains or clones them.
Invalid local close facts return `Protocol` without completing or failing the
stream; the host can retry with valid facts. Host code must validate session/stream ownership before
routing messages. Buffer limits apply per stream; the host owns aggregate limits.

Credit is returned only after reads or the initiator's explicit close drain.
Forward flush acknowledges consumption by the endpoint sink; reverse flush
acknowledges admission to the bounded read adapter. An endpoint adapter must
only read as much as it can hand to its sink or another explicitly bounded
buffer. Flush is not confirmation of a Git operation. End-write preserves the
reverse direction. Close drains/discards unread response data and exposes that
fact in its result. The endpoint host calls `complete_close` only after backend
cleanup has proved whether a connection is reusable. This crate does not infer
that health or implement SSH, HTTPS or credentials.

Peer failures expose their exact `ErrorCode` and `Effect` through
`Error::PeerFailed { code, effect }`, after any received byte prefix. `Cancel`
supports `Cancelled` and `Timeout`; other reasons are invalid messages. Local
protocol failures and endpoint timeouts emit `Failed` with `Effect::Possible`;
delivery loss remains `CarrierLost` and cannot imply a retry-safe outcome.

Capability lists mean only pairs allowed by the central policy relation:
SSH ambient uses ambient identity, SSH explicit uses an explicit key, HTTPS
anonymous uses credentials-disabled identity, and HTTPS gh uses ambient endpoint
identity. All other combinations are rejected before effects. A binding cannot
advertise a scheme or policy without a compatible partner in its capability set.

## Connection pool API

`pool::Pool::new(Config)` returns a cloneable `Pool` and one `PoolDriver`.
`checkout(Request)` returns a cancellable future for an exclusive `Lease`.
Clones share capacity and resources. The host pairs the lease with an exchange;
stream clones share that exchange, not extra pool allocations. Once backend
cleanup proves health, explicitly consume the lease with
`release(Disposition::Reusable)`. Dropping a lease discards its connection;
dropping a pending or ready-but-unclaimed checkout cancels it. The
`tests/pool_stream.rs` fake endpoint demonstrates the stream/lease ownership seam.
`PoolMachine` offers the same transitions for deterministic hosts and tests.

Pool keys contain scheme, SSH username, configured host and effective port.
Repositories are not keys. The endpoint resolves explicit SSH identity before
each checkout and supplies a current proof; an invalid or missing identity must
never reach the pool. Ambient and explicit identities have different reuse
eligibility within the same key. `connected` supplies the proven identity or
`None` for a single-use resource. HTTPS pooling carries no account identity:
the endpoint applies the permitted anonymous/gh authentication policy per request.

The driver receives local `Connect`, `CancelConnect`, `AbortConnect`, `Close` and
`Abort` actions. These are host API commands, not new wire messages. Execute
physical work outside the pool, then call `connected`/`closed` to acknowledge its
actual completion. Cancelled connectors and closing resources retain capacity;
late success after cancellation must close. A failed `connected` acknowledgment
means the connector has already disposed any partial resource. Invalid/stale
callbacks never adopt a resource: the caller must dispose it. There is no retry
or replay of a failed exchange inside the pool.

For spontaneous idle loss, dispose the unusable resource and call
`idle_closed(connection)`. It removes Idle or already-Closing state, including
an eviction that has not dispatched its Close yet, and wakes capacity waiters.
A foreign/duplicate token returns `Stale`. If checkout won first, `WrongState`
leaves that exclusive lease untouched: route the I/O failure to its active
exchange, whose host must discard the lease and acknowledge cleanup. This
callback never cancels a lease using an old idle observation.

Limits default to eight connections per user/host across ports
(`per_user_host`) and eight per host across users, ports and schemes
(`per_host`), plus 256 per endpoint and 1,024 outstanding requests. HTTPS has a
no-username capacity bucket for each configured host. Reuse still requires the
full scheme/username/host/port key.
Opening, idle, leased and closing all count. Ready and failed unclaimed results
still occupy request slots. Construction fixes these ceilings; operation fan-out
limits cannot resize the pool. Compatible waiters are served in order, while an
incompatible head does not prevent eligible reuse. Incompatible idle resources
can be retired to make room for a new identity.

Idle expiry defaults to 60 seconds from healthy release. Other defaults are
30 seconds for allocation, 10 seconds of connect-network time, 120 seconds of
helper interaction and 5 seconds for cleanup. Requests may shorten the first
three non-idle budgets. `begin_interaction`/`end_interaction` pause network time;
multiple interactions share one total allowance. None of these is an active
stream read/write timeout.

Initialize the driver clock before the first checkout and supply monotonic
milliseconds to `advance`. The host must drive clock ticks independently of
commands and service deadlines even while `next_action` is pending.
`next_deadline` is a snapshot, not a timer subscription; concurrent checkouts or
releases can introduce earlier deadlines. A host may use periodic ticks, or
recompute timers whenever it mutates the pool. The crate owns no timer task.
An idle connection is never expired while leased. Requests carry
`Owner { session, operation }`: use the fresh, never-reused session ID from the
host's binding and an operation identifier scoped within that session. One
operation may own many exchanges. `cancel_operation(&Owner)` cancels just that
operation; `cancel_session(session_id)` cancels all of that carrier session's
requests and active leases. Both preserve idle resources and other scopes. The
host stops admission from a lost session before cancellation. Late cancellation
for an older session therefore cannot affect an operation with the same name in
a fresh session. `shutdown` or dropping the final Pool owner refuses new requests
and initiates cleanup. At the cleanup deadline the host must promptly execute
abort actions; `shutdown_complete` stays false until actual disposal is
acknowledged. Before dropping its driver, the host must cancel connectors and
dispose physical resources itself. Driver loss wakes pending callers and
invalidates active leases.

## Reproducible randomized tests

`cargo test --locked --test monte_carlo` runs 3,000 cases with a fixed suite seed.
Both directions vary input bytes, write/read sizes, windows, buffers, payloads,
batching time, bounded message queues, delivery delays, flushes and half-close.
An independent oracle checks exact byte prefixes, final streams, credit and
flush ordering. Every sixteenth case is repeated to check deterministic traces.
Coverage floors on the default suite protect its adverse cases.

Failures print generator version, run seed, case index, case seed, configuration,
inputs, step, recent trace and the precise replay command. For example:

```sh
GWZ_TRANSPORT_MC_CASE_SEED=0x1234 cargo test --locked --test monte_carlo seeded_message_streams -- --exact --nocapture
GWZ_TRANSPORT_MC_SEED=0x1234 GWZ_TRANSPORT_MC_CASES=10000 cargo test --locked --test monte_carlo seeded_message_streams -- --exact --nocapture
cargo test --locked --release --test monte_carlo extended_message_streams -- --ignored --exact --nocapture
```

The extended campaign defaults to 50,000 cases with a time-derived seed printed
before execution. Pin the source revision with any recorded seed: generator
changes can change the meaning of a seed. Turn discovered failures into named
deterministic regression tests as well as retaining their seeds. This approach
follows the testkit in `sdax-rs`; it adds no dependency on that repository.

Pool tests use fake connections and a controlled clock. The default
`cargo test --locked --test pool_random` checks 2,000 seeded lifecycle schedules
against a separate resource ledger: capacity including in-flight cleanup,
identity eligibility, exclusive leases, reuse, session/operation cancellation,
spontaneous idle loss, helper budgets and
complete teardown. Generator `gwz-transport-pool-v2` normalizes diagnostic IDs
for exact replay across pool instances. Every sixteenth case repeats its trace;
the fixed suite has coverage floors for all twelve event classes.

```sh
GWZ_POOL_MC_CASE_SEED=0x1234 cargo test --locked --test pool_random seeded_pool_lifecycles -- --exact --nocapture
GWZ_POOL_MC_SEED=0x202609195eed cargo test --locked --release --test pool_random extended_pool_lifecycles -- --ignored --exact --nocapture
```

`GWZ_POOL_MC_CASES` changes the campaign size; the extended default is 50,000.
Failures print configuration, clock, step, recent events and a replay command.
