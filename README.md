# gwz-transport

Independent transport protocol package for GWZ, providing message, stream and
pool interfaces. This package is unpublished; SSH/HTTPS adapters are not enabled.

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

## Backend I/O deadlines

Only the endpoint host can distinguish a network stall from intentional waiting.
After constructing the stream, call `MessageEndpoint::advance(now_ms)` before
starting timed work. Use the same monotonic origin for every later call and
advance to the current time before reporting a state change or progress.
An event at the deadline is already late. Timer ticks must run independently of
message arrival; `next_deadline()` is a snapshot, not a subscription. A pending
`next_message()` does not supply timer service.

The endpoint host sets `IoState` through `set_io_state(state)`:

| State | Meaning and clock behavior |
|---|---|
| `Idle` (initial) | No backend operation pending; preserve the remaining I/O allowance. |
| `Network` | At least one backend operation can make peer progress; spend I/O allowance. |
| `Backpressure` | All pending work is held by consumers, bounded queues or credit; preserve allowance. |
| `Interaction` | Supported visible, cancellable helper wait; spend cumulative helper allowance and preserve I/O allowance. |

If one direction is backpressured while the other can make peer progress, report
`Network`. Call `record_io_progress(bytes)` only for bytes actually transferred
to or from the backend peer while in `Network`. Positive progress restores the
I/O allowance; zero does nothing. Local buffering, taut delivery, polling and
keepalives are not progress. EOF ends a backend wait: update the state instead
of reporting it as progress. Initiators cannot set the backend state or report
progress. Positive progress outside `Network`, or either control after close
begins, returns `WrongState` without changing state. Terminal controls return
the retained terminal error. Repeated states and pause/resume cycles never refill
the allowance.
`io_status()` exposes state, remaining network/helper milliseconds and the
active deadline; `next_deadline()` also considers batching and close cleanup.

`Config::io_timeout_ms` defaults to 3,000 ms, accepts 0–2,147,483,647, and applies
only while `Network`. Zero disables network timing while keeping the state
`Network`; `io_status` then reports zero remaining network milliseconds and no
active deadline. Positive peer progress does not enable a disabled timeout. `interaction_budget_ms` defaults to 120,000 ms and accepts
0–86,400,000; zero forbids further helper waiting. The endpoint captures native
policy. For existing Open `connect_ms` and `io_ms`, zero means disabled and
positive values accept the same range as native startup settings through
`i32::MAX`. A request can only tighten policy: zero is allowed only when the
endpoint has already disabled that timeout; a positive request can shorten a
finite timeout or impose a finite limit on a disabled one. Other Open deadlines
remain positive and are clamped to the configured endpoint limits. Carry the operation's **remaining**
helper allowance from connect/auth into this stream configuration, subtracting
time already used. Each new Open has its own policy-capped allowance, including
when it reuses a connection. The pool and stream must not each grant a fresh
full allowance for the same Open.

Close starts its separate cleanup deadline and stops the I/O clock. I/O/helper
expiry wakes blocked calls with `Error::Timeout` and sends endpoint `Failed`
with `Timeout` / `Effect::Possible`. Received bytes remain readable before the
error. A timeout is never EOF, permission to replay, or evidence of Git success.
Discard the connection lease and acknowledge actual disposal before reclaiming
capacity. Late progress, cancellation and drop cannot replace the first error.

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

`Connect.network_deadline` is `Option<u64>`: `None` denotes an active connect
with network timing disabled, not a completed connect. Bounded helper
interaction preserves the optional remaining network budget and resumes it.

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
three non-idle budgets. The connect timeout accepts 0–2,147,483,647 ms; zero
disables network timing. An omitted override inherits; `Some(0)` is allowed only
if endpoint connect timing is disabled; a positive override may cap a disabled
endpoint or shorten a finite setting. Allocation, helper, cleanup and idle
settings remain bounded to 1–86,400,000 ms. Network timing does not disable
helper limits, cancellation, shutdown or disposal deadlines. `begin_interaction`/`end_interaction` pause network time;
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

## Binding and admission

Start with a fresh, never-reused session ID. `binding::offer(session, role)`
advertises version 1, SSH/HTTPS and their supported policies using
`binding::default_limits()`. Construct `EndpointConfig` with explicit endpoint
ID, role, allowed schemes/policies, limits and trust owner; these fields have no
implicit endpoint defaults. `accept(&offer)` returns a Bound reply and Binding,
or an effect-free Failure for the host to carry as BindRejected. The initiator
uses `binding::verify(&offer, &reply)` before accepting the binding.

Before credentials, connection allocation or network work, call
`Binding::check_open(&message)`, which admits the Open under the installed
binding's negotiated receiver limits. The host also checks deadline tightening against
its captured endpoint policy before effects; Binding does not contain native
timeout settings. It validates ownership and unique stream IDs in that session,
executes only an admitted Open, and sends Opened or
OpenFailed. Construct stream endpoints only after Open/Opened has established
the agreed limits. The package does not automatically create a pool lease when
it receives Open; the host owns that sequencing. All structured failure codes
and effect classifications must reach the caller unchanged.

For typed input use `codec::admit_limited`; for encoded input use
`codec::decode_limited`. The latter checks limits before generic decoding.
Never use the unbounded generated codec as ingress validation. `Limits` defaults
and maximum negotiation ceilings are:

| Setting | Default ceiling |
|---|---:|
| Encoded transport envelope | 128 KiB (64 KiB for bootstrap) |
| Data payload | 64 KiB |
| String / metadata field | 16 KiB |
| Nesting / total collection entries | 16 / 256 |
| Decode allocation charge | 512 KiB (256 KiB for bootstrap) |
| Queued bytes / frames | 4 MiB / 64 |
| Reserved control bytes / frames | 64 KiB / 8 |
| Receive credit window | 1 MiB |

Negotiation takes bounded minima; Open and stream settings may narrow these.
The host enforces aggregate queues across streams, reserving the stated control
capacity, and accounts for any enclosing message's overhead separately. The
transport limits do not bound an arbitrary outer wrapper. The supplied
communication layer must also bound allocation before constructing that wrapper,
preserve message order, provide bounded backpressure and report closure through
`MessageEndpoint::disconnect()` (or drop). No physical framing is provided here.

`stream::Config::new` requires session ID, positive stream ID and side. Other
initial values: both limit sets above, 64 KiB send buffer, 64 KiB local and peer
receive windows, 16 KiB payload, 100 ms coalescing, 5,000 ms close timeout,
64 application waiters, plus the I/O/helper defaults above. Adjust the peer
window to its agreed advertisement. Stream IDs must be unique within a session.
Pool key, identity and owner are mandatory in `Request::new`; optional request
allocation/connect/interaction overrides default to `None` (use pool policy).

## Try the fake host locally

The package is not published. From this source directory, use Rust 1.95 or later:

```sh
cargo test --locked --test pool_stream endpoint_returns_lease_only_after_exchange_cleanup -- --exact
cargo test --locked --test stream
cargo test --locked --test io_clock
```

The first fixture demonstrates the complete host lifecycle: request a lease,
receive Connect, acknowledge a fake successful connection, take the lease,
exchange request bytes, close the stream, finish backend cleanup, then release
Reusable. A second waiting request receives the same connection with a fresh
exclusive lease. To end a host, call `Pool::shutdown()`, drain Close/Abort actions
and acknowledge every disposal until `shutdown_complete()` is true. Drop the
driver only after disposal. There are no installed services or remote resources
to undo in these tests.

For generation install the base dependencies of `taut-proto==0.9.1` and Rust
1.96.0's rustfmt, then provide the clean Taut checkout pinned in
`protocol/generator.json`:
`python3 scripts/regen.py --check --taut-source ../taut/src`.
The new optional-field compatibility requires that source extension; published
0.9.1 alone is insufficient. The workflow checks out the same immutable pin.
That pin must be published before remote CI can run; only local execution is
claimed at this candidate checkpoint. The script verifies the exact formatter
build recorded in `protocol/generator.json`. `.github/workflows/contracts.yml`
runs this drift check, formatting, the MSRV suite and standalone packaging.
The workflow is prepared for repository CI; remote execution has not been
established while this member has no remote. Core's unpublished consumer has a
separate archive proof and source-pinned generator check; it is not part of this
standalone job and does not silently fetch sibling repositories.

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

## Request mux and application ports (candidate)

`mux::Mux` routes shared messages for registered host request IDs over one
session. `Attachment = (String, Envelope)` uses the existing request_id as its
String; this is an application tuple, not a new wire wrapper. Construct an
initiator and an endpoint using the same fresh session ID and explicit
`EndpointConfig`. Register each request on both sides before calling `begin`
on the initiator. The first request owns bootstrap; its cancellation before
verified Bound installation closes the session. Cancellation after installation
leaves the binding available to other requests. This mux requires owner profile
2; the lower-level binding codec continues to support profile 1.

`Owner::new(mux)` returns an Owner and Port. Forward core `Port::next_message()`
to client `Port::deliver()`, and client `next_message()` to core `deliver()`.
Run both ordered forwarding loops independently. Cancelling a pending receive
consumes nothing; cancelling a pending delivery admits nothing. After taking
an attachment, deliver it or disconnect both ends. The supplied host propagates
closure and bounds any queues outside the mux. Last Owner or last Port drop
closes the local generation and wakes waiters. Ports own no carrier or threads.

Once `Owner::ready()` completes, inspect `Owner::binding()` for negotiated
settings; this snapshot is not independent live authority. `check_identity` and
`open` queue endpoint work. `next_action` hands admitted messages to the host worker, which sends
replies through `send`. Opened validates the bound endpoint and trust owner;
request, operation and stream correlation are enforced before dispatch. The
worker must perform credential/path checks and dispose late results itself.
The mux never creates a network connection or claims physical cleanup.

Use `cancel` to seal a request. `finish` returns WouldBlock while routes or
terminal handoffs remain; continue message delivery, drain local `next_action`
results and clock ticks, then call finish again to unregister. A queued terminal
survives sealing until transferred to the port or local action consumer. If that
handoff stalls past the original cleanup deadline, the generation closes instead
of claiming successful finish. Data frames are not retained merely for cleanup.
Request IDs cannot be reused in the session. Config.role defaults to Driver
(the CLI endpoint role), and Config.limits defaults to the Binding limits table
above. Core-local users select Local explicitly on both sides.
The default max_requests is 256
**total registrations per session**, including tombstones, bounded to 4,096;
start a fresh session explicitly when exhausted. Default max_streams is 64,
bounded to 1,024. Bootstrap and cancellation cleanup default to 5 seconds;
identity checks accept up to 120 seconds. Queue limits are the negotiated Limits,
independently applied in each direction, including decoded-allocation charges,
request-id overhead and control reserves. There are at most 128 pending async
calls; exhaustion closes the local generation and wakes existing callers.

Call `Owner::advance(monotonic_ms)` independently of waiting futures; initialize
its clock before bootstrap and continue periodic ticks while work is pending.
Timeouts emit typed failures/cancellation, never success. `ready` is only a
readiness waiter: dropping it does not cancel the registered request; the host
request guard must call cancel. The future core `transport_host` facade owns
that guard and the physical-worker cleanup policy; it is not implemented here.

The executable lifecycle/direction fixture is `tests/mux_async.rs`. The
`tests/mux.rs` randomized byte test takes `GWZ_MUX_SEED` (default
`0x47575a504c414345`) and `GWZ_MUX_CASES` (default 100); preserve the source
revision and printed seed to replay. It transports typed messages entirely in
memory, with random chunk/read sizes and small credit windows.

Valid incompatible bootstrap receives a typed `BindRejected`. The endpoint
enters `Phase::Rejecting`, admits no new work and keeps the reply until its port
transfers it (then closes), or until the bootstrap/cleanup deadline closes it.
Keep servicing the port during rejection. `Owner::ready()` returns
`Error::Rejected`; `Owner::bootstrap_failure()` retains the exact effect-free
Failure even after closure. Malformed bootstrap remains a protocol error;
carrier loss without a rejection returns Closed, not an invented peer reason.

### Construct, bind, use once, disconnect

This lower-level in-memory example uses only implemented APIs. An async host
runs the body. It deliberately refuses the identity check without touching files
or credentials; a real endpoint replaces that response with its supervised work.
The ordered single-exchange steps illustrate setup; a sustained host runs the
two forwarding loops and clock ticks independently as described above.

```rust,no_run
# async fn example() -> Result<(), gwz_transport::mux::Error> {
use gwz_transport::{binding, mux::{Config, Error, Mux, Owner}, protocol::*};

// Allocate fresh IDs for every new session/endpoint in a real host.
let (core, core_port) = Owner::new(Mux::initiator("session-1", Config::default())?);
let endpoint = binding::EndpointConfig {
    endpoint_id: "endpoint-1".into(), role: EndpointRole::Driver,
    schemes: vec![Scheme::Ssh], policies: vec![AuthPolicy::SshExplicit],
    limits: binding::default_limits(), trust_owner: "client-account".into(),
};
let (client, client_port) = Owner::new(Mux::endpoint(
    "session-1", endpoint, Config::default(),
)?);
core.advance(0);
client.advance(0);
core.register("request-1", Some("operation-1".into()))?;
client.register("request-1", None)?;
core.begin("request-1")?;
// Core -> client, then client -> core for Bind/Bound.
client_port.deliver(core_port.next_message().await?.ok_or(Error::Closed)?).await?;
core_port.deliver(client_port.next_message().await?.ok_or(Error::Closed)?).await?;
core.ready().await?;

core.check_identity("request-1", Identity {
    mode: IdentityMode::ExplicitKey, key_path: Some("selected-key".into()),
    path_base: None,
}, 1000)?;
client_port.deliver(core_port.next_message().await?.ok_or(Error::Closed)?).await?;
let (request_id, check) = client.next_action().await?.ok_or(Error::Closed)?;
client.send(&request_id, &Envelope {
    version: 2, session_id: check.session_id, stream_id: check.stream_id,
    kind: MessageKind::IdentityCheckFailed,
    identity_check_failed: Some(Failure {
        code: ErrorCode::UnsupportedOperation, effect: Effect::None, facts: None,
    }),
    ..Default::default()
})?;
assert_eq!(client.finish(&request_id), Err(Error::WouldBlock));
core_port.deliver(client_port.next_message().await?.ok_or(Error::Closed)?).await?;
let (_, outcome) = core.next_action().await?.ok_or(Error::Closed)?;
assert_eq!(outcome.kind, MessageKind::IdentityCheckFailed);
client.finish(&request_id)?;
core.finish("request-1")?;
core_port.disconnect();
client_port.disconnect();
# Ok(())
# }
```

`cargo test --doc` type-checks this example. The executable in-memory lifecycle
and closure regressions are in `tests/mux.rs` and `tests/mux_async.rs`.
