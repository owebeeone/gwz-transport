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
that health or implement pooling, SSH, HTTPS or credentials.

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
