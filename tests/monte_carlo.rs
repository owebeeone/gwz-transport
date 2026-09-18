//! Typed messages only: no codecs, serialization, sockets, framing or real clocks.
//! Inspired by sdax-rs's fixed-seed/per-case-seed/replay and coverage approach.
mod support;
use gwz_transport::{
    protocol::*,
    stream::{Config, Error, FlushTicket, Side, StreamMachine},
};
use std::{
    collections::VecDeque,
    panic::{AssertUnwindSafe, catch_unwind},
};
use support::{Random, case_seed, setting};

const RUN_SEED: u64 = 0x4757_5a54_2026_0919;
const CASES: usize = 3000;
const GENERATOR: &str = "gwz-transport-stream-v1";

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Coverage {
    blocked_writes: usize,
    blocked_reads: usize,
    partial_writes: usize,
    flushes: usize,
    tiny_windows: usize,
    empty_streams: usize,
    zero_operations: usize,
    delayed_delivery: usize,
    data_messages: usize,
}
impl Coverage {
    fn add(&mut self, other: &Self) {
        self.blocked_writes += other.blocked_writes;
        self.blocked_reads += other.blocked_reads;
        self.partial_writes += other.partial_writes;
        self.flushes += other.flushes;
        self.tiny_windows += other.tiny_windows;
        self.empty_streams += other.empty_streams;
        self.zero_operations += other.zero_operations;
        self.delayed_delivery += other.delayed_delivery;
        self.data_messages += other.data_messages;
    }
}

struct Case {
    random: Random,
    configs: [Config; 2],
    machines: [StreamMachine; 2],
    source: [Vec<u8>; 2],
    accepted: [usize; 2],
    output: [Vec<u8>; 2],
    ended: [bool; 2],
    eof: [bool; 2],
    flush: [Option<FlushTicket>; 2],
    queues: [VecDeque<Envelope>; 2],
    capacity: [usize; 2],
    // Independent message ledger, checked against original input and read counts.
    message_offset: [usize; 2],
    delivered_offset: [usize; 2],
    credit: [usize; 2],
    clock: u64,
    step: usize,
    trace: VecDeque<String>,
    digest: u64,
    coverage: Coverage,
}
impl Case {
    fn new(seed: u64) -> Self {
        let mut random = Random(seed);
        let window = [1 + random.below(32), 1 + random.below(32)];
        let payload = 1 + random.below(32);
        let mut configs = [
            Config::new("monte-carlo", 1, Side::Initiator),
            Config::new("monte-carlo", 1, Side::Endpoint),
        ];
        for side in 0..2 {
            configs[side].receive_window = window[side];
            configs[side].peer_receive_window = window[1 - side];
            configs[side].send_buffer = 1 + random.below(64);
            configs[side].max_payload = payload;
            configs[side].coalesce_delay_ms = random.below(101) as u64;
        }
        let machines = configs
            .clone()
            .map(|config| StreamMachine::new(config).unwrap());
        let source = std::array::from_fn(|_| {
            let len = random.below(513);
            (0..len).map(|_| random.next() as u8).collect::<Vec<_>>()
        });
        let capacity = [1 + random.below(4), 1 + random.below(4)];
        let coverage = Coverage {
            tiny_windows: window.iter().filter(|value| **value == 1).count(),
            empty_streams: source.iter().filter(|bytes| bytes.is_empty()).count(),
            ..Default::default()
        };
        Self {
            random,
            configs,
            machines,
            source,
            accepted: [0; 2],
            output: Default::default(),
            ended: [false; 2],
            eof: [false; 2],
            flush: [None; 2],
            queues: Default::default(),
            capacity,
            message_offset: [0; 2],
            delivered_offset: [0; 2],
            credit: [window[1], window[0]],
            clock: 0,
            step: 0,
            trace: VecDeque::new(),
            digest: 0xcbf29ce484222325,
            coverage,
        }
    }
    fn record(&mut self, event: String) {
        for byte in event.bytes() {
            self.digest = (self.digest ^ byte as u64).wrapping_mul(0x100000001b3);
        }
        if self.trace.len() == 32 {
            self.trace.pop_front();
        }
        self.trace.push_back(format!("{}: {event}", self.step));
    }
    fn write(&mut self, side: usize) {
        if self.ended[side] {
            return;
        }
        if let Some(ticket) = self.flush[side] {
            if !self.machines[side].flush_complete(ticket).unwrap() {
                return;
            }
            self.flush[side] = None;
            self.coverage.flushes += 1;
        }
        let start = self.accepted[side];
        if start == self.source[side].len() {
            self.machines[side].end_write().unwrap();
            self.ended[side] = true;
            self.record(format!("end {side}"));
            return;
        }
        let count = self.random.below(97).min(self.source[side].len() - start);
        if count == 0 {
            self.coverage.zero_operations += 1;
        }
        let result = self.machines[side].write(&self.source[side][start..start + count]);
        self.record(format!("write {side} {count}: {result:?}"));
        match result {
            Ok(written) => {
                assert!(written <= count && (written > 0 || count == 0));
                self.accepted[side] += written;
                if written < count {
                    self.coverage.partial_writes += 1;
                }
            }
            Err(Error::WouldBlock) => {
                self.coverage.blocked_writes += 1;
            }
            result => {
                panic!("unexpected write {result:?}");
            }
        }
    }
    fn read(&mut self, side: usize) {
        let count = self.random.below(65);
        let mut bytes = vec![0; count];
        let result = self.machines[side].read(&mut bytes);
        self.record(format!("read {side} {count}: {result:?}"));
        match result {
            Ok(read) => {
                assert!(read <= count);
                if count == 0 {
                    self.coverage.zero_operations += 1;
                } else if read == 0 {
                    assert!(self.ended[1 - side]);
                    assert_eq!(self.output[side], self.source[1 - side], "premature EOF");
                    self.eof[side] = true;
                }
                self.output[side].extend_from_slice(&bytes[..read]);
                assert!(self.output[side].len() <= self.accepted[1 - side]);
                assert_eq!(
                    self.output[side],
                    self.source[1 - side][..self.output[side].len()],
                    "byte oracle"
                );
            }
            Err(Error::WouldBlock) => {
                self.coverage.blocked_reads += 1;
            }
            result => {
                panic!("unexpected read {result:?}");
            }
        }
    }
    fn take(&mut self, side: usize) {
        if self.queues[side].len() == self.capacity[side] {
            self.coverage.delayed_delivery += 1;
            return;
        }
        if let Some(message) = self.machines[side].next_message() {
            self.record(format!("take {side}: {message:?}"));
            if let Some(data) = &message.data {
                let start = self.message_offset[side];
                let end = start + data.payload.len();
                assert_eq!(data.offset, start as i64);
                assert!(
                    end <= self.credit[side],
                    "sender exceeded independently recorded credit"
                );
                assert!(end <= self.accepted[side]);
                assert_eq!(data.payload, self.source[side][start..end]);
                self.message_offset[side] = end;
                self.coverage.data_messages += 1;
            }
            if let Some(window) = &message.window {
                assert_eq!(
                    window.max_offset as usize,
                    self.output[side].len() + self.configs[side].receive_window
                );
            }
            if let Some(flushed) = &message.flushed {
                let sink_offset = if side == 0 {
                    self.delivered_offset[1]
                } else {
                    self.output[side].len()
                };
                assert!(
                    flushed.offset as usize <= sink_offset,
                    "ack before sink admission"
                );
            }
            if let Some(end) = &message.end_write {
                assert_eq!(end.final_offset as usize, self.source[side].len());
            }
            self.queues[side].push_back(message);
        }
    }
    fn deliver(&mut self, side: usize) {
        if let Some(message) = self.queues[side].pop_front() {
            self.record(format!("deliver {side}: {:?}", message.kind));
            if let Some(window) = &message.window {
                self.credit[1 - side] = window.max_offset as usize;
            }
            if let Some(data) = &message.data {
                self.delivered_offset[side] += data.payload.len();
            }
            self.machines[1 - side].receive(message).unwrap();
        }
    }
    fn run(&mut self) {
        for step in 0..100_000 {
            self.step = step;
            let action = self.random.below(16);
            let side = action % 2;
            match action / 2 {
                0 | 1 => {
                    self.write(side);
                }
                2 => {
                    self.read(side);
                }
                3 => {
                    self.take(side);
                }
                4 => {
                    self.deliver(side);
                }
                5 => {
                    if !self.ended[side] && self.flush[side].is_none() && self.random.below(8) == 0
                    {
                        self.flush[side] = Some(self.machines[side].start_flush().unwrap());
                        self.record(format!("flush {side}"));
                    }
                }
                _ => {
                    self.clock += self.random.below(51) as u64;
                    for machine in &mut self.machines {
                        machine.advance(self.clock);
                    }
                    self.record(format!("clock {}", self.clock));
                }
            }
            for side in 0..2 {
                let state = self.machines[side].stats();
                assert!(state.send_buffer <= self.configs[side].send_buffer);
                assert!(state.received_buffer <= self.configs[side].receive_window);
                assert_eq!(state.consumed as usize, self.output[side].len());
                assert!(state.received <= state.advertised_limit);
                assert!(!state.terminal);
            }
            if self.eof == [true, true] {
                assert_eq!(self.output[0], self.source[1]);
                assert_eq!(self.output[1], self.source[0]);
                return;
            }
        }
        panic!("progress limit exhausted");
    }
}

fn run_case(seed: u64, run_seed: u64, index: usize) -> (u64, Coverage) {
    let mut case = match catch_unwind(|| Case::new(seed)) {
        Ok(case) => case,
        Err(failure) => {
            eprintln!(
                "generator={GENERATOR} run_seed={run_seed:#018x} case={index} case_seed={seed:#018x} failed during construction\nReplay from gwz-transport:\nGWZ_TRANSPORT_MC_CASE_SEED={seed:#018x} cargo test --locked --test monte_carlo seeded_message_streams -- --exact --nocapture"
            );
            std::panic::resume_unwind(failure);
        }
    };
    let result = catch_unwind(AssertUnwindSafe(|| case.run()));
    if let Err(failure) = result {
        eprintln!(
            "generator={GENERATOR} run_seed={run_seed:#018x} case={index} case_seed={seed:#018x} step={} clock={}\nconfigs={:#?}\ninputs={:?}\nrecent_trace={:#?}\nReplay from gwz-transport:\nGWZ_TRANSPORT_MC_CASE_SEED={seed:#018x} cargo test --locked --test monte_carlo seeded_message_streams -- --exact --nocapture",
            case.step, case.clock, case.configs, case.source, case.trace
        );
        std::panic::resume_unwind(failure);
    }
    (case.digest, case.coverage)
}

fn campaign(seed: u64, count: usize) -> Coverage {
    eprintln!("generator={GENERATOR} run_seed={seed:#018x} cases={count}");
    let mut coverage = Coverage::default();
    for index in 0..count {
        let case_seed = case_seed(seed, index);
        let result = run_case(case_seed, seed, index);
        if index % 16 == 0 {
            assert_eq!(
                result,
                run_case(case_seed, seed, index),
                "same seed must reproduce trace and coverage; generator={GENERATOR} run_seed={seed:#018x} case={index}; replay: GWZ_TRANSPORT_MC_CASE_SEED={case_seed:#018x} cargo test --locked --test monte_carlo seeded_message_streams -- --exact --nocapture"
            );
        }
        coverage.add(&result.1);
    }
    eprintln!("coverage={coverage:?}");
    coverage
}

#[test]
fn seeded_message_streams() {
    if let Some(seed) = setting("GWZ_TRANSPORT_MC_CASE_SEED") {
        let first = run_case(seed, seed, 0);
        assert_eq!(first, run_case(seed, seed, 0));
        return;
    }
    let seed = setting("GWZ_TRANSPORT_MC_SEED").unwrap_or(RUN_SEED);
    let count = setting("GWZ_TRANSPORT_MC_CASES").unwrap_or(CASES as u64) as usize;
    assert!(count > 0);
    let coverage = campaign(seed, count);
    if seed == RUN_SEED && count >= CASES {
        assert!(coverage.tiny_windows > 100 && coverage.empty_streams > 1);
        assert!(coverage.blocked_writes > 1000 && coverage.blocked_reads > 1000);
        assert!(coverage.partial_writes > 1000 && coverage.flushes > 1000);
        assert!(coverage.zero_operations > 1000 && coverage.delayed_delivery > 1000);
        assert!(coverage.data_messages > 10000);
    }
}

#[test]
#[ignore = "long randomized campaign; seed printed before work and every failure is replayable"]
fn extended_message_streams() {
    let seed = setting("GWZ_TRANSPORT_MC_SEED").unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    });
    let count = setting("GWZ_TRANSPORT_MC_CASES").unwrap_or(50_000) as usize;
    campaign(seed, count);
}
