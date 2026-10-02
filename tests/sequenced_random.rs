//! Discrete typed messages only; no carrier, codec or physical wire.
mod support;

use gwz_transport::{
    binding,
    protocol::*,
    sequenced::{ApplyError, Direction, Error, Outbound, Receiver},
    stream::{Config, Error as StreamError, Side, StreamMachine},
};
use std::{
    collections::VecDeque,
    panic::{AssertUnwindSafe, catch_unwind},
};
use support::{Random, case_seed, setting};

const RUN_SEED: u64 = 0x4757_5a53_4551_2026;
const CASES: usize = 600;

struct Consumer {
    capacity: usize,
    pending: VecDeque<u8>,
    output: Vec<u8>,
    offset: usize,
    ended: bool,
}

fn open(id: i64) -> Envelope {
    Envelope {
        version: 3,
        session_id: "mc-session".into(),
        stream_id: id,
        kind: MessageKind::Open,
        message_seq: Some(1),
        open: Some(Open {
            endpoint_id: "endpoint".into(),
            operation_id: format!("operation-{id}"),
            destination: Destination {
                scheme: Scheme::Ssh,
                host: "example.test".into(),
                port: 22,
                path: "/repo".into(),
                ssh_username: Some("git".into()),
                https_username: None,
            },
            service: GitService::UploadPackExchange,
            identity: Identity {
                mode: IdentityMode::Ambient,
                key_path: None,
                path_base: None,
            },
            policy: AuthPolicy::SshAmbient,
            deadlines: Deadlines {
                allocation_ms: 1000,
                connect_ms: 1000,
                io_ms: 1000,
                interaction_ms: 1000,
                cleanup_ms: 1000,
            },
            receive_limits: binding::default_limits(),
        }),
        ..Default::default()
    }
}

fn frames(id: i64, source: &[u8], random: &mut Random) -> Vec<Envelope> {
    let mut messages = vec![open(id)];
    let mut offset = 0;
    while offset < source.len() {
        let count = (8 + random.below(16)).min(source.len() - offset);
        messages.push(Envelope {
            version: 3,
            session_id: "mc-session".into(),
            stream_id: id,
            kind: MessageKind::Data,
            message_seq: Some(messages.len() as i64 + 1),
            data: Some(Data {
                offset: offset as i64,
                payload: source[offset..offset + count].to_vec(),
            }),
            ..Default::default()
        });
        offset += count;
    }
    messages.push(Envelope {
        version: 3,
        session_id: "mc-session".into(),
        stream_id: id,
        kind: MessageKind::EndWrite,
        message_seq: Some(messages.len() as i64 + 1),
        end_write: Some(EndWrite {
            final_offset: source.len() as i64,
        }),
        ..Default::default()
    });
    messages
}

fn apply(consumer: &mut Consumer, message: &Envelope) -> Result<(), ApplyError> {
    match message.kind {
        MessageKind::Open => Ok(()),
        MessageKind::Data => {
            let data = message.data.as_ref().unwrap();
            assert_eq!(data.offset as usize, consumer.offset);
            if consumer.pending.len() + data.payload.len() > consumer.capacity {
                return Err(ApplyError::WouldBlock);
            }
            consumer.pending.extend(&data.payload);
            consumer.offset += data.payload.len();
            Ok(())
        }
        MessageKind::EndWrite => {
            assert_eq!(
                message.end_write.as_ref().unwrap().final_offset as usize,
                consumer.offset
            );
            consumer.ended = true;
            Ok(())
        }
        _ => Err(ApplyError::Rejected),
    }
}

fn run_case(seed: u64) {
    let mut random = Random(seed);
    let source: [Vec<u8>; 2] = std::array::from_fn(|_| {
        let length = 1 + random.below(48);
        (0..length).map(|_| random.next() as u8).collect()
    });
    let mut consumer: [Consumer; 2] = std::array::from_fn(|_| Consumer {
        capacity: 24 + random.below(24),
        pending: VecDeque::new(),
        output: Vec::new(),
        offset: 0,
        ended: false,
    });
    let mut messages = Vec::new();
    for id in 1..=2 {
        messages.extend(frames(id, &source[id as usize - 1], &mut random));
    }
    for index in (1..messages.len()).rev() {
        let other = random.below(index + 1);
        messages.swap(index, other);
    }
    let mut receiver = Receiver::new(
        "mc-session",
        Direction::Initiator,
        binding::default_limits(),
    )
    .unwrap();
    receiver.register(1).unwrap();
    receiver.register(2).unwrap();
    for step in 0..2000u64 {
        let side = random.below(2);
        let count = random.below(33).min(consumer[side].pending.len());
        for _ in 0..count {
            consumer[side]
                .output
                .push(consumer[side].pending.pop_front().unwrap());
        }
        assert_eq!(
            consumer[side].output,
            source[side][..consumer[side].output.len()],
            "byte prefix"
        );
        for id in 1..=2 {
            let index = id as usize - 1;
            receiver
                .retry(id, step, |message| apply(&mut consumer[index], message))
                .unwrap();
        }
        if !messages.is_empty() {
            let index = random.below(messages.len());
            let message = messages.swap_remove(index);
            let side = message.stream_id as usize - 1;
            match receiver.submit(message.clone(), step, |item| {
                apply(&mut consumer[side], item)
            }) {
                Ok(_) => {}
                Err(Error::WouldBlock) => messages.push(message),
                Err(error) => panic!("delivery {error:?}"),
            }
        }
        if messages.is_empty()
            && consumer.iter().all(|side| side.ended)
            && consumer.iter().all(|side| side.pending.is_empty())
        {
            for side in 0..2 {
                assert_eq!(consumer[side].output, source[side]);
            }
            return;
        }
    }
    panic!("progress limit exhausted");
}

fn run_file_case(seed: u64) {
    // Compose the current file-like machine with profile-3 ordering at the
    // typed-message boundary. The actual host adapter is a later slice.
    let mut random = Random(seed);
    let source: [Vec<u8>; 2] = std::array::from_fn(|_| {
        let length = random.below(97);
        (0..length).map(|_| random.next() as u8).collect()
    });
    let mut configs = [
        Config::new("mc-session", 1, Side::Initiator),
        Config::new("mc-session", 1, Side::Endpoint),
    ];
    for config in &mut configs {
        config.receive_window = 8;
        config.peer_receive_window = 8;
        config.send_buffer = 12;
        config.max_payload = 4;
        config.coalesce_delay_ms = 0;
        config.io_timeout_ms = 0;
    }
    let mut machines = configs.map(|config| StreamMachine::new(config).unwrap());
    let mut outbound = [
        Outbound::new(
            "mc-session",
            Direction::Initiator,
            binding::default_limits(),
            32,
        )
        .unwrap(),
        Outbound::new(
            "mc-session",
            Direction::Endpoint,
            binding::default_limits(),
            32,
        )
        .unwrap(),
    ];
    let mut inbound = [
        Receiver::new(
            "mc-session",
            Direction::Initiator,
            binding::default_limits(),
        )
        .unwrap(),
        Receiver::new("mc-session", Direction::Endpoint, binding::default_limits()).unwrap(),
    ];
    for receiver in &mut inbound {
        receiver.register(1).unwrap();
    }
    let mut opening = open(1);
    opening.message_seq = None;
    outbound[0].enqueue(opening).unwrap();
    outbound[1]
        .enqueue(Envelope {
            version: 3,
            session_id: "mc-session".into(),
            stream_id: 1,
            kind: MessageKind::Opened,
            opened: Some(Opened {
                connection_id: "connection".into(),
                endpoint_id: "endpoint".into(),
                trust_owner: "owner".into(),
                receive_limits: binding::default_limits(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .unwrap();
    for side in 0..2 {
        inbound[side]
            .submit(outbound[side].pop().unwrap(), 0, |_| Ok(()))
            .unwrap();
    }
    let mut in_flight = Vec::new();
    let mut written = [0usize; 2];
    let mut ended = [false; 2];
    let mut eof = [false; 2];
    let mut output: [Vec<u8>; 2] = Default::default();
    for step in 0..20_000u64 {
        let side = random.below(2);
        match random.below(5) {
            0 => {
                if !ended[side] {
                    if written[side] == source[side].len() {
                        machines[side].end_write().unwrap();
                        ended[side] = true;
                    } else {
                        let count = (1 + random.below(17)).min(source[side].len() - written[side]);
                        match machines[side]
                            .write(&source[side][written[side]..written[side] + count])
                        {
                            Ok(count) => written[side] += count,
                            Err(StreamError::WouldBlock) => {}
                            other => panic!("write {other:?}"),
                        }
                    }
                }
            }
            1 => {
                let mut bytes = vec![0; 1 + random.below(17)];
                match machines[side].read(&mut bytes) {
                    Ok(0) => {
                        eof[side] = true;
                        assert_eq!(output[side], source[1 - side]);
                    }
                    Ok(count) => {
                        output[side].extend_from_slice(&bytes[..count]);
                        assert_eq!(output[side], source[1 - side][..output[side].len()]);
                    }
                    Err(StreamError::WouldBlock) => {}
                    other => panic!("read {other:?}"),
                }
            }
            2 => {
                if in_flight.len() < 32
                    && let Some(mut message) = machines[side].next_message()
                {
                    message.version = 3;
                    outbound[side].enqueue(message).unwrap();
                    in_flight.push((side, outbound[side].pop().unwrap()));
                }
            }
            _ => {
                if !in_flight.is_empty() {
                    let index = random.below(in_flight.len());
                    let (sending_side, message) = in_flight.swap_remove(index);
                    let result = inbound[sending_side].submit(message.clone(), step, |item| {
                        let mut item = item.clone();
                        item.version = 1;
                        item.message_seq = None;
                        machines[1 - sending_side]
                            .receive(item)
                            .map_err(|_| ApplyError::Rejected)
                    });
                    match result {
                        Ok(_) => {}
                        Err(Error::WouldBlock) => in_flight.push((sending_side, message)),
                        Err(error) => panic!("delivery {error:?}"),
                    }
                }
            }
        }
        if eof == [true, true] {
            assert_eq!(output[0], source[1]);
            assert_eq!(output[1], source[0]);
            return;
        }
    }
    panic!("file-stream progress limit exhausted");
}

#[test]
fn seeded_reordered_bidi_message_streams() {
    if let Some(seed) = setting("GWZ_SEQUENCED_CASE_SEED") {
        run_case(seed);
        return;
    }
    let run_seed = setting("GWZ_SEQUENCED_RUN_SEED").unwrap_or(RUN_SEED);
    let cases = setting("GWZ_SEQUENCED_CASES").unwrap_or(CASES as u64) as usize;
    for index in 0..cases {
        let seed = case_seed(run_seed, index);
        if let Err(failure) = catch_unwind(AssertUnwindSafe(|| run_case(seed))) {
            eprintln!(
                "run_seed={run_seed:#018x} case={index} case_seed={seed:#018x}\nReplay: GWZ_SEQUENCED_CASE_SEED={seed:#018x} cargo test --locked --features unstable-sequenced --test sequenced_random seeded_reordered_bidi_message_streams -- --exact --nocapture"
            );
            std::panic::resume_unwind(failure);
        }
    }
}

#[test]
fn seeded_file_like_streams_through_sequencer() {
    if let Some(seed) = setting("GWZ_SEQUENCED_FILE_CASE_SEED") {
        run_file_case(seed);
        return;
    }
    for index in 0..200 {
        let seed = case_seed(RUN_SEED ^ 0xfeed_beef, index);
        if let Err(failure) = catch_unwind(AssertUnwindSafe(|| run_file_case(seed))) {
            eprintln!(
                "case={index} case_seed={seed:#018x}\nReplay: GWZ_SEQUENCED_FILE_CASE_SEED={seed:#018x} cargo test --locked --features unstable-sequenced --test sequenced_random seeded_file_like_streams_through_sequencer -- --exact --nocapture"
            );
            std::panic::resume_unwind(failure);
        }
    }
}
