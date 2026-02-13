use std::time::Duration;

use bytes::Bytes;
use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use hashbrown::{HashMap, HashSet};
use ratatosk_engine::{
    command::{ClientState, execute},
    keyspace::{
        ServerState, StoredValue, StreamConsumer, StreamEntry, StreamGroup, StreamId,
        StreamPendingEntry,
    },
};
use ratatosk_resp::RespFrame;

fn bs(raw: &'static [u8]) -> Bytes {
    Bytes::from_static(raw)
}

fn cmd_frame(parts: Vec<Bytes>) -> RespFrame {
    RespFrame::Array(
        parts
            .into_iter()
            .map(|part| RespFrame::BulkString(Some(part)))
            .collect(),
    )
}

fn setup_set_state(size: usize) -> (ServerState, ClientState) {
    let mut server = ServerState::with_default_dbs();
    let client = ClientState::default();
    let db = server.db_mut(0);

    let mut s1 = HashSet::with_capacity(size);
    let mut s2 = HashSet::with_capacity(size);
    let mut s3 = HashSet::with_capacity(size);

    for i in 0..size {
        s1.insert(Bytes::from(format!("m{i}")));
        s2.insert(Bytes::from(format!("m{}", i + (size / 3))));
        s3.insert(Bytes::from(format!("m{}", i + (size / 2))));
    }

    db.insert(bs(b"s1"), StoredValue::set(s1, None));
    db.insert(bs(b"s2"), StoredValue::set(s2, None));
    db.insert(bs(b"s3"), StoredValue::set(s3, None));

    (server, client)
}

fn build_stream_entries(entry_count: usize) -> Vec<StreamEntry> {
    let mut entries = Vec::with_capacity(entry_count);
    for seq in 0..entry_count {
        entries.push(StreamEntry {
            id: StreamId {
                ms: 1,
                seq: seq as i64,
            },
            fields: vec![(bs(b"f"), bs(b"v"))],
        });
    }
    entries
}

fn setup_stream_state(entry_count: usize, pending_count: usize) -> (ServerState, ClientState) {
    let mut server = ServerState::with_default_dbs();
    let client = ClientState::default();

    let key = bs(b"s");
    let group_name = bs(b"g");
    let consumer_name = bs(b"c1");

    server.db_mut(0).insert(
        key.clone(),
        StoredValue::stream(build_stream_entries(entry_count), None),
    );

    let Some(entry) = server.db_mut(0).get_mut(&key) else {
        panic!("stream not inserted");
    };
    let Some(groups) = entry.as_stream_groups_mut() else {
        panic!("not a stream");
    };

    let pending_len = pending_count.min(entry_count);
    let mut consumer_pending = HashSet::with_capacity(pending_len);
    let mut pending = HashMap::with_capacity(pending_len);

    for seq in 0..pending_len {
        let id = StreamId {
            ms: 1,
            seq: seq as i64,
        };
        consumer_pending.insert(id);
        pending.insert(
            id,
            StreamPendingEntry {
                consumer: consumer_name.clone(),
                deliveries: 1,
                last_delivered_ms: 0,
            },
        );
    }

    let mut consumers = HashMap::new();
    consumers.insert(
        consumer_name,
        StreamConsumer {
            seen_time_ms: 0,
            pending: consumer_pending,
        },
    );

    let last_delivered_id = if pending_len == 0 {
        StreamId { ms: 0, seq: 0 }
    } else {
        StreamId {
            ms: 1,
            seq: pending_len as i64 - 1,
        }
    };

    groups.insert(
        group_name,
        StreamGroup {
            last_delivered_id,
            consumers,
            pending,
        },
    );

    (server, client)
}

fn bench_set_hotpaths(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_set_hotpaths_execute");

    for &size in &[1024usize, 4096] {
        group.throughput(Throughput::Elements(size as u64));

        let sinter_frame = cmd_frame(vec![bs(b"SINTER"), bs(b"s1"), bs(b"s2"), bs(b"s3")]);
        group.bench_with_input(BenchmarkId::new("sinter", size), &size, |b, &size| {
            b.iter_batched(
                || setup_set_state(size),
                |(mut server, mut client)| {
                    let outcome = execute(sinter_frame.clone(), &mut server, &mut client);
                    black_box(outcome.response);
                },
                BatchSize::SmallInput,
            );
        });

        let sinterstore_frame = cmd_frame(vec![
            bs(b"SINTERSTORE"),
            bs(b"dst"),
            bs(b"s1"),
            bs(b"s2"),
            bs(b"s3"),
        ]);
        group.bench_with_input(BenchmarkId::new("sinterstore", size), &size, |b, &size| {
            b.iter_batched(
                || setup_set_state(size),
                |(mut server, mut client)| {
                    let outcome = execute(sinterstore_frame.clone(), &mut server, &mut client);
                    black_box(outcome.response);
                },
                BatchSize::SmallInput,
            );
        });

        let pick_count = (size / 4).max(1);
        let srandmember_neg_frame = cmd_frame(vec![
            bs(b"SRANDMEMBER"),
            bs(b"s1"),
            Bytes::from(format!("-{pick_count}")),
        ]);
        group.bench_with_input(
            BenchmarkId::new("srandmember_neg", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || setup_set_state(size),
                    |(mut server, mut client)| {
                        let outcome =
                            execute(srandmember_neg_frame.clone(), &mut server, &mut client);
                        black_box(outcome.response);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_stream_hotpaths(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_stream_hotpaths_execute");

    for &entry_count in &[2048usize, 8192] {
        group.throughput(Throughput::Elements(entry_count as u64));

        let xreadgroup_frame = cmd_frame(vec![
            bs(b"XREADGROUP"),
            bs(b"GROUP"),
            bs(b"g"),
            bs(b"c1"),
            bs(b"COUNT"),
            bs(b"128"),
            bs(b"STREAMS"),
            bs(b"s"),
            bs(b">"),
        ]);
        group.bench_with_input(
            BenchmarkId::new("xreadgroup", entry_count),
            &entry_count,
            |b, &entry_count| {
                b.iter_batched(
                    || setup_stream_state(entry_count, 0),
                    |(mut server, mut client)| {
                        let outcome = execute(xreadgroup_frame.clone(), &mut server, &mut client);
                        black_box(outcome.response);
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        let claim_count = 128usize;
        let mut xclaim_parts = vec![bs(b"XCLAIM"), bs(b"s"), bs(b"g"), bs(b"c2"), bs(b"0")];
        xclaim_parts.extend((0..claim_count).map(|seq| Bytes::from(format!("1-{seq}"))));
        let xclaim_frame = cmd_frame(xclaim_parts);

        group.bench_with_input(
            BenchmarkId::new("xclaim_128", entry_count),
            &entry_count,
            |b, &entry_count| {
                b.iter_batched(
                    || setup_stream_state(entry_count, 512),
                    |(mut server, mut client)| {
                        let outcome = execute(xclaim_frame.clone(), &mut server, &mut client);
                        black_box(outcome.response);
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        let xautoclaim_frame = cmd_frame(vec![
            bs(b"XAUTOCLAIM"),
            bs(b"s"),
            bs(b"g"),
            bs(b"c2"),
            bs(b"0"),
            bs(b"1-0"),
            bs(b"COUNT"),
            bs(b"128"),
        ]);
        group.bench_with_input(
            BenchmarkId::new("xautoclaim_128", entry_count),
            &entry_count,
            |b, &entry_count| {
                b.iter_batched(
                    || setup_stream_state(entry_count, 1024),
                    |(mut server, mut client)| {
                        let outcome = execute(xautoclaim_frame.clone(), &mut server, &mut client);
                        black_box(outcome.response);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn criterion_config() -> Criterion {
    Criterion::default()
        .sample_size(30)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
}

criterion_group! {
    name = hotpath_benches;
    config = criterion_config();
    targets = bench_set_hotpaths, bench_stream_hotpaths
}
criterion_main!(hotpath_benches);
