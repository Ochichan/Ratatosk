use std::{collections::VecDeque, time::Duration};

use bytes::Bytes;
use criterion::{
    BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use hashbrown::HashMap;
use ratatosk_engine::{
    command::{ClientState, execute},
    keyspace::{HashFieldEntry, ServerState, SortedSet, StoredValue},
};
use ratatosk_resp::RespFrame;

fn b(raw: &'static [u8]) -> Bytes {
    Bytes::from_static(raw)
}

fn frame(parts: Vec<Bytes>) -> RespFrame {
    RespFrame::Array(
        parts
            .into_iter()
            .map(|part| RespFrame::BulkString(Some(part)))
            .collect(),
    )
}

fn setup_string_state(key_count: usize) -> (ServerState, ClientState) {
    let mut server = ServerState::with_default_dbs();
    let client = ClientState::default();
    let db = server.db_mut(0);

    for idx in 0..key_count {
        db.insert(
            Bytes::from(format!("k{idx}")),
            StoredValue::string(Bytes::from(format!("v{idx}")), None),
        );
    }

    (server, client)
}

fn setup_hash_state(field_count: usize) -> (ServerState, ClientState) {
    let mut server = ServerState::with_default_dbs();
    let client = ClientState::default();

    let mut fields = HashMap::with_capacity(field_count);
    for idx in 0..field_count {
        fields.insert(
            Bytes::from(format!("f{idx}")),
            HashFieldEntry::new(Bytes::from(format!("v{idx}"))),
        );
    }

    server
        .db_mut(0)
        .insert(b(b"h"), StoredValue::hash(fields, None));

    (server, client)
}

fn setup_list_state(len: usize) -> (ServerState, ClientState) {
    let mut server = ServerState::with_default_dbs();
    let client = ClientState::default();

    let mut list = VecDeque::with_capacity(len);
    for idx in 0..len {
        list.push_back(Bytes::from(format!("v{idx}")));
    }

    server
        .db_mut(0)
        .insert(b(b"l"), StoredValue::list(list, None));

    (server, client)
}

fn setup_zset_state(len: usize) -> (ServerState, ClientState) {
    let mut server = ServerState::with_default_dbs();
    let client = ClientState::default();

    let mut zset = SortedSet::default();
    for idx in 0..len {
        let member = Bytes::from(format!("m{idx}"));
        zset.insert(member, idx as f64);
    }

    server
        .db_mut(0)
        .insert(b(b"z"), StoredValue::sorted_set(zset, None));

    (server, client)
}

fn setup_scan_state(key_count: usize) -> (ServerState, ClientState) {
    let mut server = ServerState::with_default_dbs();
    let client = ClientState::default();
    let db = server.db_mut(0);

    for idx in 0..key_count {
        let key = if idx % 2 == 0 {
            Bytes::from(format!("scan:{idx}"))
        } else {
            Bytes::from(format!("other:{idx}"))
        };
        db.insert(key, StoredValue::string(b(b"1"), None));
    }

    (server, client)
}

fn setup_pubsub_state(channels: usize) -> (ServerState, ClientState, ClientState) {
    let mut server = ServerState::with_default_dbs();
    let mut sub_client = ClientState::new(101);
    let pub_client = ClientState::new(202);

    let mut sub_args = vec![b(b"SUBSCRIBE")];
    for idx in 0..channels {
        sub_args.push(Bytes::from(format!("ch{idx}")));
    }

    let _ = execute(frame(sub_args), &mut server, &mut sub_client);
    (server, sub_client, pub_client)
}

fn bench_connection_server(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_comprehensive_connection_server");

    group.bench_function("ping", |bench| {
        let (mut server, mut client) = setup_string_state(1);
        let ping = frame(vec![b(b"PING")]);
        bench.iter(|| {
            let outcome = execute(ping.clone(), &mut server, &mut client);
            black_box(outcome.response);
        });
    });

    group.bench_function("time", |bench| {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();
        let cmd = frame(vec![b(b"TIME")]);
        bench.iter(|| {
            let outcome = execute(cmd.clone(), &mut server, &mut client);
            black_box(outcome.response);
        });
    });

    group.bench_function("info_server", |bench| {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();
        let cmd = frame(vec![b(b"INFO"), b(b"SERVER")]);
        bench.iter(|| {
            let outcome = execute(cmd.clone(), &mut server, &mut client);
            black_box(outcome.response);
        });
    });

    group.bench_function("dbsize", |bench| {
        let (mut server, mut client) = setup_string_state(8192);
        let cmd = frame(vec![b(b"DBSIZE")]);
        bench.iter(|| {
            let outcome = execute(cmd.clone(), &mut server, &mut client);
            black_box(outcome.response);
        });
    });

    group.finish();
}

fn bench_string_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_comprehensive_string_hash");

    group.throughput(Throughput::Elements(8192));
    group.bench_function(BenchmarkId::new("string_get", 8192), |bench| {
        let (mut server, mut client) = setup_string_state(8192);
        let get = frame(vec![b(b"GET"), Bytes::from("k4096")]);
        bench.iter(|| {
            let outcome = execute(get.clone(), &mut server, &mut client);
            black_box(outcome.response);
        });
    });

    group.bench_function("string_set_overwrite", |bench| {
        let (mut server, mut client) = setup_string_state(1);
        let set = frame(vec![b(b"SET"), b(b"k0"), b(b"updated")]);
        bench.iter(|| {
            let outcome = execute(set.clone(), &mut server, &mut client);
            black_box(outcome.response);
        });
    });

    group.bench_function("string_mget_16", |bench| {
        let (mut server, mut client) = setup_string_state(8192);
        let mut args = vec![b(b"MGET")];
        for idx in 0..16 {
            args.push(Bytes::from(format!("k{}", idx * 31)));
        }
        let mget = frame(args);
        bench.iter(|| {
            let outcome = execute(mget.clone(), &mut server, &mut client);
            black_box(outcome.response);
        });
    });

    group.throughput(Throughput::Elements(4096));
    group.bench_function(BenchmarkId::new("hash_hget", 4096), |bench| {
        let (mut server, mut client) = setup_hash_state(4096);
        let hget = frame(vec![b(b"HGET"), b(b"h"), Bytes::from("f2048")]);
        bench.iter(|| {
            let outcome = execute(hget.clone(), &mut server, &mut client);
            black_box(outcome.response);
        });
    });

    group.bench_function("hash_hmget_16", |bench| {
        let (mut server, mut client) = setup_hash_state(4096);
        let mut args = vec![b(b"HMGET"), b(b"h")];
        for idx in 0..16 {
            args.push(Bytes::from(format!("f{}", idx * 17)));
        }
        let hmget = frame(args);
        bench.iter(|| {
            let outcome = execute(hmget.clone(), &mut server, &mut client);
            black_box(outcome.response);
        });
    });

    group.bench_function("hash_hgetall_512", |bench| {
        let (mut server, mut client) = setup_hash_state(512);
        let hgetall = frame(vec![b(b"HGETALL"), b(b"h")]);
        bench.iter(|| {
            let outcome = execute(hgetall.clone(), &mut server, &mut client);
            black_box(outcome.response);
        });
    });

    group.finish();
}

fn bench_list_zset(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_comprehensive_list_zset");

    group.throughput(Throughput::Elements(8192));
    group.bench_function(BenchmarkId::new("list_lrange_100", 8192), |bench| {
        let (mut server, mut client) = setup_list_state(8192);
        let cmd = frame(vec![b(b"LRANGE"), b(b"l"), b(b"0"), b(b"99")]);
        bench.iter(|| {
            let outcome = execute(cmd.clone(), &mut server, &mut client);
            black_box(outcome.response);
        });
    });

    group.bench_function("list_lindex_middle", |bench| {
        let (mut server, mut client) = setup_list_state(8192);
        let cmd = frame(vec![b(b"LINDEX"), b(b"l"), b(b"4096")]);
        bench.iter(|| {
            let outcome = execute(cmd.clone(), &mut server, &mut client);
            black_box(outcome.response);
        });
    });

    group.bench_function(BenchmarkId::new("zset_zrange_100", 8192), |bench| {
        let (mut server, mut client) = setup_zset_state(8192);
        let cmd = frame(vec![b(b"ZRANGE"), b(b"z"), b(b"0"), b(b"99")]);
        bench.iter(|| {
            let outcome = execute(cmd.clone(), &mut server, &mut client);
            black_box(outcome.response);
        });
    });

    group.bench_function("zset_zscore_middle", |bench| {
        let (mut server, mut client) = setup_zset_state(8192);
        let cmd = frame(vec![b(b"ZSCORE"), b(b"z"), Bytes::from("m4096")]);
        bench.iter(|| {
            let outcome = execute(cmd.clone(), &mut server, &mut client);
            black_box(outcome.response);
        });
    });

    group.finish();
}

fn bench_key_scan_pubsub(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_comprehensive_key_scan_pubsub");

    group.throughput(Throughput::Elements(20000));
    group.bench_function(BenchmarkId::new("scan_match_count100", 20000), |bench| {
        let (mut server, mut client) = setup_scan_state(20000);
        let scan = frame(vec![
            b(b"SCAN"),
            b(b"0"),
            b(b"MATCH"),
            b(b"scan:*"),
            b(b"COUNT"),
            b(b"100"),
        ]);
        bench.iter(|| {
            let outcome = execute(scan.clone(), &mut server, &mut client);
            black_box(outcome.response);
        });
    });

    group.bench_function("exists_32", |bench| {
        let (mut server, mut client) = setup_string_state(8192);
        let mut args = vec![b(b"EXISTS")];
        for idx in 0..16 {
            args.push(Bytes::from(format!("k{idx}")));
        }
        for idx in 0..16 {
            args.push(Bytes::from(format!("missing{idx}")));
        }
        let exists = frame(args);
        bench.iter(|| {
            let outcome = execute(exists.clone(), &mut server, &mut client);
            black_box(outcome.response);
        });
    });

    group.bench_function("publish_1sub", |bench| {
        let (mut server, sub_client, mut pub_client) = setup_pubsub_state(1);
        let sub_id = sub_client.id();
        let publish = frame(vec![b(b"PUBLISH"), b(b"ch0"), b(b"payload")]);
        bench.iter(|| {
            let outcome = execute(publish.clone(), &mut server, &mut pub_client);
            let drained = server.pubsub.drain_messages(sub_id);
            black_box(drained.len());
            black_box(outcome.response);
        });
    });

    group.bench_function("pubsub_numsub_16", |bench| {
        let (mut server, _sub_client, mut pubsub_client) = setup_pubsub_state(16);
        let mut args = vec![b(b"PUBSUB"), b(b"NUMSUB")];
        for idx in 0..16 {
            args.push(Bytes::from(format!("ch{idx}")));
        }
        let numsub = frame(args);
        bench.iter(|| {
            let outcome = execute(numsub.clone(), &mut server, &mut pubsub_client);
            black_box(outcome.response);
        });
    });

    group.finish();
}

fn criterion_config() -> Criterion {
    Criterion::default()
        .sample_size(20)
        .warm_up_time(Duration::from_millis(800))
        .measurement_time(Duration::from_secs(2))
}

criterion_group! {
    name = comprehensive_benches;
    config = criterion_config();
    targets = bench_connection_server, bench_string_hash, bench_list_zset, bench_key_scan_pubsub
}
criterion_main!(comprehensive_benches);
