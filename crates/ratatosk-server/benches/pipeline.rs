use bytes::BytesMut;
use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use ratatosk_engine::{
    command::{ClientState, execute},
    keyspace::ServerState,
};
use ratatosk_resp::{encode, parse};

fn append_resp_command(out: &mut Vec<u8>, parts: &[&[u8]]) {
    out.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
    for part in parts {
        out.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        out.extend_from_slice(part);
        out.extend_from_slice(b"\r\n");
    }
}

fn build_set_pipeline(len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len * 32);
    for i in 0..len {
        let key = format!("k{i}");
        append_resp_command(&mut out, &[b"SET", key.as_bytes(), b"v"]);
    }
    out
}

fn build_ping_pipeline(len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len * 16);
    for _ in 0..len {
        append_resp_command(&mut out, &[b"PING"]);
    }
    out
}

fn bench_pipeline_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_set_parse_execute_encode");

    for &pipeline_len in &[1usize, 32, 256, 1024] {
        let payload = build_set_pipeline(pipeline_len);
        group.throughput(Throughput::Elements(pipeline_len as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(pipeline_len),
            &payload,
            |b, payload| {
                b.iter_batched(
                    || {
                        (
                            ServerState::with_default_dbs(),
                            ClientState::default(),
                            BytesMut::from(payload.as_slice()),
                        )
                    },
                    |(mut server, mut client, mut input)| {
                        let mut encoded_bytes = 0usize;
                        while let Some(frame) = parse(&mut input).expect("valid RESP") {
                            let outcome = execute(frame, &mut server, &mut client);
                            encoded_bytes += encode(&outcome.response).len();
                        }
                        black_box(encoded_bytes);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_pipeline_ping(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_ping_parse_execute_encode");

    for &pipeline_len in &[1usize, 32, 256, 1024] {
        let payload = build_ping_pipeline(pipeline_len);
        group.throughput(Throughput::Elements(pipeline_len as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(pipeline_len),
            &payload,
            |b, payload| {
                b.iter_batched(
                    || {
                        (
                            ServerState::with_default_dbs(),
                            ClientState::default(),
                            BytesMut::from(payload.as_slice()),
                        )
                    },
                    |(mut server, mut client, mut input)| {
                        let mut encoded_bytes = 0usize;
                        while let Some(frame) = parse(&mut input).expect("valid RESP") {
                            let outcome = execute(frame, &mut server, &mut client);
                            encoded_bytes += encode(&outcome.response).len();
                        }
                        black_box(encoded_bytes);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(pipeline_benches, bench_pipeline_set, bench_pipeline_ping);
criterion_main!(pipeline_benches);
