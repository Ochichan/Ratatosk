use bytes::BytesMut;
use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use ratatosk_engine::{
    command::{ClientState, ServerAccess, execute},
    keyspace::ServerState,
};
use ratatosk_resp::{RespFrame, encode_to_vec, parse};

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

fn parse_payload_frames(payload: &[u8]) -> Vec<RespFrame> {
    let mut input = BytesMut::from(payload);
    let mut frames = Vec::new();
    while let Some(frame) = parse(&mut input).expect("valid RESP") {
        frames.push(frame);
    }
    frames
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
                        let mut encoded = Vec::with_capacity(input.len());
                        while let Some(frame) = parse(&mut input).expect("valid RESP") {
                            let mut access = ServerAccess::new_inline(&mut server);
                            let outcome = execute(frame, &mut access, &mut client);
                            encode_to_vec(&outcome.response, &mut encoded);
                        }
                        black_box(encoded.len());
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
                        let mut encoded = Vec::with_capacity(input.len());
                        while let Some(frame) = parse(&mut input).expect("valid RESP") {
                            let mut access = ServerAccess::new_inline(&mut server);
                            let outcome = execute(frame, &mut access, &mut client);
                            encode_to_vec(&outcome.response, &mut encoded);
                        }
                        black_box(encoded.len());
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_pipeline_set_parse_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_set_parse_only");

    for &pipeline_len in &[1usize, 32, 256, 1024] {
        let payload = build_set_pipeline(pipeline_len);
        group.throughput(Throughput::Elements(pipeline_len as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(pipeline_len),
            &payload,
            |b, payload| {
                b.iter_batched(
                    || BytesMut::from(payload.as_slice()),
                    |mut input| {
                        let mut parsed = 0usize;
                        while let Some(frame) = parse(&mut input).expect("valid RESP") {
                            parsed = parsed.saturating_add(1);
                            black_box(frame);
                        }
                        black_box(parsed);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_pipeline_ping_parse_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_ping_parse_only");

    for &pipeline_len in &[1usize, 32, 256, 1024] {
        let payload = build_ping_pipeline(pipeline_len);
        group.throughput(Throughput::Elements(pipeline_len as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(pipeline_len),
            &payload,
            |b, payload| {
                b.iter_batched(
                    || BytesMut::from(payload.as_slice()),
                    |mut input| {
                        let mut parsed = 0usize;
                        while let Some(frame) = parse(&mut input).expect("valid RESP") {
                            parsed = parsed.saturating_add(1);
                            black_box(frame);
                        }
                        black_box(parsed);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_pipeline_set_execute_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_set_execute_only");

    for &pipeline_len in &[1usize, 32, 256, 1024] {
        let payload = build_set_pipeline(pipeline_len);
        let frames = parse_payload_frames(&payload);
        group.throughput(Throughput::Elements(pipeline_len as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(pipeline_len),
            &frames,
            |b, frames| {
                b.iter_batched(
                    || {
                        (
                            ServerState::with_default_dbs(),
                            ClientState::default(),
                            frames.clone(),
                        )
                    },
                    |(mut server, mut client, frames)| {
                        let mut replies = 0usize;
                        for frame in frames {
                            let mut access = ServerAccess::new_inline(&mut server);
                            let outcome = execute(frame, &mut access, &mut client);
                            replies = replies.saturating_add(1);
                            black_box(outcome.response);
                        }
                        black_box(replies);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_pipeline_ping_execute_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_ping_execute_only");

    for &pipeline_len in &[1usize, 32, 256, 1024] {
        let payload = build_ping_pipeline(pipeline_len);
        let frames = parse_payload_frames(&payload);
        group.throughput(Throughput::Elements(pipeline_len as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(pipeline_len),
            &frames,
            |b, frames| {
                b.iter_batched(
                    || {
                        (
                            ServerState::with_default_dbs(),
                            ClientState::default(),
                            frames.clone(),
                        )
                    },
                    |(mut server, mut client, frames)| {
                        let mut replies = 0usize;
                        for frame in frames {
                            let mut access = ServerAccess::new_inline(&mut server);
                            let outcome = execute(frame, &mut access, &mut client);
                            replies = replies.saturating_add(1);
                            black_box(outcome.response);
                        }
                        black_box(replies);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_pipeline_set_encode_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_set_encode_only");

    for &pipeline_len in &[1usize, 32, 256, 1024] {
        let responses = vec![RespFrame::simple_str("OK"); pipeline_len];
        group.throughput(Throughput::Elements(pipeline_len as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(pipeline_len),
            &responses,
            |b, responses| {
                b.iter(|| {
                    let mut out = Vec::with_capacity(pipeline_len * 8);
                    for response in responses {
                        encode_to_vec(response, &mut out);
                    }
                    black_box(out.len());
                });
            },
        );
    }

    group.finish();
}

fn bench_pipeline_ping_encode_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_ping_encode_only");

    for &pipeline_len in &[1usize, 32, 256, 1024] {
        let responses = vec![RespFrame::simple_str("PONG"); pipeline_len];
        group.throughput(Throughput::Elements(pipeline_len as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(pipeline_len),
            &responses,
            |b, responses| {
                b.iter(|| {
                    let mut out = Vec::with_capacity(pipeline_len * 8);
                    for response in responses {
                        encode_to_vec(response, &mut out);
                    }
                    black_box(out.len());
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    pipeline_benches,
    bench_pipeline_set,
    bench_pipeline_ping,
    bench_pipeline_set_parse_only,
    bench_pipeline_ping_parse_only,
    bench_pipeline_set_execute_only,
    bench_pipeline_ping_execute_only,
    bench_pipeline_set_encode_only,
    bench_pipeline_ping_encode_only
);
criterion_main!(pipeline_benches);
