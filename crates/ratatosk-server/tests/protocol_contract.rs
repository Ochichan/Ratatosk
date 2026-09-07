use std::{net::SocketAddr, sync::Arc, time::Duration};

use ratatosk_engine::keyspace::{ServerState, SharedState};
use ratatosk_server::client::handle_client;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};

fn bulk(value: &str) -> Vec<u8> {
    let mut out = format!("${}\r\n", value.len()).into_bytes();
    out.extend_from_slice(value.as_bytes());
    out.extend_from_slice(b"\r\n");
    out
}

fn integer(value: i64) -> Vec<u8> {
    format!(":{value}\r\n").into_bytes()
}

fn aggregate(marker: u8, parts: &[Vec<u8>]) -> Vec<u8> {
    let mut out = format!("{}{}\r\n", char::from(marker), parts.len()).into_bytes();
    for part in parts {
        out.extend_from_slice(part);
    }
    out
}

fn command(parts: &[&str]) -> Vec<u8> {
    aggregate(
        b'*',
        &parts.iter().map(|part| bulk(part)).collect::<Vec<_>>(),
    )
}

fn hello(version: i64, client_id: i64) -> Vec<u8> {
    let entries = [
        (bulk("server"), bulk("ratatosk")),
        (bulk("version"), bulk(env!("CARGO_PKG_VERSION"))),
        (bulk("proto"), integer(version)),
        (bulk("id"), integer(client_id)),
        (bulk("mode"), bulk("standalone")),
        (bulk("role"), bulk("master")),
        (bulk("modules"), aggregate(b'*', &[])),
    ];

    if version == 3 {
        let mut out = format!("%{}\r\n", entries.len()).into_bytes();
        for (key, value) in entries {
            out.extend_from_slice(&key);
            out.extend_from_slice(&value);
        }
        out
    } else {
        let mut parts = Vec::with_capacity(entries.len() * 2);
        for (key, value) in entries {
            parts.push(key);
            parts.push(value);
        }
        aggregate(b'*', &parts)
    }
}

fn pubsub_ack(version: i64, kind: &str, target: &str, count: i64) -> Vec<u8> {
    aggregate(
        if version == 3 { b'>' } else { b'*' },
        &[bulk(kind), bulk(target), integer(count)],
    )
}

async fn expect_wire(stream: &mut TcpStream, expected: &[u8]) {
    let mut actual = vec![0; expected.len()];
    timeout(Duration::from_secs(1), stream.read_exact(&mut actual))
        .await
        .expect("wire reply timeout")
        .expect("read wire reply");
    assert_eq!(actual, expected);
}

async fn start_server(connection_count: usize) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback listener");
    let addr = listener.local_addr().expect("listener address");
    let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));

    let server_task = tokio::spawn(async move {
        let mut client_tasks = Vec::with_capacity(connection_count);
        for _ in 0..connection_count {
            let (socket, _) = listener.accept().await.expect("accept client");
            let shared = Arc::clone(&shared);
            client_tasks.push(tokio::spawn(async move {
                handle_client(socket, shared)
                    .await
                    .expect("handle client connection");
            }));
        }

        for client_task in client_tasks {
            client_task.await.expect("join client connection");
        }
    });

    (addr, server_task)
}

async fn finish_server(server_task: tokio::task::JoinHandle<()>) {
    timeout(Duration::from_secs(1), server_task)
        .await
        .expect("server shutdown timeout")
        .expect("server task join");
}

#[tokio::test]
async fn hello_replies_follow_each_negotiated_protocol_in_a_pipeline() {
    let (addr, server_task) = start_server(1).await;
    let mut client = TcpStream::connect(addr).await.expect("connect client");

    let mut pipeline = command(&["HELLO", "3"]);
    pipeline.extend(command(&["HELLO", "2"]));
    pipeline.extend(command(&["PING"]));
    pipeline.extend(command(&["HELLO", "3"]));
    pipeline.extend(command(&["PING"]));
    client.write_all(&pipeline).await.expect("write pipeline");

    expect_wire(&mut client, &hello(3, 1)).await;
    expect_wire(&mut client, &hello(2, 1)).await;
    expect_wire(&mut client, b"+PONG\r\n").await;
    expect_wire(&mut client, &hello(3, 1)).await;
    expect_wire(&mut client, b"+PONG\r\n").await;

    drop(client);
    finish_server(server_task).await;
}

#[tokio::test]
async fn hgetall_projects_consistently_for_fast_batch_and_exec_replies() {
    let (addr, server_task) = start_server(1).await;
    let mut client = TcpStream::connect(addr).await.expect("connect client");

    client
        .write_all(&command(&["HSET", "hash", "field", "value"]))
        .await
        .expect("seed hash");
    expect_wire(&mut client, b":1\r\n").await;

    client
        .write_all(&command(&["HGETALL", "hash"]))
        .await
        .expect("read fast hgetall");
    let resp2_hash = aggregate(b'*', &[bulk("field"), bulk("value")]);
    expect_wire(&mut client, &resp2_hash).await;

    let mut transaction = command(&["MULTI"]);
    transaction.extend(command(&["HGETALL", "hash"]));
    transaction.extend(command(&["EXEC"]));
    client
        .write_all(&transaction)
        .await
        .expect("run hgetall transaction");
    expect_wire(&mut client, b"+OK\r\n").await;
    expect_wire(&mut client, b"+QUEUED\r\n").await;
    expect_wire(
        &mut client,
        &aggregate(b'*', std::slice::from_ref(&resp2_hash)),
    )
    .await;

    client
        .write_all(&command(&["HELLO", "3"]))
        .await
        .expect("negotiate resp3");
    expect_wire(&mut client, &hello(3, 1)).await;

    let mut readonly_batch = command(&["HGETALL", "hash"]);
    readonly_batch.extend(command(&["HGETALL", "hash"]));
    client
        .write_all(&readonly_batch)
        .await
        .expect("run readonly hgetall batch");
    let resp3_hash = {
        let mut out = b"%1\r\n".to_vec();
        out.extend(bulk("field"));
        out.extend(bulk("value"));
        out
    };
    expect_wire(&mut client, &resp3_hash).await;
    expect_wire(&mut client, &resp3_hash).await;

    drop(client);
    finish_server(server_task).await;
}

#[tokio::test]
async fn watch_conflict_exec_is_a_resp2_null_array() {
    let (addr, server_task) = start_server(2).await;
    let mut watched = TcpStream::connect(addr)
        .await
        .expect("connect watched client");
    let mut writer = TcpStream::connect(addr)
        .await
        .expect("connect writer client");

    watched
        .write_all(&command(&["SET", "watched", "one"]))
        .await
        .expect("seed watched key");
    expect_wire(&mut watched, b"+OK\r\n").await;
    watched
        .write_all(&command(&["WATCH", "watched"]))
        .await
        .expect("watch key");
    expect_wire(&mut watched, b"+OK\r\n").await;
    watched
        .write_all(&command(&["MULTI"]))
        .await
        .expect("start transaction");
    expect_wire(&mut watched, b"+OK\r\n").await;
    watched
        .write_all(&command(&["SET", "watched", "queued"]))
        .await
        .expect("queue transaction write");
    expect_wire(&mut watched, b"+QUEUED\r\n").await;

    writer
        .write_all(&command(&["SET", "watched", "changed"]))
        .await
        .expect("conflict write");
    expect_wire(&mut writer, b"+OK\r\n").await;

    watched
        .write_all(&command(&["EXEC"]))
        .await
        .expect("execute conflicted transaction");
    expect_wire(&mut watched, b"*-1\r\n").await;

    drop(watched);
    drop(writer);
    finish_server(server_task).await;
}

#[tokio::test]
async fn pubsub_acknowledgements_are_independent_frames_in_resp2_and_pushes_in_resp3() {
    let (addr, server_task) = start_server(2).await;
    let mut subscriber = TcpStream::connect(addr).await.expect("connect subscriber");
    let mut publisher = TcpStream::connect(addr).await.expect("connect publisher");

    subscriber
        .write_all(&command(&["SUBSCRIBE", "a", "b"]))
        .await
        .expect("subscribe to two channels");
    expect_wire(&mut subscriber, &pubsub_ack(2, "subscribe", "a", 1)).await;
    expect_wire(&mut subscriber, &pubsub_ack(2, "subscribe", "b", 2)).await;

    subscriber
        .write_all(&command(&["PSUBSCRIBE", "p:1", "p:2"]))
        .await
        .expect("subscribe to patterns");
    expect_wire(&mut subscriber, &pubsub_ack(2, "psubscribe", "p:1", 3)).await;
    expect_wire(&mut subscriber, &pubsub_ack(2, "psubscribe", "p:2", 4)).await;

    subscriber
        .write_all(&command(&["SSUBSCRIBE", "s:1", "s:2"]))
        .await
        .expect("subscribe to shard channels");
    expect_wire(&mut subscriber, &pubsub_ack(2, "ssubscribe", "s:1", 1)).await;
    expect_wire(&mut subscriber, &pubsub_ack(2, "ssubscribe", "s:2", 2)).await;

    publisher
        .write_all(&command(&["PUBLISH", "a", "message"]))
        .await
        .expect("publish message");
    expect_wire(&mut publisher, b":1\r\n").await;
    expect_wire(
        &mut subscriber,
        &aggregate(b'*', &[bulk("message"), bulk("a"), bulk("message")]),
    )
    .await;

    subscriber
        .write_all(&command(&["UNSUBSCRIBE", "a", "b"]))
        .await
        .expect("unsubscribe from channels");
    expect_wire(&mut subscriber, &pubsub_ack(2, "unsubscribe", "a", 3)).await;
    expect_wire(&mut subscriber, &pubsub_ack(2, "unsubscribe", "b", 2)).await;

    subscriber
        .write_all(&command(&["PUNSUBSCRIBE", "p:1", "p:2"]))
        .await
        .expect("unsubscribe from patterns");
    expect_wire(&mut subscriber, &pubsub_ack(2, "punsubscribe", "p:1", 1)).await;
    expect_wire(&mut subscriber, &pubsub_ack(2, "punsubscribe", "p:2", 0)).await;

    subscriber
        .write_all(&command(&["SUNSUBSCRIBE", "s:1", "s:2"]))
        .await
        .expect("unsubscribe from shard channels");
    expect_wire(&mut subscriber, &pubsub_ack(2, "sunsubscribe", "s:1", 1)).await;
    expect_wire(&mut subscriber, &pubsub_ack(2, "sunsubscribe", "s:2", 0)).await;

    subscriber
        .write_all(&command(&["HELLO", "3"]))
        .await
        .expect("negotiate subscriber to resp3");
    expect_wire(&mut subscriber, &hello(3, 1)).await;
    subscriber
        .write_all(&command(&["SUBSCRIBE", "resp3"]))
        .await
        .expect("subscribe in resp3");
    expect_wire(&mut subscriber, &pubsub_ack(3, "subscribe", "resp3", 1)).await;

    publisher
        .write_all(&command(&["PUBLISH", "resp3", "body"]))
        .await
        .expect("publish resp3 message");
    expect_wire(&mut publisher, b":1\r\n").await;
    expect_wire(
        &mut subscriber,
        &aggregate(b'>', &[bulk("message"), bulk("resp3"), bulk("body")]),
    )
    .await;

    drop(subscriber);
    drop(publisher);
    finish_server(server_task).await;
}

#[tokio::test]
async fn exec_preserves_independent_pubsub_acknowledgements_in_both_protocols() {
    for version in [2, 3] {
        let (addr, server_task) = start_server(1).await;
        let mut client = TcpStream::connect(addr).await.expect("connect client");
        if version == 3 {
            client
                .write_all(&command(&["HELLO", "3"]))
                .await
                .expect("hello");
            expect_wire(&mut client, &hello(3, 1)).await;
        }
        let mut pipeline = command(&["MULTI"]);
        pipeline.extend_from_slice(&command(&["SUBSCRIBE", "a", "b"]));
        pipeline.extend_from_slice(&command(&["EXEC"]));
        client.write_all(&pipeline).await.expect("transaction");
        expect_wire(&mut client, b"+OK\r\n+QUEUED\r\n*1\r\n").await;
        expect_wire(&mut client, &pubsub_ack(version, "subscribe", "a", 1)).await;
        expect_wire(&mut client, &pubsub_ack(version, "subscribe", "b", 2)).await;
        drop(client);
        finish_server(server_task).await;
    }
}

#[tokio::test]
async fn hello_inside_exec_preserves_each_completed_reply_protocol() {
    for (before, after) in [(2, 3), (3, 2)] {
        let (addr, server_task) = start_server(1).await;
        let mut client = TcpStream::connect(addr).await.expect("connect client");
        if before == 3 {
            client
                .write_all(&command(&["HELLO", "3"]))
                .await
                .expect("hello");
            expect_wire(&mut client, &hello(3, 1)).await;
        }
        client
            .write_all(&command(&["HSET", "h", "k", "v"]))
            .await
            .expect("seed");
        expect_wire(&mut client, b":1\r\n").await;
        let mut pipeline = command(&["MULTI"]);
        pipeline.extend_from_slice(&command(&["HGETALL", "h"]));
        pipeline.extend_from_slice(&command(&["HELLO", if after == 3 { "3" } else { "2" }]));
        pipeline.extend_from_slice(&command(&["HGETALL", "h"]));
        pipeline.extend_from_slice(&command(&["EXEC"]));
        client.write_all(&pipeline).await.expect("transaction");
        expect_wire(
            &mut client,
            b"+OK\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n*3\r\n",
        )
        .await;
        let map = b"%1\r\n$1\r\nk\r\n$1\r\nv\r\n";
        let array = b"*2\r\n$1\r\nk\r\n$1\r\nv\r\n";
        expect_wire(&mut client, if before == 3 { map } else { array }).await;
        expect_wire(&mut client, &hello(after, 1)).await;
        expect_wire(&mut client, if after == 3 { map } else { array }).await;
        drop(client);
        finish_server(server_task).await;
    }
}
