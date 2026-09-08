use std::{future::Future, io, net::SocketAddr, pin::Pin, sync::Arc, time::Duration};

#[cfg(unix)]
use std::path::{Path, PathBuf};

use ratatosk_engine::keyspace::{ServerState, SharedState};
use ratatosk_server::{
    client::{ClientIoLimits, handle_client_with_limits},
    config::ServerConfig,
    persistence::PersistenceRuntime,
    transport::{ConnInfo, SessionStream},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};

#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};

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

async fn expect_wire<S>(stream: &mut S, expected: &[u8])
where
    S: AsyncRead + Unpin,
{
    let mut actual = vec![0; expected.len()];
    timeout(Duration::from_secs(1), stream.read_exact(&mut actual))
        .await
        .expect("wire reply timeout")
        .expect("read wire reply");
    assert_eq!(actual, expected);
}

type AcceptFuture<'a, S> = Pin<Box<dyn Future<Output = io::Result<(S, ConnInfo)>> + Send + 'a>>;

fn start_server<L, S, Accept>(
    listener: L,
    connection_count: usize,
    accept: Accept,
) -> tokio::task::JoinHandle<()>
where
    L: Send + 'static,
    S: SessionStream + Sync + 'static,
    Accept: for<'a> Fn(&'a L) -> AcceptFuture<'a, S> + Send + 'static,
{
    let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
    let persistence = Arc::new(
        PersistenceRuntime::from_config(&ServerConfig::default()).expect("persistence runtime"),
    );

    tokio::spawn(async move {
        let mut client_tasks = Vec::with_capacity(connection_count);
        for _ in 0..connection_count {
            let (socket, info) = accept(&listener).await.expect("accept client");
            let shared = Arc::clone(&shared);
            let persistence = Arc::clone(&persistence);
            client_tasks.push(tokio::spawn(async move {
                handle_client_with_limits(
                    socket,
                    info,
                    shared,
                    persistence,
                    ClientIoLimits::default(),
                )
                .await
                .expect("handle client connection");
            }));
        }

        for client_task in client_tasks {
            client_task.await.expect("join client connection");
        }
    })
}

fn accept_tcp(listener: &TcpListener) -> AcceptFuture<'_, TcpStream> {
    Box::pin(async move {
        let (stream, _) = listener.accept().await?;
        let _ = stream.set_nodelay(true);
        let info = ConnInfo::from_tcp(&stream);
        Ok((stream, info))
    })
}

async fn start_tcp_server(connection_count: usize) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback listener");
    let addr = listener.local_addr().expect("listener address");
    let server_task = start_server(listener, connection_count, accept_tcp);

    (addr, server_task)
}

#[cfg(unix)]
struct UnixContractListener {
    listener: UnixListener,
    path: PathBuf,
}

#[cfg(unix)]
fn accept_unix(listener: &UnixContractListener) -> AcceptFuture<'_, UnixStream> {
    let path = listener.path.clone();
    Box::pin(async move {
        let (stream, _) = listener.listener.accept().await?;
        let info = ConnInfo::from_unix(&path);
        Ok((stream, info))
    })
}

#[cfg(unix)]
async fn start_unix_server(path: PathBuf, connection_count: usize) -> tokio::task::JoinHandle<()> {
    let listener = UnixListener::bind(&path).expect("bind Unix listener");
    start_server(
        UnixContractListener { listener, path },
        connection_count,
        accept_unix,
    )
}

async fn finish_server(server_task: tokio::task::JoinHandle<()>) {
    timeout(Duration::from_secs(1), server_task)
        .await
        .expect("server shutdown timeout")
        .expect("server task join");
}

#[cfg(unix)]
async fn finish_unix_server(path: &Path, server_task: tokio::task::JoinHandle<()>) {
    finish_server(server_task).await;
    std::fs::remove_file(path).expect("remove Unix socket");
}

async fn hello_replies_case<S>(mut client: S)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
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
}

async fn hgetall_projects_case<S>(mut client: S)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
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
}

async fn watch_conflict_exec_case<S>(mut watched: S, mut writer: S)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
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
}

async fn pubsub_acknowledgements_case<S>(mut subscriber: S, mut publisher: S)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
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
}

async fn exec_preserves_pubsub_acknowledgements_case<S>(mut client: S, version: i64)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
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
}

async fn hello_inside_exec_case<S>(mut client: S, before: i64, after: i64)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
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
}

#[tokio::test]
async fn hello_replies_follow_each_negotiated_protocol_in_a_pipeline() {
    let (addr, server_task) = start_tcp_server(1).await;
    hello_replies_case(TcpStream::connect(addr).await.expect("connect client")).await;
    finish_server(server_task).await;
}

#[tokio::test]
async fn hgetall_projects_consistently_for_fast_batch_and_exec_replies() {
    let (addr, server_task) = start_tcp_server(1).await;
    hgetall_projects_case(TcpStream::connect(addr).await.expect("connect client")).await;
    finish_server(server_task).await;
}

#[tokio::test]
async fn watch_conflict_exec_is_a_resp2_null_array() {
    let (addr, server_task) = start_tcp_server(2).await;
    let watched = TcpStream::connect(addr)
        .await
        .expect("connect watched client");
    let writer = TcpStream::connect(addr)
        .await
        .expect("connect writer client");
    watch_conflict_exec_case(watched, writer).await;
    finish_server(server_task).await;
}

#[tokio::test]
async fn pubsub_acknowledgements_are_independent_frames_in_resp2_and_pushes_in_resp3() {
    let (addr, server_task) = start_tcp_server(2).await;
    let subscriber = TcpStream::connect(addr).await.expect("connect subscriber");
    let publisher = TcpStream::connect(addr).await.expect("connect publisher");
    pubsub_acknowledgements_case(subscriber, publisher).await;
    finish_server(server_task).await;
}

#[tokio::test]
async fn exec_preserves_independent_pubsub_acknowledgements_in_both_protocols() {
    for version in [2, 3] {
        let (addr, server_task) = start_tcp_server(1).await;
        exec_preserves_pubsub_acknowledgements_case(
            TcpStream::connect(addr).await.expect("connect client"),
            version,
        )
        .await;
        finish_server(server_task).await;
    }
}

#[tokio::test]
async fn hello_inside_exec_preserves_each_completed_reply_protocol() {
    for (before, after) in [(2, 3), (3, 2)] {
        let (addr, server_task) = start_tcp_server(1).await;
        hello_inside_exec_case(
            TcpStream::connect(addr).await.expect("connect client"),
            before,
            after,
        )
        .await;
        finish_server(server_task).await;
    }
}

#[cfg(unix)]
#[tokio::test]
async fn unix_socket_contract_cases_preserve_tcp_wire_replies() {
    let temp = tempfile::tempdir().expect("tempdir");
    let socket_path = temp.path().join("ratatosk.sock");

    let server_task = start_unix_server(socket_path.clone(), 1).await;
    hello_replies_case(
        UnixStream::connect(&socket_path)
            .await
            .expect("connect client"),
    )
    .await;
    finish_unix_server(&socket_path, server_task).await;

    let server_task = start_unix_server(socket_path.clone(), 1).await;
    hgetall_projects_case(
        UnixStream::connect(&socket_path)
            .await
            .expect("connect client"),
    )
    .await;
    finish_unix_server(&socket_path, server_task).await;

    let server_task = start_unix_server(socket_path.clone(), 2).await;
    let watched = UnixStream::connect(&socket_path)
        .await
        .expect("connect watched client");
    let writer = UnixStream::connect(&socket_path)
        .await
        .expect("connect writer client");
    watch_conflict_exec_case(watched, writer).await;
    finish_unix_server(&socket_path, server_task).await;

    let server_task = start_unix_server(socket_path.clone(), 2).await;
    let subscriber = UnixStream::connect(&socket_path)
        .await
        .expect("connect subscriber");
    let publisher = UnixStream::connect(&socket_path)
        .await
        .expect("connect publisher");
    pubsub_acknowledgements_case(subscriber, publisher).await;
    finish_unix_server(&socket_path, server_task).await;

    for version in [2, 3] {
        let server_task = start_unix_server(socket_path.clone(), 1).await;
        exec_preserves_pubsub_acknowledgements_case(
            UnixStream::connect(&socket_path)
                .await
                .expect("connect client"),
            version,
        )
        .await;
        finish_unix_server(&socket_path, server_task).await;
    }

    for (before, after) in [(2, 3), (3, 2)] {
        let server_task = start_unix_server(socket_path.clone(), 1).await;
        hello_inside_exec_case(
            UnixStream::connect(&socket_path)
                .await
                .expect("connect client"),
            before,
            after,
        )
        .await;
        finish_unix_server(&socket_path, server_task).await;
    }
}

#[cfg(all(unix, feature = "shm-transport"))]
mod shm {
    use super::*;
    use ratatosk_shm::{ClientConfig, ShmStream, accept_shm_session, connect_shm};

    struct ShmContractListener {
        listener: UnixListener,
        path: PathBuf,
        config: ratatosk_shm::ServerConfig,
    }

    fn accept_shm(listener: &ShmContractListener) -> AcceptFuture<'_, ShmStream> {
        let path = listener.path.clone();
        Box::pin(async move {
            let (control, _) = listener.listener.accept().await?;
            let stream = accept_shm_session(control, &listener.config).await?;
            let info = ConnInfo::from_shm(&path);
            Ok((stream, info))
        })
    }

    async fn start_shm_server(
        path: PathBuf,
        connection_count: usize,
    ) -> tokio::task::JoinHandle<()> {
        let listener = UnixListener::bind(&path).expect("bind shm control listener");
        let config = ratatosk_shm::ServerConfig {
            spin_iters: 0,
            default_ring_bytes: 65536,
            max_ring_bytes: 65536,
            ..ratatosk_shm::ServerConfig::default()
        };
        start_server(
            ShmContractListener {
                listener,
                path,
                config,
            },
            connection_count,
            accept_shm,
        )
    }

    async fn connect(path: &Path) -> ShmStream {
        let config = ClientConfig {
            spin_iters: 0,
            ..ClientConfig::default()
        };
        connect_shm(path, &config)
            .await
            .expect("connect shm client")
    }

    /// The same contract cases as TCP and Unix sockets, carried over shared memory.
    #[tokio::test]
    async fn shm_contract_cases_preserve_tcp_wire_replies() {
        let temp = tempfile::tempdir().expect("tempdir");
        let socket_path = temp.path().join("shm.sock");

        let server_task = start_shm_server(socket_path.clone(), 1).await;
        hello_replies_case(connect(&socket_path).await).await;
        finish_unix_server(&socket_path, server_task).await;

        let server_task = start_shm_server(socket_path.clone(), 1).await;
        hgetall_projects_case(connect(&socket_path).await).await;
        finish_unix_server(&socket_path, server_task).await;

        let server_task = start_shm_server(socket_path.clone(), 2).await;
        let watched = connect(&socket_path).await;
        let writer = connect(&socket_path).await;
        watch_conflict_exec_case(watched, writer).await;
        finish_unix_server(&socket_path, server_task).await;

        let server_task = start_shm_server(socket_path.clone(), 2).await;
        let subscriber = connect(&socket_path).await;
        let publisher = connect(&socket_path).await;
        pubsub_acknowledgements_case(subscriber, publisher).await;
        finish_unix_server(&socket_path, server_task).await;

        for version in [2, 3] {
            let server_task = start_shm_server(socket_path.clone(), 1).await;
            exec_preserves_pubsub_acknowledgements_case(connect(&socket_path).await, version).await;
            finish_unix_server(&socket_path, server_task).await;
        }

        for (before, after) in [(2, 3), (3, 2)] {
            let server_task = start_shm_server(socket_path.clone(), 1).await;
            hello_inside_exec_case(connect(&socket_path).await, before, after).await;
            finish_unix_server(&socket_path, server_task).await;
        }
    }

    /// Shared-memory clients are reported like Unix-socket clients in CLIENT LIST.
    #[tokio::test]
    async fn shm_client_list_reports_unix_flag_and_socket_addr() {
        let temp = tempfile::tempdir().expect("tempdir");
        let socket_path = temp.path().join("shm.sock");
        let server_task = start_shm_server(socket_path.clone(), 1).await;
        let mut client = connect(&socket_path).await;

        client
            .write_all(&command(&["CLIENT", "LIST"]))
            .await
            .expect("send CLIENT LIST");
        let mut reply = vec![0u8; 512];
        let n = timeout(Duration::from_secs(1), client.read(&mut reply))
            .await
            .expect("reply timeout")
            .expect("read reply");
        let text = String::from_utf8_lossy(&reply[..n]).into_owned();
        assert!(
            text.contains(&format!("addr={}:0", socket_path.display())),
            "unexpected CLIENT LIST line: {text}"
        );
        assert!(text.contains("flags=NU"), "unexpected flags: {text}");

        drop(client);
        finish_unix_server(&socket_path, server_task).await;
    }
}
