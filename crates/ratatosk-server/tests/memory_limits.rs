use std::{
    fs::File,
    io::{self, Read, Write},
    net::TcpStream as StdTcpStream,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use bytes::BytesMut;
use ratatosk_engine::keyspace::{ServerState, SharedState};
use ratatosk_resp::{RespFrame, encode_to_vec, parse};
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

fn encoded_command(parts: &[&str]) -> Vec<u8> {
    let frame = RespFrame::Array(parts.iter().map(|part| RespFrame::bulk_str(part)).collect());
    let mut encoded = Vec::new();
    encode_to_vec(&frame, &mut encoded);
    encoded
}

async fn async_command<S>(stream: &mut S, parts: &[&str]) -> io::Result<RespFrame>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    stream.write_all(&encoded_command(parts)).await?;
    let mut input = BytesMut::new();
    loop {
        match parse(&mut input) {
            Ok(Some(frame)) => return Ok(frame),
            Ok(None) => {
                let mut buf = [0u8; 4096];
                let read = timeout(Duration::from_secs(3), stream.read(&mut buf))
                    .await
                    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "reply timeout"))??;
                if read == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "server closed before replying",
                    ));
                }
                input.extend_from_slice(&buf[..read]);
            }
            Err(error) => return Err(io::Error::new(io::ErrorKind::InvalidData, error)),
        }
    }
}

fn assert_oom(frame: &RespFrame) {
    match frame {
        RespFrame::Error(message) => assert!(message.starts_with(b"OOM command not allowed")),
        other => panic!("expected OOM response, got {other:?}"),
    }
}

fn shared_runtime(
    maxmemory: usize,
    dir: &std::path::Path,
) -> (Arc<SharedState>, Arc<PersistenceRuntime>) {
    let mut state = ServerState::with_default_dbs();
    state.config.set_maxmemory(maxmemory);
    let shared = Arc::new(SharedState::new(state));
    let config = ServerConfig {
        dir: dir.to_path_buf(),
        ..ServerConfig::default()
    };
    let persistence =
        Arc::new(PersistenceRuntime::from_config(&config).expect("persistence runtime"));
    (shared, persistence)
}

async fn serve_connection<S>(
    stream: S,
    info: ConnInfo,
    shared: Arc<SharedState>,
    persistence: Arc<PersistenceRuntime>,
) where
    S: SessionStream + Sync + 'static,
{
    handle_client_with_limits(stream, info, shared, persistence, ClientIoLimits::default())
        .await
        .expect("handle test client");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_tcp_connections_share_one_live_memory_limit() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (shared, persistence) = shared_runtime(1, dir.path());
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind TCP");
    let addr = listener.local_addr().expect("TCP address");
    let server = tokio::spawn(async move {
        let mut tasks = Vec::new();
        for _ in 0..2 {
            let (stream, _) = listener.accept().await.expect("accept TCP");
            let info = ConnInfo::from_tcp(&stream);
            tasks.push(tokio::spawn(serve_connection(
                stream,
                info,
                Arc::clone(&shared),
                Arc::clone(&persistence),
            )));
        }
        for task in tasks {
            task.await.expect("join TCP client");
        }
    });

    let mut first = TcpStream::connect(addr)
        .await
        .expect("connect first TCP client");
    let mut second = TcpStream::connect(addr)
        .await
        .expect("connect second TCP client");
    let large = "x".repeat(128 * 1024);
    let first_args = ["SET", "first", large.as_str()];
    let second_args = ["SET", "second", large.as_str()];
    let (first_reply, second_reply) = tokio::join!(
        async_command(&mut first, &first_args),
        async_command(&mut second, &second_args),
    );
    let first_reply = first_reply.expect("first SET reply");
    let second_reply = second_reply.expect("second SET reply");
    assert!(
        (first_reply == RespFrame::ok() && matches!(second_reply, RespFrame::Error(_)))
            || (second_reply == RespFrame::ok() && matches!(first_reply, RespFrame::Error(_)))
    );
    if matches!(first_reply, RespFrame::Error(_)) {
        assert_oom(&first_reply);
    } else {
        assert_oom(&second_reply);
    }

    assert_eq!(
        async_command(&mut first, &["DEL", "first", "second"])
            .await
            .expect("free winning key"),
        RespFrame::Integer(1)
    );
    assert_eq!(
        async_command(&mut second, &["SET", "after-free", "v"])
            .await
            .expect("write after free"),
        RespFrame::ok()
    );
    drop(first);
    drop(second);
    timeout(Duration::from_secs(3), server)
        .await
        .expect("TCP server timeout")
        .expect("TCP server join");
}

#[cfg(unix)]
#[tokio::test]
async fn unix_socket_observes_runtime_maxmemory_updates() {
    let dir = tempfile::tempdir().expect("tempdir");
    let socket_path = dir.path().join("memory-limit.sock");
    let (shared, persistence) = shared_runtime(1, dir.path());
    let listener = UnixListener::bind(&socket_path).expect("bind Unix socket");
    let server_path = socket_path.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept Unix socket");
        serve_connection(
            stream,
            ConnInfo::from_unix(&server_path),
            shared,
            persistence,
        )
        .await;
    });

    let mut client = UnixStream::connect(&socket_path)
        .await
        .expect("connect Unix socket");
    let large = "x".repeat(128 * 1024);
    assert_eq!(
        async_command(&mut client, &["SET", "first", &large])
            .await
            .expect("initial SET"),
        RespFrame::ok()
    );
    let rejected = async_command(&mut client, &["SET", "second", "v"])
        .await
        .expect("rejected SET");
    assert_oom(&rejected);
    assert_eq!(
        async_command(&mut client, &["CONFIG", "SET", "maxmemory", "0"])
            .await
            .expect("disable memory limit"),
        RespFrame::ok()
    );
    assert_eq!(
        async_command(&mut client, &["CONFIG", "GET", "maxmemory"])
            .await
            .expect("read live memory limit"),
        RespFrame::Array(vec![
            RespFrame::bulk_str("maxmemory"),
            RespFrame::bulk_str("0")
        ])
    );
    assert_eq!(
        async_command(&mut client, &["SET", "second", "v"])
            .await
            .expect("SET after CONFIG"),
        RespFrame::ok()
    );
    drop(client);
    timeout(Duration::from_secs(3), server)
        .await
        .expect("Unix server timeout")
        .expect("Unix server join");
}

struct DiskServer {
    child: Option<Child>,
    dir: tempfile::TempDir,
    port: u16,
    maxmemory: usize,
    starts: usize,
}

impl DiskServer {
    fn new() -> io::Result<Self> {
        Ok(Self {
            child: None,
            dir: tempfile::tempdir()?,
            port: 0,
            maxmemory: 0,
            starts: 0,
        })
    }

    fn start(&mut self) -> io::Result<()> {
        self.starts += 1;
        let bound_addr_file = self.dir.path().join(format!("bound-{}.json", self.starts));
        let stdout_path = self.dir.path().join(format!("stdout-{}.log", self.starts));
        let stderr_path = self.dir.path().join(format!("stderr-{}.log", self.starts));
        let stdout = File::create(&stdout_path)?;
        let stderr = File::create(&stderr_path)?;
        let child = Command::new(ratatosk_bin()?)
            .current_dir(self.dir.path())
            .env_clear()
            .arg("--no-config-autoload")
            .env("RATATOSK_DISABLE_CONFIG_AUTOLOAD", "true")
            .env("RATATOSK_BIND", "127.0.0.1")
            .env("RATATOSK_PORT", "0")
            .env("RATATOSK_DIR", self.dir.path())
            .env("RATATOSK_APPENDONLY", "true")
            .env("RATATOSK_APPENDFSYNC", "always")
            .env("RATATOSK_MAXMEMORY", self.maxmemory.to_string())
            .env("RATATOSK_MAXMEMORY_POLICY", "noeviction")
            .env("RATATOSK_BOUND_ADDR_FILE", &bound_addr_file)
            .env("RATATOSK_AUDIT_LOG", self.dir.path().join("audit.log"))
            .env(
                "RATATOSK_AUDIT_CHAIN_STATE",
                self.dir.path().join("audit.state"),
            )
            .env("RATATOSK_CRASH_DIR", self.dir.path().join("crash"))
            .env("RATATOSK_METRICS_BIND", "127.0.0.1:0")
            .env("RATATOSK_ALLOW_NO_METRICS", "true")
            .env("RATATOSK_CONN_RATE_LIMIT_MAX_ATTEMPTS", "100000")
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()?;
        self.child = Some(child);
        self.port = wait_for_bound_address(
            &bound_addr_file,
            self.child.as_mut().expect("child"),
            &stdout_path,
            &stderr_path,
        )?;
        Ok(())
    }

    fn stop(&mut self) -> io::Result<()> {
        if let Some(mut child) = self.child.take() {
            if child.try_wait()?.is_none() {
                child.kill()?;
                child.wait()?;
            }
        }
        Ok(())
    }

    fn client(&self) -> io::Result<SyncClient> {
        SyncClient::connect(self.port)
    }
}

impl Drop for DiskServer {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

struct SyncClient {
    stream: StdTcpStream,
}

impl SyncClient {
    fn connect(port: u16) -> io::Result<Self> {
        let stream = StdTcpStream::connect(("127.0.0.1", port))?;
        stream.set_read_timeout(Some(Duration::from_secs(3)))?;
        stream.set_write_timeout(Some(Duration::from_secs(3)))?;
        Ok(Self { stream })
    }

    fn command(&mut self, parts: &[&str]) -> io::Result<RespFrame> {
        self.stream.write_all(&encoded_command(parts))?;
        let mut input = BytesMut::new();
        loop {
            match parse(&mut input) {
                Ok(Some(frame)) => return Ok(frame),
                Ok(None) => {
                    let mut buf = [0u8; 4096];
                    let read = self.stream.read(&mut buf)?;
                    if read == 0 {
                        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "closed"));
                    }
                    input.extend_from_slice(&buf[..read]);
                }
                Err(error) => return Err(io::Error::new(io::ErrorKind::InvalidData, error)),
            }
        }
    }
}

fn ratatosk_bin() -> io::Result<PathBuf> {
    std::env::var_os("CARGO_BIN_EXE_ratatosk")
        .map(PathBuf::from)
        .or_else(|| option_env!("CARGO_BIN_EXE_ratatosk").map(PathBuf::from))
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "ratatosk test binary unavailable"))
}

fn wait_for_bound_address(
    path: &Path,
    child: &mut Child,
    stdout_path: &Path,
    stderr_path: &Path,
) -> io::Result<u16> {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait()? {
            let stdout = std::fs::read_to_string(stdout_path).unwrap_or_default();
            let stderr = std::fs::read_to_string(stderr_path).unwrap_or_default();
            return Err(io::Error::other(format!(
                "ratatosk exited during startup: {status}; stdout={stdout:?}; stderr={stderr:?}"
            )));
        }
        match std::fs::read(path) {
            Ok(contents) => {
                let handoff: serde_json::Value = serde_json::from_slice(&contents)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                let address = handoff["bound_addr"].as_str().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "missing bound_addr")
                })?;
                let parsed: std::net::SocketAddr = address
                    .parse()
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                if parsed.ip() != std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
                    || parsed.port() == 0
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unexpected bound address: {parsed}"),
                    ));
                }
                return Ok(parsed.port());
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        thread::sleep(Duration::from_millis(20));
    }
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        "bound address handoff timeout",
    ))
}

#[test]
fn aof_recovery_ignores_low_cap_and_rejected_write_is_not_replayed() -> io::Result<()> {
    let mut server = DiskServer::new()?;
    server.start()?;
    let large = "x".repeat(128 * 1024);
    let mut client = server.client()?;
    assert_eq!(client.command(&["SET", "first", &large])?, RespFrame::ok());
    assert_eq!(client.command(&["SET", "second", &large])?, RespFrame::ok());
    drop(client);
    server.stop()?;

    server.maxmemory = 1;
    server.start()?;
    let mut client = server.client()?;
    assert_eq!(
        client.command(&["GET", "first"])?,
        RespFrame::bulk_str(&large)
    );
    assert_eq!(
        client.command(&["GET", "second"])?,
        RespFrame::bulk_str(&large)
    );
    let rejected = client.command(&["SET", "rejected", "v"])?;
    assert_oom(&rejected);
    drop(client);
    server.stop()?;

    server.maxmemory = 0;
    server.start()?;
    let mut client = server.client()?;
    assert_eq!(
        client.command(&["GET", "rejected"])?,
        RespFrame::BulkString(None)
    );
    assert_eq!(
        client.command(&["GET", "first"])?,
        RespFrame::bulk_str(&large)
    );
    Ok(())
}
