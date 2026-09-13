use std::{
    env, fs,
    io::{self, Read, Write},
    net::TcpStream,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use ratatosk_resp::{RespFrame, encode_to_vec, parse};

struct ChildGuard {
    child: Child,
    name: String,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
}

impl ChildGuard {
    fn new(
        child: Child,
        name: impl Into<String>,
        stdout_path: PathBuf,
        stderr_path: PathBuf,
    ) -> Self {
        Self {
            child,
            name: name.into(),
            stdout_path,
            stderr_path,
        }
    }

    fn id(&self) -> u32 {
        self.child.id()
    }

    fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.child.try_wait()
    }

    fn error_with_logs(&self, kind: io::ErrorKind, message: impl std::fmt::Display) -> io::Error {
        let stdout = fs::read_to_string(&self.stdout_path).unwrap_or_default();
        let stderr = fs::read_to_string(&self.stderr_path).unwrap_or_default();
        io::Error::new(
            kind,
            format!(
                "{} {message}; stdout={stdout:?}; stderr={stderr:?}",
                self.name
            ),
        )
    }

    fn startup_exit_error(&self, status: ExitStatus) -> io::Error {
        self.error_with_logs(
            io::ErrorKind::Other,
            format_args!("exited during startup: {status}"),
        )
    }

    fn wait_for_exit(&mut self, timeout: Duration) -> io::Result<ExitStatus> {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if let Some(status) = self.child.try_wait()? {
                return Ok(status);
            }
            thread::sleep(Duration::from_millis(50));
        }

        Err(self.error_with_logs(
            io::ErrorKind::TimedOut,
            format!(
                "child process {} did not exit within {timeout:?}",
                self.id()
            ),
        ))
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        match self.child.try_wait() {
            Ok(Some(_)) => {}
            Ok(None) => {
                let _ = self.child.kill();
                let _ = self.child.wait();
            }
            Err(_) => {
                let _ = self.child.kill();
                let _ = self.child.wait();
            }
        }
    }
}

#[cfg(unix)]
fn wait_for_unix_listener(path: &Path, child: &mut ChildGuard) -> io::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait()? {
            return Err(child.startup_exit_error(status));
        }

        match std::os::unix::net::UnixStream::connect(path) {
            Ok(stream) => {
                drop(stream);
                return Ok(());
            }
            Err(_) => thread::sleep(Duration::from_millis(50)),
        }
    }

    Err(child.error_with_logs(
        io::ErrorKind::TimedOut,
        format!("timed out waiting for Unix socket {}", path.display()),
    ))
}

fn connect_client(port: u16) -> io::Result<TcpStream> {
    let stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    Ok(stream)
}

fn ratatosk_bin() -> io::Result<PathBuf> {
    env::var_os("CARGO_BIN_EXE_ratatosk")
        .map(PathBuf::from)
        .or_else(|| option_env!("CARGO_BIN_EXE_ratatosk").map(PathBuf::from))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "CARGO_BIN_EXE_ratatosk is not available for redis interop test",
            )
        })
}

fn spawn_ratatosk_server_on_dynamic_port(dir: &Path) -> io::Result<(ChildGuard, u16)> {
    let bound_addr_file = dir.join("ratatosk-bound-addr.json");
    let stdout_path = dir.join("ratatosk.stdout.log");
    let stderr_path = dir.join("ratatosk.stderr.log");
    let stdout = fs::File::create(&stdout_path)?;
    let stderr = fs::File::create(&stderr_path)?;

    let child = Command::new(ratatosk_bin()?)
        .current_dir(dir)
        .env_clear()
        .env("RATATOSK_BIND", "127.0.0.1")
        .env("RATATOSK_PORT", "0")
        .env("RATATOSK_DIR", dir)
        .env("RATATOSK_DISABLE_CONFIG_AUTOLOAD", "true")
        .env("RATATOSK_AUDIT_LOG", dir.join("audit.log"))
        .env("RATATOSK_AUDIT_CHAIN_STATE", dir.join("audit.state"))
        .env("RATATOSK_METRICS_BIND", "127.0.0.1:0")
        .env("RATATOSK_ALLOW_NO_METRICS", "true")
        .env("RATATOSK_BOUND_ADDR_FILE", &bound_addr_file)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()?;
    let mut child = ChildGuard::new(child, "ratatosk", stdout_path, stderr_path);

    let port = wait_for_bound_port_file(&bound_addr_file, &mut child)?;

    Ok((child, port))
}

#[cfg(unix)]
fn spawn_ratatosk_unixsocket_server(socket_path: &Path, dir: &Path) -> io::Result<ChildGuard> {
    let stdout_path = dir.join("ratatosk.stdout.log");
    let stderr_path = dir.join("ratatosk.stderr.log");
    let stdout = fs::File::create(&stdout_path)?;
    let stderr = fs::File::create(&stderr_path)?;

    let child = Command::new(ratatosk_bin()?)
        .current_dir(dir)
        .env_clear()
        .env("RATATOSK_BIND", "127.0.0.1")
        .env("RATATOSK_PORT", "0")
        .env("RATATOSK_UNIXSOCKET", socket_path)
        .env("RATATOSK_UNIXSOCKETPERM", "700")
        .env("RATATOSK_DIR", dir)
        .env("RATATOSK_DISABLE_CONFIG_AUTOLOAD", "true")
        .env("RATATOSK_AUDIT_LOG", dir.join("audit.log"))
        .env("RATATOSK_AUDIT_CHAIN_STATE", dir.join("audit.state"))
        .env("RATATOSK_METRICS_BIND", "127.0.0.1:0")
        .env("RATATOSK_ALLOW_NO_METRICS", "true")
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()?;

    Ok(ChildGuard::new(child, "ratatosk", stdout_path, stderr_path))
}

fn wait_for_bound_port_file(path: &Path, child: &mut ChildGuard) -> io::Result<u16> {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        match fs::read_to_string(path) {
            Ok(contents) => {
                if let Some(port) = bound_port_from_file(&contents) {
                    return Ok(port);
                }
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "invalid bound address file {}: {contents:?}",
                        path.display()
                    ),
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }

        if let Some(status) = child.try_wait()? {
            return Err(child.startup_exit_error(status));
        }

        thread::sleep(Duration::from_millis(50));
    }

    Err(child.error_with_logs(
        io::ErrorKind::TimedOut,
        format!(
            "timed out waiting for bound address file {}",
            path.display()
        ),
    ))
}

fn bound_port_from_file(contents: &str) -> Option<u16> {
    let payload: serde_json::Value = serde_json::from_str(contents).ok()?;
    let port = payload.get("bound_port")?.as_u64()?;
    u16::try_from(port).ok()
}

#[cfg(unix)]
fn required_external_tool(name: &str) -> io::Result<PathBuf> {
    let search_path = env::var_os("PATH").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("required external tool {name} is not installed or PATH is empty"),
        )
    })?;
    let executable = env::split_paths(&search_path)
        .map(|directory| directory.join(name))
        .find(|candidate| candidate.is_file())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("required external tool {name} was not found in PATH"),
            )
        })?;
    let executable = fs::canonicalize(executable)?;
    let check_dir = tempfile::tempdir()?;
    let stdout_path = check_dir.path().join("version.stdout.log");
    let stderr_path = check_dir.path().join("version.stderr.log");
    let stdout_file = fs::File::create(&stdout_path)?;
    let stderr_file = fs::File::create(&stderr_path)?;
    let child = Command::new(&executable)
        .current_dir(check_dir.path())
        .env_clear()
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout_file))
        .stderr(Stdio::from(stderr_file))
        .spawn()?;
    let mut child = ChildGuard::new(child, format!("{name} --version"), stdout_path, stderr_path);
    let status = child.wait_for_exit(Duration::from_secs(5))?;
    if !status.success() {
        let stdout = fs::read_to_string(&child.stdout_path).unwrap_or_default();
        let stderr = fs::read_to_string(&child.stderr_path).unwrap_or_default();
        return Err(io::Error::other(format!(
            "required external tool {name} failed its version check: {}; stdout={:?}; stderr={:?}",
            status, stdout, stderr
        )));
    }

    Ok(executable)
}

#[cfg(unix)]
fn spawn_redis_server(executable: &Path, socket_path: &Path, dir: &Path) -> io::Result<ChildGuard> {
    let stdout_path = dir.join("redis-server.stdout.log");
    let stderr_path = dir.join("redis-server.stderr.log");
    let stdout = fs::File::create(&stdout_path)?;
    let stderr = fs::File::create(&stderr_path)?;
    let child = Command::new(executable)
        .current_dir(dir)
        .env_clear()
        .arg("--port")
        .arg("0")
        .arg("--unixsocket")
        .arg(socket_path)
        .arg("--unixsocketperm")
        .arg("700")
        .arg("--save")
        .arg("")
        .arg("--appendonly")
        .arg("no")
        .arg("--dir")
        .arg(dir)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()?;

    Ok(ChildGuard::new(
        child,
        "redis-server",
        stdout_path,
        stderr_path,
    ))
}

fn send_frame<S: Read + Write>(stream: &mut S, frame: RespFrame) -> io::Result<RespFrame> {
    let mut payload = Vec::new();
    encode_to_vec(&frame, &mut payload);
    stream.write_all(&payload)?;
    read_frame(stream)
}

fn read_frame<S: Read>(stream: &mut S) -> io::Result<RespFrame> {
    let mut buf = BytesMut::with_capacity(1024);
    loop {
        match parse(&mut buf) {
            Ok(Some(frame)) => return Ok(frame),
            Ok(None) => {
                let mut scratch = [0u8; 1024];
                let n = stream.read(&mut scratch)?;
                if n == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "connection closed before a full RESP frame arrived",
                    ));
                }
                buf.extend_from_slice(&scratch[..n]);
            }
            Err(error) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("failed to parse RESP frame: {error}"),
                ));
            }
        }
    }
}

fn bulk(value: &str) -> RespFrame {
    RespFrame::BulkString(Some(Bytes::from(value.to_owned())))
}

fn array(parts: &[&str]) -> RespFrame {
    RespFrame::Array(parts.iter().map(|part| bulk(part)).collect())
}

fn bulk_text(frame: RespFrame) -> io::Result<String> {
    match frame {
        RespFrame::BulkString(Some(value)) => String::from_utf8(value.to_vec()).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bulk string is not valid UTF-8: {error}"),
            )
        }),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expected bulk string, got {other:?}"),
        )),
    }
}

#[test]
fn sidecar_v1_wire_contract_smoke() -> io::Result<()> {
    let ratatosk_dir = tempfile::tempdir()?;

    let (_ratatosk, ratatosk_port) = spawn_ratatosk_server_on_dynamic_port(ratatosk_dir.path())?;

    let mut client = connect_client(ratatosk_port)?;

    assert_eq!(
        send_frame(&mut client, array(&["PING"]))?,
        RespFrame::pong()
    );

    let health = bulk_text(send_frame(&mut client, array(&["PING", "HEALTH"]))?)?;
    assert!(
        health.contains("status:") && health.contains("reasons:"),
        "PING HEALTH should expose status and reasons, got {health:?}"
    );

    let info = bulk_text(send_frame(&mut client, array(&["INFO", "server"]))?)?;
    assert!(
        info.contains("# Server\r\n")
            && info.contains("redis_mode:standalone\r\n")
            && info.contains("ratatosk_compatibility_mode:")
            && info.contains("ratatosk_protected_mode:"),
        "INFO server should expose the sidecar contract fields, got {info:?}"
    );

    assert_eq!(
        send_frame(&mut client, array(&["DBSIZE"]))?,
        RespFrame::Integer(0)
    );
    assert_eq!(
        send_frame(
            &mut client,
            array(&["SET", "app:cache:search:1", "payload", "EX", "60"])
        )?,
        RespFrame::ok()
    );
    assert_eq!(
        send_frame(&mut client, array(&["GET", "app:cache:search:1"]))?,
        bulk("payload")
    );
    match send_frame(&mut client, array(&["TTL", "app:cache:search:1"]))? {
        RespFrame::Integer(ttl) if (0..=60).contains(&ttl) => {}
        other => panic!("expected TTL for short-lived cache key to be 0..=60, got {other:?}"),
    }
    assert_eq!(
        send_frame(&mut client, array(&["DEL", "app:cache:search:1"]))?,
        RespFrame::Integer(1)
    );
    assert_eq!(
        send_frame(&mut client, array(&["GET", "app:cache:search:1"]))?,
        RespFrame::BulkString(None)
    );

    assert_eq!(
        send_frame(&mut client, array(&["INCR", "app:quota:provider:window"]))?,
        RespFrame::Integer(1)
    );
    assert_eq!(
        send_frame(
            &mut client,
            array(&["EXPIRE", "app:quota:provider:window", "30"])
        )?,
        RespFrame::Integer(1)
    );
    match send_frame(&mut client, array(&["TTL", "app:quota:provider:window"]))? {
        RespFrame::Integer(ttl) if (0..=30).contains(&ttl) => {}
        other => panic!("expected TTL for provider quota key to be 0..=30, got {other:?}"),
    }

    let xadd_id = bulk_text(send_frame(
        &mut client,
        array(&["XADD", "app:telemetry", "*", "event", "search.completed"]),
    )?)?;
    assert!(
        xadd_id.contains('-'),
        "XADD should return a stream entry id, got {xadd_id:?}"
    );

    let mut subscriber = connect_client(ratatosk_port)?;
    assert_eq!(
        send_frame(&mut subscriber, array(&["SUBSCRIBE", "app:events"]))?,
        RespFrame::Array(vec![
            bulk("subscribe"),
            bulk("app:events"),
            RespFrame::Integer(1),
        ])
    );
    assert_eq!(
        send_frame(&mut client, array(&["PUBLISH", "app:events", "changed"]))?,
        RespFrame::Integer(1)
    );
    assert_eq!(
        read_frame(&mut subscriber)?,
        RespFrame::Array(vec![bulk("message"), bulk("app:events"), bulk("changed"),])
    );

    Ok(())
}

#[test]
fn ratatosk_port_zero_advertises_bound_sidecar_port() -> io::Result<()> {
    let ratatosk_dir = tempfile::tempdir()?;

    let (_ratatosk, ratatosk_port) = spawn_ratatosk_server_on_dynamic_port(ratatosk_dir.path())?;
    assert_ne!(ratatosk_port, 0);

    let mut client = connect_client(ratatosk_port)?;
    assert_eq!(
        send_frame(&mut client, array(&["PING"]))?,
        RespFrame::pong()
    );

    Ok(())
}

#[cfg(unix)]
#[test]
fn ratatosk_exits_successfully_on_sigterm() -> io::Result<()> {
    let ratatosk_dir = tempfile::tempdir()?;

    let (mut ratatosk, ratatosk_port) = spawn_ratatosk_server_on_dynamic_port(ratatosk_dir.path())?;
    let mut client = connect_client(ratatosk_port)?;
    assert_eq!(
        send_frame(&mut client, array(&["PING"]))?,
        RespFrame::pong()
    );
    drop(client);

    let status = Command::new("kill")
        .arg("-TERM")
        .arg(ratatosk.id().to_string())
        .status()?;
    assert!(status.success(), "failed to send SIGTERM: {status}");

    let status = ratatosk.wait_for_exit(Duration::from_secs(10))?;
    assert!(
        status.success(),
        "ratatosk should exit 0 after SIGTERM: {status}"
    );

    Ok(())
}

#[cfg(unix)]
#[test]
#[ignore = "requires redis-server in PATH; run with --include-ignored"]
fn redis_interop_supported_subset_matches_redis_when_available() -> io::Result<()> {
    let redis_server = required_external_tool("redis-server")?;
    let ratatosk_dir = tempfile::tempdir()?;
    let redis_dir = tempfile::tempdir()?;
    let redis_socket = redis_dir.path().join("redis.sock");

    let mut redis_process = spawn_redis_server(&redis_server, &redis_socket, redis_dir.path())?;
    wait_for_unix_listener(&redis_socket, &mut redis_process)?;
    let (_ratatosk, ratatosk_port) = spawn_ratatosk_server_on_dynamic_port(ratatosk_dir.path())?;

    let mut ratatosk = connect_client(ratatosk_port)?;
    let mut redis = std::os::unix::net::UnixStream::connect(&redis_socket)?;
    redis.set_read_timeout(Some(Duration::from_secs(2)))?;
    redis.set_write_timeout(Some(Duration::from_secs(2)))?;

    let commands = vec![
        ("FLUSHALL", array(&["FLUSHALL"])),
        ("DBSIZE-empty", array(&["DBSIZE"])),
        ("PING", array(&["PING"])),
        ("ECHO", array(&["ECHO", "interop"])),
        ("SET", array(&["SET", "key", "value"])),
        ("GET", array(&["GET", "key"])),
        ("APPEND", array(&["APPEND", "key", "-tail"])),
        ("STRLEN", array(&["STRLEN", "key"])),
        ("BITCOUNT", array(&["BITCOUNT", "key"])),
        ("GETBIT", array(&["GETBIT", "key", "0"])),
        ("GETRANGE", array(&["GETRANGE", "key", "1", "5"])),
        ("TYPE-string", array(&["TYPE", "key"])),
        ("TYPE-missing", array(&["TYPE", "missing"])),
        ("MSET", array(&["MSET", "m1", "v1", "m2", "v2"])),
        ("MGET", array(&["MGET", "m1", "m2", "missing"])),
        ("EXISTS", array(&["EXISTS", "key"])),
        (
            "EXISTS-multi",
            array(&["EXISTS", "key", "missing", "m1", "m3"]),
        ),
        ("INCR-1", array(&["INCR", "ctr"])),
        ("INCR-2", array(&["INCR", "ctr"])),
        ("RPUSH", array(&["RPUSH", "list", "a", "b", "c"])),
        ("LLEN", array(&["LLEN", "list"])),
        ("LINDEX", array(&["LINDEX", "list", "-1"])),
        ("LRANGE", array(&["LRANGE", "list", "0", "-1"])),
        ("SADD", array(&["SADD", "set", "a", "b"])),
        ("SISMEMBER", array(&["SISMEMBER", "set", "b"])),
        ("SCARD", array(&["SCARD", "set"])),
        ("HSET", array(&["HSET", "hash", "field", "payload"])),
        ("HGET", array(&["HGET", "hash", "field"])),
        ("HMGET", array(&["HMGET", "hash", "field", "missing"])),
        ("HGETALL", array(&["HGETALL", "hash"])),
        ("HKEYS", array(&["HKEYS", "hash"])),
        ("HVALS", array(&["HVALS", "hash"])),
        ("HEXISTS", array(&["HEXISTS", "hash", "field"])),
        ("HLEN", array(&["HLEN", "hash"])),
        ("HSTRLEN", array(&["HSTRLEN", "hash", "field"])),
        ("SMISMEMBER", array(&["SMISMEMBER", "set", "a", "missing"])),
        ("ZADD", array(&["ZADD", "zset", "1", "one", "2", "two"])),
        ("ZRANGE", array(&["ZRANGE", "zset", "0", "-1"])),
        ("ZREVRANGE", array(&["ZREVRANGE", "zset", "0", "1"])),
        ("ZRANGEBYSCORE", array(&["ZRANGEBYSCORE", "zset", "1", "2"])),
        (
            "ZREVRANGEBYSCORE",
            array(&["ZREVRANGEBYSCORE", "zset", "2", "1"]),
        ),
        ("ZSCORE", array(&["ZSCORE", "zset", "two"])),
        ("ZCARD", array(&["ZCARD", "zset"])),
        ("ZMSCORE", array(&["ZMSCORE", "zset", "two", "missing"])),
        ("ZCOUNT", array(&["ZCOUNT", "zset", "1", "2"])),
        ("ZRANK", array(&["ZRANK", "zset", "two"])),
        ("ZREVRANK", array(&["ZREVRANK", "zset", "one"])),
        ("SELECT-1", array(&["SELECT", "1"])),
        ("DBSIZE-db1-empty", array(&["DBSIZE"])),
        ("SET-db1", array(&["SET", "alt", "value"])),
        ("GET-db1", array(&["GET", "alt"])),
        ("DBSIZE-db1", array(&["DBSIZE"])),
        ("SELECT-0", array(&["SELECT", "0"])),
        ("GET-db0-isolated", array(&["GET", "alt"])),
        ("DBSIZE-db0", array(&["DBSIZE"])),
        ("DEL-key", array(&["DEL", "key"])),
        ("GET-key-missing", array(&["GET", "key"])),
    ];

    for (index, (label, command)) in commands.into_iter().enumerate() {
        let ratatosk_reply = send_frame(&mut ratatosk, command.clone())?;
        let redis_reply = send_frame(&mut redis, command)?;
        assert_eq!(
            ratatosk_reply, redis_reply,
            "interop mismatch at command #{index} ({label}): ratatosk={ratatosk_reply:?} redis={redis_reply:?}"
        );
    }

    Ok(())
}

#[cfg(unix)]
#[test]
#[ignore = "requires redis-cli in PATH; run with --include-ignored"]
fn redis_cli_can_ping_ratatosk_over_unix_socket_when_available() -> io::Result<()> {
    let redis_cli = required_external_tool("redis-cli")?;
    let dir = tempfile::tempdir()?;
    let socket_path = dir.path().join("ratatosk.sock");
    let mut ratatosk = spawn_ratatosk_unixsocket_server(&socket_path, dir.path())?;
    wait_for_unix_listener(&socket_path, &mut ratatosk)?;

    let stdout_path = dir.path().join("redis-cli.stdout.log");
    let stderr_path = dir.path().join("redis-cli.stderr.log");
    let stdout_file = fs::File::create(&stdout_path)?;
    let stderr_file = fs::File::create(&stderr_path)?;
    let child = Command::new(redis_cli)
        .current_dir(dir.path())
        .env_clear()
        .arg("-s")
        .arg(&socket_path)
        .arg("PING")
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout_file))
        .stderr(Stdio::from(stderr_file))
        .spawn()?;
    let mut redis_cli = ChildGuard::new(child, "redis-cli", stdout_path, stderr_path);
    let status = redis_cli.wait_for_exit(Duration::from_secs(5))?;
    let stdout = fs::read_to_string(&redis_cli.stdout_path).unwrap_or_default();
    let stderr = fs::read_to_string(&redis_cli.stderr_path).unwrap_or_default();
    assert!(
        status.success(),
        "redis-cli failed: {status}; stdout={stdout:?}; stderr={stderr:?}"
    );
    assert_eq!(stdout.trim(), "PONG");

    Ok(())
}
