use std::{
    fs,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    path::Path,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use ratatosk_resp::{RespFrame, encode_to_vec, parse};

struct ChildGuard {
    child: Child,
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

fn reserve_port() -> io::Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

fn wait_for_tcp_listener(port: u16) -> io::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        match TcpStream::connect(("127.0.0.1", port)) {
            Ok(_) => return Ok(()),
            Err(_) => thread::sleep(Duration::from_millis(50)),
        }
    }

    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        format!("timed out waiting for TCP listener on port {port}"),
    ))
}

fn connect_client(port: u16) -> io::Result<TcpStream> {
    let stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    Ok(stream)
}

fn spawn_ratatosk_server(port: u16, dir: &Path) -> io::Result<ChildGuard> {
    let metrics_port = reserve_port()?;
    let bin = std::env::var_os("CARGO_BIN_EXE_ratatosk").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "CARGO_BIN_EXE_ratatosk is not available for redis interop test",
        )
    })?;

    let child = Command::new(bin)
        .env("RATATOSK_BIND", "127.0.0.1")
        .env("RATATOSK_PORT", port.to_string())
        .env("RATATOSK_DIR", dir)
        .env("RATATOSK_METRICS_BIND", format!("127.0.0.1:{metrics_port}"))
        .env("RATATOSK_ALLOW_NO_METRICS", "true")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;

    Ok(ChildGuard { child })
}

fn spawn_ratatosk_server_on_dynamic_port(dir: &Path) -> io::Result<(ChildGuard, u16)> {
    let metrics_port = reserve_port()?;
    let bound_addr_file = dir.join("ratatosk-bound-addr.json");
    let bin = std::env::var_os("CARGO_BIN_EXE_ratatosk").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "CARGO_BIN_EXE_ratatosk is not available for redis interop test",
        )
    })?;

    let mut child = Command::new(bin)
        .env("RATATOSK_BIND", "127.0.0.1")
        .env("RATATOSK_PORT", "0")
        .env("RATATOSK_DIR", dir)
        .env("RATATOSK_METRICS_BIND", format!("127.0.0.1:{metrics_port}"))
        .env("RATATOSK_ALLOW_NO_METRICS", "true")
        .env("RATATOSK_BOUND_ADDR_FILE", &bound_addr_file)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;

    let port = wait_for_bound_port_file(&bound_addr_file, &mut child)?;

    Ok((ChildGuard { child }, port))
}

fn wait_for_bound_port_file(path: &Path, child: &mut Child) -> io::Result<u16> {
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
            return Err(io::Error::other(format!(
                "ratatosk exited before writing bound address file {}: {status}",
                path.display()
            )));
        }

        thread::sleep(Duration::from_millis(50));
    }

    Err(io::Error::new(
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

fn spawn_redis_server(port: u16, dir: &Path) -> io::Result<Option<ChildGuard>> {
    let child = match Command::new("redis-server")
        .arg("--port")
        .arg(port.to_string())
        .arg("--bind")
        .arg("127.0.0.1")
        .arg("--save")
        .arg("")
        .arg("--appendonly")
        .arg("no")
        .arg("--dir")
        .arg(dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };

    Ok(Some(ChildGuard { child }))
}

fn send_frame(stream: &mut TcpStream, frame: RespFrame) -> io::Result<RespFrame> {
    let mut payload = Vec::new();
    encode_to_vec(&frame, &mut payload);
    stream.write_all(&payload)?;
    read_frame(stream)
}

fn read_frame(stream: &mut TcpStream) -> io::Result<RespFrame> {
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
fn scholar_v1_sidecar_wire_contract_smoke() -> io::Result<()> {
    let ratatosk_dir = tempfile::tempdir()?;
    let ratatosk_port = reserve_port()?;

    let _ratatosk = spawn_ratatosk_server(ratatosk_port, ratatosk_dir.path())?;
    wait_for_tcp_listener(ratatosk_port)?;

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
            array(&["SET", "nexus:cache:search:1", "payload", "EX", "60"])
        )?,
        RespFrame::ok()
    );
    assert_eq!(
        send_frame(&mut client, array(&["GET", "nexus:cache:search:1"]))?,
        bulk("payload")
    );
    match send_frame(&mut client, array(&["TTL", "nexus:cache:search:1"]))? {
        RespFrame::Integer(ttl) if (0..=60).contains(&ttl) => {}
        other => panic!("expected TTL for short-lived cache key to be 0..=60, got {other:?}"),
    }
    assert_eq!(
        send_frame(&mut client, array(&["DEL", "nexus:cache:search:1"]))?,
        RespFrame::Integer(1)
    );
    assert_eq!(
        send_frame(&mut client, array(&["GET", "nexus:cache:search:1"]))?,
        RespFrame::BulkString(None)
    );

    assert_eq!(
        send_frame(&mut client, array(&["INCR", "nexus:quota:provider:window"]))?,
        RespFrame::Integer(1)
    );
    assert_eq!(
        send_frame(
            &mut client,
            array(&["EXPIRE", "nexus:quota:provider:window", "30"])
        )?,
        RespFrame::Integer(1)
    );
    match send_frame(&mut client, array(&["TTL", "nexus:quota:provider:window"]))? {
        RespFrame::Integer(ttl) if (0..=30).contains(&ttl) => {}
        other => panic!("expected TTL for provider quota key to be 0..=30, got {other:?}"),
    }

    let xadd_id = bulk_text(send_frame(
        &mut client,
        array(&["XADD", "nexus:telemetry", "*", "event", "search.completed"]),
    )?)?;
    assert!(
        xadd_id.contains('-'),
        "XADD should return a stream entry id, got {xadd_id:?}"
    );

    let mut subscriber = connect_client(ratatosk_port)?;
    assert_eq!(
        send_frame(&mut subscriber, array(&["SUBSCRIBE", "nexus:events"]))?,
        RespFrame::Array(vec![
            bulk("subscribe"),
            bulk("nexus:events"),
            RespFrame::Integer(1),
        ])
    );
    assert_eq!(
        send_frame(&mut client, array(&["PUBLISH", "nexus:events", "changed"]))?,
        RespFrame::Integer(1)
    );
    assert_eq!(
        read_frame(&mut subscriber)?,
        RespFrame::Array(vec![bulk("message"), bulk("nexus:events"), bulk("changed"),])
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

#[test]
fn redis_interop_supported_subset_matches_redis_when_available() -> io::Result<()> {
    let ratatosk_dir = tempfile::tempdir()?;
    let redis_dir = tempfile::tempdir()?;
    let ratatosk_port = reserve_port()?;
    let redis_port = reserve_port()?;

    let _ratatosk = spawn_ratatosk_server(ratatosk_port, ratatosk_dir.path())?;
    let Some(_redis) = spawn_redis_server(redis_port, redis_dir.path())? else {
        eprintln!("skipping redis interop smoke test because redis-server is not installed");
        return Ok(());
    };

    wait_for_tcp_listener(ratatosk_port)?;
    wait_for_tcp_listener(redis_port)?;

    let mut ratatosk = connect_client(ratatosk_port)?;
    let mut redis = connect_client(redis_port)?;

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
