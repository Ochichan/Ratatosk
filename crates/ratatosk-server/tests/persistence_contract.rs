use std::{
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use ratatosk_resp::{RespFrame, encode_to_vec, parse};

struct Server {
    child: Option<Child>,
    dir: tempfile::TempDir,
    port: u16,
    appendonly: bool,
}

impl Server {
    fn new(appendonly: bool) -> io::Result<Self> {
        let mut server = Self {
            child: None,
            dir: tempfile::tempdir()?,
            port: reserve_port()?,
            appendonly,
        };
        server.start()?;
        Ok(server)
    }

    fn start(&mut self) -> io::Result<()> {
        let metrics_port = reserve_port()?;
        let bin = std::env::var_os("CARGO_BIN_EXE_ratatosk").ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "CARGO_BIN_EXE_ratatosk is unavailable for persistence contract tests",
            )
        })?;
        let child = Command::new(bin)
            .arg("--no-config-autoload")
            .env("RATATOSK_BIND", "127.0.0.1")
            .env("RATATOSK_PORT", self.port.to_string())
            .env("RATATOSK_DIR", self.dir.path())
            .env("RATATOSK_APPENDONLY", self.appendonly.to_string())
            .env("RATATOSK_APPENDFSYNC", "always")
            .env("RATATOSK_METRICS_BIND", format!("127.0.0.1:{metrics_port}"))
            .env("RATATOSK_ALLOW_NO_METRICS", "true")
            .env("RATATOSK_CONN_RATE_LIMIT_MAX_ATTEMPTS", "100000")
            .env("RATATOSK_SHUTDOWN_GRACE_MS", "1000")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        self.child = Some(child);
        wait_for_listener(self.port, self.child.as_mut().expect("spawned child"))
    }

    fn restart(&mut self, kill: bool) -> io::Result<()> {
        self.stop(kill)?;
        self.start()
    }

    fn stop(&mut self, kill: bool) -> io::Result<()> {
        let Some(mut child) = self.child.take() else {
            return Ok(());
        };
        if child.try_wait()?.is_none() {
            if kill {
                child.kill()?;
            } else {
                let status = Command::new("kill")
                    .arg("-TERM")
                    .arg(child.id().to_string())
                    .status()?;
                if !status.success() {
                    return Err(io::Error::other("failed to send SIGTERM to ratatosk"));
                }
            }
            let _ = child.wait()?;
        }
        Ok(())
    }

    fn client(&self) -> io::Result<Client> {
        Client::connect(self.port)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.stop(true);
    }
}

struct Client {
    stream: TcpStream,
}

impl Client {
    fn connect(port: u16) -> io::Result<Self> {
        let stream = TcpStream::connect(("127.0.0.1", port))?;
        stream.set_read_timeout(Some(Duration::from_secs(3)))?;
        stream.set_write_timeout(Some(Duration::from_secs(3)))?;
        Ok(Self { stream })
    }

    fn command(&mut self, parts: &[&str]) -> io::Result<RespFrame> {
        let frame = RespFrame::Array(
            parts
                .iter()
                .map(|part| RespFrame::BulkString(Some(Bytes::from((*part).to_owned()))))
                .collect(),
        );
        let mut encoded = Vec::new();
        encode_to_vec(&frame, &mut encoded);
        self.stream.write_all(&encoded)?;
        read_frame(&mut self.stream)
    }
}

fn reserve_port() -> io::Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

fn wait_for_listener(port: u16, child: &mut Child) -> io::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait()? {
            return Err(io::Error::other(format!(
                "ratatosk exited before accepting persistence test connections: {status}"
            )));
        }
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(20));
    }
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        "timed out waiting for ratatosk listener",
    ))
}

fn read_frame(stream: &mut TcpStream) -> io::Result<RespFrame> {
    let mut input = BytesMut::new();
    loop {
        match parse(&mut input) {
            Ok(Some(frame)) => return Ok(frame),
            Ok(None) => {
                let mut buf = [0u8; 4096];
                let read = stream.read(&mut buf)?;
                if read == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "server closed before responding",
                    ));
                }
                input.extend_from_slice(&buf[..read]);
            }
            Err(error) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid RESP response: {error}"),
                ));
            }
        }
    }
}

fn assert_ok(frame: RespFrame) {
    assert_eq!(frame, RespFrame::ok());
}

fn assert_bulk(frame: RespFrame, expected: &str) {
    assert_eq!(frame, RespFrame::bulk_str(expected));
}

#[test]
fn aof_exec_survives_sigkill_with_selected_db() -> io::Result<()> {
    let mut server = Server::new(true)?;
    let mut client = server.client()?;
    assert_ok(client.command(&["SELECT", "1"])?);
    assert_ok(client.command(&["MULTI"])?);
    assert_eq!(
        client.command(&["SET", "committed", "yes"])?,
        RespFrame::queued()
    );
    assert_eq!(
        client.command(&["EXEC"])?,
        RespFrame::Array(vec![RespFrame::ok()])
    );
    drop(client);

    server.restart(true)?;
    let mut client = server.client()?;
    assert_ok(client.command(&["SELECT", "1"])?);
    assert_bulk(client.command(&["GET", "committed"])?, "yes");
    Ok(())
}

#[test]
fn aof_recovery_does_not_reapply_rdb_history() -> io::Result<()> {
    let mut server = Server::new(true)?;
    let mut client = server.client()?;
    assert_eq!(client.command(&["INCR", "counter"])?, RespFrame::Integer(1));
    assert_ok(client.command(&["SAVE"])?);
    assert_eq!(
        client.command(&["RPUSH", "queue", "task"])?,
        RespFrame::Integer(1)
    );
    assert_ok(client.command(&["SAVE"])?);
    drop(client);

    server.restart(true)?;
    let mut client = server.client()?;
    assert_bulk(client.command(&["GET", "counter"])?, "1");
    assert_eq!(
        client.command(&["LRANGE", "queue", "0", "-1"])?,
        RespFrame::Array(vec![RespFrame::bulk_str("task")])
    );
    Ok(())
}

#[test]
fn aof_replays_absolute_ttl_and_resolved_stream_id() -> io::Result<()> {
    let mut server = Server::new(true)?;
    let mut client = server.client()?;
    assert_ok(client.command(&["SET", "expires", "v", "PX", "40"])?);
    let id = client.command(&["XADD", "events", "*", "field", "value"])?;
    let RespFrame::BulkString(Some(id)) = id else {
        panic!("XADD should return an ID");
    };
    drop(client);
    server.stop(false)?;
    thread::sleep(Duration::from_millis(80));
    server.start()?;

    let mut client = server.client()?;
    assert_eq!(
        client.command(&["GET", "expires"])?,
        RespFrame::BulkString(None)
    );
    assert_eq!(
        client.command(&["XRANGE", "events", "-", "+"])?,
        RespFrame::Array(vec![RespFrame::Array(vec![
            RespFrame::BulkString(Some(id)),
            RespFrame::Array(vec![
                RespFrame::bulk_str("field"),
                RespFrame::bulk_str("value")
            ]),
        ])])
    );
    Ok(())
}

#[test]
fn runtime_appendonly_transitions_materialize_then_stop_writing() -> io::Result<()> {
    let mut server = Server::new(false)?;
    let mut client = server.client()?;
    assert_ok(client.command(&["SET", "before", "enabled"])?);
    assert_ok(client.command(&["CONFIG", "SET", "appendonly", "yes"])?);
    assert_eq!(
        client.command(&["CONFIG", "GET", "appendonly"])?,
        RespFrame::Array(vec![
            RespFrame::bulk_str("appendonly"),
            RespFrame::bulk_str("yes")
        ])
    );
    let RespFrame::BulkString(Some(info)) = client.command(&["INFO", "persistence"])? else {
        panic!("INFO should return a bulk string");
    };
    assert!(String::from_utf8_lossy(&info).contains("aof_enabled:1"));
    assert_ok(client.command(&["SET", "during", "enabled"])?);
    assert_ok(client.command(&["CONFIG", "SET", "appendfsync", "always"])?);
    assert_ok(client.command(&["CONFIG", "SET", "appendonly", "no"])?);
    assert_ok(client.command(&["SET", "while", "disabled"])?);
    drop(client);

    server.appendonly = true;
    server.restart(true)?;
    let mut client = server.client()?;
    assert_bulk(client.command(&["GET", "before"])?, "enabled");
    assert_bulk(client.command(&["GET", "during"])?, "enabled");
    assert_eq!(
        client.command(&["GET", "while"])?,
        RespFrame::BulkString(None)
    );
    assert_ok(client.command(&["CONFIG", "SET", "appendonly", "no"])?);
    assert_ok(client.command(&["CONFIG", "SET", "appendonly", "yes"])?);
    assert_ok(client.command(&["SET", "after", "reenabled"])?);
    drop(client);

    server.restart(true)?;
    let mut client = server.client()?;
    assert_bulk(client.command(&["GET", "before"])?, "enabled");
    assert_bulk(client.command(&["GET", "during"])?, "enabled");
    assert_eq!(
        client.command(&["GET", "while"])?,
        RespFrame::BulkString(None)
    );
    assert_bulk(client.command(&["GET", "after"])?, "reenabled");
    Ok(())
}

#[test]
fn exec_runtime_aof_config_uses_the_final_committed_state() -> io::Result<()> {
    let mut server = Server::new(false)?;
    let mut client = server.client()?;

    // An aborted transaction must not activate the AOF writer merely because
    // it queued an appendonly transition.
    assert_ok(client.command(&["MULTI"])?);
    assert_eq!(
        client.command(&["CONFIG", "SET", "appendonly", "yes"])?,
        RespFrame::queued()
    );
    assert!(matches!(
        client.command(&["NO_SUCH_COMMAND"])?,
        RespFrame::Error(_)
    ));
    assert!(matches!(client.command(&["EXEC"])?, RespFrame::Error(_)));
    assert_eq!(
        client.command(&["CONFIG", "GET", "appendonly"])?,
        RespFrame::Array(vec![
            RespFrame::bulk_str("appendonly"),
            RespFrame::bulk_str("no")
        ])
    );

    // The enabled transaction has two INCRs around CONFIG SET. A fresh BASE
    // must contain the final counter exactly once, rather than replaying an
    // incremental copy of the same committed transaction on restart.
    assert_ok(client.command(&["MULTI"])?);
    assert_eq!(client.command(&["INCR", "counter"])?, RespFrame::queued());
    assert_eq!(
        client.command(&["CONFIG", "SET", "appendonly", "yes"])?,
        RespFrame::queued()
    );
    assert_eq!(
        client.command(&["CONFIG", "SET", "appendfsync", "always"])?,
        RespFrame::queued()
    );
    assert_eq!(client.command(&["INCR", "counter"])?, RespFrame::queued());
    assert_eq!(
        client.command(&["EXEC"])?,
        RespFrame::Array(vec![
            RespFrame::Integer(1),
            RespFrame::ok(),
            RespFrame::ok(),
            RespFrame::Integer(2),
        ])
    );
    assert_eq!(
        client.command(&["CONFIG", "GET", "appendfsync"])?,
        RespFrame::Array(vec![
            RespFrame::bulk_str("appendfsync"),
            RespFrame::bulk_str("always")
        ])
    );
    let RespFrame::BulkString(Some(info)) = client.command(&["INFO", "persistence"])? else {
        panic!("INFO should return a bulk string");
    };
    assert!(String::from_utf8_lossy(&info).contains("aof_enabled:1"));

    // Disabling inside EXEC still has to append both committed INCRs before
    // the old writer flushes and shuts down. The chosen fsync policy must also
    // reflect the writer's completed control command.
    assert_ok(client.command(&["MULTI"])?);
    assert_eq!(client.command(&["INCR", "counter"])?, RespFrame::queued());
    assert_eq!(
        client.command(&["CONFIG", "SET", "appendfsync", "no"])?,
        RespFrame::queued()
    );
    assert_eq!(
        client.command(&["CONFIG", "SET", "appendonly", "no"])?,
        RespFrame::queued()
    );
    assert_eq!(client.command(&["INCR", "counter"])?, RespFrame::queued());
    assert_eq!(
        client.command(&["EXEC"])?,
        RespFrame::Array(vec![
            RespFrame::Integer(3),
            RespFrame::ok(),
            RespFrame::ok(),
            RespFrame::Integer(4),
        ])
    );
    assert_eq!(
        client.command(&["CONFIG", "GET", "appendonly"])?,
        RespFrame::Array(vec![
            RespFrame::bulk_str("appendonly"),
            RespFrame::bulk_str("no")
        ])
    );
    assert_eq!(
        client.command(&["CONFIG", "GET", "appendfsync"])?,
        RespFrame::Array(vec![
            RespFrame::bulk_str("appendfsync"),
            RespFrame::bulk_str("no")
        ])
    );
    let RespFrame::BulkString(Some(info)) = client.command(&["INFO", "persistence"])? else {
        panic!("INFO should return a bulk string");
    };
    assert!(String::from_utf8_lossy(&info).contains("aof_enabled:0"));
    drop(client);

    server.appendonly = true;
    server.restart(true)?;
    let mut client = server.client()?;
    assert_bulk(client.command(&["GET", "counter"])?, "4");
    Ok(())
}
