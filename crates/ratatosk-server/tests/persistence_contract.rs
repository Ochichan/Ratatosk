use std::{
    io::{self, Read, Write},
    net::TcpStream,
    path::Path,
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
    starts: u32,
    appendonly: bool,
    compatibility_mode: &'static str,
}

impl Server {
    fn new(appendonly: bool) -> io::Result<Self> {
        let mut server = Self {
            child: None,
            dir: tempfile::tempdir()?,
            port: 0,
            starts: 0,
            appendonly,
            compatibility_mode: "compat",
        };
        server.start()?;
        Ok(server)
    }

    fn start(&mut self) -> io::Result<()> {
        // Let the OS pick the ports and read the one bound from the handoff
        // file. Reserving a port and releasing it raced with parallel tests,
        // which could then connect to another test's server.
        self.starts += 1;
        let bound_addr_file = self.dir.path().join(format!("bound-{}.json", self.starts));
        let bin = std::env::var_os("CARGO_BIN_EXE_ratatosk").ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "CARGO_BIN_EXE_ratatosk is unavailable for persistence contract tests",
            )
        })?;
        let child = Command::new(bin)
            .arg("--no-config-autoload")
            .env("RATATOSK_BIND", "127.0.0.1")
            .env("RATATOSK_PORT", "0")
            .env("RATATOSK_BOUND_ADDR_FILE", &bound_addr_file)
            .env("RATATOSK_DIR", self.dir.path())
            .env("RATATOSK_APPENDONLY", self.appendonly.to_string())
            .env("RATATOSK_COMPATIBILITY_MODE", self.compatibility_mode)
            .env("RATATOSK_APPENDFSYNC", "always")
            .env("RATATOSK_METRICS_BIND", "127.0.0.1:0")
            .env("RATATOSK_ALLOW_NO_METRICS", "true")
            .env("RATATOSK_CONN_RATE_LIMIT_MAX_ATTEMPTS", "100000")
            .env("RATATOSK_SHUTDOWN_GRACE_MS", "1000")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        self.child = Some(child);
        self.port = wait_for_bound_port(&bound_addr_file, self.child.as_mut().expect("child"))?;
        Ok(())
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

fn wait_for_bound_port(path: &Path, child: &mut Child) -> io::Result<u16> {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait()? {
            return Err(io::Error::other(format!(
                "ratatosk exited before accepting persistence test connections: {status}"
            )));
        }
        match std::fs::read(path) {
            Ok(contents) => {
                let handoff: serde_json::Value = serde_json::from_slice(&contents)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                let address: std::net::SocketAddr = handoff["bound_addr"]
                    .as_str()
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "missing bound_addr")
                    })?
                    .parse()
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                return Ok(address.port());
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        thread::sleep(Duration::from_millis(20));
    }
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        "timed out waiting for the bound address handoff",
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
fn aof_replays_trimmed_xadd_and_xtrim_to_identical_entries() -> io::Result<()> {
    fn entries(client: &mut Client, key: &str) -> io::Result<RespFrame> {
        client.command(&["XRANGE", key, "-", "+"])
    }

    let mut server = Server::new(true)?;
    let mut client = server.client()?;
    for ms in 1..=300 {
        let id = format!("{ms}-0");
        client.command(&["XADD", "events", &id, "f", "v"])?;
    }
    // `~` with LIMIT is logged as an exact MAXLEN, with the generated ID.
    client.command(&[
        "XADD", "events", "MAXLEN", "~", "3", "LIMIT", "100", "*", "g", "w",
    ])?;
    client.command(&["XTRIM", "events", "MAXLEN", "~", "4", "LIMIT", "150"])?;
    // NOMKSTREAM on a missing key creates and logs nothing.
    assert_eq!(
        client.command(&["XADD", "absent", "NOMKSTREAM", "*", "f", "v"])?,
        RespFrame::BulkString(None)
    );
    let before = entries(&mut client, "events")?;
    let RespFrame::Array(rows) = &before else {
        panic!("XRANGE should return an array");
    };
    assert_eq!(rows.len(), 101);
    drop(client);
    server.stop(false)?;
    server.start()?;

    let mut client = server.client()?;
    assert_eq!(entries(&mut client, "events")?, before);
    assert_eq!(
        client.command(&["EXISTS", "absent"])?,
        RespFrame::Integer(0)
    );
    Ok(())
}

#[test]
fn stream_metadata_survives_aof_replay_and_rewrite() -> io::Result<()> {
    fn stream_info(client: &mut Client) -> io::Result<Vec<RespFrame>> {
        let RespFrame::Array(items) = client.command(&["XINFO", "STREAM", "events"])? else {
            panic!("XINFO STREAM should return an array");
        };
        let field = |name: &str| {
            let at = items
                .iter()
                .position(|item| *item == RespFrame::bulk_str(name))
                .unwrap_or_else(|| panic!("XINFO STREAM has no {name}"));
            items[at + 1].clone()
        };
        Ok(vec![
            field("last-generated-id"),
            field("entries-added"),
            field("max-deleted-entry-id"),
        ])
    }

    let mut server = Server::new(true)?;
    let mut client = server.client()?;
    client.command(&["XADD", "events", "5-1", "f", "v"])?;
    client.command(&["XADD", "events", "6-0", "f", "v"])?;
    assert_eq!(
        client.command(&["XDEL", "events", "6-0"])?,
        RespFrame::Integer(1)
    );
    assert_ok(client.command(&["XSETID", "events", "9-9", "ENTRIESADDED", "12"])?);
    let expected = vec![
        RespFrame::bulk_str("9-9"),
        RespFrame::Integer(12),
        RespFrame::bulk_str("6-0"),
    ];
    assert_eq!(stream_info(&mut client)?, expected);
    drop(client);

    // Replaying the AOF rebuilds the metadata from the logged commands.
    server.restart(false)?;
    let mut client = server.client()?;
    assert_eq!(stream_info(&mut client)?, expected);

    // A rewrite stores the stream in the RDB BASE, which must keep it too.
    client.command(&["BGREWRITEAOF"])?;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let RespFrame::BulkString(Some(info)) = client.command(&["INFO", "persistence"])? else {
            panic!("INFO should return a bulk string");
        };
        if String::from_utf8_lossy(&info).contains("aof_rewrite_in_progress:0") {
            break;
        }
        assert!(Instant::now() < deadline, "AOF rewrite did not finish");
        thread::sleep(Duration::from_millis(20));
    }
    drop(client);
    server.restart(false)?;
    let mut client = server.client()?;
    assert_eq!(stream_info(&mut client)?, expected);
    assert_eq!(
        client.command(&["XADD", "events", "9-9", "f", "v"])?,
        RespFrame::Error(Bytes::from_static(
            b"ERR The ID specified in XADD is equal or smaller than the target stream top item"
        ))
    );
    Ok(())
}

#[test]
fn legacy_xsetid_records_now_set_the_last_id_on_replay() -> io::Result<()> {
    let mut server = Server::new(true)?;
    let mut client = server.client()?;
    client.command(&["XADD", "events", "5-1", "f", "v"])?;
    drop(client);
    server.stop(false)?;

    // Older builds logged XSETID without applying it. Its replay now applies it.
    let mut incr_files = std::fs::read_dir(server.dir.path())?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.to_string_lossy().ends_with(".incr.aof"))
        .collect::<Vec<_>>();
    incr_files.sort();
    let incr = incr_files.last().expect("AOF INCR file");
    let record = b"*3\r\n$15\r\nRATATOSK.AOF.AT\r\n:1\r\n*3\r\n$6\r\nXSETID\r\n$6\r\nevents\r\n$4\r\n90-0\r\n";
    std::fs::OpenOptions::new()
        .append(true)
        .open(incr)?
        .write_all(record)?;

    server.start()?;
    let mut client = server.client()?;
    assert_eq!(
        client.command(&["XADD", "events", "50-0", "f", "v"])?,
        RespFrame::Error(Bytes::from_static(
            b"ERR The ID specified in XADD is equal or smaller than the target stream top item"
        ))
    );
    assert_eq!(
        client.command(&["XADD", "events", "91-0", "f", "v"])?,
        RespFrame::bulk_str("91-0")
    );
    Ok(())
}

#[test]
fn startup_replays_raw_stream_trims_an_earlier_version_logged() -> io::Result<()> {
    let mut server = Server::new(true)?;
    let mut client = server.client()?;
    client.command(&["SET", "marker", "1"])?;
    drop(client);
    server.stop(false)?;

    // An earlier build logged these as sent: a LIMIT without `~`, and an ID
    // longer than a client may send.
    let padded = format!("{}2-0", "0".repeat(130));
    let mut commands: Vec<Vec<String>> = Vec::new();
    for ms in 1..=6 {
        for key in ["a", "b"] {
            commands.push(vec![
                "XADD".into(),
                key.into(),
                format!("{ms}-0"),
                "f".into(),
                "v".into(),
            ]);
        }
    }
    for tail in [
        &["XTRIM", "a", "MAXLEN", "3", "LIMIT", "2"][..],
        &["XTRIM", "b", "MAXLEN", "~", "5"],
    ] {
        commands.push(tail.iter().map(|part| (*part).to_owned()).collect());
    }
    commands.push(vec!["XDEL".into(), "b".into(), padded]);
    let mut record = Vec::new();
    for command in &commands {
        record.extend_from_slice(format!("*{}\r\n", command.len()).as_bytes());
        for part in command {
            record.extend_from_slice(format!("${}\r\n{part}\r\n", part.len()).as_bytes());
        }
    }
    let mut incr_files = std::fs::read_dir(server.dir.path())?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.to_string_lossy().ends_with(".incr.aof"))
        .collect::<Vec<_>>();
    incr_files.sort();
    let incr = incr_files.last().expect("AOF INCR file");
    std::fs::OpenOptions::new()
        .append(true)
        .open(incr)?
        .write_all(&record)?;

    server.start()?;
    let mut client = server.client()?;
    let ids = |frame: RespFrame| -> Vec<String> {
        let RespFrame::Array(rows) = frame else {
            panic!("XRANGE should return an array");
        };
        rows.into_iter()
            .map(|row| {
                let RespFrame::Array(parts) = row else {
                    panic!("entry should be an array");
                };
                let RespFrame::BulkString(Some(id)) = &parts[0] else {
                    panic!("entry should start with an ID");
                };
                String::from_utf8_lossy(id).into_owned()
            })
            .collect()
    };
    // LIMIT 2 capped the exact trim: six entries with MAXLEN 3 leave four.
    assert_eq!(
        ids(client.command(&["XRANGE", "a", "-", "+"])?),
        ["3-0", "4-0", "5-0", "6-0"]
    );
    // `~ 5` trimmed exactly (one entry), then the padded ID 2-0 was already gone.
    assert_eq!(
        ids(client.command(&["XRANGE", "b", "-", "+"])?),
        ["3-0", "4-0", "5-0", "6-0"]
    );
    Ok(())
}

#[test]
fn strict_mode_restart_replays_commands_it_rejects_from_clients() -> io::Result<()> {
    let mut server = Server::new(true)?;
    let mut client = server.client()?;
    assert_eq!(
        client.command(&["XADD", "events", "5-1", "field", "value"])?,
        RespFrame::bulk_str("5-1")
    );
    drop(client);
    server.stop(false)?;

    // Older builds logged XCFGSET. Append such a record to the newest INCR file.
    let mut incr_files = std::fs::read_dir(server.dir.path())?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.to_string_lossy().ends_with(".incr.aof"))
        .collect::<Vec<_>>();
    incr_files.sort();
    let incr = incr_files.last().expect("AOF INCR file");
    let record = b"*3\r\n$15\r\nRATATOSK.AOF.AT\r\n:1\r\n*4\r\n$7\r\nXCFGSET\r\n$6\r\nevents\r\n$13\r\nIDMP-DURATION\r\n$2\r\n10\r\n";
    std::fs::OpenOptions::new()
        .append(true)
        .open(incr)?
        .write_all(record)?;

    server.compatibility_mode = "strict";
    server.start()?;
    let mut client = server.client()?;
    assert_eq!(client.command(&["XLEN", "events"])?, RespFrame::Integer(1));
    let RespFrame::Error(message) =
        client.command(&["XCFGSET", "events", "IDMP-DURATION", "10"])?
    else {
        panic!("strict mode should reject XCFGSET from a client");
    };
    assert!(
        message.starts_with(b"ERR command XCFGSET is not supported in Ratatosk strict"),
        "unexpected error: {}",
        String::from_utf8_lossy(&message)
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
