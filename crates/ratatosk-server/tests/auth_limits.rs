use std::{
    fs,
    io::{self, Read, Write},
    net::TcpStream,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::os::unix::net::UnixStream;

use bytes::{Bytes, BytesMut};
use ratatosk_resp::{RespFrame, encode_to_vec, parse};

const PASSWORD: &str = "auth-limit-test-secret";
const AUTH_ERROR: &str = "ERR invalid username-password pair or user is disabled.";

struct ServerGuard {
    child: Child,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
}

impl ServerGuard {
    fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.child.try_wait()
    }

    fn startup_error(&self, status: ExitStatus) -> io::Error {
        let stdout = fs::read_to_string(&self.stdout_path).unwrap_or_default();
        let stderr = fs::read_to_string(&self.stderr_path).unwrap_or_default();
        io::Error::other(format!(
            "ratatosk exited during startup ({status}); stdout={stdout:?}; stderr={stderr:?}"
        ))
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        match self.child.try_wait() {
            Ok(Some(_)) => {}
            Ok(None) | Err(_) => {
                let _ = self.child.kill();
                let _ = self.child.wait();
            }
        }
    }
}

fn ratatosk_bin() -> io::Result<PathBuf> {
    std::env::var_os("CARGO_BIN_EXE_ratatosk")
        .map(PathBuf::from)
        .or_else(|| option_env!("CARGO_BIN_EXE_ratatosk").map(PathBuf::from))
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "ratatosk test binary not found"))
}

fn spawn_server(dir: &Path, unix_socket: Option<&Path>) -> io::Result<(ServerGuard, u16)> {
    let bound_addr_file = dir.join("bound-address.json");
    let stdout_path = dir.join("ratatosk.stdout.log");
    let stderr_path = dir.join("ratatosk.stderr.log");
    let stdout = fs::File::create(&stdout_path)?;
    let stderr = fs::File::create(&stderr_path)?;
    let mut command = Command::new(ratatosk_bin()?);
    command
        .current_dir(dir)
        .env_clear()
        .env("RATATOSK_BIND", "127.0.0.1")
        .env("RATATOSK_PORT", "0")
        .env("RATATOSK_DIR", dir)
        .env("RATATOSK_DISABLE_CONFIG_AUTOLOAD", "true")
        .env("RATATOSK_BOUND_ADDR_FILE", &bound_addr_file)
        .env("RATATOSK_AUDIT_LOG", dir.join("audit.log"))
        .env("RATATOSK_AUDIT_CHAIN_STATE", dir.join("audit.state"))
        .env("RATATOSK_METRICS_BIND", "127.0.0.1:0")
        .env("RATATOSK_ALLOW_NO_METRICS", "true")
        .env("RATATOSK_CONN_RATE_LIMIT_MAX_ATTEMPTS", "100")
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    if let Some(socket) = unix_socket {
        command
            .env("RATATOSK_UNIXSOCKET", socket)
            .env("RATATOSK_UNIXSOCKETPERM", "700");
    }

    let child = command.spawn()?;
    let mut server = ServerGuard {
        child,
        stdout_path,
        stderr_path,
    };
    let port = wait_for_bound_port(&bound_addr_file, &mut server)?;
    Ok((server, port))
}

fn wait_for_bound_port(path: &Path, server: &mut ServerGuard) -> io::Result<u16> {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        match fs::read_to_string(path) {
            Ok(contents) => {
                let payload: serde_json::Value = serde_json::from_str(&contents)?;
                return payload["bound_port"]
                    .as_u64()
                    .and_then(|port| u16::try_from(port).ok())
                    .ok_or_else(|| io::Error::other("bound address file has no valid port"));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        if let Some(status) = server.try_wait()? {
            return Err(server.startup_error(status));
        }
        thread::sleep(Duration::from_millis(25));
    }
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        "timed out waiting for bound address file",
    ))
}

fn connect_tcp(port: u16) -> io::Result<TcpStream> {
    let stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.set_write_timeout(Some(Duration::from_secs(10)))?;
    Ok(stream)
}

fn command(parts: &[&str]) -> RespFrame {
    RespFrame::Array(
        parts
            .iter()
            .map(|part| RespFrame::BulkString(Some(Bytes::copy_from_slice(part.as_bytes()))))
            .collect(),
    )
}

fn send_command<S: Read + Write>(stream: &mut S, parts: &[&str]) -> io::Result<RespFrame> {
    let mut payload = Vec::new();
    encode_to_vec(&command(parts), &mut payload);
    stream.write_all(&payload)?;
    read_frame(stream)
}

fn read_frame<S: Read>(stream: &mut S) -> io::Result<RespFrame> {
    let mut input = BytesMut::with_capacity(1024);
    loop {
        match parse(&mut input) {
            Ok(Some(frame)) => return Ok(frame),
            Ok(None) => {
                let mut chunk = [0u8; 1024];
                let read = stream.read(&mut chunk)?;
                if read == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "connection closed before a complete response",
                    ));
                }
                input.extend_from_slice(&chunk[..read]);
            }
            Err(error) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    error.to_string(),
                ));
            }
        }
    }
}

fn read_frames_until_eof<S: Read>(stream: &mut S) -> io::Result<Vec<RespFrame>> {
    let mut input = BytesMut::with_capacity(4096);
    loop {
        let mut chunk = [0u8; 1024];
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            break;
        }
        input.extend_from_slice(&chunk[..read]);
    }

    let mut frames = Vec::new();
    loop {
        match parse(&mut input) {
            Ok(Some(frame)) => frames.push(frame),
            Ok(None) if input.is_empty() => return Ok(frames),
            Ok(None) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed with a partial response",
                ));
            }
            Err(error) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    error.to_string(),
                ));
            }
        }
    }
}

fn expect_eof<S: Read>(stream: &mut S) -> io::Result<()> {
    let mut byte = [0u8; 1];
    let read = stream.read(&mut byte)?;
    if read == 0 {
        Ok(())
    } else {
        Err(io::Error::other(
            "connection remained open after terminal response",
        ))
    }
}

fn configure_password(port: u16) -> io::Result<()> {
    let mut bootstrap = connect_tcp(port)?;
    assert_eq!(
        send_command(
            &mut bootstrap,
            &[
                "ACL",
                "SETUSER",
                "default",
                "on",
                "resetpass",
                &format!(">{PASSWORD}"),
                "+@all",
            ],
        )?,
        RespFrame::ok()
    );
    Ok(())
}

#[test]
fn tcp_auth_and_hello_share_failures_across_reconnections() -> io::Result<()> {
    let temp = tempfile::tempdir()?;
    let (_server, port) = spawn_server(temp.path(), None)?;
    configure_password(port)?;

    let mut initial = connect_tcp(port)?;
    assert_eq!(
        send_command(&mut initial, &["PING"])?,
        RespFrame::error_str("NOAUTH Authentication required.")
    );
    drop(initial);

    for attempt in 0..20 {
        if attempt == 10 {
            let mut successful = connect_tcp(port)?;
            assert!(matches!(
                send_command(
                    &mut successful,
                    &["HELLO", "3", "AUTH", "default", PASSWORD],
                )?,
                RespFrame::Map(_)
            ));
        }

        let mut client = connect_tcp(port)?;
        let response = if attempt % 2 == 0 {
            send_command(&mut client, &["AUTH", "wrong"])?
        } else {
            send_command(&mut client, &["HELLO", "3", "AUTH", "default", "wrong"])?
        };
        assert_eq!(response, RespFrame::error_str(AUTH_ERROR));
        if attempt == 19 {
            expect_eof(&mut client)?;
        }
    }

    let mut blocked = connect_tcp(port)?;
    assert_eq!(
        send_command(&mut blocked, &["AUTH", PASSWORD])?,
        RespFrame::error_str(AUTH_ERROR)
    );
    expect_eof(&mut blocked)?;
    Ok(())
}

#[test]
fn authenticated_exec_stops_after_terminal_auth_failure() -> io::Result<()> {
    let temp = tempfile::tempdir()?;
    let (_server, port) = spawn_server(temp.path(), None)?;
    configure_password(port)?;

    let mut client = connect_tcp(port)?;
    assert_eq!(
        send_command(&mut client, &["AUTH", PASSWORD])?,
        RespFrame::ok()
    );
    assert_eq!(send_command(&mut client, &["MULTI"])?, RespFrame::ok());
    assert_eq!(
        send_command(&mut client, &["SET", "before", "present"])?,
        RespFrame::queued()
    );
    for _ in 0..5 {
        assert_eq!(
            send_command(&mut client, &["AUTH", "wrong"])?,
            RespFrame::queued()
        );
    }
    assert_eq!(
        send_command(&mut client, &["SET", "after", "present"])?,
        RespFrame::queued()
    );

    let response = send_command(&mut client, &["EXEC"])?;
    let RespFrame::Array(replies) = response else {
        return Err(io::Error::other("EXEC did not return an array"));
    };
    assert_eq!(replies.len(), 6);
    assert_eq!(replies[0], RespFrame::ok());
    assert!(
        replies[1..]
            .iter()
            .all(|reply| matches!(reply, RespFrame::Error(_)))
    );
    expect_eof(&mut client)?;

    let mut observer = connect_tcp(port)?;
    assert_eq!(
        send_command(&mut observer, &["AUTH", PASSWORD])?,
        RespFrame::ok()
    );
    assert_eq!(
        send_command(&mut observer, &["GET", "before"])?,
        RespFrame::bulk_str("present")
    );
    assert_eq!(
        send_command(&mut observer, &["GET", "after"])?,
        RespFrame::BulkString(None)
    );
    Ok(())
}

#[test]
fn terminal_auth_failure_stops_the_rest_of_a_parsed_pipeline() -> io::Result<()> {
    let temp = tempfile::tempdir()?;
    let (_server, port) = spawn_server(temp.path(), None)?;
    configure_password(port)?;

    let mut client = connect_tcp(port)?;
    assert_eq!(
        send_command(&mut client, &["AUTH", PASSWORD])?,
        RespFrame::ok()
    );

    let commands: &[&[&str]] = &[
        &["AUTH", "wrong"],
        &["HELLO", "3", "AUTH", "default", "wrong"],
        &["AUTH", "wrong"],
        &["HELLO", "3", "AUTH", "default", "wrong"],
        &["AUTH", "wrong"],
        &["AUTH", PASSWORD],
        &["SET", "pipeline-suffix", "present"],
    ];
    let mut pipeline = Vec::new();
    for parts in commands {
        encode_to_vec(&command(parts), &mut pipeline);
    }
    client.write_all(&pipeline)?;

    let replies = read_frames_until_eof(&mut client)?;
    assert_eq!(replies.len(), 5);
    assert!(
        replies
            .iter()
            .all(|reply| reply == &RespFrame::error_str(AUTH_ERROR))
    );

    let mut observer = connect_tcp(port)?;
    assert_eq!(
        send_command(&mut observer, &["AUTH", PASSWORD])?,
        RespFrame::ok()
    );
    assert_eq!(
        send_command(&mut observer, &["GET", "pipeline-suffix"])?,
        RespFrame::BulkString(None)
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn unix_sessions_keep_the_five_failure_connection_limit() -> io::Result<()> {
    let temp = tempfile::tempdir()?;
    let socket_path = temp.path().join("ratatosk.sock");
    let (_server, port) = spawn_server(temp.path(), Some(&socket_path))?;
    configure_password(port)?;

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut client = loop {
        match UnixStream::connect(&socket_path) {
            Ok(stream) => break stream,
            Err(error) if Instant::now() < deadline => {
                let _ = error;
                thread::sleep(Duration::from_millis(25));
            }
            Err(error) => return Err(error),
        }
    };
    client.set_read_timeout(Some(Duration::from_secs(10)))?;
    client.set_write_timeout(Some(Duration::from_secs(10)))?;

    for attempt in 0..5 {
        assert_eq!(
            send_command(&mut client, &["AUTH", "wrong"])?,
            RespFrame::error_str(AUTH_ERROR)
        );
        if attempt == 4 {
            expect_eof(&mut client)?;
        }
    }

    let mut fresh = UnixStream::connect(&socket_path)?;
    fresh.set_read_timeout(Some(Duration::from_secs(10)))?;
    fresh.set_write_timeout(Some(Duration::from_secs(10)))?;
    assert_eq!(
        send_command(&mut fresh, &["AUTH", PASSWORD])?,
        RespFrame::ok()
    );
    Ok(())
}
