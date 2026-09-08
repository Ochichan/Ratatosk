//! Two-process crash tests for the experimental shared-memory transport.
//!
//! Requires `--features shm-transport`. The test binary re-executes itself as
//! the client process (`RATATOSK_SHM_TEST_CLIENT=<socket>`), so both sides are
//! real processes and SIGKILL means SIGKILL.
#![cfg(all(unix, feature = "shm-transport"))]

use std::{
    fs, io,
    io::{Read, Write},
    net::TcpStream,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

const CLIENT_ENV: &str = "RATATOSK_SHM_TEST_CLIENT";

fn ratatosk_bin() -> PathBuf {
    PathBuf::from(
        std::env::var_os("CARGO_BIN_EXE_ratatosk").expect("CARGO_BIN_EXE_ratatosk for crash tests"),
    )
}

struct Server {
    child: Child,
    tcp_port: u16,
    shm_socket: PathBuf,
    _dir: tempfile::TempDir,
}

impl Server {
    fn start(label: &str) -> io::Result<Self> {
        let dir = tempfile::tempdir()?;
        // Keep the socket path short (macOS sun_path is 104 bytes); tests in this
        // binary run concurrently, so the label keeps directories distinct.
        let short = PathBuf::from(format!("/tmp/rtk-shm-{}-{label}", std::process::id()));
        let _ = fs::remove_dir_all(&short);
        fs::create_dir_all(&short)?;
        let shm_socket = short.join("s.sock");
        let bound_file = dir.path().join("bound.json");
        let child = Command::new(ratatosk_bin())
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("RATATOSK_BIND", "127.0.0.1")
            .env("RATATOSK_PORT", "0")
            .env("RATATOSK_DIR", dir.path())
            .env("RATATOSK_BOUND_ADDR_FILE", &bound_file)
            .env("RATATOSK_SHM_SOCKET", &shm_socket)
            .env("RATATOSK_SHM_SPIN_ITERS", "100")
            .env("RATATOSK_DISABLE_CONFIG_AUTOLOAD", "true")
            .env("RATATOSK_METRICS_BIND", "127.0.0.1:0")
            .env("RATATOSK_ALLOW_NO_METRICS", "true")
            .env("RATATOSK_CONN_RATE_LIMIT_MAX_ATTEMPTS", "1000")
            .env("RATATOSK_SHUTDOWN_GRACE_MS", "500")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?;
        let mut server = Self {
            child,
            tcp_port: 0,
            shm_socket,
            _dir: dir,
        };
        server.tcp_port = wait_for_bound_port(&bound_file, &mut server.child)?;
        Ok(server)
    }

    /// Number of connected clients as seen by INFO. The probing TCP connection
    /// itself is counted, so an otherwise idle server reports 1.
    fn connected_clients(&self) -> io::Result<u64> {
        let mut stream = TcpStream::connect(("127.0.0.1", self.tcp_port))?;
        stream.set_read_timeout(Some(Duration::from_secs(2)))?;
        stream.write_all(b"*2\r\n$4\r\nINFO\r\n$7\r\nclients\r\n")?;
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let n = stream.read(&mut chunk)?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            if buf.windows(2).filter(|w| w == b"\r\n").count() >= 2 && buf.ends_with(b"\r\n") {
                // bulk string: first line is $len, then payload; good enough once payload ends
                if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                    let len: usize = std::str::from_utf8(&buf[1..pos - 1])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                    if buf.len() >= pos + 1 + len + 2 {
                        break;
                    }
                }
            }
        }
        let text = String::from_utf8_lossy(&buf);
        let value = text
            .lines()
            .find_map(|line| line.strip_prefix("connected_clients:"))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("no connected_clients in {text:?}"),
                )
            })?
            .trim()
            .parse::<u64>()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(value)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(parent) = self.shm_socket.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }
}

fn wait_for_bound_port(path: &Path, child: &mut Child) -> io::Result<u16> {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Ok(contents) = fs::read_to_string(path) {
            let value: serde_json::Value = serde_json::from_str(&contents)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let port = value["bound_port"].as_u64().unwrap_or(0) as u16;
            if port != 0 && value["shm_socket"].is_string() {
                return Ok(port);
            }
        }
        if let Some(status) = child.try_wait()? {
            let mut stderr = String::new();
            if let Some(mut pipe) = child.stderr.take() {
                let _ = pipe.read_to_string(&mut stderr);
            }
            return Err(io::Error::other(format!(
                "ratatosk exited before binding: {status}\n{stderr}"
            )));
        }
        thread::sleep(Duration::from_millis(25));
    }
    Err(io::Error::new(io::ErrorKind::TimedOut, "bound file"))
}

fn wait_until<F: FnMut() -> bool>(mut condition: F, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        thread::sleep(Duration::from_millis(50));
    }
    false
}

async fn shm_client(socket: &Path) -> ratatosk_shm::ShmStream {
    let config = ratatosk_shm::ClientConfig {
        spin_iters: 100,
        ..ratatosk_shm::ClientConfig::default()
    };
    ratatosk_shm::connect_shm(socket, &config)
        .await
        .expect("connect shm client")
}

/// Child-process body: connect over shared memory, prove the session works,
/// then idle until killed. Runs only when the env var is set (see tests below).
#[test]
fn shm_test_client_child() {
    let Some(socket) = std::env::var_os(CLIENT_ENV) else {
        return;
    };
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    runtime.block_on(async {
        let mut client = shm_client(Path::new(&socket)).await;
        client
            .write_all(b"*1\r\n$4\r\nPING\r\n")
            .await
            .expect("write ping");
        let mut buf = [0u8; 16];
        let n = client.read(&mut buf).await.expect("read pong");
        assert_eq!(&buf[..n], b"+PONG\r\n");
        // Signal readiness on stdout, then idle inside the session forever.
        println!("READY");
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
}

#[test]
fn client_sigkill_releases_the_server_session() {
    let server = Server::start("client-kill").expect("start server");
    assert_eq!(
        server.connected_clients().expect("info"),
        1,
        "only the INFO probe"
    );

    let mut child = Command::new(std::env::current_exe().expect("test exe"))
        .arg("--exact")
        .arg("shm_test_client_child")
        .arg("--nocapture")
        .env(CLIENT_ENV, &server.shm_socket)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn client process");
    // Wait for READY.
    let mut stdout = child.stdout.take().expect("child stdout");
    let mut line = [0u8; 64];
    let mut got = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !String::from_utf8_lossy(&got).contains("READY") && Instant::now() < deadline {
        let n = stdout.read(&mut line).expect("read child stdout");
        if n == 0 {
            break;
        }
        got.extend_from_slice(&line[..n]);
    }
    assert!(
        String::from_utf8_lossy(&got).contains("READY"),
        "client never became ready: {}",
        String::from_utf8_lossy(&got)
    );
    assert!(
        wait_until(
            || server.connected_clients().unwrap_or(0) == 2,
            Duration::from_secs(5)
        ),
        "server should count the shm client (plus the INFO probe)"
    );

    child.kill().expect("SIGKILL client");
    child.wait().expect("reap client");

    assert!(
        wait_until(
            || server.connected_clients().unwrap_or(u64::MAX) == 1,
            Duration::from_secs(10)
        ),
        "server did not release the shm session within 10s after client SIGKILL"
    );
    // The server is still healthy for new clients on TCP and SHM.
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    runtime.block_on(async {
        let mut client = shm_client(&server.shm_socket).await;
        client
            .write_all(b"*1\r\n$4\r\nPING\r\n")
            .await
            .expect("write");
        let mut buf = [0u8; 16];
        let n = client.read(&mut buf).await.expect("read");
        assert_eq!(&buf[..n], b"+PONG\r\n");
    });
}

#[test]
fn server_sigkill_fails_the_client_in_bounded_time() {
    let mut server = Server::start("server-kill").expect("start server");
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    runtime.block_on(async {
        let mut client = shm_client(&server.shm_socket).await;
        client
            .write_all(b"*1\r\n$4\r\nPING\r\n")
            .await
            .expect("write");
        let mut buf = [0u8; 16];
        let n = client.read(&mut buf).await.expect("read");
        assert_eq!(&buf[..n], b"+PONG\r\n");

        server.child.kill().expect("SIGKILL server");
        server.child.wait().expect("reap server");

        let started = Instant::now();
        let outcome = tokio::time::timeout(Duration::from_secs(3), async {
            // A write may still succeed into the ring; the read must fail or EOF.
            let _ = client.write_all(b"*1\r\n$4\r\nPING\r\n").await;
            client.read(&mut buf).await
        })
        .await;
        match outcome {
            Ok(Ok(0)) | Ok(Err(_)) => {}
            Ok(Ok(n)) => panic!("unexpected {n} bytes after server SIGKILL"),
            Err(_) => panic!("client did not observe server death within 3s"),
        }
        assert!(started.elapsed() < Duration::from_secs(3));
    });
}
