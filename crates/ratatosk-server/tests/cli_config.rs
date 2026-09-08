use std::{
    ffi::OsStr,
    fs, io,
    path::{Path, PathBuf},
    process::{Command, Output},
};

#[cfg(unix)]
use std::{
    os::unix::fs::PermissionsExt,
    process::{Child, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

#[cfg(unix)]
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};

use serde_json::Value;

fn ratatosk_bin() -> io::Result<PathBuf> {
    std::env::var_os("CARGO_BIN_EXE_ratatosk")
        .map(PathBuf::from)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "CARGO_BIN_EXE_ratatosk is not available for CLI integration tests",
            )
        })
}

fn run_ratatosk<I, S>(cwd: &Path, args: I, envs: &[(&str, Option<&str>)]) -> io::Result<Output>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut command = Command::new(ratatosk_bin()?);
    command.current_dir(cwd).args(args);

    for name in [
        "RATATOSK_CONFIG",
        "RATATOSK_DISABLE_CONFIG_AUTOLOAD",
        "RATATOSK_BIND",
        "RATATOSK_PORT",
        "RATATOSK_UNIXSOCKET",
        "RATATOSK_UNIXSOCKETPERM",
        "RATATOSK_DIR",
        "RATATOSK_METRICS_BIND",
        "RATATOSK_ALLOW_NO_METRICS",
        "RATATOSK_BOUND_ADDR_FILE",
    ] {
        command.env_remove(name);
    }

    for (name, value) in envs {
        match value {
            Some(value) => {
                command.env(name, value);
            }
            None => {
                command.env_remove(name);
            }
        }
    }

    command.output()
}

#[cfg(unix)]
struct UnixServerGuard {
    child: Child,
}

#[cfg(unix)]
impl UnixServerGuard {
    fn id(&self) -> u32 {
        self.child.id()
    }

    fn wait_for_exit(&mut self, timeout: Duration) -> io::Result<ExitStatus> {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if let Some(status) = self.child.try_wait()? {
                return Ok(status);
            }
            thread::sleep(Duration::from_millis(25));
        }

        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "ratatosk process {} did not exit within {timeout:?}",
                self.id()
            ),
        ))
    }
}

#[cfg(unix)]
impl Drop for UnixServerGuard {
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

#[cfg(unix)]
fn unixsocket_server_command(
    cwd: &Path,
    socket_path: &Path,
    bound_addr_file: &Path,
) -> io::Result<Command> {
    let mut command = Command::new(ratatosk_bin()?);
    command
        .current_dir(cwd)
        .env_remove("RATATOSK_CONFIG")
        .env("RATATOSK_DISABLE_CONFIG_AUTOLOAD", "true")
        .env("RATATOSK_BIND", "127.0.0.1")
        .env("RATATOSK_PORT", "0")
        .env("RATATOSK_UNIXSOCKET", socket_path)
        .env("RATATOSK_UNIXSOCKETPERM", "700")
        .env("RATATOSK_DIR", cwd)
        .env("RATATOSK_BOUND_ADDR_FILE", bound_addr_file)
        .env("RATATOSK_AUDIT_LOG", cwd.join("audit.log"))
        .env("RATATOSK_AUDIT_CHAIN_STATE", cwd.join("audit.state"))
        .env("RATATOSK_METRICS_BIND", "127.0.0.1:0")
        .env("RATATOSK_ALLOW_NO_METRICS", "true")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    Ok(command)
}

#[cfg(unix)]
async fn wait_for_bound_addr_file(path: &Path, child: &mut Child) -> io::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        match fs::read(path) {
            Ok(_) => return Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }

        if let Some(status) = child.try_wait()? {
            return Err(io::Error::other(format!(
                "ratatosk exited before writing bound address file {}: {status}",
                path.display()
            )));
        }

        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        format!(
            "timed out waiting for bound address file {}",
            path.display()
        ),
    ))
}

#[cfg(unix)]
async fn read_bulk_string(stream: &mut UnixStream) -> io::Result<String> {
    let mut header = Vec::with_capacity(16);
    loop {
        let mut byte = [0_u8; 1];
        stream.read_exact(&mut byte).await?;
        header.push(byte[0]);
        if header.ends_with(b"\r\n") {
            break;
        }
        if header.len() > 32 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "RESP bulk-string length header is too long",
            ));
        }
    }

    let length = std::str::from_utf8(&header)
        .ok()
        .and_then(|header| header.strip_prefix('$'))
        .and_then(|header| header.strip_suffix("\r\n"))
        .and_then(|header| header.parse::<usize>().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid RESP bulk header"))?;
    let mut payload = vec![0; length + 2];
    stream.read_exact(&mut payload).await?;
    if payload[length..] != *b"\r\n" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "RESP bulk string did not end with CRLF",
        ));
    }
    payload.truncate(length);
    String::from_utf8(payload).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn stdout_json(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).expect("stdout should be valid json")
}

fn stderr_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn print_config_json_honors_explicit_config_and_env_overrides() -> io::Result<()> {
    let temp = tempfile::tempdir()?;
    let config_path = temp.path().join("custom.conf");
    fs::write(
        &config_path,
        r#"
bind 127.0.0.1
port 6381
dbfilename "snapshot data.rdb"
query-buffer-limit 4096
"#,
    )?;

    let output = run_ratatosk(
        temp.path(),
        [
            "--config",
            config_path.to_str().unwrap(),
            "--print-config",
            "json",
        ],
        &[("RATATOSK_PORT", Some("6382"))],
    )?;

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        stderr_text(&output)
    );

    let payload = stdout_json(&output);
    assert_eq!(payload["config_file_source"], "cli");
    assert_eq!(
        payload["config_path"],
        config_path.to_string_lossy().as_ref()
    );
    assert_eq!(payload["config"]["port"], 6382);
    assert_eq!(payload["config"]["dbfilename"], "snapshot data.rdb");
    assert_eq!(payload["config"]["query_buffer_limit"], 4096);

    Ok(())
}

#[test]
fn no_config_autoload_ignores_local_ratatosk_conf() -> io::Result<()> {
    let temp = tempfile::tempdir()?;
    fs::write(temp.path().join("ratatosk.conf"), "port 6399\n")?;

    let output = run_ratatosk(
        temp.path(),
        ["--no-config-autoload", "--print-config", "json"],
        &[],
    )?;

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        stderr_text(&output)
    );

    let payload = stdout_json(&output);
    assert_eq!(payload["config_file_source"], Value::Null);
    assert_eq!(payload["config_path"], Value::Null);
    assert_eq!(payload["auto_config_discovery_enabled"], false);
    assert_eq!(payload["config"]["port"], 6379);

    Ok(())
}

#[test]
fn check_config_fails_for_invalid_auto_loaded_local_config() -> io::Result<()> {
    let temp = tempfile::tempdir()?;
    fs::write(temp.path().join("ratatosk.conf"), "hz 0\n")?;

    let output = run_ratatosk(temp.path(), ["--check-config"], &[])?;

    assert!(!output.status.success(), "command unexpectedly succeeded");
    let stderr = stderr_text(&output);
    assert!(stderr.contains("parsing config file"));
    assert!(stderr.contains("directive 'hz' requires a value in 1..=500"));

    Ok(())
}

#[test]
fn check_config_fails_for_unwritable_bound_addr_file_parent() -> io::Result<()> {
    let temp = tempfile::tempdir()?;
    let blocker = temp.path().join("not-a-directory");
    fs::write(&blocker, "x")?;
    let bound_addr_file = blocker.join("bound-addr.json");

    let output = run_ratatosk(
        temp.path(),
        ["--check-config"],
        &[(
            "RATATOSK_BOUND_ADDR_FILE",
            Some(bound_addr_file.to_str().unwrap()),
        )],
    )?;

    assert!(!output.status.success(), "command unexpectedly succeeded");
    let stderr = stderr_text(&output);
    assert!(
        stderr.contains("running startup preflight checks"),
        "stderr did not include preflight context:\n{stderr}"
    );
    assert!(
        stderr.contains("bound address handoff parent is not a directory"),
        "stderr did not include bound address handoff failure:\n{stderr}"
    );

    Ok(())
}

#[cfg(unix)]
#[test]
fn unixsocket_config_validation_printing_and_env_override_work() -> io::Result<()> {
    let temp = tempfile::tempdir()?;
    let socket_path = temp.path().join("ratatosk.sock");

    let check = run_ratatosk(
        temp.path(),
        ["--check-config"],
        &[(
            "RATATOSK_UNIXSOCKET",
            Some(socket_path.to_str().expect("UTF-8 temp path")),
        )],
    )?;
    assert!(
        check.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&check.stdout),
        stderr_text(&check)
    );

    let printed = run_ratatosk(
        temp.path(),
        ["--print-config", "json"],
        &[
            (
                "RATATOSK_UNIXSOCKET",
                Some(socket_path.to_str().expect("UTF-8 temp path")),
            ),
            ("RATATOSK_UNIXSOCKETPERM", Some("750")),
        ],
    )?;
    assert!(
        printed.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&printed.stdout),
        stderr_text(&printed)
    );
    let payload = stdout_json(&printed);
    assert_eq!(
        payload["config"]["unixsocket"],
        socket_path.display().to_string()
    );
    assert_eq!(payload["config"]["unixsocketperm"], "750");

    let missing_parent_socket = temp.path().join("missing").join("ratatosk.sock");
    let invalid = run_ratatosk(
        temp.path(),
        ["--check-config"],
        &[(
            "RATATOSK_UNIXSOCKET",
            Some(missing_parent_socket.to_str().expect("UTF-8 temp path")),
        )],
    )?;
    assert!(!invalid.status.success(), "command unexpectedly succeeded");
    assert!(
        stderr_text(&invalid).contains("Unix socket parent directory does not exist"),
        "stderr:\n{}",
        stderr_text(&invalid)
    );

    Ok(())
}

#[cfg(unix)]
#[test]
fn unixsocket_is_removed_when_startup_fails_after_binding() -> io::Result<()> {
    let temp = tempfile::tempdir()?;
    let socket_path = temp.path().join("ratatosk.sock");
    // Startup preflight checks the parent only. A directory at the final handoff path passes
    // that preflight, then makes the post-bind atomic rename fail.
    let bound_addr_file = temp.path().join("bound-addr-directory");
    fs::create_dir(&bound_addr_file)?;

    let mut command = unixsocket_server_command(temp.path(), &socket_path, &bound_addr_file)?;
    command.stderr(Stdio::piped());
    let output = command.output()?;

    assert!(!output.status.success(), "command unexpectedly succeeded");
    assert!(
        stderr_text(&output).contains("writing bound TCP listener address file"),
        "stderr:\n{}",
        stderr_text(&output)
    );
    assert!(
        !socket_path.exists(),
        "Unix socket file should be removed after post-bind startup failure"
    );

    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn unixsocket_server_accepts_clients_reports_metadata_and_cleans_up() -> io::Result<()> {
    let temp = tempfile::tempdir()?;
    let socket_path = temp.path().join("ratatosk.sock");
    let bound_addr_file = temp.path().join("bound-addr.json");
    let mut server = UnixServerGuard {
        child: unixsocket_server_command(temp.path(), &socket_path, &bound_addr_file)?.spawn()?,
    };

    wait_for_bound_addr_file(&bound_addr_file, &mut server.child).await?;

    let bound_addr: Value =
        serde_json::from_slice(&fs::read(&bound_addr_file)?).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid bound-address handoff JSON: {error}"),
            )
        })?;
    assert_eq!(
        bound_addr["unixsocket"],
        socket_path.display().to_string(),
        "bound-address handoff should include the configured Unix socket"
    );

    let mode = fs::metadata(&socket_path)?.permissions().mode() & 0o777;
    assert_eq!(mode, 0o700, "Unix socket mode should honor unixsocketperm");

    let mut client = UnixStream::connect(&socket_path).await?;
    client.write_all(b"PING\r\n").await?;
    let mut pong = [0_u8; 7];
    client.read_exact(&mut pong).await?;
    assert_eq!(&pong, b"+PONG\r\n");

    client
        .write_all(b"*2\r\n$6\r\nCLIENT\r\n$4\r\nLIST\r\n")
        .await?;
    let client_list = read_bulk_string(&mut client).await?;
    assert!(
        client_list.contains(&format!("addr={}:0", socket_path.display())),
        "CLIENT LIST did not report the Unix socket address: {client_list}"
    );
    let flags = client_list
        .split_whitespace()
        .find(|field| field.starts_with("flags="))
        .expect("CLIENT LIST should include flags");
    assert!(
        flags.contains('U'),
        "Unix socket client should have U flag: {flags}"
    );
    drop(client);

    let mut second_command =
        unixsocket_server_command(temp.path(), &socket_path, &bound_addr_file)?;
    second_command.stdout(Stdio::null()).stderr(Stdio::piped());
    let second = second_command.output()?;
    assert!(
        !second.status.success(),
        "second server unexpectedly started"
    );
    // The advisory lock refuses the second instance before the connect probe
    // runs, so the live server's socket is never touched.
    assert!(
        stderr_text(&second).contains("owned by another running server"),
        "stderr:\n{}",
        stderr_text(&second)
    );
    assert!(socket_path.exists(), "first server's socket must survive");

    let signal = Command::new("kill")
        .arg("-TERM")
        .arg(server.id().to_string())
        .status()?;
    assert!(signal.success(), "failed to send SIGTERM: {signal}");
    let status = server.wait_for_exit(Duration::from_secs(10))?;
    assert!(status.success(), "ratatosk should exit cleanly: {status}");
    assert!(
        !socket_path.exists(),
        "Unix socket file should be removed during shutdown"
    );

    Ok(())
}
