use std::{
    ffi::OsStr,
    fs, io,
    path::{Path, PathBuf},
    process::{Command, Output},
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
