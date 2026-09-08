use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

use serde_json::Value;

#[test]
fn smoke_writes_tcp_and_unix_rows() {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let server = built_server(&workspace);
    if !server.is_file() {
        build_server(&workspace);
    }
    assert!(
        server.is_file(),
        "server binary was not built at {}",
        server.display()
    );

    let output_dir =
        env::temp_dir().join(format!("ratatosk-ipc-bench-smoke-{}", std::process::id()));
    if output_dir.exists() {
        fs::remove_dir_all(&output_dir).expect("remove stale smoke output directory");
    }
    fs::create_dir(&output_dir).expect("create smoke output directory");

    let output = Command::new(env!("CARGO_BIN_EXE_ratatosk-ipc-bench"))
        .current_dir(&workspace)
        .arg("--server")
        .arg(&server)
        .arg("--out")
        .arg(&output_dir)
        .args([
            "--samples",
            "2000",
            "--warmup",
            "200",
            "--conns",
            "1",
            "--payloads",
            "ping",
            "--transports",
            "tcp,unix",
        ])
        .output()
        .expect("run IPC benchmark smoke test");

    let rendered = format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !output.status.success() && socket_bind_is_restricted(&stderr) {
        eprintln!(
            "skipping IPC socket smoke test because this sandbox forbids socket binds: {rendered}"
        );
        let _ = fs::remove_dir_all(&output_dir);
        return;
    }
    assert!(output.status.success(), "IPC harness failed: {rendered}");

    let artifact =
        fs::read_to_string(output_dir.join("latest.json")).expect("read latest artifact");
    let artifact: Value = serde_json::from_str(&artifact).expect("parse IPC artifact JSON");
    let rows = artifact["results"]
        .as_array()
        .expect("artifact results array");
    assert!(
        rows.iter().any(|row| {
            row["server"] == "ratatosk" && row["transport"] == "tcp" && row["status"] == "ok"
        }),
        "artifact has no measured Ratatosk TCP row: {artifact}"
    );
    assert!(
        rows.iter().any(|row| {
            row["server"] == "ratatosk" && row["transport"] == "unix" && row["status"] == "ok"
        }),
        "artifact has no measured Ratatosk Unix row: {artifact}"
    );

    fs::remove_dir_all(&output_dir).expect("remove smoke output directory");
}

fn built_server(workspace: &Path) -> PathBuf {
    if let Some(path) = env::var_os("RATATOSK_BIN") {
        let path = PathBuf::from(path);
        if path.is_file() {
            return path;
        }
    }
    let target = env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace.join("target"));
    target
        .join(if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        })
        .join("ratatosk")
}

fn build_server(workspace: &Path) {
    let mut command = Command::new("cargo");
    command
        .current_dir(workspace)
        .args(["build", "-p", "ratatosk-server", "--bin", "ratatosk"]);
    if !cfg!(debug_assertions) {
        command.arg("--release");
    }
    let status = command
        .status()
        .expect("build ratatosk server for IPC smoke test");
    assert!(
        status.success(),
        "cargo build -p ratatosk-server failed with {status}"
    );
}

fn socket_bind_is_restricted(stderr: &str) -> bool {
    let stderr = stderr.to_ascii_lowercase();
    (stderr.contains("operation not permitted") || stderr.contains("permission denied"))
        && (stderr.contains("bind") || stderr.contains("listen"))
}
