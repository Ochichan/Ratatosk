use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::{
    env,
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Write},
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
};

const TOKEN_PREFIXES: [&[u8]; 5] = [b"ghp_", b"sk-", b"npm_", b"xox", b"AKIA"];
const REDACTED: &str = "[REDACTED]";
const REDACTED_BYTES: &[u8] = b"[REDACTED]";

pub(crate) fn sanitize_error_message(message: &str) -> String {
    let mut sanitized = replace_home(message);
    sanitized = redact_sensitive_text(&sanitized);
    truncate_chars(&sanitized, 256)
}

pub(crate) fn sanitize_acl_log_line(line: &str) -> String {
    let sanitized = redact_sensitive_text(line);
    truncate_chars(&sanitized, 256)
}

pub(crate) fn sanitize_slowlog_argv(argv: &[Bytes]) -> Vec<Bytes> {
    if argv.is_empty() {
        return Vec::new();
    }

    let mut out = argv.to_vec();
    let command = uppercase_ascii(&argv[0]);

    if command == b"AUTH" {
        if out.len() == 2 {
            out[1] = Bytes::from_static(REDACTED_BYTES);
        } else if out.len() >= 3 {
            out[2] = Bytes::from_static(REDACTED_BYTES);
        }
    }

    if command == b"HELLO" {
        let mut idx = 0usize;
        if let Some(first) = argv.get(1) {
            if parse_i64_ascii(first).is_some() {
                idx = 1;
            }
        }
        while idx + 1 < argv.len() {
            let token = uppercase_ascii(&argv[idx + 1]);
            if token == b"AUTH" && idx + 3 < argv.len() {
                out[idx + 3] = Bytes::from_static(REDACTED_BYTES);
                idx += 3;
            } else if token == b"SETNAME" {
                idx += 2;
            } else {
                idx += 1;
            }
        }
    }

    if command == b"ACL" && out.len() >= 3 && argv[1].eq_ignore_ascii_case(b"SETUSER") {
        for item in out.iter_mut().skip(3) {
            if let Some(first) = item.first() {
                if *first == b'>' {
                    *item = Bytes::from_static(b">[REDACTED]");
                } else if *first == b'<' {
                    *item = Bytes::from_static(b"<[REDACTED]");
                }
            }
        }
    }

    for (idx, arg) in out.iter_mut().enumerate() {
        if idx == 0 {
            continue;
        }
        if should_redact_raw_token(arg) {
            *arg = Bytes::from_static(REDACTED_BYTES);
        }
    }

    out
}

pub(crate) fn should_reject_shell_metacharacters(raw: &[u8]) -> bool {
    const SHELL_BLOCKLIST: &[u8] = b"|&;<>$`\n\r(){}[]*?!#~'\"";
    raw.iter().any(|b| SHELL_BLOCKLIST.contains(b))
}

fn redact_sensitive_text(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut i = 0usize;

    while i < bytes.len() {
        if let Some(end) = prefixed_token_end(bytes, i) {
            out.push_str(REDACTED);
            i = end;
            continue;
        }

        if bytes[i].is_ascii_alphanumeric() {
            let start = i;
            while i < bytes.len() && bytes[i].is_ascii_alphanumeric() {
                i += 1;
            }
            if i - start >= 20 {
                out.push_str(REDACTED);
            } else {
                out.push_str(&input[start..i]);
            }
            continue;
        }

        let ch = input[i..].chars().next().unwrap_or('\u{FFFD}');
        out.push(ch);
        i += ch.len_utf8();
    }

    out
}

fn replace_home(input: &str) -> String {
    let Ok(home) = std::env::var("HOME") else {
        return input.to_string();
    };
    if home.is_empty() {
        return input.to_string();
    }
    input.replace(&home, "$HOME")
}

fn prefixed_token_end(bytes: &[u8], start: usize) -> Option<usize> {
    let rest = &bytes[start..];
    for prefix in TOKEN_PREFIXES {
        if rest.starts_with(prefix) {
            let mut idx = start + prefix.len();
            while idx < bytes.len() && is_token_char(bytes[idx]) {
                idx += 1;
            }
            return Some(idx);
        }
    }
    None
}

fn is_token_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-'
}

fn should_redact_raw_token(raw: &[u8]) -> bool {
    if raw.len() >= 20 && raw.iter().all(|b| b.is_ascii_alphanumeric()) {
        return true;
    }

    for prefix in TOKEN_PREFIXES {
        if raw.windows(prefix.len()).any(|window| window == prefix) {
            return true;
        }
    }

    false
}
fn uppercase_ascii(raw: &Bytes) -> Vec<u8> {
    raw.iter().map(|byte| byte.to_ascii_uppercase()).collect()
}

fn truncate_chars(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input.to_string();
    }

    let mut out = String::with_capacity(max_chars);
    for ch in input.chars().take(max_chars) {
        out.push(ch);
    }
    out
}

fn parse_i64_ascii(raw: &Bytes) -> Option<i64> {
    std::str::from_utf8(raw).ok()?.parse::<i64>().ok()
}

#[derive(Debug, Clone)]
pub(crate) struct AuditStamp {
    pub seq: u64,
    pub prev_hash: String,
    pub hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AuditHealthSnapshot {
    pub dirty: bool,
    pub recovery_status: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AuditChainState {
    seq: u64,
    last_hash: [u8; 32],
    dirty: bool,
    recovery_status: &'static str,
}

static AUDIT_CHAIN_STATE: OnceLock<Mutex<AuditChainState>> = OnceLock::new();

fn audit_state_path() -> PathBuf {
    env::var("RATATOSK_AUDIT_CHAIN_STATE")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/tmp/ratatosk-audit-chain.state"))
}

fn audit_log_path() -> PathBuf {
    env::var("RATATOSK_AUDIT_LOG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/tmp/ratatosk-audit.log"))
}

pub fn audit_paths_from_env() -> (PathBuf, PathBuf) {
    (audit_log_path(), audit_state_path())
}

fn parse_env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn parse_env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn rotated_audit_log_path(path: &Path, suffix: usize) -> PathBuf {
    PathBuf::from(format!("{}.{}", path.display(), suffix))
}

fn rotate_audit_log_if_needed(path: &PathBuf) {
    let max_bytes = parse_env_u64("RATATOSK_AUDIT_LOG_MAX_BYTES", 32 * 1024 * 1024);
    let max_files = parse_env_usize("RATATOSK_AUDIT_LOG_MAX_FILES", 4);

    if max_files == 0 {
        return;
    }

    let Ok(metadata) = fs::metadata(path) else {
        return;
    };

    if metadata.len() < max_bytes {
        return;
    }

    for idx in (1..=max_files).rev() {
        let source = if idx == 1 {
            path.clone()
        } else {
            rotated_audit_log_path(path, idx - 1)
        };
        if !source.exists() {
            continue;
        }

        let destination = rotated_audit_log_path(path, idx);
        if destination.exists() {
            let _ = fs::remove_file(&destination);
        }
        let _ = fs::rename(source, destination);
    }
}

fn parse_hash_hex(raw: &str) -> Option<[u8; 32]> {
    if raw.len() != 64 {
        return None;
    }

    let mut out = [0u8; 32];
    for (idx, chunk) in raw.as_bytes().chunks(2).enumerate() {
        let text = std::str::from_utf8(chunk).ok()?;
        let value = u8::from_str_radix(text, 16).ok()?;
        out[idx] = value;
    }
    Some(out)
}

fn empty_audit_chain_state() -> AuditChainState {
    AuditChainState {
        seq: 0,
        last_hash: [0u8; 32],
        dirty: false,
        recovery_status: "none",
    }
}

fn parse_audit_chain_state(contents: &str) -> AuditChainState {
    let mut seq = 0u64;
    let mut hash = [0u8; 32];
    for line in contents.lines() {
        if let Some(raw_seq) = line.strip_prefix("seq=") {
            if let Ok(parsed) = raw_seq.parse::<u64>() {
                seq = parsed;
            }
            continue;
        }

        if let Some(raw_hash) = line.strip_prefix("last_hash=") {
            if let Some(parsed) = parse_hash_hex(raw_hash.trim()) {
                hash = parsed;
            }
        }
    }

    AuditChainState {
        seq,
        last_hash: hash,
        dirty: false,
        recovery_status: "none",
    }
}

fn load_audit_chain_state_file(path: &Path) -> AuditChainState {
    let Ok(contents) = fs::read_to_string(path) else {
        return empty_audit_chain_state();
    };
    parse_audit_chain_state(&contents)
}

fn parse_audit_log_line(line: &str) -> Option<AuditChainState> {
    let mut seq = None;
    let mut hash = None;

    for field in line.split('\t') {
        if let Some(raw_seq) = field.strip_prefix("seq=") {
            seq = raw_seq.parse::<u64>().ok();
            continue;
        }
        if let Some(raw_hash) = field.strip_prefix("hash=") {
            hash = parse_hash_hex(raw_hash.trim());
        }
    }

    Some(AuditChainState {
        seq: seq?,
        last_hash: hash?,
        dirty: false,
        recovery_status: "none",
    })
}

fn load_latest_audit_log_state(path: &Path) -> Option<AuditChainState> {
    let file = File::open(path).ok()?;
    let reader = BufReader::new(file);
    let mut last = None;
    for line in reader.lines() {
        let Ok(line) = line else {
            continue;
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some(parsed) = parse_audit_log_line(trimmed) {
            last = Some(parsed);
        }
    }
    last
}

fn latest_audit_log_state_with_rotations(path: &Path) -> Option<AuditChainState> {
    if let Some(state) = load_latest_audit_log_state(path) {
        return Some(state);
    }

    let max_files = parse_env_usize("RATATOSK_AUDIT_LOG_MAX_FILES", 4);
    for idx in 1..=max_files {
        let rotated_path = rotated_audit_log_path(path, idx);
        if let Some(state) = load_latest_audit_log_state(&rotated_path) {
            return Some(state);
        }
    }

    None
}

fn record_audit_chain_dirty(dirty: bool) {
    metrics::gauge!("ratatosk_audit_chain_dirty").set(if dirty { 1.0 } else { 0.0 });
}

fn record_audit_state_recovery(reason: &'static str) {
    metrics::counter!("ratatosk_audit_state_recovered_from_log_total", "reason" => reason.to_string())
        .increment(1);
}

fn sync_parent_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        let parent = path.parent().unwrap_or(Path::new("."));
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn persist_audit_chain_state_to_path(path: &Path, state: &AuditChainState) -> io::Result<()> {
    let tmp_path = path.with_extension("state.tmp");

    let parent = path
        .parent()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    fs::create_dir_all(&parent)?;

    let payload = format!(
        "seq={}\nlast_hash={}\n",
        state.seq,
        bytes_to_hex(&state.last_hash)
    );
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp_path)?;
    file.write_all(payload.as_bytes())?;
    file.flush()?;
    file.sync_all()?;

    fs::rename(&tmp_path, path)?;
    sync_parent_directory(path)?;
    Ok(())
}

fn persist_audit_chain_state(path: &Path, state: &AuditChainState) -> io::Result<()> {
    if let Err(error) = persist_audit_chain_state_to_path(path, state) {
        metrics::counter!("ratatosk_audit_state_persist_failures_total").increment(1);
        tracing::warn!(
            target = "ratatosk::audit",
            path = %path.display(),
            error = %error,
            "failed to durably persist audit chain state"
        );
        return Err(error);
    }
    Ok(())
}

fn append_audit_event_log_to_path(
    path: &Path,
    stamp: &AuditStamp,
    event: &str,
    payload: &str,
) -> io::Result<()> {
    let parent = path
        .parent()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    fs::create_dir_all(&parent)?;

    rotate_audit_log_if_needed(&path.to_path_buf());

    let mut file = OpenOptions::new().create(true).append(true).open(path)?;

    let safe_payload = sanitize_acl_log_line(payload).replace(['\n', '\r', '\t'], " ");

    writeln!(
        file,
        "seq={}\tevent={}\tprev_hash={}\thash={}\tpayload={}",
        stamp.seq, event, stamp.prev_hash, stamp.hash, safe_payload
    )?;
    file.flush()?;
    file.sync_all()?;
    Ok(())
}

fn append_audit_event_log(
    path: &Path,
    stamp: &AuditStamp,
    event: &str,
    payload: &str,
) -> io::Result<()> {
    if let Err(error) = append_audit_event_log_to_path(path, stamp, event, payload) {
        metrics::counter!("ratatosk_audit_log_write_failures_total").increment(1);
        tracing::warn!(
            target = "ratatosk::audit",
            path = %path.display(),
            error = %error,
            "failed to durably append audit event"
        );
        return Err(error);
    }
    Ok(())
}

fn resolve_audit_chain_state(
    state_file_state: AuditChainState,
    log_state: Option<AuditChainState>,
) -> (AuditChainState, Option<&'static str>) {
    let Some(log_state) = log_state else {
        return (state_file_state, None);
    };

    if log_state.seq > state_file_state.seq {
        tracing::warn!(
            target = "ratatosk::audit",
            state_seq = state_file_state.seq,
            log_seq = log_state.seq,
            "audit log is ahead of audit chain state; recovering checkpoint from durable log"
        );
        return (log_state, Some("log_ahead"));
    }

    if log_state.seq == state_file_state.seq
        && log_state.seq > 0
        && log_state.last_hash != state_file_state.last_hash
    {
        tracing::warn!(
            target = "ratatosk::audit",
            seq = log_state.seq,
            "audit chain state hash mismatched last durable log entry; recovering checkpoint from log"
        );
        return (log_state, Some("hash_mismatch"));
    }

    (state_file_state, None)
}

fn load_audit_chain_state_from_paths(log_path: &Path, state_path: &Path) -> AuditChainState {
    let disk_state = load_audit_chain_state_file(state_path);
    let (mut resolved, recovery_reason) =
        resolve_audit_chain_state(disk_state, latest_audit_log_state_with_rotations(log_path));
    resolved.recovery_status = recovery_reason.unwrap_or("none");
    if let Some(reason) = recovery_reason {
        record_audit_state_recovery(reason);
    }

    if recovery_reason.is_some() {
        if let Err(error) = persist_audit_chain_state(state_path, &resolved) {
            resolved.dirty = true;
            tracing::warn!(
                target = "ratatosk::audit",
                path = %state_path.display(),
                error = %error,
                "audit chain checkpoint recovery could not be persisted; next startup will recover from log again"
            );
        }
    }

    resolved
}

pub(crate) fn audit_health_snapshot() -> AuditHealthSnapshot {
    let state = audit_chain_state()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    AuditHealthSnapshot {
        dirty: state.dirty,
        recovery_status: state.recovery_status.to_string(),
    }
}

fn audit_chain_state() -> &'static Mutex<AuditChainState> {
    AUDIT_CHAIN_STATE.get_or_init(|| {
        let state = load_audit_chain_state_from_paths(&audit_log_path(), &audit_state_path());
        record_audit_chain_dirty(state.dirty);
        Mutex::new(state)
    })
}

fn build_next_audit_state(
    state: &AuditChainState,
    event: &str,
    payload: &str,
) -> (AuditStamp, AuditChainState) {
    let prev_hash = state.last_hash;

    let mut hasher = Sha256::new();
    hasher.update(prev_hash);
    hasher.update(b"|");
    hasher.update(event.as_bytes());
    hasher.update(b"|");
    hasher.update(payload.as_bytes());

    let digest = hasher.finalize();
    let mut next_hash = [0u8; 32];
    next_hash.copy_from_slice(&digest);

    let next_state = AuditChainState {
        seq: state.seq.saturating_add(1),
        last_hash: next_hash,
        dirty: false,
        recovery_status: state.recovery_status,
    };

    let stamp = AuditStamp {
        seq: next_state.seq,
        prev_hash: bytes_to_hex(&prev_hash),
        hash: bytes_to_hex(&next_state.last_hash),
    };

    (stamp, next_state)
}

fn advance_audit_chain(
    state: &mut AuditChainState,
    log_path: &Path,
    state_path: &Path,
    event: &str,
    payload: &str,
) -> AuditStamp {
    let (stamp, next_state) = build_next_audit_state(state, event, payload);

    if let Err(error) = append_audit_event_log(log_path, &stamp, event, payload) {
        tracing::warn!(
            target = "ratatosk::audit",
            seq = stamp.seq,
            error = %error,
            "audit chain left unchanged because the durable log append failed"
        );
        record_audit_chain_dirty(state.dirty);
        return stamp;
    }

    match persist_audit_chain_state(state_path, &next_state) {
        Ok(()) => {
            let was_dirty = state.dirty;
            *state = next_state;
            if was_dirty {
                tracing::info!(
                    target = "ratatosk::audit",
                    seq = state.seq,
                    "audit chain checkpoint caught up with durable log"
                );
            }
            record_audit_chain_dirty(false);
        }
        Err(error) => {
            let mut recovered_state = next_state;
            recovered_state.dirty = true;
            *state = recovered_state;
            tracing::warn!(
                target = "ratatosk::audit",
                seq = state.seq,
                path = %state_path.display(),
                error = %error,
                "audit log append succeeded but checkpoint persistence failed; restart will recover chain from log"
            );
            record_audit_chain_dirty(true);
        }
    }

    stamp
}

pub(crate) fn next_audit_stamp(event: &str, payload: &str) -> AuditStamp {
    let mut state = audit_chain_state()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    advance_audit_chain(
        &mut state,
        &audit_log_path(),
        &audit_state_path(),
        event,
        payload,
    )
}
fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{
        advance_audit_chain, bytes_to_hex, empty_audit_chain_state,
        latest_audit_log_state_with_rotations, load_audit_chain_state_file,
        load_audit_chain_state_from_paths, persist_audit_chain_state_to_path,
        sanitize_acl_log_line, sanitize_error_message, sanitize_slowlog_argv,
    };
    use bytes::Bytes;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn redacts_token_prefixes_and_long_sequences() {
        let text =
            "bad sk-abcdef1234567890ABCDEF and AKIA1234567890XYZABCDE and ABCDEFGHIJKLMNOPQRST";
        let sanitized = sanitize_error_message(text);
        assert!(!sanitized.contains("sk-abcdef"));
        assert!(!sanitized.contains("AKIA1234"));
        assert!(!sanitized.contains("ABCDEFGHIJKLMNOPQRST"));
    }

    #[test]
    fn redacts_acl_setuser_secret_rules_in_slowlog() {
        let argv = vec![
            Bytes::from_static(b"ACL"),
            Bytes::from_static(b"SETUSER"),
            Bytes::from_static(b"alice"),
            Bytes::from_static(b">super-secret"),
        ];
        let redacted = sanitize_slowlog_argv(&argv);
        assert_eq!(redacted[3], Bytes::from_static(b">[REDACTED]"));
    }

    #[test]
    fn acl_log_line_is_limited() {
        let long = "A".repeat(512);
        let out = sanitize_acl_log_line(&long);
        assert!(out.chars().count() <= 256);
        assert!(!out.is_empty());
    }

    #[test]
    fn audit_stamp_chain_advances() {
        let dir = tempdir().expect("create temp dir");
        let log_path = dir.path().join("audit.log");
        let state_path = dir.path().join("audit.state");
        let mut state = empty_audit_chain_state();
        let first = advance_audit_chain(&mut state, &log_path, &state_path, "TEST", "payload=one");
        let second = advance_audit_chain(&mut state, &log_path, &state_path, "TEST", "payload=two");

        assert!(second.seq > first.seq);
        assert_eq!(second.prev_hash, first.hash);
        assert_ne!(second.hash, first.hash);
    }

    #[test]
    fn audit_chain_log_append_failure_leaves_state_unchanged() {
        let dir = tempdir().expect("create temp dir");
        let log_path = dir.path().to_path_buf();
        let state_path = dir.path().join("audit.state");
        let mut state = empty_audit_chain_state();

        let stamp = advance_audit_chain(&mut state, &log_path, &state_path, "TEST", "payload=one");

        assert_eq!(stamp.seq, 1);
        assert_eq!(state, empty_audit_chain_state());
        assert!(!state_path.exists());
    }

    #[test]
    fn audit_chain_checkpoint_failure_marks_state_dirty_after_log_commit() {
        let dir = tempdir().expect("create temp dir");
        let log_path = dir.path().join("audit.log");
        let state_path = dir.path().to_path_buf();
        let mut state = empty_audit_chain_state();

        let stamp = advance_audit_chain(&mut state, &log_path, &state_path, "TEST", "payload=one");

        assert_eq!(stamp.seq, 1);
        assert_eq!(state.seq, 1);
        assert!(state.dirty);
        let log = std::fs::read_to_string(&log_path).expect("read audit log");
        assert!(log.contains("seq=1"));
    }

    #[test]
    fn audit_chain_recovery_prefers_log_when_state_file_is_stale() {
        let dir = tempdir().expect("create temp dir");
        let log_path = dir.path().join("audit.log");
        let state_path = dir.path().join("audit.state");
        let mut state = empty_audit_chain_state();

        let first = advance_audit_chain(&mut state, &log_path, &state_path, "TEST", "payload=one");
        let second = advance_audit_chain(&mut state, &log_path, &state_path, "TEST", "payload=two");

        persist_audit_chain_state_to_path(
            &state_path,
            &super::AuditChainState {
                seq: first.seq,
                last_hash: super::parse_hash_hex(&first.hash).expect("parse first hash"),
                dirty: false,
                recovery_status: "none",
            },
        )
        .expect("write stale state");

        let recovered = load_audit_chain_state_from_paths(&log_path, &state_path);
        let persisted = load_audit_chain_state_file(&state_path);

        assert_eq!(recovered.seq, second.seq);
        assert_eq!(bytes_to_hex(&recovered.last_hash), second.hash);
        assert!(!recovered.dirty);
        assert_eq!(persisted.seq, second.seq);
        assert_eq!(recovered.recovery_status, "log_ahead");
    }

    #[test]
    fn audit_chain_recovery_follows_rotated_log_tail() {
        let dir = tempdir().expect("create temp dir");
        let log_path = dir.path().join("audit.log");
        let rotated = super::rotated_audit_log_path(&log_path, 1);
        let state_path = dir.path().join("audit.state");
        let mut state = empty_audit_chain_state();

        let second = advance_audit_chain(&mut state, &rotated, &state_path, "TEST", "payload=two");
        fs::write(&log_path, "").expect("write empty current log");
        persist_audit_chain_state_to_path(
            &state_path,
            &super::AuditChainState {
                seq: 0,
                last_hash: [0u8; 32],
                dirty: false,
                recovery_status: "none",
            },
        )
        .expect("write stale checkpoint");

        let latest =
            latest_audit_log_state_with_rotations(&log_path).expect("find rotated log tail");
        let recovered = load_audit_chain_state_from_paths(&log_path, &state_path);

        assert_eq!(latest.seq, second.seq);
        assert_eq!(recovered.seq, second.seq);
        assert_eq!(recovered.recovery_status, "log_ahead");
    }
}
