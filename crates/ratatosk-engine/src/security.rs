use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::{
    env,
    fs::{self, OpenOptions},
    io::Write,
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

#[derive(Debug, Clone, Copy)]
struct AuditChainState {
    seq: u64,
    last_hash: [u8; 32],
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

fn load_audit_chain_state() -> AuditChainState {
    let path = audit_state_path();
    let Ok(contents) = fs::read_to_string(&path) else {
        return AuditChainState {
            seq: 0,
            last_hash: [0u8; 32],
        };
    };

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
    }
}

fn persist_audit_chain_state(state: &AuditChainState) {
    let path = audit_state_path();
    let tmp_path = path.with_extension("state.tmp");

    let parent = path
        .parent()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    let _ = fs::create_dir_all(parent);

    let payload = format!(
        "seq={}\nlast_hash={}\n",
        state.seq,
        bytes_to_hex(&state.last_hash)
    );
    if let Err(error) = fs::write(&tmp_path, payload) {
        metrics::counter!("ratatosk_audit_state_persist_failures_total").increment(1);
        tracing::warn!(
            target = "ratatosk::audit",
            path = %tmp_path.display(),
            error = %error,
            "failed to write audit chain state temp file"
        );
        return;
    }

    if let Err(error) = fs::rename(&tmp_path, &path) {
        metrics::counter!("ratatosk_audit_state_persist_failures_total").increment(1);
        tracing::warn!(
            target = "ratatosk::audit",
            from = %tmp_path.display(),
            to = %path.display(),
            error = %error,
            "failed to atomically persist audit chain state"
        );
    }
}

fn append_audit_event_log(stamp: &AuditStamp, event: &str, payload: &str) {
    let path = audit_log_path();
    let parent = path
        .parent()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    let _ = fs::create_dir_all(parent);

    rotate_audit_log_if_needed(&path);

    let mut file = match OpenOptions::new().create(true).append(true).open(&path) {
        Ok(file) => file,
        Err(error) => {
            metrics::counter!("ratatosk_audit_log_write_failures_total").increment(1);
            tracing::warn!(
                target = "ratatosk::audit",
                path = %path.display(),
                error = %error,
                "failed to open audit log file"
            );
            return;
        }
    };

    let safe_payload = sanitize_acl_log_line(payload)
        .replace(['\n', '\r'], " ");

    if let Err(error) = writeln!(
        file,
        "seq={}\tevent={}\tprev_hash={}\thash={}\tpayload={}",
        stamp.seq, event, stamp.prev_hash, stamp.hash, safe_payload
    ) {
        metrics::counter!("ratatosk_audit_log_write_failures_total").increment(1);
        tracing::warn!(
            target = "ratatosk::audit",
            path = %path.display(),
            error = %error,
            "failed to append audit event"
        );
    }
}

fn audit_chain_state() -> &'static Mutex<AuditChainState> {
    AUDIT_CHAIN_STATE.get_or_init(|| Mutex::new(load_audit_chain_state()))
}

pub(crate) fn next_audit_stamp(event: &str, payload: &str) -> AuditStamp {
    let mut state = audit_chain_state()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

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

    state.seq = state.seq.saturating_add(1);
    state.last_hash = next_hash;

    let stamp = AuditStamp {
        seq: state.seq,
        prev_hash: bytes_to_hex(&prev_hash),
        hash: bytes_to_hex(&state.last_hash),
    };

    append_audit_event_log(&stamp, event, payload);
    persist_audit_chain_state(&state);

    stamp
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
        next_audit_stamp, sanitize_acl_log_line, sanitize_error_message, sanitize_slowlog_argv,
    };
    use bytes::Bytes;

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
        let first = next_audit_stamp("TEST", "payload=one");
        let second = next_audit_stamp("TEST", "payload=two");

        assert!(second.seq > first.seq);
        assert_eq!(second.prev_hash, first.hash);
        assert_ne!(second.hash, first.hash);
    }
}
