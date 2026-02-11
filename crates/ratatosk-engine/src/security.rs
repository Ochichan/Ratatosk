use bytes::Bytes;

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

#[cfg(test)]
mod tests {
    use super::{sanitize_acl_log_line, sanitize_error_message, sanitize_slowlog_argv};
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
}
