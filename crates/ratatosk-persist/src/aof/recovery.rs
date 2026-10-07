use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use bytes::BytesMut;
use ratatosk_engine::{
    command::{ClientState, ServerAccess, execute},
    keyspace::ServerState,
};
use ratatosk_resp::{RespParseError, frame::RespFrame, parse};

use crate::error::PersistError;

use super::writer::{AOF_V1_HEADER, AOF_VERSION_HEADER, TIMED_COMMAND_MARKER};

/// Decode a persistence-only timestamp envelope. Legacy ordinary RESP remains
/// valid; the marker is never dispatched as a client command.
pub(crate) fn decode_timed_command(
    frame: RespFrame,
) -> Result<(Option<i64>, RespFrame), PersistError> {
    let is_timed = matches!(&frame, RespFrame::Array(items)
        if matches!(items.first(), Some(RespFrame::BulkString(Some(value))) if value.as_ref() == TIMED_COMMAND_MARKER));
    if !is_timed {
        return Ok((None, frame));
    }
    let RespFrame::Array(mut items) = frame else {
        unreachable!()
    };
    if items.len() != 3 {
        return Err(PersistError::corrupt("invalid AOF timestamp envelope"));
    }
    let command = items.pop().expect("three items");
    match items.pop() {
        Some(RespFrame::Integer(timestamp))
            if timestamp >= 0 && matches!(command, RespFrame::Array(_)) =>
        {
            Ok((Some(timestamp), command))
        }
        _ => Err(PersistError::corrupt("invalid AOF timestamp or command")),
    }
}

/// Result of AOF replay with detailed status.
#[derive(Debug, Clone)]
pub struct ReplayResult {
    pub commands_replayed: usize,
    pub bytes_processed: usize,
    pub corruption_detected: bool,
    pub truncated_at: Option<usize>,
    pub version_mismatch: bool,
    pub legacy_format: bool,
}

impl ReplayResult {
    pub fn success(commands: usize, bytes: usize) -> Self {
        Self {
            commands_replayed: commands,
            bytes_processed: bytes,
            corruption_detected: false,
            truncated_at: None,
            version_mismatch: false,
            legacy_format: false,
        }
    }
}

/// AOF recovery: replays an AOF file into a `ServerState`.
///
/// Parses RESP commands from the file and executes them
/// against the engine's command dispatcher.
pub struct AofRecovery;

impl AofRecovery {
    /// Replay an AOF file into the given server state.
    ///
    /// Returns detailed result including corruption detection.
    pub fn replay_file(path: &Path, state: &mut ServerState) -> Result<ReplayResult, PersistError> {
        let file = File::open(path).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!("opening AOF file '{}': {e}", path.display()),
            )
        })?;
        Self::replay_reader(BufReader::new(file), state)
    }

    /// Replay from any reader with version header validation.
    pub fn replay_reader<R: Read>(
        mut reader: R,
        state: &mut ServerState,
    ) -> Result<ReplayResult, PersistError> {
        let mut client = ClientState::new(0);
        client.set_aof_replay(true);
        let mut commands_replayed = 0usize;
        let mut corruption_positions = Vec::new();

        let mut raw = Vec::new();
        reader
            .read_to_end(&mut raw)
            .map_err(|e| std::io::Error::new(e.kind(), format!("reading AOF data: {e}")))?;

        let total_bytes = raw.len();
        let has_header = raw.starts_with(AOF_VERSION_HEADER) || raw.starts_with(AOF_V1_HEADER);
        // Parse in place instead of copying the whole file a second time.
        let mut buf = BytesMut::from(bytes::Bytes::from(raw));

        // Check for version header
        let (version_mismatch, legacy_format) = if has_header {
            let _ = buf.split_to(AOF_VERSION_HEADER.len());
            (false, false)
        } else if buf.is_empty() {
            // Fresh AOF file created on first boot.
            (false, false)
        } else if buf.starts_with(b"*") {
            // Old format without header - still valid but warn
            tracing::warn!(
                target = "ratatosk::aof",
                "AOF file has no version header (legacy format)"
            );
            (false, true)
        } else {
            tracing::error!(
                target = "ratatosk::aof",
                "AOF file has unknown format (neither version header nor RESP)"
            );
            (true, false)
        };

        // Recover only a verified prefix. An incomplete transaction belongs
        // wholly to the discarded tail, including complete queued commands.
        let mut transaction_start: Option<(usize, usize)> = None;
        loop {
            let position = total_bytes - buf.len();
            // Every AOF record is a RESP array. Anything else is damage, and
            // must not reach the inline-command parser and be executed.
            let frame = match buf.first() {
                None | Some(b'*') => parse(&mut buf),
                Some(_) => Err(RespParseError::InvalidFrameType(buf[0])),
            };
            match frame {
                Ok(Some(frame)) => {
                    let (timestamp, frame) = decode_timed_command(frame)?;
                    let was_in_multi = client.in_multi();
                    let outcome = {
                        let mut access = ServerAccess::new_inline(state);
                        if let Some(timestamp) = timestamp {
                            ratatosk_core::time::with_command_time(timestamp, || {
                                execute(frame, &mut access, &mut client)
                            })
                        } else {
                            execute(frame, &mut access, &mut client)
                        }
                    };
                    if response_is_error(&outcome.response) {
                        return Err(PersistError::corrupt(format!(
                            "AOF replay command failed at byte {position}: {:?}",
                            outcome.response
                        )));
                    }
                    if !was_in_multi && client.in_multi() {
                        transaction_start = Some((position, commands_replayed));
                    } else if was_in_multi && !client.in_multi() {
                        transaction_start = None;
                    }
                    commands_replayed += 1;
                }
                Ok(None) => {
                    if let Some((start, committed_commands)) = transaction_start {
                        corruption_positions.push(start);
                        commands_replayed = committed_commands;
                    } else if !buf.is_empty() {
                        corruption_positions.push(position);
                    }
                    break;
                }
                Err(error) => {
                    // Only a torn tail may be cut back. Damage followed by
                    // more data would silently drop the acknowledged writes
                    // after it, so replay stops instead, as Redis does. Never
                    // resynchronize at a RESP-looking byte either: it is not
                    // evidence of a command boundary.
                    if !is_torn_tail(&buf) {
                        return Err(PersistError::corrupt(format!(
                            "AOF is damaged at byte {position} ({error}) and data follows the damage; \
                             refusing to drop it. Back up the file; truncating it to {position} bytes \
                             keeps only the records before the damage"
                        )));
                    }
                    let boundary = if let Some((start, committed_commands)) = transaction_start {
                        commands_replayed = committed_commands;
                        start
                    } else {
                        position
                    };
                    corruption_positions.push(boundary);
                    break;
                }
            }
        }
        let result = ReplayResult {
            commands_replayed,
            bytes_processed: corruption_positions.first().copied().unwrap_or(total_bytes),
            corruption_detected: !corruption_positions.is_empty(),
            truncated_at: corruption_positions.first().copied(),
            version_mismatch,
            legacy_format,
        };

        // Log summary
        if result.corruption_detected {
            tracing::warn!(
                target = "ratatosk::aof",
                commands_replayed,
                bytes_processed = result.bytes_processed,
                total_bytes,
                corruption_points = corruption_positions.len(),
                "AOF replay completed with corruption detected"
            );
        } else {
            tracing::info!(
                target = "ratatosk::aof",
                commands_replayed,
                bytes_processed = result.bytes_processed,
                "AOF replay completed successfully"
            );
        }

        Ok(result)
    }
}

/// Whether the unparseable rest of a file is what a crash leaves behind: one
/// incomplete record, possibly followed by the zero fill some filesystems
/// expose after losing unflushed data.
fn is_torn_tail(rest: &[u8]) -> bool {
    let data_len = rest
        .iter()
        .rposition(|&byte| byte != 0)
        .map_or(0, |last| last + 1);
    if data_len == rest.len() {
        // No zero fill: the parser rejected real bytes, not a cut-off record.
        return false;
    }
    if data_len == 0 {
        return true;
    }
    let mut record = BytesMut::from(&rest[..data_len]);
    rest[0] == b'*' && matches!(parse(&mut record), Ok(None))
}

fn response_is_error(frame: &RespFrame) -> bool {
    match frame {
        RespFrame::Error(_) => true,
        // EXEC can commit successful siblings while returning errors for
        // individual commands. Those are valid legacy transaction results,
        // not evidence that the transaction envelope failed to replay.
        RespFrame::Versioned { frame, .. } => response_is_error(frame),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aof::writer::{AofWriter, FsyncPolicy};
    use bytes::Bytes;

    fn encode_aof(commands: &[&[&str]]) -> Vec<u8> {
        let mut out = b"REDIS-AOF-001\n".to_vec();
        for command in commands {
            out.extend_from_slice(format!("*{}\r\n", command.len()).as_bytes());
            for part in *command {
                out.extend_from_slice(format!("${}\r\n{part}\r\n", part.len()).as_bytes());
            }
        }
        out
    }

    fn stream_ids(state: &ServerState, key: &str) -> Vec<(u64, u64)> {
        state
            .db(0)
            .get(key.as_bytes())
            .and_then(|value| value.as_stream_entries().map(|entries| entries.to_vec()))
            .unwrap_or_default()
            .into_iter()
            .map(|entry| (entry.id.ms, entry.id.seq))
            .collect()
    }

    /// Records an earlier version logged raw must still replay, or the server
    /// cannot start: a LIMIT without `~`, `~` trims, and a zero-padded ID
    /// longer than the 127 bytes a client may send.
    #[test]
    fn replay_accepts_raw_stream_records_from_earlier_versions() {
        let padded_id = format!("{}2-0", "0".repeat(130));
        let mut commands: Vec<Vec<String>> = Vec::new();
        for (key, count) in [
            ("a", 6),
            ("b", 6),
            ("c", 6),
            ("d", 6),
            ("e", 6),
            ("f", 6),
            ("g", 6),
        ] {
            for ms in 1..=count {
                commands.push(
                    ["XADD", key, &format!("{ms}-0"), "f", "v"]
                        .map(str::to_owned)
                        .to_vec(),
                );
            }
        }
        for raw in [
            &["XTRIM", "a", "MAXLEN", "3", "LIMIT", "2"][..],
            &["XTRIM", "b", "MAXLEN", "=", "3", "LIMIT", "2"],
            &["XTRIM", "c", "MINID", "4", "LIMIT", "0"],
            &["XTRIM", "d", "MAXLEN", "~", "3"],
            // Earlier versions parsed these as unsigned numbers of any size.
            &["XTRIM", "f", "MAXLEN", "0", "LIMIT", "18446744073709551615"],
            &["XTRIM", "g", "MAXLEN", "18446744073709551615"],
        ] {
            commands.push(raw.iter().map(|part| (*part).to_owned()).collect());
        }
        commands.push(
            ["XDEL", "e", padded_id.as_str()]
                .map(str::to_owned)
                .to_vec(),
        );

        let refs: Vec<Vec<&str>> = commands
            .iter()
            .map(|command| command.iter().map(String::as_str).collect())
            .collect();
        let slices: Vec<&[&str]> = refs.iter().map(Vec::as_slice).collect();
        let mut state = ServerState::with_default_dbs();
        let result = AofRecovery::replay_reader(encode_aof(&slices).as_slice(), &mut state)
            .expect("records an earlier version logged must replay");
        assert!(!result.corruption_detected);

        let ids = |ms: &[u64]| ms.iter().map(|ms| (*ms, 0)).collect::<Vec<_>>();
        // LIMIT 2 caps an exact trim: 6 entries with MAXLEN 3 leave 4.
        assert_eq!(stream_ids(&state, "a"), ids(&[3, 4, 5, 6]));
        assert_eq!(stream_ids(&state, "b"), ids(&[3, 4, 5, 6]));
        // LIMIT 0 removed nothing in those versions.
        assert_eq!(stream_ids(&state, "c"), ids(&[1, 2, 3, 4, 5, 6]));
        // `~` trimmed exactly, with no node rounding or default cap.
        assert_eq!(stream_ids(&state, "d"), ids(&[4, 5, 6]));
        // A LIMIT past i64::MAX caps nothing, and a MAXLEN past it keeps all.
        assert_eq!(stream_ids(&state, "f"), ids(&[]));
        assert_eq!(stream_ids(&state, "g"), ids(&[1, 2, 3, 4, 5, 6]));
        // The long padded ID is 2-0.
        assert_eq!(stream_ids(&state, "e"), ids(&[1, 3, 4, 5, 6]));
    }

    /// XCLAIM and XAUTOCLAIM records an earlier version logged as sent must
    /// replay with the semantics they were written with: options among the
    /// IDs, COUNT 0 or above LONG_MAX/16, a delivery added even with JUSTID,
    /// and no consumer created by a claim that found nothing.
    #[test]
    fn replay_runs_earlier_claim_records_as_they_were_written() {
        let commands: Vec<Vec<&str>> = vec![
            vec!["XADD", "s", "1-0", "f", "v"],
            vec!["XADD", "s", "2-0", "f", "v"],
            vec!["XADD", "s", "3-0", "f", "v"],
            vec!["XGROUP", "CREATE", "s", "g", "0"],
            vec!["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"],
            vec!["XCLAIM", "s", "g", "c2", "0", "JUSTID", "1-0"],
            vec!["XCLAIM", "s", "g", "ghost", "3600000", "2-0"],
            vec![
                "XAUTOCLAIM",
                "s",
                "g",
                "c3",
                "0",
                "0",
                "COUNT",
                "0",
                "JUSTID",
            ],
            vec![
                "XAUTOCLAIM",
                "s",
                "g",
                "c4",
                "0",
                "3-0",
                "COUNT",
                "576460752303423488",
            ],
        ];
        let slices: Vec<&[&str]> = commands.iter().map(Vec::as_slice).collect();
        let mut state = ServerState::with_default_dbs();
        let result = AofRecovery::replay_reader(encode_aof(&slices).as_slice(), &mut state)
            .expect("records an earlier version logged must replay");
        assert!(!result.corruption_detected);

        let db = state.db(0);
        let group = db
            .get(b"s".as_slice())
            .and_then(|value| value.as_stream_groups())
            .and_then(|groups| groups.get(b"g".as_slice()))
            .expect("group");
        let owner_and_count = |ms: u64| {
            let pending = group
                .pending
                .get(&ratatosk_engine::keyspace::StreamId { ms, seq: 0 })
                .expect("pending entry");
            (
                String::from_utf8_lossy(&pending.consumer).into_owned(),
                pending.deliveries,
            )
        };
        // 1-0: read, claimed with JUSTID (still a delivery), claimed again by
        // COUNT 0, which claims one entry.
        assert_eq!(owner_and_count(1), ("c3".to_owned(), 3));
        assert_eq!(owner_and_count(2), ("c".to_owned(), 1));
        assert_eq!(owner_and_count(3), ("c4".to_owned(), 2));
        let mut consumers = group
            .consumers
            .keys()
            .map(|name| String::from_utf8_lossy(name).into_owned())
            .collect::<Vec<_>>();
        consumers.sort();
        assert_eq!(consumers, ["c", "c2", "c3", "c4"]);
    }

    #[test]
    fn legacy_exec_preserves_successful_siblings_of_a_runtime_error() {
        let mut state = ServerState::with_default_dbs();
        let input = b"REDIS-AOF-001\n*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nx\r\n*2\r\n$4\r\nINCR\r\n$1\r\nk\r\n*3\r\n$3\r\nSET\r\n$5\r\nafter\r\n$1\r\ny\r\n*1\r\n$4\r\nEXEC\r\n";
        let result = AofRecovery::replay_reader(input.as_slice(), &mut state)
            .expect("valid EXEC with a per-command error");
        assert_eq!(result.commands_replayed, 5);
        assert!(!result.corruption_detected);
        assert_eq!(
            state
                .db(0)
                .get(b"k".as_slice())
                .expect("first sibling")
                .as_string_bytes(),
            Some(Bytes::from("x"))
        );
        assert_eq!(
            state
                .db(0)
                .get(b"after".as_slice())
                .expect("last sibling")
                .as_string_bytes(),
            Some(Bytes::from("y"))
        );
    }

    #[test]
    fn replay_uses_execution_time_for_expiry_dependencies_and_transactions() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("timed.aof");
        let argv = |parts: &[&str]| {
            parts
                .iter()
                .map(|part| Bytes::copy_from_slice(part.as_bytes()))
                .collect::<Vec<_>>()
        };
        {
            let mut writer = AofWriter::open(&path, FsyncPolicy::No).expect("writer");
            writer
                .append_command_at(0, &argv(&["SET", "expired", "1", "PX", "100"]), 1000)
                .expect("set");
            writer
                .append_command_at(0, &argv(&["INCR", "expired"]), 1010)
                .expect("incr");
            writer
                .append_command_at(0, &argv(&["HSET", "extended", "a", "1"]), 1000)
                .expect("hset");
            writer
                .append_command_at(0, &argv(&["PEXPIRE", "extended", "100"]), 1000)
                .expect("expiry");
            writer
                .append_command_at(0, &argv(&["HSET", "extended", "b", "2"]), 1010)
                .expect("hset");
            writer
                .append_command_at(0, &argv(&["PERSIST", "extended"]), 1020)
                .expect("persist");
            writer
                .append_command_at(0, &argv(&["SET", "reused", "99", "PX", "100"]), 1000)
                .expect("set");
            writer
                .append_command_at(0, &argv(&["INCR", "reused"]), 1200)
                .expect("fresh incr");
            writer
                .append_transaction_at(
                    &[(0, argv(&["MSETEX", "1", "tx-expiry", "v", "PX", "100"]))],
                    1200,
                )
                .expect("transaction");
        }
        let mut state = ServerState::with_default_dbs();
        AofRecovery::replay_file(&path, &mut state).expect("replay");
        assert_eq!(
            state
                .db(0)
                .get(b"expired".as_slice())
                .expect("historical value")
                .expire_at_ms(),
            Some(1100)
        );
        assert_eq!(
            state
                .db(0)
                .get(b"extended".as_slice())
                .expect("hash")
                .as_hash()
                .expect("fields")
                .len(),
            2
        );
        assert_eq!(
            state
                .db(0)
                .get(b"reused".as_slice())
                .expect("fresh counter")
                .as_string_bytes(),
            Some(Bytes::from("1"))
        );
        assert_eq!(
            state
                .db(0)
                .get(b"tx-expiry".as_slice())
                .expect("transaction value")
                .expire_at_ms(),
            Some(1300)
        );
        let mut client = ClientState::new(0);
        for key in ["expired", "tx-expiry"] {
            let mut access = ServerAccess::new_inline(&mut state);
            let outcome = execute(
                RespFrame::Array(
                    argv(&["GET", key])
                        .into_iter()
                        .map(|arg| RespFrame::BulkString(Some(arg)))
                        .collect(),
                ),
                &mut access,
                &mut client,
            );
            assert!(matches!(outcome.response, RespFrame::BulkString(None)));
        }
    }

    #[test]
    fn version_one_upgrade_keeps_old_commands_and_timed_appends() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("old.aof");
        std::fs::write(
            &path,
            b"REDIS-AOF-001\n*3\r\n$3\r\nSET\r\n$3\r\nold\r\n$1\r\nv\r\n",
        )
        .expect("old file");
        {
            let mut writer = AofWriter::open(&path, FsyncPolicy::No).expect("upgrade writer");
            writer
                .append_command_at(
                    0,
                    &[Bytes::from("SET"), Bytes::from("new"), Bytes::from("v")],
                    1000,
                )
                .expect("append");
        }
        let mut state = ServerState::with_default_dbs();
        let result = AofRecovery::replay_file(&path, &mut state).expect("mixed replay");
        assert!(!result.version_mismatch);
        assert_eq!(state.db(0).len(), 2);
        assert!(
            std::fs::read(&path)
                .expect("read")
                .starts_with(AOF_VERSION_HEADER)
        );
    }

    #[test]
    fn replay_restores_state() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("test.aof");

        // Write commands
        {
            let mut writer = AofWriter::open(&path, FsyncPolicy::No).expect("open");
            writer
                .append_command(
                    0,
                    &[
                        Bytes::from("SET"),
                        Bytes::from("hello"),
                        Bytes::from("world"),
                    ],
                )
                .expect("append SET");
            writer
                .append_command(
                    0,
                    &[Bytes::from("SET"), Bytes::from("count"), Bytes::from("42")],
                )
                .expect("append SET");
        }

        // Replay into fresh state
        let mut state = ServerState::with_default_dbs();
        let result = AofRecovery::replay_file(&path, &mut state).expect("replay");
        assert_eq!(result.commands_replayed, 3); // SELECT 0 and two SETs
        assert!(!result.corruption_detected);

        // Verify data
        let db = state.db(0);
        let hello = db.get(&Bytes::from("hello"));
        assert!(hello.is_some());
        assert_eq!(
            hello.and_then(|v| v.as_string()),
            Some(&Bytes::from("world"))
        );

        let count = db.get(&Bytes::from("count"));
        assert!(count.is_some());
        assert_eq!(
            count.and_then(|v| v.as_string_bytes()),
            Some(Bytes::from("42"))
        );
    }

    #[test]
    fn replay_with_db_select() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("test.aof");

        {
            let mut writer = AofWriter::open(&path, FsyncPolicy::No).expect("open");
            writer
                .append_command(0, &[Bytes::from("SET"), Bytes::from("a"), Bytes::from("0")])
                .expect("append");
            writer
                .append_command(1, &[Bytes::from("SET"), Bytes::from("b"), Bytes::from("1")])
                .expect("append");
        }

        let mut state = ServerState::with_default_dbs();
        let result = AofRecovery::replay_file(&path, &mut state).expect("replay");
        // SELECT 0, SET a 0, SELECT 1, SET b 1.
        assert_eq!(result.commands_replayed, 4);

        assert!(state.db(0).get(&Bytes::from("a")).is_some());
        assert!(state.db(1).get(&Bytes::from("b")).is_some());
    }

    #[test]
    fn replay_empty_file_returns_zero() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("empty.aof");
        std::fs::write(&path, b"").expect("create");

        let mut state = ServerState::with_default_dbs();
        let result = AofRecovery::replay_file(&path, &mut state).expect("replay");
        assert_eq!(result.commands_replayed, 0);
        assert!(!result.version_mismatch);
        assert!(!result.legacy_format);
    }

    #[test]
    fn replay_missing_file_has_context_in_error() {
        let path = std::path::Path::new("/nonexistent/dir/missing.aof");
        let mut state = ServerState::with_default_dbs();
        let result = AofRecovery::replay_file(path, &mut state);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("opening AOF file"),
            "error should contain context about opening AOF file, got: {err_msg}"
        );
        assert!(
            err_msg.contains("missing.aof"),
            "error should contain the file path, got: {err_msg}"
        );
    }

    #[test]
    fn replay_truncated_file_is_graceful() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("truncated.aof");

        // Write a valid command followed by truncated data
        {
            let mut writer = AofWriter::open(&path, FsyncPolicy::No).expect("open");
            writer
                .append_command(
                    0,
                    &[Bytes::from("SET"), Bytes::from("ok"), Bytes::from("1")],
                )
                .expect("append");
        }

        // Append garbage
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open for append");
        file.write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nba")
            .expect("write truncated");

        let mut state = ServerState::with_default_dbs();
        let result = AofRecovery::replay_file(&path, &mut state).expect("replay");
        // Should replay at least the first command
        assert!(result.commands_replayed >= 1);
        assert!(result.corruption_detected); // Should detect corruption
        assert!(state.db(0).get(&Bytes::from("ok")).is_some());
    }

    #[test]
    fn replay_does_not_apply_an_uncommitted_transaction_tail() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("transaction-tail.aof");
        // Both frames are complete RESP commands, but the transaction has no
        // EXEC.  A crash at this point must not materialize its SET on replay.
        std::fs::write(
            &path,
            b"REDIS-AOF-001\n*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$9\r\ncommitted\r\n$3\r\nyes\r\n",
        )
        .expect("write tail");

        let mut state = ServerState::with_default_dbs();
        let result = AofRecovery::replay_file(&path, &mut state).expect("replay tail");
        assert_eq!(result.commands_replayed, 0);
        assert_eq!(result.truncated_at, Some(AOF_VERSION_HEADER.len()));
        assert!(state.db(0).get(&Bytes::from("committed")).is_none());
    }

    #[test]
    fn replay_does_not_skip_corruption_to_exec_inside_transaction() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("corrupt-transaction.aof");
        std::fs::write(
            &path,
            b"REDIS-AOF-001\n*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$7\r\nprefix\r\n$3\r\nyes\r\nnot-resp\n*1\r\n$4\r\nEXEC\r\n",
        )
        .expect("write corrupt transaction");

        let mut state = ServerState::with_default_dbs();
        let error = AofRecovery::replay_file(&path, &mut state)
            .expect_err("damage followed by more records must stop replay");
        assert!(error.to_string().contains("damaged at byte"), "{error}");
        assert!(state.db(0).get(&Bytes::from("prefix")).is_none());
    }

    #[test]
    fn replay_refuses_damage_in_the_middle_of_the_file() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("damaged.aof");
        {
            let mut writer = AofWriter::open(&path, FsyncPolicy::No).expect("open");
            for key in ["a", "b", "c"] {
                writer
                    .append_command(0, &[Bytes::from("SET"), Bytes::from(key), Bytes::from("1")])
                    .expect("append");
            }
        }
        let raw = std::fs::read(&path).expect("read aof");
        let records: Vec<usize> = raw
            .windows(8)
            .enumerate()
            .filter(|(_, window)| window == b"*3\r\n$15\r")
            .map(|(offset, _)| offset)
            .collect();
        assert_eq!(records.len(), 3, "one timestamp envelope per command");

        // A flipped type byte and a bad length both sit before intact records.
        for (offset, byte) in [(records[1], b'j'), (records[1] + 1, b'x')] {
            let mut damaged = raw.clone();
            damaged[offset] = byte;
            std::fs::write(&path, &damaged).expect("write damaged aof");
            let mut state = ServerState::with_default_dbs();
            let error = AofRecovery::replay_file(&path, &mut state)
                .expect_err("mid-file damage must stop replay");
            assert!(
                error
                    .to_string()
                    .contains(&format!("damaged at byte {}", records[1])),
                "{error}"
            );
        }

        // Zero fill after a torn or a complete last record is a crash tail.
        for kept in [records[2] + 4, records[2]] {
            let mut torn = raw[..kept].to_vec();
            torn.extend_from_slice(&[0; 64]);
            std::fs::write(&path, &torn).expect("write torn aof");
            let mut state = ServerState::with_default_dbs();
            let result = AofRecovery::replay_file(&path, &mut state).expect("replay torn tail");
            assert_eq!(result.truncated_at, Some(records[2]));
            assert!(state.db(0).get(&Bytes::from("b")).is_some());
            assert!(state.db(0).get(&Bytes::from("c")).is_none());
        }
    }
}
