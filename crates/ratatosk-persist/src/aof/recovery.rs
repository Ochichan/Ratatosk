use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use bytes::BytesMut;
use ratatosk_engine::{
    command::{ClientState, ServerAccess, execute},
    keyspace::ServerState,
};
use ratatosk_resp::{frame::RespFrame, parse};

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
        let mut buf = BytesMut::with_capacity(64 * 1024);
        let mut client = ClientState::new(0);
        let mut commands_replayed = 0usize;
        let mut corruption_positions = Vec::new();

        // Read all data into buffer
        let mut raw = Vec::new();
        reader
            .read_to_end(&mut raw)
            .map_err(|e| std::io::Error::new(e.kind(), format!("reading AOF data: {e}")))?;

        let total_bytes = raw.len();

        // Check for version header
        let (version_mismatch, legacy_format) =
            if raw.starts_with(AOF_VERSION_HEADER) || raw.starts_with(AOF_V1_HEADER) {
                // Skip header for parsing
                buf.extend_from_slice(&raw[AOF_VERSION_HEADER.len()..]);
                (false, false)
            } else if raw.is_empty() {
                // Fresh AOF file created on first boot.
                (false, false)
            } else if raw.starts_with(b"*") {
                // Old format without header - still valid but warn
                tracing::warn!(
                    target = "ratatosk::aof",
                    "AOF file has no version header (legacy format)"
                );
                buf.extend_from_slice(&raw);
                (false, true)
            } else {
                // Unknown format
                tracing::error!(
                    target = "ratatosk::aof",
                    "AOF file has unknown format (neither version header nor RESP)"
                );
                buf.extend_from_slice(&raw);
                (true, false)
            };

        // Recover only a verified prefix. An incomplete transaction belongs
        // wholly to the discarded tail, including complete queued commands.
        let mut transaction_start: Option<(usize, usize)> = None;
        loop {
            let position = total_bytes - buf.len();
            match parse(&mut buf) {
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
                Err(_) => {
                    // Never resynchronize at a RESP-looking byte in an invalid
                    // value: it is not evidence of a command boundary.
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
        let result = AofRecovery::replay_file(&path, &mut state).expect("replay corrupt tail");
        assert!(result.corruption_detected);
        assert!(state.db(0).get(&Bytes::from("prefix")).is_none());
    }
}
