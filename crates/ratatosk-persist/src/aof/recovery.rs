use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use bytes::{Buf, BytesMut};
use ratatosk_engine::{
    command::{ClientState, ServerAccess, execute},
    keyspace::ServerState,
};
use ratatosk_resp::{frame::RespFrame, parse};

use crate::error::PersistError;

/// Expected AOF version header.
const AOF_VERSION_HEADER: &[u8] = b"REDIS-AOF-001\n";

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
        // Check for version header
        let (version_mismatch, legacy_format) = if raw.starts_with(AOF_VERSION_HEADER) {
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

        // Parse and execute frames
        loop {
            let bytes_before = buf.len();
            match parse(&mut buf) {
                Ok(Some(frame)) => {
                    let outcome = {
                        let mut access = ServerAccess::new_inline(state);
                        execute(frame, &mut access, &mut client)
                    };
                    if let RespFrame::Error(message) = &outcome.response {
                        let position = total_bytes.saturating_sub(buf.len());
                        tracing::error!(
                            target = "ratatosk::aof",
                            byte_position = position,
                            error = %String::from_utf8_lossy(message),
                            "AOF replay command execution failed"
                        );
                        return Err(PersistError::corrupt(format!(
                            "AOF replay command failed at byte {}: {}",
                            position,
                            String::from_utf8_lossy(message)
                        )));
                    }
                    commands_replayed += 1;
                }
                Ok(None) => {
                    // No more complete frames
                    if !buf.is_empty() {
                        // Truncated data at end
                        let position = total_bytes - buf.len();
                        corruption_positions.push(position);
                        tracing::warn!(
                            target = "ratatosk::aof",
                            byte_position = position,
                            remaining_bytes = buf.len(),
                            "AOF truncated: incomplete command at end of file"
                        );
                    }
                    break;
                }
                Err(_) => {
                    // Parse error - record position and skip
                    let position = total_bytes - bytes_before;
                    corruption_positions.push(position);

                    // Try to recover by skipping to next RESP frame
                    if let Some(next_pos) = find_next_resp_frame(&buf) {
                        tracing::warn!(
                            target = "ratatosk::aof",
                            byte_position = position,
                            skipped_bytes = next_pos,
                            "AOF parse error: skipping to next valid frame"
                        );
                        buf.advance(next_pos);
                    } else {
                        tracing::warn!(
                            target = "ratatosk::aof",
                            byte_position = position,
                            remaining_bytes = buf.len(),
                            "AOF parse error: no valid frame found, truncating"
                        );
                        break;
                    }
                }
            }
        }
        let result = ReplayResult {
            commands_replayed,
            bytes_processed: total_bytes - buf.len(),
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

/// Find the position of the next potential RESP frame start.
fn find_next_resp_frame(buf: &[u8]) -> Option<usize> {
    // RESP frames start with: * (array), + (simple string), - (error), : (integer), $ (bulk string)
    for (i, &byte) in buf.iter().enumerate().skip(1) {
        match byte {
            b'*' | b'+' | b'-' | b':' | b'$' => return Some(i),
            _ => continue,
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aof::writer::{AofWriter, FsyncPolicy};
    use bytes::Bytes;

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
        assert_eq!(result.commands_replayed, 2);
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
        // 3 commands: SET a 0, SELECT 1, SET b 1
        assert_eq!(result.commands_replayed, 3);

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
}
