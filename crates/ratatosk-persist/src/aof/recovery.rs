use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use bytes::BytesMut;
use ratatosk_engine::{
    command::{ClientState, execute},
    keyspace::ServerState,
};
use ratatosk_resp::parse;

use crate::error::PersistError;

/// AOF recovery: replays an AOF file into a `ServerState`.
///
/// Parses RESP commands from the file and executes them
/// against the engine's command dispatcher.
pub struct AofRecovery;

impl AofRecovery {
    /// Replay an AOF file into the given server state.
    ///
    /// Returns the number of commands replayed.
    pub fn replay_file(path: &Path, state: &mut ServerState) -> Result<usize, PersistError> {
        let file = File::open(path).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!("opening AOF file '{}': {e}", path.display()),
            )
        })?;
        Self::replay_reader(BufReader::new(file), state)
    }

    /// Replay from any reader.
    pub fn replay_reader<R: Read>(
        mut reader: R,
        state: &mut ServerState,
    ) -> Result<usize, PersistError> {
        let mut buf = BytesMut::with_capacity(64 * 1024);
        let mut client = ClientState::new(0);
        let mut commands_replayed = 0usize;

        // Read all data into buffer
        let mut raw = Vec::new();
        reader
            .read_to_end(&mut raw)
            .map_err(|e| std::io::Error::new(e.kind(), format!("reading AOF data: {e}")))?;
        buf.extend_from_slice(&raw);

        // Parse and execute frames
        loop {
            match parse(&mut buf) {
                Ok(Some(frame)) => {
                    let _outcome = execute(frame, state, &mut client);
                    commands_replayed += 1;
                }
                Ok(None) => {
                    // No more complete frames
                    break;
                }
                Err(_) => {
                    // Skip malformed data — truncated AOF files are common
                    tracing::warn!(
                        remaining_bytes = buf.len(),
                        "AOF parse error: skipping remaining data"
                    );
                    break;
                }
            }
        }

        Ok(commands_replayed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crate::aof::writer::{AofWriter, FsyncPolicy};

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
                    &[
                        Bytes::from("SET"),
                        Bytes::from("count"),
                        Bytes::from("42"),
                    ],
                )
                .expect("append SET");
        }

        // Replay into fresh state
        let mut state = ServerState::with_default_dbs();
        let replayed = AofRecovery::replay_file(&path, &mut state).expect("replay");
        assert_eq!(replayed, 2);

        // Verify data
        let hello = state.db(0).get(&Bytes::from("hello"));
        assert!(hello.is_some());
        assert_eq!(hello.and_then(|v| v.as_string()), Some(&Bytes::from("world")));

        let count = state.db(0).get(&Bytes::from("count"));
        assert!(count.is_some());
        assert_eq!(count.and_then(|v| v.as_string()), Some(&Bytes::from("42")));
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
        let replayed = AofRecovery::replay_file(&path, &mut state).expect("replay");
        // 3 commands: SET a 0, SELECT 1, SET b 1
        assert_eq!(replayed, 3);

        assert!(state.db(0).get(&Bytes::from("a")).is_some());
        assert!(state.db(1).get(&Bytes::from("b")).is_some());
    }

    #[test]
    fn replay_empty_file_returns_zero() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("empty.aof");
        std::fs::write(&path, b"").expect("create");

        let mut state = ServerState::with_default_dbs();
        let replayed = AofRecovery::replay_file(&path, &mut state).expect("replay");
        assert_eq!(replayed, 0);
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
        let replayed = AofRecovery::replay_file(&path, &mut state).expect("replay");
        // Should replay at least the first command
        assert!(replayed >= 1);
        assert!(state.db(0).get(&Bytes::from("ok")).is_some());
    }
}
