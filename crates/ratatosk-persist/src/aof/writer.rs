use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::path::Path;
use std::time::Instant;

use bytes::Bytes;

use crate::error::PersistError;

/// AOF file format version header.
/// V2 adds timestamped RESP envelopes. V1 remains readable during upgrade.
pub(crate) const AOF_VERSION_HEADER: &[u8] = b"REDIS-AOF-002\n";
pub(crate) const AOF_V1_HEADER: &[u8] = b"REDIS-AOF-001\n";
pub(crate) const TIMED_COMMAND_MARKER: &[u8] = b"RATATOSK.AOF.AT";

// ---------------------------------------------------------------------------
// FsyncPolicy
// ---------------------------------------------------------------------------

/// AOF fsync policy — controls durability vs. performance trade-off.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// fsync after every write (strongest durability, slowest)
    Always,
    /// fsync at most once per second (default, good balance)
    EverySec,
    /// Never explicitly fsync (rely on OS)
    No,
}

impl FsyncPolicy {
    pub fn from_config_str(s: &[u8]) -> Option<Self> {
        match s {
            b"always" => Some(Self::Always),
            b"everysec" => Some(Self::EverySec),
            b"no" => Some(Self::No),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Always => "always",
            Self::EverySec => "everysec",
            Self::No => "no",
        }
    }
}

// ---------------------------------------------------------------------------
// AofWriter
// ---------------------------------------------------------------------------

/// Appends RESP-encoded commands to the AOF file.
pub struct AofWriter {
    writer: BufWriter<File>,
    policy: FsyncPolicy,
    last_fsync: Instant,
    // An existing AOF can end after any SELECT.  Keep this unknown until the
    // first append so that reopening a writer always establishes its replay
    // database explicitly instead of inheriting an old file tail's DB.
    current_db: Option<usize>,
}

impl AofWriter {
    /// Open or create an AOF file at the given path.
    ///
    /// If the file is newly created, writes the version header.
    pub fn open(path: &Path, policy: FsyncPolicy) -> Result<Self, PersistError> {
        let needs_header = !path.exists()
            || std::fs::metadata(path)
                .map(|metadata| metadata.len() == 0)
                .unwrap_or(false);

        if !needs_header {
            // Upgrade only the container marker, preserving every legacy byte.
            // Atomic replacement leaves either complete version on failure.
            let mut input = File::open(path)?;
            let mut header = [0; 14];
            let count = input.read(&mut header)?;
            if !header[..count].starts_with(AOF_VERSION_HEADER) {
                let skip = if header[..count].starts_with(AOF_V1_HEADER) {
                    AOF_V1_HEADER.len()
                } else if header.first() == Some(&b'*') {
                    0
                } else {
                    return Err(PersistError::corrupt(
                        "refusing to append to unknown AOF version",
                    ));
                };
                crate::atomic::atomic_write(path, |output| {
                    output.write_all(AOF_VERSION_HEADER)?;
                    output.write_all(&header[skip..count])?;
                    io::copy(&mut input, output)?;
                    Ok(())
                })?;
            }
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("opening AOF file '{}': {e}", path.display()),
                )
            })?;

        let mut writer = BufWriter::new(file);

        // Write version header for new files
        if needs_header {
            writer.write_all(AOF_VERSION_HEADER).map_err(|e| {
                PersistError::Io(io::Error::new(
                    e.kind(),
                    format!("writing AOF version header: {e}"),
                ))
            })?;
        }

        Ok(Self {
            writer,
            policy,
            last_fsync: Instant::now(),
            current_db: None,
        })
    }

    /// Append a command to the AOF file in RESP format.
    ///
    /// Automatically prepends a SELECT command if the database index changed.
    pub fn append_command(&mut self, db_index: usize, args: &[Bytes]) -> Result<(), PersistError> {
        self.append_command_at(db_index, args, ratatosk_core::time::now_ms())
    }

    pub fn append_command_at(
        &mut self,
        db_index: usize,
        args: &[Bytes],
        timestamp_ms: i64,
    ) -> Result<(), PersistError> {
        if args.is_empty() {
            return Ok(());
        }

        // Emit SELECT if DB changed
        if self.current_db != Some(db_index) {
            self.write_select_command(db_index).map_err(|e| {
                io::Error::new(e.kind(), format!("appending AOF SELECT db {db_index}: {e}"))
            })?;
            self.current_db = Some(db_index);
        }

        self.write_timed_command(args, timestamp_ms)
            .map_err(|e| io::Error::new(e.kind(), format!("appending AOF command: {e}")))?;

        self.maybe_fsync()?;

        Ok(())
    }

    /// Append a committed transaction as one recoverable AOF unit.
    ///
    /// The DB switches live inside the transaction envelope.  If a process is
    /// killed before the final EXEC reaches disk, recovery queues the prefix
    /// but never applies it, which is the failure mode required for a torn
    /// transaction tail.
    pub fn append_transaction(
        &mut self,
        commands: &[(usize, Vec<Bytes>)],
    ) -> Result<(), PersistError> {
        self.append_transaction_at(commands, ratatosk_core::time::now_ms())
    }

    pub fn append_transaction_at(
        &mut self,
        commands: &[(usize, Vec<Bytes>)],
        timestamp_ms: i64,
    ) -> Result<(), PersistError> {
        if commands.is_empty() {
            return Ok(());
        }

        self.write_resp_array_bytes(&[Bytes::from_static(b"MULTI")])
            .map_err(|e| io::Error::new(e.kind(), format!("appending AOF MULTI: {e}")))?;

        for (db_index, args) in commands {
            if args.is_empty() {
                continue;
            }

            if self.current_db != Some(*db_index) {
                self.write_select_command(*db_index).map_err(|e| {
                    io::Error::new(
                        e.kind(),
                        format!("appending AOF transaction SELECT db {db_index}: {e}"),
                    )
                })?;
                self.current_db = Some(*db_index);
            }

            self.write_resp_array_bytes(args).map_err(|e| {
                io::Error::new(e.kind(), format!("appending AOF transaction command: {e}"))
            })?;
        }

        self.write_timed_command(&[Bytes::from_static(b"EXEC")], timestamp_ms)
            .map_err(|e| io::Error::new(e.kind(), format!("appending AOF EXEC: {e}")))?;
        self.maybe_fsync()?;

        Ok(())
    }

    fn write_timed_command(&mut self, args: &[Bytes], timestamp_ms: i64) -> io::Result<()> {
        // Array(marker, integer timestamp, ordinary command array). The nested
        // command remains byte-for-byte RESP and has no client-visible opcode.
        self.writer.write_all(b"*3\r\n")?;
        write!(self.writer, "${}\r\n", TIMED_COMMAND_MARKER.len())?;
        self.writer.write_all(TIMED_COMMAND_MARKER)?;
        write!(self.writer, "\r\n:{timestamp_ms}\r\n")?;
        self.write_resp_array_bytes(args)
    }

    /// Apply a runtime `appendfsync` update without reopening the AOF file.
    pub fn set_policy(&mut self, policy: FsyncPolicy) {
        self.policy = policy;
    }

    /// Perform fsync if required by the policy.
    pub fn maybe_fsync(&mut self) -> Result<(), PersistError> {
        match self.policy {
            FsyncPolicy::Always => {
                self.writer
                    .flush()
                    .map_err(|e| io::Error::new(e.kind(), format!("flushing AOF buffer: {e}")))?;
                self.writer
                    .get_ref()
                    .sync_all()
                    .map_err(|e| io::Error::new(e.kind(), format!("fsync AOF file: {e}")))?;
                self.last_fsync = Instant::now();
            }
            FsyncPolicy::EverySec => {
                if self.last_fsync.elapsed().as_secs() >= 1 {
                    self.writer.flush().map_err(|e| {
                        io::Error::new(e.kind(), format!("flushing AOF buffer: {e}"))
                    })?;
                    self.writer
                        .get_ref()
                        .sync_all()
                        .map_err(|e| io::Error::new(e.kind(), format!("fsync AOF file: {e}")))?;
                    self.last_fsync = Instant::now();
                }
            }
            FsyncPolicy::No => {
                // Flush buffered writer but no fsync
                self.writer
                    .flush()
                    .map_err(|e| io::Error::new(e.kind(), format!("flushing AOF buffer: {e}")))?;
            }
        }
        Ok(())
    }

    /// Force a flush and fsync regardless of policy.
    pub fn force_fsync(&mut self) -> Result<(), PersistError> {
        self.writer
            .flush()
            .map_err(|e| io::Error::new(e.kind(), format!("flushing AOF buffer: {e}")))?;
        self.writer
            .get_ref()
            .sync_all()
            .map_err(|e| io::Error::new(e.kind(), format!("fsync AOF file: {e}")))?;
        self.last_fsync = Instant::now();
        Ok(())
    }

    fn write_select_command(&mut self, db_index: usize) -> io::Result<()> {
        write!(
            self.writer,
            "*2\r\n$6\r\nSELECT\r\n${}\r\n",
            decimal_len_usize(db_index)
        )?;
        write!(self.writer, "{db_index}\r\n")?;
        Ok(())
    }

    fn write_resp_array_bytes(&mut self, args: &[Bytes]) -> io::Result<()> {
        // *<count>\r\n
        write!(self.writer, "*{}\r\n", args.len())?;
        for arg in args {
            // $<len>\r\n<data>\r\n
            write!(self.writer, "${}\r\n", arg.len())?;
            self.writer.write_all(arg)?;
            self.writer.write_all(b"\r\n")?;
        }
        Ok(())
    }
}

fn decimal_len_usize(mut value: usize) -> usize {
    let mut digits = 1usize;
    while value >= 10 {
        value /= 10;
        digits = digits.saturating_add(1);
    }
    digits
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn writer_creates_valid_resp() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("test.aof");

        {
            let mut writer = AofWriter::open(&path, FsyncPolicy::No).expect("open");
            writer
                .append_command(
                    0,
                    &[Bytes::from("SET"), Bytes::from("key"), Bytes::from("value")],
                )
                .expect("append");
        }

        let content = fs::read_to_string(&path).expect("read");
        assert!(
            content.starts_with(std::str::from_utf8(AOF_VERSION_HEADER).unwrap()),
            "AOF should start with version header"
        );
        assert!(
            content.contains("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"),
            "AOF should contain RESP command"
        );
    }

    #[test]
    fn writer_emits_select_on_db_change() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("test.aof");

        {
            let mut writer = AofWriter::open(&path, FsyncPolicy::No).expect("open");
            writer
                .append_command(0, &[Bytes::from("SET"), Bytes::from("k"), Bytes::from("v")])
                .expect("append db0");
            writer
                .append_command(1, &[Bytes::from("SET"), Bytes::from("k"), Bytes::from("v")])
                .expect("append db1");
        }

        let content = fs::read_to_string(&path).expect("read");
        // Should contain SELECT 1 before the second SET
        assert!(content.contains("SELECT"));
        assert!(content.contains("$1\r\n1\r\n"));
    }

    #[test]
    fn writer_emits_one_initial_select_then_reuses_same_db() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("test.aof");

        {
            let mut writer = AofWriter::open(&path, FsyncPolicy::No).expect("open");
            writer
                .append_command(0, &[Bytes::from("SET"), Bytes::from("a"), Bytes::from("1")])
                .expect("append");
            writer
                .append_command(0, &[Bytes::from("SET"), Bytes::from("b"), Bytes::from("2")])
                .expect("append");
        }

        let content = fs::read_to_string(&path).expect("read");
        assert_eq!(content.matches("SELECT").count(), 1);
    }

    #[test]
    fn reopened_writer_reestablishes_db_before_appending() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("test.aof");

        {
            let mut writer = AofWriter::open(&path, FsyncPolicy::No).expect("open");
            writer
                .append_command(
                    1,
                    &[Bytes::from("SET"), Bytes::from("db1"), Bytes::from("v")],
                )
                .expect("append db1");
        }
        {
            let mut writer = AofWriter::open(&path, FsyncPolicy::No).expect("reopen");
            writer
                .append_command(
                    0,
                    &[Bytes::from("SET"), Bytes::from("db0"), Bytes::from("v")],
                )
                .expect("append db0");
        }

        let mut loaded = ratatosk_engine::keyspace::ServerState::with_default_dbs();
        crate::aof::AofRecovery::replay_file(&path, &mut loaded).expect("replay reopened file");
        assert!(loaded.db(0).contains_key(b"db0".as_slice()));
        assert!(!loaded.db(1).contains_key(b"db0".as_slice()));
        assert!(loaded.db(1).contains_key(b"db1".as_slice()));
    }

    #[test]
    fn open_nonexistent_path_has_context_in_error() {
        let path = std::path::Path::new("/nonexistent/dir/test.aof");
        let err = match AofWriter::open(path, FsyncPolicy::No) {
            Err(e) => e,
            Ok(_) => panic!("expected error for nonexistent path"),
        };
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("opening AOF file"),
            "error should contain context about opening AOF file, got: {err_msg}"
        );
        assert!(
            err_msg.contains("test.aof"),
            "error should contain the file path, got: {err_msg}"
        );
    }

    #[test]
    fn fsync_policy_roundtrip() {
        for policy in [FsyncPolicy::Always, FsyncPolicy::EverySec, FsyncPolicy::No] {
            let s = policy.as_str();
            assert_eq!(
                FsyncPolicy::from_config_str(s.as_bytes()),
                Some(policy),
                "roundtrip failed for {s}"
            );
        }
    }
}
