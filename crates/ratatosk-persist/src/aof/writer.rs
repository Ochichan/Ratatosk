use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::time::Instant;

use bytes::Bytes;

use crate::error::PersistError;

/// AOF file format version header.
/// Format: "REDIS-AOF-001\n" followed by RESP commands.
const AOF_VERSION_HEADER: &[u8] = b"REDIS-AOF-001\n";

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
    current_db: usize,
}

impl AofWriter {
    /// Open or create an AOF file at the given path.
    /// 
    /// If the file is newly created, writes the version header.
    pub fn open(path: &Path, policy: FsyncPolicy) -> Result<Self, PersistError> {
        let is_new = !path.exists();
        
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
        if is_new {
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
            current_db: 0,
        })
    }

    /// Append a command to the AOF file in RESP format.
    ///
    /// Automatically prepends a SELECT command if the database index changed.
    pub fn append_command(
        &mut self,
        db_index: usize,
        args: &[Bytes],
    ) -> Result<(), PersistError> {
        if args.is_empty() {
            return Ok(());
        }

        // Emit SELECT if DB changed
        if db_index != self.current_db {
            self.write_resp_array(&[
                &Bytes::from_static(b"SELECT"),
                &Bytes::from(db_index.to_string()),
            ])
            .map_err(|e| {
                io::Error::new(e.kind(), format!("appending AOF SELECT db {db_index}: {e}"))
            })?;
            self.current_db = db_index;
        }

        // Encode args as RESP array
        let refs: Vec<&Bytes> = args.iter().collect();
        self.write_resp_array(&refs)
            .map_err(|e| io::Error::new(e.kind(), format!("appending AOF command: {e}")))?;

        self.maybe_fsync()?;

        Ok(())
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
                    self.writer.get_ref().sync_all().map_err(|e| {
                        io::Error::new(e.kind(), format!("fsync AOF file: {e}"))
                    })?;
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

    fn write_resp_array(&mut self, args: &[&Bytes]) -> io::Result<()> {
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
                    &[
                        Bytes::from("SET"),
                        Bytes::from("key"),
                        Bytes::from("value"),
                    ],
                )
                .expect("append");
        }

        let content = fs::read_to_string(&path).expect("read");
        assert!(content.starts_with(std::str::from_utf8(AOF_VERSION_HEADER).unwrap()), "AOF should start with version header");
        assert!(content.contains("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"), "AOF should contain RESP command");
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
    fn writer_does_not_emit_select_for_same_db() {
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
        assert!(!content.contains("SELECT"));
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
