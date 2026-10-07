use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

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

/// Upper bound on bytes held back from the file descriptor while a background
/// fsync is in flight (`appendfsync everysec`).  When the bound is reached the
/// buffer is written to the fd even though the fsync is still running (the
/// write may block on it), so memory use stays bounded even if the disk stalls.
pub const MAX_PENDING_DURING_FSYNC: usize = 64 * 1024 * 1024;

/// Longest time records are held back from the fd behind one in-flight fsync.
/// After this the writer writes anyway and accepts that the write may block on
/// the fsync, like Redis does (it counts these in `aof_delayed_fsync`).
pub const MAX_HOLD_DURING_FSYNC: Duration = Duration::from_secs(2);

/// Without an fsync in flight the buffer is written to the fd in chunks of at
/// least this many bytes, matching the old `BufWriter` capacity, so everysec
/// does about one `write(2)` per 8 KiB instead of one per append.
const WRITE_BATCH: usize = 8 * 1024;

/// Minimum spacing between everysec fsyncs, measured from the previous
/// fsync's completion.
const EVERYSEC_INTERVAL: Duration = Duration::from_secs(1);

/// The periodic timer tick fires on a one second cadence of its own, so it
/// treats an fsync as due slightly early.  Otherwise phase drift between the
/// timer and the previous completion would delay the tail fsync by a tick.
const EVERYSEC_TICK_SLACK: Duration = Duration::from_millis(100);

/// Outcome of a background fsync, sent by the fsync thread.
struct FsyncDone {
    finished_at: Instant,
    result: io::Result<()>,
}

/// A background fsync of a cloned descriptor.
struct InFlightFsync {
    done: Receiver<FsyncDone>,
    thread: JoinHandle<()>,
    started: Instant,
    /// Already counted in `delayed_fsync`.
    counted_delayed: bool,
}

/// Replacement for `File::sync_all`, for fault injection in tests.
pub type FsyncHook = std::sync::Arc<dyn Fn(&File) -> io::Result<()> + Send + Sync>;

/// Appends RESP-encoded commands to the AOF file.
///
/// Records are encoded into an in-memory buffer and handed to the file
/// descriptor by [`AofWriter::maybe_fsync`] according to the policy.  Under
/// `EverySec` the fsync runs on its own thread against a cloned descriptor.
/// While it runs, the buffer keeps growing and is not written to the file,
/// because on macOS `F_FULLFSYNC` blocks concurrent `write` calls on the same
/// file.  The buffer is written, in order, once the fsync completes.
pub struct AofWriter {
    file: File,
    /// Encoded records not yet written to `file`.
    buf: Vec<u8>,
    policy: FsyncPolicy,
    last_fsync: Instant,
    /// True when bytes reached the descriptor after the last fsync started,
    /// so another fsync would make something durable.
    dirty: bool,
    in_flight: Option<InFlightFsync>,
    /// A background fsync failure.  It stays set until an append or timer tick
    /// returns it, because only those callers latch the server's AOF write
    /// error.  Flush, policy change, rewrite and shutdown report it too but
    /// leave it in place.
    deferred_error: Option<(io::ErrorKind, String)>,
    /// How many times records were written to the fd while an fsync was still
    /// in flight (size or time bound reached).
    delayed_fsync: u64,
    /// Time bound for holding records back; [`MAX_HOLD_DURING_FSYNC`] outside tests.
    max_hold: Duration,
    /// Held-back byte bound; [`MAX_PENDING_DURING_FSYNC`] outside tests.
    max_pending: usize,
    // An existing AOF can end after any SELECT.  Keep this unknown until the
    // first append so that reopening a writer always establishes its replay
    // database explicitly instead of inheriting an old file tail's DB.
    current_db: Option<usize>,
    fsync_hook: Option<FsyncHook>,
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
            // `read` may return fewer bytes than asked for; take the header
            // through `Read::take` so a short read cannot misidentify it.
            let mut header = Vec::with_capacity(AOF_VERSION_HEADER.len());
            (&mut input)
                .take(AOF_VERSION_HEADER.len() as u64)
                .read_to_end(&mut header)?;
            if !header.starts_with(AOF_VERSION_HEADER) {
                let skip = if header.starts_with(AOF_V1_HEADER) {
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
                    output.write_all(&header[skip..])?;
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

        let mut buf = Vec::with_capacity(8192);

        // Write version header for new files
        if needs_header {
            buf.extend_from_slice(AOF_VERSION_HEADER);
        }

        let mut writer = Self {
            file,
            buf,
            policy,
            last_fsync: Instant::now(),
            dirty: false,
            max_pending: MAX_PENDING_DURING_FSYNC,
            in_flight: None,
            deferred_error: None,
            delayed_fsync: 0,
            max_hold: MAX_HOLD_DURING_FSYNC,
            current_db: None,
            fsync_hook: None,
        };
        if needs_header {
            writer.write_pending().map_err(|e| {
                PersistError::Io(io::Error::new(
                    e.kind(),
                    format!("writing AOF version header: {e}"),
                ))
            })?;
        }
        Ok(writer)
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
        self.buf.write_all(b"*3\r\n")?;
        write!(self.buf, "${}\r\n", TIMED_COMMAND_MARKER.len())?;
        self.buf.write_all(TIMED_COMMAND_MARKER)?;
        write!(self.buf, "\r\n:{timestamp_ms}\r\n")?;
        self.write_resp_array_bytes(args)
    }

    /// Apply a runtime `appendfsync` update without reopening the AOF file.
    ///
    /// Waits for any in-flight fsync and writes the held-back buffer first, so
    /// the new policy starts from a state with nothing postponed.  Only an I/O
    /// failure of that drain fails the call (old policy stays).  A background
    /// fsync failure is kept for the next append or tick to report and latch.
    pub fn set_policy(&mut self, policy: FsyncPolicy) -> Result<(), PersistError> {
        self.drain_quiet()?;
        self.policy = policy;
        Ok(())
    }

    /// Hand buffered records to the file and fsync if the policy requires it.
    ///
    /// Under `EverySec` this never waits for the disk.  Without an fsync in
    /// flight it writes the buffer once it holds 8 KiB or an
    /// fsync is due; during an fsync it holds records back (bounded by
    /// [`MAX_PENDING_DURING_FSYNC`] bytes and [`MAX_HOLD_DURING_FSYNC`]).  A
    /// failed background fsync is returned here, once, so the caller latches it.
    pub fn maybe_fsync(&mut self) -> Result<(), PersistError> {
        let result = match self.policy {
            FsyncPolicy::Always => self.drain_quiet().and_then(|()| self.sync_now()),
            FsyncPolicy::EverySec => self.everysec_step(EVERYSEC_INTERVAL, false),
            FsyncPolicy::No => self.drain_quiet(),
        };
        self.deliver(result)
    }

    /// Periodic `EverySec` housekeeping, called from the server's timer.
    ///
    /// Collects a finished background fsync, writes the buffered tail, and
    /// starts the next fsync when due, even if no client has written since.
    /// Under other policies it only delivers a pending fsync failure.
    pub fn everysec_tick(&mut self) -> Result<(), PersistError> {
        let result = if self.policy == FsyncPolicy::EverySec {
            self.everysec_step(EVERYSEC_INTERVAL - EVERYSEC_TICK_SLACK, true)
        } else {
            Ok(())
        };
        self.deliver(result)
    }

    /// Force a flush and fsync regardless of policy.
    ///
    /// Waits for any in-flight fsync, writes the held-back buffer, and always
    /// runs the final fsync, even after an earlier failure.  Returns the first
    /// error; a stored background fsync failure is reported but stays set.
    pub fn force_fsync(&mut self) -> Result<(), PersistError> {
        let drained = self.drain_quiet();
        let synced = self.sync_now();
        drained?;
        synced?;
        match self.stored_error() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Replace the fsync call with `hook`.  Test-only fault injection; the
    /// hook runs on the fsync thread for background fsyncs.
    #[doc(hidden)]
    pub fn set_fsync_hook_for_tests(&mut self, hook: FsyncHook) {
        self.fsync_hook = Some(hook);
    }

    /// Number of times records were written while an fsync was still in flight.
    pub fn delayed_fsync_count(&self) -> u64 {
        self.delayed_fsync
    }

    /// True while a background fsync has been started and not yet collected.
    pub fn fsync_in_flight(&self) -> bool {
        self.in_flight.is_some()
    }

    /// Collect a finished background fsync without a client append.
    ///
    /// The server worker calls this on a short poll while an fsync is in
    /// flight so postponed records reach the file soon after the fsync ends,
    /// even when no further append arrives.  Failures are kept for the next
    /// append or tick.
    pub fn poll_background(&mut self) {
        if self.policy != FsyncPolicy::EverySec || self.in_flight.is_none() {
            return;
        }
        if let Err(error) = self.everysec_step(EVERYSEC_INTERVAL, true) {
            if self.deferred_error.is_none() {
                self.deferred_error = Some((io::ErrorKind::Other, error.to_string()));
            }
        }
    }

    fn stored_error(&self) -> Option<PersistError> {
        self.deferred_error
            .as_ref()
            .map(|(kind, message)| io::Error::new(*kind, message.clone()).into())
    }

    /// Return and clear a stored background fsync failure, otherwise `result`.
    fn deliver(&mut self, result: Result<(), PersistError>) -> Result<(), PersistError> {
        match self.deferred_error.take() {
            Some((kind, message)) => Err(io::Error::new(kind, message).into()),
            None => result,
        }
    }

    /// Wait for an in-flight fsync and write everything held back, without
    /// starting a new fsync.  Only write failures are returned.
    fn drain_quiet(&mut self) -> Result<(), PersistError> {
        self.drain_in_flight();
        self.write_pending_ctx()
    }

    /// One step of the everysec state machine.  `flush_tail` is set by the
    /// timer and the poll, which write whatever is buffered; appends write only
    /// when the buffer reaches [`WRITE_BATCH`] or an fsync is due.  Returns
    /// only I/O errors of its own writes; fsync failures are stored.
    fn everysec_step(&mut self, interval: Duration, flush_tail: bool) -> Result<(), PersistError> {
        if self.in_flight.is_some() {
            if self.try_reap() {
                self.write_pending_ctx()?;
            } else {
                let (held, counted) = self.in_flight.as_ref().map_or((Duration::ZERO, true), |f| {
                    (f.started.elapsed(), f.counted_delayed)
                });
                let over_size = self.buf.len() >= self.max_pending;
                let over_time = held >= self.max_hold;
                // Stop holding back: bounded memory and bounded delay win over
                // the risk that this write blocks on the fsync.  Past the time
                // bound every call writes, so no record waits longer than
                // `max_hold` however small and frequent the appends are.
                if over_size || over_time {
                    if !counted {
                        self.delayed_fsync += 1;
                        if let Some(in_flight) = self.in_flight.as_mut() {
                            in_flight.counted_delayed = true;
                        }
                    }
                    self.write_pending_ctx()?;
                }
                return Ok(());
            }
        }

        let due = (self.dirty || !self.buf.is_empty()) && self.last_fsync.elapsed() >= interval;
        if due || flush_tail || self.buf.len() >= WRITE_BATCH {
            self.write_pending_ctx()?;
        }
        if due {
            self.start_background_fsync()?;
        }
        Ok(())
    }

    /// Start an fsync of a cloned descriptor on a dedicated thread.  Everything
    /// appended so far must already be written to the file.
    fn start_background_fsync(&mut self) -> Result<(), PersistError> {
        debug_assert!(self.buf.is_empty() && self.in_flight.is_none());
        let file = self
            .file
            .try_clone()
            .map_err(|e| io::Error::new(e.kind(), format!("cloning AOF fd for fsync: {e}")))?;
        let hook = self.fsync_hook.clone();
        let (tx, rx) = mpsc::channel();
        let thread = thread::Builder::new()
            .name("aof-fsync".to_string())
            .spawn(move || {
                let result = match &hook {
                    Some(hook) => hook(&file),
                    None => file.sync_all(),
                };
                let _ = tx.send(FsyncDone {
                    finished_at: Instant::now(),
                    result,
                });
            })
            .map_err(|e| io::Error::new(e.kind(), format!("spawning AOF fsync thread: {e}")))?;
        self.dirty = false;
        self.in_flight = Some(InFlightFsync {
            done: rx,
            thread,
            started: Instant::now(),
            counted_delayed: false,
        });
        Ok(())
    }

    /// Non-blocking check of the in-flight fsync.  True means it finished and
    /// the in-flight slot is now empty (a failure is stored).
    fn try_reap(&mut self) -> bool {
        let Some(in_flight) = self.in_flight.as_ref() else {
            return true;
        };
        match in_flight.done.try_recv() {
            Ok(done) => {
                self.finish_fsync(Some(done));
                true
            }
            Err(TryRecvError::Empty) => false,
            Err(TryRecvError::Disconnected) => {
                self.finish_fsync(None);
                true
            }
        }
    }

    fn drain_in_flight(&mut self) {
        let Some(in_flight) = self.in_flight.as_ref() else {
            return;
        };
        let done = in_flight.done.recv().ok();
        self.finish_fsync(done);
    }

    fn finish_fsync(&mut self, done: Option<FsyncDone>) {
        if let Some(in_flight) = self.in_flight.take() {
            let _ = in_flight.thread.join();
        }
        let failure = match done {
            Some(FsyncDone {
                finished_at,
                result: Ok(()),
            }) => {
                self.last_fsync = finished_at;
                return;
            }
            Some(FsyncDone { result: Err(e), .. }) => (e.kind(), format!("fsync AOF file: {e}")),
            None => (
                io::ErrorKind::Other,
                "fsync AOF file: fsync thread exited without a result".to_string(),
            ),
        };
        self.dirty = true;
        if self.deferred_error.is_none() {
            self.deferred_error = Some(failure);
        }
    }

    /// Synchronous fsync of the file.  Nothing may be in flight.
    fn sync_now(&mut self) -> Result<(), PersistError> {
        debug_assert!(self.in_flight.is_none());
        let result = match &self.fsync_hook {
            Some(hook) => hook(&self.file),
            None => self.file.sync_all(),
        };
        result.map_err(|e| io::Error::new(e.kind(), format!("fsync AOF file: {e}")))?;
        self.dirty = false;
        self.last_fsync = Instant::now();
        Ok(())
    }

    fn write_pending_ctx(&mut self) -> Result<(), PersistError> {
        self.write_pending()
            .map_err(|e| io::Error::new(e.kind(), format!("flushing AOF buffer: {e}")).into())
    }

    /// Write the held-back buffer to the file in order.  On failure the
    /// unwritten tail stays buffered so a retry cannot duplicate or reorder
    /// records.  Called with an fsync in flight only when the size or time
    /// bound forces it.
    fn write_pending(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let mut written = 0;
        let result = loop {
            if written == self.buf.len() {
                break Ok(());
            }
            match self.file.write(&self.buf[written..]) {
                Ok(0) => break Err(io::ErrorKind::WriteZero.into()),
                Ok(n) => written += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => break Err(e),
            }
        };
        self.buf.drain(..written);
        if written > 0 {
            self.dirty = true;
        }
        if self.buf.is_empty() && self.buf.capacity() > 4 * 1024 * 1024 {
            self.buf = Vec::with_capacity(8192);
        }
        result
    }

    fn write_select_command(&mut self, db_index: usize) -> io::Result<()> {
        write!(
            self.buf,
            "*2\r\n$6\r\nSELECT\r\n${}\r\n",
            decimal_len_usize(db_index)
        )?;
        write!(self.buf, "{db_index}\r\n")?;
        Ok(())
    }

    fn write_resp_array_bytes(&mut self, args: &[Bytes]) -> io::Result<()> {
        // *<count>\r\n
        write!(self.buf, "*{}\r\n", args.len())?;
        for arg in args {
            // $<len>\r\n<data>\r\n
            write!(self.buf, "${}\r\n", arg.len())?;
            self.buf.write_all(arg)?;
            self.buf.write_all(b"\r\n")?;
        }
        Ok(())
    }
}

impl Drop for AofWriter {
    /// Best effort: do not lose records held back for an fsync.
    fn drop(&mut self) {
        self.drain_in_flight();
        let _ = self.write_pending();
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

    // -- everysec double-buffer tests ------------------------------------

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex};

    /// Fsync hook whose first call blocks until `release` is called and
    /// which counts calls.  `fail_first` makes the first call return an error.
    struct Gate {
        open: Mutex<bool>,
        cv: Condvar,
        calls: AtomicUsize,
        fail_first: bool,
        block_first: bool,
    }

    impl Gate {
        fn new(block_first: bool, fail_first: bool) -> Arc<Self> {
            Arc::new(Self {
                open: Mutex::new(!block_first),
                cv: Condvar::new(),
                calls: AtomicUsize::new(0),
                fail_first,
                block_first,
            })
        }

        fn release(&self) {
            *self.open.lock().unwrap() = true;
            self.cv.notify_all();
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        fn hook(self: &Arc<Self>) -> FsyncHook {
            let gate = Arc::clone(self);
            Arc::new(move |file: &File| {
                let call = gate.calls.fetch_add(1, Ordering::SeqCst);
                if call == 0 && gate.block_first {
                    let mut open = gate.open.lock().unwrap();
                    while !*open {
                        open = gate.cv.wait(open).unwrap();
                    }
                }
                if call == 0 && gate.fail_first {
                    return Err(io::Error::other("synthetic fsync failure"));
                }
                file.sync_all()
            })
        }
    }

    fn set_cmd(key: &str) -> [Bytes; 3] {
        [
            Bytes::from("SET"),
            Bytes::from(key.to_string()),
            Bytes::from("v"),
        ]
    }

    fn make_due(writer: &mut AofWriter) {
        writer.last_fsync = Instant::now() - Duration::from_secs(5);
    }

    fn wait_fsync_finished(writer: &AofWriter) {
        let in_flight = writer.in_flight.as_ref().expect("fsync in flight");
        while !in_flight.thread.is_finished() {
            thread::sleep(Duration::from_millis(1));
        }
    }

    fn key_order(path: &Path) -> Vec<String> {
        let content = fs::read(path).expect("read aof");
        let text = String::from_utf8_lossy(&content).into_owned();
        let mut found: Vec<(usize, String)> = Vec::new();
        for key in ["ka", "kb", "kc", "kd"] {
            let needle = format!("${}\r\n{key}\r\n", key.len());
            if let Some(at) = text.find(&needle) {
                found.push((at, key.to_string()));
            }
        }
        found.sort();
        found.into_iter().map(|(_, key)| key).collect()
    }

    #[test]
    fn everysec_appends_during_inflight_fsync_stay_off_the_fd_and_land_in_order() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("t.aof");
        let gate = Gate::new(true, false);
        let mut writer = AofWriter::open(&path, FsyncPolicy::EverySec).expect("open");
        writer.fsync_hook = Some(gate.hook());

        writer.append_command(0, &set_cmd("ka")).expect("a");
        make_due(&mut writer);
        writer
            .append_command(0, &set_cmd("kb"))
            .expect("b starts fsync");
        assert!(writer.in_flight.is_some(), "fsync should be in flight");
        writer.append_command(0, &set_cmd("kc")).expect("c");
        assert_eq!(key_order(&path), ["ka", "kb"], "kc must be held back");
        assert!(!writer.buf.is_empty());

        gate.release();
        wait_fsync_finished(&writer);
        writer.append_command(0, &set_cmd("kd")).expect("d");
        assert!(writer.in_flight.is_none());
        assert!(writer.buf.is_empty());
        assert_eq!(key_order(&path), ["ka", "kb", "kc", "kd"]);
        assert_eq!(gate.calls(), 1);
    }

    #[test]
    fn everysec_select_tracking_follows_logical_order_during_fsync() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("t.aof");
        let gate = Gate::new(true, false);
        let mut writer = AofWriter::open(&path, FsyncPolicy::EverySec).expect("open");
        writer.fsync_hook = Some(gate.hook());
        writer.append_command(0, &set_cmd("ka")).expect("a");
        make_due(&mut writer);
        writer.append_command(0, &set_cmd("kb")).expect("b");
        writer.append_command(1, &set_cmd("kc")).expect("c in db1");
        writer
            .append_command(0, &set_cmd("kd"))
            .expect("d back in db0");
        gate.release();
        writer.force_fsync().expect("drain");

        let mut loaded = ratatosk_engine::keyspace::ServerState::with_default_dbs();
        crate::aof::AofRecovery::replay_file(&path, &mut loaded).expect("replay");
        assert!(loaded.db(0).contains_key(b"ka".as_slice()));
        assert!(loaded.db(0).contains_key(b"kb".as_slice()));
        assert!(loaded.db(1).contains_key(b"kc".as_slice()));
        assert!(loaded.db(0).contains_key(b"kd".as_slice()));
        assert!(!loaded.db(1).contains_key(b"kd".as_slice()));
    }

    #[test]
    fn force_fsync_waits_for_inflight_and_drains_pending() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("t.aof");
        let gate = Gate::new(true, false);
        let mut writer = AofWriter::open(&path, FsyncPolicy::EverySec).expect("open");
        writer.fsync_hook = Some(gate.hook());
        writer.append_command(0, &set_cmd("ka")).expect("a");
        make_due(&mut writer);
        writer.append_command(0, &set_cmd("kb")).expect("b");
        writer.append_command(0, &set_cmd("kc")).expect("c");

        let releaser = {
            let gate = Arc::clone(&gate);
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(50));
                gate.release();
            })
        };
        writer.force_fsync().expect("force fsync");
        releaser.join().expect("releaser");
        assert!(writer.in_flight.is_none() && writer.buf.is_empty());
        assert_eq!(key_order(&path), ["ka", "kb", "kc"]);
        assert_eq!(gate.calls(), 2, "background fsync plus the forced one");
    }

    #[test]
    fn set_policy_drains_inflight_and_pending_before_switching() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("t.aof");
        let gate = Gate::new(true, false);
        let mut writer = AofWriter::open(&path, FsyncPolicy::EverySec).expect("open");
        writer.fsync_hook = Some(gate.hook());
        writer.append_command(0, &set_cmd("ka")).expect("a");
        make_due(&mut writer);
        writer.append_command(0, &set_cmd("kb")).expect("b");
        writer.append_command(0, &set_cmd("kc")).expect("c");
        gate.release();
        writer.set_policy(FsyncPolicy::No).expect("set policy");
        assert!(writer.in_flight.is_none() && writer.buf.is_empty());
        assert_eq!(key_order(&path), ["ka", "kb", "kc"]);
        assert_eq!(writer.policy, FsyncPolicy::No);
    }

    #[test]
    fn dropping_the_writer_drains_held_back_records() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("t.aof");
        let gate = Gate::new(true, false);
        {
            let mut writer = AofWriter::open(&path, FsyncPolicy::EverySec).expect("open");
            writer.fsync_hook = Some(gate.hook());
            writer.append_command(0, &set_cmd("ka")).expect("a");
            make_due(&mut writer);
            writer.append_command(0, &set_cmd("kb")).expect("b");
            writer.append_command(0, &set_cmd("kc")).expect("c");
            gate.release();
        }
        assert_eq!(key_order(&path), ["ka", "kb", "kc"]);
    }

    #[test]
    fn always_policy_fsyncs_before_returning() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("t.aof");
        let gate = Gate::new(false, false);
        let mut writer = AofWriter::open(&path, FsyncPolicy::Always).expect("open");
        writer.fsync_hook = Some(gate.hook());
        writer.append_command(0, &set_cmd("ka")).expect("a");
        assert_eq!(gate.calls(), 1);
        assert!(writer.in_flight.is_none() && writer.buf.is_empty());
        assert_eq!(key_order(&path), ["ka"]);
        writer.append_command(0, &set_cmd("kb")).expect("b");
        assert_eq!(gate.calls(), 2);
        assert_eq!(key_order(&path), ["ka", "kb"]);
    }

    #[test]
    fn background_fsync_error_is_surfaced_and_keeps_record_order() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("t.aof");
        let gate = Gate::new(false, true);
        let mut writer = AofWriter::open(&path, FsyncPolicy::EverySec).expect("open");
        writer.fsync_hook = Some(gate.hook());
        writer.append_command(0, &set_cmd("ka")).expect("a");
        make_due(&mut writer);
        writer
            .append_command(0, &set_cmd("kb"))
            .expect("b starts fsync");
        wait_fsync_finished(&writer);
        let err = writer
            .append_command(0, &set_cmd("kc"))
            .expect_err("failed fsync must surface on the next append");
        assert!(err.to_string().contains("fsync AOF file"), "{err}");
        assert!(err.to_string().contains("synthetic fsync failure"), "{err}");
        assert_eq!(key_order(&path), ["ka", "kb", "kc"]);
        // The next attempt retries the fsync and succeeds.
        writer.append_command(0, &set_cmd("kd")).expect("retry");
    }

    #[test]
    fn poll_background_collects_fsync_and_defers_its_error() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("t.aof");
        let gate = Gate::new(true, true);
        let mut writer = AofWriter::open(&path, FsyncPolicy::EverySec).expect("open");
        writer.fsync_hook = Some(gate.hook());
        writer.append_command(0, &set_cmd("ka")).expect("a");
        make_due(&mut writer);
        writer
            .append_command(0, &set_cmd("kb"))
            .expect("b starts fsync");
        writer
            .append_command(0, &set_cmd("kc"))
            .expect("c held back");
        assert_eq!(key_order(&path), ["ka", "kb"]);

        writer.poll_background();
        assert!(writer.fsync_in_flight(), "still blocked, nothing collected");

        gate.release();
        wait_fsync_finished(&writer);
        writer.poll_background();
        assert_eq!(
            key_order(&path),
            ["ka", "kb", "kc"],
            "pending written by poll"
        );
        // Flush-like callers report the failure but leave it set.
        let err = writer
            .force_fsync()
            .expect_err("stored fsync failure must be reported");
        assert!(err.to_string().contains("synthetic fsync failure"), "{err}");
        // Only an append or tick delivers it, once.
        let err = writer
            .everysec_tick()
            .expect_err("tick delivers the stored failure");
        assert!(err.to_string().contains("synthetic fsync failure"), "{err}");
        writer.force_fsync().expect("failure was delivered once");
    }

    #[test]
    fn tick_surfaces_background_fsync_error_without_new_writes() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("t.aof");
        let gate = Gate::new(false, true);
        let mut writer = AofWriter::open(&path, FsyncPolicy::EverySec).expect("open");
        writer.fsync_hook = Some(gate.hook());
        writer.append_command(0, &set_cmd("ka")).expect("a");
        make_due(&mut writer);
        writer.everysec_tick().expect("tick starts fsync");
        wait_fsync_finished(&writer);
        let err = writer
            .everysec_tick()
            .expect_err("tick reports the failure");
        assert!(err.to_string().contains("synthetic fsync failure"), "{err}");
    }

    #[test]
    fn tick_fsyncs_idle_tail_once_and_not_when_clean() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("t.aof");
        let gate = Gate::new(false, false);
        let mut writer = AofWriter::open(&path, FsyncPolicy::EverySec).expect("open");
        writer.fsync_hook = Some(gate.hook());
        writer.append_command(0, &set_cmd("ka")).expect("a");
        assert_eq!(gate.calls(), 0, "not due yet, so no inline fsync");

        // Not due: tick must not fsync.
        writer.everysec_tick().expect("early tick");
        assert!(writer.in_flight.is_none());

        make_due(&mut writer);
        writer.everysec_tick().expect("due tick");
        wait_fsync_finished(&writer);
        writer.everysec_tick().expect("reap");
        assert_eq!(gate.calls(), 1);

        // Clean and due again: nothing to make durable, so no fsync.
        make_due(&mut writer);
        writer.everysec_tick().expect("clean tick");
        assert!(writer.in_flight.is_none());
        assert_eq!(gate.calls(), 1);
    }

    #[test]
    fn tick_is_a_noop_under_other_policies() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("t.aof");
        let gate = Gate::new(false, false);
        let mut writer = AofWriter::open(&path, FsyncPolicy::No).expect("open");
        writer.fsync_hook = Some(gate.hook());
        writer.append_command(0, &set_cmd("ka")).expect("a");
        make_due(&mut writer);
        writer.everysec_tick().expect("tick");
        assert_eq!(gate.calls(), 0);
    }

    #[test]
    fn size_bound_writes_to_the_fd_even_while_fsync_is_in_flight() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("t.aof");
        let gate = Gate::new(true, false);
        let mut writer = AofWriter::open(&path, FsyncPolicy::EverySec).expect("open");
        writer.fsync_hook = Some(gate.hook());
        writer.max_pending = 120;
        writer.append_command(0, &set_cmd("ka")).expect("a");
        make_due(&mut writer);
        writer
            .append_command(0, &set_cmd("kb"))
            .expect("b starts fsync");
        writer
            .append_command(0, &set_cmd("kc"))
            .expect("c held back");
        assert_eq!(key_order(&path), ["ka", "kb"]);
        assert_eq!(writer.delayed_fsync_count(), 0);
        // Crossing the bound writes without waiting for the blocked fsync.
        writer.append_command(0, &set_cmd("kd")).expect("d");
        assert_eq!(key_order(&path), ["ka", "kb", "kc", "kd"]);
        assert!(writer.buf.is_empty());
        assert_eq!(writer.delayed_fsync_count(), 1);
        gate.release();
        writer.force_fsync().expect("drain");
    }

    #[test]
    fn time_bound_stops_holding_back_after_max_hold() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("t.aof");
        let gate = Gate::new(true, false);
        let mut writer = AofWriter::open(&path, FsyncPolicy::EverySec).expect("open");
        writer.fsync_hook = Some(gate.hook());
        writer.max_hold = Duration::from_millis(40);
        writer.append_command(0, &set_cmd("ka")).expect("a");
        make_due(&mut writer);
        writer
            .append_command(0, &set_cmd("kb"))
            .expect("b starts fsync");
        writer
            .append_command(0, &set_cmd("kc"))
            .expect("c held back");
        assert_eq!(key_order(&path), ["ka", "kb"]);
        thread::sleep(Duration::from_millis(60));
        writer.poll_background();
        assert_eq!(key_order(&path), ["ka", "kb", "kc"], "held too long");
        assert_eq!(writer.delayed_fsync_count(), 1);
        writer.poll_background();
        assert_eq!(writer.delayed_fsync_count(), 1, "counted once per fsync");
        gate.release();
        writer.force_fsync().expect("drain");
    }

    #[test]
    fn time_bound_holds_for_a_steady_trickle_of_small_appends() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("t.aof");
        let gate = Gate::new(true, false);
        let mut writer = AofWriter::open(&path, FsyncPolicy::EverySec).expect("open");
        writer.fsync_hook = Some(gate.hook());
        writer.max_hold = Duration::from_millis(40);
        writer.append_command(0, &set_cmd("ka")).expect("a");
        make_due(&mut writer);
        writer
            .append_command(0, &set_cmd("kb"))
            .expect("b starts fsync");
        writer
            .append_command(0, &set_cmd("kc"))
            .expect("c held back");
        assert_eq!(key_order(&path), ["ka", "kb"]);
        thread::sleep(Duration::from_millis(60));
        // A small append (far below 8 KiB) past the bound must flush the tail.
        writer.append_command(0, &set_cmd("kd")).expect("d");
        assert_eq!(key_order(&path), ["ka", "kb", "kc", "kd"]);
        assert!(writer.buf.is_empty());
        gate.release();
        writer.force_fsync().expect("drain");
    }

    #[test]
    fn everysec_batches_writes_until_8kib_then_tick_flushes_tail() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("t.aof");
        let mut writer = AofWriter::open(&path, FsyncPolicy::EverySec).expect("open");
        let header_len = fs::metadata(&path).expect("meta").len();
        for key in ["ka", "kb", "kc"] {
            writer.append_command(0, &set_cmd(key)).expect("append");
        }
        assert_eq!(
            fs::metadata(&path).expect("meta").len(),
            header_len,
            "small appends must not reach the fd one by one"
        );
        let big = [
            Bytes::from("SET"),
            Bytes::from("kd"),
            Bytes::from(vec![b'x'; 9000]),
        ];
        writer.append_command(0, &big).expect("big append");
        assert!(fs::metadata(&path).expect("meta").len() > header_len + 9000);
        writer.append_command(0, &set_cmd("ka")).expect("tail");
        let before = fs::metadata(&path).expect("meta").len();
        writer.everysec_tick().expect("tick");
        assert!(fs::metadata(&path).expect("meta").len() > before);
        assert!(writer.buf.is_empty());
    }

    #[test]
    fn policy_change_keeps_background_failure_for_the_next_append() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("t.aof");
        let gate = Gate::new(false, true);
        let mut writer = AofWriter::open(&path, FsyncPolicy::EverySec).expect("open");
        writer.fsync_hook = Some(gate.hook());
        writer.append_command(0, &set_cmd("ka")).expect("a");
        make_due(&mut writer);
        writer
            .append_command(0, &set_cmd("kb"))
            .expect("b starts failing fsync");
        wait_fsync_finished(&writer);
        // CONFIG SET appendfsync always must neither fail nor swallow it.
        writer.set_policy(FsyncPolicy::Always).expect("set policy");
        let err = writer
            .append_command(0, &set_cmd("kc"))
            .expect_err("failure must reach the next append");
        assert!(err.to_string().contains("synthetic fsync failure"), "{err}");
        writer
            .append_command(0, &set_cmd("kd"))
            .expect("delivered once");
        assert_eq!(key_order(&path), ["ka", "kb", "kc", "kd"]);
    }

    #[test]
    fn force_fsync_after_background_failure_still_fsyncs_held_records() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("t.aof");
        let gate = Gate::new(true, true);
        let mut writer = AofWriter::open(&path, FsyncPolicy::EverySec).expect("open");
        writer.fsync_hook = Some(gate.hook());
        writer.append_command(0, &set_cmd("ka")).expect("a");
        make_due(&mut writer);
        writer
            .append_command(0, &set_cmd("kb"))
            .expect("b starts failing fsync");
        writer
            .append_command(0, &set_cmd("kc"))
            .expect("c held back");
        gate.release();
        let err = writer.force_fsync().expect_err("failure reported");
        assert!(err.to_string().contains("synthetic fsync failure"), "{err}");
        assert_eq!(key_order(&path), ["ka", "kb", "kc"]);
        // 1 failing background fsync + 1 final synchronous fsync.
        assert_eq!(gate.calls(), 2, "final fsync must run after the failure");
        // The failure is still pending for the append/tick path.
        assert!(writer.maybe_fsync().is_err());
        assert!(writer.maybe_fsync().is_ok());
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
