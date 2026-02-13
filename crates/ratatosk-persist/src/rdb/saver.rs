use std::fs::File;
use std::io::{self, Write};
use std::path::Path;

use bytes::Bytes;
use ratatosk_engine::keyspace::{DbSnapshot, ServerState, StoredValue, ValueData};

use super::checksum::Crc64Digest;
use super::format::*;

/// RDB saver: serializes a `ServerState` snapshot to binary RDB format.
pub struct RdbSaver<W: Write> {
    writer: W,
    digest: Crc64Digest,
}

pub fn save(snapshot: &DbSnapshot, path: &Path) -> io::Result<()> {
    let file = File::create(path).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("creating RDB file '{}': {e}", path.display()),
        )
    })?;
    let mut state = ServerState::new(snapshot.len());
    state.load_from_rdb(snapshot.clone());
    RdbSaver::new(file).save_state(&state)
}

impl<W: Write> RdbSaver<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            digest: Crc64Digest::new(),
        }
    }

    /// Write the full RDB file from the given server state.
    pub fn save_state(mut self, state: &ServerState) -> io::Result<()> {
        self.write_header()
            .map_err(|e| io::Error::new(e.kind(), format!("writing RDB header: {e}")))?;
        self.write_aux(b"redis-ver", b"7.0.0")
            .map_err(|e| io::Error::new(e.kind(), format!("writing RDB aux fields: {e}")))?;
        self.write_aux(b"ratatosk-ver", b"0.1.0")
            .map_err(|e| io::Error::new(e.kind(), format!("writing RDB aux fields: {e}")))?;

        for db_idx in 0..state.db_count() {
            let db = state.db(db_idx);
            if db.is_empty() {
                continue;
            }

            self.write_select_db(db_idx).map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("writing RDB SELECTDB for db {db_idx}: {e}"),
                )
            })?;

            let expires_count = db.values().filter(|v| v.expire_at_ms.is_some()).count();
            self.write_resize_db(db.len(), expires_count).map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("writing RDB RESIZEDB for db {db_idx}: {e}"),
                )
            })?;

            for (key, value) in db.iter() {
                self.write_key_value(key, value).map_err(|e| {
                    io::Error::new(
                        e.kind(),
                        format!("writing RDB key-value in db {db_idx}: {e}"),
                    )
                })?;
            }
        }

        self.write_eof()
            .map_err(|e| io::Error::new(e.kind(), format!("writing RDB EOF/CRC: {e}")))
    }

    fn write_bytes(&mut self, data: &[u8]) -> io::Result<()> {
        self.digest.update(data);
        self.writer.write_all(data)
    }

    fn write_header(&mut self) -> io::Result<()> {
        self.write_bytes(RDB_MAGIC)?;
        self.write_bytes(RDB_VERSION_STR)
    }

    fn write_aux(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.write_bytes(&[RDB_OPCODE_AUX])?;
        self.write_string(key)?;
        self.write_string(value)
    }

    fn write_select_db(&mut self, db_idx: usize) -> io::Result<()> {
        self.write_bytes(&[RDB_OPCODE_SELECTDB])?;
        self.write_length(db_idx as u64)
    }

    fn write_resize_db(&mut self, db_size: usize, expires_size: usize) -> io::Result<()> {
        self.write_bytes(&[RDB_OPCODE_RESIZEDB])?;
        self.write_length(db_size as u64)?;
        self.write_length(expires_size as u64)
    }

    fn write_key_value(&mut self, key: &Bytes, value: &StoredValue) -> io::Result<()> {
        // Write expiry if present
        if let Some(expire_ms) = value.expire_at_ms {
            self.write_bytes(&[RDB_OPCODE_EXPIRETIME_MS])?;
            self.write_bytes(&expire_ms.to_le_bytes())?;
        }

        // Write type byte + key + value data
        match &value.data {
            ValueData::String(s) => {
                self.write_bytes(&[RDB_TYPE_STRING])?;
                self.write_string(key)?;
                self.write_string(s)?;
            }
            ValueData::List(list) => {
                self.write_bytes(&[RDB_TYPE_LIST])?;
                self.write_string(key)?;
                self.write_length(list.len() as u64)?;
                for item in list {
                    self.write_string(item)?;
                }
            }
            ValueData::Set(set) => {
                self.write_bytes(&[RDB_TYPE_SET])?;
                self.write_string(key)?;
                self.write_length(set.len() as u64)?;
                for member in set {
                    self.write_string(member)?;
                }
            }
            ValueData::Hash(hash) => {
                self.write_bytes(&[RDB_TYPE_HASH])?;
                self.write_string(key)?;
                self.write_length(hash.len() as u64)?;
                for (field, entry) in hash {
                    self.write_string(field)?;
                    self.write_string(&entry.value)?;
                }
            }
            ValueData::SortedSet(zset) => {
                self.write_bytes(&[RDB_TYPE_ZSET])?;
                self.write_string(key)?;
                self.write_length(zset.len() as u64)?;
                for entry in zset.by_score.keys() {
                    self.write_string(&entry.member)?;
                    let score_bytes = entry.score.0.to_bits().to_le_bytes();
                    self.write_bytes(&score_bytes)?;
                }
            }
            ValueData::Stream { entries, .. } => {
                // Simplified stream serialization: store as a list of entries
                self.write_bytes(&[RDB_TYPE_STREAM])?;
                self.write_string(key)?;
                self.write_length(entries.len() as u64)?;
                for entry in entries {
                    // Stream ID: ms + seq
                    self.write_bytes(&entry.id.ms.to_le_bytes())?;
                    self.write_bytes(&entry.id.seq.to_le_bytes())?;
                    // Fields
                    self.write_length(entry.fields.len() as u64)?;
                    for (field_key, field_val) in &entry.fields {
                        self.write_string(field_key)?;
                        self.write_string(field_val)?;
                    }
                }
            }
        }

        Ok(())
    }

    fn write_eof(mut self) -> io::Result<()> {
        self.write_bytes(&[RDB_OPCODE_EOF])?;
        let checksum = self.digest.value();
        // CRC64 is written raw (not through digest)
        self.writer.write_all(&checksum.to_le_bytes())?;
        self.writer.flush()
    }

    fn write_length(&mut self, len: u64) -> io::Result<()> {
        if len < 64 {
            // 6-bit encoding
            self.write_bytes(&[len as u8])
        } else if len < 16384 {
            // 14-bit encoding
            let high = 0x40 | ((len >> 8) as u8);
            let low = (len & 0xFF) as u8;
            self.write_bytes(&[high, low])
        } else if len <= u64::from(u32::MAX) {
            // 32-bit encoding
            self.write_bytes(&[RDB_32BITLEN])?;
            self.write_bytes(&(len as u32).to_be_bytes())
        } else {
            // 64-bit encoding
            self.write_bytes(&[RDB_64BITLEN])?;
            self.write_bytes(&len.to_be_bytes())
        }
    }

    fn write_string(&mut self, s: &[u8]) -> io::Result<()> {
        self.write_length(s.len() as u64)?;
        self.write_bytes(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatosk_engine::keyspace::ServerState;

    #[test]
    fn save_empty_state_produces_valid_rdb() {
        let state = ServerState::with_default_dbs();
        let mut buf = Vec::new();
        RdbSaver::new(&mut buf).save_state(&state).expect("save");

        // Should start with REDIS magic
        assert_eq!(&buf[0..5], b"REDIS");
        assert_eq!(&buf[5..9], b"0012");

        // Should end with EOF opcode + 8-byte CRC
        let eof_pos = buf.len() - 9;
        assert_eq!(buf[eof_pos], RDB_OPCODE_EOF);
    }

    #[test]
    fn save_with_string_key() {
        let mut state = ServerState::with_default_dbs();
        state.db_mut(0).insert(
            Bytes::from("hello"),
            StoredValue::string(Bytes::from("world"), None),
        );

        let mut buf = Vec::new();
        RdbSaver::new(&mut buf).save_state(&state).expect("save");

        // Should contain the key and value somewhere in the binary
        assert!(buf.windows(5).any(|w| w == b"hello"));
        assert!(buf.windows(5).any(|w| w == b"world"));
    }

    #[test]
    fn save_with_expiry() {
        let mut state = ServerState::with_default_dbs();
        state.db_mut(0).insert(
            Bytes::from("key"),
            StoredValue::string(Bytes::from("val"), Some(1_234_567_890_000)),
        );

        let mut buf = Vec::new();
        RdbSaver::new(&mut buf).save_state(&state).expect("save");

        assert!(buf.contains(&RDB_OPCODE_EXPIRETIME_MS));
    }

    #[test]
    fn save_to_failing_writer_has_context_in_error() {
        struct FailingWriter;
        impl io::Write for FailingWriter {
            fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "mock write failure",
                ))
            }
            fn flush(&mut self) -> io::Result<()> {
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "mock flush failure",
                ))
            }
        }

        let state = ServerState::with_default_dbs();
        let result = RdbSaver::new(FailingWriter).save_state(&state);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("writing RDB header"),
            "error should contain context about writing RDB header, got: {err_msg}"
        );
    }
}
