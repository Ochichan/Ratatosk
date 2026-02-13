use std::collections::VecDeque;
use std::fs::File;
use std::io::Read;
use std::path::Path;

use bytes::Bytes;
use hashbrown::{HashMap, HashSet};
use ratatosk_engine::keyspace::{
    DbSnapshot, HashFieldEntry, ServerState, SortedSet, StoredValue, StreamEntry, StreamId,
};

use crate::error::PersistError;

use super::checksum::Crc64Digest;
use super::format::*;

/// RDB loader: deserializes a binary RDB file into a `ServerState`.
pub struct RdbLoader<R: Read> {
    reader: R,
    digest: Crc64Digest,
}

pub fn load(path: &Path) -> Result<DbSnapshot, PersistError> {
    let file = File::open(path).map_err(|e| {
        std::io::Error::new(
            e.kind(),
            format!("opening RDB file '{}': {e}", path.display()),
        )
    })?;

    let mut loaded = ServerState::with_default_dbs();
    RdbLoader::new(file).load_into(&mut loaded)?;
    Ok(loaded.snapshot_dbs())
}

impl<R: Read> RdbLoader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            digest: Crc64Digest::new(),
        }
    }

    /// Load the RDB file into the given server state.
    ///
    /// Existing data in `state` is cleared before loading.
    pub fn load_into(mut self, state: &mut ServerState) -> Result<(), PersistError> {
        state.clear_all_dbs();

        self.read_header().map_err(|e| match e {
            PersistError::Io(io_err) => PersistError::Io(std::io::Error::new(
                io_err.kind(),
                format!("reading RDB header: {io_err}"),
            )),
            other => other,
        })?;
        self.skip_aux_fields(state).map_err(|e| match e {
            PersistError::Io(io_err) => PersistError::Io(std::io::Error::new(
                io_err.kind(),
                format!("reading RDB body: {io_err}"),
            )),
            other => other,
        })?;

        Ok(())
    }

    fn read_bytes(&mut self, buf: &mut [u8]) -> Result<(), PersistError> {
        self.reader
            .read_exact(buf)
            .map_err(|_| PersistError::UnexpectedEof)?;
        self.digest.update(buf);
        Ok(())
    }

    fn read_byte(&mut self) -> Result<u8, PersistError> {
        let mut buf = [0u8; 1];
        self.read_bytes(&mut buf)?;
        Ok(buf[0])
    }

    fn read_header(&mut self) -> Result<(), PersistError> {
        let mut magic = [0u8; 5];
        self.read_bytes(&mut magic)?;
        if &magic != RDB_MAGIC {
            return Err(PersistError::InvalidMagic);
        }

        let mut ver_buf = [0u8; 4];
        self.read_bytes(&mut ver_buf)?;
        let ver_str = std::str::from_utf8(&ver_buf)
            .map_err(|_| PersistError::corrupt("invalid version encoding"))?;
        let version: u32 = ver_str
            .parse()
            .map_err(|_| PersistError::corrupt("invalid version number"))?;

        if version > RDB_VERSION {
            return Err(PersistError::UnsupportedVersion { version });
        }

        Ok(())
    }

    fn skip_aux_fields(&mut self, state: &mut ServerState) -> Result<(), PersistError> {
        let mut current_db: usize = 0;
        let mut next_expire_ms: Option<i64> = None;

        loop {
            let opcode = self.read_byte()?;

            match opcode {
                RDB_OPCODE_AUX => {
                    // Read and discard aux key-value pair
                    let _key = self.read_string()?;
                    let _value = self.read_string()?;
                }
                RDB_OPCODE_SELECTDB => {
                    current_db = self.read_length()? as usize;
                    if current_db >= state.db_count() {
                        return Err(PersistError::corrupt(format!(
                            "DB index {current_db} out of range"
                        )));
                    }
                }
                RDB_OPCODE_RESIZEDB => {
                    let _db_size = self.read_length()?;
                    let _expires_size = self.read_length()?;
                }
                RDB_OPCODE_EXPIRETIME_MS => {
                    let mut buf = [0u8; 8];
                    self.read_bytes(&mut buf)?;
                    next_expire_ms = Some(i64::from_le_bytes(buf));
                }
                RDB_OPCODE_EXPIRETIME => {
                    let mut buf = [0u8; 4];
                    self.read_bytes(&mut buf)?;
                    let secs = u32::from_le_bytes(buf);
                    next_expire_ms = Some(i64::from(secs) * 1000);
                }
                RDB_OPCODE_EOF => {
                    // Verify CRC64
                    let computed_crc = self.digest.value();
                    let mut crc_buf = [0u8; 8];
                    // Read CRC without updating digest
                    self.reader
                        .read_exact(&mut crc_buf)
                        .map_err(|_| PersistError::UnexpectedEof)?;
                    let stored_crc = u64::from_le_bytes(crc_buf);

                    if stored_crc != 0 && stored_crc != computed_crc {
                        return Err(PersistError::CrcMismatch {
                            expected: stored_crc,
                            actual: computed_crc,
                        });
                    }

                    return Ok(());
                }
                type_byte => {
                    // This is a value type byte
                    let expire_ms = next_expire_ms.take();
                    self.read_key_value(state, current_db, type_byte, expire_ms)?;
                }
            }
        }
    }

    fn read_key_value(
        &mut self,
        state: &mut ServerState,
        db_idx: usize,
        type_byte: u8,
        expire_ms: Option<i64>,
    ) -> Result<(), PersistError> {
        let key = self.read_string()?;

        let value = match type_byte {
            RDB_TYPE_STRING => {
                let data = self.read_string()?;
                StoredValue::string(data, expire_ms)
            }
            RDB_TYPE_LIST => {
                let len = self.read_length()? as usize;
                let mut list = VecDeque::with_capacity(len);
                for _ in 0..len {
                    list.push_back(self.read_string()?);
                }
                StoredValue::list(list, expire_ms)
            }
            RDB_TYPE_SET => {
                let len = self.read_length()? as usize;
                let mut set = HashSet::with_capacity(len);
                for _ in 0..len {
                    set.insert(self.read_string()?);
                }
                StoredValue::set(set, expire_ms)
            }
            RDB_TYPE_HASH => {
                let len = self.read_length()? as usize;
                let mut hash = HashMap::with_capacity(len);
                for _ in 0..len {
                    let field = self.read_string()?;
                    let value = self.read_string()?;
                    hash.insert(field, HashFieldEntry::new(value));
                }
                StoredValue::hash(hash, expire_ms)
            }
            RDB_TYPE_ZSET => {
                let len = self.read_length()? as usize;
                let mut zset = SortedSet::default();
                for _ in 0..len {
                    let member = self.read_string()?;
                    let mut score_buf = [0u8; 8];
                    self.read_bytes(&mut score_buf)?;
                    let score = f64::from_bits(u64::from_le_bytes(score_buf));
                    zset.insert(member, score);
                }
                StoredValue::sorted_set(zset, expire_ms)
            }
            RDB_TYPE_STREAM => {
                let entry_count = self.read_length()? as usize;
                let mut entries = Vec::with_capacity(entry_count);
                for _ in 0..entry_count {
                    let mut ms_buf = [0u8; 8];
                    let mut seq_buf = [0u8; 8];
                    self.read_bytes(&mut ms_buf)?;
                    self.read_bytes(&mut seq_buf)?;
                    let id = StreamId {
                        ms: i64::from_le_bytes(ms_buf),
                        seq: i64::from_le_bytes(seq_buf),
                    };

                    let field_count = self.read_length()? as usize;
                    let mut fields = Vec::with_capacity(field_count);
                    for _ in 0..field_count {
                        let fk = self.read_string()?;
                        let fv = self.read_string()?;
                        fields.push((fk, fv));
                    }
                    entries.push(StreamEntry { id, fields });
                }
                StoredValue::stream(entries, expire_ms)
            }
            other => {
                return Err(PersistError::UnknownType { type_byte: other });
            }
        };

        state.db_mut(db_idx).insert(key, value);
        Ok(())
    }

    fn read_length(&mut self) -> Result<u64, PersistError> {
        let first = self.read_byte()?;
        let enc_type = (first & 0xC0) >> 6;

        match enc_type {
            0 => {
                // 6-bit length
                Ok(u64::from(first & 0x3F))
            }
            1 => {
                // 14-bit length
                let second = self.read_byte()?;
                Ok(u64::from(first & 0x3F) << 8 | u64::from(second))
            }
            2 => {
                // Check for 32-bit or 64-bit
                if first == RDB_32BITLEN {
                    let mut buf = [0u8; 4];
                    self.read_bytes(&mut buf)?;
                    Ok(u64::from(u32::from_be_bytes(buf)))
                } else if first == RDB_64BITLEN {
                    let mut buf = [0u8; 8];
                    self.read_bytes(&mut buf)?;
                    Ok(u64::from_be_bytes(buf))
                } else {
                    Err(PersistError::corrupt(format!(
                        "unexpected length encoding byte: {first:#04x}"
                    )))
                }
            }
            3 => {
                // Special encoding — for integer strings
                let enc_kind = first & 0x3F;
                match enc_kind {
                    RDB_ENC_INT8 | RDB_ENC_INT16 | RDB_ENC_INT32 => {
                        // Return special marker — caller should use read_string
                        // which handles this. For now, return the encoded type.
                        Err(PersistError::corrupt(
                            "special encoding in length context not supported",
                        ))
                    }
                    _ => Err(PersistError::corrupt(format!(
                        "unknown special encoding: {enc_kind}"
                    ))),
                }
            }
            _ => Err(PersistError::corrupt("impossible length encoding type")),
        }
    }

    fn read_string(&mut self) -> Result<Bytes, PersistError> {
        let first = self.read_byte()?;
        let enc_type = (first & 0xC0) >> 6;

        match enc_type {
            0..=2 => {
                // Normal length-prefixed string
                let len = match enc_type {
                    0 => u64::from(first & 0x3F),
                    1 => {
                        let second = self.read_byte()?;
                        u64::from(first & 0x3F) << 8 | u64::from(second)
                    }
                    2 => {
                        if first == RDB_32BITLEN {
                            let mut buf = [0u8; 4];
                            self.read_bytes(&mut buf)?;
                            u64::from(u32::from_be_bytes(buf))
                        } else if first == RDB_64BITLEN {
                            let mut buf = [0u8; 8];
                            self.read_bytes(&mut buf)?;
                            u64::from_be_bytes(buf)
                        } else {
                            return Err(PersistError::corrupt("unexpected string length byte"));
                        }
                    }
                    _ => unreachable!(),
                };

                let mut data = vec![0u8; len as usize];
                self.read_bytes(&mut data)?;
                Ok(Bytes::from(data))
            }
            3 => {
                // Integer-encoded string
                let enc_kind = first & 0x3F;
                let int_val = match enc_kind {
                    RDB_ENC_INT8 => {
                        let b = self.read_byte()?;
                        i64::from(b as i8)
                    }
                    RDB_ENC_INT16 => {
                        let mut buf = [0u8; 2];
                        self.read_bytes(&mut buf)?;
                        i64::from(i16::from_le_bytes(buf))
                    }
                    RDB_ENC_INT32 => {
                        let mut buf = [0u8; 4];
                        self.read_bytes(&mut buf)?;
                        i64::from(i32::from_le_bytes(buf))
                    }
                    _ => {
                        return Err(PersistError::corrupt(format!(
                            "unknown integer encoding: {enc_kind}"
                        )));
                    }
                };
                Ok(Bytes::from(int_val.to_string()))
            }
            _ => Err(PersistError::corrupt("impossible string encoding type")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rdb::saver::RdbSaver;

    fn roundtrip_state(state: &ServerState) -> ServerState {
        let mut buf = Vec::new();
        RdbSaver::new(&mut buf).save_state(state).expect("save");

        let mut loaded = ServerState::with_default_dbs();
        RdbLoader::new(buf.as_slice())
            .load_into(&mut loaded)
            .expect("load");

        loaded
    }

    #[test]
    fn roundtrip_empty_state() {
        let state = ServerState::with_default_dbs();
        let loaded = roundtrip_state(&state);

        for db_idx in 0..loaded.db_count() {
            assert!(loaded.db(db_idx).is_empty());
        }
    }

    #[test]
    fn roundtrip_string_keys() {
        let mut state = ServerState::with_default_dbs();
        state.db_mut(0).insert(
            Bytes::from("hello"),
            StoredValue::string(Bytes::from("world"), None),
        );
        state.db_mut(0).insert(
            Bytes::from("foo"),
            StoredValue::string(Bytes::from("bar"), Some(9999999)),
        );

        let loaded = roundtrip_state(&state);
        let db = loaded.db(0);
        assert_eq!(db.len(), 2);

        let hello = db.get(&Bytes::from("hello")).expect("hello exists");
        assert_eq!(hello.as_string(), Some(&Bytes::from("world")));
        assert_eq!(hello.expire_at_ms, None);

        let foo = db.get(&Bytes::from("foo")).expect("foo exists");
        assert_eq!(foo.as_string(), Some(&Bytes::from("bar")));
        assert_eq!(foo.expire_at_ms, Some(9999999));
    }

    #[test]
    fn roundtrip_list() {
        let mut state = ServerState::with_default_dbs();
        let list = VecDeque::from(vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]);
        state
            .db_mut(0)
            .insert(Bytes::from("mylist"), StoredValue::list(list, None));

        let loaded = roundtrip_state(&state);
        let val = loaded
            .db(0)
            .get(&Bytes::from("mylist"))
            .expect("list exists");
        let list = val.as_list().expect("is list");
        assert_eq!(list.len(), 3);
        assert_eq!(list[0], Bytes::from("a"));
        assert_eq!(list[1], Bytes::from("b"));
        assert_eq!(list[2], Bytes::from("c"));
    }

    #[test]
    fn roundtrip_set() {
        let mut state = ServerState::with_default_dbs();
        let mut set = HashSet::new();
        set.insert(Bytes::from("x"));
        set.insert(Bytes::from("y"));
        state
            .db_mut(0)
            .insert(Bytes::from("myset"), StoredValue::set(set, None));

        let loaded = roundtrip_state(&state);
        let val = loaded.db(0).get(&Bytes::from("myset")).expect("set exists");
        let loaded_set = val.as_set().expect("is set");
        assert_eq!(loaded_set.len(), 2);
        assert!(loaded_set.contains(&Bytes::from("x")));
        assert!(loaded_set.contains(&Bytes::from("y")));
    }

    #[test]
    fn roundtrip_hash() {
        let mut state = ServerState::with_default_dbs();
        let mut hash = HashMap::new();
        hash.insert(Bytes::from("f1"), HashFieldEntry::new(Bytes::from("v1")));
        hash.insert(Bytes::from("f2"), HashFieldEntry::new(Bytes::from("v2")));
        state
            .db_mut(0)
            .insert(Bytes::from("myhash"), StoredValue::hash(hash, None));

        let loaded = roundtrip_state(&state);
        let val = loaded
            .db(0)
            .get(&Bytes::from("myhash"))
            .expect("hash exists");
        let loaded_hash = val.as_hash().expect("is hash");
        assert_eq!(loaded_hash.len(), 2);
        assert_eq!(
            loaded_hash.get(&Bytes::from("f1")).map(|e| &e.value),
            Some(&Bytes::from("v1"))
        );
        assert_eq!(
            loaded_hash.get(&Bytes::from("f2")).map(|e| &e.value),
            Some(&Bytes::from("v2"))
        );
    }

    #[test]
    fn roundtrip_sorted_set() {
        let mut state = ServerState::with_default_dbs();
        let mut zset = SortedSet::default();
        zset.insert(Bytes::from("alice"), 1.5);
        zset.insert(Bytes::from("bob"), 2.0);
        state
            .db_mut(0)
            .insert(Bytes::from("myzset"), StoredValue::sorted_set(zset, None));

        let loaded = roundtrip_state(&state);
        let val = loaded
            .db(0)
            .get(&Bytes::from("myzset"))
            .expect("zset exists");
        let loaded_zset = val.as_sorted_set().expect("is zset");
        assert_eq!(loaded_zset.len(), 2);
        assert_eq!(loaded_zset.score(&Bytes::from("alice")), Some(1.5));
        assert_eq!(loaded_zset.score(&Bytes::from("bob")), Some(2.0));
    }

    #[test]
    fn roundtrip_multiple_dbs() {
        let mut state = ServerState::with_default_dbs();
        state.db_mut(0).insert(
            Bytes::from("k0"),
            StoredValue::string(Bytes::from("v0"), None),
        );
        state.db_mut(3).insert(
            Bytes::from("k3"),
            StoredValue::string(Bytes::from("v3"), None),
        );

        let loaded = roundtrip_state(&state);
        assert_eq!(loaded.db(0).len(), 1);
        assert!(loaded.db(1).is_empty());
        assert!(loaded.db(2).is_empty());
        assert_eq!(loaded.db(3).len(), 1);
        assert_eq!(
            loaded
                .db(3)
                .get(&Bytes::from("k3"))
                .and_then(|v| v.as_string()),
            Some(&Bytes::from("v3"))
        );
    }

    #[test]
    fn load_from_empty_reader_returns_error() {
        let data: &[u8] = &[];
        let mut state = ServerState::with_default_dbs();
        let result = RdbLoader::new(data).load_into(&mut state);
        assert!(result.is_err());
    }

    #[test]
    fn invalid_magic_returns_error() {
        let data = b"NOT_REDIS_DATA";
        let mut state = ServerState::with_default_dbs();
        let result = RdbLoader::new(data.as_slice()).load_into(&mut state);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), PersistError::InvalidMagic));
    }

    #[test]
    fn crc_mismatch_returns_error() {
        let mut state = ServerState::with_default_dbs();
        state.db_mut(0).insert(
            Bytes::from("k"),
            StoredValue::string(Bytes::from("v"), None),
        );

        let mut buf = Vec::new();
        RdbSaver::new(&mut buf).save_state(&state).expect("save");

        // Corrupt the CRC (last 8 bytes)
        let len = buf.len();
        buf[len - 1] ^= 0xFF;

        let mut loaded = ServerState::with_default_dbs();
        let result = RdbLoader::new(buf.as_slice()).load_into(&mut loaded);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            PersistError::CrcMismatch { .. }
        ));
    }

    #[test]
    fn roundtrip_stream() {
        let mut state = ServerState::with_default_dbs();
        let entries = vec![
            StreamEntry {
                id: StreamId { ms: 1000, seq: 0 },
                fields: vec![
                    (Bytes::from("name"), Bytes::from("alice")),
                    (Bytes::from("age"), Bytes::from("30")),
                ],
            },
            StreamEntry {
                id: StreamId { ms: 2000, seq: 1 },
                fields: vec![(Bytes::from("name"), Bytes::from("bob"))],
            },
        ];
        state
            .db_mut(0)
            .insert(Bytes::from("mystream"), StoredValue::stream(entries, None));

        let loaded = roundtrip_state(&state);
        let val = loaded
            .db(0)
            .get(&Bytes::from("mystream"))
            .expect("stream exists");
        let (loaded_entries, _) = val.as_stream().expect("is stream");
        assert_eq!(loaded_entries.len(), 2);
        assert_eq!(loaded_entries[0].id.ms, 1000);
        assert_eq!(loaded_entries[0].id.seq, 0);
        assert_eq!(loaded_entries[0].fields.len(), 2);
        assert_eq!(loaded_entries[1].id.ms, 2000);
    }

    #[test]
    fn file_roundtrip_save_load_snapshot() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("snapshot.rdb");

        let mut state = ServerState::with_default_dbs();
        state.db_mut(0).insert(
            Bytes::from("k"),
            StoredValue::string(Bytes::from("v"), None),
        );
        state.db_mut(1).insert(
            Bytes::from("n"),
            StoredValue::string(Bytes::from("1"), Some(5_000)),
        );

        crate::rdb::saver::save(&state.snapshot_dbs(), &path).expect("save snapshot");
        let snapshot = crate::rdb::loader::load(&path).expect("load snapshot");

        let mut loaded = ServerState::new(snapshot.len());
        loaded.load_from_rdb(snapshot);

        assert_eq!(loaded.db(0).len(), 1);
        assert_eq!(loaded.db(1).len(), 1);
        assert_eq!(
            loaded
                .db(0)
                .get(&Bytes::from("k"))
                .and_then(|v| v.as_string()),
            Some(&Bytes::from("v"))
        );
        assert_eq!(
            loaded
                .db(1)
                .get(&Bytes::from("n"))
                .and_then(|v| v.as_string()),
            Some(&Bytes::from("1"))
        );
    }
}
