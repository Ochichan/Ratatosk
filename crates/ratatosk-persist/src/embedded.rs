//! # Embedded Persistence API
//!
//! Direct persistence for embedded use cases. This module provides
//! simplified serialization and deserialization of `ServerState` without
//! requiring the full RDB format overhead.
//!
//! ## Example
//!
//! ```ignore
//! use ratatosk_engine::keyspace::ServerState;
//! use ratatosk_persist::embedded::EmbeddedPersistence;
//!
//! let mut server = ServerState::with_default_dbs();
//! // ... make changes ...
//!
//! // Save snapshot
//! let bytes = EmbeddedPersistence::snapshot_bytes(&server)?;
//!
//! // Load snapshot
//! let mut new_server = ServerState::with_default_dbs();
//! EmbeddedPersistence::load_from_bytes(&mut new_server, &bytes)?;
//! ```

use std::io::{self, Read, Write};

use bytes::Bytes;
use ratatosk_engine::keyspace::{
    Encoding, HashFieldEntry, ServerState, SortedSet, StoredValue, StreamConsumer, StreamEntry,
    StreamGroup, StreamId, ValueData,
};
use sha2::{Digest, Sha256};

use crate::error::PersistError;

/// Magic bytes for embedded snapshot format.
const MAGIC: &[u8; 8] = b"RATASNAP";

/// Current format version.
const VERSION: u32 = 1;

/// Embedded persistence operations.
pub struct EmbeddedPersistence;

impl EmbeddedPersistence {
    /// Serialize a server state snapshot to a writer.
    ///
    /// The format is:
    /// - 8 bytes: magic "RATASNAP"
    /// - 4 bytes: version (little-endian)
    /// - 8 bytes: number of databases (little-endian)
    /// - For each database:
    ///   - 8 bytes: number of keys (little-endian)
    ///   - For each key-value pair:
    ///     - Key encoding
    ///     - Value encoding
    /// - 32 bytes: SHA-256 checksum
    pub fn snapshot_to_writer(server: &ServerState, mut writer: impl Write) -> io::Result<()> {
        let mut hasher = Sha256::new();

        // Write header
        writer.write_all(MAGIC)?;
        hasher.update(MAGIC);

        writer.write_all(&VERSION.to_le_bytes())?;
        hasher.update(VERSION.to_le_bytes());

        let db_count = server.db_count() as u64;
        writer.write_all(&db_count.to_le_bytes())?;
        hasher.update(db_count.to_le_bytes());

        // Write each database
        for db_idx in 0..server.db_count() {
            let db = server.db(db_idx);
            let key_count = db.len() as u64;
            writer.write_all(&key_count.to_le_bytes())?;
            hasher.update(key_count.to_le_bytes());

            for (key, value) in db.iter() {
                // Write key
                Self::write_bytes_with_hash(&mut writer, &mut hasher, key)?;

                // Write value
                Self::write_stored_value(&mut writer, &mut hasher, value)?;
            }
        }

        // Write checksum
        let checksum = hasher.finalize();
        writer.write_all(&checksum)?;

        Ok(())
    }

    /// Serialize a server state to bytes.
    pub fn snapshot_bytes(server: &ServerState) -> io::Result<Vec<u8>> {
        // Estimate buffer size to avoid reallocations:
        //   52  = 8 (magic) + 4 (version) + 8 (db_count) + 32 (checksum)
        //   db_count * 8  = key_count u64 per db
        //   key_count * 64 = rough per-key average (key len + type tag + value)
        let key_count: usize = (0..server.db_count()).map(|i| server.db(i).len()).sum();
        let estimated = 52 + server.db_count() * 8 + key_count * 64;
        let mut buffer = Vec::with_capacity(estimated);
        Self::snapshot_to_writer(server, &mut buffer)?;
        Ok(buffer)
    }

    /// Load a server state from a reader.
    pub fn load_from_reader(
        server: &mut ServerState,
        mut reader: impl Read,
    ) -> Result<(), PersistError> {
        let mut hasher = Sha256::new();

        // Read and verify header
        let mut magic = [0u8; 8];
        reader.read_exact(&mut magic)?;
        if &magic != MAGIC {
            return Err(PersistError::corrupt("Invalid magic bytes"));
        }
        hasher.update(magic);

        let mut version_bytes = [0u8; 4];
        reader.read_exact(&mut version_bytes)?;
        let version = u32::from_le_bytes(version_bytes);
        if version > VERSION {
            return Err(PersistError::corrupt(format!(
                "Unsupported version: {}",
                version
            )));
        }
        hasher.update(version_bytes);

        let mut db_count_bytes = [0u8; 8];
        reader.read_exact(&mut db_count_bytes)?;
        let db_count = u64::from_le_bytes(db_count_bytes) as usize;
        hasher.update(db_count_bytes);

        // Read each database
        let mut dbs = Vec::with_capacity(db_count);
        for _ in 0..db_count {
            let mut key_count_bytes = [0u8; 8];
            reader.read_exact(&mut key_count_bytes)?;
            let key_count = u64::from_le_bytes(key_count_bytes) as usize;
            hasher.update(key_count_bytes);

            let mut db = hashbrown::HashMap::with_capacity(key_count);
            for _ in 0..key_count {
                let key = Self::read_bytes_with_hash(&mut reader, &mut hasher)?;
                let value = Self::read_stored_value(&mut reader, &mut hasher)?;
                db.insert(key, value);
            }
            dbs.push(db);
        }

        // Read and verify checksum
        let mut stored_checksum = [0u8; 32];
        reader.read_exact(&mut stored_checksum)?;
        let computed_checksum = hasher.finalize();

        if stored_checksum != computed_checksum.as_slice() {
            return Err(PersistError::corrupt(format!(
                "Checksum mismatch: expected {}, got {}",
                hex::encode(computed_checksum),
                hex::encode(stored_checksum)
            )));
        }

        server.load_from_rdb(dbs);
        Ok(())
    }

    /// Load a server state from bytes.
    pub fn load_from_bytes(server: &mut ServerState, bytes: &[u8]) -> Result<(), PersistError> {
        Self::load_from_reader(server, bytes)
    }

    // --- Helper functions for writing ---

    fn write_bytes_with_hash(
        writer: &mut impl Write,
        hasher: &mut Sha256,
        data: &[u8],
    ) -> io::Result<()> {
        let len = data.len() as u64;
        writer.write_all(&len.to_le_bytes())?;
        hasher.update(len.to_le_bytes());
        writer.write_all(data)?;
        hasher.update(data);
        Ok(())
    }

    fn write_stored_value(
        writer: &mut impl Write,
        hasher: &mut Sha256,
        value: &StoredValue,
    ) -> io::Result<()> {
        // Write expire_at_ms
        let expire = value.expire_at_ms.unwrap_or(-1i64);
        writer.write_all(&expire.to_le_bytes())?;
        hasher.update(expire.to_le_bytes());

        // Write type tag and data
        match &value.data {
            ValueData::String(s) => {
                writer.write_all(&[0u8])?;
                hasher.update([0u8]);
                Self::write_bytes_with_hash(writer, hasher, s)?;
            }
            ValueData::Hash(h) => {
                writer.write_all(&[1u8])?;
                hasher.update([1u8]);
                let len = h.len() as u64;
                writer.write_all(&len.to_le_bytes())?;
                hasher.update(len.to_le_bytes());
                for (field, entry) in h {
                    Self::write_bytes_with_hash(writer, hasher, field)?;
                    Self::write_bytes_with_hash(writer, hasher, &entry.value)?;
                    let field_expire = entry.expire_at_ms.unwrap_or(-1i64);
                    writer.write_all(&field_expire.to_le_bytes())?;
                    hasher.update(field_expire.to_le_bytes());
                }
            }
            ValueData::List(l) => {
                writer.write_all(&[2u8])?;
                hasher.update([2u8]);
                let len = l.len() as u64;
                writer.write_all(&len.to_le_bytes())?;
                hasher.update(len.to_le_bytes());
                for item in l {
                    Self::write_bytes_with_hash(writer, hasher, item)?;
                }
            }
            ValueData::Set(s) => {
                writer.write_all(&[3u8])?;
                hasher.update([3u8]);
                let len = s.len() as u64;
                writer.write_all(&len.to_le_bytes())?;
                hasher.update(len.to_le_bytes());
                for item in s {
                    Self::write_bytes_with_hash(writer, hasher, item)?;
                }
            }
            ValueData::SortedSet(zset) => {
                writer.write_all(&[4u8])?;
                hasher.update([4u8]);
                let len = zset.len() as u64;
                writer.write_all(&len.to_le_bytes())?;
                hasher.update(len.to_le_bytes());
                for entry in zset.by_score.keys() {
                    Self::write_bytes_with_hash(writer, hasher, &entry.member)?;
                    writer.write_all(&entry.score.value().to_le_bytes())?;
                    hasher.update(entry.score.value().to_le_bytes());
                }
            }
            ValueData::Stream { entries, groups } => {
                writer.write_all(&[5u8])?;
                hasher.update([5u8]);
                let entries_len = entries.len() as u64;
                writer.write_all(&entries_len.to_le_bytes())?;
                hasher.update(entries_len.to_le_bytes());
                for entry in entries {
                    writer.write_all(&entry.id.ms.to_le_bytes())?;
                    hasher.update(entry.id.ms.to_le_bytes());
                    writer.write_all(&entry.id.seq.to_le_bytes())?;
                    hasher.update(entry.id.seq.to_le_bytes());
                    let fields_len = entry.fields.len() as u64;
                    writer.write_all(&fields_len.to_le_bytes())?;
                    hasher.update(fields_len.to_le_bytes());
                    for (field, value) in &entry.fields {
                        Self::write_bytes_with_hash(writer, hasher, field)?;
                        Self::write_bytes_with_hash(writer, hasher, value)?;
                    }
                }
                let groups_len = groups.len() as u64;
                writer.write_all(&groups_len.to_le_bytes())?;
                hasher.update(groups_len.to_le_bytes());
                for (name, group) in groups {
                    Self::write_bytes_with_hash(writer, hasher, name)?;
                    writer.write_all(&group.last_delivered_id.ms.to_le_bytes())?;
                    hasher.update(group.last_delivered_id.ms.to_le_bytes());
                    writer.write_all(&group.last_delivered_id.seq.to_le_bytes())?;
                    hasher.update(group.last_delivered_id.seq.to_le_bytes());
                    // Consumers
                    let consumers_len = group.consumers.len() as u64;
                    writer.write_all(&consumers_len.to_le_bytes())?;
                    hasher.update(consumers_len.to_le_bytes());
                    for (consumer_name, consumer) in &group.consumers {
                        Self::write_bytes_with_hash(writer, hasher, consumer_name)?;
                        writer.write_all(&consumer.seen_time_ms.to_le_bytes())?;
                        hasher.update(consumer.seen_time_ms.to_le_bytes());
                        // Pending IDs
                        let pending_len = consumer.pending.len() as u64;
                        writer.write_all(&pending_len.to_le_bytes())?;
                        hasher.update(pending_len.to_le_bytes());
                        for id in &consumer.pending {
                            writer.write_all(&id.ms.to_le_bytes())?;
                            hasher.update(id.ms.to_le_bytes());
                            writer.write_all(&id.seq.to_le_bytes())?;
                            hasher.update(id.seq.to_le_bytes());
                        }
                    }
                }
            }
        }

        Ok(())
    }

    // --- Helper functions for reading ---

    fn read_bytes_with_hash(
        reader: &mut impl Read,
        hasher: &mut Sha256,
    ) -> Result<Bytes, PersistError> {
        let mut len_bytes = [0u8; 8];
        reader.read_exact(&mut len_bytes)?;
        let len = u64::from_le_bytes(len_bytes) as usize;
        hasher.update(len_bytes);

        if len > 1024 * 1024 * 1024 {
            // 1GB limit
            return Err(PersistError::corrupt(format!("Bytes too large: {}", len)));
        }

        let mut buffer = vec![0u8; len];
        reader.read_exact(&mut buffer)?;
        hasher.update(&buffer);

        Ok(Bytes::from(buffer))
    }

    fn read_stored_value(
        reader: &mut impl Read,
        hasher: &mut Sha256,
    ) -> Result<StoredValue, PersistError> {
        // Read expire_at_ms
        let mut expire_bytes = [0u8; 8];
        reader.read_exact(&mut expire_bytes)?;
        let expire = i64::from_le_bytes(expire_bytes);
        hasher.update(expire_bytes);
        let expire_at_ms = if expire < 0 { None } else { Some(expire) };

        // Read type tag
        let mut type_tag = [0u8; 1];
        reader.read_exact(&mut type_tag)?;
        hasher.update(type_tag);

        let data = match type_tag[0] {
            0 => {
                // String
                let s = Self::read_bytes_with_hash(reader, hasher)?;
                ValueData::String(s)
            }
            1 => {
                // Hash
                let mut len_bytes = [0u8; 8];
                reader.read_exact(&mut len_bytes)?;
                let len = u64::from_le_bytes(len_bytes) as usize;
                hasher.update(len_bytes);

                let mut hash = hashbrown::HashMap::with_capacity(len);
                for _ in 0..len {
                    let field = Self::read_bytes_with_hash(reader, hasher)?;
                    let value = Self::read_bytes_with_hash(reader, hasher)?;
                    let mut field_expire_bytes = [0u8; 8];
                    reader.read_exact(&mut field_expire_bytes)?;
                    let field_expire = i64::from_le_bytes(field_expire_bytes);
                    hasher.update(field_expire_bytes);

                    let entry = if field_expire < 0 {
                        HashFieldEntry::new(value)
                    } else {
                        HashFieldEntry::with_ttl(value, field_expire)
                    };
                    hash.insert(field, entry);
                }
                ValueData::Hash(hash)
            }
            2 => {
                // List
                let mut len_bytes = [0u8; 8];
                reader.read_exact(&mut len_bytes)?;
                let len = u64::from_le_bytes(len_bytes) as usize;
                hasher.update(len_bytes);

                let mut list = std::collections::VecDeque::with_capacity(len);
                for _ in 0..len {
                    list.push_back(Self::read_bytes_with_hash(reader, hasher)?);
                }
                ValueData::List(list)
            }
            3 => {
                // Set
                let mut len_bytes = [0u8; 8];
                reader.read_exact(&mut len_bytes)?;
                let len = u64::from_le_bytes(len_bytes) as usize;
                hasher.update(len_bytes);

                let mut set = hashbrown::HashSet::with_capacity(len);
                for _ in 0..len {
                    set.insert(Self::read_bytes_with_hash(reader, hasher)?);
                }
                ValueData::Set(set)
            }
            4 => {
                // SortedSet
                let mut len_bytes = [0u8; 8];
                reader.read_exact(&mut len_bytes)?;
                let len = u64::from_le_bytes(len_bytes) as usize;
                hasher.update(len_bytes);

                let mut zset = SortedSet::default();
                for _ in 0..len {
                    let member = Self::read_bytes_with_hash(reader, hasher)?;
                    let mut score_bytes = [0u8; 8];
                    reader.read_exact(&mut score_bytes)?;
                    let score = f64::from_le_bytes(score_bytes);
                    hasher.update(score_bytes);
                    zset.insert(member, score);
                }
                ValueData::SortedSet(zset)
            }
            5 => {
                // Stream
                let mut entries_len_bytes = [0u8; 8];
                reader.read_exact(&mut entries_len_bytes)?;
                let entries_len = u64::from_le_bytes(entries_len_bytes) as usize;
                hasher.update(entries_len_bytes);

                let mut entries = Vec::with_capacity(entries_len);
                for _ in 0..entries_len {
                    let mut ms_bytes = [0u8; 8];
                    reader.read_exact(&mut ms_bytes)?;
                    let ms = i64::from_le_bytes(ms_bytes);
                    hasher.update(ms_bytes);

                    let mut seq_bytes = [0u8; 8];
                    reader.read_exact(&mut seq_bytes)?;
                    let seq = i64::from_le_bytes(seq_bytes);
                    hasher.update(seq_bytes);

                    let mut fields_len_bytes = [0u8; 8];
                    reader.read_exact(&mut fields_len_bytes)?;
                    let fields_len = u64::from_le_bytes(fields_len_bytes) as usize;
                    hasher.update(fields_len_bytes);

                    let mut fields = Vec::with_capacity(fields_len);
                    for _ in 0..fields_len {
                        let field = Self::read_bytes_with_hash(reader, hasher)?;
                        let value = Self::read_bytes_with_hash(reader, hasher)?;
                        fields.push((field, value));
                    }

                    entries.push(StreamEntry {
                        id: StreamId { ms, seq },
                        fields,
                    });
                }

                let mut groups_len_bytes = [0u8; 8];
                reader.read_exact(&mut groups_len_bytes)?;
                let groups_len = u64::from_le_bytes(groups_len_bytes) as usize;
                hasher.update(groups_len_bytes);

                let mut groups = hashbrown::HashMap::with_capacity(groups_len);
                for _ in 0..groups_len {
                    let name = Self::read_bytes_with_hash(reader, hasher)?;

                    let mut ms_bytes = [0u8; 8];
                    reader.read_exact(&mut ms_bytes)?;
                    let ms = i64::from_le_bytes(ms_bytes);
                    hasher.update(ms_bytes);

                    let mut seq_bytes = [0u8; 8];
                    reader.read_exact(&mut seq_bytes)?;
                    let seq = i64::from_le_bytes(seq_bytes);
                    hasher.update(seq_bytes);

                    let mut consumers_len_bytes = [0u8; 8];
                    reader.read_exact(&mut consumers_len_bytes)?;
                    let consumers_len = u64::from_le_bytes(consumers_len_bytes) as usize;
                    hasher.update(consumers_len_bytes);

                    let mut consumers = hashbrown::HashMap::with_capacity(consumers_len);
                    for _ in 0..consumers_len {
                        let consumer_name = Self::read_bytes_with_hash(reader, hasher)?;

                        let mut seen_time_bytes = [0u8; 8];
                        reader.read_exact(&mut seen_time_bytes)?;
                        let seen_time_ms = i64::from_le_bytes(seen_time_bytes);
                        hasher.update(seen_time_bytes);

                        let mut pending_len_bytes = [0u8; 8];
                        reader.read_exact(&mut pending_len_bytes)?;
                        let pending_len = u64::from_le_bytes(pending_len_bytes) as usize;
                        hasher.update(pending_len_bytes);

                        let mut pending = hashbrown::HashSet::with_capacity(pending_len);
                        for _ in 0..pending_len {
                            reader.read_exact(&mut ms_bytes)?;
                            let p_ms = i64::from_le_bytes(ms_bytes);
                            hasher.update(ms_bytes);

                            reader.read_exact(&mut seq_bytes)?;
                            let p_seq = i64::from_le_bytes(seq_bytes);
                            hasher.update(seq_bytes);

                            pending.insert(StreamId {
                                ms: p_ms,
                                seq: p_seq,
                            });
                        }

                        consumers.insert(
                            consumer_name,
                            StreamConsumer {
                                seen_time_ms,
                                pending,
                            },
                        );
                    }

                    groups.insert(
                        name,
                        StreamGroup {
                            last_delivered_id: StreamId { ms, seq },
                            consumers,
                            pending: hashbrown::HashMap::new(), // Group pending entries not stored in this simple format
                        },
                    );
                }

                ValueData::Stream { entries, groups }
            }
            tag => {
                return Err(PersistError::corrupt(format!("Unknown type tag: {}", tag)));
            }
        };

        Ok(StoredValue::from_data(data, expire_at_ms))
    }
}

// Helper trait to create StoredValue from data
trait StoredValueExt {
    fn from_data(data: ValueData, expire_at_ms: Option<i64>) -> Self;
}

impl StoredValueExt for StoredValue {
    fn from_data(data: ValueData, expire_at_ms: Option<i64>) -> Self {
        Self {
            data,
            expire_at_ms,
            encoding: Encoding::Raw,
            lru_clock: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatosk_engine::keyspace::ServerState;

    #[test]
    fn snapshot_empty() {
        let server = ServerState::with_default_dbs();
        let bytes = EmbeddedPersistence::snapshot_bytes(&server).unwrap();
        assert!(!bytes.is_empty());

        let mut loaded = ServerState::with_default_dbs();
        EmbeddedPersistence::load_from_bytes(&mut loaded, &bytes).unwrap();
    }

    #[test]
    fn snapshot_with_string() {
        let mut server = ServerState::with_default_dbs();
        {
            let mut db = server.direct(0);
            db.set(b"key1", b"value1");
            db.set(b"key2", b"value2");
        }

        let bytes = EmbeddedPersistence::snapshot_bytes(&server).unwrap();

        let mut loaded = ServerState::with_default_dbs();
        EmbeddedPersistence::load_from_bytes(&mut loaded, &bytes).unwrap();

        let mut db = loaded.direct(0);
        assert_eq!(db.get(b"key1"), Some(Bytes::from("value1")));
        assert_eq!(db.get(b"key2"), Some(Bytes::from("value2")));
    }

    #[test]
    fn snapshot_with_sorted_set() {
        let mut server = ServerState::with_default_dbs();
        {
            let mut db = server.direct(0);
            db.zadd(b"zset1", 1.0, b"one");
            db.zadd(b"zset1", 2.0, b"two");
            db.zadd(b"zset1", 3.0, b"three");
        }

        let bytes = EmbeddedPersistence::snapshot_bytes(&server).unwrap();

        let mut loaded = ServerState::with_default_dbs();
        EmbeddedPersistence::load_from_bytes(&mut loaded, &bytes).unwrap();

        let mut db = loaded.direct(0);
        assert_eq!(db.zcard(b"zset1"), 3);
        assert_eq!(db.zscore(b"zset1", b"one"), Some(1.0));
        assert_eq!(db.zscore(b"zset1", b"two"), Some(2.0));
        assert_eq!(db.zscore(b"zset1", b"three"), Some(3.0));
    }

    #[test]
    fn snapshot_with_hash() {
        let mut server = ServerState::with_default_dbs();
        {
            let mut db = server.direct(0);
            db.hset(b"hash1", b"f1", b"v1");
            db.hset(b"hash1", b"f2", b"v2");
        }

        let bytes = EmbeddedPersistence::snapshot_bytes(&server).unwrap();

        let mut loaded = ServerState::with_default_dbs();
        EmbeddedPersistence::load_from_bytes(&mut loaded, &bytes).unwrap();

        let mut db = loaded.direct(0);
        assert_eq!(db.hget(b"hash1", b"f1"), Some(Bytes::from("v1")));
        assert_eq!(db.hget(b"hash1", b"f2"), Some(Bytes::from("v2")));
    }

    #[test]
    fn snapshot_with_set() {
        let mut server = ServerState::with_default_dbs();
        {
            let mut db = server.direct(0);
            db.sadd(b"set1", b"a");
            db.sadd(b"set1", b"b");
            db.sadd(b"set1", b"c");
        }

        let bytes = EmbeddedPersistence::snapshot_bytes(&server).unwrap();

        let mut loaded = ServerState::with_default_dbs();
        EmbeddedPersistence::load_from_bytes(&mut loaded, &bytes).unwrap();

        let mut db = loaded.direct(0);
        assert_eq!(db.scard(b"set1"), 3);
        assert!(db.sismember(b"set1", b"a"));
        assert!(db.sismember(b"set1", b"b"));
        assert!(db.sismember(b"set1", b"c"));
    }

    #[test]
    fn checksum_verification() {
        let server = ServerState::with_default_dbs();
        let mut bytes = EmbeddedPersistence::snapshot_bytes(&server).unwrap();

        // Corrupt the checksum
        let len = bytes.len();
        bytes[len - 1] ^= 0xFF;

        let mut loaded = ServerState::with_default_dbs();
        let result = EmbeddedPersistence::load_from_bytes(&mut loaded, &bytes);
        assert!(result.is_err());
    }

    #[test]
    fn magic_verification() {
        let mut bytes = vec![0u8; 100];
        bytes[0..8].copy_from_slice(b"INVALID!");

        let mut loaded = ServerState::with_default_dbs();
        let result = EmbeddedPersistence::load_from_bytes(&mut loaded, &bytes);
        assert!(result.is_err());
    }
}
