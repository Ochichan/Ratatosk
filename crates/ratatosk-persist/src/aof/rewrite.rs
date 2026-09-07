use std::{
    io,
    path::{Path, PathBuf},
};

use bytes::{Bytes, BytesMut};
use ratatosk_resp::{RespFrame, parse};

use super::{
    AofWriter, FsyncPolicy,
    recovery::decode_timed_command,
    writer::{AOF_V1_HEADER, AOF_VERSION_HEADER},
};

pub const DEFAULT_SINGLE_FILE_AOF_FILENAME: &str = "appendonly.aof";

pub fn rewrite_single_file_in_place(aof_path: &Path) -> io::Result<()> {
    let raw = std::fs::read(aof_path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "reading AOF file '{}' for rewrite: {error}",
                aof_path.display()
            ),
        )
    })?;

    let payload: &[u8] = if raw.starts_with(AOF_VERSION_HEADER) || raw.starts_with(AOF_V1_HEADER) {
        &raw[AOF_VERSION_HEADER.len()..]
    } else if raw.starts_with(b"*") || raw.is_empty() {
        &raw
    } else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "AOF file '{}' has unsupported format during rewrite",
                aof_path.display()
            ),
        ));
    };

    let mut parser_buf = BytesMut::from(payload);
    let mut current_db = 0usize;
    let tmp_path = rewrite_temp_path(aof_path);

    if tmp_path.exists() {
        let _ = std::fs::remove_file(&tmp_path);
    }

    let mut tmp_writer = AofWriter::open(&tmp_path, FsyncPolicy::No).map_err(|error| {
        io::Error::other(format!(
            "opening temporary rewrite file '{}': {error}",
            tmp_path.display()
        ))
    })?;

    while !parser_buf.is_empty() {
        let parse_start = payload.len().saturating_sub(parser_buf.len());
        match parse(&mut parser_buf) {
            Ok(Some(frame)) => {
                let (timestamp, frame) = decode_timed_command(frame).map_err(io::Error::other)?;
                let argv = frame_to_argv(frame)?;
                if let Some(db_index) = parse_select_db(&argv) {
                    current_db = db_index;
                    continue;
                }

                tmp_writer
                    .append_command_at(
                        current_db,
                        &argv,
                        timestamp.unwrap_or_else(ratatosk_core::time::now_ms),
                    )
                    .map_err(|error| {
                        io::Error::other(format!("rewriting command into AOF: {error}"))
                    })?;
            }
            Ok(None) => {
                if parser_buf.is_empty() {
                    break;
                }
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "AOF rewrite encountered truncated command near byte {}",
                        parse_start
                    ),
                ));
            }
            Err(error) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("AOF rewrite parse error near byte {}: {error}", parse_start),
                ));
            }
        }
    }

    tmp_writer
        .force_fsync()
        .map_err(|error| io::Error::other(format!("fsyncing rewritten AOF temp file: {error}")))?;
    drop(tmp_writer);

    std::fs::rename(&tmp_path, aof_path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "replacing AOF file '{}' with rewritten temp '{}': {error}",
                aof_path.display(),
                tmp_path.display()
            ),
        )
    })?;

    Ok(())
}

fn rewrite_temp_path(aof_path: &Path) -> PathBuf {
    aof_path.with_extension("rewrite.tmp")
}

fn frame_to_argv(frame: RespFrame) -> io::Result<Vec<Bytes>> {
    let RespFrame::Array(items) = frame else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "AOF rewrite expected RESP array command frame",
        ));
    };

    let mut argv = Vec::with_capacity(items.len());
    for item in items {
        match item {
            RespFrame::BulkString(Some(value)) => argv.push(value),
            RespFrame::SimpleString(value) => argv.push(value),
            RespFrame::Integer(value) => argv.push(Bytes::from(value.to_string())),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "AOF rewrite encountered unsupported argument frame",
                ));
            }
        }
    }

    Ok(argv)
}

fn parse_select_db(argv: &[Bytes]) -> Option<usize> {
    if argv.len() != 2 || !argv[0].eq_ignore_ascii_case(b"SELECT") {
        return None;
    }

    std::str::from_utf8(&argv[1])
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::aof::AofRecovery;
    use ratatosk_engine::keyspace::ServerState;

    #[test]
    fn rewrite_single_file_preserves_command_semantics_across_select_boundaries() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join(DEFAULT_SINGLE_FILE_AOF_FILENAME);

        {
            let mut writer = AofWriter::open(&path, FsyncPolicy::No).expect("open");
            writer
                .append_command(
                    2,
                    &[Bytes::from("SET"), Bytes::from("k2"), Bytes::from("v2")],
                )
                .expect("append db2");
            writer
                .append_command(
                    0,
                    &[Bytes::from("SET"), Bytes::from("k0"), Bytes::from("v0")],
                )
                .expect("append db0");
            writer.force_fsync().expect("fsync");
        }

        rewrite_single_file_in_place(&path).expect("rewrite");

        let mut state = ServerState::with_default_dbs();
        let replay = AofRecovery::replay_file(&path, &mut state).expect("replay rewritten file");
        assert_eq!(replay.commands_replayed, 4);
        assert_eq!(
            state
                .db(2)
                .get(&Bytes::from("k2"))
                .and_then(|value| value.as_string()),
            Some(&Bytes::from("v2"))
        );
        assert_eq!(
            state
                .db(0)
                .get(&Bytes::from("k0"))
                .and_then(|value| value.as_string()),
            Some(&Bytes::from("v0"))
        );
    }

    #[test]
    fn rewrite_single_file_uses_atomic_replace_tempfile() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join(DEFAULT_SINGLE_FILE_AOF_FILENAME);
        let tmp_path = rewrite_temp_path(&path);

        {
            let mut writer = AofWriter::open(&path, FsyncPolicy::No).expect("open");
            writer
                .append_command(0, &[Bytes::from("PING")])
                .expect("append");
            writer.force_fsync().expect("fsync");
        }

        rewrite_single_file_in_place(&path).expect("rewrite");

        assert!(path.exists(), "rewritten AOF should exist");
        assert!(
            !tmp_path.exists(),
            "temporary rewrite file should be removed"
        );
        let content = fs::read(&path).expect("read");
        assert!(
            content.starts_with(AOF_VERSION_HEADER),
            "rewritten file should keep AOF header"
        );
    }
}
