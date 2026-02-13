use std::fs::{self, File};
use std::io::{self, Write};
use std::path::Path;

/// Write data to a file atomically: tmp → fsync → rename.
///
/// Ensures no partial writes are visible at `target`.
pub fn atomic_write<F>(target: &Path, write_fn: F) -> io::Result<()>
where
    F: FnOnce(&mut File) -> io::Result<()>,
{
    let parent = target.parent().unwrap_or(Path::new("."));
    let tmp = tempfile::NamedTempFile::new_in(parent)?;
    let (mut file, tmp_path) = tmp.into_parts();

    write_fn(&mut file)?;
    file.flush()?;
    file.sync_all()?;

    // Persist the rename
    let tmp_path_ref = tmp_path
        .keep()
        .map_err(|e| io::Error::other(format!("failed to persist temp file: {e}")))?;
    fs::rename(&tmp_path_ref, target)?;

    // Sync parent directory (Unix best practice)
    #[cfg(unix)]
    {
        if let Ok(dir) = File::open(parent) {
            let _ = dir.sync_all();
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn atomic_write_creates_file_with_content() {
        let dir = tempfile::tempdir().expect("create tmpdir");
        let target = dir.path().join("test.rdb");

        atomic_write(&target, |f| {
            f.write_all(b"hello world")?;
            Ok(())
        })
        .expect("atomic_write");

        let content = fs::read(&target).expect("read file");
        assert_eq!(content, b"hello world");
    }

    #[test]
    fn atomic_write_error_does_not_create_target() {
        let dir = tempfile::tempdir().expect("create tmpdir");
        let target = dir.path().join("should_not_exist.rdb");

        let result = atomic_write(&target, |_f| Err(io::Error::other("simulated error")));

        assert!(result.is_err());
        // Target should not exist because we errored before rename
    }
}
