use std::fs::File;
use std::io::{self, Write};
use std::path::Path;

/// Write data to a file atomically: tmp → fsync → rename.
///
/// Ensures no partial writes are visible at `target`.
pub fn atomic_write<F>(target: &Path, write_fn: F) -> io::Result<()>
where
    F: FnOnce(&mut File) -> io::Result<()>,
{
    let parent = parent_dir(target);
    let tmp = tempfile::NamedTempFile::new_in(parent)?;
    let (mut file, tmp_path) = tmp.into_parts();

    write_fn(&mut file)?;
    file.flush()?;
    file.sync_all()?;

    // `persist` renames over the target; on failure the temp file is still
    // owned by the returned error and removed when it drops.
    tmp_path.persist(target).map_err(|error| error.error)?;
    sync_dir(parent);

    Ok(())
}

/// Directory holding `path`; a bare file name lives in the current directory.
pub(crate) fn parent_dir(path: &Path) -> &Path {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    }
}

/// Best-effort fsync of a directory so a completed rename survives a crash.
pub(crate) fn sync_dir(dir: &Path) {
    #[cfg(unix)]
    if let Ok(dir) = File::open(dir) {
        let _ = dir.sync_all();
    }
    #[cfg(not(unix))]
    let _ = dir;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

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
        assert!(!target.exists(), "failed write must not publish the target");
        assert_eq!(
            fs::read_dir(dir.path()).expect("list tmpdir").count(),
            0,
            "failed write must not leave its temp file behind"
        );
    }

    #[test]
    fn atomic_write_replaces_existing_target() {
        let dir = tempfile::tempdir().expect("create tmpdir");
        let target = dir.path().join("replace.rdb");
        fs::write(&target, b"old").expect("seed target");

        atomic_write(&target, |f| f.write_all(b"new")).expect("atomic_write");

        assert_eq!(fs::read(&target).expect("read file"), b"new");
        assert_eq!(fs::read_dir(dir.path()).expect("list tmpdir").count(), 1);
    }

    #[test]
    fn bare_file_name_resolves_to_current_directory() {
        assert_eq!(parent_dir(Path::new("dump.rdb")), Path::new("."));
        assert_eq!(parent_dir(Path::new("data/dump.rdb")), Path::new("data"));
    }
}
