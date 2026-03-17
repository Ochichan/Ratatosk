use std::{env, io, path::Path};

use fs2::available_space;

pub(crate) fn check_disk_space(dir: &Path, min_bytes: u64) -> io::Result<()> {
    let available = available_space(dir).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "failed to query available disk space in {}: {error}",
                dir.display()
            ),
        )
    })?;

    if available < min_bytes {
        return Err(io::Error::new(
            io::ErrorKind::StorageFull,
            format!(
                "insufficient disk space in {}: available={} bytes, required={} bytes",
                dir.display(),
                available,
                min_bytes
            ),
        ));
    }

    tracing::info!(
        target = "ratatosk::startup",
        dir = %dir.display(),
        available_bytes = available,
        min_bytes_required = min_bytes,
        "disk space validation passed"
    );

    Ok(())
}

pub(crate) fn validate_working_directory(dir: &Path) -> io::Result<()> {
    if !dir.exists() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("working directory does not exist: {}", dir.display()),
        ));
    }

    if !dir.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "working directory path is not a directory: {}",
                dir.display()
            ),
        ));
    }

    let test_file = dir.join(".ratatosk_write_test");
    match std::fs::File::create(&test_file) {
        Ok(_) => {
            let _ = std::fs::remove_file(&test_file);
        }
        Err(error) => {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "working directory is not writable: {} (error: {})",
                    dir.display(),
                    error
                ),
            ));
        }
    }

    Ok(())
}

pub(crate) fn validate_aof_file(path: &Path) -> io::Result<()> {
    match std::fs::File::open(path) {
        Ok(_) => {
            tracing::info!(
                target = "ratatosk::startup",
                path = %path.display(),
                "AOF file exists and is readable"
            );
            Ok(())
        }
        Err(error) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "AOF file exists but cannot be read: {} (error: {})",
                path.display(),
                error
            ),
        )),
    }
}

pub(crate) fn env_truthy(name: &str) -> bool {
    env::var(name).is_ok_and(|value| {
        value == "1"
            || value.eq_ignore_ascii_case("true")
            || value.eq_ignore_ascii_case("yes")
            || value.eq_ignore_ascii_case("on")
    })
}
