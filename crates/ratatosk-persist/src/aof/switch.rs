use std::{
    collections::HashSet,
    io,
    path::{Path, PathBuf},
};

use super::AofManifest;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ManifestSwitchOutcome {
    pub cleanup_pending: Vec<PathBuf>,
}

pub fn commit_manifest_switch(
    manifest_path: &Path,
    next_manifest: &AofManifest,
    cleanup_candidates: &[PathBuf],
) -> io::Result<ManifestSwitchOutcome> {
    validate_manifest_candidate(next_manifest)?;
    next_manifest.save_to_file(manifest_path)?;

    let active_files = next_manifest
        .recovery_files()
        .into_iter()
        .collect::<HashSet<_>>();
    let mut cleanup_pending = Vec::new();
    for path in cleanup_candidates {
        if active_files.contains(path) || !path.exists() {
            continue;
        }
        if let Err(error) = std::fs::remove_file(path) {
            tracing::warn!(
                target = "ratatosk::aof",
                path = %path.display(),
                error = %error,
                "manifest switch cleanup failed; leaving stale file behind"
            );
            cleanup_pending.push(path.clone());
        }
    }

    Ok(ManifestSwitchOutcome { cleanup_pending })
}

pub fn validate_manifest_candidate(manifest: &AofManifest) -> io::Result<()> {
    if let Some(base_path) = manifest.base_path() {
        validate_recovery_file("base", &base_path)?;
    }

    let Some(current_incr) = manifest.current_incr_path() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "manifest switch requires an active incremental AOF file",
        ));
    };
    validate_recovery_file("current incr", &current_incr)?;

    for path in manifest.incr_paths() {
        validate_recovery_file("incr", &path)?;
    }

    Ok(())
}

fn validate_recovery_file(kind: &str, path: &Path) -> io::Result<()> {
    let metadata = std::fs::metadata(path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "manifest switch requires existing {kind} file '{}': {error}",
                path.display()
            ),
        )
    })?;
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "manifest switch expected {kind} path '{}' to be a file",
                path.display()
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aof::DEFAULT_AOF_MANIFEST_FILENAME;

    #[test]
    fn commit_manifest_switch_persists_manifest_and_cleans_stale_files() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let manifest_path = dir.path().join(DEFAULT_AOF_MANIFEST_FILENAME);
        let base = dir.path().join("appendonly.aof.1.base.aof");
        let incr = dir.path().join("appendonly.aof.1.incr.aof");
        let stale = dir.path().join("appendonly.aof.0.incr.aof");
        std::fs::write(&base, b"base").expect("write base");
        std::fs::write(&incr, b"incr").expect("write incr");
        std::fs::write(&stale, b"stale").expect("write stale");

        let mut manifest = AofManifest::new(dir.path());
        manifest.set_base_after_rewrite("appendonly.aof.1.base.aof".into());
        manifest.new_incr_file();

        let outcome =
            commit_manifest_switch(&manifest_path, &manifest, std::slice::from_ref(&stale))
                .expect("commit manifest switch");

        assert!(outcome.cleanup_pending.is_empty());
        assert!(manifest_path.exists(), "manifest should be written");
        assert!(!stale.exists(), "stale file should be removed");
        let loaded = AofManifest::load_from_file(&manifest_path).expect("load manifest");
        assert_eq!(loaded.recovery_files(), manifest.recovery_files());
    }

    #[test]
    fn validate_manifest_candidate_rejects_missing_current_incr() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let mut manifest = AofManifest::new(dir.path());
        manifest.set_base_after_rewrite("appendonly.aof.base.aof".into());
        std::fs::write(manifest.base_path().expect("base path"), b"base").expect("write base");

        let error = validate_manifest_candidate(&manifest).expect_err("missing incr should fail");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn commit_manifest_switch_keeps_cleanup_failures_for_later_retry() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let manifest_path = dir.path().join(DEFAULT_AOF_MANIFEST_FILENAME);
        let incr = dir.path().join("appendonly.aof.1.incr.aof");
        let stale_dir = dir.path().join("appendonly.aof.stale-dir");
        std::fs::write(&incr, b"incr").expect("write incr");
        std::fs::create_dir(&stale_dir).expect("make stale dir");

        let mut manifest = AofManifest::new(dir.path());
        manifest.new_incr_file();

        let outcome =
            commit_manifest_switch(&manifest_path, &manifest, std::slice::from_ref(&stale_dir))
                .expect("commit manifest switch");

        assert_eq!(outcome.cleanup_pending, vec![stale_dir]);
        assert!(manifest_path.exists(), "manifest should still commit");
    }
}
