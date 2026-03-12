use std::{
    io::{self, Write},
    path::{Path, PathBuf},
};

use crate::atomic::atomic_write;

pub const DEFAULT_AOF_MANIFEST_FILENAME: &str = "appendonly.aof.manifest";

/// AOF manifest — tracks BASE + INCR file list.
///
/// Redis 7+ uses a manifest-based AOF where the AOF is split into
/// a BASE file (compact snapshot) and INCR files (incremental appends).
#[derive(Debug, Clone)]
pub struct AofManifest {
    /// Directory containing AOF files
    dir: PathBuf,
    /// Current BASE file name (produced by AOF rewrite)
    base_file: Option<String>,
    /// INCR files in chronological order
    incr_files: Vec<String>,
    /// Next sequence number for naming files
    next_seq: u64,
}

impl AofManifest {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            base_file: None,
            incr_files: Vec::new(),
            next_seq: 1,
        }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn base_file(&self) -> Option<&str> {
        self.base_file.as_deref()
    }

    pub fn base_path(&self) -> Option<PathBuf> {
        self.base_file.as_ref().map(|f| self.dir.join(f))
    }

    pub fn incr_files(&self) -> &[String] {
        &self.incr_files
    }

    pub fn incr_paths(&self) -> Vec<PathBuf> {
        self.incr_files.iter().map(|f| self.dir.join(f)).collect()
    }

    /// All files in recovery order: BASE first, then INCRs in order.
    pub fn recovery_files(&self) -> Vec<PathBuf> {
        let mut files = Vec::new();
        if let Some(base) = &self.base_file {
            files.push(self.dir.join(base));
        }
        for incr in &self.incr_files {
            files.push(self.dir.join(incr));
        }
        files
    }

    /// Create a new INCR file name and register it.
    pub fn new_incr_file(&mut self) -> PathBuf {
        let name = format!("appendonly.aof.{}.incr.aof", self.next_seq);
        self.next_seq += 1;
        self.incr_files.push(name.clone());
        self.dir.join(name)
    }

    /// Set the BASE file after an AOF rewrite completes.
    /// Clears all previous INCR files.
    pub fn set_base_after_rewrite(&mut self, base_name: String) {
        self.base_file = Some(base_name);
        self.incr_files.clear();
    }

    /// Current INCR file path (the last one added).
    pub fn current_incr_path(&self) -> Option<PathBuf> {
        self.incr_files.last().map(|f| self.dir.join(f))
    }

    pub fn default_manifest_path(dir: impl AsRef<Path>) -> PathBuf {
        dir.as_ref().join(DEFAULT_AOF_MANIFEST_FILENAME)
    }

    pub fn save_to_file(&self, path: &Path) -> io::Result<()> {
        atomic_write(path, |file| {
            writeln!(file, "ratatosk-aof-manifest-v1")?;
            writeln!(file, "next_seq {}", self.next_seq)?;
            writeln!(file, "base {}", self.base_file.as_deref().unwrap_or("-"))?;
            for incr in &self.incr_files {
                writeln!(file, "incr {incr}")?;
            }
            Ok(())
        })
    }

    pub fn load_from_file(path: &Path) -> io::Result<Self> {
        let raw = std::fs::read_to_string(path).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("reading AOF manifest '{}': {error}", path.display()),
            )
        })?;

        let mut lines = raw.lines();
        let Some(header) = lines.next() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("AOF manifest '{}' is empty", path.display()),
            ));
        };
        if header != "ratatosk-aof-manifest-v1" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "AOF manifest '{}' has unsupported header '{}'",
                    path.display(),
                    header
                ),
            ));
        }

        let dir = path.parent().unwrap_or(Path::new(".")).to_path_buf();
        let mut manifest = Self::new(dir);
        let mut saw_next_seq = false;
        let mut saw_base = false;

        for line in lines {
            if line.trim().is_empty() {
                continue;
            }
            let Some((key, value)) = line.split_once(' ') else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "AOF manifest '{}' has malformed line '{}'",
                        path.display(),
                        line
                    ),
                ));
            };

            match key {
                "next_seq" => {
                    manifest.next_seq = value.parse::<u64>().map_err(|error| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "AOF manifest '{}' has invalid next_seq '{}': {error}",
                                path.display(),
                                value
                            ),
                        )
                    })?;
                    saw_next_seq = true;
                }
                "base" => {
                    manifest.base_file = (value != "-").then(|| value.to_string());
                    saw_base = true;
                }
                "incr" => manifest.incr_files.push(value.to_string()),
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "AOF manifest '{}' has unsupported key '{}'",
                            path.display(),
                            key
                        ),
                    ));
                }
            }
        }

        if !saw_next_seq || !saw_base {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "AOF manifest '{}' is missing required metadata",
                    path.display()
                ),
            ));
        }

        Ok(manifest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_manifest_has_no_files() {
        let m = AofManifest::new("/tmp/aof");
        assert!(m.base_file().is_none());
        assert!(m.incr_files().is_empty());
        assert!(m.recovery_files().is_empty());
    }

    #[test]
    fn new_incr_creates_sequential_files() {
        let mut m = AofManifest::new("/tmp/aof");
        let p1 = m.new_incr_file();
        let p2 = m.new_incr_file();

        assert!(p1.to_str().expect("str").contains(".1."));
        assert!(p2.to_str().expect("str").contains(".2."));
        assert_eq!(m.incr_files().len(), 2);
    }

    #[test]
    fn set_base_clears_incr() {
        let mut m = AofManifest::new("/tmp/aof");
        m.new_incr_file();
        m.new_incr_file();
        assert_eq!(m.incr_files().len(), 2);

        m.set_base_after_rewrite("base.rdb".into());
        assert_eq!(m.base_file(), Some("base.rdb"));
        assert!(m.incr_files().is_empty());
    }

    #[test]
    fn recovery_files_ordered() {
        let mut m = AofManifest::new("/data");
        m.set_base_after_rewrite("base.aof".into());
        m.new_incr_file();
        m.new_incr_file();

        let files = m.recovery_files();
        assert_eq!(files.len(), 3);
        assert!(files[0].ends_with("base.aof"));
    }

    #[test]
    fn manifest_roundtrip_file_io() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let manifest_path = AofManifest::default_manifest_path(dir.path());

        let mut manifest = AofManifest::new(dir.path());
        manifest.set_base_after_rewrite("appendonly.aof.base.rdb".into());
        manifest.new_incr_file();
        manifest.new_incr_file();
        manifest
            .save_to_file(&manifest_path)
            .expect("save manifest");

        let loaded = AofManifest::load_from_file(&manifest_path).expect("load manifest");
        assert_eq!(loaded.base_file(), manifest.base_file());
        assert_eq!(loaded.incr_files(), manifest.incr_files());
        assert_eq!(loaded.recovery_files(), manifest.recovery_files());
    }
}
