use std::path::{Path, PathBuf};

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
}
