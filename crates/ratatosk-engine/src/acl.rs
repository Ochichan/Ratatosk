use argon2::Argon2;
use argon2::password_hash::{
    PasswordHash, PasswordHasher, PasswordVerifier, SaltString, rand_core::OsRng,
};
use bytes::Bytes;
use hashbrown::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering as AtomicOrdering},
};

use crate::security::sanitize_acl_log_line;

pub const DEFAULT_ACL_FILENAME: &str = "users.acl.toml";

#[derive(Debug, Serialize, Deserialize)]
struct PersistedAclState {
    version: u32,
    users: Vec<PersistedAclUser>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedAclUser {
    username: String,
    enabled: bool,
    nopass: bool,
    passwords: Vec<String>,
    allow_all_commands: bool,
    allowed_categories: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct AclUser {
    pub enabled: bool,
    pub nopass: bool,
    pub passwords: HashSet<Bytes>,
    pub allow_all_commands: bool,
    pub allowed_categories: HashSet<Bytes>,
}

impl AclUser {
    pub fn default_user() -> Self {
        Self {
            enabled: true,
            nopass: true,
            passwords: HashSet::new(),
            allow_all_commands: true,
            allowed_categories: HashSet::new(),
        }
    }

    pub fn new_disabled() -> Self {
        Self {
            enabled: false,
            nopass: false,
            passwords: HashSet::new(),
            allow_all_commands: false,
            allowed_categories: HashSet::new(),
        }
    }

    pub fn category_allowed(&self, category: &[u8]) -> bool {
        self.allow_all_commands || self.allowed_categories.contains(category)
    }
}

#[derive(Debug, Default)]
pub struct DefaultAclPolicyState {
    default_user_is_nopass_enabled: AtomicBool,
    default_user_has_full_access: AtomicBool,
}

#[derive(Debug)]
pub struct AclState {
    users: HashMap<Bytes, AclUser>,
    log: VecDeque<Bytes>,
}

impl Default for AclState {
    fn default() -> Self {
        let mut users = HashMap::new();
        users.insert(Bytes::from_static(b"default"), AclUser::default_user());
        Self {
            users,
            log: VecDeque::new(),
        }
    }
}

impl AclState {
    pub fn file_path(dir: &Path) -> PathBuf {
        dir.join(DEFAULT_ACL_FILENAME)
    }

    pub fn user_names(&self) -> Vec<Bytes> {
        let mut names = self.users.keys().cloned().collect::<Vec<_>>();
        names.sort();
        names
    }

    pub fn get_user(&self, username: &Bytes) -> Option<&AclUser> {
        self.users.get(username)
    }

    pub fn get_or_create_user_mut(&mut self, username: &Bytes) -> &mut AclUser {
        self.users
            .entry(username.clone())
            .or_insert_with(AclUser::new_disabled)
    }

    pub fn del_users(&mut self, usernames: &[Bytes]) -> i64 {
        let mut removed = 0i64;
        for username in usernames {
            if username.as_ref() == b"default" {
                continue;
            }
            if self.users.remove(username).is_some() {
                removed += 1;
            }
        }
        removed
    }

    pub fn authenticate_user(&self, username: &Bytes, password: &Bytes) -> bool {
        let Some(user) = self.users.get(username) else {
            return false;
        };
        if !user.enabled {
            return false;
        }
        if user.nopass {
            return true;
        }
        if user.passwords.is_empty() {
            return false;
        }

        for stored in &user.passwords {
            if verify_password_hash(stored, password) {
                return true;
            }
        }

        false
    }

    pub fn default_user_is_nopass_enabled(&self) -> bool {
        self.users
            .get(b"default" as &[u8])
            .is_some_and(|user| user.enabled && user.nopass)
    }

    pub fn default_user_has_full_access(&self) -> bool {
        self.users
            .get(b"default" as &[u8])
            .is_some_and(|user| user.enabled && user.allow_all_commands)
    }

    pub fn default_user_is_password_protected(&self) -> bool {
        self.users
            .get(b"default" as &[u8])
            .is_some_and(|user| user.enabled && !user.nopass && !user.passwords.is_empty())
    }

    pub fn command_allowed(&self, username: &Bytes, required_categories: &[&[u8]]) -> bool {
        let Some(user) = self.users.get(username) else {
            return false;
        };
        if !user.enabled {
            return false;
        }
        if user.allow_all_commands {
            return true;
        }

        required_categories
            .iter()
            .all(|category| user.category_allowed(category))
    }

    pub fn command_allowed_mask(&self, username: &Bytes, required_mask: u8) -> bool {
        let Some(user) = self.users.get(username) else {
            return false;
        };
        if !user.enabled {
            return false;
        }
        if user.allow_all_commands || required_mask == 0 {
            return true;
        }

        (required_mask & (1 << 0) == 0 || user.category_allowed(b"admin"))
            && (required_mask & (1 << 1) == 0 || user.category_allowed(b"write"))
            && (required_mask & (1 << 2) == 0 || user.category_allowed(b"read"))
            && (required_mask & (1 << 3) == 0 || user.category_allowed(b"pubsub"))
            && (required_mask & (1 << 4) == 0 || user.category_allowed(b"connection"))
            && (required_mask & (1 << 5) == 0 || user.category_allowed(b"fast"))
    }

    pub fn hash_password(raw_password: &[u8]) -> Option<Bytes> {
        let salt = SaltString::generate(&mut OsRng);
        let hash = Argon2::default()
            .hash_password(raw_password, &salt)
            .ok()?
            .to_string();
        Some(Bytes::from(hash))
    }

    pub fn remove_password(&mut self, username: &Bytes, raw_password: &[u8]) -> bool {
        let Some(user) = self.users.get_mut(username) else {
            return false;
        };

        let mut removed = false;
        let current = user.passwords.iter().cloned().collect::<Vec<_>>();
        for stored in current {
            if verify_password_hash(&stored, raw_password) && user.passwords.remove(&stored) {
                removed = true;
            }
        }

        removed
    }

    pub fn push_log(&mut self, line: Bytes) {
        let sanitized = sanitize_acl_log_line(&String::from_utf8_lossy(&line));
        self.log.push_front(Bytes::from(sanitized));
        while self.log.len() > 128 {
            self.log.pop_back();
        }
    }

    pub fn log(&self, count: usize) -> Vec<Bytes> {
        self.log.iter().take(count).cloned().collect()
    }

    pub fn log_reset(&mut self) {
        self.log.clear();
    }

    pub fn save_to_file(&self, path: &Path) -> io::Result<()> {
        let parent = path.parent().unwrap_or(Path::new("."));
        fs::create_dir_all(parent)?;

        let persisted = self.to_persisted();
        let encoded = toml::to_string_pretty(&persisted).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("serializing ACL state for {}: {error}", path.display()),
            )
        })?;

        let temp_path = path.with_extension("toml.tmp");
        let mut file = fs::File::create(&temp_path)?;
        file.write_all(encoded.as_bytes())?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temp_path, path)?;
        Ok(())
    }

    pub fn load_from_file(path: &Path) -> io::Result<Option<Self>> {
        if !path.exists() {
            return Ok(None);
        }

        let contents = fs::read_to_string(path)?;
        let persisted: PersistedAclState = toml::from_str(&contents).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("parsing ACL state from {}: {error}", path.display()),
            )
        })?;

        Self::from_persisted(persisted).map(Some)
    }

    fn to_persisted(&self) -> PersistedAclState {
        let mut usernames = self.user_names();
        let users = usernames
            .drain(..)
            .filter_map(|username| {
                self.users.get(&username).map(|user| {
                    let mut passwords = user
                        .passwords
                        .iter()
                        .map(|password| String::from_utf8_lossy(password).into_owned())
                        .collect::<Vec<_>>();
                    passwords.sort();

                    let mut allowed_categories = user
                        .allowed_categories
                        .iter()
                        .map(|category| String::from_utf8_lossy(category).to_ascii_lowercase())
                        .collect::<Vec<_>>();
                    allowed_categories.sort();

                    PersistedAclUser {
                        username: String::from_utf8_lossy(&username).into_owned(),
                        enabled: user.enabled,
                        nopass: user.nopass,
                        passwords,
                        allow_all_commands: user.allow_all_commands,
                        allowed_categories,
                    }
                })
            })
            .collect();

        PersistedAclState { version: 1, users }
    }

    fn from_persisted(persisted: PersistedAclState) -> io::Result<Self> {
        if persisted.version != 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported ACL state version {}", persisted.version),
            ));
        }

        let mut users = HashMap::new();
        for user in persisted.users {
            if user.username.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "ACL state contains an empty username",
                ));
            }

            let allowed_categories = user
                .allowed_categories
                .into_iter()
                .map(|category| Bytes::from(category.to_ascii_lowercase()))
                .collect::<HashSet<_>>();
            if !allowed_categories.iter().all(is_valid_acl_category) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "ACL state contains an invalid category for user '{}'",
                        user.username
                    ),
                ));
            }

            let acl_user = AclUser {
                enabled: user.enabled,
                nopass: user.nopass,
                passwords: user
                    .passwords
                    .into_iter()
                    .map(Bytes::from)
                    .collect::<HashSet<_>>(),
                allow_all_commands: user.allow_all_commands,
                allowed_categories,
            };
            users.insert(Bytes::from(user.username), acl_user);
        }

        users
            .entry(Bytes::from_static(b"default"))
            .or_insert_with(AclUser::default_user);

        Ok(Self {
            users,
            log: VecDeque::new(),
        })
    }
}

impl DefaultAclPolicyState {
    pub fn from_acl(acl: &AclState) -> Self {
        let state = Self::default();
        state.refresh_from_acl(acl);
        state
    }

    pub fn refresh_from_acl(&self, acl: &AclState) {
        self.default_user_is_nopass_enabled.store(
            acl.default_user_is_nopass_enabled(),
            AtomicOrdering::Relaxed,
        );
        self.default_user_has_full_access
            .store(acl.default_user_has_full_access(), AtomicOrdering::Relaxed);
    }

    pub fn default_user_is_nopass_enabled(&self) -> bool {
        self.default_user_is_nopass_enabled
            .load(AtomicOrdering::Relaxed)
    }

    pub fn default_user_has_full_access(&self) -> bool {
        self.default_user_has_full_access
            .load(AtomicOrdering::Relaxed)
    }
}

fn verify_password_hash(stored: &Bytes, candidate: &[u8]) -> bool {
    if stored == candidate {
        return true;
    }

    let Ok(hash_str) = std::str::from_utf8(stored) else {
        return false;
    };
    let Ok(parsed) = PasswordHash::new(hash_str) else {
        return false;
    };

    Argon2::default()
        .verify_password(candidate, &parsed)
        .is_ok()
}

fn is_valid_acl_category(category: &Bytes) -> bool {
    category.eq_ignore_ascii_case(b"admin")
        || category.eq_ignore_ascii_case(b"read")
        || category.eq_ignore_ascii_case(b"write")
        || category.eq_ignore_ascii_case(b"pubsub")
        || category.eq_ignore_ascii_case(b"connection")
        || category.eq_ignore_ascii_case(b"fast")
}

#[cfg(test)]
mod tests {
    use super::{AclState, AclUser};
    use bytes::Bytes;
    use std::io;

    #[test]
    fn acl_state_roundtrips_via_file() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = AclState::file_path(dir.path());

        let mut acl = AclState::default();
        let user = acl.get_or_create_user_mut(&Bytes::from_static(b"alice"));
        user.enabled = true;
        user.nopass = false;
        user.passwords
            .insert(Bytes::from_static(b"$argon2id$v=19$m=19456,t=2,p=1$hash"));
        user.allow_all_commands = false;
        user.allowed_categories.insert(Bytes::from_static(b"read"));

        acl.save_to_file(&path)?;
        let loaded = AclState::load_from_file(&path)?.expect("ACL file should exist");

        let default_user = loaded
            .get_user(&Bytes::from_static(b"default"))
            .expect("default user should be preserved");
        assert!(default_user.enabled);

        let alice = loaded
            .get_user(&Bytes::from_static(b"alice"))
            .expect("alice should be restored");
        assert!(alice.enabled);
        assert!(!alice.nopass);
        assert!(
            alice
                .passwords
                .contains(b"$argon2id$v=19$m=19456,t=2,p=1$hash" as &[u8])
        );
        assert!(!alice.allow_all_commands);
        assert!(alice.allowed_categories.contains(b"read" as &[u8]));

        Ok(())
    }

    #[test]
    fn default_user_password_protection_detection_tracks_acl_state() {
        let mut acl = AclState::default();
        assert!(!acl.default_user_is_password_protected());

        let default_user = acl.get_or_create_user_mut(&Bytes::from_static(b"default"));
        *default_user = AclUser::default_user();
        default_user.nopass = false;
        default_user
            .passwords
            .insert(Bytes::from_static(b"$argon2id$v=19$m=19456,t=2,p=1$hash"));

        assert!(acl.default_user_is_password_protected());
    }
}
