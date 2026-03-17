use argon2::Argon2;
use argon2::password_hash::{
    PasswordHash, PasswordHasher, PasswordVerifier, SaltString, rand_core::OsRng,
};
use bytes::Bytes;
use hashbrown::{HashMap, HashSet};
use std::collections::VecDeque;

use crate::security::sanitize_acl_log_line;

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
