/// Bitmask flags describing command properties.
///
/// Each command in the command table carries a set of these flags
/// to control dispatch behavior, ACL checks, and replication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CommandFlags(pub u64);

impl CommandFlags {
    pub const NONE: Self = Self(0);
    pub const WRITE: Self = Self(1 << 0);
    pub const READONLY: Self = Self(1 << 1);
    pub const DENYOOM: Self = Self(1 << 2);
    pub const ADMIN: Self = Self(1 << 3);
    pub const PUBSUB: Self = Self(1 << 4);
    pub const BLOCKING: Self = Self(1 << 5);
    pub const FAST: Self = Self(1 << 6);
    pub const LOADING: Self = Self(1 << 7);
    pub const STALE: Self = Self(1 << 8);
    pub const SORT_FOR_SCRIPT: Self = Self(1 << 9);
    pub const NO_MULTI: Self = Self(1 << 10);
    pub const MOVABLE_KEYS: Self = Self(1 << 11);
    pub const ALLOW_BUSY: Self = Self(1 << 12);
    pub const NO_AUTH: Self = Self(1 << 13);

    pub fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    pub fn is_empty(self) -> bool {
        self.0 == 0
    }
}

impl std::ops::BitOr for CommandFlags {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

impl std::ops::BitAnd for CommandFlags {
    type Output = Self;
    fn bitand(self, rhs: Self) -> Self {
        Self(self.0 & rhs.0)
    }
}

/// Bitmask for ACL permission categories.
///
/// Used to gate command execution based on the authenticated user's
/// allowed categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AclCategory(pub u64);

impl AclCategory {
    pub const NONE: Self = Self(0);
    pub const KEYSPACE: Self = Self(1 << 0);
    pub const READ: Self = Self(1 << 1);
    pub const WRITE: Self = Self(1 << 2);
    pub const SET: Self = Self(1 << 3);
    pub const SORTEDSET: Self = Self(1 << 4);
    pub const LIST: Self = Self(1 << 5);
    pub const HASH: Self = Self(1 << 6);
    pub const STRING: Self = Self(1 << 7);
    pub const BITMAP: Self = Self(1 << 8);
    pub const HYPERLOGLOG: Self = Self(1 << 9);
    pub const GEO: Self = Self(1 << 10);
    pub const STREAM: Self = Self(1 << 11);
    pub const PUBSUB: Self = Self(1 << 12);
    pub const ADMIN: Self = Self(1 << 13);
    pub const FAST: Self = Self(1 << 14);
    pub const SLOW: Self = Self(1 << 15);
    pub const BLOCKING: Self = Self(1 << 16);
    pub const DANGEROUS: Self = Self(1 << 17);
    pub const CONNECTION: Self = Self(1 << 18);
    pub const TRANSACTION: Self = Self(1 << 19);
    pub const SCRIPTING: Self = Self(1 << 20);

    pub fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

impl std::ops::BitOr for AclCategory {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

impl std::ops::BitAnd for AclCategory {
    type Output = Self;
    fn bitand(self, rhs: Self) -> Self {
        Self(self.0 & rhs.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_flags_compose() {
        let flags = CommandFlags::WRITE | CommandFlags::DENYOOM;
        assert!(flags.contains(CommandFlags::WRITE));
        assert!(flags.contains(CommandFlags::DENYOOM));
        assert!(!flags.contains(CommandFlags::ADMIN));
    }

    #[test]
    fn command_flags_empty() {
        assert!(CommandFlags::NONE.is_empty());
        assert!(!(CommandFlags::WRITE).is_empty());
    }

    #[test]
    fn acl_category_compose() {
        let cat = AclCategory::READ | AclCategory::WRITE;
        assert!(cat.contains(AclCategory::READ));
        assert!(cat.contains(AclCategory::WRITE));
        assert!(!cat.contains(AclCategory::ADMIN));
    }
}
