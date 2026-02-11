/// Unique identifier for a connected client session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ClientId(pub i64);

impl ClientId {
    pub fn raw(self) -> i64 {
        self.0
    }
}

impl std::fmt::Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Index into the database array (0..15 by default).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DbIndex(pub u16);

impl DbIndex {
    pub fn as_usize(self) -> usize {
        self.0 as usize
    }
}

impl std::fmt::Display for DbIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Hash slot identifier for cluster mode (0..16383).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SlotId(pub u16);

impl SlotId {
    pub fn raw(self) -> u16 {
        self.0
    }
}

impl std::fmt::Display for SlotId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_id_raw_roundtrip() {
        let id = ClientId(42);
        assert_eq!(id.raw(), 42);
        assert_eq!(format!("{id}"), "42");
    }

    #[test]
    fn db_index_as_usize() {
        let idx = DbIndex(3);
        assert_eq!(idx.as_usize(), 3);
    }

    #[test]
    fn slot_id_display() {
        let slot = SlotId(16383);
        assert_eq!(format!("{slot}"), "16383");
    }

    #[test]
    fn newtypes_are_eq_and_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(ClientId(1));
        set.insert(ClientId(2));
        assert!(set.contains(&ClientId(1)));
        assert!(!set.contains(&ClientId(3)));
    }
}
