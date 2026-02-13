use std::{collections::VecDeque, sync::OnceLock};

use parking_lot::Mutex;
use serde::Serialize;

const DEFAULT_MAX_BREADCRUMBS: usize = 256;

#[derive(Debug, Clone, Serialize)]
pub struct CommandBreadcrumb {
    pub timestamp_ms: i64,
    pub client_id: i64,
    pub command: String,
    pub selected_db: usize,
    pub retries: u64,
    pub phase: String,
}

static BREADCRUMBS: OnceLock<Mutex<VecDeque<CommandBreadcrumb>>> = OnceLock::new();

fn store() -> &'static Mutex<VecDeque<CommandBreadcrumb>> {
    BREADCRUMBS.get_or_init(|| Mutex::new(VecDeque::with_capacity(DEFAULT_MAX_BREADCRUMBS)))
}

pub fn record_command(
    client_id: i64,
    command: &str,
    selected_db: usize,
    retries: u64,
    phase: &str,
) {
    let mut breadcrumbs = store().lock();
    breadcrumbs.push_back(CommandBreadcrumb {
        timestamp_ms: ratatosk_core::time::now_ms(),
        client_id,
        command: command.to_string(),
        selected_db,
        retries,
        phase: phase.to_string(),
    });

    while breadcrumbs.len() > DEFAULT_MAX_BREADCRUMBS {
        breadcrumbs.pop_front();
    }
}

pub fn snapshot(limit: usize) -> Vec<CommandBreadcrumb> {
    let breadcrumbs = store().lock();
    let cap = if limit == 0 {
        DEFAULT_MAX_BREADCRUMBS
    } else {
        limit.min(DEFAULT_MAX_BREADCRUMBS)
    };

    breadcrumbs
        .iter()
        .rev()
        .take(cap)
        .cloned()
        .collect::<Vec<_>>()
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_MAX_BREADCRUMBS, record_command, snapshot};

    #[test]
    fn breadcrumbs_snapshot_returns_recent_entries() {
        record_command(10, "PING", 0, 0, "execute");
        record_command(10, "SET", 0, 0, "execute");

        let rows = snapshot(2);
        assert!(!rows.is_empty());
        assert!(rows.len() <= 2);
    }

    #[test]
    fn breadcrumbs_are_bounded() {
        for idx in 0..(DEFAULT_MAX_BREADCRUMBS + 8) {
            record_command(idx as i64, "SET", 0, 0, "execute");
        }

        let rows = snapshot(DEFAULT_MAX_BREADCRUMBS + 100);
        assert_eq!(rows.len(), DEFAULT_MAX_BREADCRUMBS);
    }
}
