use std::{io, path::Path, sync::Arc};

use ratatosk_core::time::now_ms;
use ratatosk_engine::keyspace::SharedState;
use ratatosk_persist::rdb;

pub async fn run_save(server_state: &Arc<SharedState>, rdb_path: &Path) -> io::Result<()> {
    let snapshot = {
        let state = server_state.meta.lock().await;
        if state.rdb_save_in_progress() {
            return Err(io::Error::other("background save already in progress"));
        }
        state.snapshot_dbs()
    };

    crate::metrics::record_rdb_save(false);

    let result = rdb::saver::save(&snapshot, rdb_path);
    let mut state = server_state.meta.lock().await;
    match &result {
        Ok(()) => {
            state.stats.mark_last_save_now();
            state.set_last_rdb_save_time_ms(now_ms());
            state.set_last_rdb_save_status(Ok(()));
        }
        Err(error) => {
            state.set_last_rdb_save_status(Err(error.to_string()));
            crate::metrics::record_rdb_save_error();
        }
    }
    result
}
