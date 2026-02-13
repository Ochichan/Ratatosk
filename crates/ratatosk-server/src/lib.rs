//! # ratatosk-server
//!
//! TCP server and event loop — the "nervous system" of Ratatosk.
//!
//! - [`event_loop`] — Main `tokio::select!` loop: TCP accept, server_cron timer
//!   (active expiry + eviction), signal handling (SIGINT/SIGTERM/SIGUSR1),
//!   and lazy-free background thread management.
//! - [`client`] — Per-client I/O task: read → parse → execute → write pipeline
//!   with query buffer limits, output buffer limits, and pub/sub message drain.
//! - [`config`] — Server configuration from environment variables.
//! - [`io_thread`] — I/O thread pool (placeholder for future parallel I/O).

pub mod client;
pub mod config;
pub mod event_loop;
pub mod io_thread;
pub mod persistence;

#[cfg(test)]
mod tests {
    #[test]
    fn test_version_format() {
        let version = env!("CARGO_PKG_VERSION");
        let git_hash = env!("GIT_HASH");

        assert!(!version.is_empty(), "version should not be empty");
        assert!(!git_hash.is_empty(), "git_hash should not be empty");

        let version_string = format!("{} ({})", version, git_hash);
        assert!(version_string.contains("("), "version string should contain git hash in parentheses");
        assert!(version_string.contains(")"), "version string should contain closing parenthesis");
    }
}
