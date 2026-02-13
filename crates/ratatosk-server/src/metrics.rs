//! Metrics infrastructure for Ratatosk.
//!
//! Provides Prometheus-compatible metrics export on a configurable port.

use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;

/// Initialize the metrics system with Prometheus exporter.
///
/// # Arguments
/// * `bind_addr` - The address to bind the metrics HTTP server to
///
/// # Returns
/// * `Ok(())` if the exporter was successfully installed
/// * `Err` if the exporter could not be installed (e.g., port in use)
pub fn init_metrics(bind_addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = bind_addr.parse()?;

    PrometheusBuilder::new()
        .with_http_listener(addr)
        .install_recorder()?;

    tracing::info!(
        target = "ratatosk::metrics",
        bind_addr = %bind_addr,
        "Prometheus metrics exporter started"
    );

    Ok(())
}

/// Initialize metrics with default localhost binding.
pub fn init_metrics_default() -> Result<(), Box<dyn std::error::Error>> {
    init_metrics("127.0.0.1:9090")
}

/// Record a command execution metric.
#[inline]
pub fn record_command(command: &str, success: bool, duration_secs: f64) {
    let status = if success { "success" } else { "error" };
    
    metrics::counter!("ratatosk_commands_total", "command" => command.to_string(), "status" => status.to_string())
        .increment(1);
    
    metrics::histogram!("ratatosk_command_duration_seconds", "command" => command.to_string())
        .record(duration_secs);
}

/// Record connection metrics.
#[inline]
pub fn record_connection_event(event: &str) {
    metrics::counter!("ratatosk_connections_total", "event" => event.to_string()).increment(1);
}

#[inline]
pub fn set_active_connections(count: usize) {
    metrics::gauge!("ratatosk_connections_active").set(count as f64);
}

/// Record memory metrics.
#[inline]
pub fn set_memory_used(bytes: u64) {
    metrics::gauge!("ratatosk_memory_used_bytes").set(bytes as f64);
}

#[inline]
pub fn record_eviction_keys(count: u64, policy: &str) {
    metrics::counter!("ratatosk_eviction_keys_total", "policy" => policy.to_string())
        .increment(count);
}

/// Record persistence metrics.
#[inline]
pub fn record_aof_write(fsync_policy: &str) {
    metrics::counter!("ratatosk_aof_writes_total", "fsync" => fsync_policy.to_string())
        .increment(1);
}

#[inline]
pub fn record_aof_write_error() {
    metrics::counter!("ratatosk_aof_write_errors_total").increment(1);
}

#[inline]
pub fn record_rdb_save(background: bool) {
    let save_type = if background { "background" } else { "foreground" };
    metrics::counter!("ratatosk_rdb_saves_total", "type" => save_type.to_string())
        .increment(1);
}

#[inline]
pub fn record_rdb_save_error() {
    metrics::counter!("ratatosk_rdb_save_errors_total").increment(1);
}

/// Record PubSub metrics.
#[inline]
pub fn record_pubsub_message_dropped(reason: &str) {
    metrics::counter!("ratatosk_pubsub_messages_dropped_total", "reason" => reason.to_string())
        .increment(1);
}

#[inline]
pub fn record_pubsub_client_overflowed() {
    metrics::counter!("ratatosk_pubsub_clients_overflowed_total").increment(1);
}

#[inline]
pub fn set_pubsub_pending_queue_size(client_id: i64, size: usize) {
    metrics::gauge!("ratatosk_pubsub_pending_queue_size", "client_id" => client_id.to_string())
        .set(size as f64);
}

/// Record authentication metrics.
#[inline]
pub fn record_auth_attempt(success: bool) {
    let result = if success { "success" } else { "failure" };
    metrics::counter!("ratatosk_auth_attempts_total", "result" => result.to_string())
        .increment(1);
}

/// Record clock jump detection.
#[inline]
pub fn record_clock_jump(direction: &str) {
    metrics::counter!("ratatosk_clock_jumps_total", "direction" => direction.to_string())
        .increment(1);
}

/// Record shutdown metrics.
#[inline]
pub fn record_shutdown_clients_aborted(count: u64) {
    metrics::counter!("ratatosk_shutdown_clients_aborted_total").increment(count);
}

/// Record rate-limited connection attempts.
#[inline]
pub fn record_rate_limited_connection() {
    metrics::counter!("ratatosk_connections_rate_limited_total").increment(1);
}

/// Set lazy-free channel utilization (0.0 to 1.0).
#[inline]
pub fn set_lazyfree_queue_utilization(utilization: f64) {
    metrics::gauge!("ratatosk_lazyfree_queue_utilization").set(utilization);
}
