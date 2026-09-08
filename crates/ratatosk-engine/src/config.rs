use std::path::PathBuf;

use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct ConfigState {
    bind: String,
    port: u16,
    unixsocket: Option<PathBuf>,
    unixsocketperm: u32,
    shm_socket: Option<PathBuf>,
    shm_ring_bytes: u32,
    shm_spin_iters: u32,
    max_clients: usize,
    output_buffer_limit_bytes: usize,
    shutdown_grace_period_ms: u64,
    client_timeout_sec: u64,
    timeout: i64,
    appendonly: bool,
    save: Bytes,
    dir: PathBuf,
    dbfilename: String,
    appendfsync: Bytes,
    maxmemory: usize,
    maxmemory_policy: Bytes,
    maxmemory_samples: usize,
    hz: u32,
    notify_keyspace_events: Bytes,
    lazyfree_lazy_expire: bool,
    lazyfree_lazy_server_del: bool,
    lazyfree_lazy_user_del: bool,
    tcp_keepalive: u32,
    pubsub_queue_hard_limit: usize,
    pubsub_queue_soft_limit: usize,
    pubsub_queue_soft_seconds: u64,
    active_expire_cycle_lookups: usize,
    active_expire_cycle_threshold_pct: u32,
    query_buffer_limit: usize,
    output_buffer_flush_threshold: usize,
    client_write_timeout_sec: u64,
    compatibility_mode: Bytes,
    protected_mode: Bytes,
}

impl Default for ConfigState {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1".to_string(),
            port: 6379,
            unixsocket: None,
            unixsocketperm: 0o700,
            shm_socket: None,
            shm_ring_bytes: 1024 * 1024,
            shm_spin_iters: 2000,
            max_clients: 4096,
            output_buffer_limit_bytes: 8 * 1024 * 1024,
            shutdown_grace_period_ms: 10_000,
            client_timeout_sec: 0,
            timeout: 0,
            appendonly: false,
            save: Bytes::from_static(b"3600 1 300 100 60 10000"),
            dir: PathBuf::from("."),
            dbfilename: "dump.rdb".to_string(),
            appendfsync: Bytes::from_static(b"everysec"),
            maxmemory: 0,
            maxmemory_policy: Bytes::from_static(b"noeviction"),
            maxmemory_samples: 5,
            hz: 10,
            notify_keyspace_events: Bytes::new(),
            lazyfree_lazy_expire: false,
            lazyfree_lazy_server_del: false,
            lazyfree_lazy_user_del: false,
            tcp_keepalive: 300,
            pubsub_queue_hard_limit: 4096,
            pubsub_queue_soft_limit: 2048,
            pubsub_queue_soft_seconds: 60,
            active_expire_cycle_lookups: 20,
            active_expire_cycle_threshold_pct: 25,
            query_buffer_limit: 1_048_576,
            output_buffer_flush_threshold: 16_384,
            client_write_timeout_sec: 5,
            compatibility_mode: Bytes::from_static(b"compat"),
            protected_mode: Bytes::from_static(b"yes"),
        }
    }
}

impl ConfigState {
    pub fn bind(&self) -> &str {
        &self.bind
    }

    pub fn set_bind(&mut self, value: String) {
        self.bind = value;
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn set_port(&mut self, value: u16) {
        self.port = value;
    }

    pub fn unixsocket(&self) -> Option<&PathBuf> {
        self.unixsocket.as_ref()
    }

    pub fn set_unixsocket(&mut self, value: Option<PathBuf>) {
        self.unixsocket = value;
    }

    pub fn unixsocketperm(&self) -> u32 {
        self.unixsocketperm
    }

    pub fn set_unixsocketperm(&mut self, value: u32) {
        self.unixsocketperm = value;
    }

    pub fn shm_socket(&self) -> Option<&PathBuf> {
        self.shm_socket.as_ref()
    }

    pub fn set_shm_socket(&mut self, value: Option<PathBuf>) {
        self.shm_socket = value;
    }

    pub fn shm_ring_bytes(&self) -> u32 {
        self.shm_ring_bytes
    }

    pub fn set_shm_ring_bytes(&mut self, value: u32) {
        self.shm_ring_bytes = value;
    }

    pub fn shm_spin_iters(&self) -> u32 {
        self.shm_spin_iters
    }

    pub fn set_shm_spin_iters(&mut self, value: u32) {
        self.shm_spin_iters = value;
    }

    pub fn max_clients(&self) -> usize {
        self.max_clients
    }

    pub fn set_max_clients(&mut self, value: usize) {
        self.max_clients = value.max(1);
    }

    pub fn output_buffer_limit_bytes(&self) -> usize {
        self.output_buffer_limit_bytes
    }

    pub fn set_output_buffer_limit_bytes(&mut self, value: usize) {
        self.output_buffer_limit_bytes = value.max(1);
    }

    pub fn shutdown_grace_period_ms(&self) -> u64 {
        self.shutdown_grace_period_ms
    }

    pub fn set_shutdown_grace_period_ms(&mut self, value: u64) {
        self.shutdown_grace_period_ms = value.max(1);
    }

    pub fn client_timeout_sec(&self) -> u64 {
        self.client_timeout_sec
    }

    pub fn set_client_timeout_sec(&mut self, value: u64) {
        self.client_timeout_sec = value;
    }

    pub fn timeout(&self) -> i64 {
        self.timeout
    }

    pub fn set_timeout(&mut self, value: i64) {
        self.timeout = value;
    }

    pub fn appendonly(&self) -> bool {
        self.appendonly
    }

    pub fn set_appendonly(&mut self, value: bool) {
        self.appendonly = value;
    }

    pub fn save(&self) -> &Bytes {
        &self.save
    }

    pub fn set_save(&mut self, value: Bytes) {
        self.save = value;
    }

    pub fn dir(&self) -> &PathBuf {
        &self.dir
    }

    pub fn set_dir(&mut self, value: PathBuf) {
        self.dir = value;
    }

    pub fn dbfilename(&self) -> &str {
        &self.dbfilename
    }

    pub fn set_dbfilename(&mut self, value: String) {
        self.dbfilename = value;
    }

    pub fn appendfsync(&self) -> &Bytes {
        &self.appendfsync
    }

    pub fn set_appendfsync(&mut self, value: Bytes) {
        self.appendfsync = value;
    }

    pub fn maxmemory(&self) -> usize {
        self.maxmemory
    }

    pub fn set_maxmemory(&mut self, value: usize) {
        self.maxmemory = value;
    }

    pub fn maxmemory_policy(&self) -> &Bytes {
        &self.maxmemory_policy
    }

    pub fn set_maxmemory_policy(&mut self, value: Bytes) {
        self.maxmemory_policy = value;
    }

    pub fn maxmemory_samples(&self) -> usize {
        self.maxmemory_samples
    }

    pub fn set_maxmemory_samples(&mut self, value: usize) {
        self.maxmemory_samples = value;
    }

    pub fn hz(&self) -> u32 {
        self.hz
    }

    pub fn set_hz(&mut self, value: u32) {
        self.hz = value.clamp(1, 500);
    }

    pub fn notify_keyspace_events(&self) -> &Bytes {
        &self.notify_keyspace_events
    }

    pub fn set_notify_keyspace_events(&mut self, value: Bytes) {
        self.notify_keyspace_events = value;
    }

    pub fn lazyfree_lazy_expire(&self) -> bool {
        self.lazyfree_lazy_expire
    }

    pub fn set_lazyfree_lazy_expire(&mut self, value: bool) {
        self.lazyfree_lazy_expire = value;
    }

    pub fn lazyfree_lazy_server_del(&self) -> bool {
        self.lazyfree_lazy_server_del
    }

    pub fn set_lazyfree_lazy_server_del(&mut self, value: bool) {
        self.lazyfree_lazy_server_del = value;
    }

    pub fn lazyfree_lazy_user_del(&self) -> bool {
        self.lazyfree_lazy_user_del
    }

    pub fn set_lazyfree_lazy_user_del(&mut self, value: bool) {
        self.lazyfree_lazy_user_del = value;
    }

    pub fn tcp_keepalive(&self) -> u32 {
        self.tcp_keepalive
    }

    pub fn set_tcp_keepalive(&mut self, value: u32) {
        self.tcp_keepalive = value;
    }

    pub fn pubsub_queue_hard_limit(&self) -> usize {
        self.pubsub_queue_hard_limit
    }

    pub fn set_pubsub_queue_hard_limit(&mut self, value: usize) {
        self.pubsub_queue_hard_limit = value;
    }

    pub fn pubsub_queue_soft_limit(&self) -> usize {
        self.pubsub_queue_soft_limit
    }

    pub fn set_pubsub_queue_soft_limit(&mut self, value: usize) {
        self.pubsub_queue_soft_limit = value;
    }

    pub fn pubsub_queue_soft_seconds(&self) -> u64 {
        self.pubsub_queue_soft_seconds
    }

    pub fn set_pubsub_queue_soft_seconds(&mut self, value: u64) {
        self.pubsub_queue_soft_seconds = value;
    }

    pub fn active_expire_cycle_lookups(&self) -> usize {
        self.active_expire_cycle_lookups
    }

    pub fn set_active_expire_cycle_lookups(&mut self, value: usize) {
        self.active_expire_cycle_lookups = value.clamp(1, 1000);
    }

    pub fn active_expire_cycle_threshold_pct(&self) -> u32 {
        self.active_expire_cycle_threshold_pct
    }

    pub fn set_active_expire_cycle_threshold_pct(&mut self, value: u32) {
        self.active_expire_cycle_threshold_pct = value.clamp(1, 100);
    }

    pub fn query_buffer_limit(&self) -> usize {
        self.query_buffer_limit
    }

    pub fn set_query_buffer_limit(&mut self, value: usize) {
        self.query_buffer_limit = value.clamp(1024, 1_073_741_824);
    }

    pub fn output_buffer_flush_threshold(&self) -> usize {
        self.output_buffer_flush_threshold
    }

    pub fn set_output_buffer_flush_threshold(&mut self, value: usize) {
        self.output_buffer_flush_threshold = value.clamp(1024, 134_217_728);
    }

    pub fn client_write_timeout_sec(&self) -> u64 {
        self.client_write_timeout_sec
    }

    pub fn set_client_write_timeout_sec(&mut self, value: u64) {
        self.client_write_timeout_sec = value.clamp(1, 3600);
    }

    pub fn compatibility_mode(&self) -> &Bytes {
        &self.compatibility_mode
    }

    pub fn set_compatibility_mode(&mut self, value: Bytes) {
        self.compatibility_mode = value;
    }

    /// True when the server runs in strict compatibility mode.
    ///
    /// In strict mode, commands that Ratatosk only accepts syntactically
    /// (`syntax_only`/`unsupported` capability tier) or that imply a durability
    /// or replication guarantee a single-node server cannot honour (`WAIT`,
    /// `WAITAOF`) are rejected with a structured error instead of silently
    /// returning a success-shaped reply. Default mode is `compat`.
    pub fn is_strict_compatibility(&self) -> bool {
        self.compatibility_mode.as_ref() == b"strict"
    }

    pub fn protected_mode(&self) -> &Bytes {
        &self.protected_mode
    }

    pub fn set_protected_mode(&mut self, value: Bytes) {
        self.protected_mode = value;
    }

    /// True when protected mode is enabled (the default).
    ///
    /// In protected mode, a non-loopback bind refuses to start while the
    /// `default` ACL user is still `nopass`, unless an operator supplies a
    /// password (`RATATOSK_DEFAULT_USER_PASSWORD`/`_HASH`) or explicitly opts
    /// out (`protected-mode no` / `RATATOSK_ALLOW_DEFAULT_USER_NOPASS=true`).
    /// This keeps small exposed deployments from silently accepting
    /// unauthenticated traffic. Default mode is `yes`.
    pub fn is_protected_mode(&self) -> bool {
        self.protected_mode.as_ref().eq_ignore_ascii_case(b"yes")
    }
}
