use std::{
    env,
    fmt::Write as _,
    fs, io,
    path::{Path, PathBuf},
};

use ratatosk_engine::eviction::EvictionPolicy;
use serde::Serialize;
use thiserror::Error;

pub const DEFAULT_CONFIG_FILENAME: &str = "ratatosk.conf";
pub const DEFAULT_MAX_CLIENTS: usize = 4096;
pub const DEFAULT_OUTPUT_BUFFER_LIMIT_BYTES: usize = 8 * 1024 * 1024;
pub const DEFAULT_SHUTDOWN_GRACE_PERIOD_MS: u64 = 10_000;
pub const DEFAULT_CLIENT_TIMEOUT_SEC: u64 = 0;
pub const DEFAULT_DIR: &str = ".";
pub const DEFAULT_DBFILENAME: &str = "dump.rdb";
pub const DEFAULT_APPENDONLY: bool = false;
pub const DEFAULT_APPENDFSYNC: &str = "everysec";
pub const DEFAULT_COMPATIBILITY_MODE: &str = "compat";
pub const DEFAULT_PROTECTED_MODE: &str = "yes";
pub const DEFAULT_TIMEOUT: i64 = 0;
pub const DEFAULT_HZ: u32 = 10;
pub const DEFAULT_SAVE: &str = "3600 1 300 100 60 10000";
pub const DEFAULT_MAXMEMORY: usize = 0;
pub const DEFAULT_MAXMEMORY_POLICY: &str = "noeviction";
pub const DEFAULT_MAXMEMORY_SAMPLES: usize = 5;
pub const DEFAULT_NOTIFY_KEYSPACE_EVENTS: &str = "";
pub const DEFAULT_LAZYFREE_LAZY_EXPIRE: bool = false;
pub const DEFAULT_LAZYFREE_LAZY_SERVER_DEL: bool = false;
pub const DEFAULT_LAZYFREE_LAZY_USER_DEL: bool = false;
pub const DEFAULT_TCP_KEEPALIVE_SEC: u32 = 300;
pub const DEFAULT_PUBSUB_QUEUE_HARD_LIMIT: usize = 4096;
pub const DEFAULT_PUBSUB_QUEUE_SOFT_LIMIT: usize = 2048;
pub const DEFAULT_PUBSUB_QUEUE_SOFT_SECONDS: u64 = 60;
pub const DEFAULT_ACTIVE_EXPIRE_CYCLE_LOOKUPS: usize = 20;
pub const DEFAULT_ACTIVE_EXPIRE_CYCLE_THRESHOLD_PCT: u32 = 25;
pub const DEFAULT_QUERY_BUFFER_LIMIT: usize = 1_048_576;
pub const DEFAULT_OUTPUT_BUFFER_FLUSH_THRESHOLD: usize = 16_384;
pub const DEFAULT_CLIENT_WRITE_TIMEOUT_SEC: u64 = 5;
pub const DEFAULT_SLOWLOG_LOG_SLOWER_THAN_US: i64 = -1;
pub const DEFAULT_SLOWLOG_MAX_LEN: usize = 128;
pub const DEFAULT_LATENCY_TRACKING: bool = false;

const APPENDFSYNC_VALUES: &[&str] = &["always", "everysec", "no"];
const COMPATIBILITY_MODE_VALUES: &[&str] = &["compat", "strict"];
const PROTECTED_MODE_VALUES: &[&str] = &["yes", "no"];

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigFileSource {
    Cli,
    Env,
    Auto,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedConfig {
    pub config: ServerConfig,
    pub config_path: Option<PathBuf>,
    pub config_file_source: Option<ConfigFileSource>,
    pub auto_config_discovery_enabled: bool,
}

impl LoadedConfig {
    pub fn load(config_path: Option<&Path>) -> Result<Self, ConfigError> {
        Self::load_with_options(config_path, true)
    }

    pub fn load_with_options(
        config_path: Option<&Path>,
        auto_config_discovery_enabled: bool,
    ) -> Result<Self, ConfigError> {
        let (config_path, config_file_source) =
            resolve_config_path(config_path, auto_config_discovery_enabled);
        let mut config = ServerConfig::default();

        if let Some(path) = config_path.as_deref() {
            apply_config_file(&mut config, path)?;
        }

        apply_env_overrides(&mut config)?;
        validate_bind_security(&config)?;

        Ok(Self {
            config,
            config_path,
            config_file_source,
            auto_config_discovery_enabled,
        })
    }

    pub fn source_description(&self) -> String {
        match (&self.config_file_source, &self.config_path) {
            (Some(ConfigFileSource::Cli), Some(path)) => {
                format!("--config {} + environment overrides", path.display())
            }
            (Some(ConfigFileSource::Env), Some(path)) => {
                format!("RATATOSK_CONFIG={} + environment overrides", path.display())
            }
            (Some(ConfigFileSource::Auto), Some(path)) => {
                format!("auto-loaded {} + environment overrides", path.display())
            }
            _ if self.auto_config_discovery_enabled => {
                "defaults + environment overrides".to_string()
            }
            _ => "defaults + environment overrides (auto config discovery disabled)".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ServerConfig {
    pub bind: String,
    pub port: u16,
    pub max_clients: usize,
    pub output_buffer_limit_bytes: usize,
    pub shutdown_grace_period_ms: u64,
    pub client_timeout_sec: u64,
    /// Working directory for persistence files (RDB, AOF).
    pub dir: PathBuf,
    /// RDB snapshot filename.
    pub dbfilename: String,
    /// Whether append-only file persistence is enabled.
    pub appendonly: bool,
    /// fsync policy for AOF: "always", "everysec", or "no".
    pub appendfsync: String,
    /// Redis compatibility strictness: "compat" (default) or "strict". In strict
    /// mode, commands Ratatosk only accepts syntactically or whose Redis
    /// durability/replication contract a single node cannot honour are rejected.
    pub compatibility_mode: String,
    /// Protected mode: "yes" (default) or "no". When enabled, a non-loopback bind
    /// refuses to start while the `default` ACL user is still `nopass`, unless a
    /// bootstrap password is supplied or the operator explicitly opts out.
    pub protected_mode: String,
    pub timeout: i64,
    pub hz: u32,
    pub save: String,
    pub maxmemory: usize,
    pub maxmemory_policy: String,
    pub maxmemory_samples: usize,
    pub notify_keyspace_events: String,
    pub lazyfree_lazy_expire: bool,
    pub lazyfree_lazy_server_del: bool,
    pub lazyfree_lazy_user_del: bool,
    pub tcp_keepalive_sec: u32,
    pub pubsub_queue_hard_limit: usize,
    pub pubsub_queue_soft_limit: usize,
    pub pubsub_queue_soft_seconds: u64,
    pub active_expire_cycle_lookups: usize,
    pub active_expire_cycle_threshold_pct: u32,
    pub query_buffer_limit: usize,
    pub output_buffer_flush_threshold: usize,
    pub client_write_timeout_sec: u64,
    pub slowlog_log_slower_than_us: i64,
    pub slowlog_max_len: usize,
    pub latency_tracking: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1".to_string(),
            port: 6379,
            max_clients: DEFAULT_MAX_CLIENTS,
            output_buffer_limit_bytes: DEFAULT_OUTPUT_BUFFER_LIMIT_BYTES,
            shutdown_grace_period_ms: DEFAULT_SHUTDOWN_GRACE_PERIOD_MS,
            client_timeout_sec: DEFAULT_CLIENT_TIMEOUT_SEC,
            dir: PathBuf::from(DEFAULT_DIR),
            dbfilename: DEFAULT_DBFILENAME.to_string(),
            appendonly: DEFAULT_APPENDONLY,
            appendfsync: DEFAULT_APPENDFSYNC.to_string(),
            compatibility_mode: DEFAULT_COMPATIBILITY_MODE.to_string(),
            protected_mode: DEFAULT_PROTECTED_MODE.to_string(),
            timeout: DEFAULT_TIMEOUT,
            hz: DEFAULT_HZ,
            save: DEFAULT_SAVE.to_string(),
            maxmemory: DEFAULT_MAXMEMORY,
            maxmemory_policy: DEFAULT_MAXMEMORY_POLICY.to_string(),
            maxmemory_samples: DEFAULT_MAXMEMORY_SAMPLES,
            notify_keyspace_events: DEFAULT_NOTIFY_KEYSPACE_EVENTS.to_string(),
            lazyfree_lazy_expire: DEFAULT_LAZYFREE_LAZY_EXPIRE,
            lazyfree_lazy_server_del: DEFAULT_LAZYFREE_LAZY_SERVER_DEL,
            lazyfree_lazy_user_del: DEFAULT_LAZYFREE_LAZY_USER_DEL,
            tcp_keepalive_sec: DEFAULT_TCP_KEEPALIVE_SEC,
            pubsub_queue_hard_limit: DEFAULT_PUBSUB_QUEUE_HARD_LIMIT,
            pubsub_queue_soft_limit: DEFAULT_PUBSUB_QUEUE_SOFT_LIMIT,
            pubsub_queue_soft_seconds: DEFAULT_PUBSUB_QUEUE_SOFT_SECONDS,
            active_expire_cycle_lookups: DEFAULT_ACTIVE_EXPIRE_CYCLE_LOOKUPS,
            active_expire_cycle_threshold_pct: DEFAULT_ACTIVE_EXPIRE_CYCLE_THRESHOLD_PCT,
            query_buffer_limit: DEFAULT_QUERY_BUFFER_LIMIT,
            output_buffer_flush_threshold: DEFAULT_OUTPUT_BUFFER_FLUSH_THRESHOLD,
            client_write_timeout_sec: DEFAULT_CLIENT_WRITE_TIMEOUT_SEC,
            slowlog_log_slower_than_us: DEFAULT_SLOWLOG_LOG_SLOWER_THAN_US,
            slowlog_max_len: DEFAULT_SLOWLOG_MAX_LEN,
            latency_tracking: DEFAULT_LATENCY_TRACKING,
        }
    }
}

impl ServerConfig {
    pub fn load(config_path: Option<&Path>) -> Result<LoadedConfig, ConfigError> {
        LoadedConfig::load(config_path)
    }

    pub fn load_with_options(
        config_path: Option<&Path>,
        auto_config_discovery_enabled: bool,
    ) -> Result<LoadedConfig, ConfigError> {
        LoadedConfig::load_with_options(config_path, auto_config_discovery_enabled)
    }

    pub fn from_env() -> Result<Self, ConfigError> {
        let auto_config_discovery_enabled = !env_truthy("RATATOSK_DISABLE_CONFIG_AUTOLOAD");
        Self::load_with_options(None, auto_config_discovery_enabled).map(|loaded| loaded.config)
    }

    pub fn listen_addr(&self) -> String {
        format!("{}:{}", self.bind, self.port)
    }

    pub fn binds_to_loopback(&self) -> bool {
        is_loopback_bind(&self.bind)
    }

    /// True when protected mode is enabled (the default). When enabled, a
    /// non-loopback bind refuses to start with a `nopass` default ACL user
    /// unless a bootstrap password or explicit opt-out is supplied.
    pub fn protected_mode_enabled(&self) -> bool {
        self.protected_mode.eq_ignore_ascii_case("yes")
    }

    pub fn render_redis_config(&self) -> String {
        let mut out = String::new();
        writeln!(&mut out, "# Ratatosk configuration file").expect("write config");
        writeln!(
            &mut out,
            "# Generated from the effective startup configuration"
        )
        .expect("write config");
        writeln!(&mut out).expect("write config");

        writeln!(&mut out, "# Network").expect("write config");
        write_scalar(&mut out, "bind", &self.bind);
        write_number(&mut out, "port", self.port);
        write_number(&mut out, "maxclients", self.max_clients);
        write_number(&mut out, "timeout", self.timeout);
        write_number(&mut out, "client-timeout-sec", self.client_timeout_sec);
        write_number(
            &mut out,
            "output-buffer-limit-bytes",
            self.output_buffer_limit_bytes,
        );
        write_number(&mut out, "shutdown-grace-ms", self.shutdown_grace_period_ms);
        write_scalar(&mut out, "compatibility-mode", &self.compatibility_mode);
        write_scalar(&mut out, "protected-mode", &self.protected_mode);
        writeln!(&mut out).expect("write config");

        writeln!(&mut out, "# Persistence").expect("write config");
        write_scalar(&mut out, "dir", &self.dir.display().to_string());
        write_scalar(&mut out, "dbfilename", &self.dbfilename);
        write_bool(&mut out, "appendonly", self.appendonly);
        write_scalar(&mut out, "appendfsync", &self.appendfsync);
        write_raw(&mut out, "save", &self.save);
        writeln!(&mut out).expect("write config");

        writeln!(&mut out, "# Memory Management").expect("write config");
        write_number(&mut out, "maxmemory", self.maxmemory);
        write_scalar(&mut out, "maxmemory-policy", &self.maxmemory_policy);
        write_number(&mut out, "maxmemory-samples", self.maxmemory_samples);
        write_bool(&mut out, "lazyfree-lazy-expire", self.lazyfree_lazy_expire);
        write_bool(
            &mut out,
            "lazyfree-lazy-server-del",
            self.lazyfree_lazy_server_del,
        );
        write_bool(
            &mut out,
            "lazyfree-lazy-user-del",
            self.lazyfree_lazy_user_del,
        );
        writeln!(&mut out).expect("write config");

        writeln!(&mut out, "# Runtime").expect("write config");
        write_number(&mut out, "hz", self.hz);
        write_number(
            &mut out,
            "active-expire-cycle-lookups",
            self.active_expire_cycle_lookups,
        );
        write_number(
            &mut out,
            "active-expire-cycle-threshold-pct",
            self.active_expire_cycle_threshold_pct,
        );
        write_number(&mut out, "query-buffer-limit", self.query_buffer_limit);
        write_number(
            &mut out,
            "output-buffer-flush-threshold",
            self.output_buffer_flush_threshold,
        );
        write_number(
            &mut out,
            "client-write-timeout-sec",
            self.client_write_timeout_sec,
        );
        write_number(&mut out, "tcp-keepalive", self.tcp_keepalive_sec);
        writeln!(&mut out).expect("write config");

        writeln!(&mut out, "# Pub/Sub And Notifications").expect("write config");
        write_scalar(
            &mut out,
            "notify-keyspace-events",
            &self.notify_keyspace_events,
        );
        write_number(
            &mut out,
            "pubsub-queue-hard-limit",
            self.pubsub_queue_hard_limit,
        );
        write_number(
            &mut out,
            "pubsub-queue-soft-limit",
            self.pubsub_queue_soft_limit,
        );
        write_number(
            &mut out,
            "pubsub-queue-soft-seconds",
            self.pubsub_queue_soft_seconds,
        );
        writeln!(&mut out).expect("write config");

        writeln!(&mut out, "# Slowlog And Latency").expect("write config");
        write_number(
            &mut out,
            "slowlog-log-slower-than",
            self.slowlog_log_slower_than_us,
        );
        write_number(&mut out, "slowlog-max-len", self.slowlog_max_len);
        write_bool(&mut out, "latency-tracking", self.latency_tracking);

        out
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("reading config file {path}: {source}")]
    ReadConfigFile { path: PathBuf, source: io::Error },

    #[error("parsing config file {path}:{line}: {message}")]
    ParseConfigFile {
        path: PathBuf,
        line: usize,
        message: String,
    },

    #[error("invalid environment override {name}: {message}")]
    InvalidEnvironmentOverride { name: String, message: String },

    #[error(
        "non-loopback RATATOSK_BIND '{value}' requires RATATOSK_ALLOW_INSECURE_BIND=true (or enable a TLS proxy)"
    )]
    InsecureBindRequiresOptIn { value: String },
}

fn resolve_config_path(
    config_path: Option<&Path>,
    auto_config_discovery_enabled: bool,
) -> (Option<PathBuf>, Option<ConfigFileSource>) {
    if let Some(path) = config_path {
        return (Some(path.to_path_buf()), Some(ConfigFileSource::Cli));
    }

    if let Some(path) = env::var_os("RATATOSK_CONFIG") {
        return (Some(PathBuf::from(path)), Some(ConfigFileSource::Env));
    }

    if auto_config_discovery_enabled {
        let default_path = PathBuf::from(DEFAULT_CONFIG_FILENAME);
        if default_path.is_file() {
            return (Some(default_path), Some(ConfigFileSource::Auto));
        }
    }

    (None, None)
}

fn apply_config_file(config: &mut ServerConfig, path: &Path) -> Result<(), ConfigError> {
    let contents = fs::read_to_string(path).map_err(|source| ConfigError::ReadConfigFile {
        path: path.to_path_buf(),
        source,
    })?;

    for (line_no, line) in contents.lines().enumerate() {
        let tokens =
            tokenize_config_line(line).map_err(|message| ConfigError::ParseConfigFile {
                path: path.to_path_buf(),
                line: line_no + 1,
                message,
            })?;
        if tokens.is_empty() {
            continue;
        }

        let directive = tokens[0].to_ascii_lowercase();
        apply_directive(config, &directive, &tokens[1..]).map_err(|message| {
            ConfigError::ParseConfigFile {
                path: path.to_path_buf(),
                line: line_no + 1,
                message,
            }
        })?;
    }

    Ok(())
}

fn apply_env_overrides(config: &mut ServerConfig) -> Result<(), ConfigError> {
    for (env_name, directive) in [
        ("RATATOSK_BIND", "bind"),
        ("RATATOSK_PORT", "port"),
        ("RATATOSK_MAX_CLIENTS", "maxclients"),
        (
            "RATATOSK_OUTPUT_BUFFER_LIMIT_BYTES",
            "output-buffer-limit-bytes",
        ),
        ("RATATOSK_SHUTDOWN_GRACE_MS", "shutdown-grace-ms"),
        ("RATATOSK_CLIENT_TIMEOUT", "client-timeout-sec"),
        ("RATATOSK_DIR", "dir"),
        ("RATATOSK_DBFILENAME", "dbfilename"),
        ("RATATOSK_APPENDONLY", "appendonly"),
        ("RATATOSK_APPENDFSYNC", "appendfsync"),
        ("RATATOSK_COMPATIBILITY_MODE", "compatibility-mode"),
        ("RATATOSK_PROTECTED_MODE", "protected-mode"),
        ("RATATOSK_TIMEOUT", "timeout"),
        ("RATATOSK_HZ", "hz"),
        ("RATATOSK_SAVE", "save"),
        ("RATATOSK_MAXMEMORY", "maxmemory"),
        ("RATATOSK_MAXMEMORY_POLICY", "maxmemory-policy"),
        ("RATATOSK_MAXMEMORY_SAMPLES", "maxmemory-samples"),
        ("RATATOSK_NOTIFY_KEYSPACE_EVENTS", "notify-keyspace-events"),
        ("RATATOSK_LAZYFREE_LAZY_EXPIRE", "lazyfree-lazy-expire"),
        (
            "RATATOSK_LAZYFREE_LAZY_SERVER_DEL",
            "lazyfree-lazy-server-del",
        ),
        ("RATATOSK_LAZYFREE_LAZY_USER_DEL", "lazyfree-lazy-user-del"),
        ("RATATOSK_TCP_KEEPALIVE", "tcp-keepalive"),
        (
            "RATATOSK_PUBSUB_QUEUE_HARD_LIMIT",
            "pubsub-queue-hard-limit",
        ),
        (
            "RATATOSK_PUBSUB_QUEUE_SOFT_LIMIT",
            "pubsub-queue-soft-limit",
        ),
        (
            "RATATOSK_PUBSUB_QUEUE_SOFT_SECONDS",
            "pubsub-queue-soft-seconds",
        ),
        (
            "RATATOSK_ACTIVE_EXPIRE_CYCLE_LOOKUPS",
            "active-expire-cycle-lookups",
        ),
        (
            "RATATOSK_ACTIVE_EXPIRE_CYCLE_THRESHOLD_PCT",
            "active-expire-cycle-threshold-pct",
        ),
        ("RATATOSK_QUERY_BUFFER_LIMIT", "query-buffer-limit"),
        (
            "RATATOSK_OUTPUT_BUFFER_FLUSH_THRESHOLD",
            "output-buffer-flush-threshold",
        ),
        (
            "RATATOSK_CLIENT_WRITE_TIMEOUT_SEC",
            "client-write-timeout-sec",
        ),
        (
            "RATATOSK_SLOWLOG_LOG_SLOWER_THAN",
            "slowlog-log-slower-than",
        ),
        ("RATATOSK_SLOWLOG_MAX_LEN", "slowlog-max-len"),
        ("RATATOSK_LATENCY_TRACKING", "latency-tracking"),
    ] {
        let Ok(value) = env::var(env_name) else {
            continue;
        };

        let values = vec![value];

        apply_directive(config, directive, &values).map_err(|message| {
            ConfigError::InvalidEnvironmentOverride {
                name: env_name.to_string(),
                message,
            }
        })?;
    }

    Ok(())
}

fn validate_bind_security(config: &ServerConfig) -> Result<(), ConfigError> {
    if !is_loopback_bind(&config.bind) && !env_truthy("RATATOSK_ALLOW_INSECURE_BIND") {
        return Err(ConfigError::InsecureBindRequiresOptIn {
            value: config.bind.clone(),
        });
    }

    Ok(())
}

fn apply_directive(
    config: &mut ServerConfig,
    directive: &str,
    values: &[String],
) -> Result<(), String> {
    match directive {
        "bind" => config.bind = expect_single_value(directive, values)?.to_string(),
        "port" => config.port = parse_u16(directive, expect_single_value(directive, values)?)?,
        "maxclients" | "max-clients" => {
            config.max_clients =
                parse_nonzero_usize(directive, expect_single_value(directive, values)?)?
        }
        "output-buffer-limit-bytes" => {
            config.output_buffer_limit_bytes =
                parse_nonzero_usize(directive, expect_single_value(directive, values)?)?
        }
        "shutdown-grace-ms" => {
            config.shutdown_grace_period_ms =
                parse_nonzero_u64(directive, expect_single_value(directive, values)?)?
        }
        "client-timeout-sec" => {
            config.client_timeout_sec =
                parse_u64(directive, expect_single_value(directive, values)?)?
        }
        "dir" => config.dir = PathBuf::from(expect_single_value(directive, values)?),
        "dbfilename" => config.dbfilename = expect_single_value(directive, values)?.to_string(),
        "appendonly" => {
            config.appendonly = parse_bool(directive, expect_single_value(directive, values)?)?
        }
        "appendfsync" => {
            let value = expect_single_value(directive, values)?.to_ascii_lowercase();
            if !APPENDFSYNC_VALUES.contains(&value.as_str()) {
                return Err(format!(
                    "directive '{directive}' requires one of: {}",
                    APPENDFSYNC_VALUES.join(", ")
                ));
            }
            config.appendfsync = value;
        }
        "compatibility-mode" => {
            let value = expect_single_value(directive, values)?.to_ascii_lowercase();
            if !COMPATIBILITY_MODE_VALUES.contains(&value.as_str()) {
                return Err(format!(
                    "directive '{directive}' requires one of: {}",
                    COMPATIBILITY_MODE_VALUES.join(", ")
                ));
            }
            config.compatibility_mode = value;
        }
        "protected-mode" => {
            let value = expect_single_value(directive, values)?.to_ascii_lowercase();
            if !PROTECTED_MODE_VALUES.contains(&value.as_str()) {
                return Err(format!(
                    "directive '{directive}' requires one of: {}",
                    PROTECTED_MODE_VALUES.join(", ")
                ));
            }
            config.protected_mode = value;
        }
        "timeout" => {
            let value = parse_i64(directive, expect_single_value(directive, values)?)?;
            if value < 0 {
                return Err(format!(
                    "directive '{directive}' requires a value greater than or equal to 0"
                ));
            }
            config.timeout = value;
        }
        "hz" => {
            let value = parse_u32(directive, expect_single_value(directive, values)?)?;
            if !(1..=500).contains(&value) {
                return Err(format!(
                    "directive '{directive}' requires a value in 1..=500"
                ));
            }
            config.hz = value;
        }
        "save" => {
            if values.is_empty() {
                return Err(format!(
                    "directive '{directive}' expects one or more values"
                ));
            }
            config.save = values.join(" ");
        }
        "maxmemory" => {
            config.maxmemory = parse_usize(directive, expect_single_value(directive, values)?)?
        }
        "maxmemory-policy" => {
            let value = expect_single_value(directive, values)?.to_ascii_lowercase();
            if EvictionPolicy::from_config_str(value.as_bytes()).is_none() {
                return Err(format!("directive '{directive}' has an unsupported policy"));
            }
            config.maxmemory_policy = value;
        }
        "maxmemory-samples" => {
            config.maxmemory_samples =
                parse_nonzero_usize(directive, expect_single_value(directive, values)?)?
        }
        "notify-keyspace-events" => {
            config.notify_keyspace_events = expect_single_value(directive, values)?.to_string()
        }
        "lazyfree-lazy-expire" => {
            config.lazyfree_lazy_expire =
                parse_bool(directive, expect_single_value(directive, values)?)?
        }
        "lazyfree-lazy-server-del" => {
            config.lazyfree_lazy_server_del =
                parse_bool(directive, expect_single_value(directive, values)?)?
        }
        "lazyfree-lazy-user-del" => {
            config.lazyfree_lazy_user_del =
                parse_bool(directive, expect_single_value(directive, values)?)?
        }
        "tcp-keepalive" => {
            config.tcp_keepalive_sec =
                parse_u32(directive, expect_single_value(directive, values)?)?
        }
        "pubsub-queue-hard-limit" => {
            config.pubsub_queue_hard_limit =
                parse_usize(directive, expect_single_value(directive, values)?)?
        }
        "pubsub-queue-soft-limit" => {
            config.pubsub_queue_soft_limit =
                parse_usize(directive, expect_single_value(directive, values)?)?
        }
        "pubsub-queue-soft-seconds" => {
            config.pubsub_queue_soft_seconds =
                parse_u64(directive, expect_single_value(directive, values)?)?
        }
        "active-expire-cycle-lookups" => {
            let value = parse_usize(directive, expect_single_value(directive, values)?)?;
            if !(1..=1000).contains(&value) {
                return Err(format!(
                    "directive '{directive}' requires a value in 1..=1000"
                ));
            }
            config.active_expire_cycle_lookups = value;
        }
        "active-expire-cycle-threshold-pct" => {
            let value = parse_u32(directive, expect_single_value(directive, values)?)?;
            if !(1..=100).contains(&value) {
                return Err(format!(
                    "directive '{directive}' requires a value in 1..=100"
                ));
            }
            config.active_expire_cycle_threshold_pct = value;
        }
        "query-buffer-limit" => {
            let value = parse_usize(directive, expect_single_value(directive, values)?)?;
            if value < 1024 {
                return Err(format!("directive '{directive}' requires a value >= 1024"));
            }
            config.query_buffer_limit = value;
        }
        "output-buffer-flush-threshold" => {
            let value = parse_usize(directive, expect_single_value(directive, values)?)?;
            if value < 1024 {
                return Err(format!("directive '{directive}' requires a value >= 1024"));
            }
            config.output_buffer_flush_threshold = value;
        }
        "client-write-timeout-sec" => {
            let value = parse_u64(directive, expect_single_value(directive, values)?)?;
            if !(1..=3600).contains(&value) {
                return Err(format!(
                    "directive '{directive}' requires a value in 1..=3600"
                ));
            }
            config.client_write_timeout_sec = value;
        }
        "slowlog-log-slower-than" => {
            config.slowlog_log_slower_than_us =
                parse_i64(directive, expect_single_value(directive, values)?)?
        }
        "slowlog-max-len" => {
            config.slowlog_max_len =
                parse_usize(directive, expect_single_value(directive, values)?)?
        }
        "latency-tracking" => {
            config.latency_tracking =
                parse_bool(directive, expect_single_value(directive, values)?)?
        }
        _ => return Err(format!("unsupported directive '{directive}'")),
    }

    Ok(())
}

fn tokenize_config_line(line: &str) -> Result<Vec<String>, String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    let mut escaped = false;
    let mut token_open = false;

    for ch in line.chars() {
        if escaped {
            current.push(match ch {
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                other => other,
            });
            escaped = false;
            token_open = true;
            continue;
        }

        match quote {
            Some(q) => match ch {
                '\\' if q == '"' => escaped = true,
                c if c == q => quote = None,
                _ => {
                    current.push(ch);
                    token_open = true;
                }
            },
            None => match ch {
                '#' => break,
                '"' | '\'' => {
                    quote = Some(ch);
                    token_open = true;
                }
                '\\' => {
                    escaped = true;
                    token_open = true;
                }
                c if c.is_whitespace() => {
                    if token_open {
                        tokens.push(std::mem::take(&mut current));
                        token_open = false;
                    }
                }
                _ => {
                    current.push(ch);
                    token_open = true;
                }
            },
        }
    }

    if escaped {
        return Err("unterminated escape sequence".to_string());
    }

    if quote.is_some() {
        return Err("unterminated quoted string".to_string());
    }

    if token_open {
        tokens.push(current);
    }

    Ok(tokens)
}

fn expect_single_value<'a>(directive: &str, values: &'a [String]) -> Result<&'a str, String> {
    if values.len() != 1 {
        return Err(format!(
            "directive '{directive}' expects 1 value, got {}",
            values.len()
        ));
    }

    Ok(values[0].as_str())
}

fn parse_bool(directive: &str, raw: &str) -> Result<bool, String> {
    match raw {
        "1" => Ok(true),
        "0" => Ok(false),
        _ if raw.eq_ignore_ascii_case("yes")
            || raw.eq_ignore_ascii_case("true")
            || raw.eq_ignore_ascii_case("on") =>
        {
            Ok(true)
        }
        _ if raw.eq_ignore_ascii_case("no")
            || raw.eq_ignore_ascii_case("false")
            || raw.eq_ignore_ascii_case("off") =>
        {
            Ok(false)
        }
        _ => Err(format!(
            "directive '{directive}' requires yes/no, true/false, or 1/0"
        )),
    }
}

fn parse_u16(directive: &str, raw: &str) -> Result<u16, String> {
    raw.parse::<u16>()
        .map_err(|_| format!("directive '{directive}' requires a valid u16 integer"))
}

fn parse_u32(directive: &str, raw: &str) -> Result<u32, String> {
    raw.parse::<u32>()
        .map_err(|_| format!("directive '{directive}' requires a valid unsigned integer"))
}

fn parse_u64(directive: &str, raw: &str) -> Result<u64, String> {
    raw.parse::<u64>()
        .map_err(|_| format!("directive '{directive}' requires a valid unsigned integer"))
}

fn parse_usize(directive: &str, raw: &str) -> Result<usize, String> {
    raw.parse::<usize>()
        .map_err(|_| format!("directive '{directive}' requires a valid unsigned integer"))
}

fn parse_nonzero_u64(directive: &str, raw: &str) -> Result<u64, String> {
    let value = parse_u64(directive, raw)?;
    if value == 0 {
        return Err(format!(
            "directive '{directive}' requires a value greater than 0"
        ));
    }
    Ok(value)
}

fn parse_nonzero_usize(directive: &str, raw: &str) -> Result<usize, String> {
    let value = parse_usize(directive, raw)?;
    if value == 0 {
        return Err(format!(
            "directive '{directive}' requires a value greater than 0"
        ));
    }
    Ok(value)
}

fn parse_i64(directive: &str, raw: &str) -> Result<i64, String> {
    raw.parse::<i64>()
        .map_err(|_| format!("directive '{directive}' requires a valid integer"))
}

fn write_scalar(out: &mut String, key: &str, value: &str) {
    writeln!(out, "{key} {}", format_scalar_value(value)).expect("write config");
}

fn write_raw(out: &mut String, key: &str, value: &str) {
    if value.is_empty() {
        writeln!(out, "{key} \"\"").expect("write config");
    } else {
        writeln!(out, "{key} {value}").expect("write config");
    }
}

fn write_bool(out: &mut String, key: &str, value: bool) {
    writeln!(out, "{key} {}", if value { "yes" } else { "no" }).expect("write config");
}

fn write_number<T: std::fmt::Display>(out: &mut String, key: &str, value: T) {
    writeln!(out, "{key} {value}").expect("write config");
}

fn format_scalar_value(value: &str) -> String {
    if value.is_empty()
        || value
            .chars()
            .any(|ch| ch.is_whitespace() || matches!(ch, '#' | '"' | '\\'))
    {
        let mut escaped = String::with_capacity(value.len() + 2);
        escaped.push('"');
        for ch in value.chars() {
            match ch {
                '\\' => escaped.push_str("\\\\"),
                '"' => escaped.push_str("\\\""),
                '\n' => escaped.push_str("\\n"),
                '\r' => escaped.push_str("\\r"),
                '\t' => escaped.push_str("\\t"),
                other => escaped.push(other),
            }
        }
        escaped.push('"');
        return escaped;
    }

    value.to_string()
}

fn is_loopback_bind(bind: &str) -> bool {
    bind.eq_ignore_ascii_case("localhost")
        || bind == "::1"
        || bind == "127.0.0.1"
        || bind.starts_with("127.")
}

fn env_truthy(name: &str) -> bool {
    env::var(name).is_ok_and(|value| {
        value == "1"
            || value.eq_ignore_ascii_case("true")
            || value.eq_ignore_ascii_case("yes")
            || value.eq_ignore_ascii_case("on")
    })
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::{Mutex, OnceLock},
    };

    use tempfile::tempdir;

    use super::{
        DEFAULT_ACTIVE_EXPIRE_CYCLE_LOOKUPS, DEFAULT_ACTIVE_EXPIRE_CYCLE_THRESHOLD_PCT,
        DEFAULT_APPENDFSYNC, DEFAULT_APPENDONLY, DEFAULT_CLIENT_TIMEOUT_SEC,
        DEFAULT_CLIENT_WRITE_TIMEOUT_SEC, DEFAULT_DBFILENAME, DEFAULT_DIR, DEFAULT_HZ,
        DEFAULT_LATENCY_TRACKING, DEFAULT_MAX_CLIENTS, DEFAULT_MAXMEMORY_POLICY,
        DEFAULT_NOTIFY_KEYSPACE_EVENTS, DEFAULT_OUTPUT_BUFFER_FLUSH_THRESHOLD,
        DEFAULT_OUTPUT_BUFFER_LIMIT_BYTES, DEFAULT_PUBSUB_QUEUE_HARD_LIMIT,
        DEFAULT_PUBSUB_QUEUE_SOFT_LIMIT, DEFAULT_PUBSUB_QUEUE_SOFT_SECONDS,
        DEFAULT_QUERY_BUFFER_LIMIT, DEFAULT_SHUTDOWN_GRACE_PERIOD_MS,
        DEFAULT_SLOWLOG_LOG_SLOWER_THAN_US, DEFAULT_SLOWLOG_MAX_LEN, DEFAULT_TIMEOUT, LoadedConfig,
        ServerConfig, is_loopback_bind,
    };

    fn env_lock() -> &'static Mutex<()> {
        static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        ENV_LOCK.get_or_init(|| Mutex::new(()))
    }

    fn with_env_vars<F: FnOnce()>(vars: &[(&str, Option<&str>)], f: F) {
        let _guard = env_lock().lock().expect("env lock");
        let saved = vars
            .iter()
            .map(|(name, _)| ((*name).to_string(), std::env::var(name).ok()))
            .collect::<Vec<_>>();

        for (name, value) in vars {
            match value {
                Some(value) => unsafe { std::env::set_var(name, value) },
                None => unsafe { std::env::remove_var(name) },
            }
        }

        f();

        for (name, value) in saved {
            match value {
                Some(value) => unsafe { std::env::set_var(&name, value) },
                None => unsafe { std::env::remove_var(&name) },
            }
        }
    }

    #[test]
    fn default_listen_addr() {
        let config = ServerConfig::default();
        assert_eq!(config.listen_addr(), "127.0.0.1:6379");
        assert_eq!(config.max_clients, DEFAULT_MAX_CLIENTS);
        assert_eq!(
            config.output_buffer_limit_bytes,
            DEFAULT_OUTPUT_BUFFER_LIMIT_BYTES
        );
        assert_eq!(
            config.shutdown_grace_period_ms,
            DEFAULT_SHUTDOWN_GRACE_PERIOD_MS
        );
        assert_eq!(config.client_timeout_sec, DEFAULT_CLIENT_TIMEOUT_SEC);
        assert_eq!(config.timeout, DEFAULT_TIMEOUT);
        assert_eq!(config.hz, DEFAULT_HZ);
        assert_eq!(config.maxmemory_policy, DEFAULT_MAXMEMORY_POLICY);
        assert_eq!(
            config.notify_keyspace_events,
            DEFAULT_NOTIFY_KEYSPACE_EVENTS
        );
        assert_eq!(
            config.active_expire_cycle_lookups,
            DEFAULT_ACTIVE_EXPIRE_CYCLE_LOOKUPS
        );
        assert_eq!(
            config.active_expire_cycle_threshold_pct,
            DEFAULT_ACTIVE_EXPIRE_CYCLE_THRESHOLD_PCT
        );
        assert_eq!(config.query_buffer_limit, DEFAULT_QUERY_BUFFER_LIMIT);
        assert_eq!(
            config.output_buffer_flush_threshold,
            DEFAULT_OUTPUT_BUFFER_FLUSH_THRESHOLD
        );
        assert_eq!(
            config.client_write_timeout_sec,
            DEFAULT_CLIENT_WRITE_TIMEOUT_SEC
        );
        assert_eq!(
            config.slowlog_log_slower_than_us,
            DEFAULT_SLOWLOG_LOG_SLOWER_THAN_US
        );
        assert_eq!(config.slowlog_max_len, DEFAULT_SLOWLOG_MAX_LEN);
        assert_eq!(config.latency_tracking, DEFAULT_LATENCY_TRACKING);
        assert_eq!(
            config.pubsub_queue_hard_limit,
            DEFAULT_PUBSUB_QUEUE_HARD_LIMIT
        );
        assert_eq!(
            config.pubsub_queue_soft_limit,
            DEFAULT_PUBSUB_QUEUE_SOFT_LIMIT
        );
        assert_eq!(
            config.pubsub_queue_soft_seconds,
            DEFAULT_PUBSUB_QUEUE_SOFT_SECONDS
        );
    }

    #[test]
    fn default_persistence_config() {
        let config = ServerConfig::default();
        assert_eq!(config.dir, PathBuf::from(DEFAULT_DIR));
        assert_eq!(config.dbfilename, DEFAULT_DBFILENAME);
        assert_eq!(config.appendonly, DEFAULT_APPENDONLY);
        assert_eq!(config.appendfsync, DEFAULT_APPENDFSYNC);
    }

    #[test]
    fn explicit_config_file_is_loaded_and_paths_with_spaces_round_trip() {
        let dir = tempdir().expect("tempdir");
        let config_path = dir.path().join("ratatosk.conf");
        let data_dir = dir.path().join("data with spaces");

        std::fs::write(
            &config_path,
            format!(
                r#"
                bind 0.0.0.0
                port 6381
                dir "{}"
                dbfilename "snapshot data.rdb"
                appendonly yes
                appendfsync always
                hz 25
                save 60 1
                notify-keyspace-events ""
                query-buffer-limit 4096
                slowlog-log-slower-than 2500
                latency-tracking yes
                "#,
                data_dir.display()
            ),
        )
        .expect("write config");

        with_env_vars(
            &[
                ("RATATOSK_ALLOW_INSECURE_BIND", Some("true")),
                ("RATATOSK_CONFIG", None),
            ],
            || {
                let loaded = LoadedConfig::load(Some(&config_path)).expect("load config");
                assert_eq!(loaded.config.bind, "0.0.0.0");
                assert_eq!(loaded.config.port, 6381);
                assert_eq!(loaded.config.dir, data_dir);
                assert_eq!(loaded.config.dbfilename, "snapshot data.rdb");
                assert!(loaded.config.appendonly);
                assert_eq!(loaded.config.appendfsync, "always");
                assert_eq!(loaded.config.hz, 25);
                assert_eq!(loaded.config.save, "60 1");
                assert_eq!(loaded.config.notify_keyspace_events, "");
                assert_eq!(loaded.config.query_buffer_limit, 4096);
                assert_eq!(loaded.config.slowlog_log_slower_than_us, 2500);
                assert!(loaded.config.latency_tracking);
            },
        );
    }

    #[test]
    fn environment_overrides_config_file_values() {
        let dir = tempdir().expect("tempdir");
        let config_path = dir.path().join("ratatosk.conf");
        std::fs::write(
            &config_path,
            r#"
            port 6379
            appendonly no
            maxmemory-policy noeviction
            "#,
        )
        .expect("write config");

        with_env_vars(
            &[
                ("RATATOSK_CONFIG", None),
                ("RATATOSK_PORT", Some("6382")),
                ("RATATOSK_APPENDONLY", Some("yes")),
                ("RATATOSK_MAXMEMORY_POLICY", Some("allkeys-lru")),
            ],
            || {
                let loaded = LoadedConfig::load(Some(&config_path)).expect("load config");
                assert_eq!(loaded.config.port, 6382);
                assert!(loaded.config.appendonly);
                assert_eq!(loaded.config.maxmemory_policy, "allkeys-lru");
            },
        );
    }

    #[test]
    fn auto_config_discovery_can_be_disabled() {
        let dir = tempdir().expect("tempdir");
        let config_path = dir.path().join("ratatosk.conf");
        std::fs::write(&config_path, "port 6381\n").expect("write config");

        let previous_dir = std::env::current_dir().expect("current dir");
        let _guard = env_lock().lock().expect("env lock");
        std::env::set_current_dir(dir.path()).expect("enter temp dir");
        unsafe { std::env::remove_var("RATATOSK_CONFIG") };

        let loaded =
            LoadedConfig::load_with_options(None, false).expect("load without auto discovery");

        std::env::set_current_dir(previous_dir).expect("restore cwd");

        assert_eq!(loaded.config.port, 6379);
        assert!(loaded.config_path.is_none());
        assert!(loaded.config_file_source.is_none());
        assert!(!loaded.auto_config_discovery_enabled);
    }

    #[test]
    fn from_env_respects_disable_config_autoload_env() {
        let dir = tempdir().expect("tempdir");
        let config_path = dir.path().join("ratatosk.conf");
        std::fs::write(&config_path, "port 6381\n").expect("write config");

        let previous_dir = std::env::current_dir().expect("current dir");
        let _guard = env_lock().lock().expect("env lock");
        std::env::set_current_dir(dir.path()).expect("enter temp dir");
        unsafe { std::env::remove_var("RATATOSK_CONFIG") };
        unsafe { std::env::set_var("RATATOSK_DISABLE_CONFIG_AUTOLOAD", "true") };

        let loaded = ServerConfig::from_env().expect("load config from env");

        std::env::set_current_dir(previous_dir).expect("restore cwd");
        unsafe { std::env::remove_var("RATATOSK_DISABLE_CONFIG_AUTOLOAD") };
        assert_eq!(loaded.port, 6379);
    }

    #[test]
    fn rendered_config_quotes_values_that_need_it() {
        let config = ServerConfig {
            dir: PathBuf::from("/tmp/ratatosk data"),
            dbfilename: "dump data.rdb".to_string(),
            ..ServerConfig::default()
        };

        let rendered = config.render_redis_config();
        assert!(rendered.contains("dir \"/tmp/ratatosk data\""));
        assert!(rendered.contains("dbfilename \"dump data.rdb\""));
    }

    #[test]
    fn protected_mode_survives_render_and_reload_round_trip() {
        let dir = tempdir().expect("tempdir");
        let config_path = dir.path().join("ratatosk.conf");

        // protected-mode and compatibility-mode must round-trip through
        // render_redis_config (used by CONFIG REWRITE) back into a fresh load.
        let original = ServerConfig {
            protected_mode: "no".to_string(),
            compatibility_mode: "strict".to_string(),
            ..ServerConfig::default()
        };
        std::fs::write(&config_path, original.render_redis_config()).expect("write config");

        with_env_vars(
            &[
                ("RATATOSK_CONFIG", None),
                ("RATATOSK_PROTECTED_MODE", None),
                ("RATATOSK_COMPATIBILITY_MODE", None),
            ],
            || {
                let loaded = LoadedConfig::load(Some(&config_path)).expect("load config");
                assert_eq!(loaded.config.protected_mode, "no");
                assert_eq!(loaded.config.compatibility_mode, "strict");
            },
        );
    }

    #[test]
    fn loopback_bind_detection() {
        assert!(is_loopback_bind("127.0.0.1"));
        assert!(is_loopback_bind("127.0.0.42"));
        assert!(is_loopback_bind("::1"));
        assert!(is_loopback_bind("localhost"));
        assert!(!is_loopback_bind("0.0.0.0"));
        assert!(!is_loopback_bind("192.168.0.10"));
    }
}
