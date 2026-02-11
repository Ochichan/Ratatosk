use std::{env, num::ParseIntError};

use thiserror::Error;

pub const DEFAULT_MAX_CLIENTS: usize = 4096;
pub const DEFAULT_OUTPUT_BUFFER_LIMIT_BYTES: usize = 8 * 1024 * 1024;
pub const DEFAULT_SHUTDOWN_GRACE_PERIOD_MS: u64 = 10_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    pub bind: String,
    pub port: u16,
    pub max_clients: usize,
    pub output_buffer_limit_bytes: usize,
    pub shutdown_grace_period_ms: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1".to_string(),
            port: 6379,
            max_clients: DEFAULT_MAX_CLIENTS,
            output_buffer_limit_bytes: DEFAULT_OUTPUT_BUFFER_LIMIT_BYTES,
            shutdown_grace_period_ms: DEFAULT_SHUTDOWN_GRACE_PERIOD_MS,
        }
    }
}

impl ServerConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let default = Self::default();

        let bind = env::var("RATATOSK_BIND").unwrap_or(default.bind);
        if !is_loopback_bind(&bind) && !env_truthy("RATATOSK_ALLOW_INSECURE_BIND") {
            return Err(ConfigError::InsecureBindRequiresOptIn { value: bind });
        }

        let port_raw = env::var("RATATOSK_PORT").unwrap_or_else(|_| default.port.to_string());
        let port = port_raw
            .parse::<u16>()
            .map_err(|source| ConfigError::InvalidPort {
                value: port_raw,
                source,
            })?;

        let max_clients_raw =
            env::var("RATATOSK_MAX_CLIENTS").unwrap_or_else(|_| default.max_clients.to_string());
        let max_clients =
            max_clients_raw
                .parse::<usize>()
                .map_err(|source| ConfigError::InvalidMaxClients {
                    value: max_clients_raw,
                    source,
                })?;
        if max_clients == 0 {
            return Err(ConfigError::ZeroMaxClients);
        }

        let output_buffer_limit_raw = env::var("RATATOSK_OUTPUT_BUFFER_LIMIT_BYTES")
            .unwrap_or_else(|_| default.output_buffer_limit_bytes.to_string());
        let output_buffer_limit_bytes =
            output_buffer_limit_raw.parse::<usize>().map_err(|source| {
                ConfigError::InvalidOutputBufferLimit {
                    value: output_buffer_limit_raw,
                    source,
                }
            })?;
        if output_buffer_limit_bytes == 0 {
            return Err(ConfigError::ZeroOutputBufferLimit);
        }

        let shutdown_grace_raw = env::var("RATATOSK_SHUTDOWN_GRACE_MS")
            .unwrap_or_else(|_| default.shutdown_grace_period_ms.to_string());
        let shutdown_grace_period_ms = shutdown_grace_raw.parse::<u64>().map_err(|source| {
            ConfigError::InvalidShutdownGraceMs {
                value: shutdown_grace_raw,
                source,
            }
        })?;
        if shutdown_grace_period_ms == 0 {
            return Err(ConfigError::ZeroShutdownGraceMs);
        }

        Ok(Self {
            bind,
            port,
            max_clients,
            output_buffer_limit_bytes,
            shutdown_grace_period_ms,
        })
    }

    pub fn listen_addr(&self) -> String {
        format!("{}:{}", self.bind, self.port)
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("invalid RATATOSK_PORT value '{value}'")]
    InvalidPort {
        value: String,
        source: ParseIntError,
    },

    #[error("invalid RATATOSK_MAX_CLIENTS value '{value}'")]
    InvalidMaxClients {
        value: String,
        source: ParseIntError,
    },

    #[error("RATATOSK_MAX_CLIENTS must be greater than 0")]
    ZeroMaxClients,

    #[error("invalid RATATOSK_OUTPUT_BUFFER_LIMIT_BYTES value '{value}'")]
    InvalidOutputBufferLimit {
        value: String,
        source: ParseIntError,
    },

    #[error("RATATOSK_OUTPUT_BUFFER_LIMIT_BYTES must be greater than 0")]
    ZeroOutputBufferLimit,

    #[error("invalid RATATOSK_SHUTDOWN_GRACE_MS value '{value}'")]
    InvalidShutdownGraceMs {
        value: String,
        source: ParseIntError,
    },

    #[error("RATATOSK_SHUTDOWN_GRACE_MS must be greater than 0")]
    ZeroShutdownGraceMs,

    #[error(
        "non-loopback RATATOSK_BIND '{value}' requires RATATOSK_ALLOW_INSECURE_BIND=true (or enable a TLS proxy)"
    )]
    InsecureBindRequiresOptIn { value: String },
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
    use super::{
        DEFAULT_MAX_CLIENTS, DEFAULT_OUTPUT_BUFFER_LIMIT_BYTES, DEFAULT_SHUTDOWN_GRACE_PERIOD_MS,
        ServerConfig, is_loopback_bind,
    };

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
