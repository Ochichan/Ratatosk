/// Domain error type for Redis operations.
///
/// Maps to Redis error prefixes like `WRONGTYPE`, `OOM`, etc.
/// Public API boundaries return this instead of `anyhow::Result`.
#[derive(Debug, thiserror::Error)]
pub enum RedisError {
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,

    #[error("ERR wrong number of arguments for '{command}' command")]
    WrongArity { command: String },

    #[error("OOM command not allowed when used memory > 'maxmemory'")]
    Oom,

    #[error("LOADING Redis is loading the dataset in memory")]
    Loading,

    #[error(
        "BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE"
    )]
    Busy,

    #[error("NOAUTH Authentication required")]
    NoAuth,

    #[error("NOPERM this user has no permissions to run the '{command}' command")]
    NoPerm { command: String },

    #[error("ERR value is not an integer or out of range")]
    NotInteger,

    #[error("ERR {0}")]
    Generic(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_messages() {
        assert_eq!(
            RedisError::WrongType.to_string(),
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        );
        assert_eq!(
            RedisError::WrongArity {
                command: "GET".into()
            }
            .to_string(),
            "ERR wrong number of arguments for 'GET' command"
        );
        assert_eq!(
            RedisError::Oom.to_string(),
            "OOM command not allowed when used memory > 'maxmemory'"
        );
        assert_eq!(
            RedisError::NoAuth.to_string(),
            "NOAUTH Authentication required"
        );
    }
}
