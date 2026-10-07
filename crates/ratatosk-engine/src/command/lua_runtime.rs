//! Lua 5.1 scripting runtime for EVAL/EVALSHA commands.
//!
//! Feature-gated behind `lua-scripting`. Provides a sandboxed Lua 5.1
//! environment with `redis.call()` / `redis.pcall()` bridged back to the
//! engine's `execute()` function. One `LuaRuntime` is created per thread
//! via `thread_local!` to avoid Send/Sync issues with mlua's Lua state.

use std::cell::RefCell;

use bytes::Bytes;
use mlua::{HookTriggers, Lua, MultiValue, Result as LuaResult, StdLib, Value};
use ratatosk_resp::frame::RespFrame;

use crate::keyspace::ServerState;

use super::ClientState;

/// Command-dispatch permissions for one script invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScriptMode {
    ReadWrite,
    ReadOnly,
}

/// Maximum memory a single Lua VM may allocate (1 MB).
const LUA_MEMORY_LIMIT: usize = 1_048_576;

/// Maximum number of Lua VM instructions before a script is killed.
const LUA_INSTRUCTION_LIMIT: u32 = 100_000;

// ---------------------------------------------------------------------------
// Thread-local Lua VM
// ---------------------------------------------------------------------------

thread_local! {
    static LUA_RT: std::cell::RefCell<Option<LuaRuntime>> =
        const { std::cell::RefCell::new(None) };
}

/// Obtain a thread-local `LuaRuntime`, creating it lazily on first use.
/// Returns an error string if VM creation fails.
fn with_lua_runtime<F, R>(f: F) -> Result<R, String>
where
    F: FnOnce(&LuaRuntime) -> Result<R, String>,
{
    LUA_RT.with(|cell| {
        let mut opt = cell.borrow_mut();
        if opt.is_none() {
            *opt =
                Some(LuaRuntime::new().map_err(|e| format!("ERR failed to create Lua VM: {e}"))?);
        }
        f(opt.as_ref().unwrap())
    })
}

// ---------------------------------------------------------------------------
// Public API used by cmd_script.rs
// ---------------------------------------------------------------------------

/// Execute a Lua script with the given KEYS and ARGV arrays.
/// `redis.call()` / `redis.pcall()` are bridged back through `execute()`.
pub(crate) fn eval_script(request: EvalRequest<'_>) -> RespFrame {
    match with_lua_runtime(|rt| rt.eval(request)) {
        Ok(frame) => frame,
        Err(msg) => RespFrame::Error(inline_reply_bytes(msg.as_bytes())),
    }
}

// ---------------------------------------------------------------------------
// LuaRuntime — thin wrapper around mlua::Lua
// ---------------------------------------------------------------------------

struct LuaRuntime {
    lua: Lua,
}

pub(crate) struct EvalRequest<'a> {
    pub(crate) source: &'a [u8],
    pub(crate) keys: &'a [Bytes],
    pub(crate) argv: &'a [Bytes],
    pub(crate) server: &'a mut ServerState,
    pub(crate) client: &'a mut ClientState,
    pub(crate) mode: ScriptMode,
}

impl LuaRuntime {
    fn new() -> LuaResult<Self> {
        // Safe subset: no PACKAGE (no require/dofile), no IO, no OS, no DEBUG.
        // The base library (pcall, print, tostring, type, etc.) is always
        // included by new_with().
        let lua = Lua::new_with(
            StdLib::TABLE | StdLib::STRING | StdLib::MATH,
            mlua::LuaOptions::default(),
        )?;

        // Memory cap
        lua.set_memory_limit(LUA_MEMORY_LIMIT)?;

        // Instruction cap -- fires after every N-th VM instruction.
        lua.set_hook(
            HookTriggers::new().every_nth_instruction(LUA_INSTRUCTION_LIMIT),
            |_lua, _debug| {
                Err(mlua::Error::RuntimeError(
                    "ERR Script exceeded instruction limit".into(),
                ))
            },
        )?;

        // Remove remaining dangerous globals.
        let globals = lua.globals();
        for name in &["dofile", "loadfile", "collectgarbage"] {
            globals.set(*name, Value::Nil)?;
        }

        Ok(Self { lua })
    }

    /// Run `source` inside a `Lua::scope` so that the `redis.call()` closure
    /// can borrow `server` and `client` without requiring `'static`.
    ///
    /// Both `redis.call()` and `redis.pcall()` need mutable access to `server`
    /// and `client`. Since they coexist as separate Lua functions within the
    /// same scope, Rust's borrow checker won't allow two `&mut` closures.
    /// We use `RefCell` for interior mutability -- this is safe because Lua
    /// execution is single-threaded and the closures never overlap.
    fn eval(&self, request: EvalRequest<'_>) -> Result<RespFrame, String> {
        let EvalRequest {
            source,
            keys,
            argv,
            server,
            client,
            mode,
        } = request;

        // Redis restores the caller's selected db when a script ends.
        let caller_db = client.selected_db;

        // Wrap mutable references in RefCell for shared access by closures.
        let server_cell = RefCell::new(server);
        let client_cell = RefCell::new(client);

        let result = self
            .lua
            .scope(|scope| {
                let globals = self.lua.globals();

                // ---- KEYS table (1-indexed) ----
                let keys_table = self.lua.create_table()?;
                for (i, key) in keys.iter().enumerate() {
                    keys_table.set((i + 1) as i64, self.lua.create_string(key.as_ref())?)?;
                }
                globals.set("KEYS", keys_table)?;

                // ---- ARGV table (1-indexed) ----
                let argv_table = self.lua.create_table()?;
                for (i, arg) in argv.iter().enumerate() {
                    argv_table.set((i + 1) as i64, self.lua.create_string(arg.as_ref())?)?;
                }
                globals.set("ARGV", argv_table)?;

                // ---- redis.call() / redis.pcall() ----
                let call_fn = scope.create_function(|lua, args: MultiValue| {
                    let mut srv = server_cell.borrow_mut();
                    let mut cli = client_cell.borrow_mut();
                    redis_call_impl(
                        lua,
                        args,
                        &mut srv,
                        &mut cli,
                        RedisCallPolicy {
                            protected: false,
                            mode,
                        },
                    )
                })?;

                let pcall_fn = scope.create_function(|lua, args: MultiValue| {
                    let mut srv = server_cell.borrow_mut();
                    let mut cli = client_cell.borrow_mut();
                    redis_call_impl(
                        lua,
                        args,
                        &mut srv,
                        &mut cli,
                        RedisCallPolicy {
                            protected: true,
                            mode,
                        },
                    )
                })?;

                // redis.log()
                let redis_log_fn = scope.create_function(|_lua, args: MultiValue| {
                    let mut iter = args.into_iter();
                    let level = match iter.next() {
                        Some(Value::Integer(n)) => n,
                        _ => {
                            return Err(mlua::Error::RuntimeError(
                                "ERR First argument must be a number (log level)".into(),
                            ));
                        }
                    };
                    let msg: String = iter
                        .map(|v| match v {
                            Value::String(s) => match s.to_str() {
                                Ok(cow) => cow.to_string(),
                                Err(_) => "<invalid utf8>".to_string(),
                            },
                            Value::Integer(n) => n.to_string(),
                            Value::Number(n) => n.to_string(),
                            Value::Boolean(b) => b.to_string(),
                            _ => "<value>".to_string(),
                        })
                        .collect::<Vec<_>>()
                        .join(" ");
                    match level {
                        0 => tracing::debug!(target: "ratatosk::lua", "{}", msg),
                        1 => tracing::info!(target: "ratatosk::lua", "{}", msg),
                        2 => tracing::warn!(target: "ratatosk::lua", "{}", msg),
                        _ => tracing::warn!(target: "ratatosk::lua", "{}", msg),
                    }
                    Ok(())
                })?;

                // redis.error_reply / redis.status_reply helpers
                let error_reply_fn = scope.create_function(|lua, msg: mlua::String| {
                    let tbl = lua.create_table()?;
                    tbl.set("err", msg)?;
                    Ok(Value::Table(tbl))
                })?;

                let status_reply_fn = scope.create_function(|lua, msg: mlua::String| {
                    let tbl = lua.create_table()?;
                    tbl.set("ok", msg)?;
                    Ok(Value::Table(tbl))
                })?;

                let redis_table = self.lua.create_table()?;
                redis_table.set("call", call_fn)?;
                redis_table.set("pcall", pcall_fn)?;
                redis_table.set("log", redis_log_fn)?;
                redis_table.set("error_reply", error_reply_fn)?;
                redis_table.set("status_reply", status_reply_fn)?;
                // Log level constants
                redis_table.set("LOG_DEBUG", 0i64)?;
                redis_table.set("LOG_VERBOSE", 1i64)?;
                redis_table.set("LOG_NOTICE", 2i64)?;
                redis_table.set("LOG_WARNING", 3i64)?;
                globals.set("redis", redis_table)?;

                // ---- Execute ----
                let source_str = std::str::from_utf8(source).map_err(|_| {
                    mlua::Error::RuntimeError("ERR Script is not valid UTF-8".into())
                })?;

                let result = self.lua.load(source_str).eval::<MultiValue>()?;

                // Convert the first return value to a RESP frame.
                Ok(lua_multi_to_resp(&result))
            })
            .map_err(lua_err);
        // Every exit path of the scope, including script errors, lands here.
        client_cell.borrow_mut().selected_db = caller_db;
        result
    }
}

// ---------------------------------------------------------------------------
// redis.call() / redis.pcall() implementation
// ---------------------------------------------------------------------------

struct RedisCallPolicy {
    protected: bool,
    mode: ScriptMode,
}

/// Bridge from Lua `redis.call(cmd, ...)` / `redis.pcall(cmd, ...)` back into
/// the engine's `execute()`.
fn redis_call_impl(
    lua: &Lua,
    args: MultiValue,
    server: &mut ServerState,
    client: &mut ClientState,
    policy: RedisCallPolicy,
) -> LuaResult<Value> {
    let RedisCallPolicy { protected, mode } = policy;
    // Marshal Lua arguments to Bytes
    let mut cmd_args: Vec<Bytes> = Vec::with_capacity(args.len());
    for val in args {
        match val {
            Value::String(s) => {
                cmd_args.push(Bytes::copy_from_slice(&s.as_bytes()));
            }
            Value::Integer(n) => {
                cmd_args.push(Bytes::from(n.to_string()));
            }
            Value::Number(n) => {
                // Redis truncates floats to integer representation in protocol
                cmd_args.push(Bytes::from(format!("{n}")));
            }
            Value::Boolean(b) => {
                cmd_args.push(Bytes::from(if b { "1" } else { "0" }));
            }
            _ => {
                return Err(mlua::Error::RuntimeError(
                    "ERR Lua redis() command arguments must be strings or integers".into(),
                ));
            }
        }
    }

    if cmd_args.is_empty() {
        return Err(mlua::Error::RuntimeError(
            "ERR Please specify at least one argument for redis.call()".into(),
        ));
    }

    let cmd_upper: Vec<u8> = cmd_args[0].iter().map(|b| b.to_ascii_uppercase()).collect();
    if let Some(message) = script_command_restriction(&cmd_upper, mode) {
        return reject_script_call(lua, protected, message);
    }

    // Redis checks this per nested command (scriptVerifyWriteCommandAllow), not
    // for the EVAL itself, so read-only scripts keep running under the latch.
    if server.aof_enabled() && super::is_write_command(&cmd_args) {
        if let Some(detail) = server.aof_last_error() {
            let message = format!(
                "MISCONF writes are blocked because AOF persistence is in an error state; last_error={detail}"
            );
            return reject_script_call(lua, protected, &message);
        }
    }

    // Build a RespFrame::Array for execute()
    let frame = RespFrame::Array(
        cmd_args
            .iter()
            .map(|b| RespFrame::BulkString(Some(b.clone())))
            .collect(),
    );

    let outcome = {
        let mut access = super::ServerAccess::new_inline(server);
        super::execute(frame, &mut access, client)
    };
    // Redis 7 effects replication: log what the script did, not the script.
    client.collect_script_durability_effects();

    if !protected {
        // redis.call() -- propagate errors as Lua errors
        if let RespFrame::Error(ref e) = outcome.response {
            return Err(mlua::Error::RuntimeError(
                String::from_utf8_lossy(e).to_string(),
            ));
        }
    }

    resp_to_lua(lua, &outcome.response)
}

fn reject_script_call(lua: &Lua, protected: bool, message: &str) -> LuaResult<Value> {
    if protected {
        let table = lua.create_table()?;
        table.set("err", message)?;
        Ok(Value::Table(table))
    } else {
        Err(mlua::Error::RuntimeError(message.into()))
    }
}

/// Return an error before dispatch whenever a script command is forbidden.
///
/// `noscript` metadata applies to every script mode. Authentication commands
/// are explicit because their current metadata predates that Redis rule. RO
/// scripts then use a source-audited allowlist, so newly added commands remain
/// forbidden until their handlers have been checked for command-visible side
/// effects.
fn script_command_restriction(command: &[u8], mode: ScriptMode) -> Option<&'static str> {
    if matches!(command, b"EVAL" | b"EVALSHA" | b"EVAL_RO" | b"EVALSHA_RO") {
        return Some("ERR Lua scripts can't execute EVAL or EVALSHA commands");
    }

    if matches!(command, b"AUTH" | b"HELLO" | b"RESET") {
        return Some("ERR This Redis command is not allowed from script");
    }

    if super::registry::find_command_spec_upper(command)
        .is_some_and(|spec| spec.flags.contains(&"noscript"))
    {
        return Some("ERR This Redis command is not allowed from script");
    }

    if mode == ScriptMode::ReadOnly && !is_read_only_script_command(command) {
        return Some("ERR Write commands are not allowed from read-only scripts");
    }

    None
}

/// Commands admitted by Ratatosk's current read-only Lua subset.
///
/// This intentionally does not promise full Redis EVAL_RO parity. The listed
/// handlers only read dataset values (apart from lazy expiry and read
/// bookkeeping). Dynamic commands such as SORT, GEO*, and BITFIELD* remain
/// fail-closed even when a particular argument form could be read-only.
fn is_read_only_script_command(command: &[u8]) -> bool {
    matches!(
        command,
        // Connection-local commands with no state transition.
        b"PING"
            | b"ECHO"
            // Strings and generic key reads.
            | b"GET"
            | b"MGET"
            | b"STRLEN"
            | b"GETRANGE"
            | b"SUBSTR"
            | b"EXISTS"
            | b"TTL"
            | b"PTTL"
            | b"EXPIRETIME"
            | b"PEXPIRETIME"
            | b"TYPE"
            | b"KEYS"
            | b"SCAN"
            | b"RANDOMKEY"
            | b"DUMP"
            | b"DIGEST"
            // Hash reads.
            | b"HGET"
            | b"HMGET"
            | b"HGETALL"
            | b"HKEYS"
            | b"HVALS"
            | b"HSTRLEN"
            | b"HRANDFIELD"
            | b"HEXISTS"
            | b"HLEN"
            | b"HSCAN"
            | b"HTTL"
            | b"HPTTL"
            | b"HEXPIRETIME"
            | b"HPEXPIRETIME"
            // List reads.
            | b"LRANGE"
            | b"LLEN"
            | b"LPOS"
            | b"LINDEX"
            // Set reads.
            | b"SISMEMBER"
            | b"SMISMEMBER"
            | b"SMEMBERS"
            | b"SCARD"
            | b"SRANDMEMBER"
            | b"SSCAN"
            | b"SDIFF"
            | b"SINTER"
            | b"SINTERCARD"
            | b"SUNION"
            // Sorted-set reads.
            | b"ZSCORE"
            | b"ZCARD"
            | b"ZMSCORE"
            | b"ZRANGE"
            | b"ZRANGEBYSCORE"
            | b"ZREVRANGEBYSCORE"
            | b"ZRANGEBYLEX"
            | b"ZREVRANGEBYLEX"
            | b"ZREVRANGE"
            | b"ZCOUNT"
            | b"ZLEXCOUNT"
            | b"ZRANK"
            | b"ZREVRANK"
            | b"ZUNION"
            | b"ZINTER"
            | b"ZINTERCARD"
            | b"ZDIFF"
            | b"ZRANDMEMBER"
            | b"ZSCAN"
    )
}

// ---------------------------------------------------------------------------
// RESP <-> Lua value conversion
// ---------------------------------------------------------------------------

/// Convert a Lua `MultiValue` (return values from script) to a single `RespFrame`.
/// Per Redis convention, only the *first* return value is used.
fn lua_multi_to_resp(values: &MultiValue) -> RespFrame {
    match values.iter().next() {
        Some(val) => lua_value_to_resp(val),
        None => RespFrame::BulkString(None), // no return value => nil
    }
}

/// Convert a single Lua value to a `RespFrame` following Redis conventions:
///
/// - `nil`        -> BulkString(None)
/// - `false`      -> BulkString(None)
/// - `true`       -> Integer(1)
/// - integer      -> Integer(n)
/// - number       -> Integer(truncated)
/// - string       -> BulkString
/// - table        -> if has "err" key: Error; if has "ok" key: SimpleString;
///   otherwise: Array (sequential integer keys)
fn lua_value_to_resp(val: &Value) -> RespFrame {
    match val {
        Value::Nil => RespFrame::BulkString(None),
        Value::Boolean(false) => RespFrame::BulkString(None),
        Value::Boolean(true) => RespFrame::Integer(1),
        Value::Integer(n) => RespFrame::Integer(*n),
        Value::Number(n) => RespFrame::Integer(*n as i64),
        Value::String(s) => RespFrame::BulkString(Some(Bytes::copy_from_slice(&s.as_bytes()))),
        Value::Table(tbl) => table_to_resp(tbl),
        // UserData, Function, etc. => nil
        _ => RespFrame::BulkString(None),
    }
}

// RESP simple strings and errors are line-delimited. Lua exception traces
// and user-supplied status tables may contain CR/LF; never let those bytes
// introduce another wire frame. Bulk string payloads remain byte-preserving.
fn inline_reply_bytes(value: &[u8]) -> Bytes {
    Bytes::from(
        value
            .iter()
            .map(|&byte| {
                if matches!(byte, b'\r' | b'\n') {
                    b' '
                } else {
                    byte
                }
            })
            .collect::<Vec<_>>(),
    )
}

/// Convert a Lua table to a RespFrame.
///
/// Redis convention:
/// - `{ err = "message" }` => Error frame
/// - `{ ok = "message" }`  => SimpleString frame
/// - sequential table      => Array frame
fn table_to_resp(tbl: &mlua::Table) -> RespFrame {
    // Check for err/ok status tables first.
    if let Ok(Value::String(s)) = tbl.raw_get::<Value>("err") {
        return RespFrame::Error(inline_reply_bytes(&s.as_bytes()));
    }
    if let Ok(Value::String(s)) = tbl.raw_get::<Value>("ok") {
        return RespFrame::SimpleString(inline_reply_bytes(&s.as_bytes()));
    }

    // Sequential array table: iterate integer keys 1..n
    let len = tbl.raw_len();
    let mut items = Vec::with_capacity(len);
    for i in 1..=len as i64 {
        match tbl.raw_get::<Value>(i) {
            Ok(val) => items.push(lua_value_to_resp(&val)),
            Err(_) => break,
        }
    }
    RespFrame::Array(items)
}

/// Convert a `RespFrame` to a Lua `Value`.
///
/// Redis convention:
/// - Integer(n)             -> integer
/// - BulkString(Some(b))    -> string
/// - BulkString(None)       -> false (Lua boolean)
/// - SimpleString(s)        -> { ok = s }  table
/// - Error(e)               -> { err = e } table
/// - Array(items)           -> sequential Lua table
/// - Null / NullArray       -> false
fn resp_to_lua(lua: &Lua, frame: &RespFrame) -> LuaResult<Value> {
    match frame {
        RespFrame::Versioned { frame, .. } => resp_to_lua(lua, frame),
        RespFrame::Integer(n) => Ok(Value::Integer(*n)),
        RespFrame::BulkString(Some(b)) => Ok(Value::String(lua.create_string(b.as_ref())?)),
        RespFrame::BulkString(None) | RespFrame::Null | RespFrame::NullArray => {
            Ok(Value::Boolean(false))
        }
        RespFrame::SimpleString(s) => {
            let tbl = lua.create_table()?;
            tbl.set("ok", lua.create_string(s.as_ref())?)?;
            Ok(Value::Table(tbl))
        }
        RespFrame::Error(e) => {
            let tbl = lua.create_table()?;
            tbl.set("err", lua.create_string(e.as_ref())?)?;
            Ok(Value::Table(tbl))
        }
        RespFrame::Array(items) | RespFrame::Push(items) | RespFrame::Sequence(items) => {
            let tbl = lua.create_table()?;
            for (i, item) in items.iter().enumerate() {
                tbl.set((i + 1) as i64, resp_to_lua(lua, item)?)?;
            }
            Ok(Value::Table(tbl))
        }
        RespFrame::Map(pairs) => {
            let tbl = lua.create_table()?;
            // Flatten map to sequential array: [k1, v1, k2, v2, ...]
            let mut idx = 1i64;
            for (k, v) in pairs {
                tbl.set(idx, resp_to_lua(lua, k)?)?;
                idx += 1;
                tbl.set(idx, resp_to_lua(lua, v)?)?;
                idx += 1;
            }
            Ok(Value::Table(tbl))
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert an mlua::Error to a Redis-style error string.
fn lua_err(e: mlua::Error) -> String {
    match e {
        mlua::Error::RuntimeError(msg) => {
            if msg.starts_with("ERR ") || msg.starts_with("NOSCRIPT ") {
                msg
            } else {
                format!("ERR {msg}")
            }
        }
        mlua::Error::SyntaxError { message, .. } => {
            format!("ERR Error compiling script: {message}")
        }
        mlua::Error::MemoryError(msg) => {
            format!("ERR Script exceeded memory limit: {msg}")
        }
        other => format!("ERR {other}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readonly_allowlist_is_fail_closed_and_covers_audited_data_families() {
        for command in [
            b"PING".as_slice(),
            b"ECHO",
            b"GET",
            b"HGET",
            b"LRANGE",
            b"SISMEMBER",
            b"ZRANGE",
        ] {
            assert_eq!(
                script_command_restriction(command, ScriptMode::ReadOnly),
                None,
                "{} should be allowed",
                String::from_utf8_lossy(command)
            );
        }

        for command in [
            b"SET".as_slice(),
            b"PUBLISH",
            b"SPUBLISH",
            b"SELECT",
            b"SORT_RO",
            b"GEOSEARCH",
            b"BITFIELD_RO",
            b"FUTURE_READ_COMMAND",
        ] {
            assert_eq!(
                script_command_restriction(command, ScriptMode::ReadOnly),
                Some("ERR Write commands are not allowed from read-only scripts"),
                "{} should fail closed",
                String::from_utf8_lossy(command)
            );
        }
    }

    #[test]
    fn authentication_nested_eval_and_noscript_metadata_apply_to_all_modes() {
        for mode in [ScriptMode::ReadWrite, ScriptMode::ReadOnly] {
            for command in [b"AUTH".as_slice(), b"HELLO", b"RESET"] {
                assert_eq!(
                    script_command_restriction(command, mode),
                    Some("ERR This Redis command is not allowed from script")
                );
            }

            assert_eq!(
                script_command_restriction(b"EVAL", mode),
                Some("ERR Lua scripts can't execute EVAL or EVALSHA commands")
            );
            assert_eq!(
                script_command_restriction(b"CONFIG", mode),
                Some("ERR This Redis command is not allowed from script")
            );
        }

        assert_eq!(
            script_command_restriction(b"SET", ScriptMode::ReadWrite),
            None
        );
    }
}
