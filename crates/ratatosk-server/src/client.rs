use std::{io, sync::Arc, time::Duration};

use bytes::{Bytes, BytesMut};
use ratatosk_engine::{
    command::{
        ClientState, CommandOutcome, DurabilityEffects, ExecuteArgvPrecheck, ServerAccess,
        apply_post_execute_side_effects, execute, execute_argv, is_write_command,
        post_execute_tracking_flags, precheck_execute_argv_with_default_acl,
    },
    keyspace::{PubSubMessage, ServerState, SharedState},
    object::normalize_range,
};
use ratatosk_resp::{RespFrame, encode, parse};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Notify,
    time::timeout,
};
use tracing::Instrument;

use self::execution::run_with_blocking_retry;
use self::io_support::{
    WaitResult, encode_pubsub_message, load_connection_runtime_config,
    reload_connection_runtime_config, wait_for_async_push_or_input, write_all_with_timeout,
};
use self::readonly_batch::{try_execute_lock_free_fast_command, try_run_readonly_batch};
use self::session_loop::{
    apply_command_outcomes, collect_parsed_frames, drain_post_command_async_output,
    drain_preloop_async_output, execute_client_pipeline,
};
use self::session_runtime::{initialize_session_runtime, run_client_session_loop};
use self::shared_support::{
    disconnect_reason_for_result, finish_client_connection, refresh_client_snapshot,
    register_client_connection,
};
use crate::breadcrumbs;
use crate::config::DEFAULT_OUTPUT_BUFFER_LIMIT_BYTES;
use crate::metrics;
use crate::persistence::{
    PersistenceRuntime, append_aof_effects, disable_aof, enable_aof_from_snapshot, run_save,
    set_aof_fsync_policy, start_bgrewriteaof, start_bgsave,
};
use crate::transport::{ConnInfo, PeerKind, SessionStream};

mod execution;
mod io_support;
mod readonly_batch;
mod session_loop;
mod session_runtime;
mod shared_support;

// Fallback defaults — runtime values are read from ConfigState at connection start.
#[allow(dead_code)]
const QUERY_BUFFER_LIMIT: usize = 1024 * 1024;
#[allow(dead_code)]
const OUTPUT_BUFFER_FLUSH_THRESHOLD: usize = 16 * 1024;
#[allow(dead_code)]
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
// Maximum wait between blocking retries.  With FIFO wake-one semantics only
// the front-of-queue waiter is notified, so exponential backoff is no longer
// needed — this constant caps the wait that guards against lost notifications.
const BLOCKING_RETRY_POLL_CAP: Duration = Duration::from_millis(500);
const AOF_APPEND_SLOW_THRESHOLD: Duration = Duration::from_secs(3);
const OUTPUT_BUFFER_LIMIT_ERR: &str = "ERR output buffer limit exceeded";
const AOF_WRITE_LATCH_ERR_PREFIX: &str =
    "MISCONF writes are blocked because AOF persistence is in an error state";

pub type SharedServerState = Arc<SharedState>;

#[derive(Debug, Clone, Copy)]
pub struct ClientIoLimits {
    pub output_buffer_limit_bytes: usize,
    pub client_read_timeout_sec: u64,
}

impl Default for ClientIoLimits {
    fn default() -> Self {
        Self {
            output_buffer_limit_bytes: DEFAULT_OUTPUT_BUFFER_LIMIT_BYTES,
            client_read_timeout_sec: 0,
        }
    }
}

pub async fn handle_client(stream: TcpStream, server_state: SharedServerState) -> io::Result<()> {
    // The production accept loop sets TCP_NODELAY before handing the stream over;
    // this convenience entry point must do the same so embedders and tests see
    // identical latency behaviour.
    if let Err(error) = stream.set_nodelay(true) {
        tracing::debug!(error = %error, "failed to enable TCP_NODELAY");
    }
    let info = ConnInfo::from_tcp(&stream);
    let persistence = Arc::new(
        PersistenceRuntime::from_config(&crate::config::ServerConfig::default()).map_err(
            |error| {
                io::Error::new(
                    error.kind(),
                    format!("creating default persistence runtime for client handler: {error}"),
                )
            },
        )?,
    );
    handle_client_with_limits(
        stream,
        info,
        server_state,
        persistence,
        ClientIoLimits::default(),
    )
    .await
}

pub async fn handle_client_with_limits<S: SessionStream>(
    stream: S,
    info: ConnInfo,
    server_state: SharedServerState,
    persistence: Arc<PersistenceRuntime>,
    io_limits: ClientIoLimits,
) -> io::Result<()> {
    let (client_id, remote_addr) = register_client_connection(&info, &server_state);

    // Create a tracing span for this client session
    let client_span = tracing::info_span!(
        "client_session",
        client_id = client_id,
        remote_addr = %info.remote_display,
    );

    async move {
        tracing::debug!(
            target = "ratatosk::client",
            client_id = client_id,
            remote_addr = %remote_addr,
            "client connected"
        );

        let result = handle_client_inner(
            stream,
            &info,
            &server_state,
            &persistence,
            client_id,
            io_limits,
        )
        .await
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "handling client I/O (client_id={}, remote_addr={}): {}",
                    client_id, remote_addr, error
                ),
            )
        });

        let disconnect_reason = disconnect_reason_for_result(&result);
        finish_client_connection(&server_state, client_id, disconnect_reason).await;

        tracing::debug!(
            target = "ratatosk::client",
            client_id = client_id,
            reason = disconnect_reason,
            "client disconnected"
        );

        result
    }
    .instrument(client_span)
    .await
}

#[allow(clippy::too_many_arguments)]
async fn handle_client_inner<S: SessionStream>(
    mut stream: S,
    info: &ConnInfo,
    server_state: &SharedServerState,
    persistence: &Arc<PersistenceRuntime>,
    client_id: i64,
    io_limits: ClientIoLimits,
) -> io::Result<()> {
    let mut client_state = ClientState::new(client_id);
    client_state.set_unix_socket(matches!(&info.kind, PeerKind::Unix | PeerKind::Shm));
    let mut runtime =
        initialize_session_runtime(info, server_state, client_id, &client_state).await;

    run_client_session_loop(
        &mut stream,
        server_state,
        persistence,
        &mut client_state,
        &mut runtime,
        io_limits,
    )
    .await
}
#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use hashbrown::{HashMap, HashSet};
    use ratatosk_engine::{
        acl::AclUser,
        command::ClientState,
        keyspace::{HashFieldEntry, ServerState, SharedState, SortedSet, StoredValue},
    };
    use ratatosk_resp::RespFrame;
    use std::{
        collections::VecDeque,
        future::Future,
        sync::Arc,
        task::{Context, Poll, Waker},
        time::Duration,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        time::timeout,
    };

    use super::{
        ClientIoLimits, handle_client, handle_client_with_limits,
        try_execute_lock_free_fast_command, try_run_readonly_batch,
    };
    use crate::config::DEFAULT_OUTPUT_BUFFER_LIMIT_BYTES;
    use crate::persistence::PersistenceRuntime;
    use crate::transport::ConnInfo;

    async fn setup_client_server() -> (TcpStream, tokio::task::JoinHandle<()>) {
        setup_client_server_with_limits(ClientIoLimits::default()).await
    }

    #[tokio::test(flavor = "current_thread")]
    async fn client_tracing_span_does_not_leak_after_pending_poll() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let client = TcpStream::connect(addr).await.expect("connect client");
        let (server, _) = listener.accept().await.expect("accept client");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        let persistence = Arc::new(
            PersistenceRuntime::from_config(&crate::config::ServerConfig::default())
                .expect("persistence runtime"),
        );
        let info = ConnInfo::from_tcp(&server);

        tracing::subscriber::with_default(tracing_subscriber::registry(), || {
            assert!(tracing::Span::current().id().is_none());

            let mut session = Box::pin(handle_client_with_limits(
                server,
                info,
                shared,
                persistence,
                ClientIoLimits::default(),
            ));
            let mut context = Context::from_waker(Waker::noop());
            assert!(matches!(session.as_mut().poll(&mut context), Poll::Pending));
            assert!(
                tracing::Span::current().id().is_none(),
                "client session span leaked into its caller after returning Pending"
            );

            drop(session);
            drop(client);
        });
    }

    async fn setup_client_server_with_shared(
        io_limits: ClientIoLimits,
    ) -> (TcpStream, Arc<SharedState>, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        let shared_for_server = Arc::clone(&shared);

        let server_task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept");
            let info = ConnInfo::from_tcp(&socket);
            let persistence = Arc::new(
                PersistenceRuntime::from_config(&crate::config::ServerConfig::default())
                    .expect("persistence runtime"),
            );
            handle_client_with_limits(socket, info, shared_for_server, persistence, io_limits)
                .await
                .expect("handle client");
        });

        let client = TcpStream::connect(addr).await.expect("connect client");
        (client, shared, server_task)
    }

    async fn setup_client_server_with_limits(
        io_limits: ClientIoLimits,
    ) -> (TcpStream, tokio::task::JoinHandle<()>) {
        let (client, _shared, server_task) = setup_client_server_with_shared(io_limits).await;
        (client, server_task)
    }

    async fn setup_client_server_with_persistence(
        io_limits: ClientIoLimits,
        persistence: Arc<PersistenceRuntime>,
    ) -> (TcpStream, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let mut state = ServerState::with_default_dbs();
        let enabled = persistence.aof_sender().is_some();
        state.set_aof_enabled(enabled);
        state.config.set_appendonly(enabled);
        state
            .config
            .set_appendfsync(Bytes::from(persistence.aof_policy().as_str()));
        let shared = Arc::new(SharedState::new(state));

        let server_task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept");
            let info = ConnInfo::from_tcp(&socket);
            handle_client_with_limits(socket, info, shared, persistence, io_limits)
                .await
                .expect("handle client");
        });

        let client = TcpStream::connect(addr).await.expect("connect client");
        (client, server_task)
    }

    async fn read_reply(stream: &mut TcpStream) -> Vec<u8> {
        let mut buf = vec![0u8; 4096];
        let n = timeout(Duration::from_secs(1), stream.read(&mut buf))
            .await
            .expect("read timeout")
            .expect("read reply");
        buf.truncate(n);
        buf
    }

    /// Search the temp dir for any AOF file and return its concatenated contents.
    fn find_aof_content(dir: &std::path::Path) -> String {
        let mut content = String::new();
        for entry in std::fs::read_dir(dir).expect("read dir") {
            let entry = entry.expect("dir entry");
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.ends_with(".aof") && !name_str.ends_with(".manifest") {
                let text = std::fs::read_to_string(entry.path()).unwrap_or_default();
                content.push_str(&text);
            }
        }
        assert!(
            !content.is_empty(),
            "no AOF file found in {}",
            dir.display()
        );
        content
    }

    fn parse_integer_reply(reply: &[u8]) -> i64 {
        let text = std::str::from_utf8(reply).expect("valid integer reply utf8");
        text.trim_start_matches(':')
            .trim()
            .parse::<i64>()
            .expect("integer reply")
    }

    async fn read_exact_reply(stream: &mut TcpStream, expected_len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(expected_len);
        while out.len() < expected_len {
            let chunk = read_reply(stream).await;
            out.extend_from_slice(&chunk);
        }
        out
    }

    #[test]
    fn lock_free_fast_path_auto_auths_default_user_and_updates_atomic_stats() {
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        let mut client = ClientState::new(7);
        let argv = vec![Bytes::from_static(b"PING")];

        let outcome = try_execute_lock_free_fast_command(&argv, &shared, &mut client)
            .expect("lock-free PING should be handled");

        assert_eq!(outcome.response, RespFrame::pong());
        assert!(client.is_authenticated());
        assert_eq!(client.acl_user(), &Bytes::from_static(b"default"));
        assert_eq!(shared.stats.total_commands_processed(), 1);
    }

    #[test]
    fn lock_free_fast_path_skips_non_default_authenticated_users() {
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        let mut client = ClientState::new(8);
        client.authenticate_as(Bytes::from_static(b"alice"));
        let argv = vec![Bytes::from_static(b"PING")];

        assert!(try_execute_lock_free_fast_command(&argv, &shared, &mut client).is_none());
        assert_eq!(shared.stats.total_commands_processed(), 0);
    }

    #[tokio::test]
    async fn lock_free_fast_path_obeys_acl_cache_and_declines_ping_health() {
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));

        {
            let mut server = shared.meta.lock().await;
            let default_user = server
                .acl
                .get_or_create_user_mut(&Bytes::from_static(b"default"));
            default_user.nopass = false;
            shared.update_acl_policy_cache(&server.acl);
        }

        let mut unauthenticated_client = ClientState::new(9);
        let ping_argv = vec![Bytes::from_static(b"PING")];
        assert!(
            try_execute_lock_free_fast_command(&ping_argv, &shared, &mut unauthenticated_client)
                .is_none()
        );

        let mut authenticated_client = ClientState::new(10);
        authenticated_client.authenticate_as(Bytes::from_static(b"default"));
        let health_argv = vec![Bytes::from_static(b"PING"), Bytes::from_static(b"HEALTH")];
        assert!(
            try_execute_lock_free_fast_command(&health_argv, &shared, &mut authenticated_client)
                .is_none()
        );
    }

    #[test]
    fn lock_free_fast_path_handles_dbsize_and_purges_expired_keys() {
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        {
            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"expired"),
                StoredValue::string(Bytes::from_static(b"gone"), Some(1)),
            );
            db.data.insert(
                Bytes::from_static(b"live"),
                StoredValue::string(Bytes::from_static(b"ok"), None),
            );
        }

        let mut client = ClientState::new(11);
        let argv = vec![Bytes::from_static(b"DBSIZE")];
        let outcome = try_execute_lock_free_fast_command(&argv, &shared, &mut client)
            .expect("lock-free DBSIZE should be handled");

        assert_eq!(outcome.response, RespFrame::Integer(1));
        let db = shared.data.read_db(0);
        assert!(!db.data.contains_key(b"expired" as &[u8]));
        assert!(db.data.contains_key(b"live" as &[u8]));
    }

    #[test]
    fn lock_free_fast_path_handles_type_and_exists_and_updates_atomic_stats() {
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        {
            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"expired"),
                StoredValue::string(Bytes::from_static(b"gone"), Some(1)),
            );
            db.data.insert(
                Bytes::from_static(b"live"),
                StoredValue::string(Bytes::from_static(b"ok"), None),
            );
        }

        let mut client = ClientState::new(12);
        let type_argv = vec![Bytes::from_static(b"TYPE"), Bytes::from_static(b"live")];
        let type_outcome = try_execute_lock_free_fast_command(&type_argv, &shared, &mut client)
            .expect("lock-free TYPE should be handled");
        assert_eq!(type_outcome.response, RespFrame::simple_str("string"));

        let exists_argv = vec![
            Bytes::from_static(b"EXISTS"),
            Bytes::from_static(b"live"),
            Bytes::from_static(b"expired"),
            Bytes::from_static(b"missing"),
        ];
        let exists_outcome = try_execute_lock_free_fast_command(&exists_argv, &shared, &mut client)
            .expect("lock-free EXISTS should be handled");
        assert_eq!(exists_outcome.response, RespFrame::Integer(1));

        let missing_type_argv = vec![Bytes::from_static(b"TYPE"), Bytes::from_static(b"missing")];
        let missing_type =
            try_execute_lock_free_fast_command(&missing_type_argv, &shared, &mut client)
                .expect("lock-free TYPE none should be handled");
        assert_eq!(missing_type.response, RespFrame::simple_str("none"));

        let db = shared.data.read_db(0);
        assert!(!db.data.contains_key(b"expired" as &[u8]));
        assert!(db.data.contains_key(b"live" as &[u8]));
        assert_eq!(shared.stats.keyspace_hits(), 1);
        assert_eq!(shared.stats.keyspace_misses(), 2);
        assert_eq!(shared.stats.total_commands_processed(), 3);
    }

    #[test]
    fn lock_free_fast_path_handles_string_reads_and_get_stats() {
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        {
            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"live"),
                StoredValue::string(Bytes::from_static(b"value"), None),
            );
            db.data.insert(
                Bytes::from_static(b"expired"),
                StoredValue::string(Bytes::from_static(b"gone"), Some(1)),
            );
        }

        let mut client = ClientState::new(13);

        let get_argv = vec![Bytes::from_static(b"GET"), Bytes::from_static(b"live")];
        let get_outcome = try_execute_lock_free_fast_command(&get_argv, &shared, &mut client)
            .expect("lock-free GET should be handled");
        assert_eq!(
            get_outcome.response,
            RespFrame::BulkString(Some(Bytes::from_static(b"value")))
        );

        let strlen_argv = vec![Bytes::from_static(b"STRLEN"), Bytes::from_static(b"live")];
        let strlen_outcome = try_execute_lock_free_fast_command(&strlen_argv, &shared, &mut client)
            .expect("lock-free STRLEN should be handled");
        assert_eq!(strlen_outcome.response, RespFrame::Integer(5));

        let mget_argv = vec![
            Bytes::from_static(b"MGET"),
            Bytes::from_static(b"live"),
            Bytes::from_static(b"expired"),
            Bytes::from_static(b"missing"),
        ];
        let mget_outcome = try_execute_lock_free_fast_command(&mget_argv, &shared, &mut client)
            .expect("lock-free MGET should be handled");
        assert_eq!(
            mget_outcome.response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"value"))),
                RespFrame::BulkString(None),
                RespFrame::BulkString(None),
            ])
        );

        let missing_get_argv = vec![Bytes::from_static(b"GET"), Bytes::from_static(b"missing")];
        let missing_get =
            try_execute_lock_free_fast_command(&missing_get_argv, &shared, &mut client)
                .expect("lock-free GET miss should be handled");
        assert_eq!(missing_get.response, RespFrame::BulkString(None));

        let db = shared.data.read_db(0);
        assert!(!db.data.contains_key(b"expired" as &[u8]));
        assert_eq!(shared.stats.keyspace_hits(), 1);
        assert_eq!(shared.stats.keyspace_misses(), 1);
        assert_eq!(shared.stats.total_commands_processed(), 4);
    }

    #[test]
    fn lock_free_fast_path_handles_getrange_and_substr() {
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        {
            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"alpha"),
                StoredValue::string(Bytes::from_static(b"value"), None),
            );
            db.data.insert(
                Bytes::from_static(b"expired"),
                StoredValue::string(Bytes::from_static(b"gone"), Some(1)),
            );
        }

        let mut client = ClientState::new(14);

        let getrange_argv = vec![
            Bytes::from_static(b"GETRANGE"),
            Bytes::from_static(b"alpha"),
            Bytes::from_static(b"1"),
            Bytes::from_static(b"3"),
        ];
        let getrange = try_execute_lock_free_fast_command(&getrange_argv, &shared, &mut client)
            .expect("lock-free GETRANGE should be handled");
        assert_eq!(
            getrange.response,
            RespFrame::BulkString(Some(Bytes::from_static(b"alu")))
        );

        let substr_argv = vec![
            Bytes::from_static(b"SUBSTR"),
            Bytes::from_static(b"alpha"),
            Bytes::from_static(b"-2"),
            Bytes::from_static(b"-1"),
        ];
        let substr = try_execute_lock_free_fast_command(&substr_argv, &shared, &mut client)
            .expect("lock-free SUBSTR should be handled");
        assert_eq!(
            substr.response,
            RespFrame::BulkString(Some(Bytes::from_static(b"ue")))
        );

        let expired_argv = vec![
            Bytes::from_static(b"GETRANGE"),
            Bytes::from_static(b"expired"),
            Bytes::from_static(b"0"),
            Bytes::from_static(b"10"),
        ];
        let expired = try_execute_lock_free_fast_command(&expired_argv, &shared, &mut client)
            .expect("lock-free GETRANGE on expired key should be handled");
        assert_eq!(expired.response, RespFrame::BulkString(Some(Bytes::new())));

        let db = shared.data.read_db(0);
        assert!(!db.data.contains_key(b"expired" as &[u8]));
        assert_eq!(shared.stats.total_commands_processed(), 3);
    }

    #[test]
    fn lock_free_fast_path_handles_hash_set_and_zset_reads() {
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        {
            let mut hash = HashMap::new();
            hash.insert(
                Bytes::from_static(b"live"),
                HashFieldEntry::new(Bytes::from_static(b"payload")),
            );
            hash.insert(
                Bytes::from_static(b"expired"),
                HashFieldEntry::with_ttl(Bytes::from_static(b"gone"), 1),
            );

            let mut set = HashSet::new();
            set.insert(Bytes::from_static(b"a"));
            set.insert(Bytes::from_static(b"b"));

            let mut zset = SortedSet::default();
            assert!(zset.insert(Bytes::from_static(b"one"), 1.0));
            assert!(zset.insert(Bytes::from_static(b"two"), 2.5));

            let mut db = shared.data.write_db(0);
            db.data
                .insert(Bytes::from_static(b"hash"), StoredValue::hash(hash, None));
            db.data
                .insert(Bytes::from_static(b"set"), StoredValue::set(set, None));
            db.data.insert(
                Bytes::from_static(b"zset"),
                StoredValue::sorted_set(zset, None),
            );
        }

        let mut client = ClientState::new(15);

        let hget = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"HGET"),
                Bytes::from_static(b"hash"),
                Bytes::from_static(b"live"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free HGET should be handled");
        assert_eq!(
            hget.response,
            RespFrame::BulkString(Some(Bytes::from_static(b"payload")))
        );

        let hmget = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"HMGET"),
                Bytes::from_static(b"hash"),
                Bytes::from_static(b"live"),
                Bytes::from_static(b"expired"),
                Bytes::from_static(b"missing"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free HMGET should be handled");
        assert_eq!(
            hmget.response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"payload"))),
                RespFrame::BulkString(None),
                RespFrame::BulkString(None),
            ])
        );

        let hgetall = try_execute_lock_free_fast_command(
            &[Bytes::from_static(b"HGETALL"), Bytes::from_static(b"hash")],
            &shared,
            &mut client,
        )
        .expect("lock-free HGETALL should be handled");
        assert_eq!(
            hgetall.response,
            RespFrame::Map(vec![(
                RespFrame::BulkString(Some(Bytes::from_static(b"live"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"payload"))),
            )])
        );

        let hkeys = try_execute_lock_free_fast_command(
            &[Bytes::from_static(b"HKEYS"), Bytes::from_static(b"hash")],
            &shared,
            &mut client,
        )
        .expect("lock-free HKEYS should be handled");
        assert_eq!(
            hkeys.response,
            RespFrame::Array(vec![RespFrame::BulkString(Some(Bytes::from_static(
                b"live"
            )))])
        );

        let hvals = try_execute_lock_free_fast_command(
            &[Bytes::from_static(b"HVALS"), Bytes::from_static(b"hash")],
            &shared,
            &mut client,
        )
        .expect("lock-free HVALS should be handled");
        assert_eq!(
            hvals.response,
            RespFrame::Array(vec![RespFrame::BulkString(Some(Bytes::from_static(
                b"payload"
            )))])
        );

        let hexists = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"HEXISTS"),
                Bytes::from_static(b"hash"),
                Bytes::from_static(b"expired"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free HEXISTS should be handled");
        assert_eq!(hexists.response, RespFrame::Integer(0));

        let hlen = try_execute_lock_free_fast_command(
            &[Bytes::from_static(b"HLEN"), Bytes::from_static(b"hash")],
            &shared,
            &mut client,
        )
        .expect("lock-free HLEN should be handled");
        assert_eq!(hlen.response, RespFrame::Integer(1));

        let sismember = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"SISMEMBER"),
                Bytes::from_static(b"set"),
                Bytes::from_static(b"b"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free SISMEMBER should be handled");
        assert_eq!(sismember.response, RespFrame::Integer(1));

        let smismember = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"SMISMEMBER"),
                Bytes::from_static(b"set"),
                Bytes::from_static(b"a"),
                Bytes::from_static(b"missing"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free SMISMEMBER should be handled");
        assert_eq!(
            smismember.response,
            RespFrame::Array(vec![RespFrame::Integer(1), RespFrame::Integer(0)])
        );

        let scard = try_execute_lock_free_fast_command(
            &[Bytes::from_static(b"SCARD"), Bytes::from_static(b"set")],
            &shared,
            &mut client,
        )
        .expect("lock-free SCARD should be handled");
        assert_eq!(scard.response, RespFrame::Integer(2));

        let zscore = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"ZSCORE"),
                Bytes::from_static(b"zset"),
                Bytes::from_static(b"two"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free ZSCORE should be handled");
        assert_eq!(
            zscore.response,
            RespFrame::BulkString(Some(Bytes::from_static(b"2.5")))
        );

        let zcard = try_execute_lock_free_fast_command(
            &[Bytes::from_static(b"ZCARD"), Bytes::from_static(b"zset")],
            &shared,
            &mut client,
        )
        .expect("lock-free ZCARD should be handled");
        assert_eq!(zcard.response, RespFrame::Integer(2));

        assert_eq!(shared.stats.total_commands_processed(), 12);
    }

    #[test]
    fn lock_free_fast_path_handles_bitmap_hash_strlen_and_zset_rank_reads() {
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        {
            let mut hash = HashMap::new();
            hash.insert(
                Bytes::from_static(b"live"),
                HashFieldEntry::new(Bytes::from_static(b"payload")),
            );
            hash.insert(
                Bytes::from_static(b"expired"),
                HashFieldEntry::with_ttl(Bytes::from_static(b"gone"), 1),
            );

            let mut zset = SortedSet::default();
            assert!(zset.insert(Bytes::from_static(b"alpha"), 1.0));
            assert!(zset.insert(Bytes::from_static(b"beta"), 2.0));
            assert!(zset.insert(Bytes::from_static(b"gamma"), 3.0));

            let mut db = shared.data.write_db(0);
            db.data
                .insert(Bytes::from_static(b"hash"), StoredValue::hash(hash, None));
            db.data.insert(
                Bytes::from_static(b"bits"),
                StoredValue::string(Bytes::from_static(b"A"), None),
            );
            db.data.insert(
                Bytes::from_static(b"expired-bits"),
                StoredValue::string(Bytes::from_static(b"\xFF"), Some(1)),
            );
            db.data.insert(
                Bytes::from_static(b"zset"),
                StoredValue::sorted_set(zset, None),
            );
        }

        let mut client = ClientState::new(20);

        let hstrlen = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"HSTRLEN"),
                Bytes::from_static(b"hash"),
                Bytes::from_static(b"live"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free HSTRLEN should be handled");
        assert_eq!(hstrlen.response, RespFrame::Integer(7));

        let expired_hstrlen = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"HSTRLEN"),
                Bytes::from_static(b"hash"),
                Bytes::from_static(b"expired"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free HSTRLEN on expired field should be handled");
        assert_eq!(expired_hstrlen.response, RespFrame::Integer(0));

        let getbit = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"GETBIT"),
                Bytes::from_static(b"bits"),
                Bytes::from_static(b"1"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free GETBIT should be handled");
        assert_eq!(getbit.response, RespFrame::Integer(1));

        let expired_getbit = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"GETBIT"),
                Bytes::from_static(b"expired-bits"),
                Bytes::from_static(b"0"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free GETBIT on expired key should be handled");
        assert_eq!(expired_getbit.response, RespFrame::Integer(0));

        let zmscore = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"ZMSCORE"),
                Bytes::from_static(b"zset"),
                Bytes::from_static(b"beta"),
                Bytes::from_static(b"missing"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free ZMSCORE should be handled");
        assert_eq!(
            zmscore.response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"2"))),
                RespFrame::BulkString(None),
            ])
        );

        let zcount = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"ZCOUNT"),
                Bytes::from_static(b"zset"),
                Bytes::from_static(b"1"),
                Bytes::from_static(b"2"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free ZCOUNT should be handled");
        assert_eq!(zcount.response, RespFrame::Integer(2));

        let zlexcount = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"ZLEXCOUNT"),
                Bytes::from_static(b"zset"),
                Bytes::from_static(b"[beta"),
                Bytes::from_static(b"[gamma"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free ZLEXCOUNT should be handled");
        assert_eq!(zlexcount.response, RespFrame::Integer(2));

        let zrank = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"ZRANK"),
                Bytes::from_static(b"zset"),
                Bytes::from_static(b"beta"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free ZRANK should be handled");
        assert_eq!(zrank.response, RespFrame::Integer(1));

        let zrevrank = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"ZREVRANK"),
                Bytes::from_static(b"zset"),
                Bytes::from_static(b"beta"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free ZREVRANK should be handled");
        assert_eq!(zrevrank.response, RespFrame::Integer(1));

        let db = shared.data.read_db(0);
        assert!(!db.data.contains_key(b"expired-bits" as &[u8]));
        assert_eq!(shared.stats.total_commands_processed(), 9);
    }

    #[test]
    fn lock_free_fast_path_handles_zrange_variants() {
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        {
            let mut zset = SortedSet::default();
            assert!(zset.insert(Bytes::from_static(b"alpha"), 1.0));
            assert!(zset.insert(Bytes::from_static(b"beta"), 1.0));
            assert!(zset.insert(Bytes::from_static(b"gamma"), 2.0));

            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"zset"),
                StoredValue::sorted_set(zset, None),
            );
        }

        let mut client = ClientState::new(22);

        let rank = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"ZRANGE"),
                Bytes::from_static(b"zset"),
                Bytes::from_static(b"0"),
                Bytes::from_static(b"-1"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free ZRANGE rank should be handled");
        assert_eq!(
            rank.response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"alpha"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"beta"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"gamma"))),
            ])
        );

        let with_scores = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"ZRANGE"),
                Bytes::from_static(b"zset"),
                Bytes::from_static(b"0"),
                Bytes::from_static(b"1"),
                Bytes::from_static(b"WITHSCORES"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free ZRANGE WITHSCORES should be handled");
        assert_eq!(
            with_scores.response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"alpha"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"1"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"beta"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"1"))),
            ])
        );

        let by_score_rev = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"ZRANGE"),
                Bytes::from_static(b"zset"),
                Bytes::from_static(b"2"),
                Bytes::from_static(b"1"),
                Bytes::from_static(b"BYSCORE"),
                Bytes::from_static(b"REV"),
                Bytes::from_static(b"LIMIT"),
                Bytes::from_static(b"0"),
                Bytes::from_static(b"2"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free ZRANGE BYSCORE REV should be handled");
        assert_eq!(
            by_score_rev.response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"gamma"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"beta"))),
            ])
        );

        let by_lex = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"ZRANGE"),
                Bytes::from_static(b"zset"),
                Bytes::from_static(b"[alpha"),
                Bytes::from_static(b"[beta"),
                Bytes::from_static(b"BYLEX"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free ZRANGE BYLEX should be handled");
        assert_eq!(
            by_lex.response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"alpha"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"beta"))),
            ])
        );

        let by_score = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"ZRANGEBYSCORE"),
                Bytes::from_static(b"zset"),
                Bytes::from_static(b"1"),
                Bytes::from_static(b"2"),
                Bytes::from_static(b"WITHSCORES"),
                Bytes::from_static(b"LIMIT"),
                Bytes::from_static(b"1"),
                Bytes::from_static(b"2"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free ZRANGEBYSCORE should be handled");
        assert_eq!(
            by_score.response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"beta"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"1"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"gamma"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"2"))),
            ])
        );

        let rev_by_score = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"ZREVRANGEBYSCORE"),
                Bytes::from_static(b"zset"),
                Bytes::from_static(b"2"),
                Bytes::from_static(b"1"),
                Bytes::from_static(b"WITHSCORES"),
                Bytes::from_static(b"LIMIT"),
                Bytes::from_static(b"0"),
                Bytes::from_static(b"2"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free ZREVRANGEBYSCORE should be handled");
        assert_eq!(
            rev_by_score.response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"gamma"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"2"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"beta"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"1"))),
            ])
        );

        let rev_by_lex = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"ZREVRANGEBYLEX"),
                Bytes::from_static(b"zset"),
                Bytes::from_static(b"[beta"),
                Bytes::from_static(b"[alpha"),
                Bytes::from_static(b"LIMIT"),
                Bytes::from_static(b"0"),
                Bytes::from_static(b"2"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free ZREVRANGEBYLEX should be handled");
        assert_eq!(
            rev_by_lex.response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"beta"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"alpha"))),
            ])
        );

        let rev_rank = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"ZREVRANGE"),
                Bytes::from_static(b"zset"),
                Bytes::from_static(b"0"),
                Bytes::from_static(b"1"),
                Bytes::from_static(b"WITHSCORES"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free ZREVRANGE should be handled");
        assert_eq!(
            rev_rank.response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"gamma"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"2"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"beta"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"1"))),
            ])
        );

        assert_eq!(shared.stats.total_commands_processed(), 8);
    }

    #[test]
    fn lock_free_fast_path_handles_bitcount() {
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        {
            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"bits"),
                StoredValue::string(Bytes::from_static(b"AB"), None),
            );
            db.data.insert(
                Bytes::from_static(b"expired"),
                StoredValue::string(Bytes::from_static(b"\xFF"), Some(1)),
            );
        }

        let mut client = ClientState::new(21);

        let all_bits = try_execute_lock_free_fast_command(
            &[Bytes::from_static(b"BITCOUNT"), Bytes::from_static(b"bits")],
            &shared,
            &mut client,
        )
        .expect("lock-free BITCOUNT should be handled");
        assert_eq!(all_bits.response, RespFrame::Integer(4));

        let last_byte = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"BITCOUNT"),
                Bytes::from_static(b"bits"),
                Bytes::from_static(b"-1"),
                Bytes::from_static(b"-1"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free BITCOUNT byte range should be handled");
        assert_eq!(last_byte.response, RespFrame::Integer(2));

        let first_byte_bits = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"BITCOUNT"),
                Bytes::from_static(b"bits"),
                Bytes::from_static(b"0"),
                Bytes::from_static(b"7"),
                Bytes::from_static(b"BIT"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free BITCOUNT bit mode should be handled");
        assert_eq!(first_byte_bits.response, RespFrame::Integer(2));

        let prefix_bits = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"BITCOUNT"),
                Bytes::from_static(b"bits"),
                Bytes::from_static(b"0"),
                Bytes::from_static(b"3"),
                Bytes::from_static(b"BIT"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free BITCOUNT partial bit range should be handled");
        assert_eq!(prefix_bits.response, RespFrame::Integer(1));

        let missing = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"BITCOUNT"),
                Bytes::from_static(b"missing"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free BITCOUNT missing should be handled");
        assert_eq!(missing.response, RespFrame::Integer(0));

        let expired = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"BITCOUNT"),
                Bytes::from_static(b"expired"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free BITCOUNT expired should be handled");
        assert_eq!(expired.response, RespFrame::Integer(0));

        let db = shared.data.read_db(0);
        assert!(!db.data.contains_key(b"expired" as &[u8]));
        assert_eq!(shared.stats.total_commands_processed(), 6);
    }

    #[test]
    fn lock_free_fast_path_handles_list_reads() {
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        {
            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"list"),
                StoredValue::list(
                    VecDeque::from(vec![
                        Bytes::from_static(b"zero"),
                        Bytes::from_static(b"one"),
                        Bytes::from_static(b"two"),
                    ]),
                    None,
                ),
            );
            db.data.insert(
                Bytes::from_static(b"expired"),
                StoredValue::list(VecDeque::from(vec![Bytes::from_static(b"gone")]), Some(1)),
            );
        }

        let mut client = ClientState::new(16);

        let llen = try_execute_lock_free_fast_command(
            &[Bytes::from_static(b"LLEN"), Bytes::from_static(b"list")],
            &shared,
            &mut client,
        )
        .expect("lock-free LLEN should be handled");
        assert_eq!(llen.response, RespFrame::Integer(3));

        let lindex = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"LINDEX"),
                Bytes::from_static(b"list"),
                Bytes::from_static(b"-1"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free LINDEX should be handled");
        assert_eq!(
            lindex.response,
            RespFrame::BulkString(Some(Bytes::from_static(b"two")))
        );

        let lrange = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"LRANGE"),
                Bytes::from_static(b"list"),
                Bytes::from_static(b"0"),
                Bytes::from_static(b"1"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free LRANGE should be handled");
        assert_eq!(
            lrange.response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"zero"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"one"))),
            ])
        );

        let expired = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"LRANGE"),
                Bytes::from_static(b"expired"),
                Bytes::from_static(b"0"),
                Bytes::from_static(b"-1"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free LRANGE on expired key should be handled");
        assert_eq!(expired.response, RespFrame::Array(vec![]));

        let db = shared.data.read_db(0);
        assert!(!db.data.contains_key(b"expired" as &[u8]));
        assert_eq!(shared.stats.total_commands_processed(), 4);
    }

    #[test]
    fn lock_free_fast_path_handles_ttl_family() {
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        let expire_at_ms = ratatosk_core::time::now_ms().saturating_add(5_000);
        {
            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"expiring"),
                StoredValue::string(Bytes::from_static(b"value"), Some(expire_at_ms)),
            );
            db.data.insert(
                Bytes::from_static(b"persistent"),
                StoredValue::string(Bytes::from_static(b"value"), None),
            );
            db.data.insert(
                Bytes::from_static(b"expired"),
                StoredValue::string(Bytes::from_static(b"gone"), Some(1)),
            );
        }

        let mut client = ClientState::new(19);

        let ttl = try_execute_lock_free_fast_command(
            &[Bytes::from_static(b"TTL"), Bytes::from_static(b"expiring")],
            &shared,
            &mut client,
        )
        .expect("lock-free TTL should be handled");
        let RespFrame::Integer(ttl_value) = ttl.response else {
            panic!("TTL should return integer");
        };
        assert!((4..=5).contains(&ttl_value), "ttl={ttl_value}");

        let pttl = try_execute_lock_free_fast_command(
            &[Bytes::from_static(b"PTTL"), Bytes::from_static(b"expiring")],
            &shared,
            &mut client,
        )
        .expect("lock-free PTTL should be handled");
        let RespFrame::Integer(pttl_value) = pttl.response else {
            panic!("PTTL should return integer");
        };
        assert!((4_000..=5_000).contains(&pttl_value), "pttl={pttl_value}");

        let expiretime = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"EXPIRETIME"),
                Bytes::from_static(b"expiring"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free EXPIRETIME should be handled");
        assert_eq!(expiretime.response, RespFrame::Integer(expire_at_ms / 1000));

        let pexpiretime = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"PEXPIRETIME"),
                Bytes::from_static(b"expiring"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free PEXPIRETIME should be handled");
        assert_eq!(pexpiretime.response, RespFrame::Integer(expire_at_ms));

        let persistent = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"TTL"),
                Bytes::from_static(b"persistent"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free TTL persistent should be handled");
        assert_eq!(persistent.response, RespFrame::Integer(-1));

        let missing = try_execute_lock_free_fast_command(
            &[Bytes::from_static(b"PTTL"), Bytes::from_static(b"missing")],
            &shared,
            &mut client,
        )
        .expect("lock-free PTTL missing should be handled");
        assert_eq!(missing.response, RespFrame::Integer(-2));

        let expired = try_execute_lock_free_fast_command(
            &[Bytes::from_static(b"TTL"), Bytes::from_static(b"expired")],
            &shared,
            &mut client,
        )
        .expect("lock-free TTL expired should be handled");
        assert_eq!(expired.response, RespFrame::Integer(-2));

        let db = shared.data.read_db(0);
        assert!(!db.data.contains_key(b"expired" as &[u8]));
        assert_eq!(shared.stats.total_commands_processed(), 7);
    }

    #[tokio::test]
    async fn inline_ping_and_echo() {
        let (mut client, server_task) = setup_client_server().await;

        client.write_all(b"PING\r\n").await.expect("write ping");
        let ping = read_reply(&mut client).await;
        assert_eq!(ping, b"+PONG\r\n");

        client.write_all(b"ECHO hi\r\n").await.expect("write echo");
        let echo = read_reply(&mut client).await;
        assert_eq!(echo, b"$2\r\nhi\r\n");

        client.write_all(b"QUIT\r\n").await.expect("write quit");
        let quit = read_reply(&mut client).await;
        assert_eq!(quit, b"+OK\r\n");

        let mut eof = [0u8; 1];
        let n = client.read(&mut eof).await.expect("read eof");
        assert_eq!(n, 0);

        server_task.await.expect("server task complete");
    }

    #[tokio::test]
    async fn m1_set_get_exists_del_select() {
        let (mut client, server_task) = setup_client_server().await;

        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
            .await
            .expect("write set");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");

        client
            .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
            .await
            .expect("write get");
        assert_eq!(read_reply(&mut client).await, b"$3\r\nbar\r\n");

        client
            .write_all(b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n")
            .await
            .expect("write select");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");

        client
            .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
            .await
            .expect("write get db1");
        assert_eq!(read_reply(&mut client).await, b"$-1\r\n");

        client.write_all(b"QUIT\r\n").await.expect("write quit");
        let _ = read_reply(&mut client).await;

        server_task.await.expect("server task complete");
    }

    #[tokio::test]
    async fn pipelined_commands_return_batched_replies() {
        let (mut client, server_task) = setup_client_server().await;

        client
            .write_all(b"PING\r\nECHO hi\r\nQUIT\r\n")
            .await
            .expect("write pipelined commands");

        let expected = b"+PONG\r\n$2\r\nhi\r\n+OK\r\n";
        let reply = read_exact_reply(&mut client, expected.len()).await;
        assert_eq!(reply, expected);

        let mut eof = [0u8; 1];
        let n = client.read(&mut eof).await.expect("read eof");
        assert_eq!(n, 0);

        server_task.await.expect("server task complete");
    }

    #[tokio::test]
    async fn pipelined_lock_free_readonly_batch_keeps_stats_in_sync() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;

        client
            .write_all(b"PING\r\nECHO hi\r\nDBSIZE\r\n")
            .await
            .expect("write readonly batch");

        let expected = b"+PONG\r\n$2\r\nhi\r\n:0\r\n";
        let reply = read_exact_reply(&mut client, expected.len()).await;
        assert_eq!(reply, expected);

        client.write_all(b"QUIT\r\n").await.expect("write quit");
        let quit = read_reply(&mut client).await;
        assert_eq!(quit, b"+OK\r\n");

        server_task.await.expect("server task complete");

        let server = shared.meta.lock().await;
        assert_eq!(
            shared.stats.total_commands_processed(),
            server.stats.total_commands_processed()
        );
        assert_eq!(shared.stats.total_commands_processed(), 4);
    }

    #[tokio::test]
    async fn fast_path_commands_still_queue_inside_multi() {
        let (mut client, server_task) = setup_client_server().await;

        client
            .write_all(b"MULTI\r\nDBSIZE\r\nEXEC\r\n")
            .await
            .expect("write multi dbsize exec");
        let reply = read_exact_reply(&mut client, b"+OK\r\n+QUEUED\r\n*1\r\n:0\r\n".len()).await;
        assert_eq!(reply, b"+OK\r\n+QUEUED\r\n*1\r\n:0\r\n");

        client.write_all(b"QUIT\r\n").await.expect("write quit");
        let quit = read_reply(&mut client).await;
        assert_eq!(quit, b"+OK\r\n");

        server_task.await.expect("server task complete");
    }

    #[tokio::test]
    async fn monitor_receives_lock_free_dbsize_commands() {
        const CONNECTIONS: usize = 2;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        let shared_for_accept = Arc::clone(&shared);

        let accept_task = tokio::spawn(async move {
            let persistence = Arc::new(
                PersistenceRuntime::from_config(&crate::config::ServerConfig::default())
                    .expect("persistence runtime"),
            );
            let mut tasks = Vec::with_capacity(CONNECTIONS);

            for _ in 0..CONNECTIONS {
                let (socket, _) = listener.accept().await.expect("accept");
                let shared = Arc::clone(&shared_for_accept);
                let persistence = Arc::clone(&persistence);
                tasks.push(tokio::spawn(async move {
                    let info = ConnInfo::from_tcp(&socket);
                    handle_client_with_limits(
                        socket,
                        info,
                        shared,
                        persistence,
                        ClientIoLimits::default(),
                    )
                    .await
                    .expect("handle client");
                }));
            }

            for task in tasks {
                task.await.expect("join client task");
            }
        });

        let mut monitor_client = TcpStream::connect(addr).await.expect("connect monitor");
        let mut command_client = TcpStream::connect(addr)
            .await
            .expect("connect command client");

        monitor_client
            .write_all(b"MONITOR\r\n")
            .await
            .expect("enable monitor");
        assert_eq!(read_reply(&mut monitor_client).await, b"+OK\r\n");

        command_client
            .write_all(b"DBSIZE\r\n")
            .await
            .expect("write dbsize");
        assert_eq!(read_reply(&mut command_client).await, b":0\r\n");

        let monitor_line = read_reply(&mut monitor_client).await;
        let monitor_text = String::from_utf8_lossy(&monitor_line);
        assert!(monitor_text.starts_with('+'));
        assert!(monitor_text.contains("\"DBSIZE\""), "{monitor_text}");

        drop(command_client);

        monitor_client
            .write_all(b"QUIT\r\n")
            .await
            .expect("quit monitor client");
        assert_eq!(read_reply(&mut monitor_client).await, b"+OK\r\n");

        accept_task.await.expect("accept task complete");
    }

    #[tokio::test]
    async fn exists_fast_path_keeps_keyspace_stats_in_sync() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;
        {
            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"live"),
                StoredValue::string(Bytes::from_static(b"ok"), None),
            );
        }

        client
            .write_all(b"EXISTS live missing\r\n")
            .await
            .expect("write exists");
        assert_eq!(read_reply(&mut client).await, b":1\r\n");

        client.write_all(b"QUIT\r\n").await.expect("write quit");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");

        server_task.await.expect("server task complete");

        let server = shared.meta.lock().await;
        assert_eq!(shared.stats.keyspace_hits(), 1);
        assert_eq!(shared.stats.keyspace_misses(), 1);
        assert_eq!(server.stats.keyspace_hits(), 1);
        assert_eq!(server.stats.keyspace_misses(), 1);
        assert_eq!(
            shared.stats.total_commands_processed(),
            server.stats.total_commands_processed()
        );
    }

    #[tokio::test]
    async fn pipelined_lock_free_string_reads_keep_stats_in_sync() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;
        {
            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"alpha"),
                StoredValue::string(Bytes::from_static(b"value"), None),
            );
        }

        client
            .write_all(b"GET alpha\r\nSTRLEN alpha\r\nMGET alpha missing\r\n")
            .await
            .expect("write readonly string batch");

        let expected = b"$5\r\nvalue\r\n:5\r\n*2\r\n$5\r\nvalue\r\n$-1\r\n";
        let reply = read_exact_reply(&mut client, expected.len()).await;
        assert_eq!(reply, expected);

        client.write_all(b"QUIT\r\n").await.expect("write quit");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");

        server_task.await.expect("server task complete");

        let server = shared.meta.lock().await;
        assert_eq!(shared.stats.keyspace_hits(), 1);
        assert_eq!(shared.stats.keyspace_misses(), 0);
        assert_eq!(server.stats.keyspace_hits(), 1);
        assert_eq!(server.stats.keyspace_misses(), 0);
        assert_eq!(
            shared.stats.total_commands_processed(),
            server.stats.total_commands_processed()
        );
        assert_eq!(shared.stats.total_commands_processed(), 4);
    }

    #[tokio::test]
    async fn pipelined_lock_free_string_ranges_work() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;
        {
            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"alpha"),
                StoredValue::string(Bytes::from_static(b"value"), None),
            );
        }

        client
            .write_all(b"GETRANGE alpha 1 3\r\nSUBSTR alpha -2 -1\r\n")
            .await
            .expect("write readonly range batch");

        let expected = b"$3\r\nalu\r\n$2\r\nue\r\n";
        let reply = read_exact_reply(&mut client, expected.len()).await;
        assert_eq!(reply, expected);

        client.write_all(b"QUIT\r\n").await.expect("write quit");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");

        server_task.await.expect("server task complete");

        let server = shared.meta.lock().await;
        assert_eq!(
            shared.stats.total_commands_processed(),
            server.stats.total_commands_processed()
        );
        assert_eq!(shared.stats.total_commands_processed(), 3);
    }

    #[tokio::test]
    async fn pipelined_lock_free_hash_set_and_zset_reads_work() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;
        {
            let mut hash = HashMap::new();
            hash.insert(
                Bytes::from_static(b"live"),
                HashFieldEntry::new(Bytes::from_static(b"payload")),
            );
            let mut set = HashSet::new();
            set.insert(Bytes::from_static(b"a"));
            set.insert(Bytes::from_static(b"b"));
            let mut zset = SortedSet::default();
            assert!(zset.insert(Bytes::from_static(b"one"), 1.0));
            assert!(zset.insert(Bytes::from_static(b"two"), 2.5));

            let mut db = shared.data.write_db(0);
            db.data
                .insert(Bytes::from_static(b"hash"), StoredValue::hash(hash, None));
            db.data
                .insert(Bytes::from_static(b"set"), StoredValue::set(set, None));
            db.data.insert(
                Bytes::from_static(b"zset"),
                StoredValue::sorted_set(zset, None),
            );
        }

        client
            .write_all(
                b"HGET hash live\r\nHGETALL hash\r\nHKEYS hash\r\nHVALS hash\r\nHLEN hash\r\nSISMEMBER set b\r\nSMISMEMBER set a missing\r\nZSCORE zset two\r\n",
            )
            .await
            .expect("write readonly collection batch");

        let expected = b"$7\r\npayload\r\n*2\r\n$4\r\nlive\r\n$7\r\npayload\r\n*1\r\n$4\r\nlive\r\n*1\r\n$7\r\npayload\r\n:1\r\n:1\r\n*2\r\n:1\r\n:0\r\n$3\r\n2.5\r\n";
        let reply = read_exact_reply(&mut client, expected.len()).await;
        assert_eq!(reply, expected);

        client.write_all(b"QUIT\r\n").await.expect("write quit");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");

        server_task.await.expect("server task complete");

        let server = shared.meta.lock().await;
        assert_eq!(
            shared.stats.total_commands_processed(),
            server.stats.total_commands_processed()
        );
        assert_eq!(shared.stats.total_commands_processed(), 9);
    }

    #[tokio::test]
    async fn pipelined_lock_free_list_reads_work() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;
        {
            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"list"),
                StoredValue::list(
                    VecDeque::from(vec![
                        Bytes::from_static(b"zero"),
                        Bytes::from_static(b"one"),
                        Bytes::from_static(b"two"),
                    ]),
                    None,
                ),
            );
        }

        client
            .write_all(b"LLEN list\r\nLINDEX list -1\r\nLRANGE list 0 1\r\n")
            .await
            .expect("write readonly list batch");

        let expected = b":3\r\n$3\r\ntwo\r\n*2\r\n$4\r\nzero\r\n$3\r\none\r\n";
        let reply = read_exact_reply(&mut client, expected.len()).await;
        assert_eq!(reply, expected);

        client.write_all(b"QUIT\r\n").await.expect("write quit");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");

        server_task.await.expect("server task complete");

        let server = shared.meta.lock().await;
        assert_eq!(
            shared.stats.total_commands_processed(),
            server.stats.total_commands_processed()
        );
        assert_eq!(shared.stats.total_commands_processed(), 4);
    }

    #[tokio::test]
    async fn readonly_batch_supports_non_default_authenticated_user() {
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        {
            let mut server = shared.meta.lock().await;
            let alice = server
                .acl
                .get_or_create_user_mut(&Bytes::from_static(b"alice"));
            *alice = AclUser {
                enabled: true,
                nopass: false,
                passwords: HashSet::new(),
                allow_all_commands: true,
                allowed_categories: HashSet::new(),
            };
        }
        {
            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"key"),
                StoredValue::string(Bytes::from_static(b"value"), None),
            );
        }

        let mut client = ClientState::new(17);
        client.authenticate_as(Bytes::from_static(b"alice"));

        let outcomes = try_run_readonly_batch(
            vec![
                RespFrame::Array(vec![
                    RespFrame::BulkString(Some(Bytes::from_static(b"GET"))),
                    RespFrame::BulkString(Some(Bytes::from_static(b"key"))),
                ]),
                RespFrame::Array(vec![
                    RespFrame::BulkString(Some(Bytes::from_static(b"STRLEN"))),
                    RespFrame::BulkString(Some(Bytes::from_static(b"key"))),
                ]),
            ],
            &shared,
            &mut client,
        )
        .await
        .expect("readonly batch should execute for authenticated alice");

        assert_eq!(outcomes.len(), 2);
        assert_eq!(
            outcomes[0].response,
            RespFrame::BulkString(Some(Bytes::from_static(b"value")))
        );
        assert_eq!(outcomes[1].response, RespFrame::Integer(5));

        let server = shared.meta.lock().await;
        assert_eq!(shared.stats.total_commands_processed(), 2);
        assert_eq!(server.stats.total_commands_processed(), 2);
    }

    #[tokio::test]
    async fn readonly_batch_accepts_supported_readonly_commands() {
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        {
            let mut server = shared.meta.lock().await;
            let alice = server
                .acl
                .get_or_create_user_mut(&Bytes::from_static(b"alice"));
            *alice = AclUser {
                enabled: true,
                nopass: false,
                passwords: HashSet::new(),
                allow_all_commands: true,
                allowed_categories: HashSet::new(),
            };
        }
        {
            let mut hash = HashMap::new();
            hash.insert(
                Bytes::from_static(b"field"),
                HashFieldEntry::new(Bytes::from_static(b"payload")),
            );

            let mut zset = SortedSet::default();
            assert!(zset.insert(Bytes::from_static(b"one"), 1.0));
            assert!(zset.insert(Bytes::from_static(b"two"), 2.0));
            assert!(zset.insert(Bytes::from_static(b"three"), 3.0));

            let mut db = shared.data.write_db(0);
            db.data
                .insert(Bytes::from_static(b"hash"), StoredValue::hash(hash, None));
            db.data.insert(
                Bytes::from_static(b"zset"),
                StoredValue::sorted_set(zset, None),
            );
            db.data.insert(
                Bytes::from_static(b"key"),
                StoredValue::string(Bytes::from_static(b"value"), None),
            );
        }

        let mut client = ClientState::new(18);
        client.authenticate_as(Bytes::from_static(b"alice"));

        let outcomes = try_run_readonly_batch(
            vec![
                RespFrame::Array(vec![
                    RespFrame::BulkString(Some(Bytes::from_static(b"HSTRLEN"))),
                    RespFrame::BulkString(Some(Bytes::from_static(b"hash"))),
                    RespFrame::BulkString(Some(Bytes::from_static(b"field"))),
                ]),
                RespFrame::Array(vec![
                    RespFrame::BulkString(Some(Bytes::from_static(b"ZRANGE"))),
                    RespFrame::BulkString(Some(Bytes::from_static(b"zset"))),
                    RespFrame::BulkString(Some(Bytes::from_static(b"0"))),
                    RespFrame::BulkString(Some(Bytes::from_static(b"-1"))),
                ]),
                RespFrame::Array(vec![
                    RespFrame::BulkString(Some(Bytes::from_static(b"ZREVRANGE"))),
                    RespFrame::BulkString(Some(Bytes::from_static(b"zset"))),
                    RespFrame::BulkString(Some(Bytes::from_static(b"0"))),
                    RespFrame::BulkString(Some(Bytes::from_static(b"1"))),
                    RespFrame::BulkString(Some(Bytes::from_static(b"WITHSCORES"))),
                ]),
                RespFrame::Array(vec![
                    RespFrame::BulkString(Some(Bytes::from_static(b"ZRANGEBYSCORE"))),
                    RespFrame::BulkString(Some(Bytes::from_static(b"zset"))),
                    RespFrame::BulkString(Some(Bytes::from_static(b"2"))),
                    RespFrame::BulkString(Some(Bytes::from_static(b"3"))),
                ]),
                RespFrame::Array(vec![
                    RespFrame::BulkString(Some(Bytes::from_static(b"TTL"))),
                    RespFrame::BulkString(Some(Bytes::from_static(b"key"))),
                ]),
            ],
            &shared,
            &mut client,
        )
        .await
        .expect("readonly batch should accept non-fast readonly commands");

        assert_eq!(outcomes.len(), 5);
        assert_eq!(outcomes[0].response, RespFrame::Integer(7));
        assert_eq!(
            outcomes[1].response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"one"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"two"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"three"))),
            ])
        );
        assert_eq!(
            outcomes[2].response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"three"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"3"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"two"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"2"))),
            ])
        );
        assert_eq!(
            outcomes[3].response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"two"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"three"))),
            ])
        );
        assert_eq!(outcomes[4].response, RespFrame::Integer(-1));

        let server = shared.meta.lock().await;
        assert_eq!(shared.stats.total_commands_processed(), 5);
        assert_eq!(server.stats.total_commands_processed(), 5);
    }

    #[tokio::test]
    async fn pipelined_lock_free_bitmap_hash_and_zset_rank_reads_work() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;
        {
            let mut hash = HashMap::new();
            hash.insert(
                Bytes::from_static(b"field"),
                HashFieldEntry::new(Bytes::from_static(b"payload")),
            );
            let mut zset = SortedSet::default();
            assert!(zset.insert(Bytes::from_static(b"alpha"), 1.0));
            assert!(zset.insert(Bytes::from_static(b"beta"), 2.0));
            assert!(zset.insert(Bytes::from_static(b"gamma"), 3.0));

            let mut db = shared.data.write_db(0);
            db.data
                .insert(Bytes::from_static(b"hash"), StoredValue::hash(hash, None));
            db.data.insert(
                Bytes::from_static(b"bits"),
                StoredValue::string(Bytes::from_static(b"A"), None),
            );
            db.data.insert(
                Bytes::from_static(b"zset"),
                StoredValue::sorted_set(zset, None),
            );
        }

        client
            .write_all(
                b"HSTRLEN hash field\r\nGETBIT bits 1\r\nZMSCORE zset beta missing\r\nZCOUNT zset 1 2\r\nZRANK zset beta\r\nZREVRANK zset beta\r\n",
            )
            .await
            .expect("write readonly bitmap/hash/zset batch");

        let expected = b":7\r\n:1\r\n*2\r\n$1\r\n2\r\n$-1\r\n:2\r\n:1\r\n:1\r\n";
        let reply = read_exact_reply(&mut client, expected.len()).await;
        assert_eq!(reply, expected);

        client.write_all(b"QUIT\r\n").await.expect("write quit");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");

        server_task.await.expect("server task complete");

        let server = shared.meta.lock().await;
        assert_eq!(
            shared.stats.total_commands_processed(),
            server.stats.total_commands_processed()
        );
        assert_eq!(shared.stats.total_commands_processed(), 7);
    }

    #[tokio::test]
    async fn pipelined_lock_free_bitcount_reads_work() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;
        {
            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"bits"),
                StoredValue::string(Bytes::from_static(b"AB"), None),
            );
        }

        client
            .write_all(b"BITCOUNT bits\r\nBITCOUNT bits -1 -1\r\nBITCOUNT bits 0 3 BIT\r\n")
            .await
            .expect("write readonly bitcount batch");

        let expected = b":4\r\n:2\r\n:1\r\n";
        let reply = read_exact_reply(&mut client, expected.len()).await;
        assert_eq!(reply, expected);

        client.write_all(b"QUIT\r\n").await.expect("write quit");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");

        server_task.await.expect("server task complete");

        let server = shared.meta.lock().await;
        assert_eq!(
            shared.stats.total_commands_processed(),
            server.stats.total_commands_processed()
        );
        assert_eq!(shared.stats.total_commands_processed(), 4);
    }

    #[tokio::test]
    async fn pipelined_lock_free_zrange_reads_work() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;
        {
            let mut zset = SortedSet::default();
            assert!(zset.insert(Bytes::from_static(b"alpha"), 1.0));
            assert!(zset.insert(Bytes::from_static(b"beta"), 1.0));
            assert!(zset.insert(Bytes::from_static(b"gamma"), 2.0));

            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"zset"),
                StoredValue::sorted_set(zset, None),
            );
        }

        client
            .write_all(
                b"ZRANGE zset 0 -1\r\nZRANGE zset 0 1 WITHSCORES\r\nZRANGE zset 2 1 BYSCORE REV LIMIT 0 2\r\nZREVRANGE zset 0 1 WITHSCORES\r\nZRANGEBYSCORE zset 1 2 WITHSCORES LIMIT 1 2\r\n",
            )
            .await
            .expect("write readonly zrange batch");

        let expected = b"*3\r\n$5\r\nalpha\r\n$4\r\nbeta\r\n$5\r\ngamma\r\n*4\r\n$5\r\nalpha\r\n$1\r\n1\r\n$4\r\nbeta\r\n$1\r\n1\r\n*2\r\n$5\r\ngamma\r\n$4\r\nbeta\r\n*4\r\n$5\r\ngamma\r\n$1\r\n2\r\n$4\r\nbeta\r\n$1\r\n1\r\n*4\r\n$4\r\nbeta\r\n$1\r\n1\r\n$5\r\ngamma\r\n$1\r\n2\r\n";
        let reply = read_exact_reply(&mut client, expected.len()).await;
        assert_eq!(reply, expected);

        client.write_all(b"QUIT\r\n").await.expect("write quit");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");

        server_task.await.expect("server task complete");

        let server = shared.meta.lock().await;
        assert_eq!(
            shared.stats.total_commands_processed(),
            server.stats.total_commands_processed()
        );
        assert_eq!(shared.stats.total_commands_processed(), 6);
    }

    #[tokio::test]
    async fn pipelined_lock_free_ttl_family_reads_work() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;
        let expire_at_ms = ratatosk_core::time::now_ms().saturating_add(10_000);
        {
            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"expiring"),
                StoredValue::string(Bytes::from_static(b"value"), Some(expire_at_ms)),
            );
            db.data.insert(
                Bytes::from_static(b"persistent"),
                StoredValue::string(Bytes::from_static(b"value"), None),
            );
        }

        client
            .write_all(
                b"EXPIRETIME expiring\r\nPEXPIRETIME expiring\r\nTTL persistent\r\nPTTL missing\r\n",
            )
            .await
            .expect("write readonly ttl batch");

        let expected = format!(
            ":{}\r\n:{}\r\n:-1\r\n:-2\r\n",
            expire_at_ms / 1000,
            expire_at_ms
        );
        let reply = read_exact_reply(&mut client, expected.len()).await;
        assert_eq!(reply, expected.as_bytes());

        client.write_all(b"QUIT\r\n").await.expect("write quit");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");

        server_task.await.expect("server task complete");

        let server = shared.meta.lock().await;
        assert_eq!(
            shared.stats.total_commands_processed(),
            server.stats.total_commands_processed()
        );
        assert_eq!(shared.stats.total_commands_processed(), 5);
    }

    #[tokio::test]
    async fn oversized_response_disconnects_client() {
        let limits = ClientIoLimits {
            output_buffer_limit_bytes: 256,
            client_read_timeout_sec: 0,
        };
        let (mut client, server_task) = setup_client_server_with_limits(limits).await;

        let payload = "x".repeat(1024);
        let command = format!("ECHO {payload}\r\n");
        client
            .write_all(command.as_bytes())
            .await
            .expect("write oversized echo");

        let reply = read_reply(&mut client).await;
        assert_eq!(reply, b"-ERR output buffer limit exceeded\r\n");

        let mut eof = [0u8; 1];
        let n = client.read(&mut eof).await.expect("read eof");
        assert_eq!(n, 0);

        server_task.await.expect("server task complete");
    }

    #[tokio::test]
    async fn peer_close_after_ping_cleans_up_connection_state() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;

        client.write_all(b"PING\r\n").await.expect("write ping");
        assert_eq!(read_reply(&mut client).await, b"+PONG\r\n");

        drop(client);

        timeout(Duration::from_secs(1), server_task)
            .await
            .expect("server task timeout")
            .expect("server task complete");

        assert_eq!(shared.stats.connected_clients(), 0);
        assert_eq!(shared.stats.total_connections_received(), 1);

        let server = shared.meta.lock().await;
        assert_eq!(server.stats.connected_clients(), 0);
        assert_eq!(server.stats.total_connections_received(), 1);
        assert_eq!(server.connected_client_snapshots(), 0);
        assert_eq!(server.blocked_clients(), 0);
        assert_eq!(server.tracking_clients(), 0);
        assert_eq!(server.monitor_client_count(), 0);
        assert!(server.client_snapshot(1).is_none());
        assert!(server.pubsub.client_channels(1).is_empty());
        assert!(server.pubsub.client_shard_channels(1).is_empty());
        assert!(server.pubsub.client_patterns(1).is_empty());
    }

    #[tokio::test]
    async fn peer_close_after_subscribe_cleans_up_pubsub_state() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;

        client
            .write_all(b"*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nnews\r\n")
            .await
            .expect("subscribe");
        let subscribe_reply = read_reply(&mut client).await;
        assert!(
            subscribe_reply
                .windows(b"subscribe".len())
                .any(|window| window == b"subscribe")
        );

        drop(client);

        timeout(Duration::from_secs(1), server_task)
            .await
            .expect("server task timeout")
            .expect("server task complete");

        let server = shared.meta.lock().await;
        assert_eq!(server.connected_client_snapshots(), 0);
        assert!(server.client_snapshot(1).is_none());
        assert!(server.pubsub.client_channels(1).is_empty());
        assert_eq!(server.pubsub.numsub(&[Bytes::from("news")])[0].1, 0);
    }

    #[tokio::test]
    async fn repeated_peer_closes_do_not_leave_connected_clients_behind() {
        const CONNECTIONS: usize = 8;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        let shared_for_accept = Arc::clone(&shared);

        let accept_task = tokio::spawn(async move {
            let persistence = Arc::new(
                PersistenceRuntime::from_config(&crate::config::ServerConfig::default())
                    .expect("persistence runtime"),
            );
            let mut tasks = Vec::with_capacity(CONNECTIONS);

            for _ in 0..CONNECTIONS {
                let (socket, _) = listener.accept().await.expect("accept");
                let shared = Arc::clone(&shared_for_accept);
                let persistence = Arc::clone(&persistence);
                tasks.push(tokio::spawn(async move {
                    let info = ConnInfo::from_tcp(&socket);
                    handle_client_with_limits(
                        socket,
                        info,
                        shared,
                        persistence,
                        ClientIoLimits::default(),
                    )
                    .await
                    .expect("handle client");
                }));
            }

            for task in tasks {
                task.await.expect("join client task");
            }
        });

        for _ in 0..CONNECTIONS {
            let mut client = TcpStream::connect(addr).await.expect("connect client");
            client.write_all(b"PING\r\n").await.expect("write ping");
            assert_eq!(read_reply(&mut client).await, b"+PONG\r\n");
            drop(client);
        }

        timeout(Duration::from_secs(2), accept_task)
            .await
            .expect("accept task timeout")
            .expect("accept task complete");

        assert_eq!(shared.stats.connected_clients(), 0);
        assert_eq!(
            shared.stats.total_connections_received(),
            CONNECTIONS as u64
        );

        let server = shared.meta.lock().await;
        assert_eq!(server.stats.connected_clients(), 0);
        assert_eq!(
            server.stats.total_connections_received(),
            CONNECTIONS as u64
        );
        assert_eq!(server.connected_client_snapshots(), 0);
    }

    #[tokio::test]
    async fn info_stats_reports_total_connections_received_from_runtime_stats() {
        let (mut client, server_task) = setup_client_server().await;

        client
            .write_all(b"INFO stats\r\n")
            .await
            .expect("write info stats");
        let info = read_reply(&mut client).await;
        let info_text = String::from_utf8_lossy(&info);
        assert!(
            info_text.contains("total_connections_received:1"),
            "{info_text}"
        );

        client.write_all(b"QUIT\r\n").await.expect("quit");
        let _ = read_reply(&mut client).await;
        server_task.await.expect("server task complete");
    }

    #[tokio::test]
    async fn runtime_stats_keep_atomic_and_meta_counters_in_sync() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;

        client
            .write_all(b"PING\r\nECHO hi\r\nQUIT\r\n")
            .await
            .expect("write commands");

        let expected = b"+PONG\r\n$2\r\nhi\r\n+OK\r\n";
        let reply = read_exact_reply(&mut client, expected.len()).await;
        assert_eq!(reply, expected);

        server_task.await.expect("server task complete");

        let server = shared.meta.lock().await;
        assert_eq!(
            shared.stats.total_commands_processed(),
            server.stats.total_commands_processed()
        );
        assert_eq!(
            shared.stats.total_net_input_bytes(),
            server.stats.total_net_input_bytes()
        );
        assert_eq!(
            shared.stats.total_net_output_bytes(),
            server.stats.total_net_output_bytes()
        );
        assert_eq!(shared.stats.total_commands_processed(), 3);
        assert!(shared.stats.total_net_input_bytes() > 0);
        assert!(shared.stats.total_net_output_bytes() > 0);
    }

    #[tokio::test]
    async fn config_set_query_buffer_limit_applies_to_current_connection() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;

        client
            .write_all(b"CONFIG SET query-buffer-limit 1024\r\n")
            .await
            .expect("set query buffer limit");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");
        assert_eq!(shared.config_cache.load().query_buffer_limit(), 1024);

        let payload = "x".repeat(1100);
        let command = format!("ECHO {payload}\r\n");
        client
            .write_all(command.as_bytes())
            .await
            .expect("write oversized query");

        let reply = read_reply(&mut client).await;
        assert_eq!(reply, b"-ERR query buffer limit exceeded\r\n");

        let mut eof = [0u8; 1];
        let n = client.read(&mut eof).await.expect("read eof");
        assert_eq!(n, 0);

        server_task.await.expect("server task complete");
    }

    #[tokio::test]
    async fn config_set_hz_round_trips_and_updates_config_cache() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;

        client
            .write_all(b"CONFIG SET hz 25\r\n")
            .await
            .expect("set hz");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");
        assert_eq!(shared.config_cache.load().hz(), 25);

        client
            .write_all(b"CONFIG GET hz\r\n")
            .await
            .expect("get hz");
        assert_eq!(
            read_reply(&mut client).await,
            b"*2\r\n$2\r\nhz\r\n$2\r\n25\r\n"
        );

        client.write_all(b"QUIT\r\n").await.expect("quit");
        let _ = read_reply(&mut client).await;
        server_task.await.expect("server task complete");
    }

    #[tokio::test]
    async fn pubsub_cross_client_fanout() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));

        let shared_for_accept = Arc::clone(&shared);
        let accept_task = tokio::spawn(async move {
            let (sock_a, _) = listener.accept().await.expect("accept a");
            let (sock_b, _) = listener.accept().await.expect("accept b");

            let shared_a = Arc::clone(&shared_for_accept);
            let task_a = tokio::spawn(async move {
                handle_client(sock_a, shared_a).await.expect("handle a");
            });
            let shared_b = Arc::clone(&shared_for_accept);
            let task_b = tokio::spawn(async move {
                handle_client(sock_b, shared_b).await.expect("handle b");
            });

            task_a.await.expect("join a");
            task_b.await.expect("join b");
        });

        let mut sub = TcpStream::connect(addr).await.expect("connect sub");
        let mut pubc = TcpStream::connect(addr).await.expect("connect pub");

        sub.write_all(b"*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nnews\r\n")
            .await
            .expect("subscribe");
        let sub_ack = read_reply(&mut sub).await;
        assert!(
            sub_ack
                .windows(b"subscribe".len())
                .any(|w| w == b"subscribe")
        );

        pubc.write_all(b"*3\r\n$7\r\nPUBLISH\r\n$4\r\nnews\r\n$5\r\nhello\r\n")
            .await
            .expect("publish");
        let pub_reply = read_reply(&mut pubc).await;
        assert_eq!(pub_reply, b":1\r\n");

        let pushed = read_reply(&mut sub).await;
        assert!(pushed.windows(b"message".len()).any(|w| w == b"message"));
        assert!(pushed.windows(b"news".len()).any(|w| w == b"news"));
        assert!(pushed.windows(b"hello".len()).any(|w| w == b"hello"));

        sub.write_all(b"*2\r\n$10\r\nSSUBSCRIBE\r\n$6\r\nshard1\r\n")
            .await
            .expect("ssubscribe");
        let ssub_ack = read_reply(&mut sub).await;
        assert!(
            ssub_ack
                .windows(b"ssubscribe".len())
                .any(|w| w == b"ssubscribe")
        );

        pubc.write_all(b"*3\r\n$8\r\nSPUBLISH\r\n$6\r\nshard1\r\n$5\r\nworld\r\n")
            .await
            .expect("spublish");
        let spub_reply = read_reply(&mut pubc).await;
        assert_eq!(spub_reply, b":1\r\n");

        let shard_pushed = read_reply(&mut sub).await;
        assert!(
            shard_pushed
                .windows(b"smessage".len())
                .any(|w| w == b"smessage")
        );
        assert!(
            shard_pushed
                .windows(b"shard1".len())
                .any(|w| w == b"shard1")
        );
        assert!(shard_pushed.windows(b"world".len()).any(|w| w == b"world"));

        sub.write_all(b"QUIT\r\n").await.expect("quit sub");
        let _ = read_reply(&mut sub).await;
        pubc.write_all(b"QUIT\r\n").await.expect("quit pub");
        let _ = read_reply(&mut pubc).await;

        accept_task.await.expect("accept task join");
    }

    #[tokio::test]
    async fn client_tracking_pushes_invalidation_cross_client() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));

        let shared_for_accept = Arc::clone(&shared);
        let accept_task = tokio::spawn(async move {
            let (sock_tracker, _) = listener.accept().await.expect("accept tracker");
            let (sock_writer, _) = listener.accept().await.expect("accept writer");

            let shared_tracker = Arc::clone(&shared_for_accept);
            let task_tracker = tokio::spawn(async move {
                handle_client(sock_tracker, shared_tracker)
                    .await
                    .expect("handle tracker");
            });
            let shared_writer = Arc::clone(&shared_for_accept);
            let task_writer = tokio::spawn(async move {
                handle_client(sock_writer, shared_writer)
                    .await
                    .expect("handle writer");
            });

            task_tracker.await.expect("join tracker");
            task_writer.await.expect("join writer");
        });

        let mut tracker = TcpStream::connect(addr).await.expect("connect tracker");
        let mut writer = TcpStream::connect(addr).await.expect("connect writer");

        writer
            .write_all(b"*3\r\n$3\r\nSET\r\n$7\r\ntracked\r\n$2\r\nv1\r\n")
            .await
            .expect("seed tracked key");
        assert_eq!(read_reply(&mut writer).await, b"+OK\r\n");

        tracker
            .write_all(b"*3\r\n$6\r\nCLIENT\r\n$8\r\nTRACKING\r\n$2\r\nON\r\n")
            .await
            .expect("enable tracking");
        assert_eq!(read_reply(&mut tracker).await, b"+OK\r\n");

        tracker
            .write_all(b"*2\r\n$3\r\nGET\r\n$7\r\ntracked\r\n")
            .await
            .expect("read tracked key");
        assert_eq!(read_reply(&mut tracker).await, b"$2\r\nv1\r\n");

        writer
            .write_all(b"*3\r\n$3\r\nSET\r\n$7\r\ntracked\r\n$2\r\nv2\r\n")
            .await
            .expect("update tracked key");
        assert_eq!(read_reply(&mut writer).await, b"+OK\r\n");

        let invalidation = read_reply(&mut tracker).await;
        assert!(
            invalidation
                .windows(b"invalidate".len())
                .any(|w| w == b"invalidate"),
            "expected invalidate push, got {:?}",
            String::from_utf8_lossy(&invalidation)
        );
        assert!(
            invalidation
                .windows(b"tracked".len())
                .any(|w| w == b"tracked"),
            "expected tracked key in invalidate push, got {:?}",
            String::from_utf8_lossy(&invalidation)
        );

        tracker.write_all(b"QUIT\r\n").await.expect("quit tracker");
        let _ = read_reply(&mut tracker).await;
        writer.write_all(b"QUIT\r\n").await.expect("quit writer");
        let _ = read_reply(&mut writer).await;

        accept_task.await.expect("accept task join");
    }

    #[tokio::test]
    async fn client_tracking_bcast_prefix_pushes_matching_invalidation() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));

        let shared_for_accept = Arc::clone(&shared);
        let accept_task = tokio::spawn(async move {
            let (sock_tracker, _) = listener.accept().await.expect("accept tracker");
            let (sock_writer, _) = listener.accept().await.expect("accept writer");

            let shared_tracker = Arc::clone(&shared_for_accept);
            let task_tracker = tokio::spawn(async move {
                handle_client(sock_tracker, shared_tracker)
                    .await
                    .expect("handle tracker");
            });
            let shared_writer = Arc::clone(&shared_for_accept);
            let task_writer = tokio::spawn(async move {
                handle_client(sock_writer, shared_writer)
                    .await
                    .expect("handle writer");
            });

            task_tracker.await.expect("join tracker");
            task_writer.await.expect("join writer");
        });

        let mut tracker = TcpStream::connect(addr).await.expect("connect tracker");
        let mut writer = TcpStream::connect(addr).await.expect("connect writer");

        tracker
            .write_all(
                b"*6\r\n$6\r\nCLIENT\r\n$8\r\nTRACKING\r\n$2\r\nON\r\n$5\r\nBCAST\r\n$6\r\nPREFIX\r\n$5\r\nuser:\r\n",
            )
            .await
            .expect("enable bcast prefix tracking");
        assert_eq!(read_reply(&mut tracker).await, b"+OK\r\n");

        writer
            .write_all(b"*3\r\n$3\r\nSET\r\n$6\r\nother:\r\n$2\r\nv1\r\n")
            .await
            .expect("write non matching key");
        assert_eq!(read_reply(&mut writer).await, b"+OK\r\n");

        writer
            .write_all(b"*3\r\n$3\r\nSET\r\n$6\r\nuser:1\r\n$2\r\nv2\r\n")
            .await
            .expect("write matching key");
        assert_eq!(read_reply(&mut writer).await, b"+OK\r\n");

        let invalidation = read_reply(&mut tracker).await;
        assert!(
            invalidation
                .windows(b"invalidate".len())
                .any(|w| w == b"invalidate"),
            "expected invalidate push, got {:?}",
            String::from_utf8_lossy(&invalidation)
        );
        assert!(
            invalidation
                .windows(b"user:1".len())
                .any(|w| w == b"user:1"),
            "expected matching key in invalidate push, got {:?}",
            String::from_utf8_lossy(&invalidation)
        );
        assert!(
            !invalidation
                .windows(b"other:".len())
                .any(|w| w == b"other:"),
            "unexpected non-matching key in invalidate push, got {:?}",
            String::from_utf8_lossy(&invalidation)
        );

        tracker.write_all(b"QUIT\r\n").await.expect("quit tracker");
        let _ = read_reply(&mut tracker).await;
        writer.write_all(b"QUIT\r\n").await.expect("quit writer");
        let _ = read_reply(&mut writer).await;

        accept_task.await.expect("accept task join");
    }

    #[tokio::test]
    async fn client_tracking_redirect_pushes_invalidation_to_target() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));

        let shared_for_accept = Arc::clone(&shared);
        let accept_task = tokio::spawn(async move {
            let (sock_tracker, _) = listener.accept().await.expect("accept tracker");
            let (sock_target, _) = listener.accept().await.expect("accept target");
            let (sock_writer, _) = listener.accept().await.expect("accept writer");

            let shared_tracker = Arc::clone(&shared_for_accept);
            let task_tracker = tokio::spawn(async move {
                handle_client(sock_tracker, shared_tracker)
                    .await
                    .expect("handle tracker");
            });
            let shared_target = Arc::clone(&shared_for_accept);
            let task_target = tokio::spawn(async move {
                handle_client(sock_target, shared_target)
                    .await
                    .expect("handle target");
            });
            let shared_writer = Arc::clone(&shared_for_accept);
            let task_writer = tokio::spawn(async move {
                handle_client(sock_writer, shared_writer)
                    .await
                    .expect("handle writer");
            });

            task_tracker.await.expect("join tracker");
            task_target.await.expect("join target");
            task_writer.await.expect("join writer");
        });

        let mut tracker = TcpStream::connect(addr).await.expect("connect tracker");
        let mut target = TcpStream::connect(addr).await.expect("connect target");
        let mut writer = TcpStream::connect(addr).await.expect("connect writer");

        target
            .write_all(b"*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n")
            .await
            .expect("request target id");
        let target_id = parse_integer_reply(&read_reply(&mut target).await);

        let tracking_command = format!(
            "*5\r\n$6\r\nCLIENT\r\n$8\r\nTRACKING\r\n$2\r\nON\r\n$8\r\nREDIRECT\r\n${}\r\n{}\r\n",
            target_id.to_string().len(),
            target_id
        );
        tracker
            .write_all(tracking_command.as_bytes())
            .await
            .expect("enable redirect tracking");
        assert_eq!(read_reply(&mut tracker).await, b"+OK\r\n");

        writer
            .write_all(b"*3\r\n$3\r\nSET\r\n$7\r\ntracked\r\n$2\r\nv1\r\n")
            .await
            .expect("seed tracked key");
        assert_eq!(read_reply(&mut writer).await, b"+OK\r\n");

        tracker
            .write_all(b"*2\r\n$3\r\nGET\r\n$7\r\ntracked\r\n")
            .await
            .expect("read tracked key");
        assert_eq!(read_reply(&mut tracker).await, b"$2\r\nv1\r\n");

        writer
            .write_all(b"*3\r\n$3\r\nSET\r\n$7\r\ntracked\r\n$2\r\nv2\r\n")
            .await
            .expect("update tracked key");
        assert_eq!(read_reply(&mut writer).await, b"+OK\r\n");

        let invalidation = read_reply(&mut target).await;
        assert!(
            invalidation
                .windows(b"invalidate".len())
                .any(|w| w == b"invalidate"),
            "expected invalidate push, got {:?}",
            String::from_utf8_lossy(&invalidation)
        );
        assert!(
            invalidation
                .windows(b"tracked".len())
                .any(|w| w == b"tracked"),
            "expected tracked key in invalidate push, got {:?}",
            String::from_utf8_lossy(&invalidation)
        );

        tracker.write_all(b"QUIT\r\n").await.expect("quit tracker");
        let _ = read_reply(&mut tracker).await;
        target.write_all(b"QUIT\r\n").await.expect("quit target");
        let _ = read_reply(&mut target).await;
        writer.write_all(b"QUIT\r\n").await.expect("quit writer");
        let _ = read_reply(&mut writer).await;

        accept_task.await.expect("accept task join");
    }

    #[tokio::test]
    async fn client_tracking_redirect_disconnect_marks_broken_redirect_and_falls_back() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));

        let shared_for_accept = Arc::clone(&shared);
        let accept_task = tokio::spawn(async move {
            let (sock_tracker, _) = listener.accept().await.expect("accept tracker");
            let (sock_target, _) = listener.accept().await.expect("accept target");
            let (sock_writer, _) = listener.accept().await.expect("accept writer");

            let shared_tracker = Arc::clone(&shared_for_accept);
            let task_tracker = tokio::spawn(async move {
                handle_client(sock_tracker, shared_tracker)
                    .await
                    .expect("handle tracker");
            });
            let shared_target = Arc::clone(&shared_for_accept);
            let task_target = tokio::spawn(async move {
                handle_client(sock_target, shared_target)
                    .await
                    .expect("handle target");
            });
            let shared_writer = Arc::clone(&shared_for_accept);
            let task_writer = tokio::spawn(async move {
                handle_client(sock_writer, shared_writer)
                    .await
                    .expect("handle writer");
            });

            task_tracker.await.expect("join tracker");
            task_target.await.expect("join target");
            task_writer.await.expect("join writer");
        });

        let mut tracker = TcpStream::connect(addr).await.expect("connect tracker");
        let mut target = TcpStream::connect(addr).await.expect("connect target");
        let mut writer = TcpStream::connect(addr).await.expect("connect writer");

        tracker
            .write_all(b"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n")
            .await
            .expect("switch tracker to resp3");
        let hello = read_reply(&mut tracker).await;
        assert!(
            hello.windows(b"proto".len()).any(|w| w == b"proto"),
            "expected HELLO map, got {:?}",
            String::from_utf8_lossy(&hello)
        );

        target
            .write_all(b"*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n")
            .await
            .expect("request target id");
        let target_id = parse_integer_reply(&read_reply(&mut target).await);

        let tracking_command = format!(
            "*5\r\n$6\r\nCLIENT\r\n$8\r\nTRACKING\r\n$2\r\nON\r\n$8\r\nREDIRECT\r\n${}\r\n{}\r\n",
            target_id.to_string().len(),
            target_id
        );
        tracker
            .write_all(tracking_command.as_bytes())
            .await
            .expect("enable redirect tracking");
        assert_eq!(read_reply(&mut tracker).await, b"+OK\r\n");

        writer
            .write_all(b"*3\r\n$3\r\nSET\r\n$7\r\ntracked\r\n$2\r\nv1\r\n")
            .await
            .expect("seed tracked key");
        assert_eq!(read_reply(&mut writer).await, b"+OK\r\n");

        tracker
            .write_all(b"*2\r\n$3\r\nGET\r\n$7\r\ntracked\r\n")
            .await
            .expect("read tracked key");
        assert_eq!(read_reply(&mut tracker).await, b"$2\r\nv1\r\n");

        target.write_all(b"QUIT\r\n").await.expect("quit target");
        let _ = read_reply(&mut target).await;
        let target_id_text = target_id.to_string();
        let broken_redirect = read_reply(&mut tracker).await;
        assert_eq!(broken_redirect.first().copied(), Some(b'>'));
        assert!(
            broken_redirect
                .windows(b"tracking-redir-broken".len())
                .any(|w| w == b"tracking-redir-broken"),
            "expected tracking-redir-broken push, got {:?}",
            String::from_utf8_lossy(&broken_redirect)
        );
        assert!(
            broken_redirect
                .windows(target_id_text.len())
                .any(|w| w == target_id_text.as_bytes()),
            "expected broken redirect id in push, got {:?}",
            String::from_utf8_lossy(&broken_redirect)
        );

        tracker
            .write_all(b"*2\r\n$6\r\nCLIENT\r\n$8\r\nGETREDIR\r\n")
            .await
            .expect("request active redirect");
        assert_eq!(
            read_reply(&mut tracker).await,
            format!(":{}\r\n", target_id).as_bytes()
        );

        writer
            .write_all(b"*3\r\n$3\r\nSET\r\n$7\r\ntracked\r\n$2\r\nv2\r\n")
            .await
            .expect("update tracked key");
        assert_eq!(read_reply(&mut writer).await, b"+OK\r\n");

        let invalidation = read_reply(&mut tracker).await;
        assert!(
            invalidation
                .windows(b"invalidate".len())
                .any(|w| w == b"invalidate"),
            "expected invalidate push, got {:?}",
            String::from_utf8_lossy(&invalidation)
        );
        assert!(
            invalidation
                .windows(b"tracked".len())
                .any(|w| w == b"tracked"),
            "expected tracked key in invalidate push, got {:?}",
            String::from_utf8_lossy(&invalidation)
        );

        tracker.write_all(b"QUIT\r\n").await.expect("quit tracker");
        let _ = read_reply(&mut tracker).await;
        writer.write_all(b"QUIT\r\n").await.expect("quit writer");
        let _ = read_reply(&mut writer).await;

        accept_task.await.expect("accept task join");
    }

    #[tokio::test]
    async fn client_registry_reports_blocked_and_tracking_clients() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));

        let shared_for_accept = Arc::clone(&shared);
        let accept_task = tokio::spawn(async move {
            let (sock_blocked, _) = listener.accept().await.expect("accept blocked");
            let (sock_observer, _) = listener.accept().await.expect("accept observer");

            let shared_blocked = Arc::clone(&shared_for_accept);
            let task_blocked = tokio::spawn(async move {
                handle_client(sock_blocked, shared_blocked)
                    .await
                    .expect("handle blocked");
            });
            let shared_observer = Arc::clone(&shared_for_accept);
            let task_observer = tokio::spawn(async move {
                handle_client(sock_observer, shared_observer)
                    .await
                    .expect("handle observer");
            });

            task_blocked.await.expect("join blocked");
            task_observer.await.expect("join observer");
        });

        let mut blocked = TcpStream::connect(addr).await.expect("connect blocked");
        let mut observer = TcpStream::connect(addr).await.expect("connect observer");

        observer
            .write_all(b"*3\r\n$6\r\nCLIENT\r\n$8\r\nTRACKING\r\n$2\r\nON\r\n")
            .await
            .expect("enable tracking");
        assert_eq!(read_reply(&mut observer).await, b"+OK\r\n");

        blocked
            .write_all(b"*3\r\n$5\r\nBLPOP\r\n$7\r\nmissing\r\n$1\r\n1\r\n")
            .await
            .expect("start blocking pop");
        tokio::time::sleep(Duration::from_millis(50)).await;

        observer
            .write_all(b"*2\r\n$4\r\nINFO\r\n$7\r\nclients\r\n")
            .await
            .expect("info clients");
        let info = read_reply(&mut observer).await;
        let info_text = String::from_utf8_lossy(&info);
        assert!(info_text.contains("connected_clients:2"), "{info_text}");
        assert!(info_text.contains("blocked_clients:1"), "{info_text}");
        assert!(info_text.contains("tracking_clients:1"), "{info_text}");

        observer
            .write_all(b"*2\r\n$6\r\nCLIENT\r\n$4\r\nLIST\r\n")
            .await
            .expect("client list");
        let list = read_reply(&mut observer).await;
        let list_text = String::from_utf8_lossy(&list);
        assert!(list_text.contains("flags=Nt"), "{list_text}");
        assert!(list_text.contains("flags=Nb"), "{list_text}");

        let blocked_reply = read_reply(&mut blocked).await;
        assert!(
            blocked_reply == b"*-1\r\n" || blocked_reply == b"$-1\r\n",
            "unexpected BLPOP timeout reply: {:?}",
            String::from_utf8_lossy(&blocked_reply)
        );

        blocked.write_all(b"QUIT\r\n").await.expect("quit blocked");
        let _ = read_reply(&mut blocked).await;
        observer
            .write_all(b"QUIT\r\n")
            .await
            .expect("quit observer");
        let _ = read_reply(&mut observer).await;

        accept_task.await.expect("accept task join");
    }

    #[tokio::test]
    async fn blocked_client_disconnect_finishes_session_and_cleans_up() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        let shared_for_server = Arc::clone(&shared);

        let server_task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept blocked client");
            handle_client(socket, shared_for_server).await
        });

        let mut client = TcpStream::connect(addr)
            .await
            .expect("connect blocked client");
        client
            .write_all(b"*3\r\n$5\r\nBLPOP\r\n$1\r\nk\r\n$1\r\n0\r\n")
            .await
            .expect("start blocking pop");

        timeout(Duration::from_secs(1), async {
            loop {
                if shared.meta.lock().await.blocked_clients() == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("blocked client was not registered");

        drop(client);

        let _session_result = timeout(Duration::from_secs(2), server_task)
            .await
            .expect("blocked client session timeout")
            .expect("blocked client session join");

        assert_eq!(shared.stats.connected_clients(), 0);
        let server = shared.meta.lock().await;
        assert_eq!(server.blocked_clients(), 0, "blocked snapshot must be gone");
        assert!(
            !server.write_observers_active(),
            "blocking registry must not keep waiters for a disconnected client"
        );
    }

    /// A client that pipelines more bytes behind a blocking command and then
    /// closes is torn down without executing the pipelined command. This is the
    /// Redis behaviour (a blocked client that hits EOF is freed) and pins the
    /// `peer_closed` semantics: read-closed with unread bytes still ends the session.
    #[tokio::test]
    async fn blocked_client_close_with_pipelined_bytes_ends_session() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        let shared_for_server = Arc::clone(&shared);

        let server_task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept blocked client");
            handle_client(socket, shared_for_server).await
        });

        let mut client = TcpStream::connect(addr)
            .await
            .expect("connect blocked client");
        client
            .write_all(b"*3\r\n$5\r\nBLPOP\r\n$1\r\nk\r\n$1\r\n0\r\n")
            .await
            .expect("start blocking pop");

        timeout(Duration::from_secs(1), async {
            loop {
                if shared.meta.lock().await.blocked_clients() == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("blocked client was not registered");

        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n")
            .await
            .expect("pipeline a write behind the block");
        drop(client);

        let _session_result = timeout(Duration::from_secs(2), server_task)
            .await
            .expect("blocked client session timeout")
            .expect("blocked client session join");

        assert_eq!(shared.stats.connected_clients(), 0);
        let server = shared.meta.lock().await;
        assert!(
            server.db(0).get(&Bytes::from_static(b"a")).is_none(),
            "pipelined command behind a closed blocking client must not execute"
        );
        assert_eq!(server.blocked_clients(), 0);
        assert!(!server.write_observers_active());
    }

    /// Bytes pipelined behind a blocking command are buffered while the command
    /// is parked (consuming socket readiness instead of spinning on it) and are
    /// executed in order once the blocking reply has been sent — for both a
    /// wake-up and a timeout completion.
    #[tokio::test]
    async fn pipelined_command_behind_blocking_pop_runs_after_it_completes() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));

        let shared_for_accept = Arc::clone(&shared);
        let accept_task = tokio::spawn(async move {
            let (sock_blocked, _) = listener.accept().await.expect("accept blocked");
            let (sock_writer, _) = listener.accept().await.expect("accept writer");
            let shared_blocked = Arc::clone(&shared_for_accept);
            let task_blocked = tokio::spawn(async move {
                handle_client(sock_blocked, shared_blocked)
                    .await
                    .expect("handle blocked");
            });
            let shared_writer = Arc::clone(&shared_for_accept);
            let task_writer = tokio::spawn(async move {
                handle_client(sock_writer, shared_writer)
                    .await
                    .expect("handle writer");
            });
            task_blocked.await.expect("join blocked");
            task_writer.await.expect("join writer");
        });

        let mut blocked = TcpStream::connect(addr).await.expect("connect blocked");
        let mut writer = TcpStream::connect(addr).await.expect("connect writer");

        // 1. Wake-up path: BLPOP parked, then PING pipelined behind it.
        blocked
            .write_all(b"*3\r\n$5\r\nBLPOP\r\n$7\r\nwake-me\r\n$1\r\n5\r\n")
            .await
            .expect("start blocking pop");
        timeout(Duration::from_secs(1), async {
            loop {
                if shared.meta.lock().await.blocked_clients() == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("blocked client was not registered");
        blocked
            .write_all(b"*1\r\n$4\r\nPING\r\n")
            .await
            .expect("pipeline ping behind the block");
        // Give the parked session a moment: it must absorb the bytes, not answer yet.
        tokio::time::sleep(Duration::from_millis(50)).await;

        writer
            .write_all(b"*3\r\n$5\r\nLPUSH\r\n$7\r\nwake-me\r\n$7\r\npayload\r\n")
            .await
            .expect("push payload");
        assert_eq!(read_reply(&mut writer).await, b":1\r\n");

        let mut reply = read_reply(&mut blocked).await;
        if !reply.ends_with(b"+PONG\r\n") {
            reply.extend(read_reply(&mut blocked).await);
        }
        let reply_text = String::from_utf8_lossy(&reply);
        assert!(reply_text.contains("payload"), "{reply_text}");
        assert!(reply_text.ends_with("+PONG\r\n"), "{reply_text}");

        // 2. Timeout path: BLPOP with a 1s deadline, PING pipelined behind it.
        blocked
            .write_all(b"*3\r\n$5\r\nBLPOP\r\n$5\r\nempty\r\n$1\r\n1\r\n")
            .await
            .expect("start timed blocking pop");
        tokio::time::sleep(Duration::from_millis(50)).await;
        blocked
            .write_all(b"*1\r\n$4\r\nPING\r\n")
            .await
            .expect("pipeline ping behind the timed block");
        let mut nil = timeout(Duration::from_secs(3), read_reply(&mut blocked))
            .await
            .expect("blocking pop must time out");
        if !nil.ends_with(b"+PONG\r\n") {
            nil.extend(read_reply(&mut blocked).await);
        }
        assert!(
            nil.starts_with(b"*-1") || nil.starts_with(b"$-1") || nil.starts_with(b"_"),
            "expected null reply, got {:?}",
            String::from_utf8_lossy(&nil)
        );
        assert!(
            nil.ends_with(b"+PONG\r\n"),
            "pipelined PING must run after the timed-out block: {:?}",
            String::from_utf8_lossy(&nil)
        );

        blocked.write_all(b"QUIT\r\n").await.expect("quit blocked");
        let _ = read_reply(&mut blocked).await;
        writer.write_all(b"QUIT\r\n").await.expect("quit writer");
        let _ = read_reply(&mut writer).await;
        accept_task.await.expect("accept task join");
    }

    #[tokio::test]
    async fn blocking_list_pop_wakes_on_matching_write() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));

        let shared_for_accept = Arc::clone(&shared);
        let accept_task = tokio::spawn(async move {
            let (sock_blocked, _) = listener.accept().await.expect("accept blocked");
            let (sock_writer, _) = listener.accept().await.expect("accept writer");

            let shared_blocked = Arc::clone(&shared_for_accept);
            let task_blocked = tokio::spawn(async move {
                handle_client(sock_blocked, shared_blocked)
                    .await
                    .expect("handle blocked");
            });
            let shared_writer = Arc::clone(&shared_for_accept);
            let task_writer = tokio::spawn(async move {
                handle_client(sock_writer, shared_writer)
                    .await
                    .expect("handle writer");
            });

            task_blocked.await.expect("join blocked");
            task_writer.await.expect("join writer");
        });

        let mut blocked = TcpStream::connect(addr).await.expect("connect blocked");
        let mut writer = TcpStream::connect(addr).await.expect("connect writer");

        blocked
            .write_all(b"*3\r\n$5\r\nBLPOP\r\n$7\r\nwake-me\r\n$1\r\n5\r\n")
            .await
            .expect("start blocking pop");
        tokio::time::sleep(Duration::from_millis(50)).await;

        writer
            .write_all(b"*3\r\n$5\r\nLPUSH\r\n$7\r\nwake-me\r\n$7\r\npayload\r\n")
            .await
            .expect("push payload");
        assert_eq!(read_reply(&mut writer).await, b":1\r\n");

        let reply = read_reply(&mut blocked).await;
        let reply_text = String::from_utf8_lossy(&reply);
        assert!(reply_text.contains("wake-me"), "{reply_text}");
        assert!(reply_text.contains("payload"), "{reply_text}");

        blocked.write_all(b"QUIT\r\n").await.expect("quit blocked");
        let _ = read_reply(&mut blocked).await;
        writer.write_all(b"QUIT\r\n").await.expect("quit writer");
        let _ = read_reply(&mut writer).await;

        accept_task.await.expect("accept task join");
    }

    #[tokio::test]
    async fn client_read_timeout_disconnects_idle_client() {
        let limits = ClientIoLimits {
            output_buffer_limit_bytes: DEFAULT_OUTPUT_BUFFER_LIMIT_BYTES,
            client_read_timeout_sec: 1,
        };
        let (mut client, server_task) = setup_client_server_with_limits(limits).await;

        client.write_all(b"PING\r\n").await.expect("write ping");
        let ping = read_reply(&mut client).await;
        assert_eq!(ping, b"+PONG\r\n");

        tokio::time::sleep(Duration::from_secs(2)).await;

        let mut buf = [0u8; 1];
        let n = client.read(&mut buf).await.expect("read after timeout");
        assert_eq!(n, 0, "expected EOF after timeout");

        server_task.await.expect("server task complete");
    }

    #[tokio::test]
    async fn write_commands_append_to_aof() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let config = crate::config::ServerConfig {
            dir: dir.path().to_path_buf(),
            appendonly: true,
            appendfsync: "always".to_string(),
            ..crate::config::ServerConfig::default()
        };
        let persistence = Arc::new(PersistenceRuntime::from_config(&config).expect("runtime"));

        let (mut client, server_task) =
            setup_client_server_with_persistence(ClientIoLimits::default(), persistence).await;

        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
            .await
            .expect("write set");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");

        client.write_all(b"QUIT\r\n").await.expect("quit");
        let _ = read_reply(&mut client).await;
        server_task.await.expect("server task complete");

        // Find the actual AOF file written — may be the legacy filename or a
        // manifest-managed incremental file depending on bootstrap layout.
        let aof_content = find_aof_content(dir.path());
        assert!(
            aof_content.contains("SET"),
            "AOF should contain SET command"
        );
        assert!(aof_content.contains("foo"), "AOF should contain key 'foo'");
        assert!(
            aof_content.contains("bar"),
            "AOF should contain value 'bar'"
        );
    }
}
