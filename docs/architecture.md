# Ratatosk Architecture & Compatibility Reference

이 문서는 6개의 개별 문서를 통합한 단일 참조 문서다.

- Part 1: Architecture (원본: `architecture-ratatosk.md`)
- Part 2: Eviction & Expiry Detail (원본: `eviction-and-expiry.md`)
- Part 3: Persistence Detail (원본: `persistence.md`)
- Part 4: Capability Declarations (원본: `capability-declarations.md`)
- Part 5: Redis Gap Analysis (원본: `redis-gap-analysis.md`)
- Part 6: Redis Gap Ledger (원본: `redis-gap-ledger.md`)

---

# Part 1: Architecture

이 문서는 **현재 저장소 코드 기준**(2026-06-02) 아키텍처를 설명한다.
과거 계획 문서가 아니라 실제 구현 상태를 기준으로 작성했다.

## Snapshot

- Workspace crates: `ratatosk-core`, `ratatosk-resp`, `ratatosk-engine`, `ratatosk-persist`, `ratatosk-server`
- Runtime model: tokio TCP accept loop + per-client task + server_cron timer
- Shared state: `SharedState` wrapping `Mutex<ServerState>` + per-DB `parking_lot::RwLock` + lock-free components
- Protocol: RESP2/RESP3 호환 파싱 경로
- Command catalog: `420` entries. 구현 상태와 Redis 의미론 tier는 [Part 6: Redis Gap Ledger](#part-6-redis-gap-ledger)를 함께 봐야 한다.
- Persistence: RDB snapshot + AOF append-only file
- Eviction: 8가지 maxmemory 정책 (LRU/LFU/random/TTL)

## Crate Graph

```
ratatosk-server
  ├── ratatosk-engine
  ├── ratatosk-persist
  └── ratatosk-core

ratatosk-persist
  ├── ratatosk-engine
  ├── ratatosk-resp (AOF recovery: RESP 파싱)
  └── ratatosk-core

ratatosk-engine
  ├── ratatosk-resp (frame type usage)
  └── ratatosk-core

ratatosk-resp
  └── ratatosk-core

ratatosk-core
  └── (leaf crate — bytes, thiserror만 의존)
```

의존 방향: `server → {engine, persist} → resp → core`. 역방향 의존 없음.

## Crate Responsibilities

| Crate | Role | `unsafe` |
|-------|------|----------|
| `ratatosk-core` | 도메인 타입 (`ClientId`, `DbIndex`, `SlotId`), 비트마스크 플래그, 에러, 시간 유틸 | `forbid` |
| `ratatosk-resp` | RESP2/RESP3 zero-copy 파서 + 인코더 | `forbid` |
| `ratatosk-engine` | Keyspace, config/stats state, 420개 명령 핸들러, eviction, active expiry, pub/sub, notification, HLL, 슬롯, Lua scripting (`lua-scripting` feature) | `forbid` |
| `ratatosk-persist` | RDB saver/loader, AOF writer/manifest/recovery, atomic file write, CRC64 | `forbid` |
| `ratatosk-server` | TCP accept loop, per-client I/O, server_cron, lazy-free thread, config | — |

## Request Lifecycle

### 1) Accept / Connection Admission

`crates/ratatosk-server/src/event_loop.rs`:

- `TcpListener::bind(config.listen_addr())`로 소켓 바인딩.
- `Semaphore`로 동시 접속 수를 제한 (`RATATOSK_MAX_CLIENTS`, 기본 4096).
- accept 에러는 transient/non-transient를 구분하고 지수 백오프(50ms → 2s max)로 재시도.

### 2) Per-client I/O Loop

`crates/ratatosk-server/src/client.rs`:

- 클라이언트마다 tokio task를 생성해 read/parse/execute/write 루프 수행.
- `QUERY_BUFFER_LIMIT` = 1 MiB 초과 시 즉시 에러 후 연결 종료.
- `RATATOSK_OUTPUT_BUFFER_LIMIT_BYTES` (기본 8 MiB) 초과 응답은 에러 후 종료.
- write는 5초 타임아웃으로 보호.

### 3) Parse (zero-copy path)

`crates/ratatosk-resp/src/parse.rs`:

- 파서는 two-phase 구조:
  1. shape/length 검증(`describe_*`)
  2. `split_to(...).freeze()` 후 materialize
- bulk/array/map에 상한이 있다:
  - bulk: 512 MiB
  - array: 1,048,576 elements
  - map: 524,288 entries

### 4) Execute (state mutation)

`crates/ratatosk-engine/src/command/mod.rs`:

- 명령 실행 전에 인증/ACL 검사.
- `execute(frame, &mut access, &mut client_state)` 호출. `access`는 `ServerAccess` 래퍼로, `SharedState` 내부 `Mutex<ServerState>`를 잠근 뒤 생성된다.
  DB 값 자체는 per-DB `parking_lot::RwLock` 기반 `DataState`에 놓여 있고, `SharedState.data`와 inner `ServerState.data`는 같은 backing shard를 공유한다.
  다만 현재 런타임의 대부분의 명령은 여전히 `meta` mutex를 잡은 상태에서 실행되므로, cross-DB 병렬성은 저장소 레이어의 잠재력이고 일반 command path의 기본 성질은 아니다.
  예외적으로 `PING` / `ECHO` / `TIME` / `DBSIZE` / `TYPE` / `EXISTS` / `GET` / `MGET` / `STRLEN` / `BITCOUNT` / `GETBIT` / `GETRANGE` / `SUBSTR` / `HGET` / `HMGET` / `HGETALL` / `HKEYS` / `HVALS` / `HEXISTS` / `HLEN` / `HSTRLEN` / `SISMEMBER` / `SMISMEMBER` / `SCARD` / `ZSCORE` / `ZCARD` / `ZMSCORE` / `ZCOUNT` / `ZLEXCOUNT` / `ZRANGE` / `ZRANGEBYSCORE` / `ZREVRANGEBYSCORE` / `ZRANGEBYLEX` / `ZREVRANGEBYLEX` / `ZREVRANGE` / `ZRANK` / `ZREVRANK` / `LLEN` / `LINDEX` / `LRANGE` / `TTL` / `PTTL` / `EXPIRETIME` / `PEXPIRETIME`는 default ACL policy cache가 `nopass + full access`인 연결에 한해 lock-free fast path를 탈 수 있다. readonly pipeline이 이 커맨드들로만 이루어진 경우에도 batch 전체가 lock-free로 처리된다. fast path를 탈 수 없는 readonly batch는 더 이상 배치 전체를 한 번에 잠그지 않고, 명령별로 `meta` lock을 다시 잡으며 순차 실행한다. 이 batch gate는 이제 fast path 집합 외에도 non-blocking readonly command spec을 받아들이며, `WAIT` / `WAITAOF` / connection / pubsub 계열은 제외한다. 일반 단건 경로도 lock 밖에서 만든 argv를 재사용하므로, locked path에서 같은 RESP frame을 다시 파싱하지 않는다. 여기에 더해 default-user `nopass` 승격과 즉시 `NOAUTH`로 끝나는 요청은 공용 precheck helper로 먼저 걸러서, 불필요하게 `meta` lock을 잡지 않도록 했다. `PING HEALTH`처럼 서버 메타 상태가 필요한 변형은 여전히 locked path를 사용한다. 다만 fast path도 실행 후에는 공용 post-execute helper를 통해 slowlog/latency, client tracking reset, MONITOR broadcast를 맞추고, `MULTI` 안에서는 큐잉 의미론을 우회하지 않도록 비활성화된다. `EXISTS`와 `GET`은 atomic keyspace hit/miss counter까지 함께 갱신한다.
  stats/config 읽기/client id 할당/default ACL cache는 lock-free.
- `CommandOutcome { response, close, retry_blocking, config_dirty, acl_dirty }`로 결과를 통일한다.

### 5) Blocking command retry

- 블로킹 명령은 `retry_blocking`으로 재시도 지시를 반환.
- 서버 루프는 lock을 놓은 뒤 deadline까지 남은 시간과 500ms 폴링 상한(`BLOCKING_RETRY_POLL_CAP`) 중 작은 값만큼 대기한 후 재실행 (지수 백오프 아님 — FIFO wake-one 의미론이라 불필요).
- timeout deadline을 넘기면 마지막 응답을 반환.

### 6) Encode / Flush

`crates/ratatosk-resp/src/encode.rs`:

- `+OK`, `+PONG`, `:0`, `:1`, `$-1`는 shared static 인코딩 사용.
- `encoded_len()`으로 flush 전 output limit 예측.
- `OUTPUT_BUFFER_FLUSH_THRESHOLD` (16 KiB)마다 배치 flush.

## Core State Model

### SharedState

`SharedState`는 `Mutex<ServerState>`를 감싸면서 독립적으로 접근 가능한 lock-free 컴포넌트를 제공한다:

| Component | Type | 역할 |
|-----------|------|------|
| `atomic_stats` | `AtomicStatsState` | 11개 lock-free atomic counter (total_commands_processed, connected_clients, total_connections_received, net_input/output_bytes, evicted/expired_keys, keyspace_hits/misses, instantaneous_ops_per_sec, cached_memory_estimate) |
| `default_acl_policy` | `DefaultAclPolicyState` | default user의 `nopass` / full-access 여부를 lock-free로 캐시 |
| `config_cache` | `arc_swap::ArcSwap<ConfigState>` | lock-free config 읽기 (`config_cache.load()`) |
| `next_client_id` | `AtomicI64` | lock 없이 새 client ID 할당 |

이 구조로 client 요청당 ~9회의 lock 획득이 제거된다.

참고:
- `crates/ratatosk-engine/src/config.rs` — `ConfigState`
- `crates/ratatosk-engine/src/stats.rs` — `StatsState`, `AtomicStatsState`

Workspace dependency: `arc-swap = "1"`

### ServerState + DataState (Per-DB RwLock)

`crates/ratatosk-engine/src/keyspace.rs`의 `ServerState`:

- `data: DataState` — DB별 per-shard RwLock 계층
- `pubsub: PubSubState`
- `stats: StatsState`
- `acl: AclState`
- `config: ConfigState`
- `script_cache: ScriptCache`
- `cluster_node_id`
- `lazy_free_tx: Option<LazyFreeSender>` — 백그라운드 삭제 채널

`DataState`는 DB별 `parking_lot::RwLock<DbShard>`를 관리하고, outer/inner state handle이 같은 backing store를 공유한다:

```rust
#[derive(Clone)]
pub struct DataState {
    shards: Arc<[parking_lot::RwLock<DbShard>]>,
    next_key_version: Arc<AtomicU64>,
    db_memory_bytes: Arc<[AtomicUsize]>, // per-DB estimated memory in bytes
}

pub struct DbShard {
    pub data: HashMap<Bytes, StoredValue>,
    pub key_versions: HashMap<Bytes, u64>,
    pub expires: HashMap<Bytes, i64>, // TTL side-index (key -> expire_at_ms)
}
```

- `db(idx)` → `MappedRwLockReadGuard<HashMap>` (auto-deref로 `&HashMap`처럼 사용)
- `db_mut(idx)` → `MappedRwLockWriteGuard<HashMap>` (auto-deref로 `&mut HashMap`처럼 사용)
- `write_two_dbs(a, b)` — ascending index 순서로 2개 DB 동시 write lock (MOVE, SWAPDB용)
- `write_all_dbs()` — 전체 DB ascending lock (FLUSHALL, load_from_rdb용)
- `snapshot_all()` — DB별 순차 read-lock + clone (BGSAVE용)

기본 DB 개수는 16(`DEFAULT_DB_COUNT`).

Lock ordering invariant: **항상 ascending index 순서로 DB lock 획득** → deadlock 방지.

### Hot Stats Synchronization

hot counter는 `AtomicStatsState`가 가장 빠른 경로이고, inner `StatsState`는 두 방향으로 이를 따라간다.

- command/runtime fast path는 outer atomic stats를 먼저 갱신할 수 있다.
- `server_cron`은 sampling 전에 `StatsState::catch_up_from_atomic(...)`으로 inner 기준선을 따라잡는다.
- client snapshot refresh와 `INFO`/`PING HEALTH`는 merged snapshot을 사용해 atomic/inner 괴리를 줄인다.

즉 현재 모델은 "single stats source"까지는 아니지만, outer-only drift가 장시간 누적되지 않도록 역동기화 경로가 들어와 있다.

### StoredValue

메모리 최적화(optimization.md Phase 1)로 `StoredValue`는 스택에서 **24 bytes**로 고정된다 (`keyspace.rs`의 test invariant로 강제). payload는 `Box`로 옮기고, encoding tag와 LRU/LFU clock은 단일 `u32`에 패킹한다.

```rust
pub struct StoredValue {
    data: Box<ValueData>,     // 박싱하여 StoredValue를 24 bytes로 유지
    expire_at_ms: i64,        // 0 = no expiry (Option<i64> 아님)
    encoding_and_lru: u32,    // high 4 bits = Encoding, low 28 bits = LRU/LFU clock
}
```

`Encoding` tag (`#[repr(u8)]` 판별자):

- `Raw = 0`, `Int = 1`, `QuickList = 4`, `HashTable = 5`, `IntSet = 6`, `SkipList = 10`, `StreamTree = 12`

### ValueData

- `String(Bytes)`
- `StringInt(i64)` — i64에 들어가는 정수 문자열 (Phase 4A compact encoding)
- `Hash(HashMap<Bytes, HashFieldEntry>)`
- `List(VecDeque<Bytes>)`
- `Set(HashSet<Bytes>)`
- `SetInt(Vec<i64>)` — 멤버가 모두 i64인 작은 Set (Phase 4B compact encoding)
- `SortedSet(SortedSet)` — `{ by_score: BTreeMap, by_member: HashMap }`
- `Stream { entries, groups }`

### ConfigState 확장

eviction/cron/notification 관련 설정이 `ConfigState`에 포함된다:

| Field | Default | 용도 |
|-------|---------|------|
| `maxmemory` | `0` (무제한) | 메모리 한도 |
| `maxmemory_policy` | `noeviction` | eviction 정책 |
| `maxmemory_samples` | `5` | eviction 샘플링 수 |
| `hz` | `10` | server_cron 주파수 |
| `notify_keyspace_events` | `""` (비활성) | keyspace notification 설정 |
| `lazyfree_lazy_*` | `false` | lazy free 정책 |
| `tcp_keepalive` | `300` | TCP keepalive 초 |

## Nervous System: server_cron

`event_loop.rs`의 `tokio::select!` 루프에 `cron_interval.tick()` 분기가 포함된다.
기본 10Hz(100ms 간격)로 다음을 수행:

1. **Active expiry cycle** — 만료 후보 키를 샘플링(DB당 20개)하여 삭제. 만료율 < 25%면 조기 중단.
2. **Eviction check** — `maxmemory` 초과 시 `perform_eviction()` 호출. 최대 128라운드.

추가 signal 핸들링:
- **SIGINT/SIGTERM** → graceful shutdown (drain + grace period)
- **SIGUSR1** → RDB save 트리거 (persistence runtime 연동 완료)

## Pub/Sub Internals

- channel/shard/pattern subscription map을 분리 유지.
- RESP3 Push wrapping: 모든 메시지 타입(Message, SMessage, PMessage, Invalidate, TrackingRedirectBroken)이 RESP3 push frame으로 래핑된다.
- **mpsc push delivery**: per-subscriber `tokio::sync::mpsc::channel` 기반. `register_client()`가 `mpsc::Receiver<PubSubMessage>`를 반환하며, channel capacity는 `hard_limit`으로 설정된다.
- `publish()`는 `try_send()`로 메시지를 전달한다. channel이 가득 차면 overflow로 간주하고, receiver가 `None`을 수신하여 disconnect된다.
- **초기화**: `PubSubState::new(&ConfigState)`로 생성되어 `ConfigState`가 유일한 기본값 출처 (single source of truth). `pending_queue_limit()`도 `self.hard_limit`을 반환하여 런타임 변경이 즉시 반영됨.
- `CONFIG GET/SET pubsub-queue-hard-limit`으로 런타임 조정 가능.
- Client tracking invalidation도 동일한 per-client mpsc 채널을 통해 자동 전달된다 (`mark_write_command → invalidate_tracked_keys → tracking_invalidate_keys → pubsub.enqueue_invalidation`).
- Monitor notifications는 별도 `Arc<Notify>` 경로를 사용한다 (`register_monitor_notifier()`).

### Client Loop (WaitResult)

subscribed/tracking client의 이벤트 루프는 `WaitResult` enum으로 통합:

- `PubSubMsg` — mpsc 채널에서 메시지 수신
- `PubSubClosed` — 채널 닫힘 (overflow 등)
- `MonitorWake` — monitor notifier 깨어남
- `NetworkRead` — 네트워크 입력

제거된 API: `drain_messages()`, `take_overflowed_client()`, `client_notifiers` HashMap, `soft_limit_exceeded_at`.

## Concurrency Model

현재 모델은 "single event loop only"가 아니라 `SharedState` 기반 계층적 동시성이다.

- 네트워크는 tokio per-client task로 동시 처리.
- command execution은 `SharedState` 내부 `Mutex<ServerState>`를 잠근 뒤, per-DB `parking_lot::RwLock`을 통해 DB에 접근한다.
  - **서로 다른 DB**는 저장소 레이어에서 독립 lock을 가지지만, 일반 command path는 여전히 `meta` mutex 영향 아래 있다.
  - **같은 DB** 내 읽기 명령은 저장소 레이어에서 read sharing이 가능하다.
  - 쓰기 명령은 해당 DB의 write lock을 획득.
- **lock-free 경로** (Mutex 불필요):
  - `AtomicStatsState`: 11개 atomic counter (total_commands_processed, connected_clients 등) — 매 요청마다 lock 없이 갱신.
  - `ArcSwap<ConfigState>`: config 읽기는 `config_cache.load()`로 lock-free. 쓰기만 lock 필요.
  - `next_client_id`: 새 연결 시 atomic increment로 client ID 할당.
- 이 구조로 client 요청당 ~9회의 lock 획득이 제거됨.
- **Lock ordering**: DB shard lock은 항상 ascending index 순서로 획득. `parking_lot` guard는 `!Send`이므로 `.await`를 넘을 수 없어 compile-time deadlock 방지.
- `ServerAccess` 래퍼가 `execute()` 함수의 인자로 사용되어 DB 접근과 meta-state 접근을 분리한다.

### Background Threads

| Thread | 역할 | 통신 |
|--------|------|------|
| Lazy-free | 대용량 값 비동기 삭제 | `crossbeam_channel::bounded(4096)` |
| RDB save | Background snapshot (`BGSAVE`) | tokio task + shutdown drain |
| AOF rewrite | Background AOF 재작성 (`BGREWRITEAOF`) | AOF worker channel |

참고: `crates/ratatosk-server/src/io_thread.rs`의 `IoThreadPool`은 현재 placeholder다.

## Security / Safety Controls

### Config guardrails

`crates/ratatosk-server/src/config.rs`:

- 기본 bind는 loopback(`127.0.0.1`).
- non-loopback bind는 `RATATOSK_ALLOW_INSECURE_BIND=true` 없으면 거부.
- max clients/output buffer/shutdown grace에 대해 0값 방어 검증.

### AUTH brute force prevention

`crates/ratatosk-engine/src/command/cmd_auth_session.rs`, `crates/ratatosk-server/src/rate_limiter.rs`:

- **progressive delay**: 실패 횟수에 따라 응답 전 지수 지연을 적용한다. `delay_ms = min(100 × 2^(failures-1), 2000) × jitter(0.8..1.2)`. `tokio::time::sleep`으로 비동기 대기하므로 OS 스레드를 차단하지 않는다.
- per-connection: 5회 연속 AUTH 실패 시 지연 후 연결을 종료한다 (`CommandOutcome::close_with_delay`).
- per-IP: per-IP AUTH 실패 추적 한도(`AuthRateLimiter`, 60초 윈도우 / 20회)는 `crates/ratatosk-server/src/rate_limiter.rs`에 정의되어 있으나 현재 런타임 경로에 연결되어 있지 않다(데드 코드). 실제 accept 경로에 연결된 것은 IP별 *연결 시도* 횟수를 제한하는 `ConnectionRateLimiter`뿐이다.

### Sanitization helpers

`crates/ratatosk-engine/src/security.rs`:

- 에러/ACL/slowlog 문자열 sanitization + 256자 제한.
- token prefix redaction (`ghp_`, `sk-`, `npm_`, `xox`, `AKIA`).
- 20자 이상 영숫자 토큰 redaction.
- shell metacharacter blocklist 기반 입력 거부 helper 제공.

## Lua Scripting (feature-gated)

`lua-scripting` feature를 활성화하면 Lua 5.1 스크립팅을 사용할 수 있다.

- Dependency: `mlua = { version = "0.11", features = ["lua51", "vendored"] }`
- 구현: `crates/ratatosk-engine/src/command/lua_runtime.rs` (thread-local Lua VM)
- Sandbox: TABLE+STRING+MATH+OS+BASE 라이브러리만 허용, 1MB 메모리 제한, 100K instruction 제한
- `redis.call()` / `redis.pcall()`: `mlua::Scope` + `RefCell`로 내부 `execute()`에 bridge
- Type conversion: Redis 규약 (true->1, false->nil, table->Array, number->Integer)
- Nested EVAL/EVALSHA는 거부됨

지원 명령:

| Command | 설명 |
|---------|------|
| `EVAL` / `EVAL_RO` | Lua 스크립트 직접 실행 |
| `EVALSHA` / `EVALSHA_RO` | SHA1으로 캐시된 스크립트 실행 |
| `SCRIPT LOAD` | 스크립트를 SHA1 캐시에 등록 |
| `SCRIPT EXISTS` | 캐시 존재 여부 확인 |
| `SCRIPT FLUSH` | 캐시 초기화 |

## Compatibility Notes

- [Part 6: Redis Gap Ledger](#part-6-redis-gap-ledger) 기준 명령 카탈로그는 420개 엔트리이며, 현재 ledger는 status와 별도로 `capability_tier`를 기록한다.
- 현재 tier summary는 `unsupported=63`, `syntax_only=6`, `baseline_local=76`, `behavioral_subset=275`, `distributed_parity=0`이다.
- 일부 서버/복제/운영 명령은 "standalone baseline semantics"(ack/no-op 포함)으로 구현되어 있다.
  - 예: 복제/클러스터 계열은 standalone 호환 응답 중심.
  - 단, `SAVE`/`BGSAVE`는 실제 RDB 스냅샷을 수행하며, `BGSAVE`는 background snapshot worker로 동작한다 (단순 timestamp 갱신이 아님).
- `CLIENT REPLY`는 I/O loop에서 실제 적용된다 (`off`=응답 억제, `skip`=다음 1회 억제, push notification 무영향).
- `CLIENT SETINFO`는 `lib-name`/`lib-ver`를 `ClientState`에 저장하고, `CLIENT LIST` 출력에 반영한다.
- `CLIENT TRACKING`은 `BCAST`+`OPTIN`/`OPTOUT` 비호환 조합을 검증해 거부한다.
- `PSYNC`는 standalone mode에서 ERR를 반환한다 (fake FULLRESYNC 대신).
- `REPLICAOF`는 `NO ONE` 이외의 인자에 대해 ERR를 반환한다.

즉, "명령 surface"는 넓지만 내부 동작 parity 수준은 명령별로 다를 수 있다.

## Runtime Configuration

주요 환경변수:

| Variable | Default | Description |
|----------|---------|-------------|
| `RATATOSK_BIND` | `127.0.0.1` | listen address |
| `RATATOSK_PORT` | `6379` | listen port |
| `RATATOSK_MAX_CLIENTS` | `4096` | concurrent connection cap |
| `RATATOSK_OUTPUT_BUFFER_LIMIT_BYTES` | `8388608` | per-client output limit |
| `RATATOSK_SHUTDOWN_GRACE_MS` | `10000` | graceful drain window |
| `RATATOSK_ALLOW_INSECURE_BIND` | unset | non-loopback bind opt-in |
| `RATATOSK_SHUTDOWN_BEST_EFFORT` | unset | appendonly shutdown flush failure override |

`CONFIG SET`을 통한 런타임 설정 변경:
- `hz`, `timeout`, `appendonly`, `appendfsync`, `save`
- `compatibility-mode`, `protected-mode`, `dbfilename`, `dir`
- `slowlog-log-slower-than`, `slowlog-max-len`, `latency-tracking`
- `active-expire-cycle-lookups`, `active-expire-cycle-threshold-pct`
- `query-buffer-limit`, `output-buffer-flush-threshold`, `client-write-timeout-sec`
- `pubsub-queue-hard-limit`, `pubsub-queue-soft-limit`, `pubsub-queue-soft-seconds`

(`maxmemory*`, `notify-keyspace-events`, `tcp-keepalive`, `lazyfree-lazy-*`는 `CONFIG GET` 전용이며 `CONFIG SET`은 'ERR Unknown option'을 반환한다.)
- `pubsub-queue-hard-limit`, `pubsub-queue-soft-limit`, `pubsub-queue-soft-seconds`

## Build / Run / Test

```bash
# Nix dev shell (toolchain 자동 세팅)
nix develop

# workspace tests
cargo test --workspace

# clippy lint
cargo clippy --all-targets -- -D warnings

# run server
cargo run -p ratatosk-server --bin ratatosk

# benchmark baseline
./scripts/bench_baseline.sh
```

## Source Index

- Domain types: `crates/ratatosk-core/src/{types,flags,error,time}.rs`
- RESP parse/encode: `crates/ratatosk-resp/src/{parse,encode}.rs`
- Command dispatcher: `crates/ratatosk-engine/src/command/mod.rs`
- Keyspace/state: `crates/ratatosk-engine/src/keyspace.rs`
- Eviction: `crates/ratatosk-engine/src/eviction.rs`
- Active expiry: `crates/ratatosk-engine/src/expiry.rs`
- Keyspace notifications: `crates/ratatosk-engine/src/notification.rs`
- Security helpers: `crates/ratatosk-engine/src/security.rs`
- RDB saver/loader: `crates/ratatosk-persist/src/rdb/{saver,loader}.rs`
- AOF writer/recovery: `crates/ratatosk-persist/src/aof/{writer,recovery}.rs`
- AOF manifest: `crates/ratatosk-persist/src/aof/manifest.rs`
- Atomic file write: `crates/ratatosk-persist/src/atomic.rs`
- Runtime accept loop: `crates/ratatosk-server/src/event_loop.rs`
- Client pipeline: `crates/ratatosk-server/src/client.rs`
- Persistence runtime: `crates/ratatosk-server/src/persistence/{mod,rdb,aof,util}.rs`
- Lua runtime: `crates/ratatosk-engine/src/command/lua_runtime.rs`
- Config: `crates/ratatosk-server/src/config.rs`

---

# Part 2: Eviction & Expiry Detail

Ratatosk의 메모리 관리 및 키 만료 시스템.
기준: 2026-06-02 코드 상태.

## 개요

| 기능 | 위치 | 트리거 |
|------|------|--------|
| Passive expiry | `keyspace.rs` | 키 접근 시 만료 확인 |
| Active expiry | `expiry.rs` | server_cron (10Hz) |
| Eviction | `eviction.rs` | server_cron, maxmemory 초과 시 |
| Lazy free | `keyspace.rs` + `event_loop.rs` | 대용량 키 삭제 시 |
| Keyspace notification | `notification.rs` | 키 변경/만료/eviction 시 |

---

## Eviction Policies

`crates/ratatosk-engine/src/eviction.rs`

Redis 호환 8가지 maxmemory 정책:

### 정책 목록

| Policy | 대상 | 기준 | 설명 |
|--------|------|------|------|
| `noeviction` | — | — | eviction 안 함. OOM 에러 반환 |
| `allkeys-lru` | 모든 키 | LRU | 최근 가장 적게 사용된 키 제거 |
| `volatile-lru` | TTL 있는 키 | LRU | TTL 키 중 LRU 기반 제거 |
| `allkeys-lfu` | 모든 키 | LFU | 접근 빈도가 가장 낮은 키 제거 |
| `volatile-lfu` | TTL 있는 키 | LFU | TTL 키 중 LFU 기반 제거 |
| `allkeys-random` | 모든 키 | 무작위 | 무작위 키 제거 |
| `volatile-random` | TTL 있는 키 | 무작위 | TTL 키 중 무작위 제거 |
| `volatile-ttl` | TTL 있는 키 | TTL | 만료 시간이 가장 가까운 키 제거 |

### 설정

```
```
# maxmemory 계열은 런타임 CONFIG SET 미지원 — ratatosk.conf로만 설정한다
maxmemory 100mb
maxmemory-policy allkeys-lru
maxmemory-samples 5
# 런타임에는 CONFIG GET maxmemory 등 조회만 가능
```
```

| 설정 | 기본값 | 설명 |
|------|--------|------|
| `maxmemory` | `0` (무제한) | 메모리 상한 (바이트) |
| `maxmemory-policy` | `noeviction` | eviction 정책 |
| `maxmemory-samples` | `5` | eviction 후보 샘플링 수 |

### 샘플링 기반 eviction

Redis와 동일한 근사 알고리즘을 사용한다:

1. 모든 DB를 순회하며 `maxmemory_samples`개의 키를 무작위 샘플링
2. 각 키에 대해 정책 기반 eviction score 계산
3. 가장 높은 score의 키를 제거
4. 메모리가 `maxmemory` 이하가 될 때까지 반복 (최대 128라운드)

`volatile-*` 정책은 TTL이 없는 키를 건너뛴다.

---

## LRU Clock

LRU/LFU 클럭은 `StoredValue`의 `encoding_and_lru` 필드(상위 4비트=Encoding, 하위 28비트=LRU/LFU 클럭 영역)에 패킹되며 `lru_clock()` / `set_lru_clock()` 접근자로 읽고 쓴다.

```
24-bit wrapping seconds: (unix_sec / 1) & 0xFFFFFF
최대값: 16,777,215 (약 194일)
해상도: 1초
```

유휴 시간 추정:
```rust
if server_clock >= object_clock {
    idle = server_clock - object_clock
} else {
    idle = LRU_CLOCK_MAX - object_clock + server_clock  // wrap-around
}
```

LFU 모드에서는 `lru_clock` 필드를 접근 빈도 카운터로 재활용한다.

---

## 메모리 추정

`estimate_object_memory(key, value)`:

각 키-값 쌍의 힙 메모리 사용량을 근사 계산한다.

| 항목 | 추정 방식 |
|------|-----------|
| HashMap entry overhead | 64 bytes per slot |
| String | `bytes.len()` |
| List | 24 bytes per element + content |
| Hash | 64 bytes per field + key.len() + value.len() |
| Set | 48 bytes per element + content |
| Sorted Set | 128 bytes per entry (dual index) |
| Stream | 32 bytes per entry + field overhead + 256 bytes per group |

`estimate_used_memory(state)`: 전체 DB의 합산. 각 DB는 per-DB `RwLock` read guard를 순차적으로 획득하여 조회한다.

---

## Active Expiry

`crates/ratatosk-engine/src/expiry.rs`

server_cron에서 주기적으로 호출되어 만료된 키를 능동적으로 삭제한다.

### 알고리즘

```
for each DB:
    sampled = 0
    expired = 0
    while sampled < 20:
        pick random key with TTL
        if expired → remove
        sampled++
    if expired / sampled < 0.25:
        stop (CPU 절약)
```

| 설정 | 기본값 | 설명 |
|------|--------|------|
| `active-expire-cycle-lookups` | 20 | DB당 최대 샘플 수 (1–1000) |
| `active-expire-cycle-threshold-pct` | 25 | 조기 중단 임계값 (%, 1–100) |

런타임 조정:

```
CONFIG SET active-expire-cycle-lookups 40
CONFIG SET active-expire-cycle-threshold-pct 10
```

특징:
- 만료 가능한 키(TTL 있는 키)만 대상
- 빈 DB나 TTL 키가 없는 DB는 건너뜀
- 만료율이 threshold 미만이면 해당 DB에서 조기 중단 → CPU 낭비 방지
- lookups를 높이면 만료가 더 적극적이지만 CPU 사용량 증가
- threshold를 낮추면 만료 키가 적은 DB에서도 계속 샘플링
- **Per-DB RwLock**: sampling 단계에서 read guard를 획득하고, guard를 해제한 뒤 expiry 단계에서 write guard를 획득한다. 하나의 DB를 정리하는 동안 다른 DB는 차단되지 않는다.

---

## server_cron

`crates/ratatosk-server/src/event_loop.rs`

`tokio::time::interval`로 구현된 주기적 하우스키핑 타이머.

```
기본 주파수: 10 Hz (100ms 간격)
설정: CONFIG SET hz <1-500>
MissedTickBehavior: Skip
```

매 tick 수행 순서:

1. **Active expiry cycle** — `active_expire_cycle(&mut server, now_ms)`
2. **Eviction check** — `maxmemory > 0`이면 O(1) 증분 추정치(`estimated_memory()`)로 `needs_eviction()` 판정 → `perform_eviction()`. 전체 DB 스캔(`estimate_used_memory()`)은 매 10 tick마다 증분 카운터 드리프트(>5%) 보정 용도로만 호출된다.

### Signal Handling

| Signal | 동작 |
|--------|------|
| `SIGINT` | Graceful shutdown |
| `SIGTERM` | Graceful shutdown |
| `SIGUSR1` | RDB save 트리거 (로그 출력) |

Non-Unix 플랫폼에서는 `Ctrl+C`로 대체, SIGUSR1은 `pending()` future로 비활성화.

---

## Lazy Free

대용량 키 삭제 시 메인 이벤트 루프의 블로킹을 방지한다.

### 구조

```
Main thread                    Background thread
    │                               │
    ├── remove key from HashMap     │
    ├── send(StoredValue) ──────────┤
    │   (즉시 반환)                 ├── drop(value)  ← 실제 메모리 해제
    │                               │
```

### 구현

- 채널: `crossbeam_channel::bounded(4096)`
- 백그라운드 스레드: 100ms timeout `recv_timeout` 루프 + `AtomicBool` shutdown flag
- Threshold: `LAZY_FREE_THRESHOLD = 64` — 컬렉션 요소 64개 이상일 때만 lazy free

### 적용 대상

| 함수 | 설명 |
|------|------|
| `lazy_free_del(key)` | 키 제거 후 값을 채널로 전송 (threshold 이상) |
| `lazy_free_flush_db(db_idx)` | DB 전체를 새 HashMap으로 교체, 이전 값들을 채널로 전송 |
| `should_lazy_free(value)` | 값 크기가 threshold 이상인지 판단 |

---

## Keyspace Notifications

`crates/ratatosk-engine/src/notification.rs`

키 변경 이벤트를 Pub/Sub 채널로 자동 발행한다.

### 설정

```
CONFIG SET notify-keyspace-events "KEA"
```

| 플래그 | 의미 |
|--------|------|
| `K` | `__keyspace@<db>__:<key>` 채널 활성화 |
| `E` | `__keyevent@<db>__:<event>` 채널 활성화 |
| `g` | generic 명령 (DEL, EXPIRE, RENAME 등) |
| `$` | string 명령 (SET, APPEND 등) |
| `l` | list 명령 (LPUSH, RPOP 등) |
| `s` | set 명령 (SADD, SREM 등) |
| `h` | hash 명령 (HSET, HDEL 등) |
| `z` | sorted set 명령 (ZADD, ZREM 등) |
| `x` | expired 이벤트 |
| `e` | evicted 이벤트 |
| `t` | stream 명령 (XADD 등) |
| `m` | key miss 이벤트 (A 단축키에는 미포함) |
| `A` | 모든 이벤트 (`g$lshzxet`와 동일) |

`K` 또는 `E` 중 하나 이상이 설정되어야 알림이 활성화된다.

### 채널 형식

```
__keyspace@0__:mykey → "set"     (keyspace: 어떤 이벤트가 발생했는지)
__keyevent@0__:set   → "mykey"   (keyevent: 어떤 키에서 발생했는지)
```

### 사용

```rust
// 명령 핸들러에서:
notify!($server, b'$', b"set", db_index, &key);
```

`notify!` 매크로가 `notify_keyspace_event()` 호출을 간결하게 래핑한다.

---

## Eviction & Expiry 테스트 커버리지

| 모듈 | 테스트 수 | 주요 검증 |
|------|-----------|-----------|
| `eviction` | 11 | 8가지 정책, LRU clock wrap, 메모리 추정, volatile 키 필터링 |
| `expiry` | 3 | 만료 키 제거, 빈 DB, volatile 키 없는 DB |
| `notification` | 7 | 설정 파싱, K/E 플래그, A 단축키, pub/sub 연동, 매크로 |

---

# Part 3: Persistence Detail

Ratatosk의 데이터 영속성 시스템.
`ratatosk-persist` 크레이트가 RDB 스냅샷과 AOF 로그의 format/codec을 담당하고,
`ratatosk-server/src/persistence/` 모듈이 runtime orchestration을 담당한다.

### 모듈 구조

```
crates/ratatosk-server/src/persistence/
├── mod.rs   — PersistenceRuntime, from_config, load_startup_data, pub API re-exports
├── rdb.rs   — run_save (synchronous RDB snapshot)
├── aof.rs   — AofWorkerCommand, spawn_aof_worker, append/flush/rewrite, manifest, startup replay
└── util.rs  — check_disk_space, validate_working_directory, env_truthy
```

기준: 2026-06-02 코드 상태.

## 개요

| 방식 | 파일 | 특성 |
|------|------|------|
| RDB | `dump.rdb` | Point-in-time binary snapshot. 빠른 로딩, 데이터 손실 가능 (마지막 save 이후) |
| AOF | `appendonly.aof.*` | 명령 단위 append. 더 강한 durability, 파일 크기 큼 |

Recovery 순서: RDB 로드 → AOF replay.

---

## RDB (스냅샷)

### 파일 형식

```
[Magic: "REDIS"]          5 bytes
[Version: "0012"]         4 bytes  (Redis 7+ 호환)
[AUX fields]*             key-value 메타데이터
[DB section]*
  [SELECTDB opcode + DB number]
  [RESIZEDB + db_size + expires_size]
  [EXPIRETIME_MS + 8-byte LE]?   (밀리초 만료 시간, 선택)
  [type_byte] [key] [value]
  ...
[EOF: 0xFF]               1 byte
[CRC64]                   8 bytes LE
```

### Opcodes

| Byte | 이름 | 설명 |
|------|------|------|
| `0xFA` | `AUX` | 보조 메타데이터 (redis-ver, ratatosk-ver 등) |
| `0xFB` | `RESIZEDB` | DB 크기 힌트 (db_size + expires_size) |
| `0xFC` | `EXPIRETIME_MS` | 다음 키의 만료 시간 (밀리초, 8-byte LE) |
| `0xFD` | `EXPIRETIME` | 다음 키의 만료 시간 (초, 4-byte LE) |
| `0xFE` | `SELECTDB` | DB 선택 (length-encoded DB number) |
| `0xFF` | `EOF` | 파일 끝 (이후 8-byte CRC64) |

### Type Bytes

| Byte | 타입 |
|------|------|
| `0` | String |
| `1` | List |
| `2` | Set |
| `4` | Hash |
| `5` | Sorted Set |
| `19` | Stream |

### Length Encoding

RDB는 가변 길이 인코딩을 사용한다:

| 첫 2비트 | 형식 | 범위 |
|-----------|------|------|
| `00` | 6-bit inline | 0 ~ 63 |
| `01` | 14-bit (2 bytes) | 0 ~ 16,383 |
| `10000000` | 32-bit (5 bytes) | 0 ~ 2^32-1 |
| `10000001` | 64-bit (9 bytes) | 0 ~ 2^64-1 |
| `11` | Special encoding | 정수, LZF 압축 등 |

### CRC64

ECMA-182 다항식 기반 CRC64 체크섬.
파일 시작(`REDIS0012`)부터 `0xFF` EOF 바이트까지의 모든 데이터에 대해 계산.
로딩 시 불일치하면 `PersistError::CrcMismatch` 반환.

### RdbSaver

`crates/ratatosk-persist/src/rdb/saver.rs`

```rust
let file = File::create("dump.rdb")?;
let saver = RdbSaver::new(BufWriter::new(file));
saver.save_state(&server_state)?;
```

처리 순서:
1. 헤더 (magic + version)
2. AUX 필드 (redis-ver, ratatosk-ver)
3. DB별 순회:
   - `SELECTDB` + DB index
   - `RESIZEDB` + 크기 힌트
   - 키-값 쌍 (만료 시간 → 타입 → 키 → 값)
4. `EOF` + CRC64

### RdbLoader

`crates/ratatosk-persist/src/rdb/loader.rs`

```rust
let file = File::open("dump.rdb")?;
let mut state = ServerState::with_default_dbs();
RdbLoader::new(BufReader::new(file)).load_into(&mut state)?;
```

에러 처리:
- `InvalidMagic` — 파일이 RDB가 아님
- `UnsupportedVersion` — 지원하지 않는 RDB 버전
- `CrcMismatch` — 데이터 손상 감지
- `UnexpectedEof` — 파일 절단
- `UnknownType` — 알 수 없는 타입 바이트

### Atomic Write

`crates/ratatosk-persist/src/atomic.rs`

RDB 파일 저장 시 중간 상태가 노출되지 않도록 atomic write를 사용:

```
1. tempfile 생성 (같은 디렉토리)
2. 데이터 쓰기 + flush
3. fsync (디스크 동기화)
4. rename (원자적 교체)
5. 부모 디렉토리 fsync (Unix)
```

쓰기 중 에러 발생 시 원본 파일은 변경되지 않는다.

---

## AOF (Append-Only File)

### AofWriter

`crates/ratatosk-persist/src/aof/writer.rs`

쓰기 명령을 RESP 형식으로 AOF 파일에 append한다.

```rust
let mut writer = AofWriter::open(&path, FsyncPolicy::EverySec)?;
writer.append_command(0, &[
    Bytes::from("SET"),
    Bytes::from("key"),
    Bytes::from("value"),
])?;
```

기능:
- RESP array 형식으로 인코딩 (`*<count>\r\n$<len>\r\n<data>\r\n...`)
- DB index가 변경되면 자동으로 `SELECT` 명령 삽입
- 빈 명령은 무시

### Fsync Policy

| Policy | 동작 | 특성 |
|--------|------|------|
| `Always` | 매 write 후 `flush + sync_all` | 가장 강한 durability, 가장 느림 |
| `EverySec` | 마지막 fsync에서 1초 이상 경과 시 | 기본값, 좋은 균형 |
| `No` | `flush`만 (OS에 맡김) | 가장 빠름, 데이터 손실 가능 |

`CONFIG SET appendfsync always|everysec|no`로 런타임 변경 가능.

### AofManifest

`crates/ratatosk-persist/src/aof/manifest.rs`

Redis 7+ 호환 manifest 기반 AOF 관리. AOF를 BASE + INCR 파일로 분리한다.

```
<dir>/
  appendonly.aof.manifest     (manifest: BASE/INCR 목록)
  appendonly.aof.1.incr.aof   (INCR: 증분 append)
  appendonly.aof.2.incr.aof   (INCR: 증분 append)
  appendonly.aof.base.aof     (BASE: rewrite 결과)
```

주요 API:

| 메서드 | 설명 |
|--------|------|
| `new_incr_file()` | 새 INCR 파일 생성 (시퀀스 번호 자동 증가) |
| `set_base_after_rewrite(name)` | BASE 파일 설정 + 이전 INCR 파일 목록 초기화 |
| `recovery_files()` | 복구 순서: BASE 먼저, 이후 INCR 순서대로 |
| `current_incr_path()` | 현재 활성 INCR 파일 경로 |

### AofRecovery

`crates/ratatosk-persist/src/aof/recovery.rs`

AOF 파일을 재생하여 서버 상태를 복구한다.

```rust
let mut state = ServerState::with_default_dbs();
let replayed = AofRecovery::replay_file(&path, &mut state)?;
```

처리:
1. 파일 전체를 메모리에 읽기
2. `ratatosk_resp::parse()`로 RESP 프레임 파싱
3. `ratatosk_engine::command::execute()`로 각 명령 실행
4. 파싱 에러 발생 시 남은 데이터 건너뜀 (graceful 절단 처리)

절단된 AOF 파일은 마지막 완전한 명령까지만 복구하고 경고 로그를 남긴다.

---

## Recovery 순서

서버 시작 시 데이터 복구:

```
1. RDB 파일 존재? → RdbLoader::load_into()
2. AOF 파일 존재? → AofRecovery::replay_file() (manifest의 recovery_files() 순서)
3. 둘 다 없으면 빈 상태로 시작
```

AOF가 RDB 이후에 재생되므로, RDB 스냅샷 이후의 변경 사항이 AOF에서 복구된다.

참고: Pub/Sub delivery는 per-subscriber `mpsc::channel` 기반 push 방식이므로 AOF에 기록되지 않는다. AOF는 state-mutating 명령만 기록하며, Pub/Sub 메시지와 client tracking invalidation은 휘발성 delivery 경로로 처리된다.

## Runtime Observability

`INFO persistence`는 현재 활성 AOF 증분 파일과 manifest BASE 파일의 실제 크기를 노출한다.

- `aof_current_size`: 현재 write target인 active AOF file size
- `aof_base_size`: manifest BASE file size (`BASE`가 없으면 `0`)
- `audit_chain_dirty`: audit log append 이후 checkpoint state가 뒤처진 경우 `1`
- `audit_recovery_status`: startup 시 audit checkpoint를 durable log tail에서 복구한 경우 `log_ahead` 또는 `hash_mismatch`, 복구가 없으면 `none`

Legacy single-file AOF 모드에서는 `aof_current_size`만 의미가 있으며, `aof_base_size`는 `0`이다.

---

## 에러 타입

`crates/ratatosk-persist/src/error.rs`:

| Variant | 설명 |
|---------|------|
| `Io` | 파일 I/O 에러 |
| `InvalidMagic` | RDB magic bytes 불일치 |
| `UnsupportedVersion` | 지원하지 않는 RDB 버전 |
| `CrcMismatch` | CRC64 체크섬 불일치 (데이터 손상) |
| `Corrupt` | RDB 데이터 구조 손상 |
| `UnexpectedEof` | 파일 절단 |
| `UnknownType` | 알 수 없는 RDB 타입 바이트 |

---

## Persistence 테스트 커버리지

| 모듈 | 테스트 수 | 주요 검증 |
|------|-----------|-----------|
| `rdb/saver` | 4 | 빈 상태, string 키, expiry 직렬화, writer 에러 컨텍스트 |
| `rdb/loader` | 12 | 6가지 타입 roundtrip, CRC 불일치, magic 검증, 빈 reader, 다중 DB, 파일 roundtrip |
| `rdb/checksum` | 3 | deterministic, 빈 입력, known value |
| `atomic` | 2 | 정상 쓰기, 에러 시 원본 보존 |
| `aof/writer` | 5 | RESP 정확성, SELECT 자동 삽입, DB 유지, open 에러 컨텍스트, fsync policy roundtrip |
| `aof/manifest` | 5 | 빈 manifest, 순차 INCR, BASE 설정, recovery 순서, 파일 roundtrip |
| `aof/recovery` | 5 | 상태 복원, DB select, 빈 파일, 누락 파일 에러 컨텍스트, 절단 파일 graceful 처리 |

합계: 36개 테스트.

---
## 운영 상태 (2026-06-02)

| 항목 | 상태 | 설명 |
|------|------|------|
| Background RDB save | 구현 | `BGSAVE`가 백그라운드 태스크로 실행되고 shutdown 시 drain된다. snapshot은 per-DB 순차 read-lock + clone으로 수행 (`DataState::snapshot_all()`). |
| AOF rewrite (`BGREWRITEAOF`) | 구현 | AOF 워커에서 rewrite를 수행한 뒤 writer를 reopen한다. |
| AOF writer 서버 통합 | 구현 | 쓰기 명령이 AOF 워커 큐(`append`)로 비동기 전달된다. |
| Shutdown AOF flush gate | 구현 | appendonly 인스턴스는 종료 시 AOF flush를 시도하고, 실패하면 기본적으로 종료를 실패 처리한다. `RATATOSK_SHUTDOWN_BEST_EFFORT=true`일 때만 경고 후 계속 종료한다. |
| Legacy AOF format gate | 구현 | headerless AOF는 기본 거부하며 `RATATOSK_ALLOW_LEGACY_AOF=true`에서만 임시 허용한다. |
| Legacy AOF migration | 구현 | `RATATOSK_MIGRATE_AOF=true` 설정 시 레거시 단일 파일 AOF를 manifest 기반으로 자동 변환. startup에서 감지 후 경고 메시지 출력. |
| Manifest bootstrap/recovery | 구현 | manifest save/load, startup discovery, recovery-file 순차 replay가 baseline으로 연결된다. manifest가 가리키는 recovery file이 없으면 기본적으로 startup을 중단한다. |
| server_cron 통합 | 부분 | SIGUSR1 수신은 구현되어 있고, 추가 save 정책 자동화는 별도 작업이다. |
| LZF 압축 | 미구현 | RDB string 압축 (큰 값 전용) |
| Manifest rewrite switch | 부분 | manifest candidate validation/cleanup helper와 manifest-backed rewrite 후 새 INCR 회전은 연결됐지만 BASE materialization과 full atomic manifest switch는 아직 남아 있다. |

---

## Legacy AOF 게이트 운영 정책

- 기본값: `RATATOSK_ALLOW_LEGACY_AOF`는 설정하지 않는다(또는 `false`)를 유지한다.
- 예외 허용: 마이그레이션 윈도우에서만 `RATATOSK_ALLOW_LEGACY_AOF=true`를 단기 적용한다.
- 종료 조건: 레거시 포맷으로 1회 부팅 후 즉시 `BGREWRITEAOF`를 실행해 버전 헤더가 있는 AOF로 재작성한다.
- 재기동 검증: 우회 변수를 제거한 상태에서 재시작해도 정상 부팅되어야 릴리즈 가능으로 판정한다.

## Incomplete Manifest Chain 정책

- 기본값: `RATATOSK_ALLOW_INCOMPLETE_AOF_CHAIN`는 설정하지 않는다(또는 `false`)를 유지한다.
- 기본 동작: manifest가 참조하는 `BASE` 또는 `INCR` recovery file 중 하나라도 없으면 startup을 실패시킨다.
- 예외 허용: 수동 복구 창에서만 `RATATOSK_ALLOW_INCOMPLETE_AOF_CHAIN=true`를 단기 적용한다.
- 운영 원칙: 우회 부팅은 "부분 복구 상태"일 수 있으므로, 부팅 직후 데이터 검증과 `BGREWRITEAOF` 또는 오프라인 복구 절차를 반드시 수행한다.

## Shutdown Durability 정책

- 기본값: `RATATOSK_SHUTDOWN_BEST_EFFORT`는 설정하지 않는다(또는 `false`)를 유지한다.
- 기본 동작: appendonly 인스턴스는 종료 시 AOF flush가 성공해야 clean shutdown으로 간주한다.
- 실패 동작: AOF flush가 실패하면 경고만 남기고 성공 종료하지 않고, 종료 결과를 실패로 돌린다.
- 예외 허용: 운영 환경에서 durability보다 종료 진행이 더 중요한 경우에만 `RATATOSK_SHUTDOWN_BEST_EFFORT=true`를 단기 적용한다.
- 관측성: shutdown flush는 success/error/best-effort 결과와 duration 메트릭을 남긴다.

---

## 릴리즈 체크리스트 (AOF/BGREWRITEAOF)

1. `BGREWRITEAOF` 실기동 스모크를 실행한다.
   `bash scripts/smoke_bgrewriteaof.sh`
2. appendonly 비활성 인스턴스에서 `BGREWRITEAOF`가 거부되는지 확인한다.
   기대값: `ERR BGREWRITEAOF requires appendonly to be enabled`
3. 레거시(headerless) AOF 기본 차단을 확인한다.
   기대값: 부팅 실패 + `RATATOSK_ALLOW_LEGACY_AOF=true` 안내 메시지
4. 레거시 AOF 마이그레이션 시나리오를 확인한다.
   1회성으로 `RATATOSK_ALLOW_LEGACY_AOF=true`로 부팅 -> `BGREWRITEAOF` 실행 -> 변수 제거 후 재기동
5. manifest recovery chain 누락 기본 차단을 확인한다.
   기대값: 부팅 실패 + `RATATOSK_ALLOW_INCOMPLETE_AOF_CHAIN=true` 안내 메시지
6. 롤백 안전성 확인: 신규 빌드로 rewrite된 AOF로 재기동 후 기존 운영 변수셋(우회 변수 없음)에서 문제 없이 올라오는지 점검한다.

---

# Part 4: Capability Declarations

Deployment model, distribution boundaries, and honest limitations for Ratatosk.

Last updated: 2026-06-02

---

## Deployment Model

| Property | Value |
|----------|-------|
| Supported deployment | **Single-node only** |
| Horizontal scaling | Not supported |
| Clustering | Not implemented |
| Replication | Metadata skeleton only; no network replication stream |
| Failover | Not supported |

Ratatosk is a standalone, single-process, in-memory data server. It listens on a single TCP endpoint and serves all clients from one process. There is no built-in mechanism to distribute data across multiple Ratatosk instances.

---

## What "Cluster" Means Here

Ratatosk accepts cluster-related commands at the protocol level for client compatibility, but **no distributed system exists behind them**.

| Command | Behavior |
|---------|----------|
| `CLUSTER INFO` | Returns `cluster_enabled:0` with zeroed counters |
| `CLUSTER MYID` | Returns a local node ID (not part of any cluster) |
| `CLUSTER KEYSLOT` | Computes CRC16 hash slot (local calculation only) |
| `CLUSTER COUNTKEYSINSLOT` / `GETKEYSINSLOT` | Operates on local keyspace |
| `CLUSTER HELP` | Lists subcommands with standalone caveat |
| `CLUSTER SLOTS` / `SHARDS` / `LINKS` | Return an empty array (no slots/shards/bus links in standalone mode) |
| All other `CLUSTER` subcommands | Return `ERR` (disabled in standalone mode) |
| `READONLY` / `READWRITE` / `ASKING` | Return `OK` (no-op) |
| `SENTINEL` (all subcommands except `HELP`) | Return `ERR This instance is not configured as a Sentinel` |

There is no cluster bus, no node table, no slot migration, no MOVED/ASK redirection, and no gossip protocol. Cluster-aware Redis clients will not function correctly against Ratatosk.

---

## What "Replication" Means Here

Ratatosk maintains a replication metadata skeleton for protocol compatibility. Actual data replication does not exist.

| Command | Behavior |
|---------|----------|
| `REPLICAOF NO ONE` | Accepted (already standalone) |
| `REPLICAOF <host> <port>` | Returns `ERR` |
| `SLAVEOF` | Same as `REPLICAOF` |
| `PSYNC` | Returns `ERR` (standalone mode) |
| `REPLCONF` | Accepts LISTENING-PORT/CAPA/IP-ADDRESS/ACK/GETACK for metadata tracking |
| `ROLE` | Returns master/replica mode and logical replication offset |
| `INFO replication` | Returns local replication metadata |
| `WAIT` | Returns immediately with currently tracked replica ACK count (no blocking) |
| `WAITAOF` | Returns immediately with local AOF health (no blocking) |

Key limitations:

- **No backlog**: There is no replication backlog buffer.
- **No network stream**: No data is transmitted to any replica.
- **No partial resync**: `PSYNC` is rejected entirely.
- **`WAIT` is non-blocking**: It returns the current replica ACK count instantly rather than waiting for acknowledgement during the timeout window.
- **0 replicas in practice**: `WAIT` and `WAITAOF` will typically return 0 because no actual replication connections exist.

---

## Data Durability vs Distribution

Ratatosk provides **durability** (surviving restarts) but not **distribution** (surviving node failure with zero downtime).

| Mechanism | What it provides | What it does NOT provide |
|-----------|-----------------|------------------------|
| RDB snapshot | Point-in-time backup, fast reload | Real-time redundancy |
| AOF append-only file | Command-level durability (fsync policy) | Replication to another node |
| Background save (`BGSAVE`) | Non-blocking snapshot creation | Offsite backup or failover |
| AOF manifest (BASE + INCR) | Structured recovery ordering | Distribution or sharding |

Recovery order on startup: RDB load, then AOF replay. Both operate on the local filesystem only.

**Ratatosk is a single point of failure.** If the node goes down, the service is unavailable until the process restarts and replays persisted data.

---

## Concurrency Model (Capability View)

| Property | Value |
|----------|-------|
| Network I/O | Per-client tokio tasks (concurrent) |
| State mutation | `SharedState` wrapping `Mutex<ServerState>` (serialized) |
| Lock-free stats | `AtomicStatsState` — 11 atomic counters (no lock per request) |
| Lock-free config reads | `arc_swap::ArcSwap<ConfigState>` via `config_cache.load()` |
| Lock-free client ID | `AtomicI64` for new connection ID allocation |
| Pub/Sub delivery | Per-subscriber `tokio::sync::mpsc::channel` (push, no polling) |
| Background threads | Lazy-free, RDB save, AOF rewrite |
| I/O thread pool | Placeholder only (not active) |

Command execution is effectively serial for state mutation. The mutex is held for the duration of each mutating command. However, `SharedState` eliminates ~9 lock acquisitions per client request by moving stats, config reads, and client ID allocation to lock-free paths. Pub/Sub delivery operates outside the mutex via mpsc channels.

---

## Lua Scripting (Capability View, feature-gated)

| Property | Value |
|----------|-------|
| Feature gate | `lua-scripting` |
| Lua version | 5.1 (vendored via `mlua`) |
| Sandbox | TABLE+STRING+MATH+OS+BASE libs only |
| Memory limit | 1 MB per script |
| Instruction limit | 100K per script |
| Nested EVAL | Rejected |

Supported commands when `lua-scripting` is enabled:

| Command | Status |
|---------|--------|
| `EVAL` / `EVAL_RO` | Functional |
| `EVALSHA` / `EVALSHA_RO` | Functional |
| `SCRIPT LOAD` | Functional |
| `SCRIPT EXISTS` | Functional |
| `SCRIPT FLUSH` | Functional |
| `FCALL` / `FUNCTION *` | **Not implemented** (Redis Functions are not supported) |

When `lua-scripting` is disabled, `EVAL` and `EVALSHA` return unsupported errors.

---

## Capability Tier Summary

From the command ledger (`docs/redis-gap-ledger.json`), as of 2026-06-02:

| Tier | Count | Meaning |
|------|-------|---------|
| `distributed_parity` | 0 | No command achieves Redis distributed semantics |
| `behavioral_subset` | 275 | Locally correct behavior for common use cases |
| `baseline_local` | 76 | Standalone-compatible response shell |
| `syntax_only` | 6 | Parses and responds but lacks backing subsystem |
| `unsupported` | 63 | Returns explicit error |

The runtime exposes `ratatosk_capability_tier` in `COMMAND DOCS` responses so clients can programmatically inspect implementation depth.

---

## Honest Summary

Ratatosk is a **standalone in-memory data server** with broad Redis command vocabulary.

It is well suited for:

- Single-node cache with TTL and eviction
- Pub/Sub event bus (local, mpsc push delivery)
- Session/rate-limit counter storage
- Persistent data with RDB + AOF durability
- Lua 5.1 scripting (`lua-scripting` feature gate) — EVAL/EVALSHA with sandboxed execution

It is **not** suited for:

- Multi-node deployments requiring automatic failover
- Workloads that depend on Redis Cluster slot routing
- Sentinel-based service discovery
- Write durability guarantees via replica acknowledgement (`WAIT` with actual replicas)
- Redis Functions execution (`FCALL` / `FUNCTION` family)

---

# Part 5: Redis Gap Analysis

기준일: 2026-06-02 (updated with ecosystem hardening changes)

이 문서는 현재 Ratatosk 코드베이스와 Redis 공식 문서를 대조해, "명령 이름이 존재하는가"가 아니라 "Redis가 약속하는 동작 계약과 운영 모델을 얼마나 실제로 충족하는가"를 정리한다.

비교 기준:

- Ratatosk: 현재 저장소 작업 트리
- Redis: 공식 문서 `latest` 경로(2026-03-12 시점)
- 초점: standalone key-value 서버 이상의 호환성, 운영 특성, 확장 경로

이 문서는 [Part 6: Redis Gap Ledger](#part-6-redis-gap-ledger)를 대체하지 않는다. 오히려 그 문서의 status와 `capability_tier`를 해석하는 보완 문서다.

현재 런타임도 `COMMAND DOCS` 응답에 `ratatosk_capability_tier`를 포함해, 클라이언트가 메타데이터 단계에서 구현 수준을 구분할 수 있게 했다.

## Executive Summary

Ratatosk는 이미 다음 영역에서는 꽤 많이 진척되어 있다.

- RESP 파싱/인코딩
- 주요 자료구조(string/hash/list/set/zset/stream)
- expiry/eviction/lazy free
- RDB/AOF 기본 경로
- ACL, Pub/Sub, 일부 운영 명령의 형태적 호환성

하지만 Redis를 실제 운영에 투입할 때 중요한 다음 축에서는 아직 큰 갭이 있다.

1. 복제, Sentinel, Cluster가 "시스템"으로 존재하지 않는다.
2. Lua scripting은 feature-gated로 구현됐지만 (`lua-scripting`), Redis Functions는 아직 비어 있다.
3. blocking command, client-side caching이 Redis 내부 모델과 다르다. Pub/Sub delivery는 mpsc push로 개선됨.
4. persistence background work가 Redis의 fork/COW, multipart AOF 모델과 다르다.
5. `SharedState`로 lock-free fast path를 분리하고, DB 접근은 per-DB `parking_lot::RwLock`으로 분리했다. Meta-state(pubsub, ACL 등)는 여전히 `Mutex<ServerState>`에 의존하고, 일반 command path는 여전히 그 mutex 영향 아래 있다.
6. 여러 server/client/admin 명령이 실제 동작보다 "syntax-compatible shell"에 가깝다.

현재 ledger tier summary:

- `unsupported=63`
- `syntax_only=6`
- `baseline_local=76`
- `behavioral_subset=275`
- `distributed_parity=0`

결론적으로 현재 Ratatosk는 "Redis 명령을 많이 이해하는 standalone 메모리 서버"로는 설명될 수 있지만, Redis를 대체하는 드롭인 시스템이라고 보기에는 이른 상태다. 특히 replication, failover, cluster redirection, client-side caching invalidation, scripting 생태계에 의존하는 워크로드는 그대로 이식되기 어렵다.

## Method

내부 근거는 다음 파일을 중심으로 확인했다.

- `crates/ratatosk-engine/src/command/cmd_server.rs`
- `crates/ratatosk-engine/src/command/cmd_cluster.rs`
- `crates/ratatosk-engine/src/command/cmd_sentinel.rs`
- `crates/ratatosk-engine/src/command/cmd_client.rs`
- `crates/ratatosk-engine/src/command/cmd_script.rs`
- `crates/ratatosk-engine/src/command/cmd_generic.rs`
- `crates/ratatosk-engine/src/command/cmd_list.rs`
- `crates/ratatosk-engine/src/command/cmd_stream.rs`
- `crates/ratatosk-engine/src/keyspace.rs`
- `crates/ratatosk-server/src/client.rs`
- `crates/ratatosk-server/src/event_loop.rs`
- `crates/ratatosk-server/src/persistence/`
- `crates/ratatosk-persist/src/aof/manifest.rs`

외부 근거는 Redis 공식 문서를 사용했다.

- Persistence: <https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/>
- Replication: <https://redis.io/docs/latest/operate/oss_and_stack/management/replication/>
- Sentinel: <https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/>
- Scaling / Cluster: <https://redis.io/docs/latest/operate/oss_and_stack/management/scaling/>
- Client-side caching: <https://redis.io/docs/latest/develop/reference/client-side-caching/>
- Lua scripting: <https://redis.io/docs/latest/develop/programmability/eval-intro/>
- Redis Functions: <https://redis.io/docs/latest/develop/programmability/functions-intro/>
- WAIT: <https://redis.io/docs/latest/commands/wait/>
- WAITAOF: <https://redis.io/docs/latest/commands/waitaof/>
- MONITOR: <https://redis.io/docs/latest/commands/monitor/>
- ROLE: <https://redis.io/docs/latest/commands/role/>
- XREAD: <https://redis.io/docs/latest/commands/xread/>

## Structural Findings

### 1. Replication / HA 계층이 없다

Redis 공식 문서는 replication을 비동기 복제와 partial resynchronization(PSYNC) 기반으로 설명하고, `WAIT`/`WAITAOF`는 write durability 또는 replica acknowledgement를 기다리는 동기화 장치로 둔다. Ratatosk는 2026-03-12 기준으로 standalone-local replication metadata skeleton을 갖췄지만, 실제 네트워크 복제 스트림과 backlog는 아직 없다.

현재 코드:

- `MONITOR`는 `+OK`를 반환하고 연결을 monitor 모드로 등록한 뒤, 실행된 명령을 등록된 monitor 클라이언트로 broadcast한다 (baseline): `crates/ratatosk-engine/src/command/cmd_server.rs`
- `ROLE`은 이제 master/replica 모드, logical replication offset, 등록된 replica 목록을 반영한다: `crates/ratatosk-engine/src/command/cmd_server_replication.rs`
- `REPLCONF`는 LISTENING-PORT/CAPA/IP-ADDRESS/ACK/GETACK를 per-client replica metadata에 연결한다: `crates/ratatosk-engine/src/command/cmd_server_replication.rs`
- `PSYNC`는 ERR를 반환한다 (standalone mode): `crates/ratatosk-engine/src/command/cmd_server_replication.rs`
- `REPLICAOF`/`SLAVEOF`는 `NO ONE` 이외의 인자에 대해 ERR를 반환한다. `NO ONE`은 여전히 수용된다: `crates/ratatosk-engine/src/command/cmd_server_replication.rs`
- `WAIT`/`WAITAOF`는 tracked replica ACK offset과 local AOF health를 즉시 반영하지만, 여전히 timeout 동안 ACK를 기다리는 blocking semantics는 없다: `crates/ratatosk-engine/src/command/cmd_generic_replication.rs`

실제 영향:

- Redis 클라이언트나 운영 도구는 이제 최소한 replication progress와 replica lag의 local snapshot은 읽을 수 있다.
- 하지만 `WAIT`/`WAITAOF`는 여전히 non-blocking immediate accounting이므로, Redis처럼 timeout 동안 실제 ACK를 기다리지는 못한다.
- Redis ecosystem에서 흔한 "primary + replica + Sentinel" 운영 모델과 직접 호환되지 않는다.

미래 파손 시나리오:

- 쓰기 직후 `WAIT 1 1000`은 이미 ACK된 replica 수는 반영하지만, timeout 동안 새 ACK를 기다리지는 못해 Redis보다 일찍 0 또는 부분 결과를 반환할 수 있다.
- 재시작/장애 전환 시 replica catch-up 또는 partial sync 같은 개념이 없으므로 운영 자동화가 구성되지 않는다.

권장 순서:

1. 완료: `ROLE`, `INFO replication`, replication offsets를 standalone-local 상태로 연결
2. 다음: `REPLICAOF`/`PSYNC`에 실제 network replication stream과 backlog를 붙일 것
3. 그 후에만 `WAIT`/`WAITAOF`를 blocking semantics까지 승격할 것

Migration cost: high

### 2. Sentinel과 Cluster는 명령 표면만 있고 시스템은 없다

Redis 공식 문서에서 Sentinel은 분산 감시/쿼럼/자동 failover 시스템이고, Cluster는 16384 hash slot, node metadata, redirect, gossip, failover를 포함한 분산 데이터 시스템이다. Ratatosk는 cluster hash slot 계산 일부와 help text는 있지만, 실제 cluster bus, node table, redirect, Sentinel state machine이 없다.

현재 코드:

- `CLUSTER`는 `INFO`, `MYID`, `KEYSLOT`, `COUNTKEYSINSLOT`, `GETKEYSINSLOT`, `HELP`를 처리하고, `SLOTS`/`SHARDS`/`LINKS`는 빈 배열, `NODES`/`REPLICAS`/`SLAVES`는 single-node 에러, 그 외 서브커맨드는 disabled 에러다: `crates/ratatosk-engine/src/command/cmd_cluster.rs:24-37`, `:283-291`
- `CLUSTER INFO`도 `cluster_enabled:0`과 zeroed counters를 돌려준다: `crates/ratatosk-engine/src/command/cmd_cluster.rs:39-58`
- `READONLY`, `READWRITE`, `ASKING`은 모두 단순 `OK`: `crates/ratatosk-engine/src/command/cmd_cluster.rs:293-313`
- `SENTINEL`은 `HELP` 외에는 항상 "not configured as a Sentinel" 에러다: `crates/ratatosk-engine/src/command/cmd_sentinel.rs:11-20`
- 그런데 help text에는 Redis Sentinel의 전체 운영 서브커맨드가 나열돼 있다: `crates/ratatosk-engine/src/command/cmd_sentinel.rs:23-123`

실제 영향:

- slot 계산이나 slot별 key 조회는 가능해도, MOVED/ASK redirect가 없으므로 cluster-aware client는 정상 동작하지 않는다.
- Sentinel discovery endpoint를 기대하는 서비스 디스커버리 또는 failover automation은 Ratatosk를 대상으로 붙을 수 없다.
- "명령이 있다"와 "분산 시스템이 있다"가 완전히 분리되어 있다.

미래 파손 시나리오:

- cluster client가 slot map을 얻고 redirect를 기대하는 순간, Ratatosk는 standalone semantics만 제공해 라우팅 계약이 깨진다.
- Sentinel-aware connection bootstrap이 master 주소를 질의하면 아예 운영 정보가 나오지 않는다.

권장 순서:

1. standalone only 제품으로 계속 갈지, replication+Sentinel, 혹은 cluster까지 갈지 먼저 제품 경계를 결정할 것
2. 그 전까지는 cluster/sentinel 명령을 "지원"으로 마케팅하거나 ledger에서 `done`으로 표시하지 않는 편이 안전하다

Migration cost: high

### 3. Lua Scripting은 feature-gated로 구현됐지만, Functions는 아직 비어 있다

Redis 공식 문서는 `EVAL`의 atomic execution과 Lua integration을, Functions 문서는 library lifecycle과 server-side programmability를 전제로 한다.

**Lua 5.1 scripting** (`lua-scripting` feature gate):

- `EVAL`, `EVALSHA`, `EVAL_RO`, `EVALSHA_RO` — Lua 5.1 VM에서 실행. `redis.call()` / `redis.pcall()`로 내부 `execute()`에 bridge.
- `SCRIPT LOAD/EXISTS/FLUSH` — SHA1 캐시 관리.
- Sandbox: TABLE+STRING+MATH+OS+BASE 라이브러리만 허용, 1MB 메모리 제한, 100K instruction 제한.
- Nested EVAL/EVALSHA 거부. Type conversion은 Redis 규약 준수 (true->1, false->nil, table->Array, number->Integer).
- 구현: `crates/ratatosk-engine/src/command/lua_runtime.rs` (thread-local Lua VM, mlua 기반)

**Redis Functions는 아직 비어 있다:**

- `FCALL`은 항상 function not found: `crates/ratatosk-engine/src/command/cmd_script.rs:292-301`
- `FUNCTION LOAD/DELETE/RESTORE`는 unsupported, `LIST`는 빈 배열, `DUMP`는 null, `STATS`는 빈 엔진 목록이다: `crates/ratatosk-engine/src/command/cmd_script.rs:303-416`

실제 영향:

- `lua-scripting` feature 활성화 시, Redis에서 흔한 Lua-based atomic business logic, compare-and-set, rate limiting 스크립트를 그대로 옮길 수 있다.
- Redis Functions를 사용하는 최신 Redis 배포 패턴과는 여전히 호환되지 않는다.
- feature 미활성화 시 `EVAL`은 unsupported 에러를 반환한다.

미래 파손 시나리오:

- Functions (`FCALL`) 기반 서버측 로직을 Ratatosk에선 재현할 수 없다.
- Lua scripting feature가 비활성화된 빌드에서 `EVALSHA` hot path는 여전히 실패한다.

권장 순서:

1. ~~Lua engine을 실제로 넣을지~~ → 완료 (`lua-scripting` feature gate)
2. Redis Functions를 구현할지, 명시적으로 "not supported"로 유지할지 결정

Migration cost: medium (Lua scripting 해결, Functions 미해결)

### 4. Blocking command는 blocked wait registry를 갖췄지만 Redis scheduler parity에는 아직 못 미친다

Redis 공식 문서의 blocking commands는 서버가 blocked clients를 관리하고 조건이 충족되면 깨워서 응답하는 형태다. Ratatosk는 2026-03-12 기준으로 더 이상 polling-only는 아니다. 이제 한 번 실행해서 결과가 없으면 `CommandOutcome::blocking(...)`이 watched key 집합을 담아 반환되고, server state가 blocked wait registry를 유지하며 write path가 해당 key waiter를 깨운다. 다만 Redis의 공정한 blocked-client scheduler와 full command coverage까지 올라온 상태는 아니다.

현재 코드:

- `BLPOP`/`BRPOP`/`BLMOVE`/`BLMPOP`, `BZ*`, `XREAD BLOCK`/`XREADGROUP BLOCK`은 1회 probe 후 watched key를 담은 blocking outcome을 만든다: `crates/ratatosk-engine/src/command/cmd_list.rs`, `crates/ratatosk-engine/src/command/cmd_sorted_set.rs`, `crates/ratatosk-engine/src/command/cmd_stream.rs`
- server state는 blocked client별 watched key와 notifier를 registry로 유지한다: `crates/ratatosk-engine/src/keyspace.rs`
- write path는 변경된 key에 대해 blocked waiter를 먼저 notify하고, 그 다음 tracking invalidation을 발행한다: `crates/ratatosk-engine/src/command/mod.rs`
- runtime은 blocking 대기 중 `Notify`, timeout, disconnect를 동시에 기다리며 wakeup 시 즉시 재실행한다: `crates/ratatosk-server/src/client.rs`

실제 영향:

- list/sorted-set/stream blocking command는 이제 producer-side wakeup을 가지므로, matching write가 들어오면 polling interval을 기다리지 않고 다시 실행된다.
- blocked client inventory가 runtime registry에 반영돼 `CLIENT LIST`/`INFO clients`와도 일관성을 가진다.
- 다만 Redis처럼 command family 전반이 동일 scheduler를 공유하는 구조는 아니고, fairness/priority ordering, cross-command wakeup 정책, `CLIENT UNBLOCK` integration은 아직 없다.

미래 파손 시나리오:

- 많은 blocked consumer가 한 key set을 공유할 때 Redis와 같은 fairness나 starvation 방지가 없어 wakeup ordering이 다를 수 있다.
- `WAIT`/`WAITAOF`, Pub/Sub, redirect tracking 같은 인접 기능은 아직 별도 wakeup model을 쓰거나 polling에 의존하므로 운영 특성이 균일하지 않다.

권장 순서:

1. 완료: list/sorted-set/stream blocking command에 blocked wait registry와 producer-side wakeup 연결
2. 다음: fairness, `CLIENT UNBLOCK`, 추가 blocking families를 같은 scheduler 모델로 확장
3. 그 후: timeout/backoff 재시도는 strict fallback 경로로 더 축소

Migration cost: high

### 5. Persistence background work가 Redis와 다른 비용 모델을 가진다

Redis persistence 문서는 RDB background save와 AOF rewrite가 copy-on-write/fork와 background I/O를 활용하고, AOF rewrite는 현재 데이터셋을 재구성하는 최소 명령 집합을 만드는 방향이다. Ratatosk는 여전히 snapshot clone과 single-file rewrite에 크게 의존하지만, multipart AOF manifest의 bootstrap/startup recovery baseline은 이제 runtime에 연결됐다.

현재 코드:

- `DataState::snapshot_all()`은 per-DB read lock을 순차적으로 잡아 각 DB를 clone한다 (global lock이 아닌 per-DB `parking_lot::RwLock` 사용): `crates/ratatosk-engine/src/keyspace.rs`
- `BGSAVE` 시작 시 per-DB read lock을 순차적으로 잡아 snapshot clone을 만든다 (전체 DB를 한 번에 잠그지 않음): `crates/ratatosk-server/src/persistence/`
- synchronous `SAVE`도 동일하게 clone 기반이다: `crates/ratatosk-server/src/persistence/:679-686`
- runtime은 legacy single-file 경로를 compatibility fallback으로 유지하지만, appendonly bootstrap은 manifest를 우선 사용한다: `crates/ratatosk-server/src/persistence/`
- `BGREWRITEAOF`는 여전히 기존 AOF를 읽어서 `SELECT`를 건너뛰고 동일 command stream을 다시 append한다: `crates/ratatosk-server/src/persistence/`
- `AofManifest`는 save/load, bootstrap, startup recovery baseline에 더해 manifest-backed rewrite 후 새 incr 회전과 manifest commit baseline까지 runtime 경로에 연결됐다. 다만 BASE file materialization과 full atomic switch transaction은 아직 없다: `crates/ratatosk-persist/src/aof/manifest.rs`, `crates/ratatosk-persist/src/aof/switch.rs`, `crates/ratatosk-server/src/persistence/`

실제 영향:

- 큰 데이터셋에서 `SAVE`/`BGSAVE` 시작 순간 메모리 사용량과 pause cost가 Redis보다 나빠질 수 있다.
- AOF rewrite가 "현재 상태 compact"가 아니라 "기존 로그 재작성"에 가깝기 때문에 로그 정리 효과가 제한적이다.
- Redis 7+ multipart AOF의 bootstrap/recovery/rewrite-rotation baseline에는 가까워졌지만, BASE materialization과 current-state compaction 모델은 아직 맞지 않는다.

미래 파손 시나리오:

- 데이터셋이 커질수록 BGSAVE 트리거 순간 clone cost가 latency spike와 memory spike로 나타난다.
- 긴 수명의 write-heavy 시스템에서 BGREWRITEAOF 이후에도 파일 압축 효과가 충분하지 않을 수 있다.

권장 순서:

1. persistence snapshot abstraction을 `ServerState` clone에서 분리
2. 최소한 keyspace serialization 전용 snapshot view를 만들 것
3. AOF rewrite는 current state materialization 기반으로 다시 설계할 것
4. multipart AOF manifest switch와 rewrite rotation까지 runtime/persist 경계에 맞춰 완성할 것

Migration cost: high

### 6. Runtime concurrency 모델이 Redis와도 다르고, 멀티코어 확장 모델과도 다르다

Ratatosk는 per-client tokio task를 띄우지만, 실제 명령 실행은 여전히 `SharedState` 내부의 `Mutex<ServerState>`를 잠그고 진행한다. 최근 리팩터링으로 `SharedState.data`와 inner `ServerState.data`가 같은 `DataState` backing shard를 공유하게 되어 outer/inner data divergence 리스크는 줄었지만, command execution path의 기본 성질이 mutex-serialized라는 점은 그대로다. `SharedState`가 `AtomicStatsState`, `ArcSwap<ConfigState>`, atomic `next_client_id`를 lock-free로 분리하여 요청당 ~9회의 lock 획득을 제거했지만, state mutation 자체는 여전히 단일 mutex 기반이다. Redis의 전통적인 장점은 단일 event loop 위에서 명확한 순서를 유지하는 데 있고, 최근 버전은 I/O thread 등 경계를 명확히 둔다. Ratatosk는 그 중간 형태라서 lock-free fast path 개선에도 불구하고 state mutation 병목은 남아 있다.

현재 코드:

- shared state: `SharedState` (내부 `Mutex<ServerState>` + lock-free components): `crates/ratatosk-server/src/client.rs`
- 일반 명령 실행 직전마다 내부 Mutex를 잡는다: `crates/ratatosk-server/src/client.rs`
- outer `SharedState.data`와 inner `ServerState.data`는 이제 같은 `Arc`-backed shard storage를 공유한다: `crates/ratatosk-engine/src/keyspace.rs`
- stats 갱신, config 읽기, client id 할당은 lock-free 경로로 분리됨
- Pub/Sub delivery는 per-subscriber mpsc channel로 Mutex 밖에서 수행됨
- accept loop는 클라이언트별 task를 무제한 생성하는 구조다: `crates/ratatosk-server/src/event_loop.rs`
- `IoThreadPool`은 placeholder다: `crates/ratatosk-server/src/io_thread.rs`

실제 영향:

- lock-free fast path (stats, config, client id, pub/sub delivery)로 경합이 줄었지만, state mutation은 하나의 lock으로 수렴한다.
- ~~blocked command polling, Pub/Sub polling~~ → Pub/Sub는 mpsc push로 전환됨. cron, persistence bookkeeping은 같은 상태 경계에 남아 있다.
- "async 서버인데 실제 state path는 serial mutex"라는 형태 때문에 성능 분석과 최적화가 더 어렵다.

미래 파손 시나리오:

- 연결 수가 많을수록 tokio task overhead와 mutex contention이 누적된다.
- tail latency가 command complexity보다 lock wait time에 더 민감해질 수 있다.

권장 순서:

1. 제품 전략을 "single-threaded core + background I/O"로 갈지, "sharded state"로 갈지 먼저 고를 것
2. 그 전까지는 global mutex path의 observability를 더 강화할 것

Migration cost: high

## Tactical Findings

### 1. CLIENT TRACKING은 direct-key invalidation skeleton까지는 올라왔지만 Redis full tracking과는 아직 거리가 있다

Redis client-side caching 문서는 server-side key tracking과 invalidation delivery를 전제로 한다. Ratatosk는 2026-03-13 기준으로 direct-key invalidation registry와 async invalidate push를 넘어, `BCAST`/`PREFIX`/`NOLOOP`와 `OPTIN`/`OPTOUT` one-shot gating, redirect target notifier wakeup, connected-target validation, dead-target drop, target disconnect 시 `broken_redirect` marking, RESP3 `tracking-redir-broken` push, tracker direct fallback까지 server-side/runtime path에 연결했다. 다만 unsupported 조합 처리와 Redis full tracking contract 전체는 아직 남아 있다.

현재 코드:

- key access/write 경로가 tracking registry, broadcast registry, invalidate push를 함께 건드린다: `crates/ratatosk-engine/src/command/mod.rs`, `crates/ratatosk-engine/src/keyspace.rs`
- `CLIENT TRACKING`은 ON/OFF/REDIRECT와 함께 `BCAST`/`PREFIX`/`NOLOOP`/`OPTIN`/`OPTOUT`를 registry reset과 함께 처리한다: `crates/ratatosk-engine/src/command/cmd_client.rs`
- runtime은 client별 async-push notifier와 pending queue를 함께 사용해 invalidation을 실제 전달한다: `crates/ratatosk-server/src/client.rs`
- `TRACKINGINFO`와 `GETREDIR`는 configured redirect를 유지하고, breakage는 `broken_redirect` flag로 별도 노출한다: `crates/ratatosk-engine/src/command/cmd_client.rs`
- `CLIENT CACHING YES|NO`는 `OPTIN`/`OPTOUT` 모드에서 다음 read 1회에만 적용되는 gating으로 동작한다: `crates/ratatosk-engine/src/command/cmd_client.rs`, `crates/ratatosk-engine/src/command/mod.rs`
- `REDIRECT`는 이제 연결된 target client만 수용하고, disconnect된 target으로는 pending invalidation을 쌓지 않는다. RESP3 tracker에는 `tracking-redir-broken` push를 보내고, delivery는 tracker direct fallback으로 전환된다: `crates/ratatosk-engine/src/command/cmd_client.rs`, `crates/ratatosk-engine/src/keyspace.rs`, `crates/ratatosk-server/src/client.rs`

영향:

- direct-key tracking과 broadcast-prefix tracking을 쓰는 클라이언트는 invalidate push를 받을 수 있다.
- self-write invalidation은 `NOLOOP`로 억제할 수 있다.
- assisted caching `OPTIN`/`OPTOUT`도 이제 next-command gating까지 baseline 수준으로 동작한다.
- redirect target도 이제 polling tick을 기다리지 않고 notifier로 깨어난다.
- dead target으로의 stale enqueue도 이제 막힌다.
- target disconnect 뒤에도 tracker는 configured redirect id를 유지하고, `TRACKINGINFO flags`에 `broken_redirect`가 붙는다.
- RESP3 tracker는 `tracking-redir-broken` push를 받고, 이후 tracked key invalidation은 tracker 자신에게 직접 돌아온다.
- 하지만 unsupported 조합 처리와 fallback semantics 전체가 Redis와 완전히 같지는 않다.

Fix:

- 남은 작업: unsupported 조합 처리와 redirect breakage 이후 fallback semantics를 Redis 계약에 더 가깝게 만들 것

### 2. CLIENT PAUSE/UNPAUSE/UNBLOCK/SETINFO/REPLY는 대부분 no-op 또는 local-only다

현재 코드:

- `CLIENT PAUSE` / `UNPAUSE`는 ERR를 반환한다: `crates/ratatosk-engine/src/command/cmd_client.rs:197-213`
- `CLIENT UNBLOCK`은 항상 `0`: `crates/ratatosk-engine/src/command/cmd_client.rs:215-236`
- `CLIENT SETINFO`는 `lib-name`/`lib-ver`를 `ClientState`에 저장하고, `CLIENT LIST` 출력에 반영한다: `crates/ratatosk-engine/src/command/cmd_client_tracking.rs`
- `CLIENT REPLY`는 I/O loop에서 실제 적용된다. `off`는 응답을 억제하고, `skip`은 다음 응답 하나를 억제하며, push notification은 영향받지 않는다: `crates/ratatosk-engine/src/command/cmd_client_tracking.rs`

영향:

- 운영 중 pause/unblock 제어를 쓰는 도구와 맞지 않는다.
- library metadata나 reply suppression semantics를 기대하는 클라이언트가 오판할 수 있다.

Fix:

- 지원하지 않을 기능은 명시적 unsupported error로 바꾸고, 지원할 기능만 실제 경로에 연결할 것

### 3. CLIENT LIST/INFO가 실제 connection inventory를 노출하지 않는다

현재 코드:

- runtime은 live socket metadata를 server-wide client registry에 밀어 넣고, `CLIENT LIST`/`CLIENT INFO`/`INFO clients`가 이를 읽는다: `crates/ratatosk-server/src/client.rs`, `crates/ratatosk-engine/src/keyspace.rs`, `crates/ratatosk-engine/src/command/cmd_client.rs`, `crates/ratatosk-engine/src/command/cmd_server.rs`
- formatter는 여전히 `fd`, memory counters, 일부 subscription 세부 필드를 baseline 수준으로만 채운다.

영향:

- 운영자는 이제 실제 connected/blocked/tracking inventory를 볼 수 있다.
- 다만 Redis tooling이 기대하는 전체 필드 충실도와 server-wide control plane은 아직 부족하다.

Fix:

- 남은 작업: Redis가 노출하는 전체 client metadata와 pause/unblock/kill semantics를 registry에 연결할 것

### 4. Pub/Sub는 mpsc push delivery로 전환됐지만 subscribed-state 제약은 아직 약하다

현재 코드:

- ~~pending queue polling~~ → per-subscriber `tokio::sync::mpsc::channel` 기반 push delivery로 전환 완료.
- `register_client()`가 `mpsc::Receiver<PubSubMessage>`를 반환. channel capacity = hard_limit.
- `publish()`는 `try_send()` 사용. channel full 시 overflow → disconnect.
- client loop는 `WaitResult` enum (`PubSubMsg | PubSubClosed | MonitorWake | NetworkRead`)으로 통합.
- monitor notifications는 별도 `Arc<Notify>` 경로 (`register_monitor_notifier()`).
- 제거: `drain_messages()`, `take_overflowed_client()`, `client_notifiers` HashMap, `soft_limit_exceeded_at`.
- subscription 상태여도 일반 command pipeline이 계속 동작한다: `crates/ratatosk-server/src/client.rs`

영향:

- ~~Redis의 push-heavy event loop delivery보다 coarse하다~~ → mpsc push delivery로 Redis의 push 모델에 근접.
- subscribed-state에서 허용 명령 집합 차이로 일부 클라이언트 가정이 깨질 수 있다.

Fix:

- subscribed client state와 allowed command subset을 명시화할 것

### 5. MONITOR, MEMORY, LATENCY 일부 응답은 baseline diagnostics다

현재 코드:

- `MONITOR`는 `+OK`를 반환하고 연결을 monitor 모드로 등록한다 (`register_monitor` + `set_monitor`). 이후 실행되는 명령은 Redis 형식의 MONITOR 라인(`+<ts>.<us> [db addr] "CMD" "arg"...`)으로 등록된 monitor 클라이언트에게 broadcast된다: `crates/ratatosk-engine/src/command/cmd_server.rs` (`cmd_monitor`), broadcast는 `crates/ratatosk-engine/src/command/mod.rs` (`format_monitor_line` / `broadcast_monitor_message`)
- `LATENCY GRAPH`는 16행 ASCII art를 출력하며 normalized min-max scaling을 적용한다. `LATENCY DOCTOR`는 이벤트별 median/avg/min/max 통계를 출력한다. `LATENCY HISTOGRAM`은 coarse summary다: `crates/ratatosk-engine/src/command/cmd_server_latency.rs`
- `MEMORY DOCTOR`: 실제 진단 구현 완료. 빈 인스턴스 감지, dataset 대비 overhead 비율 분석. 정상 시 `"Sam, I have no memory problems"`, 문제 시 항목별 진단 보고.
- `MEMORY MALLOC-STATS`: `#[cfg(feature = "mimalloc")]` 활성화 시 `mi_stats_merge()` + `mi_stats_print_out()` FFI로 mimalloc 통계 출력. 미활성화 시 allocator 정보 메시지 반환.
- `MEMORY PURGE`: `#[cfg(feature = "mimalloc")]` 활성화 시 `mi_collect(true)`로 적극적 메모리 회수. 미활성화 시 OK 반환.

영향:

- mimalloc feature 미활성화 시 MALLOC-STATS는 여전히 제한적 정보만 반환한다.

Fix:

- ~~진짜 지표를 채우거나, 최소한 help/ledger/status에서 baseline semantics를 더 강하게 표기할 것~~ → DOCTOR/PURGE 구현 완료. MALLOC-STATS는 mimalloc feature 의존.

## Dependency Graph Issues

### 1. `ratatosk-persist`의 multipart AOF abstraction이 runtime 전체 lifecycle을 아직 소유하지 못한다

- 문제: `AofManifest` save/load, startup recovery, rewrite 후 incr rotation/manifest commit baseline은 올라왔지만, BASE materialization과 full atomic switch transaction은 아직 `ratatosk-server` orchestration과 기존 single-file rewrite 모델에 묶여 있다.
- 증거: `crates/ratatosk-persist/src/aof/manifest.rs`, `crates/ratatosk-persist/src/aof/rewrite.rs`, `crates/ratatosk-server/src/persistence/`
- 영향: Redis 7+ persistence evolution을 따라갈 때 crate 경계가 다시 흐려지고, multipart 운영 규칙이 runtime 정책과 섞인다.
- fix: rewrite lifecycle과 file rotation 정책을 `ratatosk-persist` 쪽으로 더 끌어내릴 것

### 2. Command surface가 실제 subsystem readiness보다 앞서 있다

- 문제: `cmd_cluster`, `cmd_sentinel`, `cmd_client`, `cmd_script`가 많은 명령명을 외부 계약으로 노출하지만, 내부 subsystem이 그만큼 존재하지 않는다.
- 영향: crate 경계상 `command` layer가 capability advertisement를 과도하게 담당한다.
- 진행 상태(2026-03-12): `COMMAND DOCS`가 `ratatosk_capability_tier`와 tier별 summary를 노출하고, `CLUSTER HELP`/`SENTINEL HELP`가 standalone 한계를 명시하도록 보강됐다.
- 남은 작업: unsupported/no-op 계열을 `COMMAND INFO`, 기타 introspection surface, 문서 생성 파이프라인까지 일관되게 전파할 것

## Gap Priority

P0:

- Replication / PSYNC / ROLE / WAIT / WAITAOF를 실제 semantics로 만들지 않으면, Redis replacement로 포지셔닝하기 어렵다.
- Cluster / Sentinel은 help text보다 실제 subsystem 유무가 중요하므로, 제품 범위를 먼저 결정해야 한다.
- ~~Scripting / Functions 부재는 많은 실사용 Redis 워크로드를 바로 막는다~~ → Lua scripting은 `lua-scripting` feature gate로 해결. Redis Functions는 아직 미구현.

P1:

- ~~Blocking commands를 waiter model로 전환~~ → 완료
- client-side caching invalidation을 redirect wakeup까지 확장
- persistence rewrite를 current-state compaction 모델로 재설계

P2:

- CLIENT LIST/INFO 실상화
- ~~Pub/Sub delivery path 개선~~ → 완료 (mpsc push delivery)
- MONITOR/MEMORY/LATENCY 운영 진단 개선

## Suggested Refactoring Sequence

1. 제품 경계 재정의

- "Redis-compatible standalone server"로 제한할지
- "replication + Sentinel까지" 갈지
- "cluster까지" 갈지 먼저 결정해야 한다

2. 허위 양성 제거

- 완료: `redis-gap-ledger`의 `done` 중 no-op/unsupported/baseline shell 항목을 capability tier로 재분류
- 완료: help text와 `COMMAND DOCS` metadata에서 unsupported 기능을 명확히 드러냄
- 잔여: `COMMAND INFO` 등 다른 introspection surface도 같은 tier semantics를 반영할 것

3. replication skeleton 우선 구축

- replica state
- replication offsets
- `ROLE`
- `INFO replication`
- backlog
- `WAIT`

4. blocking/client-tracking/runtime 정비

- blocked scheduler parity(`CLIENT UNBLOCK`, fairness, 추가 blocking families)
- invalidation registry 확장(redirect wakeup)
- client registry
- lock contention observability

5. persistence 재설계

- snapshot clone 제거 또는 축소
- AOF rewrite를 current dataset materialization으로 전환
- multipart AOF manifest switch와 rewrite rotation 완성

6. 그 다음에만 Sentinel/Cluster 판단

- replication이 없는 상태에서 Sentinel/Cluster를 확장하는 것은 순서가 뒤집혀 있다

## Detailed Execution Plans

### Workstream A: client-side caching redirect wakeup 정합화

목표:

- `CLIENT TRACKING REDIRECT <id>`가 단순 target accounting이 아니라 실제 delivery/wakeup 계약까지 갖도록 만든다.
- direct tracking, `BCAST`, `PREFIX`, `NOLOOP`, `OPTIN`, `OPTOUT`가 redirect target에서도 같은 규칙으로 동작하게 만든다.
- invalidation delivery가 현재의 polling 의존 경로에서 event-driven 경로로 더 이동하도록 만든다.

권장 범위:

- 이번 workstream은 Redis full monitor/pubsub overhaul이 아니다.
- 범위는 `CLIENT TRACKING`, invalidate push delivery, redirect target lifecycle, wakeup observability로 한정한다.

Phase A0. 계약 표 확정

1. direct tracking / broadcast tracking / redirect 조합별 기대 동작 표를 문서에 먼저 고정한다.
2. `REDIRECT` 대상이 없을 때, 끊겼을 때, tracker와 target이 같을 때, target이 blocked 상태일 때 동작을 결정한다.
3. `NOLOOP`의 기준이 tracker client인지 target client인지 명시한다.
4. RESP2/RESP3 연결에서 invalidate push 허용 조건을 명시한다.

성공 기준:

- [Part 5: Redis Gap Analysis](#part-5-redis-gap-analysis)와 `docs/redis-gap-review-2026-03-12.md`에 behavior matrix가 생긴다.
- 구현 전후에 어떤 케이스가 바뀌는지 reviewer가 문서만 보고 판단할 수 있다.

Phase A1. 상태 모델 정규화

1. `ClientState`의 tracking 설정과 runtime delivery capability를 분리한다.
2. `ServerState`에 tracker config, redirect target liveness, pending invalidate delivery state를 한곳에서 볼 수 있는 registry를 둔다.
3. disconnect, `RESET`, `CLIENT TRACKING OFF`, redirect target 교체 시 정리 순서를 명시한다.
4. `CLIENT LIST`/`CLIENT INFO`/`TRACKINGINFO`가 같은 source-of-truth를 읽게 만든다.

대상 파일:

- `crates/ratatosk-engine/src/keyspace.rs`
- `crates/ratatosk-engine/src/command/mod.rs`
- `crates/ratatosk-engine/src/command/cmd_client.rs`
- `crates/ratatosk-server/src/client.rs`

성공 기준:

- tracker/target disconnect에서 stale redirect entry가 남지 않는다.
- `GETREDIR`/`TRACKINGINFO`/runtime delivery state가 서로 어긋나지 않는다.

Phase A2. delivery notifier 도입

1. 완료: invalidate queue enqueue와 동시에 target client notifier를 깨우는 경로를 추가했다.
2. 완료: 현재 pubsub/tracking polling loop와 공존시키되, wakeup path를 우선 사용하고 polling은 fallback으로 남겼다.
3. blocked client, subscribed client, redirected tracking target이 notifier를 공유할지 분리할지 결정한다.
4. notifier fan-out이 과도한 락 경쟁을 만들지 않도록 wakeup granularity를 측정한다.

성공 기준:

- redirected target이 invalidation enqueue 직후 다음 polling tick을 기다리지 않고 깨어난다.
- notifier 추가로 disconnect/shutdown path가 꼬이지 않는다.

Phase A3. command semantics 정합화

1. `CLIENT TRACKING ON REDIRECT <id>`에서 target 존재성 검증 시점을 정한다.
2. target이 현재 push 수신이 불가능하면 명시적 에러를 낼지, best-effort queueing을 할지 결정한다.
3. `CLIENT CACHING YES|NO`의 one-shot gating이 redirect target delivery와 충돌하지 않도록 정리한다.
4. `CLIENT UNBLOCK`, reply mode, Pub/Sub subscribed-state와의 상호작용을 최소 계약 수준으로 문서화한다.

성공 기준:

- direct mode와 redirect mode의 invalidation selection 결과가 tracker 설정만으로 설명된다.
- unsupported인 조합은 명시적으로 문서와 에러 경로에 반영된다.

Phase A4. observability와 회귀 테스트

1. engine unit test:
   - direct + redirect
   - `BCAST + PREFIX + REDIRECT`
   - `NOLOOP + REDIRECT`
   - `OPTIN/OPTOUT + REDIRECT`
2. TCP integration test:
   - tracker/target/writer 3-connection 시나리오
   - target disconnect/reconnect
   - blocked target, subscribed target
3. 운영 가시성:
   - `INFO clients` 또는 별도 stats에 tracking redirect delivery counters 추가 검토
   - dropped invalidation, redirected wake count, target missing count 추가 검토

검증 명령:

- `cargo test -p ratatosk-engine client_tracking_`
- `cargo test -p ratatosk-server client_tracking_`
- `cargo test -p ratatosk-server client_registry_reports_blocked_and_tracking_clients`
- `cargo clippy --workspace --all-targets -- -D warnings`

종료 조건:

- redirect target wakeup이 polling-only가 아니고 notifier 기반으로 동작한다.
- tracking 관련 남은 문서 갭이 "Redis full contract 대비 세부 차이" 수준으로 좁혀진다.

Phase A5. redirect lifecycle / reconnect policy 고정

1. redirect binding의 source-of-truth를 `tracker_id -> target_id` 단일 매핑이 아니라 "tracker config + target liveness" 두 층으로 나눈다.
2. 권장 정책:
   - target disconnect 시 모든 tracker의 active redirect binding을 즉시 해제한다.
   - tracker의 tracking 자체는 유지하되 `redirect=-1`로 떨어뜨리고, 이후 invalidation은 direct tracker connection으로만 전달한다.
   - reconnect한 새 client id는 이전 binding을 자동 승계하지 않는다. explicit `CLIENT TRACKING ... REDIRECT <newid>`만 허용한다.
3. `RESET`, `CLIENT ID` 재할당 없음, connection close, target replacement 순서에서 cleanup 순서를 문서와 코드에 동시에 고정한다.
4. target missing, tracker missing, tracker==target, dead target with pending queue를 각각 state transition table로 정리한다.

상태 전이 표(제안 계약):

| 이벤트 | tracker tracking | redirect field | pending invalidation | expected action |
|------|------|------|------|------|
| tracker enables redirect to live target | 유지 | `target_id` | 새 queue 가능 | bind + notifier armed |
| target disconnect | 유지 | `-1` | target queue drop | trackers auto-detach |
| tracker disconnect | 제거 | n/a | tracker-owned entries drop | registry cleanup |
| tracker sends `TRACKING OFF` | 제거 | `-1` | tracker/redirect queue drop | full cleanup |
| target reconnects with new id | 유지 | `-1` | none | explicit rebind required |

성공 기준:

- reconnect 이후 stale target id로 invalidation이 재개되지 않는다.
- `TRACKINGINFO`, `CLIENT LIST`, delivery registry가 같은 detach 결과를 보여준다.

Phase A6. unsupported combination matrix와 에러 계약

1. Redis 문서를 다시 대조해 `REDIRECT`, `BCAST`, `PREFIX`, `NOLOOP`, `OPTIN`, `OPTOUT`, RESP2/RESP3, subscribed-state, blocked-state 조합을 허용/거부/미지원 세 그룹으로 나눈다.
2. "아직 구현하지 않은데 조용히 accept"하는 조합을 없애고, 허용되지 않은 조합은 명시적 `ERR`로 고정한다.
3. 조합별 ownership을 나눈다.
   - parser/validation: `cmd_client.rs`
   - access marking / one-shot gating: `command/mod.rs`
   - registry state invariants: `keyspace.rs`
   - network delivery preconditions: `client.rs`
4. 문서에 behavior matrix를 넣고, test 이름도 matrix row와 일치시키는 규칙을 만든다.

조합 매트릭스 초안(문서화 대상):

| 조합 축 | 허용 여부 | 구현 책임 | 메모 |
|------|------|------|------|
| `REDIRECT + disconnected target` | 거부 | `cmd_client.rs` | 이미 connected-target validation 있음 |
| `REDIRECT + target disconnect after bind` | baseline broken-redirect + fallback | `keyspace.rs` / `client.rs` | unsupported 조합과 exact fallback semantics gap이 남음 |
| `BCAST + PREFIX + REDIRECT` | 허용 | `mod.rs` / `client.rs` | 회귀 테스트 고정 필요 |
| `NOLOOP + REDIRECT` | 허용 | `mod.rs` | loop 기준 명확화 필요 |
| `OPTIN/OPTOUT + REDIRECT` | 조건부 허용 | `cmd_client.rs` / `mod.rs` | gating과 delivery 순서 문서화 필요 |
| `TRACKING + subscribed/blocking target` | baseline/local only | `client.rs` | unsupported 면적을 명시해야 함 |

성공 기준:

- parser acceptance와 실제 delivery behavior가 분리되지 않는다.
- unsupported 조합은 모두 문서, runtime error, 테스트 이름이 같은 표현을 쓴다.

---

# Part 6: Redis Gap Ledger

> 이 섹션의 원본은 `docs/redis-gap-ledger.json`이며, `scripts/redis_gap_ledger.py`로 재생성할 수 있다.

Redis 명령 카탈로그 대비 Ratatosk 구현 상태 추적표.
원본은 `docs/redis-gap-ledger.json`, 이 문서는 `scripts/redis_gap_ledger.py`로 생성된다.

상태(`status`)와 동작 등급(`capability_tier`)은 다르다.

- `status`: 구현 추적 상태 (`planned`, `partial`, `done` 등)
- `capability_tier`: Redis 의미론 대비 수준 (`unsupported`, `syntax_only`, `baseline_local`, `behavioral_subset`, `distributed_parity`)

## Summary

| Metric | Value |
| --- | ---: |
| Total commands | 420 |
| planned | 0 |
| in_progress | 0 |
| partial | 0 |
| done | 420 |
| excluded | 0 |

## Capability Tier Summary

| Tier | Value |
| --- | ---: |
| unsupported | 63 |
| syntax_only | 6 |
| baseline_local | 76 |
| behavioral_subset | 275 |
| distributed_parity | 0 |

## Group Progress

| Group | done | partial | in_progress | planned | excluded | total |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| bitmap | 7 | 0 | 0 | 0 | 0 | 7 |
| cluster | 35 | 0 | 0 | 0 | 0 | 35 |
| connection | 26 | 0 | 0 | 0 | 0 | 26 |
| generic | 34 | 0 | 0 | 0 | 0 | 34 |
| geo | 10 | 0 | 0 | 0 | 0 | 10 |
| hash | 28 | 0 | 0 | 0 | 0 | 28 |
| hyperloglog | 5 | 0 | 0 | 0 | 0 | 5 |
| list | 22 | 0 | 0 | 0 | 0 | 22 |
| pubsub | 15 | 0 | 0 | 0 | 0 | 15 |
| scripting | 23 | 0 | 0 | 0 | 0 | 23 |
| sentinel | 22 | 0 | 0 | 0 | 0 | 22 |
| server | 83 | 0 | 0 | 0 | 0 | 83 |
| set | 17 | 0 | 0 | 0 | 0 | 17 |
| sorted_set | 35 | 0 | 0 | 0 | 0 | 35 |
| stream | 28 | 0 | 0 | 0 | 0 | 28 |
| string | 25 | 0 | 0 | 0 | 0 | 25 |
| transactions | 5 | 0 | 0 | 0 | 0 | 5 |

## Group Capability Tiers

| Group | distributed_parity | behavioral_subset | baseline_local | syntax_only | unsupported | total |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| bitmap | 0 | 7 | 0 | 0 | 0 | 7 |
| cluster | 0 | 0 | 10 | 3 | 22 | 35 |
| connection | 0 | 11 | 12 | 1 | 2 | 26 |
| generic | 0 | 32 | 2 | 0 | 0 | 34 |
| geo | 0 | 10 | 0 | 0 | 0 | 10 |
| hash | 0 | 28 | 0 | 0 | 0 | 28 |
| hyperloglog | 0 | 5 | 0 | 0 | 0 | 5 |
| list | 0 | 22 | 0 | 0 | 0 | 22 |
| pubsub | 0 | 15 | 0 | 0 | 0 | 15 |
| scripting | 0 | 1 | 11 | 0 | 11 | 23 |
| sentinel | 0 | 0 | 0 | 1 | 21 | 22 |
| server | 0 | 34 | 41 | 1 | 7 | 83 |
| set | 0 | 17 | 0 | 0 | 0 | 17 |
| sorted_set | 0 | 35 | 0 | 0 | 0 | 35 |
| stream | 0 | 28 | 0 | 0 | 0 | 28 |
| string | 0 | 25 | 0 | 0 | 0 | 25 |
| transactions | 0 | 5 | 0 | 0 | 0 | 5 |

## Command Ledger

| Command | Group | Since | Status | Tier | Milestone | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| `ACL` | server | 6.0.0 | done | behavioral_subset | m0-foundation | M0 ACL baseline implemented with central subcommand dispatch and stateful user/log management. |
| `ACL CAT` | server | 6.0.0 | done | behavioral_subset | m0-foundation | M0 ACL baseline implemented (category and category-filter list responses). |
| `ACL DELUSER` | server | 6.0.0 | done | behavioral_subset | m0-foundation | M0 ACL baseline implemented (multi-user delete, default user protected). |
| `ACL DRYRUN` | server | 7.0.0 | done | syntax_only | m0-foundation | M0 ACL baseline implemented (syntax/arity validation with standalone OK simulation). |
| `ACL GENPASS` | server | 6.0.0 | done | behavioral_subset | m0-foundation | M0 ACL baseline implemented (bit-length parsing and deterministic hex password generation). |
| `ACL GETUSER` | server | 6.0.0 | done | behavioral_subset | m0-foundation | M0 ACL baseline implemented (flags/passwords/commands/keys/channels/selectors map response). |
| `ACL HELP` | server | 6.0.0 | done | behavioral_subset | m0-foundation | M0 ACL baseline implemented (help text). |
| `ACL LIST` | server | 6.0.0 | done | behavioral_subset | m0-foundation | M0 ACL baseline implemented (user rule lines). |
| `ACL LOAD` | server | 6.0.0 | done | syntax_only | m0-foundation | M0 ACL baseline implemented (standalone no-op OK). |
| `ACL LOG` | server | 6.0.0 | done | behavioral_subset | m0-foundation | M0 ACL baseline implemented (count retrieval + RESET). |
| `ACL SAVE` | server | 6.0.0 | done | syntax_only | m0-foundation | M0 ACL baseline implemented (standalone no-op OK). |
| `ACL SETUSER` | server | 6.0.0 | done | behavioral_subset | m0-foundation | M0 ACL baseline implemented (on/off/nopass/resetpass/password and rule-token parsing). |
| `ACL USERS` | server | 6.0.0 | done | behavioral_subset | m0-foundation | M0 ACL baseline implemented (sorted user list). |
| `ACL WHOAMI` | server | 6.0.0 | done | behavioral_subset | m0-foundation | M0 ACL baseline implemented (current authenticated ACL user). |
| `APPEND` | string | 2.0.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `ASKING` | cluster | 3.0.0 | done | syntax_only | m5-advanced |  |
| `AUTH` | connection | 1.0.0 | done | behavioral_subset | m0-foundation | M0 compatibility baseline implemented (accepts AUTH <username> <password>). |
| `BGREWRITEAOF` | server | 1.0.0 | done | behavioral_subset | m3-persistence | Async AOF worker rewrite path wired (start/in-progress gate/shutdown drain). |
| `BGSAVE` | server | 1.0.0 | done | behavioral_subset | m3-persistence | M3 baseline implemented (non-blocking acknowledgement path with timestamp update). |
| `BITCOUNT` | bitmap | 2.6.0 | done | behavioral_subset | m4-extended-types |  |
| `BITFIELD` | bitmap | 3.2.0 | done | behavioral_subset | m4-extended-types |  |
| `BITFIELD_RO` | bitmap | 6.0.0 | done | behavioral_subset | m4-extended-types |  |
| `BITOP` | bitmap | 2.6.0 | done | behavioral_subset | m4-extended-types |  |
| `BITPOS` | bitmap | 2.8.7 | done | behavioral_subset | m4-extended-types |  |
| `BLMOVE` | list | 6.2.0 | done | behavioral_subset | m2-collections | M2 blocking semantics implemented (blocked wait registry + producer wakeup with timeout fallback; nil on timeout). |
| `BLMPOP` | list | 7.0.0 | done | behavioral_subset | m2-collections | M2 blocking semantics implemented (blocked wait registry + producer wakeup with timeout fallback; nil on timeout). |
| `BLPOP` | list | 2.0.0 | done | behavioral_subset | m2-collections | M2 blocking semantics implemented (blocked wait registry + producer wakeup with timeout fallback; nil on timeout). |
| `BRPOP` | list | 2.0.0 | done | behavioral_subset | m2-collections | M2 blocking semantics implemented (blocked wait registry + producer wakeup with timeout fallback; nil on timeout). |
| `BRPOPLPUSH` | list | 2.2.0 | done | behavioral_subset | m2-collections | M2 blocking semantics implemented (blocked wait registry + producer wakeup with timeout fallback; nil on timeout). |
| `BZMPOP` | sorted_set | 7.0.0 | done | behavioral_subset | m2-collections | M2 blocking semantics implemented (blocked wait registry + producer wakeup with timeout fallback). |
| `BZPOPMAX` | sorted_set | 5.0.0 | done | behavioral_subset | m2-collections | M2 blocking semantics implemented (blocked wait registry + producer wakeup with timeout fallback). |
| `BZPOPMIN` | sorted_set | 5.0.0 | done | behavioral_subset | m2-collections | M2 blocking semantics implemented (blocked wait registry + producer wakeup with timeout fallback). |
| `CLIENT` | connection | 2.4.0 | done | baseline_local | m0-foundation | M0 compatibility baseline implemented with HELP/ID/GETNAME/SETNAME/INFO/LIST. |
| `CLIENT CACHING` | connection | 6.0.0 | done | baseline_local | m0-foundation | M0 client-tracking baseline implemented: YES/NO parsing and per-client state toggle. |
| `CLIENT GETNAME` | connection | 2.6.9 | done | behavioral_subset | m0-foundation | M0 compatibility baseline implemented. |
| `CLIENT GETREDIR` | connection | 6.0.0 | done | baseline_local | m0-foundation | M0 client-tracking baseline implemented: returns configured tracking redirect id (`0` for self-redirection while enabled, `-1` when tracking is off) and cooperates with `broken_redirect` tracking state. |
| `CLIENT HELP` | connection | 5.0.0 | done | behavioral_subset | m0-foundation | M0 compatibility baseline implemented. |
| `CLIENT ID` | connection | 5.0.0 | done | behavioral_subset | m0-foundation | M0 compatibility baseline implemented. |
| `CLIENT INFO` | connection | 6.2.0 | done | baseline_local | m0-foundation | M0 compatibility baseline implemented (single-connection info string). |
| `CLIENT KILL` | connection | 2.4.0 | done | baseline_local | m0-foundation | M0 client-admin baseline implemented: legacy addr form + ID-filter parsing with deterministic kill count. |
| `CLIENT LIST` | connection | 2.4.0 | done | baseline_local | m0-foundation | M0 compatibility baseline implemented (single-connection list with TYPE/ID filtering baseline). |
| `CLIENT NO-EVICT` | connection | 7.0.0 | done | baseline_local | m0-foundation | M0 client baseline implemented: ON/OFF parsing and local state toggle. |
| `CLIENT NO-TOUCH` | connection | 7.2.0 | done | baseline_local | m0-foundation | M0 client baseline implemented: ON/OFF parsing and local state toggle. |
| `CLIENT PAUSE` | connection | 3.0.0 | done | unsupported | m0-foundation | Returns ERR; CLIENT PAUSE is not supported in this Ratatosk build. |
| `CLIENT REPLY` | connection | 3.2.0 | done | behavioral_subset | m0-foundation | Reply mode enforced in I/O loop: OFF suppresses all responses, SKIP suppresses next response only. Push notifications (pub/sub, invalidation) unaffected. |
| `CLIENT SETINFO` | connection | 7.2.0 | done | behavioral_subset | m0-foundation | LIB-NAME/LIB-VER stored in ClientState and included in CLIENT LIST output. |
| `CLIENT SETNAME` | connection | 2.6.9 | done | behavioral_subset | m0-foundation | M0 compatibility baseline implemented. |
| `CLIENT TRACKING` | connection | 6.0.0 | done | baseline_local | m0-foundation | M0 client-tracking baseline implemented with direct-key invalidation, BCAST/PREFIX/NOLOOP registry, OPTIN/OPTOUT next-command gating, async invalidate push, connected-target REDIRECT validation, target wakeup delivery, `broken_redirect` marking, and RESP3 `tracking-redir-broken` push. |
| `CLIENT TRACKINGINFO` | connection | 6.2.0 | done | baseline_local | m0-foundation | M0 client-tracking baseline implemented: flags/redirect/prefix state plus current direct-key/BCAST tracking metadata surface, configured redirect visibility, and `broken_redirect` state reporting. |
| `CLIENT UNBLOCK` | connection | 5.0.0 | done | syntax_only | m0-foundation | M0 client-admin baseline implemented: ID/mode parsing with deterministic no-op unblock result. |
| `CLIENT UNPAUSE` | connection | 6.2.0 | done | unsupported | m0-foundation | Returns ERR; CLIENT UNPAUSE is not supported in this Ratatosk build. |
| `CLUSTER` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER ADDSLOTS` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER ADDSLOTSRANGE` | cluster | 7.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER BUMPEPOCH` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER COUNT-FAILURE-REPORTS` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER COUNTKEYSINSLOT` | cluster | 3.0.0 | done | baseline_local | m5-advanced |  |
| `CLUSTER DELSLOTS` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER DELSLOTSRANGE` | cluster | 7.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER FAILOVER` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER FLUSHSLOTS` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER FORGET` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER GETKEYSINSLOT` | cluster | 3.0.0 | done | baseline_local | m5-advanced |  |
| `CLUSTER HELP` | cluster | 5.0.0 | done | baseline_local | m5-advanced |  |
| `CLUSTER INFO` | cluster | 3.0.0 | done | baseline_local | m5-advanced |  |
| `CLUSTER KEYSLOT` | cluster | 3.0.0 | done | baseline_local | m5-advanced |  |
| `CLUSTER LINKS` | cluster | 7.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER MEET` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER MIGRATION` | cluster | 8.4.0 | done | unsupported | m5-advanced |  |
| `CLUSTER MYID` | cluster | 3.0.0 | done | baseline_local | m5-advanced |  |
| `CLUSTER MYSHARDID` | cluster | 7.2.0 | done | unsupported | m5-advanced |  |
| `CLUSTER NODES` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER REPLICAS` | cluster | 5.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER REPLICATE` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER RESET` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER SAVECONFIG` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER SET-CONFIG-EPOCH` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER SETSLOT` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER SHARDS` | cluster | 7.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER SLAVES` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER SLOT-STATS` | cluster | 8.2.0 | done | unsupported | m5-advanced |  |
| `CLUSTER SLOTS` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER SYNCSLOTS` | cluster | 8.4.0 | done | unsupported | m5-advanced |  |
| `COMMAND` | server | 2.8.13 | done | behavioral_subset | m0-foundation | M0 baseline implemented with COUNT/LIST/INFO/HELP and root reply. |
| `COMMAND COUNT` | server | 2.8.13 | done | behavioral_subset | m0-foundation | M0 baseline implemented. |
| `COMMAND DOCS` | server | 7.0.0 | done | behavioral_subset | m0-foundation | M0 command-metadata baseline implemented (returns per-command docs map with summary/arity/flags). |
| `COMMAND GETKEYS` | server | 2.8.13 | done | behavioral_subset | m0-foundation | M0 compatibility baseline implemented. |
| `COMMAND GETKEYSANDFLAGS` | server | 7.0.0 | done | behavioral_subset | m0-foundation | M0 compatibility baseline implemented. |
| `COMMAND HELP` | server | 5.0.0 | done | behavioral_subset | m0-foundation | M0 baseline implemented. |
| `COMMAND INFO` | server | 2.8.13 | done | behavioral_subset | m0-foundation | M0 baseline implemented. |
| `COMMAND LIST` | server | 7.0.0 | done | behavioral_subset | m0-foundation | M0 baseline implemented. |
| `CONFIG` | server | 2.0.0 | done | baseline_local | m0-foundation | M0 operational baseline implemented (GET/SET/HELP/RESETSTAT subset). |
| `CONFIG GET` | server | 2.0.0 | done | baseline_local | m0-foundation | M0 operational baseline implemented (glob pattern matching over core params). |
| `CONFIG HELP` | server | 5.0.0 | done | baseline_local | m0-foundation | M0 operational baseline implemented. |
| `CONFIG RESETSTAT` | server | 2.0.0 | done | baseline_local | m0-foundation | M0 operational baseline implemented. |
| `CONFIG REWRITE` | server | 2.8.0 | done | baseline_local | m0-foundation | M0 operational baseline implemented (in-memory acknowledge path). |
| `CONFIG SET` | server | 2.0.0 | done | baseline_local | m0-foundation | M0 operational baseline implemented (timeout/appendonly/save/slowlog params). |
| `COPY` | generic | 6.2.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented (DB/REPLACE options). |
| `DBSIZE` | server | 1.0.0 | done | behavioral_subset | m0-foundation | M0 compatibility baseline implemented. |
| `DEBUG` | server | 1.0.0 | done | unsupported | m0-foundation | M0 admin baseline implemented (HELP + unsupported subcommand response). |
| `DECR` | string | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `DECRBY` | string | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `DEL` | generic | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `DELEX` | string | 8.4.0 | done | behavioral_subset | m1-kv-core | M1 baseline implemented: DELEX key + IFEQ/IFNE/IFDEQ/IFDNE conditions for string values. |
| `DIGEST` | string | 8.4.0 | done | behavioral_subset | m1-kv-core | M1 baseline implemented: DIGEST returns deterministic signed integer hash string for string values. |
| `DISCARD` | transactions | 2.0.0 | done | behavioral_subset | m1-kv-core | M1 transaction baseline implemented. |
| `DUMP` | generic | 2.6.0 | done | behavioral_subset | m1-kv-core | M1 baseline implemented: RATSK1 internal payload serialization for string/hash/list/set. |
| `ECHO` | connection | 1.0.0 | done | behavioral_subset | m0-foundation | M0 implemented and tested. |
| `EVAL` | scripting | 2.6.0 | done | unsupported | m5-advanced |  |
| `EVALSHA` | scripting | 2.6.0 | done | unsupported | m5-advanced |  |
| `EVALSHA_RO` | scripting | 7.0.0 | done | unsupported | m5-advanced |  |
| `EVAL_RO` | scripting | 7.0.0 | done | unsupported | m5-advanced |  |
| `EXEC` | transactions | 1.2.0 | done | behavioral_subset | m1-kv-core | M1 transaction baseline implemented with WATCH conflict abort (null reply) and EXECABORT on queue-time errors. |
| `EXISTS` | generic | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `EXPIRE` | generic | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented (EXPIRE [NX\|XX\|GT\|LT]). |
| `EXPIREAT` | generic | 1.2.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `EXPIRETIME` | generic | 7.0.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `FAILOVER` | server | 6.2.0 | done | unsupported | m0-foundation | M0 standalone baseline implemented (returns unsupported-in-standalone error). |
| `FCALL` | scripting | 7.0.0 | done | unsupported | m5-advanced |  |
| `FCALL_RO` | scripting | 7.0.0 | done | unsupported | m5-advanced |  |
| `FLUSHALL` | server | 1.0.0 | done | behavioral_subset | m0-foundation | M0 compatibility baseline implemented (SYNC/ASYNC accepted). |
| `FLUSHDB` | server | 1.0.0 | done | behavioral_subset | m0-foundation | M0 compatibility baseline implemented. |
| `FUNCTION` | scripting | 7.0.0 | done | syntax_only | m5-advanced |  |
| `FUNCTION DELETE` | scripting | 7.0.0 | done | unsupported | m5-advanced |  |
| `FUNCTION DUMP` | scripting | 7.0.0 | done | syntax_only | m5-advanced |  |
| `FUNCTION FLUSH` | scripting | 7.0.0 | done | syntax_only | m5-advanced |  |
| `FUNCTION HELP` | scripting | 7.0.0 | done | syntax_only | m5-advanced |  |
| `FUNCTION KILL` | scripting | 7.0.0 | done | behavioral_subset | m5-advanced |  |
| `FUNCTION LIST` | scripting | 7.0.0 | done | syntax_only | m5-advanced |  |
| `FUNCTION LOAD` | scripting | 7.0.0 | done | unsupported | m5-advanced |  |
| `FUNCTION RESTORE` | scripting | 7.0.0 | done | unsupported | m5-advanced |  |
| `FUNCTION STATS` | scripting | 7.0.0 | done | syntax_only | m5-advanced |  |
| `GEOADD` | geo | 3.2.0 | done | behavioral_subset | m4-extended-types |  |
| `GEODIST` | geo | 3.2.0 | done | behavioral_subset | m4-extended-types |  |
| `GEOHASH` | geo | 3.2.0 | done | behavioral_subset | m4-extended-types |  |
| `GEOPOS` | geo | 3.2.0 | done | behavioral_subset | m4-extended-types |  |
| `GEORADIUS` | geo | 3.2.0 | done | behavioral_subset | m4-extended-types |  |
| `GEORADIUSBYMEMBER` | geo | 3.2.0 | done | behavioral_subset | m4-extended-types |  |
| `GEORADIUSBYMEMBER_RO` | geo | 3.2.10 | done | behavioral_subset | m4-extended-types |  |
| `GEORADIUS_RO` | geo | 3.2.10 | done | behavioral_subset | m4-extended-types |  |
| `GEOSEARCH` | geo | 6.2.0 | done | behavioral_subset | m4-extended-types |  |
| `GEOSEARCHSTORE` | geo | 6.2.0 | done | behavioral_subset | m4-extended-types |  |
| `GET` | string | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `GETBIT` | bitmap | 2.2.0 | done | behavioral_subset | m4-extended-types |  |
| `GETDEL` | string | 6.2.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `GETEX` | string | 6.2.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `GETRANGE` | string | 2.4.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `GETSET` | string | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `HDEL` | hash | 2.0.0 | done | behavioral_subset | m2-collections | M2 hash core baseline implemented. |
| `HELLO` | connection | 6.0.0 | done | behavioral_subset | m0-foundation | M0 parity baseline implemented (proto negotiation + AUTH/SETNAME syntax + NOPROTO handling). |
| `HEXISTS` | hash | 2.0.0 | done | behavioral_subset | m2-collections | M2 hash core baseline implemented. |
| `HEXPIRE` | hash | 7.4.0 | done | behavioral_subset | m2-collections |  |
| `HEXPIREAT` | hash | 7.4.0 | done | behavioral_subset | m2-collections |  |
| `HEXPIRETIME` | hash | 7.4.0 | done | behavioral_subset | m2-collections |  |
| `HGET` | hash | 2.0.0 | done | behavioral_subset | m2-collections | M2 hash core baseline implemented. |
| `HGETALL` | hash | 2.0.0 | done | behavioral_subset | m2-collections | M2 hash core baseline implemented. |
| `HGETDEL` | hash | 8.0.0 | done | behavioral_subset | m2-collections |  |
| `HGETEX` | hash | 8.0.0 | done | behavioral_subset | m2-collections |  |
| `HINCRBY` | hash | 2.0.0 | done | behavioral_subset | m2-collections | Batch-7 hash extended baseline implemented. |
| `HINCRBYFLOAT` | hash | 2.6.0 | done | behavioral_subset | m2-collections | Batch-7 hash extended baseline implemented. |
| `HKEYS` | hash | 2.0.0 | done | behavioral_subset | m2-collections | Batch-5 baseline implemented (ordered 1->2 execution). |
| `HLEN` | hash | 2.0.0 | done | behavioral_subset | m2-collections | M2 hash core baseline implemented. |
| `HMGET` | hash | 2.0.0 | done | behavioral_subset | m2-collections | M2 hash core baseline implemented. |
| `HMSET` | hash | 2.0.0 | done | behavioral_subset | m2-collections | Batch-7 hash extended baseline implemented. |
| `HOTKEYS` | server | 8.6.0 | done | syntax_only | m0-foundation | M0 admin baseline implemented (GET/RESET/START/STOP/HELP container). |
| `HOTKEYS GET` | server | 8.6.0 | done | syntax_only | m0-foundation | M0 admin baseline implemented (returns empty sample list). |
| `HOTKEYS RESET` | server | 8.6.0 | done | syntax_only | m0-foundation | M0 admin baseline implemented (no-op OK). |
| `HOTKEYS START` | server | 8.6.0 | done | syntax_only | m0-foundation | M0 admin baseline implemented (no-op OK). |
| `HOTKEYS STOP` | server | 8.6.0 | done | syntax_only | m0-foundation | M0 admin baseline implemented (no-op OK). |
| `HPERSIST` | hash | 7.4.0 | done | behavioral_subset | m2-collections |  |
| `HPEXPIRE` | hash | 7.4.0 | done | behavioral_subset | m2-collections |  |
| `HPEXPIREAT` | hash | 7.4.0 | done | behavioral_subset | m2-collections |  |
| `HPEXPIRETIME` | hash | 7.4.0 | done | behavioral_subset | m2-collections |  |
| `HPTTL` | hash | 7.4.0 | done | behavioral_subset | m2-collections |  |
| `HRANDFIELD` | hash | 6.2.0 | done | behavioral_subset | m2-collections | Batch-7 hash extended baseline implemented. |
| `HSCAN` | hash | 2.8.0 | done | behavioral_subset | m2-collections | M2 scan family baseline + cursor progression semantics refined. |
| `HSET` | hash | 2.0.0 | done | behavioral_subset | m2-collections | M2 hash core baseline implemented. |
| `HSETEX` | hash | 8.0.0 | done | behavioral_subset | m2-collections |  |
| `HSETNX` | hash | 2.0.0 | done | behavioral_subset | m2-collections | Batch-7 hash extended baseline implemented. |
| `HSTRLEN` | hash | 3.2.0 | done | behavioral_subset | m2-collections | Batch-7 hash extended baseline implemented. |
| `HTTL` | hash | 7.4.0 | done | behavioral_subset | m2-collections |  |
| `HVALS` | hash | 2.0.0 | done | behavioral_subset | m2-collections | Batch-5 baseline implemented (ordered 1->2 execution). |
| `INCR` | string | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `INCRBY` | string | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `INCRBYFLOAT` | string | 2.6.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `INFO` | server | 1.0.0 | done | baseline_local | m0-foundation | M0 compatibility baseline implemented for SERVER/CLIENTS/STATS/KEYSPACE sections. |
| `KEYS` | generic | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `LASTSAVE` | server | 1.0.0 | done | behavioral_subset | m0-foundation | M0 compatibility baseline implemented. |
| `LATENCY` | server | 2.8.13 | done | baseline_local | m0-foundation | M0 latency baseline implemented with in-memory sample tracking and LATENCY subcommand dispatch. |
| `LATENCY DOCTOR` | server | 2.8.13 | done | baseline_local | m0-foundation | M0 latency baseline implemented (human-readable diagnostics). |
| `LATENCY GRAPH` | server | 2.8.13 | done | baseline_local | m0-foundation | M0 latency baseline implemented (event summary graph text). |
| `LATENCY HELP` | server | 2.8.13 | done | baseline_local | m0-foundation | M0 latency baseline implemented (HELP text). |
| `LATENCY HISTOGRAM` | server | 7.0.0 | done | baseline_local | m0-foundation | M0 latency baseline implemented (coarse latency bucket output). |
| `LATENCY HISTORY` | server | 2.8.13 | done | baseline_local | m0-foundation | M0 latency baseline implemented (timestamp/latency sample rows). |
| `LATENCY LATEST` | server | 2.8.13 | done | baseline_local | m0-foundation | M0 latency baseline implemented (event/latest/max tuple rows). |
| `LATENCY RESET` | server | 2.8.13 | done | baseline_local | m0-foundation | M0 latency baseline implemented (event/all reset with removed count). |
| `LCS` | string | 7.0.0 | done | behavioral_subset | m1-kv-core | M1 baseline implemented: LCS key1 key2 + LEN option for string values. |
| `LINDEX` | list | 1.0.0 | done | behavioral_subset | m2-collections |  |
| `LINSERT` | list | 2.2.0 | done | behavioral_subset | m2-collections |  |
| `LLEN` | list | 1.0.0 | done | behavioral_subset | m2-collections | M2 list core baseline implemented. |
| `LMOVE` | list | 6.2.0 | done | behavioral_subset | m2-collections | M2 set/list extension baseline implemented. |
| `LMPOP` | list | 7.0.0 | done | behavioral_subset | m2-collections | M2 list extension baseline implemented. |
| `LOLWUT` | server | 5.0.0 | done | syntax_only | m0-foundation | M0 informational baseline implemented (static ascii-text response with VERSION option). |
| `LPOP` | list | 1.0.0 | done | behavioral_subset | m2-collections | M2 list core baseline implemented. |
| `LPOS` | list | 6.0.6 | done | behavioral_subset | m2-collections | M2 set/list extension baseline implemented. |
| `LPUSH` | list | 1.0.0 | done | behavioral_subset | m2-collections | M2 list core baseline implemented. |
| `LPUSHX` | list | 2.2.0 | done | behavioral_subset | m2-collections | M2 set/list extension baseline implemented. |
| `LRANGE` | list | 1.0.0 | done | behavioral_subset | m2-collections | M2 list core baseline implemented. |
| `LREM` | list | 1.0.0 | done | behavioral_subset | m2-collections | M2 set/list extension baseline implemented. |
| `LSET` | list | 1.0.0 | done | behavioral_subset | m2-collections | Batch-5 baseline implemented (ordered 1->2 execution). |
| `LTRIM` | list | 1.0.0 | done | behavioral_subset | m2-collections | Batch-5 baseline implemented (ordered 1->2 execution). |
| `MEMORY` | server | 4.0.0 | done | baseline_local | m0-foundation | M0 operational baseline implemented (USAGE/HELP subset). |
| `MEMORY DOCTOR` | server | 4.0.0 | done | behavioral_subset | m0-foundation | Real diagnostics: empty instance, overhead ratio analysis. Redis-style report format. |
| `MEMORY HELP` | server | 4.0.0 | done | baseline_local | m0-foundation | M0 operational baseline implemented. |
| `MEMORY MALLOC-STATS` | server | 4.0.0 | done | behavioral_subset | m0-foundation | mimalloc FFI (`mi_stats_merge` + `mi_stats_print_out`) when `mimalloc` feature enabled. |
| `MEMORY PURGE` | server | 4.0.0 | done | behavioral_subset | m0-foundation | mimalloc `mi_collect(true)` when `mimalloc` feature enabled. OK otherwise. |
| `MEMORY STATS` | server | 4.0.0 | done | baseline_local | m0-foundation | M0 operational baseline implemented. |
| `MEMORY USAGE` | server | 4.0.0 | done | baseline_local | m0-foundation | M0 operational baseline implemented (SAMPLES syntax accepted). |
| `MGET` | string | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `MIGRATE` | generic | 2.6.0 | done | behavioral_subset | m1-kv-core | M1 baseline implemented: standalone-compatible MIGRATE returns NOKEY (no remote transfer). |
| `MODULE` | server | 4.0.0 | done | unsupported | m0-foundation | M0 module baseline implemented (HELP/LIST supported, load/unload return unsupported). |
| `MODULE HELP` | server | 5.0.0 | done | behavioral_subset | m0-foundation | M0 module baseline implemented (help text). |
| `MODULE LIST` | server | 4.0.0 | done | behavioral_subset | m0-foundation | M0 module baseline implemented (returns empty list). |
| `MODULE LOAD` | server | 4.0.0 | done | unsupported | m0-foundation | M0 module baseline implemented (unsupported in this build). |
| `MODULE LOADEX` | server | 7.0.0 | done | unsupported | m0-foundation | M0 module baseline implemented (unsupported in this build). |
| `MODULE UNLOAD` | server | 4.0.0 | done | unsupported | m0-foundation | M0 module baseline implemented (unsupported in this build). |
| `MONITOR` | server | 1.0.0 | done | unsupported | m0-foundation | Returns ERR; MONITOR is not supported in this Ratatosk build. |
| `MOVE` | generic | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `MSET` | string | 1.0.1 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `MSETEX` | string | 8.4.0 | done | behavioral_subset | m1-kv-core | M1 baseline implemented: numkeys KV block + NX/XX + EX/PX shared expiration, atomic all-or-nothing. |
| `MSETNX` | string | 1.0.1 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `MULTI` | transactions | 1.2.0 | done | behavioral_subset | m1-kv-core | M1 transaction baseline implemented. |
| `OBJECT` | generic | 2.2.3 | done | behavioral_subset | m1-kv-core | M1 generic/object baseline implemented (HELP and key introspection subcommands). |
| `OBJECT ENCODING` | generic | 2.2.3 | done | behavioral_subset | m1-kv-core | M1 generic/object baseline implemented. |
| `OBJECT FREQ` | generic | 4.0.0 | done | behavioral_subset | m1-kv-core | M1 generic/object baseline implemented. |
| `OBJECT HELP` | generic | 6.2.0 | done | behavioral_subset | m1-kv-core | M1 generic/object baseline implemented. |
| `OBJECT IDLETIME` | generic | 2.2.3 | done | behavioral_subset | m1-kv-core | M1 generic/object baseline implemented. |
| `OBJECT REFCOUNT` | generic | 2.2.3 | done | behavioral_subset | m1-kv-core | M1 generic/object baseline implemented. |
| `PERSIST` | generic | 2.2.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `PEXPIRE` | generic | 2.6.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `PEXPIREAT` | generic | 2.6.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `PEXPIRETIME` | generic | 7.0.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `PFADD` | hyperloglog | 2.8.9 | done | behavioral_subset | m4-extended-types |  |
| `PFCOUNT` | hyperloglog | 2.8.9 | done | behavioral_subset | m4-extended-types |  |
| `PFDEBUG` | hyperloglog | 2.8.9 | done | behavioral_subset | m4-extended-types |  |
| `PFMERGE` | hyperloglog | 2.8.9 | done | behavioral_subset | m4-extended-types |  |
| `PFSELFTEST` | hyperloglog | 2.8.9 | done | behavioral_subset | m4-extended-types |  |
| `PING` | connection | 1.0.0 | done | behavioral_subset | m0-foundation | M0 implemented and tested. |
| `PSETEX` | string | 2.6.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `PSUBSCRIBE` | pubsub | 2.0.0 | done | behavioral_subset | m3-events | M3 events baseline implemented with pattern fanout and async push delivery. |
| `PSYNC` | server | 2.8.0 | done | unsupported | m0-foundation | Returns ERR; Ratatosk runs in standalone mode. |
| `PTTL` | generic | 2.6.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `PUBLISH` | pubsub | 2.0.0 | done | behavioral_subset | m3-events | M3 events baseline implemented with receiver counting and server-side fanout queue. |
| `PUBSUB` | pubsub | 2.8.0 | done | behavioral_subset | m3-events | M3 pubsub baseline implemented with CHANNELS/NUMSUB/NUMPAT/HELP dispatch. |
| `PUBSUB CHANNELS` | pubsub | 2.8.0 | done | behavioral_subset | m3-events | M3 pubsub baseline implemented (optional glob filter). |
| `PUBSUB HELP` | pubsub | 6.2.0 | done | behavioral_subset | m3-events | M3 pubsub baseline implemented (help text). |
| `PUBSUB NUMPAT` | pubsub | 2.8.0 | done | behavioral_subset | m3-events | M3 pubsub baseline implemented (unique pattern count). |
| `PUBSUB NUMSUB` | pubsub | 2.8.0 | done | behavioral_subset | m3-events | M3 pubsub baseline implemented (channel subscriber count pairs). |
| `PUBSUB SHARDCHANNELS` | pubsub | 7.0.0 | done | behavioral_subset | m3-events | M3 sharded pubsub baseline implemented (active shard channel listing with optional pattern). |
| `PUBSUB SHARDNUMSUB` | pubsub | 7.0.0 | done | behavioral_subset | m3-events | M3 sharded pubsub baseline implemented (per-channel shard subscriber counts). |
| `PUNSUBSCRIBE` | pubsub | 2.0.0 | done | behavioral_subset | m3-events | M3 pubsub baseline implemented: pattern unsubscribe and no-arg unsubscribe-all behavior. |
| `QUIT` | connection | 1.0.0 | done | behavioral_subset | m0-foundation | M0 implemented and tested. |
| `RANDOMKEY` | generic | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `READONLY` | cluster | 3.0.0 | done | syntax_only | m5-advanced |  |
| `READWRITE` | cluster | 3.0.0 | done | syntax_only | m5-advanced |  |
| `RENAME` | generic | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `RENAMENX` | generic | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `REPLCONF` | server | 3.0.0 | done | baseline_local | m0-foundation | M0 replication-control baseline implemented with LISTENING-PORT/CAPA/ACK/GETACK/IP-ADDRESS subset backed by per-client replica metadata. |
| `REPLICAOF` | server | 5.0.0 | done | unsupported | m0-foundation | Returns ERR for replication targets; REPLICAOF NO ONE still accepted for standalone confirmation. |
| `RESET` | connection | 6.2.0 | done | behavioral_subset | m0-foundation | M0 compatibility baseline implemented. |
| `RESTORE` | generic | 2.6.0 | done | behavioral_subset | m1-kv-core | M1 baseline implemented: RESTORE payload import with REPLACE/ABSTTL support and BUSYKEY handling. |
| `RESTORE-ASKING` | server | 3.0.0 | done | behavioral_subset | m0-foundation | M0 replication-control baseline implemented as RESTORE alias behavior. |
| `ROLE` | server | 2.8.12 | done | baseline_local | m0-foundation | M0 replication-control baseline implemented with master/replica role reporting, replica list, and logical replication offsets. |
| `RPOP` | list | 1.0.0 | done | behavioral_subset | m2-collections | M2 list core baseline implemented. |
| `RPOPLPUSH` | list | 1.2.0 | done | behavioral_subset | m2-collections | M2 list extension baseline implemented. |
| `RPUSH` | list | 1.0.0 | done | behavioral_subset | m2-collections | M2 list core baseline implemented. |
| `RPUSHX` | list | 2.2.0 | done | behavioral_subset | m2-collections | M2 set/list extension baseline implemented. |
| `SADD` | set | 1.0.0 | done | behavioral_subset | m2-collections | M2 set core baseline implemented. |
| `SAVE` | server | 1.0.0 | done | behavioral_subset | m0-foundation | M0 compatibility baseline implemented (in-memory no-op save + LASTSAVE timestamp update). |
| `SCAN` | generic | 2.8.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline + m2 scan semantics refined (cursor progression with MATCH/TYPE/COUNT). |
| `SCARD` | set | 1.0.0 | done | behavioral_subset | m2-collections | M2 set core baseline implemented. |
| `SCRIPT` | scripting | 2.6.0 | done | baseline_local | m5-advanced |  |
| `SCRIPT DEBUG` | scripting | 3.2.0 | done | behavioral_subset | m5-advanced |  |
| `SCRIPT EXISTS` | scripting | 2.6.0 | done | behavioral_subset | m5-advanced |  |
| `SCRIPT FLUSH` | scripting | 2.6.0 | done | syntax_only | m5-advanced |  |
| `SCRIPT HELP` | scripting | 5.0.0 | done | syntax_only | m5-advanced |  |
| `SCRIPT KILL` | scripting | 2.6.0 | done | behavioral_subset | m5-advanced |  |
| `SCRIPT LOAD` | scripting | 2.6.0 | done | behavioral_subset | m5-advanced |  |
| `SDIFF` | set | 1.0.0 | done | behavioral_subset | m2-collections | M2 set algebra baseline implemented. |
| `SDIFFSTORE` | set | 1.0.0 | done | behavioral_subset | m2-collections | M2 set algebra baseline implemented. |
| `SELECT` | connection | 1.0.0 | done | behavioral_subset | m0-foundation | M1 baseline implemented for client DB switching. |
| `SENTINEL` | sentinel | 2.8.4 | done | unsupported | m5-advanced |  |
| `SENTINEL CKQUORUM` | sentinel | 2.8.4 | done | unsupported | m5-advanced |  |
| `SENTINEL CONFIG` | sentinel | 6.2.0 | done | unsupported | m5-advanced |  |
| `SENTINEL DEBUG` | sentinel | 7.0.0 | done | unsupported | m5-advanced |  |
| `SENTINEL FAILOVER` | sentinel | 2.8.4 | done | unsupported | m5-advanced |  |
| `SENTINEL FLUSHCONFIG` | sentinel | 2.8.4 | done | unsupported | m5-advanced |  |
| `SENTINEL GET-MASTER-ADDR-BY-NAME` | sentinel | 2.8.4 | done | unsupported | m5-advanced |  |
| `SENTINEL HELP` | sentinel | 6.2.0 | done | syntax_only | m5-advanced |  |
| `SENTINEL INFO-CACHE` | sentinel | 3.2.0 | done | unsupported | m5-advanced |  |
| `SENTINEL IS-MASTER-DOWN-BY-ADDR` | sentinel | 2.8.4 | done | unsupported | m5-advanced |  |
| `SENTINEL MASTER` | sentinel | 2.8.4 | done | unsupported | m5-advanced |  |
| `SENTINEL MASTERS` | sentinel | 2.8.4 | done | unsupported | m5-advanced |  |
| `SENTINEL MONITOR` | sentinel | 2.8.4 | done | unsupported | m5-advanced |  |
| `SENTINEL MYID` | sentinel | 6.2.0 | done | unsupported | m5-advanced |  |
| `SENTINEL PENDING-SCRIPTS` | sentinel | 2.8.4 | done | unsupported | m5-advanced |  |
| `SENTINEL REMOVE` | sentinel | 2.8.4 | done | unsupported | m5-advanced |  |
| `SENTINEL REPLICAS` | sentinel | 5.0.0 | done | unsupported | m5-advanced |  |
| `SENTINEL RESET` | sentinel | 2.8.4 | done | unsupported | m5-advanced |  |
| `SENTINEL SENTINELS` | sentinel | 2.8.4 | done | unsupported | m5-advanced |  |
| `SENTINEL SET` | sentinel | 2.8.4 | done | unsupported | m5-advanced |  |
| `SENTINEL SIMULATE-FAILURE` | sentinel | 3.2.0 | done | unsupported | m5-advanced |  |
| `SENTINEL SLAVES` | sentinel | 2.8.0 | done | unsupported | m5-advanced |  |
| `SET` | string | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented (SET [NX\|XX]). |
| `SETBIT` | bitmap | 2.2.0 | done | behavioral_subset | m4-extended-types |  |
| `SETEX` | string | 2.0.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `SETNX` | string | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `SETRANGE` | string | 2.2.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `SFLUSH` | server | 8.0.0 | done | syntax_only | m0-foundation | M0 admin baseline implemented (SYNC/ASYNC option parse + OK no-op). |
| `SHUTDOWN` | server | 1.0.0 | done | unsupported | m0-foundation | M0 standalone baseline implemented (returns unsupported-in-build error). |
| `SINTER` | set | 1.0.0 | done | behavioral_subset | m2-collections | M2 set algebra baseline implemented. |
| `SINTERCARD` | set | 7.0.0 | done | behavioral_subset | m2-collections | M2 set algebra baseline implemented (numkeys/LIMIT parser + cardinality-only path). |
| `SINTERSTORE` | set | 1.0.0 | done | behavioral_subset | m2-collections | M2 set algebra baseline implemented. |
| `SISMEMBER` | set | 1.0.0 | done | behavioral_subset | m2-collections | M2 set core baseline implemented. |
| `SLAVEOF` | server | 1.0.0 | done | unsupported | m0-foundation | REPLICAOF alias; returns ERR for replication targets, NO ONE still accepted. |
| `SLOWLOG` | server | 2.2.12 | done | behavioral_subset | m0-foundation | M0 operational baseline implemented (GET/LEN/RESET/HELP subset). |
| `SLOWLOG GET` | server | 2.2.12 | done | behavioral_subset | m0-foundation | M0 operational baseline implemented. |
| `SLOWLOG HELP` | server | 6.2.0 | done | behavioral_subset | m0-foundation | M0 operational baseline implemented. |
| `SLOWLOG LEN` | server | 2.2.12 | done | behavioral_subset | m0-foundation | M0 operational baseline implemented. |
| `SLOWLOG RESET` | server | 2.2.12 | done | behavioral_subset | m0-foundation | M0 operational baseline implemented. |
| `SMEMBERS` | set | 1.0.0 | done | behavioral_subset | m2-collections | M2 set core baseline implemented. |
| `SMISMEMBER` | set | 6.2.0 | done | behavioral_subset | m2-collections | M2 set/list extension baseline implemented. |
| `SMOVE` | set | 1.0.0 | done | behavioral_subset | m2-collections | Batch-5 baseline implemented (ordered 1->2 execution). |
| `SORT` | generic | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented (list/set sorting with ASC/DESC, ALPHA, LIMIT, STORE). |
| `SORT_RO` | generic | 7.0.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented (list/set sorting with ASC/DESC, ALPHA, LIMIT). |
| `SPOP` | set | 1.0.0 | done | behavioral_subset | m2-collections | M2 set/list extension baseline implemented. |
| `SPUBLISH` | pubsub | 7.0.0 | done | behavioral_subset | m3-events | M3 sharded pubsub baseline implemented with shard-channel fanout and receiver counting. |
| `SRANDMEMBER` | set | 1.0.0 | done | behavioral_subset | m2-collections | M2 set/list extension baseline implemented. |
| `SREM` | set | 1.0.0 | done | behavioral_subset | m2-collections | M2 set core baseline implemented. |
| `SSCAN` | set | 2.8.0 | done | behavioral_subset | m2-collections | M2 scan family baseline + cursor progression semantics refined. |
| `SSUBSCRIBE` | pubsub | 7.0.0 | done | behavioral_subset | m3-events | M3 sharded pubsub baseline implemented with per-client shard subscriptions and RESP subscribe ack. |
| `STRLEN` | string | 2.2.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `SUBSCRIBE` | pubsub | 2.0.0 | done | behavioral_subset | m3-events | M3 events baseline implemented with cross-client fanout and async push delivery. |
| `SUBSTR` | string | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented as GETRANGE alias. |
| `SUNION` | set | 1.0.0 | done | behavioral_subset | m2-collections | M2 set algebra baseline implemented. |
| `SUNIONSTORE` | set | 1.0.0 | done | behavioral_subset | m2-collections | M2 set algebra baseline implemented. |
| `SUNSUBSCRIBE` | pubsub | 7.0.0 | done | behavioral_subset | m3-events | M3 sharded pubsub baseline implemented with explicit/all unsubscribe behavior and RESP unsubscribe ack. |
| `SWAPDB` | server | 4.0.0 | done | behavioral_subset | m0-foundation | M0 admin baseline implemented (DB payload swap with range checks). |
| `SYNC` | server | 1.0.0 | done | unsupported | m0-foundation | M0 replication-control baseline implemented (standalone unsupported error baseline). |
| `TIME` | server | 2.6.0 | done | behavioral_subset | m0-foundation | M0 compatibility baseline implemented. |
| `TOUCH` | generic | 3.2.1 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `TRIMSLOTS` | server | 8.4.0 | done | syntax_only | m0-foundation | M0 admin baseline implemented (integer argument parse + no-op 0 result). |
| `TTL` | generic | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `TYPE` | generic | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `UNLINK` | generic | 4.0.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented (synchronous fallback). |
| `UNSUBSCRIBE` | pubsub | 2.0.0 | done | behavioral_subset | m3-events | M3 pubsub baseline implemented: unsubscribe channel/all with remaining-subscription counts. |
| `UNWATCH` | transactions | 2.2.0 | done | behavioral_subset | m1-kv-core | M1 transaction baseline implemented. |
| `WAIT` | generic | 3.0.0 | done | syntax_only | m1-kv-core | M1 baseline implemented: immediate standalone WAIT result from tracked replica ACK offsets without blocking timeout semantics. |
| `WAITAOF` | generic | 7.2.0 | done | baseline_local | m1-kv-core | M1 baseline implemented: immediate standalone WAITAOF vector from local AOF health and tracked replica ACK offsets. |
| `WATCH` | transactions | 2.2.0 | done | behavioral_subset | m1-kv-core | M1 transaction baseline implemented with key-version tracking. |
| `XACK` | stream | 5.0.0 | done | behavioral_subset | m3-events | M3 stream group baseline implemented. |
| `XACKDEL` | stream | 8.2.0 | done | behavioral_subset | m3-events | Batch-6 stream extended deletion/config baseline implemented. |
| `XADD` | stream | 5.0.0 | done | behavioral_subset | m3-events | M3 stream core baseline implemented (auto-ID and explicit ID checks, field/value append). |
| `XAUTOCLAIM` | stream | 6.2.0 | done | behavioral_subset | m3-events | Batch-5 baseline implemented (ordered 1->2 execution). |
| `XCFGSET` | stream | 8.6.0 | done | behavioral_subset | m3-events | Batch-6 stream extended deletion/config baseline implemented. |
| `XCLAIM` | stream | 5.0.0 | done | behavioral_subset | m3-events | Batch-5 baseline implemented (ordered 1->2 execution). |
| `XDEL` | stream | 5.0.0 | done | behavioral_subset | m3-events | Batch-5 baseline implemented (ordered 1->2 execution). |
| `XDELEX` | stream | 8.2.0 | done | behavioral_subset | m3-events | Batch-6 stream extended deletion/config baseline implemented. |
| `XGROUP` | stream | 5.0.0 | done | behavioral_subset | m3-events | M3 stream group baseline implemented (CREATE/DESTROY/SETID/CREATECONSUMER/DELCONSUMER/HELP dispatch). |
| `XGROUP CREATE` | stream | 5.0.0 | done | behavioral_subset | m3-events | M3 stream group baseline implemented (MKSTREAM + BUSYGROUP + id/$ support). |
| `XGROUP CREATECONSUMER` | stream | 6.2.0 | done | behavioral_subset | m3-events | M3 stream group baseline implemented. |
| `XGROUP DELCONSUMER` | stream | 5.0.0 | done | behavioral_subset | m3-events | M3 stream group baseline implemented (consumer pending removal count). |
| `XGROUP DESTROY` | stream | 5.0.0 | done | behavioral_subset | m3-events | M3 stream group baseline implemented. |
| `XGROUP HELP` | stream | 5.0.0 | done | behavioral_subset | m3-events | M3 stream group baseline implemented. |
| `XGROUP SETID` | stream | 5.0.0 | done | behavioral_subset | m3-events | M3 stream group baseline implemented. |
| `XINFO` | stream | 5.0.0 | done | behavioral_subset | m3-events | Batch-5 baseline implemented (ordered 1->2 execution). |
| `XINFO CONSUMERS` | stream | 5.0.0 | done | behavioral_subset | m3-events | Batch-5 baseline implemented (ordered 1->2 execution). |
| `XINFO GROUPS` | stream | 5.0.0 | done | behavioral_subset | m3-events | Batch-5 baseline implemented (ordered 1->2 execution). |
| `XINFO HELP` | stream | 5.0.0 | done | behavioral_subset | m3-events | Batch-5 baseline implemented (ordered 1->2 execution). |
| `XINFO STREAM` | stream | 5.0.0 | done | behavioral_subset | m3-events | Batch-5 baseline implemented (ordered 1->2 execution). |
| `XLEN` | stream | 5.0.0 | done | behavioral_subset | m3-events | M3 stream core baseline implemented. |
| `XPENDING` | stream | 5.0.0 | done | behavioral_subset | m3-events | M3 stream group baseline implemented (summary + range forms). |
| `XRANGE` | stream | 5.0.0 | done | behavioral_subset | m3-events | M3 stream core baseline implemented (inclusive range + COUNT option). |
| `XREAD` | stream | 5.0.0 | done | behavioral_subset | m3-events | M3 stream core baseline implemented (COUNT/BLOCK parse + STREAMS read, blocked wait registry + producer wakeup with timeout fallback). |
| `XREADGROUP` | stream | 5.0.0 | done | behavioral_subset | m3-events | M3 stream group baseline implemented (GROUP/COUNT/BLOCK/NOACK parse + STREAMS read path with blocked wait registry + producer wakeup fallback). |
| `XREVRANGE` | stream | 5.0.0 | done | behavioral_subset | m3-events | M3 stream core baseline implemented (reverse inclusive range + COUNT option). |
| `XSETID` | stream | 5.0.0 | done | behavioral_subset | m3-events | Batch-5 baseline implemented (ordered 1->2 execution). |
| `XTRIM` | stream | 5.0.0 | done | behavioral_subset | m3-events | Batch-5 baseline implemented (ordered 1->2 execution). |
| `ZADD` | sorted_set | 1.2.0 | done | behavioral_subset | m2-collections |  |
| `ZCARD` | sorted_set | 1.2.0 | done | behavioral_subset | m2-collections |  |
| `ZCOUNT` | sorted_set | 2.0.0 | done | behavioral_subset | m2-collections |  |
| `ZDIFF` | sorted_set | 6.2.0 | done | behavioral_subset | m2-collections |  |
| `ZDIFFSTORE` | sorted_set | 6.2.0 | done | behavioral_subset | m2-collections |  |
| `ZINCRBY` | sorted_set | 1.2.0 | done | behavioral_subset | m2-collections |  |
| `ZINTER` | sorted_set | 6.2.0 | done | behavioral_subset | m2-collections |  |
| `ZINTERCARD` | sorted_set | 7.0.0 | done | behavioral_subset | m2-collections |  |
| `ZINTERSTORE` | sorted_set | 2.0.0 | done | behavioral_subset | m2-collections |  |
| `ZLEXCOUNT` | sorted_set | 2.8.9 | done | behavioral_subset | m2-collections |  |
| `ZMPOP` | sorted_set | 7.0.0 | done | behavioral_subset | m2-collections |  |
| `ZMSCORE` | sorted_set | 6.2.0 | done | behavioral_subset | m2-collections |  |
| `ZPOPMAX` | sorted_set | 5.0.0 | done | behavioral_subset | m2-collections |  |
| `ZPOPMIN` | sorted_set | 5.0.0 | done | behavioral_subset | m2-collections |  |
| `ZRANDMEMBER` | sorted_set | 6.2.0 | done | behavioral_subset | m2-collections |  |
| `ZRANGE` | sorted_set | 1.2.0 | done | behavioral_subset | m2-collections |  |
| `ZRANGEBYLEX` | sorted_set | 2.8.9 | done | behavioral_subset | m2-collections |  |
| `ZRANGEBYSCORE` | sorted_set | 1.0.5 | done | behavioral_subset | m2-collections |  |
| `ZRANGESTORE` | sorted_set | 6.2.0 | done | behavioral_subset | m2-collections |  |
| `ZRANK` | sorted_set | 2.0.0 | done | behavioral_subset | m2-collections |  |
| `ZREM` | sorted_set | 1.2.0 | done | behavioral_subset | m2-collections |  |
| `ZREMRANGEBYLEX` | sorted_set | 2.8.9 | done | behavioral_subset | m2-collections |  |
| `ZREMRANGEBYRANK` | sorted_set | 2.0.0 | done | behavioral_subset | m2-collections |  |
| `ZREMRANGEBYSCORE` | sorted_set | 1.2.0 | done | behavioral_subset | m2-collections |  |
| `ZREVRANGE` | sorted_set | 1.2.0 | done | behavioral_subset | m2-collections |  |
| `ZREVRANGEBYLEX` | sorted_set | 2.8.9 | done | behavioral_subset | m2-collections |  |
| `ZREVRANGEBYSCORE` | sorted_set | 2.2.0 | done | behavioral_subset | m2-collections |  |
| `ZREVRANK` | sorted_set | 2.0.0 | done | behavioral_subset | m2-collections |  |
| `ZSCAN` | sorted_set | 2.8.0 | done | behavioral_subset | m2-collections |  |
| `ZSCORE` | sorted_set | 1.2.0 | done | behavioral_subset | m2-collections |  |
| `ZUNION` | sorted_set | 6.2.0 | done | behavioral_subset | m2-collections |  |
| `ZUNIONSTORE` | sorted_set | 2.0.0 | done | behavioral_subset | m2-collections |  |
