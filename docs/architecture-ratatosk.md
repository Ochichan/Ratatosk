# Ratatosk Architecture

이 문서는 **현재 저장소 코드 기준**(2026-03-16) 아키텍처를 설명한다.
과거 계획 문서가 아니라 실제 구현 상태를 기준으로 작성했다.

## Snapshot

- Workspace crates: `ratatosk-core`, `ratatosk-resp`, `ratatosk-engine`, `ratatosk-persist`, `ratatosk-server`
- Runtime model: tokio TCP accept loop + per-client task + server_cron timer
- Shared state: `SharedState` wrapping `Mutex<ServerState>` + per-DB `parking_lot::RwLock` + lock-free components
- Protocol: RESP2/RESP3 호환 파싱 경로
- Command catalog: `420` entries. 구현 상태와 Redis 의미론 tier는 `docs/redis-gap-ledger.json` / `docs/redis-gap-ledger.md`를 함께 봐야 한다.
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
  DB 접근은 내부 per-DB `parking_lot::RwLock`을 통해 이루어지므로, 서로 다른 DB에 대한 명령은 잠재적으로 병렬 실행 가능하다.
  stats/config 읽기/client id 할당은 lock-free.
- `CommandOutcome { response, close, retry_blocking }`로 결과를 통일.

### 5) Blocking command retry

- 블로킹 명령은 `retry_blocking`으로 재시도 지시를 반환.
- 서버 루프는 lock을 놓은 뒤 sleep/backoff(10ms → 200ms) 후 재실행.
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
| `atomic_stats` | `AtomicStatsState` | 10개 lock-free atomic counter (total_commands_processed, connected_clients, net_input/output_bytes, evicted/expired_keys, keyspace_hits/misses, ops_per_sec, cached_memory_estimate) |
| `config_cache` | `arc_swap::ArcSwap<ConfigState>` | lock-free config 읽기 (`config_cache.load()`) |
| `next_client_id` | `AtomicU64` | lock 없이 새 client ID 할당 |

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

`DataState`는 DB별 `parking_lot::RwLock<DbShard>`를 관리한다:

```rust
pub struct DataState {
    shards: Vec<parking_lot::RwLock<DbShard>>,
    next_key_version: AtomicU64,
}

pub struct DbShard {
    pub data: HashMap<Bytes, StoredValue>,
    pub key_versions: HashMap<Bytes, u64>,
}
```

- `db(idx)` → `MappedRwLockReadGuard<HashMap>` (auto-deref로 `&HashMap`처럼 사용)
- `db_mut(idx)` → `MappedRwLockWriteGuard<HashMap>` (auto-deref로 `&mut HashMap`처럼 사용)
- `write_two_dbs(a, b)` — ascending index 순서로 2개 DB 동시 write lock (MOVE, SWAPDB용)
- `write_all_dbs()` — 전체 DB ascending lock (FLUSHALL, load_from_rdb용)
- `snapshot_all()` — DB별 순차 read-lock + clone (BGSAVE용)

기본 DB 개수는 16(`DEFAULT_DB_COUNT`).

Lock ordering invariant: **항상 ascending index 순서로 DB lock 획득** → deadlock 방지.

### StoredValue

```rust
pub struct StoredValue {
    pub data: ValueData,
    pub expire_at_ms: Option<i64>,
    pub encoding: Encoding,   // Raw, Int, QuickList, HashTable, SkipList, StreamTree
    pub lru_clock: u32,       // 24-bit LRU seconds 또는 LFU counter
}
```

### ValueData

- `String(Bytes)`
- `Hash(HashMap<Bytes, HashFieldEntry>)`
- `List(VecDeque<Bytes>)`
- `Set(HashSet<Bytes>)`
- `SortedSet { by_score: BTreeMap, by_member: HashMap }`
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

## Eviction System

`crates/ratatosk-engine/src/eviction.rs`:

8가지 Redis maxmemory 정책을 샘플링 기반으로 구현:

| Policy | 대상 | 기준 |
|--------|------|------|
| `noeviction` | — | eviction 안 함 (OOM 에러) |
| `allkeys-lru` | 모든 키 | LRU clock 기반 유휴 시간 |
| `volatile-lru` | TTL 있는 키만 | LRU clock 기반 유휴 시간 |
| `allkeys-lfu` | 모든 키 | LFU 접근 빈도 (역순) |
| `volatile-lfu` | TTL 있는 키만 | LFU 접근 빈도 (역순) |
| `allkeys-random` | 모든 키 | 무작위 |
| `volatile-random` | TTL 있는 키만 | 무작위 |
| `volatile-ttl` | TTL 있는 키만 | 가장 가까운 만료 시간 |

LRU clock: 24-bit wrapping seconds (`(now_sec / 1) & 0xFFFFFF`).

메모리 추정: `estimate_object_memory()`가 키/값/HashMap entry 오버헤드를 합산.

## Lazy Free

대용량 키 삭제 시 메인 이벤트 루프 블로킹을 방지한다.

- `crossbeam_channel::bounded(4096)` 채널로 `StoredValue`를 백그라운드 스레드에 전송.
- 백그라운드 스레드가 `drop(value)` 수행 → 메모리 해제.
- Threshold: 컬렉션 크기 64개 이상(`LAZY_FREE_THRESHOLD`)일 때만 lazy free, 이하는 동기 drop.
- `UNLINK`, `FLUSHDB ASYNC` 등에서 활용.

## Keyspace Notifications

`crates/ratatosk-engine/src/notification.rs`:

Redis 호환 keyspace notification 시스템. `CONFIG SET notify-keyspace-events` 설정에 따라:

- `__keyspace@<db>__:<key>` → 이벤트 이름 publish
- `__keyevent@<db>__:<event_name>` → 키 이름 publish

설정 문자열 플래그: `K`(keyspace), `E`(keyevent), `g`(generic), `$`(string), `l`(list), `s`(set), `h`(hash), `z`(sorted set), `x`(expired), `e`(evicted), `t`(stream), `A`(all).

`notify!` 매크로로 명령 핸들러에서 간결하게 호출 가능.

## Persistence

`crates/ratatosk-persist/`:

### RDB (스냅샷)

- Redis 호환 RDB 형식 (magic `REDIS0012`, version 12).
- `RdbSaver`: `ServerState` → binary file. 6가지 데이터 타입 직렬화.
- `RdbLoader`: binary file → `ServerState`. CRC64 검증 + 손상 감지.
- Atomic write: `tempfile → fsync → rename → parent dir fsync`.
- CRC64: ECMA-182 호환 체크섬.

### AOF (Append-Only File)

- `AofWriter`: RESP 형식으로 명령 append. DB 변경 시 자동 `SELECT` 삽입.
- `FsyncPolicy`: `Always` / `EverySec` / `No`.
- `AofManifest`: BASE + INCR 파일 목록 관리 (Redis 7+ 호환).
- `AofRecovery`: AOF 파일을 `ratatosk_resp::parse()` → `execute()`로 재생.
  - 절단된 파일은 graceful하게 처리 (마지막 완전한 명령까지 복구).

자세한 내용: [`docs/persistence.md`](persistence.md)

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
  - **서로 다른 DB**에 대한 명령은 잠재적으로 병렬 실행 가능 (per-DB RwLock).
  - **같은 DB** 내 읽기 명령은 동시 실행 가능 (RwLock read sharing).
  - 쓰기 명령은 해당 DB의 write lock을 획득.
- **lock-free 경로** (Mutex 불필요):
  - `AtomicStatsState`: 10개 atomic counter (total_commands_processed, connected_clients 등) — 매 요청마다 lock 없이 갱신.
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

`crates/ratatosk-engine/src/command/cmd_acl.rs`, `crates/ratatosk-server/src/client.rs`:

- **progressive delay**: 실패 횟수에 따라 응답 전 지수 지연을 적용한다. `delay_ms = min(100 × 2^(failures-1), 2000) × jitter(0.8..1.2)`. `tokio::time::sleep`으로 비동기 대기하므로 OS 스레드를 차단하지 않는다.
- per-connection: 5회 연속 AUTH 실패 시 지연 후 연결을 종료한다 (`CommandOutcome::close_with_delay`).
- per-IP: `AuthRateLimiter`가 IP별 실패를 추적하며, 60초 윈도우 내 20회 실패 시 해당 IP의 신규 AUTH 시도를 거부한다.

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

- `docs/redis-gap-ledger.json` 기준 명령 카탈로그는 420개 엔트리이며, 현재 ledger는 status와 별도로 `capability_tier`를 기록한다.
- 현재 tier summary는 `unsupported=64`, `syntax_only=30`, `baseline_local=45`, `behavioral_subset=281`, `distributed_parity=0`이다.
- 일부 서버/복제/운영 명령은 "standalone baseline semantics"(ack/no-op 포함)으로 구현되어 있다.
  - 예: `SAVE`/`BGSAVE`는 현재 통계 timestamp 갱신 중심.
  - 예: 복제/클러스터 계열은 standalone 호환 응답 중심.
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

`CONFIG SET`을 통한 런타임 설정 변경:
- `maxmemory`, `maxmemory-policy`, `maxmemory-samples`
- `hz`, `notify-keyspace-events`, `tcp-keepalive`
- `lazyfree-lazy-expire`, `lazyfree-lazy-server-del`, `lazyfree-lazy-user-del`
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
