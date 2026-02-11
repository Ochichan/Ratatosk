# Ratatosk Architecture

이 문서는 **현재 저장소 코드 기준**(2026-02-11) 아키텍처를 설명한다.
과거 계획 문서가 아니라 실제 구현 상태를 기준으로 작성했다.

## Snapshot

- Workspace crates: `ratatosk-core`, `ratatosk-resp`, `ratatosk-engine`, `ratatosk-persist`, `ratatosk-server`
- Runtime model: tokio TCP accept loop + per-client task + server_cron timer
- Shared state: `Arc<tokio::sync::Mutex<ServerState>>`
- Protocol: RESP2/RESP3 호환 파싱 경로
- Command coverage: `420 / 420 done` (source: `docs/redis-gap-ledger.json`)
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
| `ratatosk-engine` | Keyspace, 420개 명령 핸들러, eviction, active expiry, pub/sub, notification, HLL, 슬롯 | `forbid` |
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
- `execute(frame, &mut server, &mut client_state)` 호출 시점에 Mutex를 잠그므로
  state mutation은 사실상 직렬화된다.
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

`crates/ratatosk-engine/src/keyspace.rs`의 `ServerState`:

- `dbs: Vec<HashMap<Bytes, StoredValue>>`
- `key_versions` (WATCH/transaction versioning)
- `pubsub: PubSubState`
- `stats: StatsState`
- `acl: AclState`
- `config: ConfigState`
- `script_cache: ScriptCache`
- `cluster_node_id`
- `lazy_free_tx: Option<LazyFreeSender>` — 백그라운드 삭제 채널

기본 DB 개수는 16(`DEFAULT_DB_COUNT`).

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
- **SIGUSR1** → RDB save 트리거 (로그 출력, persistence 연동 예정)

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
- client별 pending queue를 운영하며 limit 초과 시 overflow 플래그 설정.
- pending queue limit: 4096 (`PUBSUB_PENDING_QUEUE_LIMIT`).
- overflow client는 다음 처리 루프에서 에러 응답 후 연결 종료.

## Concurrency Model

현재 모델은 "single event loop only"가 아니라 아래와 같다.

- 네트워크는 tokio per-client task로 동시 처리.
- command execution은 공유 `Mutex<ServerState>`로 직렬화.
- 즉, I/O 병렬성은 있지만 state mutation은 단일 임계구역 기반.

### Background Threads

| Thread | 역할 | 통신 |
|--------|------|------|
| Lazy-free | 대용량 값 비동기 삭제 | `crossbeam_channel::bounded(4096)` |
| (미래) RDB save | Background snapshot | `AtomicBool` shutdown flag |
| (미래) AOF rewrite | AOF 재작성 | — |

참고: `crates/ratatosk-server/src/io_thread.rs`의 `IoThreadPool`은 현재 placeholder다.

## Security / Safety Controls

### Config guardrails

`crates/ratatosk-server/src/config.rs`:

- 기본 bind는 loopback(`127.0.0.1`).
- non-loopback bind는 `RATATOSK_ALLOW_INSECURE_BIND=true` 없으면 거부.
- max clients/output buffer/shutdown grace에 대해 0값 방어 검증.

### Sanitization helpers

`crates/ratatosk-engine/src/security.rs`:

- 에러/ACL/slowlog 문자열 sanitization + 256자 제한.
- token prefix redaction (`ghp_`, `sk-`, `npm_`, `xox`, `AKIA`).
- 20자 이상 영숫자 토큰 redaction.
- shell metacharacter blocklist 기반 입력 거부 helper 제공.

## Compatibility Notes

- `docs/redis-gap-ledger.json` 기준 명령 커버리지는 420/420 `done`.
- 단, 일부 서버/복제/운영 명령은 "standalone baseline semantics"(ack/no-op 포함)으로 구현되어 있다.
  - 예: `SAVE`/`BGSAVE`는 현재 통계 timestamp 갱신 중심.
  - 예: 복제/클러스터 계열은 standalone 호환 응답 중심.

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
- Config: `crates/ratatosk-server/src/config.rs`
