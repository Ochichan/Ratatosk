# Ratatosk

Redis를 Rust로 재해석한 인메모리 데이터 스토어. Zero-Copy, Pub/Sub, Nervous System — 이 세 축이 프로젝트의 정체성이다.

## Build & Run

```bash
nix develop                              # Nix dev shell
cargo build --release                    # Full build
cargo build --release -p ratatosk-server     # Single crate
cargo test                               # All tests
cargo test -p ratatosk-engine                # Crate-specific
cargo bench                              # Benchmarks (RESP parsing, data structures)
```
## Gap Ledger

```bash
# 1) Redis command catalog snapshot 갱신
python3 scripts/redis_gap_ledger.py snapshot --redis-dir "<path-to-redis-repo>"

# 2) Ledger JSON + Markdown 동기화
python3 scripts/redis_gap_ledger.py sync

# 3) CI와 동일한 정합성 검사
python3 scripts/redis_gap_ledger.py check
```


## Project Structure

```
crates/
├── ratatosk-core/       # Domain types, errors, Key, Value, ClientId, DbIndex
├── ratatosk-resp/       # RESP3 zero-copy parser/serializer (bytes::Bytes 기반)
├── ratatosk-engine/     # Keyspace, data structures, expiry, eviction, command dispatch
├── ratatosk-pubsub/     # Pub/Sub channels, pattern matching, keyspace notifications
├── ratatosk-persist/    # AOF + RDB persistence, background save, recovery
├── ratatosk-server/     # Event loop (nervous system), networking, client management
└── ratatosk-cluster/    # Cluster support (미래)
```

**Dependency direction**: `ratatosk-server → {ratatosk-engine, ratatosk-pubsub, ratatosk-persist} → ratatosk-resp → ratatosk-core`. Core와 resp crate은 상위 crate을 절대 import하지 않는다.

## Nix

프로젝트 루트에 `flake.nix` 필수. `nix develop`으로 Rust toolchain, pkg-config 등 개발 환경 일괄 세팅. `nix build`로 릴리스 빌드.

---

## 1. Identity — 세 가지 축

코드를 쓰기 **전에** 이 정체성을 먼저 이해한다.

### Zero-Copy

데이터 경로에서 불필요한 복사를 제거한다. 이것은 성능 최적화가 아니라 **설계 철학**이다.

- **RESP 파싱**: `bytes::Bytes` 기반 zero-copy 파서. 원본 버퍼의 slice를 참조하여 파싱 결과를 표현. 클라이언트 요청을 파싱할 때 새 `String`/`Vec<u8>` 할당 금지.
- **응답 직렬화**: scatter/gather I/O (`writev`). 여러 버퍼를 하나로 합치지 않고 커널에 직접 전달.
- **Value 저장**: `Bytes` 또는 inline small-string optimization. 작은 값은 스택에, 큰 값은 reference-counted shared buffer.
- **클라이언트 간 공유**: 동일 value를 여러 클라이언트에 전송할 때 `Bytes::clone()` (RC bump only, 데이터 복사 없음).
- **파일 I/O**: RDB 로딩 시 mmap 가능. AOF는 direct I/O 또는 buffered append.

### Pub/Sub

Pub/Sub은 볼트온 기능이 아니라 **1급 아키텍처 요소**다.

- **내부 이벤트 버스**: keyspace mutation, expiry, eviction 이벤트가 내부 pub/sub 채널을 통해 전파.
- **Keyspace Notifications**: `__keyevent@<db>__:*`, `__keyspace@<db>__:*` 패턴으로 key 변경 사항 구독.
- **채널 타입**: Global channels (SUBSCRIBE), Pattern channels (PSUBSCRIBE), Shard channels (SSUBSCRIBE — 클러스터 대비).
- **Backpressure**: subscriber output buffer에 hard/soft limit 적용. 느린 subscriber는 연결 해제.
- **확장점**: 새로운 이벤트 소스를 추가할 때 pub/sub 채널로 자연스럽게 통합.

### Nervous System

이벤트 루프는 이 시스템의 **신경계**다. 모든 것이 이벤트에 반응한다.

- **Single-threaded core**: 메인 이벤트 루프는 단일 스레드. 락 경합 없이 모든 명령을 처리.
- **I/O 스레드 풀**: 읽기/쓰기 I/O는 별도 스레드 풀에서 병렬 처리. 메인 루프는 명령 실행에 집중.
- **Background 스레드**: persistence (fsync, RDB save), lazy free (대용량 키 삭제), AOF rewrite는 별도 스레드.
- **Cron heartbeat**: 주기적 하우스키핑 (만료 키 정리, 메모리 통계, persistence 트리거) — `server_cron` 타이머.
- **beforeSleep / afterSleep**: 이벤트 루프 sleep 전후 훅. 배치 처리, flush 등.
- **반응성**: 모든 처리는 이벤트 기반. 폴링이나 busy-wait 금지.

---

## 2. Architecture Principles

### Dependency Direction
- **leaf → core only.** Core/data crate은 상위 orchestration crate을 import하지 않는다.
- 순환 의존성은 leaf crate에 trait 정의, 상위에서 impl로 해소.

### Module Design
- 단일 파일 800 LOC, 단일 모듈 2000 LOC 초과 시 분리.
- God struct 금지 — config는 서브시스템별로 분리하고 각 모듈은 자기 slice만 받는다.
- `pub` 최소화 — 외부 소비자가 없는 타입/함수/필드는 `pub(crate)` 또는 비공개.
- 모듈 depth 4 이하. 깊은 nesting은 발견성을 죽인다.

### Type Safety
- **Newtype wrapper**: 도메인 ID에 raw `u64`/`String` 금지.
  - `ClientId(u64)`, `DbIndex(u16)`, `SlotId(u16)`, `CommandFlags(u64)`, `AclCategory(u64)`.
- **Boolean 파라미터 금지**: `fn get(key: &Key, readonly: bool)` → `enum AccessMode { ReadOnly, ReadWrite }`.
- **Typestate**: 클라이언트 상태 `Connected → Authenticated → Subscribed`. 트랜잭션 `Idle → Multi → Exec/Discard`.
- **Exhaustive match**: enum에 `_ =>` wildcard 사용 금지. 새 variant 추가 시 컴파일러가 잡아야 한다.

### Error Design
- crate 공개 API에서 `anyhow::Result` 반환 금지 → 도메인별 `thiserror` enum 필수.
- 내부 구현에서는 anyhow 사용 가능하되, 공개 경계에서 변환.
- 모든 에러 전파에 `.with_context(|| format!("..."))` 필수 — bare 에러 메시지 금지.
- 에러 로그에 context 포함: `client_id`, `db_index`, `key`, `command`, `duration_ms` 등.

### Extensibility
- Match arms > 4 and growing → trait object 또는 registry pattern.
- Feature flag는 boundary/entry point에서만.
- Platform code는 platform trait 뒤에 — 비즈니스 로직에 `#[cfg(unix)]` 금지.

---

## 3. Domain Design — Redis Engine

### Data Types

| 타입 | Rust 표현 | Redis 인코딩 | 용도 |
|------|-----------|-------------|------|
| String | `Bytes` (zero-copy) | raw / int-encoded | GET, SET, INCR 등 |
| List | `VecDeque<Bytes>` / `Listpack` | quicklist / listpack | LPUSH, RPOP, LRANGE |
| Set | `HashSet<Bytes>` / `IntSet` | hashtable / intset / listpack | SADD, SMEMBERS, SINTER |
| Hash | `HashMap<Bytes, Bytes>` / `Listpack` | hashtable / listpack | HSET, HGET, HGETALL |
| Sorted Set | `BTreeMap` + `HashMap` | skiplist+hashtable / listpack | ZADD, ZRANGE, ZRANGEBYSCORE |
| Stream | `RadixTree<StreamEntry>` | rax tree | XADD, XREAD, XRANGE |

- 각 타입은 **인코딩 전환** 로직 포함: 요소 수/크기 threshold 초과 시 compact → full encoding 전환.
- Compact encoding (listpack, intset)은 작은 데이터에 메모리 효율적. Threshold는 config.
- 인코딩 전환은 **한 방향** (compact → full). 역방향 전환 없음.

### RESP Protocol (ratatosk-resp)

RESP3 기반, RESP2 하위 호환.

```
Simple String:  +OK\r\n
Error:          -ERR message\r\n
Integer:        :1000\r\n
Bulk String:    $5\r\nhello\r\n
Array:          *2\r\n$5\r\nhello\r\n$5\r\nworld\r\n
Null:           _\r\n
Map:            %2\r\n...  (RESP3)
Push:           >3\r\n...  (RESP3, Pub/Sub용)
```

- 파서는 **incremental**: 불완전한 입력에서 `Incomplete` 반환, 다음 데이터 도착 시 이어서 파싱.
- Zero-copy: `Bytes::slice()` 로 원본 버퍼 참조. 파싱 결과에 새 할당 없음.
- 직렬화: `IoSlice` 배열 생성 → `writev` 시스템콜로 zero-copy 전송.

### Keyspace (ratatosk-engine)

- **DB 배열**: 기본 16개 DB (`SELECT 0`~`SELECT 15`), config으로 변경 가능.
- **Key → Value 매핑**: `HashMap<Key, Object>`. Key는 `Bytes`, Object는 타입 + 인코딩 + 데이터 + TTL.
- **Expiry**: 별도 자료구조 (min-heap 또는 radix tree)로 관리. `server_cron`에서 주기적 lazy expiry + 접근 시 active expiry.
- **Eviction**: `noeviction`, `allkeys-lru`, `volatile-lru`, `allkeys-random`, `volatile-random`, `volatile-ttl`, `allkeys-lfu`, `volatile-lfu`.
- **LRU/LFU clock**: Object 메타데이터에 마지막 접근 시간 (LRU) 또는 접근 빈도 (LFU) 기록. 샘플링 기반 근사.

### Command Dispatch

- **Command table**: static registry. 각 명령에 이름, 핸들러 함수, flags (read/write/admin/pubsub/blocking 등), arity, key-spec.
- **ACL**: category 기반 권한 (keyspace, read, write, set, sortedset, list, hash, string, pubsub, admin 등).
- **핸들러 시그니처**: `fn(client: &mut Client, args: &[Bytes]) -> Result<(), CommandError>`.
- **MULTI/EXEC**: 트랜잭션 큐에 명령 적립 → EXEC 시 일괄 실행. WATCH로 optimistic locking.

### Persistence (ratatosk-persist)

**RDB (스냅샷)**:
- Point-in-time binary dump. Background fork (`fork()`) 또는 background thread.
- LZ4 압축 (LZF 대안). CRC64 체크섬.
- 파일 형식: `[Magic] [Version] [DB-selector] [Key-Value pairs...] [EOF] [CRC64]`.

**AOF (Append-Only File)**:
- Manifest 기반: BASE file + INCR files.
- `fsync` 정책: `always`, `everysec` (기본), `no`.
- AOF rewrite: background에서 현재 keyspace를 새 AOF로 덤프 + rewrite 동안의 diff를 INCR 파일에 기록.
- Recovery: RDB → AOF replay 순서.

**Data Integrity**:
- CRC64 검증: RDB 파일 전체, AOF entry별.
- Atomic write: `tmp file → fsync → rename`. 부모 디렉토리도 fsync (Unix).

### Networking (ratatosk-server)

- **Event loop**: `mio` 기반 (또는 `tokio` single-threaded runtime). epoll/kqueue 백엔드.
- **Client**: 각 연결 = `Client` 구조체. 읽기 버퍼 (`BytesMut`), 쓰기 버퍼 (응답 큐), 상태 (db index, auth, flags, subscriptions).
- **I/O 스레드**: 읽기 스레드가 RESP 파싱까지 수행 → 메인 루프에 파싱된 명령 전달 → 메인 루프가 실행 → 쓰기 스레드가 응답 전송.
- **Output buffer limit**: 클라이언트별 hard/soft limit. 초과 시 연결 해제.
- **Inline command**: RESP 외에 `PING\r\n` 같은 인라인 명령도 지원 (telnet 호환).

---

## 4. Rust Coding Rules

### Absolute Rules
- **No-Panic**: 런타임 코드에서 `unwrap()`, `expect()` 금지. 에러는 명시적 처리.
- **Zero-Allocation in loops**: 루프 내부 `Vec::new()`, `Box::new()`, `format!()` 금지. 사전 할당.
- **Data-Oriented**: SoA 또는 flat `Vec<T>` 선호. 포인터 체이싱 지양.
- **Unsafe**: SIMD/FFI/mmap 전용. compile-time assertion 필수.
- **코멘트**: "What" 아닌 "Why" (수학/물리학적 이유).
- **Dead code**: 사용 안 되면 삭제하거나 `#[allow(dead_code)]` 명시.
- **들여쓰기 일관성**: 클로저/블록 내부 들여쓰기 어긋나면 수정.

---

## 5. Test Quality

### Required
- **Assertion strength**: `is_ok()` / `is_some()`만으로 끝내지 말 것. 실제 값 검증.
- **Error path tests**: `Result` 반환 함수는 성공 + 최소 1개 실패 테스트.
- **Boundary values**: 빈 입력, 단일 요소, 정확한 limit, limit+1, 최대값.
- **Roundtrip tests**: RESP encode → decode == 원본. RDB save → load == 원본.
- **Determinism**: HashMap 순서, 시스템 시간, 랜덤 시드 의존 금지.

### Redis-Specific
- **RESP 파싱 정확도**: 모든 RESP 타입에 대해 known-answer 테스트.
- **Incremental 파싱**: 불완전한 입력 → `Incomplete`, 바이트 하나씩 추가 → 최종 완전한 파싱 결과.
- **Zero-copy 검증**: 파싱 결과의 `Bytes`가 원본 버퍼를 reference하는지 포인터 비교로 확인.
- **Data type encoding 전환**: threshold 전후로 compact ↔ full encoding 전환 + 데이터 정합성.
- **Expiry 정확도**: TTL 설정 → 만료 전 존재 확인 → 만료 후 부재 확인 (시간 mock 사용).
- **Persistence roundtrip**: RDB dump → reload → 모든 key-value 동일. AOF replay → 동일 상태.
- **Pub/Sub 전달 보장**: publish → 모든 subscriber 수신. Pattern matching 정확도.
- **MULTI/EXEC**: 트랜잭션 내 실패한 명령 → 나머지 명령 실행 결과 검증. WATCH → DISCARD 시나리오.

### Architecture
- 테스트 간 공유 상태 없음.
- Mock은 실패 모드도 시뮬레이션.
- Snapshot 테스트보다 구조적 property assert 선호.

### Naming
- 행동 기술: `test_resp_bulk_string_zero_copy`, `test_expires_key_after_ttl`, `test_rejects_wrong_arity`.

---

## 6. Security Rules

### Input Validation
- **Key 이름**: 최대 길이 제한 (`MAX_KEY_SIZE`, 기본 512MB — Redis 호환). NUL 바이트 허용 (binary-safe).
- **Value 크기**: 최대 크기 제한 (`proto-max-bulk-len`, 기본 512MB).
- **명령 인자 수**: 각 명령의 arity 검증. 초과/부족 시 에러.
- **INTEGER 범위**: `INCR`/`DECR` 결과가 i64 범위 초과 시 에러.
- **ACL**: 인증되지 않은 클라이언트는 AUTH와 일부 명령만 허용.
- **에러 메시지 sanitization**: 내부 경로 노출 금지, 256자 제한.

### Filesystem
- **Atomic write**: tmp → fsync → rename. 직접 in-place 덮어쓰기 금지.
- **삭제 전**: `fs::symlink_metadata()` 체크 필수.
- **디렉토리 생성**: `ensure_not_symlink()` → `create_dir_all()` → `ensure_not_symlink()` (TOCTOU 방지).
- **권한 검증**: fd 기반 — 파일 열기 → `file.metadata()?.mode()` → `mode & 0o077` 체크.
- **RDB/AOF 파일**: 생성 시 0o600 권한 설정.
- **CONFIG REWRITE**: 원본 파일 읽기 → 수정 → atomic write.

### Network
- **Protected mode**: 비밀번호 미설정 + 비로컬 연결 → 거부.
- **최대 클라이언트 수**: `maxclients` 설정. FD limit 고려.
- **Query buffer limit**: 클라이언트당 읽기 버퍼 최대 크기 (`client-query-buffer-limit`). 초과 시 연결 해제.
- **Output buffer limit**: 클라이언트 유형별 (normal, pubsub, replica) hard/soft limit.

---

## 7. Stability Rules

### Event Loop
- 메인 이벤트 루프에서 blocking 작업 절대 금지.
- 모든 I/O는 non-blocking + event-driven.
- `server_cron` 타이머: 기본 10Hz. 매 tick마다 expiry scan, client timeout check, persistence check, memory stats.
- Slow command 감지: 명령 실행 시간 기록, threshold 초과 시 slowlog에 기록.

### Concurrency
- **Single-threaded command execution**: 메인 루프에서 모든 명령 직렬 실행. 데이터 구조에 락 불필요.
- **I/O 스레드**: RESP 파싱/직렬화만 수행. 데이터 접근 금지.
- **Background 스레드**: persistence, lazy free 전용. `Arc<AtomicBool>` shutdown 플래그 패턴.
- **Background save**: fork 또는 background thread. 진행 중 상태를 `AtomicU8`로 추적.
- 글로벌 카운터: `static AtomicU64`.
- Mutex poisoned 복구 시 **항상 로그** (release 포함).

### Backpressure
- **클라이언트 output buffer**: hard limit 초과 → 즉시 연결 해제. Soft limit + 시간 초과 → 연결 해제.
- **Pub/Sub subscriber**: output buffer limit 적용. 느린 subscriber는 분리.
- **AOF rewrite buffer**: rewrite 중 누적되는 diff에 최대 크기 제한.
- **Replication backlog**: 고정 크기 ring buffer.

### Retry & Backoff
- 고정 간격 retry 금지 → exponential backoff: `(ms * 2).min(max)`.
- Background save 실패 시 재시도 간격 증가.

### Resource Management
- `Vec::remove(0)` 금지 → `VecDeque::pop_front()`.
- 캐시: LRU eviction + 최대 크기 const.
- Ring buffer capacity: 2의 거듭제곱.
- 설정값: 환경변수 override 가능 + `.clamp()`.
- **메모리 추적**: 전역 allocator wrapper로 사용량 추적. `maxmemory` 정책 시행.
- **Lazy free**: 대용량 키 삭제 시 background thread에서 점진적 해제. 메인 루프 블로킹 방지.

---

## 8. Performance Rules

### General
- `HashSet<usize>` → `u16`/`u32` 비트마스크 (≤16 요소).
- `HashMap<String, _>` → `HashMap<usize, _>` (인덱스 대체 가능 시).
- Hot path: `format!()` → `String::with_capacity()` + `push_str()`.
- `.clone()` → `Bytes::clone()` (RC bump) 또는 구조체 분해로 소유권 이전.
- Borrow 충돌: `Vec<(idx, value)>` 수집 → 루프 밖에서 처리.
- Magic number → `const` (예: `DEFAULT_DB_COUNT`, `CRON_HZ`, `MAX_INLINE_SIZE`).
- 반복 문자열은 변수 저장 후 재사용.

### Zero-Copy Hot Path
- RESP 파싱: `Bytes::slice()` 로 sub-buffer 참조. 새 `Vec<u8>` 할당 금지.
- 응답 직렬화: pre-allocated `IoSlice` 배열로 scatter write.
- **Shared objects**: `+OK\r\n`, `:0\r\n`, `:1\r\n`, `$-1\r\n` 등 자주 쓰는 응답은 `static Bytes`로 미리 할당.
- **Integer cache**: 0~9999 정수 응답은 사전 할당된 `Bytes`에서 반환 (Redis의 OBJ_SHARED_INTEGERS).
- 클라이언트 간 값 공유: `Bytes::clone()` = reference count 증가만. 데이터 복사 없음.

### Memory
- Key: `Bytes` (reference-counted, zero-copy slice 가능).
- Small values: inline/stack 저장 (SmallVec 또는 compact representation).
- Deletion: lazy free를 background thread에서 수행. 메인 루프에서 `drop()` 호출만.
- Pre-allocation: `Vec::with_capacity()` 필수. 특히 RESP 배열 응답, KEYS 결과.
- 대용량 임시 버퍼: 스레드 로컬 재사용 (`thread_local!`).

### I/O
- 읽기: `BytesMut` 버퍼에 `read()` → 누적 → 파싱. 버퍼 재사용.
- 쓰기: 응답 청크 목록 → `writev()` 로 한 번에 전송. 개별 `write()` 호출 금지.
- TCP: `TCP_NODELAY` 활성화 (Redis 기본 동작).
- Keep-alive: `SO_KEEPALIVE` 설정 (기본 300초).

---

## 9. Crate별 참고

### ratatosk-core
- 순수 도메인 모델. `#![forbid(unsafe_code)]`.
- `ClientId(u64)`, `DbIndex(u16)`, `SlotId(u16)` — newtype ID 타입.
- `CommandFlags(u64)` — 비트마스크: WRITE, READONLY, DENYOOM, ADMIN, PUBSUB, BLOCKING, FAST 등.
- `AclCategory(u64)` — 비트마스크: KEYSPACE, READ, WRITE, SET, SORTEDSET, LIST, HASH, STRING, PUBSUB, ADMIN 등.
- `RedisError` — crate 전체 공개 에러 타입 (`thiserror`). WRONGTYPE, WRONGARG, OOM, LOADING, BUSY 등.

### ratatosk-resp
- `#![forbid(unsafe_code)]` (bytes crate의 safe API만 사용).
- `RespValue` enum: SimpleString, Error, Integer, BulkString, Array, Null, Map, Push 등.
- `parse(buf: &mut BytesMut) -> Result<Option<RespFrame>, RespError>` — incremental zero-copy 파서.
- `encode(frame: &RespFrame) -> Vec<IoSlice>` — scatter-write용 직렬화.
- inline command 파싱 지원 (telnet 호환).

### ratatosk-engine
- **Keyspace**: `Vec<Db>` (DB 배열). 각 DB는 `HashMap<Bytes, Object>`.
- **Object**: `type_tag` + `encoding` + `data` + `lru_clock`/`lfu_counter` + `expire`.
- **Command table**: `static` registry. `CommandInfo { name, handler, flags, arity, key_spec }`.
- **Transaction**: `MultiState { queued: Vec<QueuedCommand>, watch_keys: Vec<(DbIndex, Bytes)> }`.
- **Eviction**: 샘플링 기반 LRU/LFU. `maxmemory-samples` 설정.
- **Blocking**: BLPOP/BRPOP/XREAD 등 blocking 명령은 클라이언트를 대기 목록에 등록, 조건 충족 시 wake.

### ratatosk-pubsub
- `#![forbid(unsafe_code)]`.
- **ChannelStore**: 채널 이름 → subscriber 집합 매핑.
- **PatternStore**: 패턴 → subscriber 집합 매핑. Glob 패턴 매칭.
- **KeyspaceNotifier**: keyspace/keyevent 이벤트 생성. Engine에서 호출.
- **PubSubType trait**: global pub/sub와 shard pub/sub 다형성.
- **Client mode 전환**: SUBSCRIBE 시 클라이언트는 pub/sub 모드 진입. 일반 명령 불가 (SUBSCRIBE, UNSUBSCRIBE, PING, RESET만 허용).

### ratatosk-persist
- **RdbSaver**: keyspace → binary format → 파일. Background thread 또는 fork.
- **RdbLoader**: 파일 → keyspace 복원. CRC64 검증. 손상 감지 시 abort.
- **AofWriter**: 명령 → RESP format → append. fsync 정책 적용.
- **AofRewriter**: background에서 현재 keyspace를 compact AOF로 덤프.
- **AofManifest**: BASE + INCR 파일 목록 관리.
- **Recovery**: RDB 로드 → AOF replay. 둘 다 없으면 빈 상태로 시작.

### ratatosk-server
- **EventLoop**: `mio::Poll` 기반. `beforeSleep` / `afterSleep` 훅.
- **ClientManager**: `HashMap<ClientId, Client>`. Accept → register → read/write → close.
- **IoThreadPool**: 읽기/쓰기 I/O 병렬화. `crossbeam-channel` 기반 작업 분배.
- **Config**: 파일 파싱 + 환경변수 override. `CONFIG SET`/`CONFIG GET` 런타임 변경.
- **ServerCron**: 타이머 기반 하우스키핑. 주기적 expiry scan, persistence trigger, stats 갱신.
- **Signal handling**: SIGTERM → graceful shutdown. SIGUSR1 → RDB save. SIGHUP → config reload.

---

## 10. Safe Pattern Reference

자주 참조하는 코드 패턴. 새 코드 작성 시 해당 상황이면 이 패턴을 그대로 따른다.

<details>
<summary>Zero-copy RESP parsing</summary>

```rust
pub fn parse(buf: &mut BytesMut) -> Result<Option<RespFrame>, RespError> {
    if buf.is_empty() {
        return Ok(None); // Incomplete
    }
    match buf[0] {
        b'+' => parse_simple_string(buf),
        b'-' => parse_error(buf),
        b':' => parse_integer(buf),
        b'$' => parse_bulk_string(buf),
        b'*' => parse_array(buf),
        _ => parse_inline(buf),
    }
}

fn parse_bulk_string(buf: &mut BytesMut) -> Result<Option<RespFrame>, RespError> {
    let (len, header_end) = parse_length(buf)?;
    let total = header_end + len + 2; // data + \r\n
    if buf.len() < total {
        return Ok(None); // Incomplete
    }
    let _ = buf.split_to(header_end); // consume header
    let data = buf.split_to(len).freeze(); // zero-copy: Bytes from BytesMut
    let _ = buf.split_to(2); // consume \r\n
    Ok(Some(RespFrame::Bulk(data)))
}
```
</details>

<details>
<summary>Scatter-write response (writev)</summary>

```rust
struct ResponseWriter {
    chunks: Vec<Bytes>,
}

impl ResponseWriter {
    fn write_simple_string(&mut self, s: &str) {
        self.chunks.push(Bytes::from_static(b"+"));
        self.chunks.push(Bytes::copy_from_slice(s.as_bytes()));
        self.chunks.push(Bytes::from_static(b"\r\n"));
    }

    fn write_shared_ok(&mut self) {
        static OK: Bytes = Bytes::from_static(b"+OK\r\n");
        self.chunks.push(OK.clone()); // RC bump only
    }

    fn flush(&self, fd: RawFd) -> io::Result<usize> {
        let slices: Vec<IoSlice> = self.chunks.iter()
            .map(|b| IoSlice::new(b))
            .collect();
        nix::sys::uio::writev(fd, &slices)
    }
}
```
</details>

<details>
<summary>Shared integer cache</summary>

```rust
const SHARED_INTEGERS: usize = 10000;

struct SharedObjects {
    integers: Vec<Bytes>, // pre-encoded ":0\r\n" .. ":9999\r\n"
    ok: Bytes,
    null_bulk: Bytes,
    empty_array: Bytes,
}

impl SharedObjects {
    fn new() -> Self {
        let integers = (0..SHARED_INTEGERS)
            .map(|i| Bytes::from(format!(":{}\r\n", i)))
            .collect();
        Self {
            integers,
            ok: Bytes::from_static(b"+OK\r\n"),
            null_bulk: Bytes::from_static(b"$-1\r\n"),
            empty_array: Bytes::from_static(b"*0\r\n"),
        }
    }

    fn integer(&self, n: i64) -> Option<Bytes> {
        if n >= 0 && (n as usize) < SHARED_INTEGERS {
            Some(self.integers[n as usize].clone()) // RC bump only
        } else {
            None
        }
    }
}
```
</details>

<details>
<summary>Command handler 등록 패턴</summary>

```rust
struct CommandInfo {
    name: &'static str,
    handler: fn(&mut Client, &[Bytes]) -> Result<(), CommandError>,
    flags: CommandFlags,
    arity: i8, // positive = exact, negative = minimum
    key_spec: KeySpec,
}

static COMMANDS: &[CommandInfo] = &[
    CommandInfo {
        name: "GET",
        handler: cmd_get,
        flags: CommandFlags::READONLY | CommandFlags::FAST,
        arity: 2,
        key_spec: KeySpec::single(1),
    },
    CommandInfo {
        name: "SET",
        handler: cmd_set,
        flags: CommandFlags::WRITE | CommandFlags::DENYOOM,
        arity: -3,
        key_spec: KeySpec::single(1),
    },
];
```
</details>

<details>
<summary>Pub/Sub publish flow</summary>

```rust
fn publish(
    channels: &ChannelStore,
    patterns: &PatternStore,
    channel: &Bytes,
    message: &Bytes,
) -> usize {
    let mut receivers = 0;

    // Exact channel subscribers
    if let Some(subscribers) = channels.get(channel) {
        for client in subscribers.iter() {
            client.push_pubsub_message(channel, message);
            receivers += 1;
        }
    }

    // Pattern subscribers
    for (pattern, subscribers) in patterns.iter() {
        if glob_match(pattern, channel) {
            for client in subscribers.iter() {
                client.push_pubsub_pmessage(pattern, channel, message);
                receivers += 1;
            }
        }
    }

    receivers
}
```
</details>

<details>
<summary>Atomic file swap (tmp → fsync → rename)</summary>

```rust
fn atomic_write(target: &Path, data: &[u8]) -> Result<()> {
    let tmp = target.with_extension("tmp");
    let mut file = File::create(&tmp)?;
    file.write_all(data)?;
    file.sync_all()?;
    fs::rename(&tmp, target)?;
    #[cfg(unix)]
    if let Some(parent) = target.parent() {
        if let Ok(dir) = File::open(parent) { let _ = dir.sync_all(); }
    }
    Ok(())
}
```
</details>

<details>
<summary>Expiry lazy scan (server_cron)</summary>

```rust
const ACTIVE_EXPIRE_CYCLE_LOOKUPS: usize = 20;
const ACTIVE_EXPIRE_CYCLE_THRESHOLD: f64 = 0.25; // stop if < 25% expired

fn active_expire_cycle(db: &mut Db) {
    let mut expired = 0;
    let mut sampled = 0;

    while sampled < ACTIVE_EXPIRE_CYCLE_LOOKUPS {
        let Some((key, expiry)) = db.expiry.random_sample() else { break };
        sampled += 1;
        if expiry <= now() {
            db.delete(&key);
            expired += 1;
        }
    }

    // stop early if few keys are expiring (avoid wasting CPU)
    if sampled > 0 && (expired as f64 / sampled as f64) < ACTIVE_EXPIRE_CYCLE_THRESHOLD {
        return;
    }
}
```
</details>

<details>
<summary>Graceful shutdown</summary>

```rust
let shutdown = Arc::new(AtomicBool::new(false));
let flag = Arc::clone(&shutdown);
thread::spawn(move || {
    while !flag.load(Ordering::Relaxed) {
        // ... background work ...
    }
});
// cleanup: shutdown.store(true, Ordering::Relaxed);
```
</details>

<details>
<summary>Client output buffer limit</summary>

```rust
struct OutputBufferLimits {
    hard_limit: usize,
    soft_limit: usize,
    soft_seconds: u64,
}

fn check_output_buffer(client: &mut Client, limits: &OutputBufferLimits) -> bool {
    let buf_size = client.output_buffer_size();

    if limits.hard_limit > 0 && buf_size >= limits.hard_limit {
        return true; // disconnect immediately
    }

    if limits.soft_limit > 0 && buf_size >= limits.soft_limit {
        if client.soft_limit_reached_at.is_none() {
            client.soft_limit_reached_at = Some(Instant::now());
        }
        if let Some(reached) = client.soft_limit_reached_at {
            if reached.elapsed().as_secs() >= limits.soft_seconds {
                return true; // disconnect: soft limit exceeded for too long
            }
        }
    } else {
        client.soft_limit_reached_at = None; // reset
    }

    false
}
```
</details>

<details>
<summary>Blocking command (BLPOP)</summary>

```rust
fn cmd_blpop(client: &mut Client, keys: &[Bytes], timeout: Duration) -> Result<(), CommandError> {
    // Try immediate pop
    for key in keys {
        if let Some(value) = try_lpop(client.db(), key) {
            client.reply_array(&[key.clone(), value]);
            return Ok(());
        }
    }

    // No data available: block the client
    client.block(BlockingState {
        keys: keys.to_vec(),
        timeout,
        operation: BlockOp::ListPop,
    });

    // When another client pushes to any watched key:
    // unblock_client(client) → deliver the popped value
    Ok(())
}
```
</details>

<details>
<summary>Mutex poisoned recovery (always log)</summary>

```rust
fn lock_read<'a, T>(lock: &'a RwLock<T>, ctx: &'static str) -> RwLockReadGuard<'a, T> {
    match lock.read() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::warn!("RwLock read poisoned in {}", ctx);
            poisoned.into_inner()
        }
    }
}
```
</details>

---

*이 파일은 Claude가 프로젝트 컨텍스트를 이해하는 데 사용됩니다. 실수가 반복되면 여기에 규칙을 추가하세요.*
