# Ratatosk Performance & Optimization

---

## Part 1: Performance Baseline

Ratatosk 성능 기준선 및 최적화 계획.

### Baseline

#### Quick Workflow

```bash
# 1) baseline 측정 (default allocator)
./scripts/bench_baseline.sh

# 2) 선택: mimalloc 비교
WITH_MIMALLOC=1 ./scripts/bench_baseline.sh

# 3) guardrail 검증
python3 scripts/perf_guardrail_check.py --log <benchmark_log>
```

#### Bench Scope

- bench target: `cargo bench -p ratatosk-server --bench pipeline`
- metrics:
  - `pipeline_set_parse_execute_encode`
  - `pipeline_ping_parse_execute_encode`
- pipeline lengths: `1`, `32`, `256`, `1024`

#### Guardrail Defaults

- `pipeline_set_parse_execute_encode/256` upper <= `110 us`
- `pipeline_ping_parse_execute_encode/256` upper <= `35 us`
- checker: `scripts/perf_guardrail_check.py`

#### Reference Logs

| Log | set@256 upper | ping@256 upper | Guardrail Result | Classification |
| --- | ---: | ---: | --- | --- |
| `benchmarks/baseline-default-20260208-185945.log` | 101.510 us | 29.338 us | PASS | canonical baseline |
| `benchmarks/baseline-default-20260208-191454.log` | 101.610 us | 30.074 us | PASS | compatible baseline |
| `benchmarks/baseline-default-20260208-195805.log` | 103.610 us | 77.826 us | FAIL (ping) | high-variance outlier |

운영 규칙:
- PASS 로그만 guardrail 업데이트 후보로 사용한다.
- FAIL/고분산 로그는 회귀 원인 조사 참고용으로만 보관한다.

#### Allocator A/B Policy

- 현재 기본 allocator를 유지한다.
- `mimalloc`은 feature flag(`--features mimalloc`)로 재측정할 수 있다.
- allocator 전환은 최소 2회 이상 일관된 개선 결과가 있을 때만 검토한다.

#### Record Template

| Date | Commit | Allocator | set@256 upper (us) | ping@256 upper (us) | Guardrail | Notes |
| --- | --- | --- | ---: | ---: | --- | --- |
| YYYY-MM-DD | <sha> | default |  |  | PASS/FAIL |  |
| YYYY-MM-DD | <sha> | mimalloc |  |  | PASS/FAIL |  |

---

### SharedState: Lock-Free Fast Paths

`SharedState` 구조체가 `Mutex<ServerState>` 외부에 lock-free 컴포넌트를 분리하여, client 요청당 ~9회의 lock 획득을 제거한다.

#### Per-DB RwLock

`ServerState` 내부 `DataState`가 DB별 `parking_lot::RwLock<DbShard>`를 관리한다:

- `db(idx)` → `MappedRwLockReadGuard<HashMap>`: 읽기 명령은 read lock 공유
- `db_mut(idx)` → `DbWriteGuard<'_>`: 쓰기 명령은 해당 DB만 exclusive lock. guard의 `insert`/`remove`/`set_key_expiry`가 메모리 추정치와 expires index를 함께 갱신한다
- 서로 다른 DB에 대한 명령은 lock contention 없이 병렬 실행 가능
- `snapshot_all()`: BGSAVE 시 DB별 순차 read-lock + clone. 전체 global lock 점유 대신 DB 하나씩 짧게 잠금
- Lock ordering: 항상 ascending DB index 순서로 획득 → deadlock 방지
- `parking_lot` guard는 `!Send` → `.await`를 넘을 수 없어 compile-time 안전성 보장

#### AtomicStatsState

11개 atomic counter로 매 요청마다 lock 없이 stats를 갱신 (`crates/ratatosk-engine/src/stats.rs`의 `AtomicStatsState`):

- `total_commands_processed`, `connected_clients`, `total_connections_received`
- `total_net_input_bytes`, `total_net_output_bytes`
- `evicted_keys`, `expired_keys`
- `keyspace_hits`, `keyspace_misses`
- `instantaneous_ops_per_sec`, `cached_memory_estimate`

`INFO` 명령의 stats 섹션이 이 atomic counter를 직접 읽으므로, stats 조회도 lock-free.

#### ArcSwap<ConfigState>

`arc_swap::ArcSwap<ConfigState>`를 통해 config 읽기가 lock-free:

- 읽기: `config_cache.load()` — 매 요청의 config 참조가 lock 없이 수행됨
- 쓰기: `CONFIG SET` 시에만 lock 필요

#### Atomic Client ID

`AtomicI64::fetch_add`로 새 연결의 client ID를 할당. accept 경로에서 lock이 불필요.

### Pub/Sub: mpsc Push Delivery

기존 `HashMap<i64, Vec<PubSubMessage>>` 기반 polling을 per-subscriber `tokio::sync::mpsc::channel`로 교체.

성능 이점:
- **Zero polling overhead**: 이전의 20ms polling interval이 제거됨. 메시지가 즉시 push 전달.
- **Backpressure**: `try_send()` 기반. channel capacity (= `max(hard_limit, 1)`) 초과 시 즉시 overflow → disconnect.
- **Client loop 통합**: `WaitResult` enum으로 pub/sub, monitor, network read를 단일 `select!`에서 처리.
- **Client tracking invalidation**: 동일 mpsc 채널을 통해 자동 전달. 별도 delivery 경로 불필요.

---

### Optimization Plan: cmd_string.rs (completed)

이 절에 있던 string hot-path 계획은 모두 반영되었다. 현재 상태:

- `cmd_get` (`crates/ratatosk-engine/src/command/cmd_string_access.rs`): 단일 `db.get(key)` 조회. double lookup 없음.
- `INCR`/`DECR` (`cmd_string_numeric.rs`): 결과를 `StoredValue::string_int(next, expire_at_ms)`로 저장하므로 요청마다 문자열 heap allocation이 발생하지 않는다.
- `SET` 옵션 파싱 (`cmd_string_access.rs`): `eq_ignore_ascii_case` 기반 slice 비교. 대문자 변환 allocation 없음.
- 정수 문자열은 `ValueData::StringInt(i64)`로 보관되며 `OBJECT ENCODING`은 `int`를 반환한다.

---

## Part 2: RAM/CPU Optimization Master Plan

> 작성일: 2026-04-05
> 기준 코드: 현재 `main` 브랜치
> 총 소스: ~55,000 lines Rust (5 workspace crates)

> **구현 현황 (2026-06-02 코드 기준):** 이 마스터 플랜의 상당 부분은 이미 `main`에 반영되었다. 아래 Phase별 섹션은 설계 근거를 담은 계획 문서로 유지하되, 현재 코드와 대조해서 읽어야 한다.
>
> - ✅ 반영됨:
>   - Phase 1 — `StoredValue` 24-byte 레이아웃 (`Box<ValueData>` + packed `encoding_and_lru: u32`, test invariant로 고정)
>   - Phase 2 — incremental memory tracking (`AtomicStatsState::cached_memory_estimate`)
>   - Phase 3 — expires side-index (`DbShard.expires: HashMap<Bytes, i64>`, `sync_expires()`)
>   - Phase 4A — `ValueData::StringInt(i64)` / `Encoding::Int`
>   - Phase 4B — `ValueData::SetInt(Vec<i64>)` / `Encoding::IntSet`
>   - Phase 6 — `mimalloc`/`jemalloc` 모두 feature로 선택 가능 (`mimalloc`, `jemalloc` feature). 기본 빌드는 `default = []`라 어떤 allocator feature도 켜지지 않으며 시스템 allocator를 사용한다
> - ⏳ 미반영:
>   - Phase 4C — Listpack 인코딩 (코드에 Listpack 변형 없음)
>   - Phase 7 — SortedSet 전용 SkipList (현재 `SortedSet`은 `BTreeMap` + `HashMap` 기반)
> - Phase 0 / 5 / 8은 코드와 직접 대조해 상태를 확인할 것.
>
> 참고: 아래 Part 3 "실측 기준 데이터"의 일부 수치(예: `StoredValue = 96 bytes`)는 **Phase 1 이전** 측정값이다. Phase 1 적용 후 현재 `StoredValue`는 24 bytes다.

---

### 목차

1. [Phase 1: StoredValue 메모리 레이아웃 압축](#phase-1)
2. [Phase 2: Incremental Memory Tracking (O(N)→O(1))](#phase-2)
3. [Phase 3: Expires Index 분리](#phase-3)
4. [Phase 4: Compact Encoding (Listpack/IntSet)](#phase-4)
5. [Phase 5: BGSAVE Snapshot 최적화](#phase-5)
6. [Phase 6: jemalloc 도입](#phase-6)
7. [Phase 7: SortedSet 구조 개선](#phase-7)
8. [Phase 8: 소규모 Hot Path 최적화 모음](#phase-8)
9. [검증 전략](#verification)
10. [의존성 그래프 및 일정](#schedule)

---

<a id="phase-1"></a>
### Phase 1: StoredValue 메모리 레이아웃 압축

#### 1.1 현황 분석

```
현재 StoredValue (추정 ~120 bytes per key):
┌─────────────────────────────────────┐
│ ValueData (enum)         ~96 bytes  │  ← SortedSet variant가 가장 큼
│ expire_at_ms: Option<i64>  16 bytes │  ← 대부분 None인데 항상 16B
│ encoding: Encoding (u8)    1 byte   │
│ [padding]                  7 bytes  │  ← alignment
│ lru_clock: u32             4 bytes  │
│ [padding]                  4 bytes  │  ← alignment
└─────────────────────────────────────┘
```

**핵심 문제**: Rust enum은 가장 큰 variant 크기로 통일. `ValueData::String(Bytes)`는 
32 bytes면 충분하지만 `ValueData::SortedSet`의 88 bytes가 기준이 됨.

#### 1.2 개선 설계

##### Step 1: `Box<ValueData>` 도입

```rust
// Before (120 bytes)
pub struct StoredValue {
    pub data: ValueData,          // 96 bytes inline
    pub expire_at_ms: Option<i64>,
    pub encoding: Encoding,
    pub lru_clock: u32,
}

// After (32 bytes)
pub struct StoredValue {
    data: Box<ValueData>,         // 8 bytes (pointer)
    expire_at_ms: i64,            // 8 bytes (0 = no expiry)
    encoding_and_lru: u32,        // 4 bytes packed
    _pad: u32,                    // alignment
}
```

**절감**: 120B → 32B = **키당 88 bytes 절감**
**1M 키 기준**: ~84 MB 절감

##### Step 2: `expire_at_ms` 규약 변경

```rust
// Before
pub expire_at_ms: Option<i64>,   // 16 bytes

// After: 0 means "no expiry" (Unix ms epoch 0 = 1970-01-01, 실용적으로 불가능)
pub expire_at_ms: i64,           // 8 bytes

// 헬퍼 메서드
impl StoredValue {
    #[inline]
    pub fn has_expiry(&self) -> bool { self.expire_at_ms > 0 }
    
    #[inline]
    pub fn expire_at_ms(&self) -> Option<i64> {
        if self.expire_at_ms > 0 { Some(self.expire_at_ms) } else { None }
    }
    
    #[inline]
    pub fn set_expire_at_ms(&mut self, ms: Option<i64>) {
        self.expire_at_ms = ms.unwrap_or(0);
    }
}
```

##### Step 3: encoding + lru_clock 패킹

```rust
// encoding: 4 bits (0-15, 현재 6가지 사용)
// lru_clock: 28 bits (268M 초 = ~8.5년 wrap, 충분)

impl StoredValue {
    const ENCODING_BITS: u32 = 4;
    const LRU_MASK: u32 = (1 << 28) - 1;
    
    #[inline]
    pub fn encoding(&self) -> Encoding {
        Encoding::from_u8((self.encoding_and_lru >> 28) as u8)
    }
    
    #[inline]
    pub fn lru_clock(&self) -> u32 {
        self.encoding_and_lru & Self::LRU_MASK
    }
    
    #[inline]
    pub fn set_lru_clock(&mut self, clock: u32) {
        let enc = self.encoding_and_lru & !Self::LRU_MASK;
        self.encoding_and_lru = enc | (clock & Self::LRU_MASK);
    }
}
```

#### 1.3 변경 범위

| 파일 | 변경 내용 | 위험도 |
|------|----------|--------|
| `keyspace.rs` | `StoredValue` 필드 변경 + 접근자 메서드 추가 | 중 |
| `eviction.rs` | `value.lru_clock` → `value.lru_clock()` | 하 |
| `expiry.rs` | `value.expire_at_ms` → `value.expire_at_ms()` | 하 |
| `command/cmd_*.rs` (전체) | `.expire_at_ms` 직접 접근 → 메서드 호출 | 중 |
| `rdb/saver.rs`, `rdb/loader.rs` | 직렬화/역직렬화 업데이트 | 중 |
| `embedded.rs` | StoredValue 생성 패턴 업데이트 | 하 |
| `direct.rs` | StoredValue 접근 패턴 업데이트 | 하 |

#### 1.4 마이그레이션 전략

1. **먼저 접근자 메서드만 추가** (필드는 그대로):
   - `pub fn expire_at_ms_opt(&self) -> Option<i64>`
   - `pub fn lru_clock_value(&self) -> u32`
2. 모든 직접 접근을 메서드 호출로 전환 (기계적 리팩터링)
3. 테스트 전체 통과 확인
4. 내부 필드를 새 레이아웃으로 교체
5. 접근자 이름을 최종 형태로 정리

#### 1.5 성능 영향

- **Box 역참조 비용**: L1 cache에서 ~1ns 추가. 하지만 StoredValue가 작아져 cache line 
  활용률이 높아지므로 순효과는 긍정적 (HashMap 순회 시 한 cache line에 더 많은 키가 적재)
- Redis도 `robj` (redisObject) 16 bytes + 별도 데이터 포인터 구조 사용

---

<a id="phase-2"></a>
### Phase 2: Incremental Memory Tracking (O(N) → O(1))

#### 2.1 현황 분석

현재 `estimate_used_memory()`는 **전체 DB를 순회**하며 메모리 합산:

```rust
// eviction.rs:174 — 전체 DB 순회 O(N)
pub fn estimate_used_memory(state: &ServerState) -> usize {
    for db_idx in 0..state.db_count() {
        let db = state.db(db_idx);
        for (key, value) in db.iter() {      // ← O(N)
            total += estimate_object_memory(key, value);
        }
    }
}

// perform_eviction에서 이 함수를 최대 128회 호출:
// 1회 (memory_before) + 최대 128회 (loop) + 1회 (memory_after)
```

**최악 시나리오**: 키 100만 개 × 130 순회 = 1.3억 반복/eviction cycle

#### 2.2 개선 설계

##### Per-DB Atomic Memory Counter

```rust
// DataState에 추가
pub struct DataState {
    shards: Arc<[parking_lot::RwLock<DbShard>]>,
    next_key_version: Arc<AtomicU64>,
    // NEW: per-DB estimated memory (atomic, lock-free update)
    db_memory_bytes: Arc<[AtomicUsize]>,
}

impl DataState {
    /// Adjust memory estimate when inserting/removing/modifying a key.
    pub fn adjust_memory(&self, db_idx: usize, delta: isize) {
        if delta >= 0 {
            self.db_memory_bytes[db_idx].fetch_add(delta as usize, Ordering::Relaxed);
        } else {
            self.db_memory_bytes[db_idx].fetch_sub((-delta) as usize, Ordering::Relaxed);
        }
    }
    
    /// O(1) total memory estimate.
    pub fn estimated_memory(&self) -> usize {
        self.db_memory_bytes.iter()
            .map(|a| a.load(Ordering::Relaxed))
            .sum()
    }
}
```

##### Insert/Remove Hook

```rust
// ServerState의 db_mut() 반환 타입을 MemTrackingDbGuard로 교체

pub struct MemTrackingDbGuard<'a> {
    guard: parking_lot::MappedRwLockWriteGuard<'a, HashMap<Bytes, StoredValue>>,
    memory: &'a AtomicUsize,
}

impl MemTrackingDbGuard<'_> {
    pub fn insert(&mut self, key: Bytes, value: StoredValue) -> Option<StoredValue> {
        let new_mem = estimate_object_memory(&key, &value);
        let old = self.guard.insert(key.clone(), value);
        if let Some(ref old_val) = old {
            let old_mem = estimate_object_memory(&key, old_val);
            // Adjust delta only
            if new_mem >= old_mem {
                self.memory.fetch_add(new_mem - old_mem, Ordering::Relaxed);
            } else {
                self.memory.fetch_sub(old_mem - new_mem, Ordering::Relaxed);
            }
        } else {
            self.memory.fetch_add(new_mem, Ordering::Relaxed);
        }
        old
    }
    
    pub fn remove(&mut self, key: &Bytes) -> Option<StoredValue> {
        let old = self.guard.remove(key);
        if let Some(ref old_val) = old {
            let mem = estimate_object_memory(key, old_val);
            self.memory.fetch_sub(mem, Ordering::Relaxed);
        }
        old
    }
}
```

##### perform_eviction 수정

```rust
pub fn perform_eviction(state: &mut ServerState, config: &EvictionConfig) -> usize {
    // O(1) instead of O(N)
    let memory_before = state.data.estimated_memory();
    
    for _ in 0..128 {
        let used = state.data.estimated_memory();  // O(1)
        if used <= config.maxmemory {
            break;
        }
        // ... evict one key (adjusts counter automatically)
    }
    
    let memory_after = state.data.estimated_memory();  // O(1)
    // ...
}
```

#### 2.3 드리프트 보정

Atomic counter는 추정치이므로 점진적 드리프트가 발생할 수 있다.

```rust
// server_cron에서 주기적 보정 (매 100 tick = ~10초)
if cron_tick % 100 == 0 {
    let actual = estimate_used_memory_full_scan(&server);
    let tracked = server.data.estimated_memory();
    let drift_pct = ((actual as f64 - tracked as f64) / actual as f64).abs() * 100.0;
    
    if drift_pct > 5.0 {
        server.data.reset_memory_estimate(actual);
        metrics::counter!("ratatosk_memory_drift_resets").increment(1);
    }
}
```

#### 2.4 변경 범위

| 파일 | 변경 내용 |
|------|----------|
| `keyspace.rs` (DataState) | `db_memory_bytes` 필드 추가, `adjust_memory()`, `estimated_memory()` |
| `keyspace.rs` (ServerState) | `db_mut()` 반환 타입 변경 또는 insert/remove wrapper |
| `eviction.rs` | `estimate_used_memory()` → `data.estimated_memory()` |
| `event_loop.rs` | server_cron에서 주기적 보정 |
| `command/cmd_*.rs` | insert/remove 호출이 자동 추적되면 변경 불필요 |

#### 2.5 예상 효과

| 시나리오 | Before | After |
|---------|--------|-------|
| 100만 키, eviction 1 cycle | ~130M 반복 | ~130 반복 |
| server_cron memory check | O(N) every 10 ticks | O(1) every tick |
| CPU 절감 | — | **eviction 시 99%+** |

---

<a id="phase-3"></a>
### Phase 3: Expires Index 분리

#### 3.1 현황 분석

Active expiry에서 **volatile 키를 찾기 위해 전체 DB를 2번 순회**:

```rust
// expiry.rs:142 — 1차 순회: volatile 키 카운트
let volatile_count = db.iter()
    .filter(|(_, v)| v.expire_at_ms.is_some())
    .count();

// expiry.rs:163 — 2차 순회: reservoir sampling으로 키 수집
for (key, value) in db.iter() {
    if value.expire_at_ms.is_none() { continue; }
    // ...
}
```

Redis는 별도 `expires` dict를 유지하여 volatile 키만 O(1)으로 카운트/접근한다.

#### 3.2 개선 설계

##### DbShard에 expires index 추가

```rust
#[derive(Debug, Clone, Default)]
pub struct DbShard {
    pub data: HashMap<Bytes, StoredValue>,
    pub key_versions: HashMap<Bytes, u64>,
    // NEW: volatile 키만 모은 인덱스 (key → expire_at_ms)
    pub expires: HashMap<Bytes, i64>,
}
```

##### 동기화 규칙

| 연산 | data | expires |
|------|------|---------|
| `SET key val EX 60` | insert | insert(key, expire_at_ms) |
| `SET key val` (no TTL) | insert | remove(key) |
| `PERSIST key` | update expire_at_ms=None | remove(key) |
| `DEL key` | remove | remove(key) |
| `EXPIRE key 60` | update expire_at_ms | insert(key, expire_at_ms) |
| Passive expiry (키 접근 시 만료) | remove | remove(key) |
| Active expiry | remove | remove(key) |

##### Helper 함수

```rust
impl DbShard {
    /// Insert or update a key, keeping expires index in sync.
    pub fn set_key(&mut self, key: Bytes, value: StoredValue) -> Option<StoredValue> {
        if value.has_expiry() {
            self.expires.insert(key.clone(), value.expire_at_ms_raw());
        } else {
            self.expires.remove(&key);
        }
        self.data.insert(key, value)
    }
    
    /// Remove a key, keeping expires index in sync.
    pub fn del_key(&mut self, key: &Bytes) -> Option<StoredValue> {
        self.expires.remove(key);
        self.data.remove(key)
    }
    
    /// Update TTL on an existing key.
    pub fn set_expiry(&mut self, key: &Bytes, expire_at_ms: Option<i64>) {
        if let Some(value) = self.data.get_mut(key) {
            value.set_expire_at_ms(expire_at_ms);
            match expire_at_ms {
                Some(ms) => { self.expires.insert(key.clone(), ms); }
                None => { self.expires.remove(key); }
            }
        }
    }
    
    /// Volatile key count (O(1)).
    pub fn volatile_count(&self) -> usize {
        self.expires.len()
    }
}
```

##### Active Expiry 수정

```rust
pub fn active_expire_cycle(state: &mut ServerState, now_ms: i64) -> usize {
    for db_idx in 0..state.db_count() {
        let sampled_keys = {
            let shard = state.data.read_db(db_idx);
            let expires = &shard.expires;
            
            if expires.is_empty() { continue; }  // O(1) check
            
            // 직접 expires map에서 샘플링 — volatile 키만 순회
            let samples_to_take = cycle_lookups.min(expires.len());
            // Reservoir sampling on expires.iter() — 전체 DB가 아닌 volatile 키만
            reservoir_sample(expires.iter(), samples_to_take, &mut rng)
        };
        
        // Expiry phase
        for (key, expire_ms) in sampled_keys {
            if expire_ms <= now_ms {
                let mut shard = state.data.write_db(db_idx);
                shard.del_key(&key);
                expired += 1;
            }
        }
    }
}
```

#### 3.3 메모리 비용

- `expires` HashMap: 키당 추가 ~72 bytes (Bytes 32B + i64 8B + entry overhead 32B)
- **하지만 volatile 키에만 적용** — 대부분의 워크로드에서 전체 키의 10-30%
- CPU 절감이 메모리 비용보다 훨씬 크다

#### 3.4 변경 범위

| 파일 | 변경 |
|------|------|
| `keyspace.rs` (DbShard) | `expires` 필드 추가, helper 메서드 |
| `expiry.rs` | `expires` 에서 직접 샘플링 |
| `eviction.rs` | volatile 정책에서 `expires`를 사용해 후보 선택 |
| `command/cmd_key.rs` | EXPIRE/PERSIST 시 `set_expiry()` 호출 |
| `command/cmd_generic_string.rs` | SET EX/PX 시 `set_key()` 호출 |
| `command/cmd_*.rs` (전체) | `purge_expired_key` 내부 수정 |
| `rdb/loader.rs` | RDB 로드 시 expires 구축 |
| `rdb/saver.rs` | 직렬화는 data에서 읽으므로 변경 불필요 |

#### 3.5 예상 효과

| 시나리오 | Before | After |
|---------|--------|-------|
| 100만 키, 10% volatile | 200만 반복/cycle | 20만 반복/cycle |
| 100만 키, 0% volatile | 200만 반복 (낭비) | 0 반복 (즉시 skip) |
| volatile_count 확인 | O(N) | O(1) |

---

<a id="phase-4"></a>
### Phase 4: Compact Encoding (Listpack/IntSet)

#### 4.1 개요

Redis의 핵심 메모리 절감 기법. 소규모 컬렉션을 **연속 바이트 배열**로 직렬화하여
HashMap/BTreeMap 오버헤드를 제거한다.

```
Redis 인코딩 전략:
┌──────────┬───────────────┬──────────────┬─────────────────────┐
│ 타입     │ 소규모        │ 대규모       │ 전환 기준            │
├──────────┼───────────────┼──────────────┼─────────────────────┤
│ Hash     │ listpack      │ hashtable    │ entries>128∨val>64B │
│ Set      │ intset/lpack  │ hashtable    │ entries>128∨val>64B │
│ ZSet     │ listpack      │ skiplist+ht  │ entries>128∨val>64B │
│ List     │ quicklist     │ quicklist    │ node size = -2 (8KB)│
│ String   │ int encoding  │ raw/embstr   │ fits i64?           │
└──────────┴───────────────┴──────────────┴─────────────────────┘
```

#### 4.2 구현 우선순위

##### 4.2.1 Priority A: String Int Encoding (가장 쉬움)

i64로 파싱 가능한 문자열은 별도 Bytes 할당 없이 정수로 저장.

```rust
pub enum ValueData {
    String(Bytes),
    StringInt(i64),         // NEW: "12345" → 8 bytes (Bytes 32B → 8B)
    Hash(HashMap<...>),
    // ...
}

// SET/GET에서 자동 감지
fn cmd_set(...) {
    let value_data = if let Some(int_val) = try_parse_i64(&value) {
        ValueData::StringInt(int_val)
    } else {
        ValueData::String(value)
    };
    // ...
}
```

**절감**: 정수 문자열 키당 24 bytes (Bytes 32B - i64 8B)
**적용 비율**: 카운터, 타임스탬프, ID 등 — 일반 워크로드의 20-40%

##### 4.2.2 Priority B: IntSet (정수 Set)

모든 멤버가 정수인 소규모 Set을 `Vec<i64>`로 직렬화.

```rust
pub enum ValueData {
    // ...
    Set(HashSet<Bytes>),
    SetInt(Vec<i64>),       // NEW: sorted i64 array, binary search
    // ...
}

// CONFIG 파라미터:
// set-max-intset-entries = 512 (default)
```

```rust
impl SetInt {
    // Binary search on sorted vec — O(log n)
    pub fn contains(&self, value: i64) -> bool {
        self.0.binary_search(&value).is_ok()
    }
    
    pub fn insert(&mut self, value: i64) -> bool {
        match self.0.binary_search(&value) {
            Ok(_) => false,
            Err(pos) => { self.0.insert(pos, value); true }
        }
    }
    
    // 전환: 멤버가 threshold 초과 시 HashSet으로 업그레이드
    pub fn should_upgrade(&self, max_entries: usize) -> bool {
        self.0.len() >= max_entries
    }
}
```

**절감**: IntSet 멤버당 48B → 8B = **6x**

##### 4.2.3 Priority C: Listpack (범용 compact encoding)

소규모 Hash/Set/ZSet을 **연속 바이트 배열**로 직렬화.

```
Listpack 포맷 (간소화):
┌────────┬─────────┬─────────┬───┬─────────┬────┐
│ total  │ entry1  │ entry2  │...│ entryN  │ FF │
│ bytes  │ len|val │ len|val │   │ len|val │end │
│ (4B)   │         │         │   │         │    │
└────────┴─────────┴─────────┴───┴─────────┴────┘

Entry: [encoding_byte] [data] [backlen]
- encoding_byte: 0xxx = 7-bit int, 10xx = 13-bit int, 
                  110x = 16/24/32/64-bit int, 1110 = string
- backlen: variable-length previous entry size (for reverse traversal)
```

```rust
/// Compact byte-buffer encoding for small collections.
pub struct Listpack {
    buf: Vec<u8>,
    count: u16,
}

impl Listpack {
    pub fn new() -> Self { Self { buf: vec![0; 7], count: 0 } }
    
    pub fn push(&mut self, entry: &[u8]) { /* append encoded entry */ }
    pub fn get(&self, index: usize) -> Option<&[u8]> { /* linear scan */ }
    pub fn find(&self, key: &[u8]) -> Option<&[u8]> { /* linear scan for key-value */ }
    pub fn len(&self) -> usize { self.count as usize }
    
    /// Convert to full encoding when threshold exceeded
    pub fn to_hash(&self) -> HashMap<Bytes, HashFieldEntry> { /* ... */ }
    pub fn to_sorted_set(&self) -> SortedSet { /* ... */ }
}
```

##### ValueData 확장

```rust
pub enum ValueData {
    String(Bytes),
    StringInt(i64),
    
    // Hash: small → Listpack, large → HashMap
    Hash(HashMap<Bytes, HashFieldEntry>),
    HashPack(Listpack),              // NEW
    
    // Set: ints → IntSet, small strings → Listpack, large → HashSet
    Set(HashSet<Bytes>),
    SetInt(Vec<i64>),                // NEW
    SetPack(Listpack),              // NEW
    
    // Sorted Set: small → Listpack, large → BTreeMap+HashMap
    SortedSet(SortedSet),
    SortedSetPack(Listpack),         // NEW
    
    // List: stays as VecDeque (quicklist 구현은 별도)
    List(VecDeque<Bytes>),
    
    Stream { ... },
}
```

#### 4.3 자동 Encoding 전환

```rust
// CONFIG 파라미터
pub struct CompactEncodingConfig {
    pub hash_max_listpack_entries: usize,    // default 128
    pub hash_max_listpack_value: usize,      // default 64 bytes
    pub set_max_intset_entries: usize,       // default 512
    pub set_max_listpack_entries: usize,     // default 128
    pub set_max_listpack_value: usize,       // default 64 bytes
    pub zset_max_listpack_entries: usize,    // default 128
    pub zset_max_listpack_value: usize,      // default 64 bytes
}

// 예시: HSET에서 자동 전환
fn cmd_hset(key: &Bytes, fields: &[(Bytes, Bytes)], db: &mut DbShard, config: &CompactEncodingConfig) {
    match db.get_mut(key) {
        Some(entry) if entry.is_hash_pack() => {
            let pack = entry.as_hash_pack_mut().unwrap();
            for (field, value) in fields {
                pack.push_pair(field, value);
            }
            // Threshold 초과 시 업그레이드
            if pack.len() > config.hash_max_listpack_entries 
               || pack.max_entry_len() > config.hash_max_listpack_value {
                let hash = pack.to_hash();
                entry.data = ValueData::Hash(hash);
                entry.encoding = Encoding::HashTable;
            }
        }
        _ => { /* full encoding path */ }
    }
}
```

#### 4.4 변경 범위 (대규모)

| 파일 | 변경 |
|------|------|
| `keyspace.rs` | `ValueData` enum 확장, `Listpack` 모듈 추가 |
| `config.rs` | compact encoding threshold 설정 추가 |
| `command/cmd_hash*.rs` | Listpack ↔ HashMap 전환 로직 |
| `command/cmd_set*.rs` | IntSet/Listpack ↔ HashSet 전환 로직 |
| `command/cmd_sorted_set*.rs` | Listpack ↔ SortedSet 전환 로직 |
| `command/cmd_string*.rs` | StringInt ↔ String 전환 로직 |
| `rdb/saver.rs` | compact encoding RDB 직렬화 |
| `rdb/loader.rs` | compact encoding RDB 역직렬화 |
| `eviction.rs` | compact variant 메모리 추정 |
| `object.rs` | compact variant 접근 헬퍼 |
| `direct.rs` | compact variant DirectDb 지원 |

#### 4.5 단계별 구현 순서

1. **Week 1**: `StringInt` — 가장 간단, 영향 범위 작음
2. **Week 2**: `IntSet` — Set 명령 한정, binary search 구현
3. **Week 3-4**: `Listpack` 코어 구현 + Hash 적용
4. **Week 5**: `Listpack` Set/ZSet 적용
5. **Week 6**: CONFIG 파라미터 + RDB 호환성 + 벤치마크

#### 4.6 예상 효과

| 타입 | 시나리오 | Before/entry | After/entry | 절감률 |
|------|---------|:----:|:----:|:----:|
| String(int) | INCR counter | 32B | 8B | 75% |
| Hash(small) | 10 fields × 10B | 112B | ~30B | 73% |
| Set(int) | 100 integer members | 48B | 8B | 83% |
| ZSet(small) | 50 members × 10B | 128B | ~28B | 78% |

---

<a id="phase-5"></a>
### Phase 5: BGSAVE Snapshot 최적화

#### 5.1 현황 분석

현재 BGSAVE 플로우:

```
1. state.snapshot_dbs()
   → 각 DB read-lock → HashMap.clone() → release
   → 전체 HashMap 구조체 + StoredValue 모두 deep copy
   → Bytes는 Arc clone (cheap) 하지만 StoredValue wrapper는 full copy

2. rdb::saver::save(&snapshot, path)
   → ServerState::new(16) 생성
   → state.load_from_rdb(snapshot.clone())    ← 추가 clone!
   → RdbSaver.save_state(&state)
```

**문제**: 스냅샷 시점에 **2번의 deep clone** + 피크 메모리 2x

#### 5.2 개선 설계

##### Step 1: 이중 clone 제거 (즉시 적용 가능)

`RdbSaver.save_snapshot()` 메서드를 추가하여 `DbSnapshot`을 직접 직렬화:

```rust
// rdb/saver.rs
impl<W: Write> RdbSaver<W> {
    /// Save directly from DbSnapshot without intermediate ServerState.
    pub fn save_snapshot(mut self, snapshot: &DbSnapshot) -> io::Result<()> {
        self.write_header()?;
        self.write_aux(b"redis-ver", b"7.0.0")?;
        self.write_aux(b"ratatosk-ver", b"0.1.0")?;
        
        for (db_idx, db) in snapshot.iter().enumerate() {
            if db.is_empty() { continue; }
            
            self.write_select_db(db_idx)?;
            let expires_count = db.values()
                .filter(|v| v.expire_at_ms.is_some()).count();
            self.write_resize_db(db.len(), expires_count)?;
            
            for (key, value) in db.iter() {
                self.write_key_value(key, value)?;
            }
        }
        
        self.write_eof()
    }
}

// save() 함수 수정
pub fn save(snapshot: &DbSnapshot, path: &Path) -> io::Result<()> {
    let file = File::create(path)?;
    RdbSaver::new(file).save_snapshot(snapshot)  // clone 제거
}
```

**절감**: snapshot.clone() 1회 제거 → 피크 메모리 33% 감소

##### Step 2: Arc<StoredValue> 도입 (중기)

```rust
pub struct DbShard {
    pub data: HashMap<Bytes, Arc<StoredValue>>,
    // ...
}
```

스냅샷 시 Arc clone만 수행 (8 bytes atomic increment):

```rust
pub fn snapshot_all(&self) -> Vec<HashMap<Bytes, Arc<StoredValue>>> {
    // HashMap structure는 복제되지만 StoredValue 자체는 공유
    // 키 100만 개: HashMap 구조 ~64B/entry + Arc 8B = ~72MB
    // vs 현재: HashMap 구조 ~64B + StoredValue ~120B = ~184MB
}
```

**주의**: Arc를 도입하면 `&mut StoredValue` 직접 수정이 불가. 
`Arc::make_mut()` (COW) 또는 `Arc::get_mut()` 사용 필요.

##### Step 3: Streaming RDB Save (장기)

스냅샷을 만들지 않고 **DB를 순회하면서 직접 직렬화**:

```rust
pub fn save_streaming(data: &DataState, path: &Path) -> io::Result<()> {
    let file = File::create(path)?;
    let mut saver = RdbSaver::new(file);
    saver.write_header()?;
    
    for db_idx in 0..data.db_count() {
        let guard = data.read_db(db_idx);
        if guard.data.is_empty() { continue; }
        
        saver.write_select_db(db_idx)?;
        // ... serialize directly from read guard
        
        // Guard is held per-DB, not globally
        // 다른 DB는 동시에 쓰기 가능
    }
    
    saver.write_eof()?;
    Ok(())
}
```

**주의**: 직렬화 중 해당 DB에 read lock이 걸려 쓰기가 블록됨.
하지만 DB별로 짧게 잠그므로 전체 snapshot보다 훨씬 나음.

#### 5.3 변경 범위

| Step | 파일 | 난이도 |
|------|------|--------|
| Step 1 | `rdb/saver.rs` | 하 |
| Step 2 | `keyspace.rs`, 모든 cmd 핸들러 | 상 |
| Step 3 | `rdb/saver.rs`, `persistence/rdb.rs` | 중 |

#### 5.4 예상 효과

| Step | 피크 메모리 | 절감 |
|------|:---------:|:----:|
| 현재 | 3x (원본 + snapshot + load clone) | — |
| Step 1 | 2x (원본 + snapshot) | 33% |
| Step 2 | ~1.5x (원본 + Arc clone) | 50% |
| Step 3 | ~1.05x (read lock 중 serialize) | 95% |

---

<a id="phase-6"></a>
### Phase 6: jemalloc 도입

#### 6.1 배경

| Allocator | 장점 | 단점 |
|-----------|------|------|
| system (glibc/libmalloc) | 설정 불필요 | 단편화 심함, stats 없음 |
| mimalloc | 소규모 alloc 빠름 | 장기 단편화 미흡 |
| **jemalloc** | 단편화 최소, stats 풍부, Redis 사용 | 바이너리 크기 증가 ~1MB |

Redis가 jemalloc을 사용하는 이유:
- `malloc_stats` → 실제 RSS vs 논리 사용량 비교 가능
- `MEMORY DOCTOR` → 단편화 진단
- arena / tcache 튜닝으로 멀티스레드 최적화

#### 6.2 구현

##### Cargo.toml

```toml
[workspace.dependencies]
tikv-jemallocator = "0.6"
tikv-jemalloc-ctl = "0.6"

# ratatosk-server/Cargo.toml
[features]
default = []
mimalloc = ["dep:mimalloc", "ratatosk-engine/mimalloc"]
jemalloc = ["dep:tikv-jemallocator", "dep:tikv-jemalloc-ctl"]

[dependencies]
tikv-jemallocator = { workspace = true, optional = true }
tikv-jemalloc-ctl = { workspace = true, optional = true }
```

##### main.rs

```rust
#[cfg(feature = "jemalloc")]
use tikv_jemallocator::Jemalloc;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL_ALLOCATOR: Jemalloc = Jemalloc;
```

##### MEMORY 명령에 jemalloc stats 노출

```rust
// cmd_server_memory.rs
#[cfg(feature = "jemalloc")]
fn jemalloc_stats() -> Option<JemallocStats> {
    use tikv_jemalloc_ctl::{stats, epoch};
    epoch::advance().ok()?;
    Some(JemallocStats {
        allocated: stats::allocated::read().ok()?,
        active: stats::active::read().ok()?,
        resident: stats::resident::read().ok()?,
        mapped: stats::mapped::read().ok()?,
        retained: stats::retained::read().ok()?,
    })
}

// INFO memory 섹션에 추가:
// mem_allocator: jemalloc-5.3.0
// allocator_allocated: 12345678
// allocator_active: 13456789
// allocator_resident: 14567890
// mem_fragmentation_ratio: 1.08
```

#### 6.3 변경 범위

| 파일 | 변경 |
|------|------|
| `Cargo.toml` (workspace) | `tikv-jemallocator`, `tikv-jemalloc-ctl` 추가 |
| `ratatosk-server/Cargo.toml` | `jemalloc` feature 추가 |
| `main.rs` | `#[global_allocator]` 조건 추가 |
| `cmd_server_memory.rs` | jemalloc stats 노출 |
| `cmd_server_info.rs` | INFO memory에 allocator 정보 |
| `metrics.rs` | jemalloc gauge 노출 |

#### 6.4 예상 효과

- 장기 운영 시 **메모리 단편화 30-50% 감소**
- 정확한 메모리 사용량 측정 → eviction 정확도 향상
- `MEMORY DOCTOR` 스타일 진단 기능 제공

---

<a id="phase-7"></a>
### Phase 7: SortedSet 구조 개선

#### 7.1 현황 분석

```rust
pub struct SortedSet {
    pub by_score: BTreeMap<SortedSetEntry, ()>,  // BTreeMap node ~48B/entry
    pub by_member: HashMap<Bytes, SortedSetScore>, // HashMap entry ~64B/entry
}
// → 멤버당 총 오버헤드: ~112B + member.len() × 2 (양쪽에 Bytes clone)
```

`rank()` 연산이 **O(N)**: `by_score.range(..entry).count()`

#### 7.2 개선 설계

##### Option A: Augmented BTreeMap (rank O(log N))

표준 BTreeMap으로는 rank를 O(log N)에 구할 수 없다. 옵션:

1. **Order-statistic tree**: 각 노드에 서브트리 크기 저장
   - Rust 생태계: `order-stat-tree` crate (불안정)
   - 직접 구현 필요 → 높은 복잡도

2. **Fenwick tree / BIT**: score를 양자화하여 prefix sum
   - score가 연속 정수가 아니면 좌표 압축 필요

##### Option B: Skip List (Redis 방식, 추천)

```rust
pub struct SkipList {
    head: Box<SkipNode>,
    tail: *mut SkipNode,
    length: usize,
    level: usize,
}

struct SkipNode {
    member: Bytes,
    score: f64,
    backward: *mut SkipNode,
    levels: Vec<SkipLevel>,
}

struct SkipLevel {
    forward: *mut SkipNode,
    span: usize,  // ← rank 계산에 사용
}
```

- `rank()` = O(log N) — span 합산
- `insert()`/`remove()` = O(log N)
- `range_by_score()` = O(log N + M)
- 메모리: 노드당 ~64B (평균 level 2) vs BTreeMap ~48B + HashMap 64B = 112B

**결론**: 단일 인덱스로 O(log N) rank + score lookup → **메모리 50%+ 절감 + CPU 절감**

#### 7.3 구현 계획

1. `crates/ratatosk-engine/src/skiplist.rs` 신규 모듈
2. Redis `t_zset.c`의 skiplist 구현 참조 (BSD 라이선스)
3. `unsafe` 필요 (raw pointer forward/backward) — 철저한 테스트 + Miri
4. `SortedSet`을 내부적으로 skiplist로 교체하되 공개 API는 유지

#### 7.4 변경 범위

| 파일 | 변경 |
|------|------|
| `skiplist.rs` (신규) | ~500 lines, unsafe |
| `keyspace.rs` | `SortedSet` 내부 구조 교체 |
| `command/cmd_sorted_set*.rs` | 대부분 API 호환으로 변경 최소 |
| `rdb/saver.rs`, `rdb/loader.rs` | 직렬화/역직렬화 |

#### 7.5 난이도 및 위험

- **난이도**: 상 (unsafe + concurrent correctness)
- **위험**: 메모리 안전성 — Miri + AddressSanitizer 필수
- **대안**: Phase 4의 Listpack으로 소규모 ZSet 커버 후, 대규모는 현재 구조 유지

---

<a id="phase-8"></a>
### Phase 8: 소규모 Hot Path 최적화 모음

#### 8.1 cmd_append: 불필요한 clone 제거

```rust
// Before (cmd_string.rs:29)
let (value, expire_at_ms) = if let Some(existing) = db.get(key) {
    (s.clone(), existing.expire_at_ms)   // ← Bytes clone
};

// After: entry API로 in-place 수정
use hashbrown::hash_map::Entry;
let mut db = server.db_mut(client.selected_db);
match db.entry(key.clone()) {
    Entry::Occupied(mut occ) => {
        let entry = occ.get_mut();
        if let ValueData::String(ref mut s) = entry.data {
            let mut new = Vec::with_capacity(s.len() + append.len());
            new.extend_from_slice(s);
            new.extend_from_slice(append);
            *s = Bytes::from(new);
            return CommandOutcome::reply(RespFrame::Integer(s.len() as i64));
        }
        wrong_type_response()
    }
    Entry::Vacant(vac) => {
        vac.insert(StoredValue::string(Bytes::copy_from_slice(append), None));
        CommandOutcome::reply(RespFrame::Integer(append.len() as i64))
    }
}
```

#### 8.2 format_f64_for_redis: ryu crate 사용

```rust
// Before (object.rs)
pub fn format_f64_for_redis(value: f64) -> Bytes {
    Bytes::from(value.to_string())  // heap allocation
}

// After: ryu로 stack buffer 사용
pub fn format_f64_for_redis(value: f64) -> Bytes {
    let mut buf = ryu::Buffer::new();
    let s = buf.format(value);
    Bytes::copy_from_slice(s.as_bytes())  // 여전히 allocation이지만 formatting이 ~3x 빠름
}
```

#### 8.3 purge_expired_key 중복 호출 최소화

현재 221회 호출. 대부분 동일한 패턴:

```rust
let now = now_ms();
let mut db = server.db_mut(client.selected_db);
purge_expired_key(&mut db, key, now);  // ← 매번 호출
```

개선: `get_or_purge()` 통합 메서드:

```rust
/// Get a key, purging it if expired. Returns None if not found or expired.
fn get_valid<'a>(
    db: &'a HashMap<Bytes, StoredValue>, 
    key: &Bytes, 
    now_ms: i64
) -> Option<&'a StoredValue> {
    let entry = db.get(key)?;
    if entry.expire_at_ms.is_some_and(|at| at <= now_ms) {
        None  // expired — caller should remove
    } else {
        Some(entry)
    }
}
```

#### 8.4 Lazy DB initialization

```rust
// Before: 16개 DB 모두 즉시 할당
pub fn new(db_count: usize) -> Self {
    let mut shards = Vec::with_capacity(db_count);
    for _ in 0..db_count {
        shards.push(parking_lot::RwLock::new(DbShard::default()));
    }
}

// After: DbShard 자체는 default (empty HashMap)이므로 할당 비용이 이미 미미
// → 변경 불필요. hashbrown::HashMap::default()는 할당 없음.
// 결론: 이 항목은 생략 (이미 최적)
```

#### 8.5 SortedSet::rank() 임시 개선

skiplist 도입 전 임시 개선:

```rust
// Before: O(N) — by_score.range(..entry).count()
pub fn rank(&self, member: &Bytes) -> Option<usize> {
    let score = self.by_member.get(member)?;
    let entry = SortedSetEntry { score: *score, member: member.clone() };
    Some(self.by_score.range(..&entry).count())
}

// 임시 개선: 변경 없음 (BTreeMap에서 O(log N) rank는 불가능)
// → Phase 7 (skiplist)에서 근본 해결
```

#### 8.6 RDB save 이중 clone 제거 (Phase 5 Step 1)

이것은 Phase 5에서 다루지만 독립 실행 가능하므로 여기에도 포함:

```rust
// Before
pub fn save(snapshot: &DbSnapshot, path: &Path) -> io::Result<()> {
    let state = ServerState::new(snapshot.len());
    state.load_from_rdb(snapshot.clone());  // ← 불필요한 clone
    RdbSaver::new(file).save_state(&state)
}

// After
pub fn save(snapshot: &DbSnapshot, path: &Path) -> io::Result<()> {
    let file = File::create(path)?;
    RdbSaver::new(file).save_snapshot(snapshot)  // 직접 직렬화
}
```

---

<a id="verification"></a>
### 검증 전략

#### 각 Phase별 검증

| Phase | 단위 테스트 | 통합 테스트 | 벤치마크 |
|-------|:---------:|:---------:|:-------:|
| 1 StoredValue | size_of 어설션 + 기존 전체 테스트 | redis-cli 스모크 | pipeline bench |
| 2 Mem Tracking | counter 정확성 + drift 테스트 | eviction 동작 검증 | eviction CPU profile |
| 3 Expires Index | invariant 검증 (data⇄expires 동기) | TTL/PERSIST/EXPIRE 통합 | active expiry bench |
| 4 Compact Encoding | 인코딩 왕복 + threshold 전환 | RDB save/load 호환 | memory benchmark |
| 5 BGSAVE | RDB 무결성 + CRC | BGSAVE + concurrent write | peak memory profile |
| 6 jemalloc | feature flag 컴파일 | INFO memory 확인 | fragmentation bench |
| 7 SkipList | 정합성 + Miri | ZADD/ZRANK/ZRANGE 통합 | rank O(log N) bench |
| 8 Hot Path | 기존 테스트 통과 | 회귀 없음 확인 | micro benchmark |

#### 불변 조건 (모든 Phase)

```bash
# 모든 Phase 완료 후 반드시 통과:
cargo fmt --all --check
cargo check --workspace --quiet
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --quiet
```

#### 메모리 프로파일링

```bash
# 1. 기준선 측정
cargo build --release
./target/release/ratatosk &
redis-benchmark -p 6379 -t set -n 1000000 -d 100
redis-cli -p 6379 INFO memory > baseline_memory.txt

# 2. 각 Phase 후 동일 측정
# 3. 비교
diff baseline_memory.txt phase_N_memory.txt
```

---

<a id="schedule"></a>
### 의존성 그래프 및 일정

```
Phase 1 (StoredValue)  ──────┐
                              ├──→ Phase 4 (Compact Encoding)
Phase 3 (Expires Index) ──────┤        ↓
                              ├──→ Phase 7 (SkipList)
Phase 2 (Mem Tracking) ───────┘

Phase 5 Step 1 (RDB clone) ── 독립 실행 가능
Phase 6 (jemalloc)         ── 독립 실행 가능
Phase 8 (Hot Path)         ── 독립 실행 가능 (일부 Phase 1 이후)
```

#### 추천 실행 순서 및 일정

| 순서 | Phase | 예상 기간 | 난이도 | 선행 조건 | RAM 절감 | CPU 절감 |
|:----:|-------|:--------:|:------:|:---------:|:--------:|:--------:|
| **1** | 8.6 RDB clone 제거 | 0.5일 | 하 | 없음 | ⭐⭐ | ⭐ |
| **2** | 6 jemalloc 도입 | 1일 | 하 | 없음 | ⭐⭐⭐ | ⭐ |
| **3** | 1 StoredValue 압축 | 3일 | 중 | 없음 | ⭐⭐⭐⭐⭐ | ⭐ |
| **4** | 2 Mem Tracking | 2일 | 중 | Phase 1 | ⭐ | ⭐⭐⭐⭐⭐ |
| **5** | 3 Expires Index | 2일 | 중 | 없음 | ⭐⭐ | ⭐⭐⭐⭐ |
| **6** | 8 Hot Path 모음 | 1일 | 하 | Phase 1 | ⭐ | ⭐⭐ |
| **7** | 4a StringInt | 1일 | 하 | Phase 1 | ⭐⭐⭐ | ⭐ |
| **8** | 4b IntSet | 2일 | 중 | Phase 1 | ⭐⭐⭐ | ⭐ |
| **9** | 4c Listpack | 5일 | 상 | Phase 1 | ⭐⭐⭐⭐⭐ | ⭐⭐ |
| **10** | 5 BGSAVE 최적화 | 3일 | 중상 | Phase 1 | ⭐⭐⭐⭐ | ⭐⭐ |
| **11** | 7 SkipList | 5일 | 상 | Phase 4c | ⭐⭐⭐ | ⭐⭐⭐ |

**총 예상**: ~25일 (풀타임 기준)
**Quick wins (1주일 내)**: Phase 8.6 + 6 + 1 = RDB clone 제거 + jemalloc + StoredValue 압축

#### 전체 예상 절감 (1M 키 기준)

| 항목 | RAM 절감 | CPU 절감 |
|------|:--------:|:--------:|
| StoredValue 압축 | ~84 MB | — |
| Compact encoding (전체) | ~200-400 MB | 소폭 |
| Incremental mem tracking | — | eviction 99%↓ |
| Expires index | ~소폭 증가 | expiry 90%↓ |
| BGSAVE 최적화 | 피크 50-95%↓ | — |
| jemalloc | 단편화 30-50%↓ | — |
| SkipList | ~30% (ZSet) | rank O(N)→O(log N) |
| **합계** | **수백 MB** | **대폭** |

---

## Part 3: Optimization Execution Checklist

> 작성일: 2026-04-05 · 체크 상태는 2026-09-08에 현재 코드와 대조해 갱신했다. 수치는 각 Phase 실행 시점의 기록이다.
> 기반: Part 2 (RAM/CPU Optimization Master Plan) + 실측 검토 결과
> 불변 규칙: **모든 Phase 완료 시 zero-warning 상태 유지**

### 실측 기준 데이터

> 아래 수치는 **Phase 1 적용 이전** 기준선이다. Phase 1 반영 후 현재 `StoredValue`는 24 bytes다 (`Box<ValueData>` + packed `encoding_and_lru: u32`).

```
StoredValue  = 96 bytes   (계획 추정 120B → 실측 96B)
ValueData    = 72 bytes   (계획 추정 96B → 실측 72B)
Box<VD>      = 8 bytes
Encoding     = 1 byte
HashFieldEntry = 48 bytes
SortedSet    = 64 bytes
SortedSetEntry = 40 bytes
Bytes        = 32 bytes
Option<i64>  = 16 bytes
hashbrown HashMap shell = 40 bytes
```

---

### Phase 0: RDB Save 이중 Clone 제거 (0.5일)

> **위험도 하, 독립 실행, 즉시 효과**
> BGSAVE 시 `saver::save()`가 `snapshot.clone()` + `ServerState::new()` +
> `load_from_rdb()`를 수행하는 불필요한 이중 복사를 제거.
> 1M 키 기준 ~130MB 불필요 복사 제거.

#### 사전 조건
- [x] `cargo test --workspace` 전체 통과 확인 (baseline)
- [x] BGSAVE 통합 테스트 존재 여부 확인 (`persistence/rdb.rs` 테스트)

#### 구현
- [x] `crates/ratatosk-persist/src/rdb/saver.rs`에 `save_snapshot(&DbSnapshot)` 메서드 추가
  - `write_preamble()` + `write_db()` private helper로 분리
  - `for (db_idx, db) in snapshot.iter().enumerate()` 직접 순회
  - `state.db(idx)` 대신 `&snapshot[idx]` 사용
  - `write_eof()` + CRC 기록 동일
- [x] 기존 `save()` 함수를 `save_snapshot()` 호출로 교체
  - `ServerState::new()` 제거
  - `snapshot.clone()` 제거
  - `load_from_rdb()` 제거
- [x] `save_state()` 공개 API는 유지 (테스트에서 사용 중) — `write_preamble()+write_db()` 재사용
- [x] `crates/ratatosk-server/src/persistence/aof.rs`의 `save()` 호출도 동일하게 확인
  - 현재는 `rdb::saver::save_atomic(snapshot, &base_path)`로 AOF BASE 스냅샷을 쓴다

#### 검증
- [x] `cargo test -p ratatosk-persist` — RDB saver 테스트 전체 통과
- [x] `cargo test -p ratatosk-server` — persistence 통합 테스트 통과
- [x] `cargo clippy --workspace --all-targets -- -D warnings` 통과
- [x] RDB 파일 무결성: 기존 save_state 테스트 4개 + save→load 왕복 모두 통과

#### 검토 노트
- `save_state()`는 테스트 코드에서 직접 사용하므로 삭제하지 않는다.
- `save_snapshot()`과 `save_state()` 간 직렬화 로직 중복을 
  private helper로 통합할 수 있지만, 이 Phase에서는 범위를 제한한다.

---

### Phase 1: StoredValue 메모리 레이아웃 압축 (3일)

> **위험도 중, 최대 단일 RAM 절감**
> 96B → 24B = **키당 72B 절감**. 1M 키 기준 ~69MB.
> `data` 필드를 `Box<ValueData>`로, `expire_at_ms`를 i64 sentinel로, 
> `encoding`+`lru_clock`을 u32 비트 패킹.

#### Step 1-A/B: 레이아웃 교체 + 접근자 전환 (통합 완료)

> 필드 private 전환 + Box<ValueData> + i64 sentinel + packed u32 을 한 번에 수행.

- [x] `StoredValue` 필드 private + `Box<ValueData>` + i64 sentinel + packed u32 적용:
  ```rust
  pub fn expire_at_ms(&self) -> Option<i64> { if self.expire_at_ms > 0 { Some(self.expire_at_ms) } else { None } }
  pub fn set_expire_at_ms(&mut self, ms: Option<i64>) { self.expire_at_ms = ms.unwrap_or(0); }
  pub fn encoding(&self) -> Encoding { Encoding::from_u8((self.encoding_and_lru >> LRU_BITS) as u8) }
  pub fn set_encoding(&mut self, enc: Encoding) { self.encoding_and_lru = ((enc as u32) << LRU_BITS) | (self.encoding_and_lru & LRU_MASK); }
  pub fn lru_clock(&self) -> u32 { self.encoding_and_lru & LRU_MASK }
  pub fn set_lru_clock(&mut self, clock: u32) { self.encoding_and_lru = (self.encoding_and_lru & !LRU_MASK) | (clock & LRU_MASK); }
  ```
- [x] 전체 소스 `.expire_at_ms` 직접 접근 → `.expire_at_ms()` / `.set_expire_at_ms()` 전환 (86건)
- [x] `.lru_clock` → `.lru_clock()` / `.set_lru_clock()` 전환 (3건)
- [x] `.data` → `.data()` / `.data_mut()` 전환 (6건)
- [x] `embedded.rs`는 encoding별 생성을 위해 `StoredValueExt::from_data()`를 유지
- [x] `cargo test --workspace` 통과
- [x] `cargo clippy --workspace --all-targets -- -D warnings` 통과

#### 최종 레이아웃 (24 bytes, 실측 확인):
  ```rust
  pub struct StoredValue {
      data: Box<ValueData>,    // 8B
      expire_at_ms: i64,       // 8B (0 = no expiry)
      encoding_and_lru: u32,   // 4B (4bit encoding + 28bit lru)
      // 정렬 패딩 4B → 총 24B (명시적 _pad 필드 없음)
  }
  ```
- [x] `Encoding::from_u8()` 추가 (4-bit packed field 복원)
- [x] 생성자 `new()` + `string/hash/list/set/sorted_set/stream` 모두 `Box::new` 사용
- [x] `is_*()` → `matches!(*self.data, ...)` (명시적 deref 필요 확인)
- [x] `as_*()` / `as_*_mut()` → `&*self.data` / `&mut *self.data` match 패턴
- [x] `should_lazy_free()` → `value.data()` 사용
- [x] `estimate_object_memory()` — `size_of::<StoredValue>()` 자동 반영 (96→24)
- [x] Box clone 비용 수용 확인 (cache friendliness net positive)

#### 검증
- [x] `cargo test --workspace` 전체 통과
- [x] `cargo clippy --workspace --all-targets -- -D warnings` 통과
- [x] `cargo fmt --all --check` 통과
- [x] `size_of::<StoredValue>()` == 24 확인 테스트 추가
- [x] `size_of::<ValueData>()` == 72 불변 확인 테스트 추가
- [x] Encoding roundtrip 테스트 추가
- [x] expire sentinel roundtrip 테스트 추가
- [x] LRU clock packing 테스트 추가
- [x] RDB save → load 왕복 — 기존 48개 persist 테스트 통과

#### 검토 노트
- **Box 역참조 비용**: L1 cache miss 가능성이 있으나, StoredValue가 작아져 
  HashMap bucket당 더 많은 entry가 cache line에 적재됨 (net positive).
- **matches! 매크로와 Box**: `Box<T>` 자체가 `Deref<Target=T>`이므로 
  `matches!(self.data, ValueData::String(_))`는 자동으로 `matches!(*self.data, ...)`로 
  동작한다. 하지만 `pub data: Box<ValueData>`가 아니라 private이면 외부에서 
  `match &*value.data`가 필요할 수 있다 → 기존 `as_*()` 메서드로 접근하므로 문제 없음.

---

### Phase 2: Incremental Memory Tracking (2일)

> **위험도 중, eviction CPU 99% 절감**
> `estimate_used_memory()` O(N) 전체 순회를 O(1) atomic read로 교체.
> `perform_eviction()` 내부에서 최대 128회 O(N) → 128회 O(1).

#### 사전 조건
- [x] Phase 1 완료 (StoredValue 96B→24B, estimate에 자동 반영)

#### Step 2-A: Per-DB Atomic Counter 추가

- [x] `keyspace.rs`의 `DataState`에 `db_memory_bytes: Arc<[AtomicUsize]>` 추가
- [x] `DataState::new(db_count)` — 0으로 초기화
- [x] `DataState::estimated_memory() -> usize` — 전 DB 합산 O(1)
- [x] `DataState::add_memory()` / `sub_memory()` (saturating) / `reset_memory()` 추가

#### Step 2-B: Insert/Remove 추적 함수

> **접근 방식**: `DbShard`에 tracked helper 메서드를 추가하되, 기존 HashMap 직접 접근도 유지.
> 새 코드에서는 tracked helper를 사용하고, 기존 코드는 점진적으로 전환.
> 전환 완료 전까지는 주기적 full-scan 보정이 정확성을 보장.

- [x] 추적은 `DbWriteGuard::insert` / `DbWriteGuard::remove` (`keyspace.rs`)가 담당한다. 별도의 `tracked_*` 메서드는 두지 않았다.
- [x] `eviction.rs`의 `estimate_object_memory()` 재활용
- [x] `perform_eviction()`은 `state.db_mut(db_idx).remove(&key)`로 제거하며 accounting은 guard 안에서 일어난다

#### Step 2-C: perform_eviction 수정

- [x] `memory_before`/loop `used`/`memory_after` → `state.data.estimated_memory()` O(1)
- [x] eviction remove는 `DbWriteGuard::remove()`를 거치므로 accounting이 자동으로 반영된다
- [x] `estimate_used_memory()` 유지 (주기적 full-scan 보정용)

#### Step 2-D: 주기적 보정

- [x] `event_loop.rs` `server_cron`: MEMORY_ESTIMATE_INTERVAL마다 full-scan
- [x] tracked vs full-scan 5% drift 초과 시 per-DB `reset_memory()` 호출
- [x] RDB `load_from_snapshot()`: memory + expires 재구축
- [x] `clear_db()` / `clear_all_dbs()`: `reset_memory(idx, 0)` + `expires.clear()`

#### 검증
- [x] `cargo test --workspace` 통과
- [x] `cargo clippy --workspace --all-targets -- -D warnings` 통과
- [x] eviction 테스트 통과
- [x] `cargo fmt --all --check` 통과

#### 검토 노트
- **get_mut() 76건은 추적 불가**: in-place 수정(e.g. list push, hash field add)은
  메모리 변동을 추적하기 어렵다. 이는 주기적 full-scan 보정으로 커버.
- **드리프트 허용 범위**: eviction 판단은 근사치로 충분 (Redis도 sampling 기반).
  5% 드리프트는 실질적 문제 없음.
- **AtomicUsize Ordering**: `Relaxed`로 충분 — eviction은 best-effort.

---

### Phase 3: Expires Index 분리 (2일)

> **위험도 중, active expiry CPU 90% 절감**
> 별도 `expires: HashMap<Bytes, i64>` 사이드맵으로 volatile 키만 O(1) 카운트/접근.

#### 사전 조건
- [x] Phase 1 완료 (expire_at_ms 접근자 안정화)

#### Step 3-A: DbShard 확장

- [x] `keyspace.rs`의 `DbShard`에 `pub expires: HashMap<Bytes, i64>` 추가
- [x] `DbShard::default()` — derive Default로 자동 초기화
- [x] `Clone` derive로 `expires` 포함 확인

#### Step 3-B: 동기화 Helper

- [x] `DbShard::sync_expires(&mut self, key, expire_at_ms)` 추가
- [x] `DbShard::volatile_count()` O(1) 추가:
  ```rust
  pub fn sync_expires(&mut self, key: &Bytes, expire_at_ms: Option<i64>) {
      match expire_at_ms {
          Some(ms) if ms > 0 => { self.expires.insert(key.clone(), ms); }
          _ => { self.expires.remove(key); }
      }
  }
  ```
- [x] 만료 키 purge 경로에서 `expires` 동기화
- [x] DB clear 경로에서 `expires` 정리
- [x] `swap_dbs()` — DbShard 전체 swap이므로 `expires`도 함께 이동

#### Step 3-C: 커맨드 핸들러 동기화 지점

> 키 레벨 TTL 변경은 `DbWriteGuard::insert()`(삽입 시 동기화)와 `DbWriteGuard::set_key_expiry()`를 통해서만 이루어진다.

- [x] PERSIST / EXPIRE / PEXPIRE / RESTORE / `direct.rs`의 TTL 변경은 `DbWriteGuard::set_key_expiry()`를 사용한다
- [x] **insert() 경로**: `DbWriteGuard::insert()`가 `StoredValue`의 expire 포함 여부에 따라 `expires`를 자동 동기화

#### Step 3-D: Active Expiry 수정

- [x] `expiry.rs` 완전 재작성: `shard.expires` 에서 직접 sampling
- [x] `volatile_count` = `shard.expires.len()` O(1)
- [x] reservoir sampling on `shard.expires.iter()` (volatile-only)
- [x] expired 키 삭제 시 `shard.data.remove()` + `shard.expires.remove()` 동시 수행

#### Step 3-E: Eviction 수정

- [x] `eviction.rs`:
  - `volatile-*` 정책: `shard.expires`에서 직접 샘플링
  - `allkeys-*` 정책: 기존대로 `shard.data`에서 샘플링

#### Step 3-F: RDB/Startup 시 Expires 구축

- [x] `DataState::load_from_snapshot()`: snapshot load 후 각 DB expires 재구축
- [x] AOF recovery는 명령 재실행이므로 핸들러 레벨에서 동기화 — 후속 점진 적용

#### 검증
- [x] `cargo test --workspace` 통과
- [x] `cargo clippy --workspace --all-targets -- -D warnings` 통과
- [x] expiry 관련 검증 통과 (guard insert가 expires를 자동 동기화)
  - SET key val EX 60 → `shard.expires.contains_key(key)` == true
  - PERSIST key → `shard.expires.contains_key(key)` == false
  - DEL key → `shard.expires.contains_key(key)` == false
  - EXPIRE → `shard.expires[key]` == expected_ms
- [x] `active_expire_cycle_removes_expired_keys` — expires index 기반 샘플링 검증
- [x] `expiry.rs` 테스트 3개 통과
- [ ] `#[cfg(test)] fn assert_expires_invariant(shard: &DbShard)` — 
  data 내 expire와 expires index가 일치하는지 검증하는 불변식 체크 함수 추가
- [ ] RDB save → load 후 expires index 재구축 검증

#### 검토 노트
- **메모리 비용**: volatile 키당 +80B. 10% volatile × 1M 키 = +7.6MB.
  Active expiry CPU 절감 (10x) 대비 미미.
- **cmd_hash_ttl.rs의 expire_at_ms**: 이것은 **HashFieldEntry**의 TTL이며
  **키 레벨** TTL이 아님. expires index 대상이 아니므로 무시.
- **동기화 누락 위험**: `assert_expires_invariant()` 테스트 함수를 
  주요 명령 테스트 끝에 호출하여 누락 감지.

---

### Phase 4A: String Int Encoding (1일)

> **위험도 하, 독립 실행 가능**
> i64 파싱 가능 문자열을 `ValueData::StringInt(i64)`로 저장.
> 정수 키당 24B 절감 (Bytes 32B → i64 8B, Box 내부).

#### 사전 조건
- [x] Phase 1 완료 (ValueData가 Box 안에 있어 enum size 영향 없음 확인)

#### 구현
- [x] `keyspace.rs`의 `ValueData`에 `StringInt(i64)` variant 추가
- [ ] `StoredValue::is_string()` — `StringInt`도 true 반환
- [ ] `StoredValue::as_string()` — `StringInt(n)` → lazy `Bytes` 변환? 
  - **결정 필요**: 매번 `itoa` 변환 vs 캐싱
  - **추천**: `as_string_or_int()` → `Either<&Bytes, i64>` 반환
  - 또는: 기존 `as_string()`은 `String` variant만, 새 `as_int_value()` 추가
- [ ] `StoredValue::string()` 생성자에서 i64 파싱 시도:
  ```rust
  pub fn string(value: Bytes, expire_at_ms: Option<i64>) -> Self {
      if let Ok(s) = std::str::from_utf8(&value) {
          if let Ok(n) = s.parse::<i64>() {
              return Self::int(n, expire_at_ms);
          }
      }
      Self { data: Box::new(ValueData::String(value)), ... }
  }
  ```
- [ ] `type_name()` — `StringInt` → "string"
- [ ] `Encoding` — `StringInt` → `Encoding::Int`

#### 영향받는 커맨드 핸들러
- [ ] `cmd_string_numeric.rs`: INCR/DECR — `StringInt(n)` 직접 연산 (parse 불필요)
- [ ] `cmd_string.rs`: APPEND — `StringInt` → `String`으로 전환 후 append
- [ ] `cmd_string.rs`: STRLEN — `itoa::Buffer` 길이 계산
- [ ] `cmd_string.rs`: GETRANGE — `itoa` 변환 후 slice
- [ ] `cmd_string_access.rs`: GET/GETEX — `StringInt` → bulk string 응답
- [ ] `cmd_generic_string.rs`: SET — 생성자에서 자동 감지
- [ ] `readonly_batch.rs`: GET fast path — `StringInt` 처리

#### 검증
- [ ] `cargo test --workspace` 통과
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` 통과
- [x] `StoredValue::string()` 에서 i64 자동 감지 (leading zero/+/- 거부)
- [x] `as_string_bytes()` 추가: String(Bytes clone) + StringInt(itoa materialise)
- [x] `as_int()` 추가: StringInt에서 직접 i64 반환
- [x] `is_string()` / `type_name()` — StringInt도 "string" 으로 처리
- [x] INCR/DECR: `as_int()` fast path + `string_int()` 생성자 사용
- [x] INCRBYFLOAT: `as_int()` fast path
- [x] GET/MGET/GETEX/GETDEL/GETSET: `as_string_bytes()` 사용
- [x] RDB saver/embedded: StringInt → itoa 직렬화
- [x] `should_lazy_free` / `estimate_object_memory` / `cmd_server_memory` 모두 업데이트
- [x] RDB save/load 왕복 48개 테스트 통과
- [x] AOF replay 테스트 통과
- [x] `cargo test --workspace` 전체 통과
- [x] `cargo clippy --workspace --all-targets -- -D warnings` 통과

#### 검토 노트
- **ValueData enum 크기**: `StringInt(i64)` = 8B < 현재 최대 72B → enum 크기 불변 (`keyspace.rs`의 size 테스트가 72B를 고정)
- **OBJECT ENCODING 응답**: Redis는 정수 문자열에 "int" 반환 → `Encoding::Int` 사용
- **edge case**: 빈 문자열 "", "+0", "-0", leading zeros "007" → String으로 유지
  (Redis 동작과 일치)

---

### Phase 4B: IntSet (정수 Set) (2일)

> **위험도 중하, Set 명령 한정**
> 모든 멤버가 i64인 소규모 Set을 `Vec<i64>` (정렬, binary search)로 저장.
> 멤버당 48B → 8B = 83% 절감.

#### 사전 조건
- [ ] Phase 1 완료

#### 구현
- [x] `keyspace.rs`에 `ValueData::SetInt(Vec<i64>)` variant 추가
- [ ] CONFIG 파라미터: `set-max-intset-entries` (기본 512) → `config.rs`에 추가
- [ ] IntSet helper 메서드:
  ```rust
  fn intset_contains(set: &[i64], value: i64) -> bool { set.binary_search(&value).is_ok() }
  fn intset_insert(set: &mut Vec<i64>, value: i64) -> bool { /* sorted insert */ }
  fn intset_remove(set: &mut Vec<i64>, value: i64) -> bool { /* sorted remove */ }
  fn intset_to_hashset(set: &[i64]) -> HashSet<Bytes> { /* upgrade */ }
  ```
- [ ] 업그레이드 조건: 멤버 수 > threshold **또는** 비-정수 멤버 추가 시
- [ ] `StoredValue::set()` 생성자: 모든 멤버가 i64이고 threshold 이내면 `SetInt`

#### 영향받는 커맨드
- [ ] `cmd_set.rs`: SADD — IntSet insert 또는 upgrade
- [ ] `cmd_set.rs`: SREM — IntSet remove
- [ ] `cmd_set.rs`: SISMEMBER/SMISMEMBER — binary search
- [ ] `cmd_set.rs`: SCARD — `vec.len()`
- [ ] `cmd_set.rs`: SMEMBERS/SRANDMEMBER — iterate
- [ ] `cmd_set.rs`: SPOP — random index remove
- [ ] `cmd_set_algebra.rs`: SUNION/SINTER/SDIFF — IntSet 간 연산 또는 upgrade

#### 검증
- [ ] `cargo test --workspace` 통과
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` 통과
- [ ] 신규 테스트: SADD 100개 정수 → SCARD 100 → SISMEMBER 확인
- [ ] 신규 테스트: SADD "not-int" → HashSet으로 업그레이드
- [ ] 신규 테스트: SADD 513개 (threshold 초과) → 업그레이드
- [ ] 신규 테스트: SUNION IntSet + IntSet → IntSet
- [ ] 신규 테스트: SUNION IntSet + HashSet → HashSet
- [ ] RDB save/load 왕복
- [ ] OBJECT ENCODING 응답: "intset"

---

### Phase 4C: Listpack (5일)

> **위험도 상, 가장 큰 구현 범위, 가장 큰 메모리 절감**
> 소규모 Hash/Set/ZSet을 연속 바이트 배열로 인코딩.
> 소규모 컬렉션 2.9~4.9x 메모리 절감.

#### 사전 조건
- [ ] Phase 1 완료
- [ ] Phase 4A 완료 (ValueData variant 추가 패턴 확립)

#### Step 4C-1: Listpack 코어 (2일)

- [ ] `crates/ratatosk-engine/src/listpack.rs` 신규 모듈 생성
- [ ] 인코딩 포맷 설계 (Redis listpack 간소화):
  - 헤더: total_bytes(u32) + num_elements(u16) + end_marker(0xFF)
  - entry: type_byte(u8) + data + backlen(1-5 bytes)
  - type: 7-bit int, 13-bit int, i16/i32/i64, string(len-prefixed)
- [ ] 핵심 연산 구현:
  - `Listpack::new() -> Self`
  - `Listpack::push(entry: &[u8])`
  - `Listpack::push_int(value: i64)`
  - `Listpack::get(index: usize) -> Option<ListpackEntry>`
  - `Listpack::find(key: &[u8]) -> Option<ListpackEntry>` (key-value pair용)
  - `Listpack::remove(index: usize)`
  - `Listpack::len() -> usize`
  - `Listpack::iter() -> ListpackIter`
- [ ] `ListpackEntry` enum: `Int(i64)` | `Str(&[u8])`
- [ ] 단위 테스트: push/get/find/remove/iter 왕복

#### Step 4C-2: Hash Listpack 적용 (1일)

- [ ] `ValueData::HashPack(Listpack)` variant 추가
- [ ] CONFIG: `hash-max-listpack-entries` (128), `hash-max-listpack-value` (64)
- [ ] Hash 생성 시 compact 여부 판단 → `HashPack` 또는 `Hash`
- [ ] `cmd_hash.rs`: HSET/HMSET — listpack push pair 또는 upgrade
- [ ] `cmd_hash_read.rs`: HGET/HGETALL/HKEYS/HVALS — listpack scan
- [ ] `cmd_hash.rs`: HDEL — listpack remove pair
- [ ] 업그레이드: entries > threshold 또는 value > max_value → `Hash`로 전환

#### Step 4C-3: Set/ZSet Listpack 적용 (2일)

- [ ] `ValueData::SetPack(Listpack)` variant 추가
- [ ] `ValueData::SortedSetPack(Listpack)` variant 추가
- [ ] Set: SADD → listpack push (비-정수, 소규모)
- [ ] ZSet: ZADD → listpack push(member, score) pair
  - 정렬 유지: score 순 삽입
  - ZRANGE/ZRANGEBYSCORE — linear scan on listpack (소규모이므로 OK)
  - ZRANK — linear scan count
- [ ] 업그레이드 조건 동일

#### 검증
- [ ] `cargo test --workspace` 통과
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` 통과
- [ ] Listpack 단위 테스트: encoding/decoding 왕복, edge case (빈 string, i64::MAX 등)
- [ ] 각 타입별 compact ↔ full 전환 테스트
- [ ] RDB save/load: compact encoding 직렬화 호환
- [ ] OBJECT ENCODING 응답: "listpack" / "hashtable" / "skiplist"
- [ ] 메모리 벤치마크: 소규모 hash 1만개 Before/After 비교

#### 검토 노트
- **Listpack 선형 탐색**: O(N)이지만 N <= 128이므로 cache-friendly linear scan이 
  hash lookup보다 빠를 수 있음 (L1 cache line = 64B에 여러 entry 적재).
- **backlen 필요성**: 역방향 순회 (LRANGE -1 -1 등)에 필요. 
  순방향만 필요하면 생략 가능 → 구현 간소화 옵션.
- **unsafe 불필요**: `Vec<u8>` 기반이므로 safe Rust로 구현 가능.

---

### Phase 5: BGSAVE Snapshot 개선 (3일)

> **위험도 중상, 피크 메모리 대폭 절감**
> Step 1: Phase 0에서 완료. Step 2: Arc<StoredValue> 도입.

#### 사전 조건
- [ ] Phase 0 완료 (이중 clone 제거)
- [ ] Phase 1 완료 (StoredValue 크기 최적화)

#### Step 5-A: snapshot_all() 최적화 검토

- [ ] 현재 `guard.data.clone()` → HashMap + StoredValue 전체 deep copy
- [ ] Bytes는 Arc clone (cheap) 하지만 `Box<ValueData>`는 heap alloc
- [ ] 선택지 평가:
  - **A. Arc<StoredValue>**: HashMap<Bytes, Arc<StoredValue>> → snapshot은 Arc clone만
  - **B. Streaming save**: snapshot 없이 DB read-lock 중 직접 직렬화
  - **C. 현상 유지**: Phase 0의 1회 clone 제거로 충분한지 평가

#### Step 5-B: (선택) Arc<StoredValue> 도입

- [ ] `DbShard::data` 타입 변경: `HashMap<Bytes, StoredValue>` → `HashMap<Bytes, Arc<StoredValue>>`
- [ ] **영향 범위 거대**: 모든 `db.get()`, `db.get_mut()`, `db.insert()` 패턴 변경
  - `db.get(key)` → `Arc<StoredValue>` 반환 → `.as_string()` 등 동일
  - `db.get_mut(key)` → **불가** → `Arc::make_mut()` 사용 (COW)
  - `db.insert(key, StoredValue::...)` → `db.insert(key, Arc::new(StoredValue::...))`
- [ ] `get_mut` 76건 + `insert` 99건 + `entry` 7건 = **182건 변경**
- [ ] **결정**: 비용 대비 효과가 Phase 4 (compact encoding)보다 낮을 수 있음
  → Phase 4 완료 후 재평가

#### Step 5-C: (대안) Streaming RDB Save

- [ ] `RdbSaver::save_streaming(data: &DataState)` 구현
  - DB별 read-lock → 직렬화 → release → 다음 DB
  - snapshot clone 완전 제거
- [ ] **주의**: 직렬화 중 해당 DB write 차단 → 대규모 DB에서 latency spike
- [ ] `BGSAVE` 시 `spawn_blocking` 내에서 호출

#### 검증
- [ ] `cargo test --workspace` 통과
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` 통과
- [ ] RDB 파일 무결성: save → load → 키 검증
- [ ] 피크 메모리 측정: BGSAVE 중 RSS 모니터링
- [ ] 동시 쓰기 중 BGSAVE 정합성 테스트

#### 검토 노트
- **Arc<StoredValue> vs Streaming**: 
  - Arc: 182건 코드 변경, COW 패턴 복잡성, 일상 read/write에도 Arc 오버헤드
  - Streaming: save 코드만 변경, 하지만 save 중 write 차단
  - **추천**: Streaming 방식이 변경 범위 대비 효과가 크다
- **Phase 5를 후순위로 미루는 이유**: Phase 4의 compact encoding이 StoredValue 크기를 
  더 줄이면 snapshot clone 비용도 자동으로 줄어듬

---

### Phase 6: jemalloc 도입 (1일)

> **위험도 하, 독립 실행, 장기 단편화 30-50% 감소**

#### 사전 조건
- [x] 없음 (독립 실행 가능)
- [x] macOS arm64에서 `tikv-jemallocator` 빌드 가능 확인

#### 구현
- [x] `Cargo.toml` (workspace):
  ```toml
  tikv-jemallocator = "0.6"
  tikv-jemalloc-ctl = "0.6"
  ```
- [x] `crates/ratatosk-server/Cargo.toml`:
  ```toml
  [features]
  jemalloc = ["dep:tikv-jemallocator", "dep:tikv-jemalloc-ctl"]
  
  [dependencies]
  tikv-jemallocator = { workspace = true, optional = true }
  tikv-jemalloc-ctl = { workspace = true, optional = true }
  ```
- [x] `main.rs`:
  ```rust
  #[cfg(feature = "jemalloc")]
  use tikv_jemallocator::Jemalloc;
  #[cfg(feature = "jemalloc")]
  #[global_allocator]
  static GLOBAL_ALLOCATOR: Jemalloc = Jemalloc;
  ```
- [ ] `cmd_server_memory.rs`: `MEMORY` 명령에 jemalloc stats 추가 (feature-gated) — 후속 작업
- [ ] `cmd_server_info.rs`: INFO memory 섹션에 `mem_allocator` 필드 — 후속 작업
- [ ] `metrics.rs`: jemalloc gauge 노출 — 후속 작업

#### 빌드 확인
- [x] `cargo check -p ratatosk-server --features jemalloc` 성공 (macOS arm64)
- [ ] `cargo build -p ratatosk-server --features jemalloc` 성공 (Linux, CI)
- [x] `cargo build -p ratatosk-server` (default, no jemalloc) 여전히 동작
- [x] `cargo build -p ratatosk-server --features mimalloc` 여전히 동작
- [x] mimalloc + jemalloc 동시 활성화 방지: `compile_error!` 가드 추가

#### 검증
- [x] `cargo test --workspace` 통과
- [ ] `cargo test --workspace --features jemalloc` 통과 — CI에서 확인
- [x] `cargo clippy --workspace --all-targets -- -D warnings` 통과
- [x] `cargo clippy -p ratatosk-server --features jemalloc -- -D warnings` 통과
- [ ] INFO memory 출력에 allocator 정보 확인 — 후속 작업
- [ ] 벤치마크: default vs jemalloc — 후속 작업

#### 검토 노트
- **macOS arm64**: `tikv-jemallocator` 0.6은 macOS aarch64 지원 확인됨.
  다만 일부 jemalloc feature (transparent huge pages 등)는 Linux-only.
- **바이너리 크기**: jemalloc 링크로 ~1MB 증가. release profile에서 strip으로 완화.
- **mimalloc과 공존**: `--features mimalloc,jemalloc` 방지 필요.
  ```rust
  #[cfg(all(feature = "mimalloc", feature = "jemalloc"))]
  compile_error!("Cannot enable both mimalloc and jemalloc");
  ```

---

### Phase 7: SortedSet SkipList (5일)

> **위험도 상, unsafe 필요, rank O(N)→O(log N)**
> BTreeMap+HashMap 이중 인덱스 → skiplist 단일 인덱스.
> 멤버당 ~120B → ~70B, rank O(log N).

#### 사전 조건
- [ ] Phase 4C 완료 (소규모 ZSet은 listpack으로 커버)
- [ ] Phase 1 완료

#### Step 7-A: SkipList 코어 구현

- [ ] `crates/ratatosk-engine/src/skiplist.rs` 신규 모듈
- [ ] Redis `t_zset.c`의 `zslCreate/zslInsert/zslDelete/zslGetRank` 참조
- [ ] 구조체:
  ```rust
  pub struct SkipList { head, tail, length, max_level }
  struct SkipNode { member: Bytes, score: f64, backward, levels: Vec<SkipLevel> }
  struct SkipLevel { forward: *mut SkipNode, span: usize }
  ```
- [ ] 핵심 연산: insert, remove, find, rank, rev_rank, range_by_score, range_by_rank
- [ ] `unsafe` 최소화: raw pointer는 내부에만, 공개 API는 safe
- [ ] `Drop` 구현: 모든 노드 해제
- [ ] `Clone` 구현: deep copy

#### Step 7-B: 단위 테스트 + Safety

- [ ] 기본 연산: insert/remove/find 왕복
- [ ] rank 정확성: 1000개 랜덤 insert → 전체 rank 검증
- [ ] edge case: 동일 score 다른 member, score NaN/Inf 거부
- [ ] 빈 skiplist 연산
- [ ] `cargo +nightly miri test` — UB 검출
- [ ] `cargo test` with AddressSanitizer (가능 시)

#### Step 7-C: SortedSet 교체

- [ ] `SortedSet` 내부를 skiplist + `HashMap<Bytes, f64>` (score lookup)로 교체
  - 또는 skiplist가 score lookup도 제공하면 HashMap 제거 가능
  - Redis는 skiplist + dict 이중 구조 유지 (O(1) score lookup)
  - **추천**: skiplist + HashMap 유지 (기존 API 호환 최대)
- [ ] 기존 `SortedSet` 공개 API (`insert`, `remove`, `score`, `rank`, `rev_rank` 등) 유지
- [ ] `remove_range_by_score()` — skiplist range 탐색 활용

#### Step 7-D: 커맨드 핸들러

- [ ] `cmd_sorted_set*.rs` — 대부분 `SortedSet` API 호출이므로 변경 최소
- [ ] ZRANGEBYLEX — skiplist에 lex 비교 추가 필요

#### 검증
- [ ] `cargo test --workspace` 통과
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` 통과
- [ ] `cargo +nightly miri test -p ratatosk-engine` — skiplist UB 없음
- [ ] 기존 sorted set 테스트 전체 통과
- [ ] ZRANK 벤치마크: 10K members — O(N) vs O(log N) 비교
- [ ] 메모리 벤치마크: 10K members — Before/After 비교
- [ ] RDB save/load 왕복

#### 검토 노트
- **외부 crate 사용 여부**: `skiplist` crate (1.1.0) 존재하나 span(rank) 미지원.
  `crossbeam-skiplist`는 concurrent but rank 미지원. → **직접 구현 필요**.
- **unsafe 면적**: ~200 lines (insert/remove/drop). Miri 필수.
- **대안**: Phase 4C의 listpack이 소규모(<=128)를 커버하므로, 
  대규모 ZSet만 현재 BTreeMap+HashMap 유지해도 실용적 충분할 수 있음.
  → Phase 7은 **선택적** — 대규모 ZSet 워크로드가 중요할 때만.

---

### Phase 8: Hot Path 소규모 최적화 (1일)

> **위험도 하, 독립 실행 가능**

#### 사전 조건
- [ ] Phase 1 완료 (일부 항목은 Phase 1 이후)

#### 8-A: format_f64_for_redis → ryu

- [x] `Cargo.toml` (workspace): `ryu = "1"` 추가
- [x] `ratatosk-engine/Cargo.toml`: `ryu = { workspace = true }` 추가
- [x] `object.rs` — ryu::Buffer + strip trailing ".0" for Redis compat:
  ```rust
  pub fn format_f64_for_redis(value: f64) -> Bytes {
      let mut buf = ryu::Buffer::new();
      let s = buf.format(value);
      let trimmed = s.strip_suffix(".0").unwrap_or(s);
      Bytes::copy_from_slice(trimmed.as_bytes())
  }
  ```
- [x] 테스트: workspace 전체 통과 (f64 formatting 역호환 확인)

#### 8-B: cmd_append entry API 최적화

- [ ] `cmd_string.rs` `cmd_append()`:
  - 현재: `db.get()` → clone → concat → `db.insert()`
  - 개선: `db.get_mut()` → in-place 확장 (BytesMut 사용 또는 새 Vec 할당)
  - **주의**: Bytes는 immutable이므로 in-place 불가 → Vec concat 후 Bytes::from 유지
  - 진짜 개선: `db.get(key)` 1회 → clone 없이 길이 확인 → concat → insert
- [ ] 테스트: APPEND 기존 테스트 통과

#### 8-C: Shared static responses 확장

- [x] `encode.rs`의 `shared_encoding()` — 추가 패턴:
  - `RespFrame::Integer(-1)` → `b":-1\r\n"` (MISSING 등)
  - `RespFrame::Integer(-2)` → `b":-2\r\n"` (NO KEY 등)
  - `RespFrame::SimpleString("QUEUED")` → `b"+QUEUED\r\n"`
- [x] QUEUED, -1, -2 shared static 추가
- [x] 테스트: encoded_len 일치 확인 (encode tests 통과)

#### 검증
- [x] `cargo test --workspace` 통과 (362 tests)
- [x] `cargo clippy --workspace --all-targets -- -D warnings` 통과
- [x] `cargo fmt --all --check` 통과

---

### 전체 실행 순서 요약

```
Week 1:  Phase 0 (RDB clone) → Phase 6 (jemalloc) → Phase 1 (StoredValue)
Week 2:  Phase 2 (Mem Tracking) → Phase 3 (Expires Index)
Week 3:  Phase 8 (Hot Path) → Phase 4A (StringInt) → Phase 4B (IntSet)
Week 4-5: Phase 4C (Listpack)
Week 5-6: Phase 5 (BGSAVE) → Phase 7 (SkipList, 선택적)
```

#### 각 Phase 완료 후 반드시

```bash
cargo fmt --all --check
cargo check --workspace --quiet
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --quiet
```
