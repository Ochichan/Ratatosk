# Performance

Ratatosk 성능 기준선 및 최적화 계획.

## Baseline

### Quick Workflow

```bash
# 1) baseline 측정 (default allocator)
./scripts/bench_baseline.sh

# 2) 선택: mimalloc 비교
WITH_MIMALLOC=1 ./scripts/bench_baseline.sh

# 3) guardrail 검증
python3 scripts/perf_guardrail_check.py --log <benchmark_log>
```

### Bench Scope

- bench target: `cargo bench -p ratatosk-server --bench pipeline`
- metrics:
  - `pipeline_set_parse_execute_encode`
  - `pipeline_ping_parse_execute_encode`
- pipeline lengths: `1`, `32`, `256`, `1024`

### Guardrail Defaults

- `pipeline_set_parse_execute_encode/256` upper <= `110 us`
- `pipeline_ping_parse_execute_encode/256` upper <= `35 us`
- checker: `scripts/perf_guardrail_check.py`

### Reference Logs

| Log | set@256 upper | ping@256 upper | Guardrail Result | Classification |
| --- | ---: | ---: | --- | --- |
| `benchmarks/baseline-default-20260208-185945.log` | 101.510 us | 29.338 us | PASS | canonical baseline |
| `benchmarks/baseline-default-20260208-191454.log` | 101.610 us | 30.074 us | PASS | compatible baseline |
| `benchmarks/baseline-default-20260208-195805.log` | 103.610 us | 77.826 us | FAIL (ping) | high-variance outlier |

운영 규칙:
- PASS 로그만 guardrail 업데이트 후보로 사용한다.
- FAIL/고분산 로그는 회귀 원인 조사 참고용으로만 보관한다.

### Allocator A/B Policy

- 현재 기본 allocator를 유지한다.
- `mimalloc`은 feature flag(`--features mimalloc`)로 재측정할 수 있다.
- allocator 전환은 최소 2회 이상 일관된 개선 결과가 있을 때만 검토한다.

### Record Template

| Date | Commit | Allocator | set@256 upper (us) | ping@256 upper (us) | Guardrail | Notes |
| --- | --- | --- | ---: | ---: | --- | --- |
| YYYY-MM-DD | <sha> | default |  |  | PASS/FAIL |  |
| YYYY-MM-DD | <sha> | mimalloc |  |  | PASS/FAIL |  |

---

## SharedState: Lock-Free Fast Paths

`SharedState` 구조체가 `Mutex<ServerState>` 외부에 lock-free 컴포넌트를 분리하여, client 요청당 ~9회의 lock 획득을 제거한다.

### Per-DB RwLock

`ServerState` 내부 `DataState`가 DB별 `parking_lot::RwLock<DbShard>`를 관리한다:

- `db(idx)` → `MappedRwLockReadGuard<HashMap>`: 읽기 명령은 read lock 공유
- `db_mut(idx)` → `MappedRwLockWriteGuard<HashMap>`: 쓰기 명령은 해당 DB만 exclusive lock
- 서로 다른 DB에 대한 명령은 lock contention 없이 병렬 실행 가능
- `snapshot_all()`: BGSAVE 시 DB별 순차 read-lock + clone. 전체 global lock 점유 대신 DB 하나씩 짧게 잠금
- Lock ordering: 항상 ascending DB index 순서로 획득 → deadlock 방지
- `parking_lot` guard는 `!Send` → `.await`를 넘을 수 없어 compile-time 안전성 보장

### AtomicStatsState

10개 `AtomicU64` counter로 매 요청마다 lock 없이 stats를 갱신:

- `total_commands_processed`, `connected_clients`
- `net_input_bytes`, `net_output_bytes`
- `evicted_keys`, `expired_keys`
- `keyspace_hits`, `keyspace_misses`
- `ops_per_sec`, `cached_memory_estimate`

`INFO` 명령의 stats 섹션이 이 atomic counter를 직접 읽으므로, stats 조회도 lock-free.

### ArcSwap<ConfigState>

`arc_swap::ArcSwap<ConfigState>`를 통해 config 읽기가 lock-free:

- 읽기: `config_cache.load()` — 매 요청의 config 참조가 lock 없이 수행됨
- 쓰기: `CONFIG SET` 시에만 lock 필요

### Atomic Client ID

`AtomicU64::fetch_add`로 새 연결의 client ID를 할당. accept 경로에서 lock이 불필요.

## Pub/Sub: mpsc Push Delivery

기존 `HashMap<i64, Vec<PubSubMessage>>` 기반 polling을 per-subscriber `tokio::sync::mpsc::channel`로 교체.

성능 이점:
- **Zero polling overhead**: 이전의 20ms polling interval이 제거됨. 메시지가 즉시 push 전달.
- **Backpressure**: `try_send()` 기반. channel capacity (= hard_limit) 초과 시 즉시 overflow → disconnect.
- **Client loop 통합**: `WaitResult` enum으로 pub/sub, monitor, network read를 단일 `select!`에서 처리.
- **Client tracking invalidation**: 동일 mpsc 채널을 통해 자동 전달. 별도 delivery 경로 불필요.

---

## Optimization Plan: cmd_string.rs

### Priority 1: Hot Path - High Impact

#### 1.1 `cmd_get` - Double Lookup 제거

**현재 문제**:
```rust
let found = db.contains_key(key);  // 1st lookup
if !found { ... }
let entry = &server.db(client.selected_db)[key];  // 2nd lookup
```

**개선안**:
```rust
let Some(entry) = db.get(key) else {
    server.stats.mark_keyspace_miss();
    return CommandOutcome::reply(RespFrame::BulkString(None));
};

server.stats.mark_keyspace_hit();
if !entry.is_string() {
    return wrong_type_response();
}
CommandOutcome::reply(RespFrame::BulkString(entry.as_string().cloned()))
```

**예상 효과**: GET 명령어 15-25% latency 감소

#### 1.2 `cmd_incr_decr_with_delta` - Allocation Churn 제거

**현재 문제**:
```rust
Bytes::from(next.to_string())  // heap allocation on every INCR/DECR
```

**개선안**: `itoa` crate 사용
```rust
let mut buffer = itoa::Buffer::new();
let formatted = buffer.format(next);
Bytes::from(formatted.to_owned())
```

**예상 효과**: INCR/DECR ~30-50% allocation overhead 감소

#### 1.3 `cmd_incrbyfloat` - 불필요한 Clone 제거

**개선안**: encoded를 한 번만 clone
```rust
let encoded = format_f64_for_redis(next);
let encoded_for_reply = encoded.clone();
db.insert(key.clone(), StoredValue::string(encoded, expire_at_ms));
CommandOutcome::reply(RespFrame::BulkString(Some(encoded_for_reply)))
```

**예상 효과**: INCRBYFLOAT 1 allocation 감소

### Priority 2: Medium Impact

#### 2.1 `cmd_set` - Option Parsing Allocation 제거

**개선안**: Case-insensitive direct slice 비교로 `to_uppercase_bytes()` 호출 제거
```rust
fn option_matches(bytes: &[u8], expected: &[u8]) -> bool {
    bytes.len() == expected.len() &&
    bytes.iter().zip(expected).all(|(b, e)| b.to_ascii_uppercase() == *e)
}
```

**예상 효과**: SET with options ~20% allocation 감소

#### 2.2 `cmd_msetnx` - 단일 패스 최적화 (보류)

원자성 보장을 위해 두 패스가 필요할 수 있음. Semantic 변경 위험 > 성능 이익.

### Priority 3: Architecture Review

#### 3.1 `cmd_mget` - Clone 필수 여부

`Bytes`는 Arc-based로 clone이 cheap (O(1)). **현재 구현이 이미 최적화됨. 변경 불필요.**

### Implementation Order

1. **`cmd_get` double lookup** - 즉시 적용, 낮은 위험
2. **`cmd_incrbyfloat` clone 제거** - 즉시 적용, 낮은 위험
3. **`cmd_incr_decr_with_delta` allocation** - itoa 의존성 추가 후 적용
4. **`cmd_set` option parsing** - 리팩토링 필요, 중간 위험

### Testing Strategy

1. 기존 unit test 통과 확인
2. Integration test with redis-benchmark
3. Criterion microbenchmark for affected functions
4. Memory allocation counting before/after
