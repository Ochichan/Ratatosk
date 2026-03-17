# Eviction & Expiry

Ratatosk의 메모리 관리 및 키 만료 시스템.
기준: 2026-03-16 코드 상태.

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
CONFIG SET maxmemory 100mb
CONFIG SET maxmemory-policy allkeys-lru
CONFIG SET maxmemory-samples 5
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

`StoredValue.lru_clock` 필드에 저장.

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
2. **Eviction check** — `maxmemory > 0`이면 `estimate_used_memory()` → `perform_eviction()`

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

## 테스트 커버리지

| 모듈 | 테스트 수 | 주요 검증 |
|------|-----------|-----------|
| `eviction` | 11 | 8가지 정책, LRU clock wrap, 메모리 추정, volatile 키 필터링 |
| `expiry` | 3 | 만료 키 제거, 빈 DB, volatile 키 없는 DB |
| `notification` | 7 | 설정 파싱, K/E 플래그, A 단축키, pub/sub 연동, 매크로 |
