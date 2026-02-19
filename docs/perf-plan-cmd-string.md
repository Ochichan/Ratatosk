# Performance Improvement Plan: cmd_string.rs

## Priority 1: Hot Path - High Impact

### 1.1 `cmd_get` - Double Lookup 제거
**파일**: `cmd_string.rs:563-570`

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

---

### 1.2 `cmd_incr_decr_with_delta` - Allocation Churn 제거
**파일**: `cmd_string.rs:417`

**현재 문제**:
```rust
Bytes::from(next.to_string())  // heap allocation on every INCR/DECR
```

**개선안**: `itoa` crate 사용
```rust
// Cargo.toml에 itoa 추가
let mut buffer = itoa::Buffer::new();
let formatted = buffer.format(next);
Bytes::from(formatted.to_owned())
```

**대안**: Stack buffer 직접 구현 (itoa 없이)
```rust
fn format_i64_to_bytes(n: i64) -> Bytes {
    const MAX_I64_LEN: usize = 20; // "-9223372036854775808"
    let mut buf = [0u8; MAX_I64_LEN];
    let mut pos = MAX_I64_LEN;
    let mut val = n.abs();

    if n == 0 {
        return Bytes::from(&b"0"[..]);
    }

    while val > 0 {
        pos -= 1;
        buf[pos] = b'0' + (val % 10) as u8;
        val /= 10;
    }

    if n < 0 {
        pos -= 1;
        buf[pos] = b'-';
    }

    Bytes::copy_from_slice(&buf[pos..])
}
```

**예상 효과**: INCR/DECR ~30-50% allocation overhead 감소

---

### 1.3 `cmd_incrbyfloat` - 불필요한 Clone 제거
**파일**: `cmd_string.rs:381-386`

**현재 문제**:
```rust
let encoded = format_f64_for_redis(next);
db.insert(key.clone(), StoredValue::string(encoded.clone(), expire_at_ms));
CommandOutcome::reply(RespFrame::BulkString(Some(encoded)))  // encoded already moved!
```

**개선안**: encoded를 한 번만 clone
```rust
let encoded = format_f64_for_redis(next);
let encoded_for_reply = encoded.clone();
db.insert(key.clone(), StoredValue::string(encoded, expire_at_ms));
CommandOutcome::reply(RespFrame::BulkString(Some(encoded_for_reply)))
```

**예상 효과**: INCRBYFLOAT 1 allocation 감소

---

## Priority 2: Medium Impact

### 2.1 `cmd_set` - Option Parsing Allocation 제거
**파일**: `cmd_string.rs:442`

**현재 문제**:
```rust
let option = to_uppercase_bytes(&args[idx]);  // Vec allocation per option
match option.as_slice() { ... }
```

**개선안 1**: Const byte slices 사용
```rust
// 알려진 옵션들에 대해 direct slice 비교
fn match_option(bytes: &[u8]) -> Option<SetOption> {
    match bytes {
        b"NX" | b"nx" => Some(SetOption::Nx),
        b"XX" | b"xx" => Some(SetOption::Xx),
        b"GET" | b"get" => Some(SetOption::Get),
        b"KEEPTTL" | b"keepttl" => Some(SetOption::Keepttl),
        b"EX" | b"ex" => Some(SetOption::Ex),
        b"PX" | b"px" => Some(SetOption::Px),
        b"EXAT" | b"exat" => Some(SetOption::Exat),
        b"PXAT" | b"pxat" => Some(SetOption::Pxat),
        _ => None,
    }
}
```

**개선안 2**: In-place uppercase 변환 (allocation 없이)
```rust
fn option_matches(bytes: &[u8], expected: &[u8]) -> bool {
    bytes.len() == expected.len() &&
    bytes.iter().zip(expected).all(|(b, e)| b.to_ascii_uppercase() == *e)
}
```

**예상 효과**: SET with options ~20% allocation 감소

---

### 2.2 `cmd_msetnx` - 단일 패스 최적화 (검토 필요)
**파일**: `cmd_string.rs:270-286`

**현재 문제**: 두 번 iterate (contains_key check + insert)

**주의**: 원자성 보장을 위해 두 패스가 필요할 수 있음. 변경 시 semantic 검증 필요.

**검토 사항**:
- 다른 클라이언트가 중간에 키를 추가할 수 있는지?
- 트랜잭션 컨텍스트에서 실행되는지?

**결론**: Semantic 변경 위험 > 성능 이익. **보류**

---

## Priority 3: Architecture Review

### 3.1 `cmd_mget` - Clone 필수 여부 검토
**파일**: `cmd_string.rs:229`

**현재**:
```rust
let value = db.get(key).and_then(|entry| entry.as_string().cloned());
```

**분석**:
- `RespFrame::BulkString`이 `Option<Bytes>`를 사용
- `Bytes`는 이미 Arc-based로 clone이 cheap (O(1))
- **결론**: 현재 구현이 이미 최적화됨. 변경 불필요

---

## Implementation Order

1. **`cmd_get` double lookup** - 즉시 적용, 낮은 위험
2. **`cmd_incrbyfloat` clone 제거** - 즉시 적용, 낮은 위험
3. **`cmd_incr_decr_with_delta` allocation** - itoa 의존성 추가 후 적용
4. **`cmd_set` option parsing** - 리팩토링 필요, 중간 위험

## Dependencies

```
[dependencies]
itoa = "1.0"  # i64 formatting without allocation
```

## Testing Strategy

1. 기존 unit test 통과 확인
2. Integration test with redis-benchmark
3. Criterion microbenchmark for affected functions
4. Memory allocation counting before/after

## Rollback Plan

각 변경사항을 별도 commit으로 분리. 문제 발생 시 revert.
