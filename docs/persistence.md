# Persistence

Ratatosk의 데이터 영속성 시스템.
`ratatosk-persist` 크레이트가 RDB 스냅샷과 AOF 로그를 담당한다.

기준: 2026-02-11 코드 상태.

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
appendonlydir/
  appendonly.aof.1.incr.aof   (INCR: 증분 append)
  appendonly.aof.2.incr.aof   (INCR: 증분 append)
  base.aof                    (BASE: rewrite 결과)
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

## 테스트 커버리지

| 모듈 | 테스트 수 | 주요 검증 |
|------|-----------|-----------|
| `rdb/saver` | 3 | 빈 상태, string 키, expiry 직렬화 |
| `rdb/loader` | 8 | 6가지 타입 roundtrip, CRC 불일치, magic 검증, 다중 DB |
| `rdb/checksum` | 3 | deterministic, 빈 입력, known value |
| `atomic` | 2 | 정상 쓰기, 에러 시 원본 보존 |
| `aof/writer` | 4 | RESP 정확성, SELECT 자동 삽입, DB 유지, fsync policy roundtrip |
| `aof/manifest` | 4 | 빈 manifest, 순차 INCR, BASE 설정, recovery 순서 |
| `aof/recovery` | 4 | 상태 복원, DB select, 빈 파일, 절단 파일 graceful 처리 |

합계: 30개 테스트.

---

## 미구현 / 향후 작업

| 항목 | 상태 | 설명 |
|------|------|------|
| Background RDB save | 미구현 | `fork()` 또는 background thread 기반 snapshot |
| AOF rewrite | 미구현 | 현재 keyspace를 compact AOF로 덤프 |
| server_cron 통합 | 부분 | SIGUSR1 시그널 수신은 구현, 실제 save 트리거 미연결 |
| AOF writer 서버 통합 | 미구현 | 쓰기 명령 후 AOF append 연동 |
| LZF 압축 | 미구현 | RDB string 압축 (큰 값 전용) |
| Manifest 파일 I/O | 미구현 | manifest를 디스크에 직렬화/역직렬화 |
