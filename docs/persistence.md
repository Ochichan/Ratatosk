# Persistence

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

기준: 2026-03-16 코드 상태.

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
## 운영 상태 (2026-03-16)

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
- 운영 원칙: 우회 부팅은 “부분 복구 상태”일 수 있으므로, 부팅 직후 데이터 검증과 `BGREWRITEAOF` 또는 오프라인 복구 절차를 반드시 수행한다.

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
