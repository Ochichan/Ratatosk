# IPC 전송 판단 기록 — UDS · 공유메모리 · 인코딩 수치

작성일: 2026-09-08 · 대상: 내부 계획 문서의 미검증 주장(“UDS·shared-memory·sub-µs 주장은 현재 근거 없음”) · 상태: **측정 완료, 판단 확정**

## 1. 무엇이 사실이 되었는가

| 원래 주장 | 2026-09-08 이전 | 지금 | 근거 |
|---|---|---|---|
| UDS 지원 | 없음 (`unixsocket` = unsupported directive) | **제품 기능.** `unixsocket`/`unixsocketperm`(기본 700), TCP 병행, CLIENT LIST `U`, MONITOR `unix:`, CONFIG GET/REWRITE, stale 소켓 안전 처리, 시작 실패 시 파일 정리 | `crates/ratatosk-server/src/event_loop.rs`, `tests/{protocol_contract,cli_config,redis_interop}.rs`, `docs/operations.md` |
| 공유메모리 IPC | 없음 | **실험 기능(feature `shm-transport`).** 별도 crate `ratatosk-shm`: memfd/shm_open 익명 세그먼트, SCM_RIGHTS fd 전달, SPSC 링 + Dekker 파킹, loom 모델·적대적 peer·두-프로세스 SIGKILL 시험 통과 | `crates/ratatosk-shm/`, `tests/shm_crash.rs`, `docs/shm-transport.md` |
| 3.5 ns | 출처 미기록 | **RESP 응답 1건 인코딩 CPU 비용**(`+OK` 3.1 ns, `+PONG` 3.9 ns, 사전할당 buffer, Apple M5 Pro). IPC 지연이 아니며 그렇게 인용하지 않는다 | `docs/optimization.md` “RESP Reply Encode Cost”, `benchmarks/encode-reply-20260908-181900.log` |
| 실제 IPC 지연 | 미측정 | **두-프로세스 왕복 실측** (아래 표) | `benchmarks/ipc/20260908T105558Z-local-final-hybrid.json`, `crates/ratatosk-ipc-bench` |

## 2. 실측 (환경·표본·분포·CPU·RSS를 함께 기록하는 benchmark 계약 준수)

조건: Apple M5 Pro (arm64, macOS 25.6.0), release, 서버 `26d8877b1-dirty` (이 브랜치 작업 트리, sha256 `aea7cda2…`), lockstep 요청/응답(파이프라인 없음), 표본 100 000/행, warm-up 10 000, 동일 host monotonic clock. 서버 hybrid 대기(spin 2000), SHM 클라이언트 spin 20 000. TCP는 loopback + `TCP_NODELAY`. 전원/열 상태 미제어(노트북, 무부하).

### 2.1 PING 왕복 (µs)

| 전송 | conns | p50 | p99 | p99.9 | max |
|---|---:|---:|---:|---:|---:|
| TCP loopback | 1 | 15.6 | 25.8 | 35.3 | 103 |
| Unix socket | 1 | 5.0 | 11.8 | 17.1 | 80 |
| Shared memory | 1 | **1.1** | **2.0** | 4.8 | 52 |
| TCP loopback | 4 | 27.7 | 53.1 | 68.1 | 113 |
| Unix socket | 4 | 20.3 | 39.6 | 59.0 | 96 |
| Shared memory | 4 | 7.9 | 35.0 | 49.2 | 87 |

### 2.2 payload별 (conns=1, p50 / p99, µs)

| payload | TCP | UDS | SHM |
|---|---:|---:|---:|
| SET 64 B | 17.3 / 27.8 | 6.2 / 13.5 | 1.5 / 2.7 |
| SET 1 KiB | 17.3 / 31.0 | 10.3 / 15.3 | 2.3 / 4.4 |
| GET 64 B | 17.3 / 28.9 | 5.1 / 12.6 | 1.3 / 2.3 |

### 2.3 SHM 대기 정책 (PING, 표본 50 000; CPU는 호출 전체 user+sys 초)

| client spin | server spin | conns=1 p50 / p99 | conns=4 p50 / p99 | client CPU | server CPU |
|---:|---:|---:|---:|---:|---:|
| 20 000 | 2000 | 1.1 / 2.3 | 8.3 / 28.2 | 0.55 | 0.47 |
| 20 000 | 0 | 1.1 / 2.4 | 8.4 / 24.9 | 0.55 | 0.47 |
| 0 | 2000 | 2.5 / 8.7 | 19.8 / 42.0 | 0.49 | 0.94 |
| 0 | 0 | 2.6 / 8.6 | 19.0 / 31.2 | 0.48 | 0.92 |

읽는 법: 클라이언트 spin이 지연을 결정한다(spin 없음 = doorbell syscall 왕복 ≈ +1.4 µs). 서버 spin은 이 부하에서 차이가 없어 **서버 기본 2000은 낮춰도 된다**. 파킹 전용 SHM(2.5 µs)도 UDS(5.0 µs)보다 빠르다.

### 2.4 측정에서 드러난 전송 밖의 사실

- **4 연결에서 모든 전송의 p50이 커진다** (TCP 15.6→27.7 = 1.8배, UDS 5.0→20.3 = 4배, SHM 1.1→7.9 = 7배 µs). 전송이 아니라 서버 측 직렬화(명령 실행 경로의 공유 잠금)의 성질이다. 이 작업 범위 밖이며 별도 항목으로 보고한다.
- **SET 행의 max 1~6 ms 꼬리**는 세 전송에 공통이다. 고유 키 10만 개 이상을 넣을 때의 hashmap 성장(rehash) 정지로 보인다. 역시 전송 밖의 서버 성질.
- **sub-µs는 달성하지 않았다.** 최선 p50 1.1 µs에는 harness 클라이언트의 tokio `block_on` 진입/이탈, byte 단위 atomic 복사, RESP 파싱·실행이 포함된다. “실측 전 보장 금지” 원칙은 그대로 유지한다.

## 3. 판단 (P5)

**SHM 필요성: “조건부 — 제품 기본은 UDS, SHM은 experimental로 유지.”**

근거:
1. 이 서버를 로컬 알림·캐시 계층으로 쓰는 소비자의 요구는 인간 시간 스케일이며 UDS p99 12 µs로 충분하다. 알림 손실·재조회·권한 검사가 병목이지 전송 지연이 아니다.
2. SHM은 p50 기준 UDS 대비 4.5배, TCP 대비 14배 빠르지만, 값어치가 나오는 곳은 단일 연결 lockstep 경로뿐이다. 4 연결에서는 서버 직렬화가 이득을 삼킨다(2.4). 전송을 바꾸기 전에 그 병목을 먼저 풀어야 한다.
3. SHM의 안전성 근거(loom, 적대적 peer, SIGKILL 양방향)는 확보했지만, 운영 근거(장시간 soak, Linux 실기, 다중 클라이언트 spin의 CPU 예산)는 아직 없다. 지원 계약에 넣을 수준이 아니다.

따라서:
- **PRODUCT_CONTRACT**: UDS는 지원 열, SHM은 experimental 행. 완료.
- **내부 계획 문서**: “근거 없음” 항목을 이 문서를 가리키도록 갱신(비공개 문서).
- **후속 갭(신규, 제안)**: (a) 서버 명령 경로의 다중 연결 직렬화 완화, (b) 대량 삽입 시 rehash 꼬리, (c) SHM Linux 실기·soak, (d) SHM 클라이언트를 동기(블로킹) API로 제공해 harness의 `block_on` 오버헤드 제거.

## 4. 재현

```bash
# 인코딩 수치
cargo bench -p ratatosk-server --bench pipeline -- --sample-size 20 encode
# 전송 실측 (feature 빌드를 별도 target에 두면 다른 빌드에 덮어써지지 않는다)
CARGO_TARGET_DIR=target/shm cargo build -p ratatosk-server --release --features shm-transport
cargo build -p ratatosk-ipc-bench --release
RATATOSK_BIN=$PWD/target/shm/release/ratatosk ./target/release/ratatosk-ipc-bench \
  --transports tcp,unix,shm --conns 1,4 --samples 100000 --warmup 10000 --tag <tag>
python3 scripts/perf_guardrail_check.py --ipc benchmarks/ipc/latest.json
```
