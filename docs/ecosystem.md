# Ratatosk Ecosystem Integration

Ratatosk은 RESP3 기반 인메모리 데이터 스토어이며, 캐시 + Pub/Sub 이벤트 버스 역할을 맡는다.
이 문서는 **현재 코드 상태**(2026-02-11)와 외부 프로젝트 통합 기준을 정리한다.

## Implementation Status (2026-02-11)

기준 파일: `docs/redis-gap-ledger.json`

- 명령 카탈로그: `420` entries
- status summary: `done=420`
- capability tier summary: `unsupported=64`, `syntax_only=30`, `baseline_local=45`, `behavioral_subset=281`, `distributed_parity=0`
- 즉, "명령 이름 존재"와 "Redis 행동 parity"는 같은 뜻이 아니다.

### 인프라 구현 상태

| 서브시스템 | 상태 | 설명 |
|-----------|------|------|
| RESP2/3 파서 | 완료 | zero-copy incremental parser |
| 420 명령 엔트리 | 완료 | status 기준으로는 모두 `done`, 다만 parity tier는 명령별로 다름 |
| Eviction (8 정책) | 완료 | LRU/LFU/random/TTL 샘플링 |
| Active expiry | 완료 | server_cron 10Hz 샘플링 기반 |
| server_cron | 완료 | tokio interval timer |
| Keyspace notifications | 완료 | `__keyspace@<db>__` / `__keyevent@<db>__` |
| Lazy free | 완료 | crossbeam 백그라운드 스레드 |
| RDB snapshot | 완료 | save/load + CRC64 + atomic write |
| AOF writer | 완료 | RESP append + fsync 정책 |
| AOF recovery | 완료 | RESP 파싱 → execute 재생 |
| AOF manifest | 부분 구현 | save/load, bootstrap/recovery, manifest switch helper, rewrite 후 새 INCR rotation은 연결됐지만 BASE materialization과 runtime full atomic switch는 아직 없음 |
| Background save | 완료 | background snapshot worker 연결 |
| AOF rewrite | 완료 | background rewrite worker 연결, 다만 Redis식 current-state compaction은 아님 |
| AOF 서버 통합 | 완료 | write 명령 후 자동 append 경로 존재 |
| Replication | 부분 구현 | role 전이, logical repl offset, replica ACK accounting은 있으나 backlog/network stream/failover는 아직 없음 |
| Cluster | 미구현 | 해시 슬롯 helper 일부만 존재, distributed routing 없음 |

중요:
- 명령 surface는 넓지만, 일부 운영/복제/클러스터 명령은 `unsupported` 또는 `syntax_only`/`baseline_local` tier다.
- 통합 시에는 "명령 존재"와 "행동 parity"를 분리해서 검증해야 한다.

## Ratatosk가 맡는 역할

### 1) High-throughput cache

- TTL 기반 임시 데이터 캐시.
- `bytes::Bytes` 기반 데이터 경로로 복사 비용을 줄임.
- maxmemory + eviction 정책으로 메모리 한도 관리.
- 빠른 키 조회, 세션 컨텍스트, rate-limit 카운터 저장에 적합.

### 2) Event bus

- `SUBSCRIBE`/`PSUBSCRIBE`/`SSUBSCRIBE` + `PUBLISH`/`SPUBLISH` 제공.
- keyspace notification으로 키 변경 이벤트 자동 발행.
- pending queue 기반 backpressure로 느린 subscriber를 보호.

### 3) Persistent data store

- RDB snapshot으로 주기적 데이터 백업.
- AOF로 명령 단위 durability 확보.
- 서버 재시작 시 RDB → AOF 순서로 데이터 복구.

### 4) Redis-compatible endpoint

- 기존 Redis 클라이언트/SDK를 그대로 붙여 초기 통합 비용을 낮춤.
- 운영 도구(`INFO`, `CONFIG`, `SLOWLOG`, `LATENCY`)를 기본 제공.

## Integration Matrix

| Service | Role with Ratatosk | Protocol | Typical Use |
| --- | --- | --- | --- |
| Conductor | 실행 상태 캐시 + 실행 이벤트 fanout | RESP3 TCP | DAG node intermediate result, execution events |
| Ironclaw | 세션 캐시 + provider rate-limit counter | RESP3 TCP | session context TTL, API quota counter |
| command-center | TUI 상태 공유 + Pub/Sub 수신 | RESP3 TCP | live event stream, undo/clipboard cache |
| Rustmux | 세션 메타데이터 캐시(선택) | RESP3 TCP | terminal session index/cache |
| Muninn | 검색 결과 단기 캐시(선택) | RESP3 TCP | hot query result cache |

## Deployment Baseline

### Local/manual

```bash
cargo run -p ratatosk-server --bin ratatosk --release
```

- default bind/port: `127.0.0.1:6379`

### Autostart (systemd --user)

```bash
./scripts/install-ratatosk-launcher.sh
./scripts/install-ratatosk-autostart.sh
```

- autostart unit은 기본 포트 `6380`을 사용해 수동 실행과 충돌을 피한다.
- unit 파일 경로: `~/.config/systemd/user/ratatosk-serve.service`

## Runtime Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `RATATOSK_BIND` | `127.0.0.1` | listen address |
| `RATATOSK_PORT` | `6379` | listen port |
| `RATATOSK_MAX_CLIENTS` | `4096` | concurrent connection cap |
| `RATATOSK_OUTPUT_BUFFER_LIMIT_BYTES` | `8388608` | per-client output limit |
| `RATATOSK_SHUTDOWN_GRACE_MS` | `10000` | graceful drain window |
| `RATATOSK_ALLOW_INSECURE_BIND` | unset | non-loopback bind opt-in |

런타임 `CONFIG SET` 지원:
- `maxmemory`, `maxmemory-policy`, `maxmemory-samples`
- `hz`, `notify-keyspace-events`, `tcp-keepalive`
- `lazyfree-lazy-expire`, `lazyfree-lazy-server-del`, `lazyfree-lazy-user-del`
- `appendfsync` (always/everysec/no)

## Recommended Key Naming

- Conductor: `conductor:exec:<exec_id>:node:<node_id>`
- Ironclaw session: `ironclaw:session:<sid>`
- Ironclaw rate-limit: `ironclaw:ratelimit:<provider>:<window>`
- command-center session: `cc:session:<sid>:state`
- Muninn cache: `muninn:cache:<query_hash>`

운영 규칙:
- 공유 키는 서비스 prefix를 강제한다.
- 캐시 키는 TTL을 기본값으로 둔다(무기한 키 금지).
- Pub/Sub 채널은 도메인 prefix로 분리한다(`conductor:events:*`).

## Integration Playbooks

### Conductor -> Ratatosk

- 중간 산출물은 TTL key로 저장하고, 완료 이벤트는 Pub/Sub으로 전파.
- 장애 시 fallback: 프로세스 로컬 메모리 캐시 + polling 이벤트 경로.

### Ironclaw -> Ratatosk

- active session context를 TTL key로 저장.
- provider quota는 `INCR` + `EXPIRE` 조합으로 window counter 구현.

### command-center -> Ratatosk

- TUI 상태를 hash/list로 저장하고, Pub/Sub으로 실시간 이벤트 수신.
- Ratatosk 미가용 시 로컬 상태 모드로 degrade.

### Muninn -> Ratatosk

- semantic search 결과를 short TTL로 캐시.
- 캐시 미스 시에만 Muninn 검색 경로 실행.

## Operational Notes

### Graceful degradation

Ratatosk은 선택적 의존성으로 취급한다.
미가용 시 캐시 미스/실시간 이벤트 지연은 허용하되 core 기능은 유지해야 한다.

### Security defaults

- loopback bind 기본 + insecure bind explicit opt-in.
- TLS 종단은 프록시 계층(stunnel, nginx stream, envoy 등)에서 처리 권장.

### Backpressure and limits

- query buffer limit: 1 MiB
- output buffer limit: 기본 8 MiB
- pubsub pending queue limit: 4096
- lazy free channel capacity: 4096

## Roadmap Priorities

1. **Persistence 서버 통합**: AOF writer를 이벤트 루프에 연결, background RDB save 구현
2. **AOF rewrite**: background에서 compact AOF 생성, manifest 기반 파일 전환
3. **Notification wiring**: 80+ mutation site에 `notify!` 매크로 삽입
4. **Redis parity hardening**: edge-case semantics를 Redis와 byte-level 비교 검증
5. **통합 계약 테스트**: 주요 서비스별 smoke + failure-path 자동화
6. **성능 회귀 자동화**: benchmark guardrail CI 루틴 고정
