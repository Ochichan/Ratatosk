# Ratatosk Operations & Deployment Guide

---

## Part 1: Product Contract

<!-- Source: product-contract.md -->

# Ratatosk Product Contract (v1)

기준일: 2026-06-02

이 문서는 Ratatosk v1이 무엇을 보장하고, 무엇을 보장하지 않는지 명확히 선언한다.
목표는 "single-node Ratatosk GA"이며, "Redis drop-in distributed replacement"가 아니다.

## 1. 제품 경계

Ratatosk v1은 다음 범위를 지원한다.

- single-node only
- RESP2/RESP3 TCP server
- cache + pub/sub + local persistence
- RDB snapshot + AOF durability
- standalone 운영용 `INFO`, `CONFIG`, `SLOWLOG`, `LATENCY`

Ratatosk v1은 다음을 제공하지 않는다.

- Redis Cluster
- Sentinel failover
- real network replication stream
- replica-backed `WAIT`/`WAITAOF`
- Redis Functions parity

근거: `README.md`, `docs/architecture.md` (Part 4: Capability Declarations), `docs/architecture.md` (Part 6: Redis Gap Ledger)

## 2. 지원 계약

Ratatosk v1은 아래만 "지원 계약"에 포함한다.

- README와 capability declarations에 standalone support로 명시된 기능
- `behavioral_subset` 또는 v1에서 명시적으로 승인한 `baseline_local` tier 명령
- local durability contract로 문서화된 RDB/AOF behavior
- loopback-default 보안 모델과 proxy-terminated TLS deployment 모델

지원 계약에 포함되지 않는 것은 다음과 같이 취급한다.

- `unsupported`: ship contract 바깥
- `experimental`: feature-gated, no compatibility guarantee
- `syntax_only`: protocol shell only, no Redis parity promise

## 3. 보안/배포 모델

- 기본 bind는 loopback
- non-loopback bind는 `RATATOSK_ALLOW_INSECURE_BIND=true`와 함께 ACL bootstrap 절차가 필요하다
- non-loopback bind에서는 `RATATOSK_DEFAULT_USER_PASSWORD` 또는 `RATATOSK_DEFAULT_USER_PASSWORD_HASH` 없이 기본 `nopass` 사용자를 유지하지 않는다
- built-in TLS는 제공하지 않으며, proxy-layer termination이 기본 배포 모델이다
- production deployment는 ACL bootstrap, metrics scraping, alerting, backup drill이 완료되어야 한다

### TLS termination recipes

Ratatosk speaks plaintext RESP. For encryption in transit, terminate TLS at a
proxy and have it connect to Ratatosk over loopback. Keep Ratatosk bound to
`127.0.0.1` so only the proxy can reach it.

**stunnel** (smallest footprint):

```ini
[ratatosk]
accept  = 0.0.0.0:6380
connect = 127.0.0.1:6379
cert    = /etc/ratatosk/tls/server.pem
key     = /etc/ratatosk/tls/server-key.pem
```

**nginx** (`stream` module, nginx ≥ 1.9.0):

```nginx
stream {
  server {
    listen 6380 ssl;
    ssl_certificate     /etc/ratatosk/tls/server.pem;
    ssl_certificate_key /etc/ratatosk/tls/server-key.pem;
    proxy_pass 127.0.0.1:6379;
  }
}
```

**Envoy** (TCP proxy + downstream TLS): configure a `tcp_proxy` filter to cluster
`127.0.0.1:6379` with a `transport_socket` of type
`envoy.transport_sockets.tls` on the listener. mTLS is added with
`require_client_certificate: true` + a `validation_context`.

Hardening checklist: client keeps `127.0.0.1` binding; proxy enforces TLS ≥ 1.2;
for mutual auth, require client certs at the proxy; Ratatosk ACL/AUTH still
applies behind the proxy (TLS is transport, not authentication).

## 4. 지속성 계약

- RDB는 point-in-time snapshot이다
- AOF는 local durability mechanism이다
- durability는 distribution을 의미하지 않는다
- startup replay 순서는 RDB -> AOF다
- persistence format migration이나 rollback safety에 예외 플래그가 필요하면, 그 환경은 ship-ready로 간주하지 않는다

### Durability contract by fsync policy

What survives a crash depends on `appendonly` and `appendfsync`. This table is
the contract — it is exercised by `scripts/recovery_matrix.sh` (Phase 4).

| Config | Acknowledged-write loss window on `kill -9` | Loss on graceful shutdown | Notes |
|---|---|---|---|
| `appendonly no` (RDB only) | everything since the last `SAVE`/`BGSAVE` | nothing if shutdown flush succeeds | snapshot durability; cron `save` rules bound the window |
| `appendonly yes`, `appendfsync always` | **0** — every acknowledged write is fsynced | 0 | strongest; highest per-write cost |
| `appendonly yes`, `appendfsync everysec` | **≤ 1 second** of acknowledged writes | 0 | default-recommended balance |
| `appendonly yes`, `appendfsync no` | up to the OS page-cache flush interval | 0 | OS decides; weakest AOF durability |

Recovery invariants (asserted by `scripts/recovery_matrix.sh`):

- **Truncated AOF tail** → server starts and recovers a consistent *prefix*; it never crashes and never invents writes beyond what was acknowledged.
- **`kill -9` during `BGREWRITEAOF`** → restart recovers the full pre-rewrite dataset; no corruption.
- **Missing manifest with segments on disk** → server either rebuilds or fails startup cleanly; it never reports an empty keyspace as a successful start (no silent data loss).
- **Repeated `BGREWRITEAOF`** → keyspace size is stable across rewrites.
- **Disk full** is surfaced via `ratatosk_aof_write_errors_total` + the `RatatoskAofWriteErrors` alert (constrained-FS injection is CI-only).

Backup / restore / rollback is reproducible via `scripts/backup_restore_drill.sh`.

## 5. 운영 계약

v1 shipment 전 필수 조건:

- quality gate green
- security gate green
- performance guardrail green
- supported command differential suite green
- Redis interop workflow green
- backup/restore/rollback drill 완료
- alert/dashboard/runbook 존재

## 6. Stretch Goal

Redis drop-in GA는 별도 계약이다. 그 목표는 replication/Sentinel/Cluster/Functions를 포함하는 후속 프로그램으로 다룬다.

---

## Part 2: Configuration

<!-- Source: configuration.md -->

# Ratatosk Configuration

Ratatosk accepts configuration from built-in defaults, a Redis-style config file, and environment overrides.

## Resolution Order

Ratatosk resolves configuration in this order:

1. Built-in defaults
2. `--config /path/to/ratatosk.conf`
3. `RATATOSK_CONFIG=/path/to/ratatosk.conf`
4. Auto-loaded `./ratatosk.conf` when present
5. Environment variable overrides

Use `--no-config-autoload` or `RATATOSK_DISABLE_CONFIG_AUTOLOAD=true` to disable step 4 while still allowing explicit `--config` and `RATATOSK_CONFIG`.

## CLI

```bash
# Validate the resolved configuration and startup preflight checks
ratatosk --check-config

# Print the effective configuration as Redis-style text
ratatosk --print-config text

# Print the effective configuration as JSON
ratatosk --print-config json

# Start with an explicit config file
ratatosk --config /etc/ratatosk/ratatosk.conf

# Require explicit config selection; ignore local ./ratatosk.conf
ratatosk --no-config-autoload --config /etc/ratatosk/ratatosk.conf
```

## Config File Format

- One directive per line
- `#` starts a comment
- Values with spaces should be quoted
- Empty string values can be written as `""`
- `CONFIG REWRITE` writes a round-trippable `ratatosk.conf` under the active `dir`

Example:

```conf
bind 127.0.0.1
port 6380
dir "/var/lib/ratatosk data"
dbfilename "snapshot data.rdb"
appendonly yes
appendfsync everysec
```

## Directives

### Network

| Directive | Env Override | Default |
| --- | --- | --- |
| `bind` | `RATATOSK_BIND` | `127.0.0.1` |
| `port` | `RATATOSK_PORT` | `6379` |
| `maxclients` | `RATATOSK_MAX_CLIENTS` | `4096` |
| `timeout` | `RATATOSK_TIMEOUT` | `0` |
| `client-timeout-sec` | `RATATOSK_CLIENT_TIMEOUT` | `0` |
| `output-buffer-limit-bytes` | `RATATOSK_OUTPUT_BUFFER_LIMIT_BYTES` | `8388608` |
| `shutdown-grace-ms` | `RATATOSK_SHUTDOWN_GRACE_MS` | `10000` |

### Compatibility

| Directive | Env Override | Default |
| --- | --- | --- |
| `compatibility-mode` | `RATATOSK_COMPATIBILITY_MODE` | `compat` |

`compat` (default) accepts `syntax_only` / unsupported commands as no-ops for
Redis-client friendliness. `strict` makes those commands — plus `WAIT` /
`WAITAOF` (which imply replica-backed acknowledgement a single node cannot
honour) — return a structured error instead of a misleading success. Runtime
toggle: `CONFIG SET compatibility-mode strict`. Full policy and the exact blocked
set: `docs/PRODUCT_CONTRACT.md`.

### Security

| Directive | Env Override | Default |
| --- | --- | --- |
| `protected-mode` | `RATATOSK_PROTECTED_MODE` | `yes` |

`protected-mode yes` (default) makes a non-loopback bind refuse to start while the
`default` ACL user is still `nopass`. Supply a bootstrap password with
`RATATOSK_DEFAULT_USER_PASSWORD` / `RATATOSK_DEFAULT_USER_PASSWORD_HASH`, or set
`protected-mode no` (equivalent to `RATATOSK_ALLOW_DEFAULT_USER_NOPASS=true`) as an
explicitly insecure opt-out. Loopback binds are unaffected (the convenience default
for local development). The bind-time guard is evaluated **at startup**; the value
is introspectable at runtime via `CONFIG GET protected-mode` / `INFO server`
(`ratatosk_protected_mode`), `CONFIG SET protected-mode …` updates it, and
`CONFIG REWRITE` persists it — but changing it on a running server does not
re-evaluate the already-bound socket.

### Persistence

| Directive | Env Override | Default |
| --- | --- | --- |
| `dir` | `RATATOSK_DIR` | `.` |
| `dbfilename` | `RATATOSK_DBFILENAME` | `dump.rdb` |
| `appendonly` | `RATATOSK_APPENDONLY` | `no` |
| `appendfsync` | `RATATOSK_APPENDFSYNC` | `everysec` |
| `save` | `RATATOSK_SAVE` | `3600 1 300 100 60 10000` |

### Memory And Runtime

| Directive | Env Override | Default |
| --- | --- | --- |
| `maxmemory` | `RATATOSK_MAXMEMORY` | `0` |
| `maxmemory-policy` | `RATATOSK_MAXMEMORY_POLICY` | `noeviction` |
| `maxmemory-samples` | `RATATOSK_MAXMEMORY_SAMPLES` | `5` |
| `lazyfree-lazy-expire` | `RATATOSK_LAZYFREE_LAZY_EXPIRE` | `no` |
| `lazyfree-lazy-server-del` | `RATATOSK_LAZYFREE_LAZY_SERVER_DEL` | `no` |
| `lazyfree-lazy-user-del` | `RATATOSK_LAZYFREE_LAZY_USER_DEL` | `no` |
| `hz` | `RATATOSK_HZ` | `10` |
| `active-expire-cycle-lookups` | `RATATOSK_ACTIVE_EXPIRE_CYCLE_LOOKUPS` | `20` |
| `active-expire-cycle-threshold-pct` | `RATATOSK_ACTIVE_EXPIRE_CYCLE_THRESHOLD_PCT` | `25` |
| `query-buffer-limit` | `RATATOSK_QUERY_BUFFER_LIMIT` | `1048576` |
| `output-buffer-flush-threshold` | `RATATOSK_OUTPUT_BUFFER_FLUSH_THRESHOLD` | `16384` |
| `client-write-timeout-sec` | `RATATOSK_CLIENT_WRITE_TIMEOUT_SEC` | `5` |
| `tcp-keepalive` | `RATATOSK_TCP_KEEPALIVE` | `300` |

### Pub/Sub And Observability

| Directive | Env Override | Default |
| --- | --- | --- |
| `notify-keyspace-events` | `RATATOSK_NOTIFY_KEYSPACE_EVENTS` | `""` |
| `pubsub-queue-hard-limit` | `RATATOSK_PUBSUB_QUEUE_HARD_LIMIT` | `4096` |
| `pubsub-queue-soft-limit` | `RATATOSK_PUBSUB_QUEUE_SOFT_LIMIT` | `2048` |
| `pubsub-queue-soft-seconds` | `RATATOSK_PUBSUB_QUEUE_SOFT_SECONDS` | `60` |
| `slowlog-log-slower-than` | `RATATOSK_SLOWLOG_LOG_SLOWER_THAN` | `-1` |
| `slowlog-max-len` | `RATATOSK_SLOWLOG_MAX_LEN` | `128` |
| `latency-tracking` | `RATATOSK_LATENCY_TRACKING` | `no` |

## Operational Notes

- Non-loopback `bind` still requires `RATATOSK_ALLOW_INSECURE_BIND=true`
- For non-loopback bind, also set `RATATOSK_DEFAULT_USER_PASSWORD` or `RATATOSK_DEFAULT_USER_PASSWORD_HASH`
- `RATATOSK_ALLOW_DEFAULT_USER_NOPASS=true` is still available, but it is explicitly insecure
- `--check-config` validates both config parsing and startup preflight access to persistence and audit paths
- Inspection commands avoid starting the metrics listener so `--print-config json` remains machine-readable

---

## Part 3: Ecosystem Integration

<!-- Source: ecosystem.md -->

# Ratatosk Ecosystem Integration

Ratatosk은 RESP3 기반 인메모리 데이터 스토어이며, 캐시 + Pub/Sub 이벤트 버스 역할을 맡는다.
이 문서는 **현재 코드 상태**(2026-06-02)와 외부 프로젝트 통합 기준을 정리한다.

## Implementation Status (2026-06-02)

기준 파일: `docs/redis-gap-ledger.json`

- 명령 카탈로그: `420` entries
- status summary: `done=420`
- capability tier summary: `unsupported=63`, `syntax_only=6`, `baseline_local=76`, `behavioral_subset=275`, `distributed_parity=0`
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
| AOF recovery | 완료 | RESP 파싱 -> execute 재생 |
| AOF manifest | 부분 구현 | save/load, bootstrap/recovery, manifest switch helper, rewrite 후 새 INCR rotation 연결. BASE materialization과 full atomic switch는 아직 없음 |
| Background save | 완료 | `BGSAVE` background snapshot worker + shutdown drain |
| AOF rewrite | 완료 | `BGREWRITEAOF` background rewrite worker. Redis식 current-state compaction은 아님 |
| AOF 서버 통합 | 완료 | write 명령 후 자동 append 경로 존재 |
| Blocking commands | 완료 | blocked wait registry + producer-side wakeup (list/sorted-set/stream) |
| Client tracking | 부분 구현 | direct/BCAST/PREFIX/NOLOOP/OPTIN/OPTOUT + redirect wakeup. Redis full contract는 아직 |
| Replication | 부분 구현 | role 전이, logical repl offset, replica ACK accounting. `PSYNC`는 ERR 반환 (standalone mode), `REPLICAOF`는 `NO ONE` 이외 ERR. backlog/network stream/failover 없음 |
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
- per-subscriber `tokio::sync::mpsc` 채널 기반 push delivery. `try_send()` overflow 시 subscriber disconnect.

### 3) Persistent data store

- RDB snapshot으로 주기적 데이터 백업.
- AOF로 명령 단위 durability 확보.
- 서버 재시작 시 RDB -> AOF 순서로 데이터 복구.

### 4) Redis-compatible endpoint

- 기존 Redis 클라이언트/SDK를 그대로 붙여 초기 통합 비용을 낮춤.
- 운영 도구(`INFO`, `CONFIG`, `SLOWLOG`, `LATENCY`, `MONITOR`)를 기본 제공. `MONITOR`는 실행된 명령을 모니터링 클라이언트로 broadcast하는 baseline 수준으로 동작한다 (full Redis parity는 아님).

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
- **Recommended coexistence port**: `6380` -- avoids collision with a co-located Redis instance.
  Set `RATATOSK_PORT=6380` when running alongside Redis. A startup warning is emitted when using port 6379.
- **Dynamic sidecar port**: `RATATOSK_PORT=0` asks the OS to choose an ephemeral loopback port.
  Sidecar supervisors should set `RATATOSK_BOUND_ADDR_FILE=/path/to/bound-addr.json`; Ratatosk
  writes `{"bound_addr":"127.0.0.1:<port>","bound_port":<port>}` after the TCP listener binds.
  The structured startup log event `ratatosk listener bound` also includes `bound_port`.

### Autostart (systemd --user)

```bash
./scripts/install-ratatosk-launcher.sh
./scripts/install-ratatosk-autostart.sh
```

- autostart unit은 기본 포트 `6380`을 사용해 수동 실행과 충돌을 피한다.
- unit 파일 경로: `~/.config/systemd/user/ratatosk-serve.service`

### Autostart (macOS launchd)

```bash
./scripts/install-ratatosk-autostart-macos.sh
```

- macOS 기본 포트: `6379` -- Kirei bridges.toml 설정과 일치.
- plist 경로: `~/Library/LaunchAgents/dev.ratatosk.serve.plist`
- 로그 경로: `~/Library/Logs/Ratatosk/`
- 데이터/audit 경로: `<repo>/data/`

## Runtime Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `RATATOSK_BIND` | `127.0.0.1` | listen address |
| `RATATOSK_PORT` | `6379` | listen port; `0` selects an OS-assigned ephemeral port |
| `RATATOSK_BOUND_ADDR_FILE` | unset | optional sidecar handoff file for the actual bound address/port when using `RATATOSK_PORT=0` |
| `RATATOSK_MAX_CLIENTS` | `4096` | concurrent connection cap |
| `RATATOSK_OUTPUT_BUFFER_LIMIT_BYTES` | `8388608` | per-client output limit |
| `RATATOSK_SHUTDOWN_GRACE_MS` | `10000` | graceful drain window |
| `RATATOSK_ALLOW_INSECURE_BIND` | unset | non-loopback bind opt-in |
| `RATATOSK_SHUTDOWN_BEST_EFFORT` | unset | allow shutdown to continue after appendonly flush failure |
| `RATATOSK_AUDIT_LOG` | `/tmp/ratatosk-audit.log` | append-only audit event log path |
| `RATATOSK_AUDIT_CHAIN_STATE` | `/tmp/ratatosk-audit-chain.state` | audit chain checkpoint path |

런타임 `CONFIG SET` 지원:
- `timeout`, `hz`, `appendonly`, `appendfsync` (always/everysec/no)
- `compatibility-mode`, `protected-mode`, `dbfilename`, `dir`, `save`
- `slowlog-log-slower-than`, `slowlog-max-len`, `latency-tracking`
- `pubsub-queue-hard-limit` (mpsc channel capacity), `pubsub-queue-soft-limit`, `pubsub-queue-soft-seconds`
- `active-expire-cycle-lookups`, `active-expire-cycle-threshold-pct`
- `query-buffer-limit`, `output-buffer-flush-threshold`, `client-write-timeout-sec`
- `maxmemory`/`maxmemory-policy`/`maxmemory-samples`, `notify-keyspace-events`, `tcp-keepalive`, `lazyfree-lazy-*`는 `CONFIG GET`에서만 노출되며 런타임 `CONFIG SET`으로는 변경할 수 없다(catch-all에서 `ERR Unknown option` 반환).

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

### Key Prefix Convention

Ratatosk does **not** implement built-in key-prefix enforcement — there is no
`enforce-key-prefix` directive and any key name is accepted. The prefixes below
are an operational **convention** for services that coexist on one instance,
enforced by clients/operators rather than by the server:

- `conductor:`, `ironclaw:`, `cc:`, `muninn:`, `rustmux:`

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
- AUTH brute force prevention: per-connection progressive delay (지수 백오프 + 지터, 최대 2초) + 5회 연속 실패 시 연결 종료, per-IP `AuthRateLimiter` (60초 윈도우 내 20회 실패 시 거부).
- audit trail은 append log를 `flush + sync_all` 한 뒤 checkpoint state를 atomic rename으로 저장한다. checkpoint가 뒤처져도 startup에서 durable audit log를 우선해 복구한다.
- `MONITOR`는 `+OK`를 반환하고 해당 연결을 monitor 모드로 등록하며, 이후 실행된 명령을 `Arc<Notify>` 기반으로 등록된 클라이언트에게 broadcast한다 (baseline 수준, full Redis parity는 아님). 명령 인자가 그대로 노출되므로 신뢰된 운영 연결에서만 사용할 것.
- TLS 종단은 프록시 계층(stunnel, nginx stream, envoy 등)에서 처리 권장.

### Backpressure and limits

- query buffer limit: 1 MiB
- output buffer limit: 기본 8 MiB
- pubsub delivery: per-subscriber `mpsc::channel` (capacity = hard_limit). `try_send()` 실패 시 overflow -> disconnect
- lazy free channel capacity: 4096

## Roadmap Priorities

1. ~~**Persistence 서버 통합**~~: 완료 -- AOF writer 이벤트 루프 연결, background RDB save 구현
2. ~~**AOF rewrite**~~: 완료 -- `BGREWRITEAOF` background rewrite worker 연결
3. ~~**Notification wiring**~~: 완료 -- `notify!` 매크로 삽입
4. ~~**SharedState concurrency model**~~: 완료 -- `AtomicStatsState` (10 lock-free counters), `ArcSwap<ConfigState>` lock-free config reads, atomic `next_client_id`, per-DB `parking_lot::RwLock<DbShard>` (서로 다른 DB 병렬 접근, 같은 DB 읽기 공유)
5. ~~**Pub/Sub push delivery**~~: 완료 -- per-subscriber `mpsc::channel` 기반 push delivery, `WaitResult` enum client loop
6. ~~**Lua 5.1 scripting**~~: 완료 -- `lua-scripting` feature gate, EVAL/EVALSHA/SCRIPT LOAD/EXISTS/FLUSH, sandbox (1MB mem / 100K instr limit)
7. **Persistence 재설계**: snapshot clone -> iterable view, AOF rewrite -> current-state materialization, multipart manifest atomic switch
8. **Redis parity hardening**: edge-case semantics를 Redis와 byte-level 비교 검증
9. **Replication 실체화**: backlog, network stream, WAIT/WAITAOF blocking semantics
10. **통합 계약 테스트**: 주요 서비스별 smoke + failure-path 자동화
11. **성능 회귀 자동화**: benchmark guardrail CI 루틴 고정

---

## Part 4: Ecosystem Port Configuration

<!-- Source: ecosystem-ports.md -->

# Ecosystem Port Configuration

Standard port assignments and configuration for all services in the ecosystem.

## Quick Reference

| Service | Port | Protocol | Purpose | Env Override |
|---------|------|----------|---------|--------------|
| Ratatosk | 6379 | TCP (RESP3) | Redis-compatible data store (default) | `RATATOSK_PORT` |
| Ratatosk | 6380 | TCP (RESP3) | Recommended coexistence port | `RATATOSK_PORT` |
| Muninn | 6333 | HTTP | REST API (axum) | `MUNINN_PORT` |
| Muninn | 6334 | gRPC | gRPC API (tonic) | `MUNINN_GRPC_PORT` |
| Conductor | 9100 | TCP (JSON-RPC) | Command-center bridge | `CONDUCTOR_COMMAND_CENTER_ADDR` |
| Conductor | 8090 | HTTP | Planner (FastAPI/uvicorn) | `CONDUCTOR_PLANNER_URL` |
| Ironclaw | 8080 | HTTP/WebSocket | Gateway | `IRONCLAW_GATEWAY_PORT` |

## Ratatosk

RESP3 in-memory data store serving as cache and Pub/Sub event bus.

| Property | Value |
|----------|-------|
| Default port | `6379` |
| Recommended coexistence port | `6380` (avoids collision with co-located Redis) |
| Port env var | `RATATOSK_PORT` |
| Bind address | `127.0.0.1` |
| Bind env var | `RATATOSK_BIND` |
| Protocol | TCP (RESP2/RESP3) |
| TLS | Not built-in; proxy-layer termination recommended (stunnel, nginx stream, envoy) |

A startup warning is emitted when using port 6379. The systemd autostart unit defaults to 6380.

## Muninn

Vector database with REST and gRPC interfaces for semantic search and memory.

### REST API

| Property | Value |
|----------|-------|
| Default port | `6333` |
| Port env var | `MUNINN_PORT` |
| Bind address | `127.0.0.1` |
| Bind env var | `MUNINN_HOST` |
| Protocol | HTTP (axum) |
| TLS | Not built-in; non-loopback bind requires `MUNINN_ALLOW_INSECURE_BIND=true` or TLS proxy |

### gRPC API

| Property | Value |
|----------|-------|
| Default port | `6334` |
| Port env var | `MUNINN_GRPC_PORT` |
| Bind address | `127.0.0.1` (shares `MUNINN_HOST`) |
| Protocol | gRPC (tonic) |
| TLS | Not built-in; same insecure-bind guard as REST |

## Conductor

Workflow execution engine with a JSON-RPC bridge and HTTP planner.

### Command-center bridge (TCP)

| Property | Value |
|----------|-------|
| Default address | `127.0.0.1:9100` |
| Env var | `CONDUCTOR_COMMAND_CENTER_ADDR` (full `host:port`) |
| Protocol | TCP line-delimited JSON-RPC |
| TLS | Not built-in |

### Platform API

| Property | Value |
|----------|-------|
| Default address | `127.0.0.1:9150` |
| Env var | `CONDUCTOR_PLATFORM_API_ADDR` (full `host:port`) |
| Protocol | TCP JSON-RPC |
| TLS | Not built-in |

### Planner (HTTP)

| Property | Value |
|----------|-------|
| Default URL | `http://127.0.0.1:8090` |
| Env var | `CONDUCTOR_PLANNER_URL` (full URL) |
| Protocol | HTTP (FastAPI/uvicorn) |
| TLS | Required for non-local hosts (enforced by `PlannerEndpoint`) |

## Ironclaw

AI agent gateway serving HTTP and WebSocket connections.

| Property | Value |
|----------|-------|
| Default port | `8080` |
| Port env var | `IRONCLAW_GATEWAY_PORT` |
| Bind address | `127.0.0.1` (loopback) |
| Bind env var | `IRONCLAW_GATEWAY_BIND` |
| Protocol | HTTP + WebSocket |
| TLS | Configurable via `gateway.require_tls`; proxy-layer termination supported via `trusted_proxies` |

## Standardized Naming Convention

Environment variables follow a `{SERVICE}_{COMPONENT}` pattern:

| Pattern | Examples |
|---------|----------|
| `{SERVICE}_PORT` | `RATATOSK_PORT`, `MUNINN_PORT`, `IRONCLAW_GATEWAY_PORT` |
| `{SERVICE}_BIND` | `RATATOSK_BIND`, `MUNINN_HOST`, `IRONCLAW_GATEWAY_BIND` |
| `{SERVICE}_ADDR` | `CONDUCTOR_COMMAND_CENTER_ADDR` (combined `host:port`) |
| `{SERVICE}_URL` | `CONDUCTOR_PLANNER_URL` (full URL with scheme) |

Conventions:
- Separate `_PORT` / `_BIND` variables when the service uses a simple TCP/HTTP listener.
- Combined `_ADDR` (`host:port`) when the variable configures a connection target rather than a listener.
- Full `_URL` (with scheme) when the protocol may vary (HTTP vs HTTPS).
- All services default to loopback (`127.0.0.1`) and require explicit opt-in for non-loopback binding.

## Cross-Service Dependencies

| Consumer | Dependency | Default Target | Env Override (consumer side) |
|----------|------------|----------------|------------------------------|
| Ironclaw | Ratatosk (cache) | `redis://127.0.0.1/` | `IRONCLAW_STORAGE_REDIS_URL` |
| Ironclaw | Muninn (memory) | `http://127.0.0.1:8000` | `IRONCLAW_STORAGE_MUNINN_URL` |
| Ironclaw | Conductor (bridge) | env-only, no default | `IRONCLAW_CONDUCTOR_ADDR` |
| Conductor | Muninn | env-only | `CONDUCTOR_MUNINN_ADDR` |
| Conductor | Ratatosk | via planner/runtime | `CONDUCTOR_PLANNER_URL` |

---

## Part 5: Health Protocol

<!-- Source: health-protocol.md -->

# Ecosystem Health Protocol

Standard health response format used across all components.

## Wire Format

```json
{
  "status": "healthy",
  "timestamp_unix_s": 1710518400,
  "version": "0.1.0",
  "bridge_contract_version": "0.1",
  "components": [
    {
      "name": "storage",
      "status": "ok",
      "message": null
    },
    {
      "name": "cache",
      "status": "degraded",
      "message": "circuit breaker open"
    }
  ]
}
```

## Status Values

### Top-level `status`

| Value | Meaning | HTTP Code |
|-------|---------|-----------|
| `healthy` | All components operational | 200 |
| `degraded` | Some components impaired, core function continues | 200 |
| `unhealthy` | Critical failure, not serving traffic | 503 |

### Component `status`

| Value | Meaning |
|-------|---------|
| `ok` | Fully operational |
| `degraded` | Impaired but functional |
| `failed` | Not operational |
| `unknown` | Status cannot be determined |
| `noop` | Using noop/stub implementation |

## Aggregation Rule

- Any component `failed` -> top-level `unhealthy`
- Any component `degraded` or `noop` -> top-level `degraded`
- All components `ok` -> top-level `healthy`

## Per-Component Implementation

### Ratatosk

- **Endpoint**: `INFO server` section (field: `health_status`)
- **Components**: persistence (AOF latch + last RDB/AOF rewrite status + audit checkpoint state), memory (`maxmemory` headroom when configured)
- **Contract version**: Exposed in `INFO server` as `bridge_contract_version`

Current Ratatosk mapping:
- `healthy`: persistence status is OK, audit chain is not dirty, and memory headroom is within configured `maxmemory` budget
- `degraded`: last RDB/AOF rewrite status is error, audit checkpoint is dirty, or storage headroom is low
- `unhealthy`: AOF write is latched or cached memory estimate exceeds configured `maxmemory`

Ratatosk persistence/health surfaces also expose:
- `INFO persistence`: `audit_chain_dirty`, `audit_recovery_status`
- `PING HEALTH`: `status`, `audit_chain_dirty`, `audit_recovery_status`

### Muninn

- **Endpoint**: `GET /health`
- **Components**: inference (engine health), disk (space check), storage (write probe), recovery (WAL replay status)
- **Contract version**: Field in health JSON response

### Conductor

- **Endpoint**: Bridge health handler (JSON-RPC `health` method)
- **Components**: planner (HTTP ready check), sqlite (ping), memory (adapter health), cache (circuit breaker state), inference_pool (if configured)
- **Contract version**: Field in health JSON response

### Ironclaw

- **Endpoint**: `health` RPC method
- **Components**: storage (session backend), channels (per-channel connected status), memory (Muninn gateway), agent (LLM provider), mcp (server statuses)
- **Contract version**: Field in health JSON response

## Probing

Upstream services should:
1. Check `status` field first (fast path)
2. Inspect `components` only when `status != "healthy"`
3. Use `bridge_contract_version` to detect version skew
4. Treat missing fields as `unknown` (forward-compatible)

## Timeouts

- Health probes: 2 second timeout
- Dependency probes within health: 1 second each
- Disk probes: 500ms

---

## Part 6: Observability

<!-- Source: observability.md -->

# Ratatosk Observability Guide

기준일: 2026-06-02

이 문서는 Ratatosk single-node 운영에 필요한 최소 관측성 구성을 정리한다.

## 1. Metrics

- Prometheus exporter 기본 bind: `127.0.0.1:9090`
- override: `RATATOSK_METRICS_BIND`
- exporter 초기화 실패는 기본적으로 startup failure다
- 예외적으로 `RATATOSK_ALLOW_NO_METRICS=true`에서만 metrics 없이 계속 실행할 수 있다

근거: `crates/ratatosk-server/src/metrics.rs`, `crates/ratatosk-server/src/main.rs`

## 2. Health Surfaces

- `INFO server`: `health_status`, `bridge_contract_version`
- `INFO persistence`: audit / persistence status fields
- `PING HEALTH`: detailed human-readable health payload

## 3. Starter Alert Pack

Prometheus rule file:

- `monitoring/prometheus/ratatosk-alerts.yml`

Grafana starter dashboard:

- `monitoring/grafana/ratatosk-single-node-dashboard.json`

## 4. Minimum Alert Set

- AOF write latched
- RDB save errors
- AOF write errors
- accept error surge
- FD utilization high
- memory estimate stale
- blocking retry deadline exhausted

## 5. Runbook Policy

모든 page-level alert는 아래를 가져야 한다.

- what happened
- user impact
- immediate mitigation
- verification step
- rollback or recovery step

## 6. Ship Gate

single-node GA 전 필수:

- metrics scrape 가능
- alert rules load 가능
- dashboard import 가능
- health/INFO surfaces documented

---

## Part 7: Support & Versioning Policy

<!-- Source: support-and-versioning-policy.md -->

# Support And Versioning Policy

기준일: 2026-03-26

이 문서는 Ratatosk의 릴리스, 버전 호환성, 지원 범위, 저장소 거버넌스 정책을 정의한다.

## 1. Versioning

- Ratatosk는 [Semantic Versioning 2.0.0](https://semver.org/)를 따른다.
- `0.y.z` 구간에서는 빠른 정리가 우선되며, minor에도 breaking change가 들어갈 수 있다.
- `1.0.0` 이후에는 public contract 범위에서:
  - patch: bug fix only
  - minor: backward-compatible feature
  - major: breaking change

## 2. Public Contract Surface

SemVer는 아래 surface에만 적용한다.

- documented CLI/runtime environment contract
- documented standalone Redis-compatible command subset
- persisted on-disk format contract that release notes에서 호환성을 약속한 범위
- metrics/health fields documented as stable

다음은 기본적으로 stable contract에 포함하지 않는다.

- experimental feature gates
- syntax-only or unsupported commands
- undocumented internal metrics
- ad-hoc environment variables with no docs entry

## 3. Release Channels

- development: `main`
- release candidate: `vX.Y.Z-rcN` tag
- general availability: `vX.Y.Z` tag

모든 GA release는 최소 1개의 RC를 거친다.

## 4. Required Release Gates

GA 전 필수:

- `cargo fmt --all --check`
- `cargo check --workspace --quiet`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace --quiet`
- `cargo audit`
- `cargo deny check advisories bans licenses sources`
- performance guardrail workflow
- supported command differential tests
- Redis interop workflow
- persistence recovery/failure-path tests

## 5. Repository Governance

필수 저장소 설정:

- protected default branch
- required status checks
- CODEOWNERS review
- signed tags for release
- release notes and changelog update per release

권장 required checks:

- Rust CI
- Security
- Gap Ledger
- Performance Guardrail
- Redis Interop

## 6. Support Windows

초기 정책:

- latest GA: full support
- previous GA minor: security and critical bug fixes only
- pre-GA tags: no support guarantee

`0.y.z` 동안에는 빠른 변경이 가능하므로, 운영 투입은 latest patch만 권장한다.

## 7. Change Management

breaking change는 아래를 포함해야 한다.

- changelog entry
- migration note
- rollback note
- persisted format 영향 여부
- compatibility tier 변경 여부

## 8. Release Artifacts

모든 GA artifact는 아래를 포함해야 한다.

- release binary
- checksum
- README and docs bundle
- example config

## 9. Security And Disclosure

- security-sensitive 이슈는 공개 issue 전에 비공개 경로를 우선한다
- dependency advisories는 Dependabot + cargo-audit + cargo-deny로 추적한다
- release note에는 known-risk와 mitigations를 함께 기록한다

---

## Part 8: Ship Readiness Plan

<!-- Source: ship-readiness-plan.md -->

# Ratatosk Ship Readiness Master Plan

기준일: 2026-03-26

이 문서는 Ratatosk 코드베이스와 공식 외부 문서를 함께 대조해, 현재 상태를 ship-ready 관점에서 점수화하고, 모든 카테고리를 10/10까지 끌어올리기 위한 상세 계획을 정리한 것이다.

핵심 결론은 간단하다.

- 현재 Ratatosk는 `public GA` 기준으로는 아직 `NO-SHIP`이다.
- 하지만 `single-node durable cache + pub/sub + local persistence server`라는 현재 README 경계 안에서는 매우 강한 프리프로덕션 단계다.
- 가장 현실적인 v1 목표는 "Redis 드롭인 대체재"가 아니라 "single-node Ratatosk GA"다.
- 만약 목표를 "Redis drop-in GA"로 잡으면, replication, Sentinel, Cluster, WAIT/WAITAOF, client-side caching, Functions까지 포함된 멀티쿼터 프로그램으로 커진다.

## 1. 평가 기준

### 점수 의미

- `10/10`: production shipment에 필요한 계약, 자동화, 검증, 문서 정합성이 모두 갖춰짐
- `8-9/10`: 강하지만 아직 운영/릴리스 안전장치가 일부 부족함
- `5-7/10`: 실제 사용은 가능하지만 ship gate를 통과하기엔 중요한 구멍이 남아 있음
- `0-4/10`: 구조적 blocker가 있어 먼저 설계/프로세스 수정이 필요함

### 권장 제품 경계

README가 이미 선언하듯 Ratatosk의 현재 경계는 다음과 같다.

- single-node only
- RESP2/RESP3 TCP server
- cache + pub/sub + local persistence
- RDB snapshot + AOF durability
- broad Redis command surface, but not full Redis distributed parity

근거: [README](../README.md#L3-L11), [What Ratatosk Does Not Provide](../README.md#L35-L41)

이 문서의 기본 계획은 이 경계를 기준으로 `v1 single-node GA`를 만드는 것이다.

## 2. 증거 기반 요약

### 코드베이스에서 확인한 사실

- Rust workspace 5개 crate로 구성되어 있다.
- `ratatosk-engine`가 가장 크고 핵심 복잡도를 거의 전부 떠안고 있다.
- 가장 큰 파일은 `crates/ratatosk-engine/src/command/mod.rs`로 1만 줄이 넘는다.
- 최근 리팩터링으로 동시성/상태 모델 문서를 상당 부분 맞췄지만, authoritative boundary와 future parallelism 설명은 더 선명해져야 한다.
- `SharedState.data`와 inner `ServerState.data`는 이제 같은 backing shard를 공유한다.
- `SharedState`에는 default-user ACL policy cache가 추가됐고, `PING` / `ECHO` / `TIME` / `DBSIZE` / `TYPE` / `EXISTS` / `GET` / `MGET` / `STRLEN` / `BITCOUNT` / `GETBIT` / `GETRANGE` / `SUBSTR` / `HGET` / `HMGET` / `HGETALL` / `HKEYS` / `HVALS` / `HEXISTS` / `HLEN` / `HSTRLEN` / `SISMEMBER` / `SMISMEMBER` / `SCARD` / `ZSCORE` / `ZCARD` / `ZMSCORE` / `ZCOUNT` / `ZLEXCOUNT` / `ZRANGE` / `ZRANGEBYSCORE` / `ZREVRANGEBYSCORE` / `ZRANGEBYLEX` / `ZREVRANGEBYLEX` / `ZREVRANGE` / `ZRANK` / `ZREVRANK` / `LLEN` / `LINDEX` / `LRANGE` / `TTL` / `PTTL` / `EXPIRETIME` / `PEXPIRETIME`는 조건부 lock-free fast path를 탄다.
- 이 커맨드들만으로 이뤄진 readonly pipeline은 batch 전체가 lock-free로 처리된다.
- fast path를 탈 수 없는 readonly batch도 더 이상 배치 전체를 한 번에 잠그지 않고, 명령 단위로 `meta` lock을 다시 잡으며 순차 실행한다.
- readonly batch gate는 fast path 집합 외에도 non-blocking readonly command spec을 받아들이며, `WAIT` / `WAITAOF` / connection / pubsub 계열은 제외한다.
- 일반 단건 경로와 readonly batch fallback은 이미 파싱한 argv를 재사용해서, locked path에서 같은 RESP frame을 다시 파싱하지 않는다.
- default-user `nopass` 승격과 즉시 `NOAUTH`로 끝나는 요청은 공용 precheck helper로 먼저 걸러서, 일부 실패/초기 인증 경로는 `meta` lock을 잡지 않게 됐다.
- fast path도 공용 post-execute helper를 타도록 맞춰져서 `MONITOR`, tracking reset, slowlog/latency 의미론을 locked path와 더 가깝게 유지한다.
- `MULTI` 안에서는 fast path가 비활성화돼 queue/`EXEC` 의미론을 우회하지 않는다.
- `stats`는 아직 완전히 단일화되지는 않았지만, outer atomic -> inner stats 역동기화 경로가 cron/client snapshot 경로에 추가됐고 `EXISTS`/`GET` fast path의 keyspace hit/miss도 atomic 쪽에서 먼저 반영된다.
- Redis gap ledger 기준 command surface는 넓지만 parity tier는 분산돼 있다.

근거:

- [SharedState definition](../crates/ratatosk-engine/src/keyspace.rs#L581-L640)
- [ServerState definition](../crates/ratatosk-engine/src/keyspace.rs#L813-L887)
- [Runtime execute path](../crates/ratatosk-server/src/client.rs#L375-L410)
- [Capability summary](./redis-gap-ledger.md#L11-L30)

### 로컬 검증 결과

2026-03-26 기준 로컬에서 아래를 확인했다.

- `cargo fmt --all --check` 통과
- `cargo check --workspace --quiet` 통과
- `cargo clippy --workspace --all-targets -- -D warnings` 통과
- `cargo test --workspace --quiet` 통과
- `cargo audit` 통과
- `cargo deny check advisories bans licenses sources` 통과
- `python3 scripts/perf_guardrail_check.py --log benchmarks/baseline-default-20260208-185945.log` 통과
- supported-subset Redis interop harness 추가 및 workspace test green

로컬 환경에는 `redis-server` binary가 없어 interop test는 skip 경로만 확인했다. 대신 저장소에는 `redis-server`를 설치해 해당 differential test를 실행하는 GitHub Actions workflow를 추가했다.

`cargo deny`는 현재 저장소 기준으로 복구됐다.

- `deny.toml`을 `cargo-deny 0.19.0` schema에 맞게 수정했다.
- workspace crate는 `publish = false`로 명시해 licensing/bans gate를 정리했다.

근거: [deny.toml](../deny.toml#L1-L13), [security workflow](../.github/workflows/security.yml#L1-L35), [cargo-deny advisories config](https://embarkstudios.github.io/cargo-deny/checks/advisories/cfg.html)

### 저장소 운영/릴리스 체계에서 확인한 사실

- `CHANGELOG.md`, `CODEOWNERS`, `dependabot.yml`, release/perf/redis-interop workflow가 추가됐다.
- alert rules와 Grafana dashboard starter artifact가 repo에 들어왔다.
- release tag, SBOM / provenance, protected branch 같은 외부 플랫폼 설정은 아직 남아 있다.
- 최근 commit 메시지 상당수가 `changes`, `aa`, `a`처럼 low-signal이다.

이 항목들은 코드 품질과 별개로 ship-ready 점수를 크게 깎는다.

## 3. 현재 점수표

| 카테고리 | 점수 | 한 줄 요약 |
| --- | ---: | --- |
| 아키텍처 정합성 | 6/10 | shared backing store, default ACL cache, 일부 single-command/batch lock-free fast path는 들어왔지만 meta lock serialization과 stats split이 남아 있음 |
| 선언 범위 기능 완성도 | 7/10 | single-node cache/pub/sub/persistence는 강함 |
| Redis 계약 정합성 | 6/10 | supported subset interop이 생겼지만 semantics 편차는 여전히 큼 |
| 지속성/복구 | 7/10 | RDB/AOF는 강하지만 rewrite/manifest 마감이 덜 됨 |
| 테스트/검증 | 7/10 | interop와 quality gate는 좋아졌지만 fuzz/property/chaos가 아직 없음 |
| 보안/공급망 | 7/10 | remote bind hardening과 dependency hygiene는 좋아졌지만 TLS/SBOM/provenance가 남음 |
| 관측성/운영 | 7/10 | unhealthy 상태, alert, dashboard가 추가됐지만 SLO/parity 정리는 남음 |
| 성능/용량 | 7/10 | guardrail은 좋지만 CI 고정과 capacity envelope가 없음 |
| 배포/롤백 | 6/10 | autostart/Nix/preflight는 좋지만 release packaging/canary/rollback drill이 없음 |
| 릴리스 엔지니어링 | 6/10 | changelog/policy/workflow는 생겼지만 tag/provenance/live repo policy가 남음 |

평균: `6.6/10`

## 4. 카테고리별 상세 평가와 10점 조건

### 4.1 아키텍처 정합성 -- 6/10

#### 현재 상태

`SharedState` 바깥에 `data`와 `stats`가 있고, `ServerState` 안에도 `data`와 `stats`가 있다. 다만 `data`는 이제 outer/inner가 같은 `DataState` backing shard를 공유한다. 여기에 default-user ACL policy cache와 `PING` / `ECHO` / `TIME` / `DBSIZE` / string/hash/bitmap/zset/list의 일부 readonly 커맨드용 lock-free fast path가 추가됐다. 반면 일반 명령 실행은 여전히 `server_state.meta.lock().await`로 `ServerState` mutex를 잡은 뒤 `execute()`로 들어가며, `stats`도 아직 완전 단일 소스는 아니다.

근거:

- [SharedState](../crates/ratatosk-engine/src/keyspace.rs#L581-L640)
- [ServerState](../crates/ratatosk-engine/src/keyspace.rs#L813-L928)
- [execute path](../crates/ratatosk-server/src/client.rs#L375-L410)

문서 설명은 최근 수정으로 개선됐지만, 아직도 장기 설계와 현재 runtime path를 분리해 설명할 필요가 있다.

- capability 문서는 "state mutation은 serialized"라고 설명한다.
- 실제 코드는 현재 기준으로 이 설명에 더 가깝다.

근거:

- [architecture doc](./architecture.md)
- [capability doc](./PRODUCT_CONTRACT.md)

#### 부족한 점

- DB data divergence 리스크는 줄었지만, authoritative access path가 하나로 정리되지는 않았다.
- stats는 여전히 dual-path다. 다만 cron과 client snapshot refresh가 outer atomic snapshot을 inner stats로 다시 흡수한다.
- authoritative boundary 설명이 아직 충분히 단순하지 않다.
- 장기적으로는 성능/정확성/운영 관측이 모두 이 구조에 발목 잡힌다.

#### 10점 조건

- DB data는 정확히 한 곳만 authoritative 해야 한다.
- stats도 정확히 한 곳만 authoritative 해야 한다.
- docs가 runtime path와 일치해야 한다.
- "different DB parallelism"이 실제 benchmark와 contention metric으로 입증되어야 한다.

#### 계획

- outer/inner `data` handle을 하나의 authoritative access path로 정리한다.
- `ServerAccess`를 재설계해서 DB read/write가 whole-command `meta` lock 안에 머물지 않게 한다.
- `ServerState.stats`와 `AtomicStatsState`를 병합한다.
- refactor 뒤 architecture doc과 capability doc을 같은 PR에서 업데이트한다.

#### 종료 조건

- 한 source of truth만 존재
- docs/runtime mismatch 0건
- lock-hold metric 감소 확인
- cross-DB parallel benchmark 추가

### 4.2 선언 범위 기능 완성도 -- 7/10

#### 현재 상태

현재 선언된 범위인 single-node cache/pubsub/local persistence는 꽤 강하다.

- RESP2/3 파서
- 주요 자료구조
- eviction/expiry/lazy free
- RDB/AOF
- Pub/Sub push delivery
- startup preflight
- autostart/Nix

근거:

- [README](../README.md#L3-L11)
- [ecosystem status](./operations.md)

#### 부족한 점

- 일부 운영 surface는 아직 consistency가 약하다.
- scripting/Functions/client tracking은 선언 범위 안에서도 완성도가 고르지 않다.
- contract 범위를 실제 interop suite가 아직 일부 subset만 덮고 있다.

#### 10점 조건

- README가 말하는 범위를 실제 product contract로 굳힌다.
- 그 범위에 필요한 기능은 모두 문서/테스트/운영 절차까지 닫는다.
- 범위 밖 기능은 명확히 unsupported 또는 experimental로 내린다.

#### 계획

- v1 product contract 문서를 별도로 만들고 README에서 링크한다.
- `COMMAND DOCS`와 gap-ledger에서 v1 지원 범위를 기계적으로 추출하게 만든다.
- range 밖 기능은 no-op/placeholder 대신 explicit unsupported 에러나 experimental feature gate로 전환한다.

#### 종료 조건

- v1 contract 문서 존재
- contract에 포함된 기능은 모두 smoke/interop/ops 문서까지 존재
- contract 밖 기능은 marketing과 runtime 모두에서 과장 없음

### 4.3 Redis 계약 정합성 -- 5/10

#### 현재 상태

gap-ledger 기준:

- total commands: 420
- `unsupported`: 63
- `syntax_only`: 6
- `baseline_local`: 76
- `behavioral_subset`: 275
- `distributed_parity`: 0

근거: [gap-ledger summary](./redis-gap-ledger.md#L11-L30)

특히 아래는 공식 Redis 계약과 차이가 크다.

- replication backlog / PSYNC 없음
- WAIT / WAITAOF blocking semantics 없음
- Sentinel 없음
- Cluster routing/MOVED/ASK 없음
- client-side caching / Functions는 부분 구현 또는 부재

근거:

- [capability declarations](./PRODUCT_CONTRACT.md)
- [Redis replication docs](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/)
- [WAIT docs](https://redis.io/docs/latest/commands/wait/)
- [WAITAOF docs](https://redis.io/docs/latest/commands/waitaof/)
- [Sentinel docs](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/)
- [Cluster docs](https://redis.io/docs/latest/operate/oss_and_stack/management/scaling/)
- [Client-side caching docs](https://redis.io/docs/latest/develop/reference/client-side-caching/)

#### 부족한 점

- "명령 존재"와 "Redis 의미론 보장"이 섞여 보인다.
- 일부 no-op/metadata-only command가 클라이언트에게 실제 지원처럼 보일 수 있다.

#### 10점 조건

이 카테고리의 10점은 두 가지 방식 중 하나여야 한다.

- `Option A`: single-node product로 범위를 좁히고, 지원 범위의 Redis semantics만 100% 맞춘다.
- `Option B`: 실제 replication/Sentinel/Cluster/WAIT/WAITAOF/Functions까지 구현한다.

#### 권장 계획

- v1은 `Option A`를 권장한다.
- `syntax_only`와 `baseline_local` 중 사용자가 오해하기 쉬운 command를 재분류한다.
- differential test를 지원 범위 전체에 붙인다.
- `COMMAND DOCS`가 capability tier를 항상 내보내도록 유지한다.

#### 종료 조건

- v1 supported subset에 대해 Redis differential test green
- unsupported/experimental command는 명확한 에러 또는 feature gate
- README/ledger/COMMAND DOCS/runtime behavior 일치

### 4.4 지속성/복구 -- 7/10

#### 현재 상태

Ratatosk는 persistence 쪽이 예상보다 강하다.

- RDB snapshot
- AOF writer
- startup replay
- legacy format gate
- incomplete chain gate
- shutdown flush gate
- BGREWRITEAOF smoke script

근거:

- [persistence status](./operations.md)
- [startup replay gate](../crates/ratatosk-server/src/persistence/aof.rs#L294-L351)
- [AOF bootstrap](../crates/ratatosk-server/src/persistence/aof.rs#L477-L513)

하지만 Redis 공식 persistence 모델과 비교하면 아직 차이가 있다.

- Redis는 background rewrite에서 minimal command set과 atomic manifest switch를 제공한다.
- Ratatosk는 manifest rewrite switch와 current-state materialization이 아직 부분 구현이다.

근거:

- [Ratatosk persistence status](./operations.md)
- [Redis persistence docs](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)

#### 부족한 점

- rewrite의 최종 안전 모델이 아직 미완성이다.
- power-loss / partial corruption / repeated rewrite failure에 대한 체계적 drill이 없다.

#### 10점 조건

- declared durability contract가 코드/테스트/문서에 닫혀 있어야 한다.
- rewrite/manifest/switch 경로가 원자성과 rollback safety를 만족해야 한다.
- backup/restore/partial corruption drill이 자동화돼야 한다.

#### 계획

- multipart manifest switch를 끝까지 완성하거나, 미완성 범위를 명확히 내린다.
- current-state materialization 기반 AOF rewrite 설계를 확정한다.
- kill -9, disk full, truncated AOF, missing BASE/INCR, legacy bypass 시나리오 테스트를 추가한다.
- backup/restore/rollback playbook을 문서화하고 smoke script를 늘린다.

#### 종료 조건

- persistence failure-path test matrix green
- one-command durability claim이 fsync 정책별로 문서화됨
- restore drill과 rollback drill 1회 이상 검증 완료

### 4.5 테스트/검증 -- 7/10

#### 현재 상태

- workspace quality gate는 좋다.
- 소스에서 `#[test]`/`#[tokio::test]`는 300개 이상 확인된다.
- persistence, protocol, command, event loop, rate limiter, performance 관련 테스트가 있다.
- supported subset에 대한 Redis interop harness와 CI workflow가 추가됐다.

#### 부족한 점

- fuzz 없음
- proptest/quickcheck 없음
- loom 같은 concurrency model 검증 없음
- chaos/fault injection 자동화 부족
- real Redis differential coverage가 아직 narrow subset이다.

#### 10점 조건

- unit/integration/perf 외에 fuzz/property/concurrency/crash test가 추가돼야 한다.
- supported command set은 실제 Redis와 기계적으로 비교해야 한다.

#### 계획

- RESP parser fuzzing 추가
- RDB/AOF roundtrip property tests 추가
- blocking wakeup / tracking invalidation / stats invariants에 대한 targeted concurrency tests 추가
- real Redis differential harness를 supported subset 전체로 확장
- performance regression check를 CI mandatory gate로 승격

#### 종료 조건

- supported command differential suite green
- parser/persistence fuzz target 운영
- concurrency invariant tests green
- perf gate CI required

### 4.6 보안/공급망 -- 7/10

#### 현재 상태

좋은 점:

- loopback bind 기본
- non-loopback bind는 explicit opt-in 필요
- non-loopback bind에서는 password/hash bootstrap 없이 default `nopass` user를 유지하지 않음
- AUTH brute-force 완화
- audit trail
- gitleaks
- cargo audit workflow 존재
- cargo deny green
- Dependabot / CODEOWNERS 존재

근거:

- [config insecure bind guard](../crates/ratatosk-server/src/config.rs#L195-L198)
- [rate limiter](../crates/ratatosk-server/src/rate_limiter.rs#L1-L163)
- [security defaults doc](./operations.md)
- [security workflow](../.github/workflows/security.yml#L1-L35)

주의할 점:

- loopback/개발 환경에서는 default ACL user가 여전히 `nopass` + full access다.
- built-in TLS가 없다.
- SBOM 생성 workflow(`.github/workflows/sbom.yml`)는 추가됐지만 release artifact signing/provenance는 아직 없다.

근거:

- [default ACL user](../crates/ratatosk-engine/src/acl.rs#L21-L29)
- [TLS note](./operations.md)
- [cargo-deny docs](https://embarkstudios.github.io/cargo-deny/checks/advisories/cfg.html)
- [CODEOWNERS docs](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-code-owners)
- [Dependabot docs](https://docs.github.com/en/code-security/how-tos/secure-your-supply-chain/secure-your-dependencies/configuring-dependabot-version-updates)
- [SBOM docs](https://docs.github.com/en/code-security/how-tos/secure-your-supply-chain/establish-provenance-and-integrity/exporting-a-software-bill-of-materials-for-your-repository)

#### 10점 조건

- production profile에서 인증/노출/의존성 검사가 안전하게 닫혀 있어야 한다.
- dependency hygiene가 자동화돼야 한다.
- release artifact의 provenance와 inventory를 남길 수 있어야 한다.

#### 계획

- non-loopback bind 시 `default nopass` 금지
- production bootstrap secret 또는 mandatory ACL bootstrap flow 추가
- `cargo deny` 설정 수정 및 CI mandatory
- `.github/dependabot.yml` 추가
- `CODEOWNERS` 추가
- SBOM 생성 workflow 추가
- release artifact checksum/signing/provenance 정책 추가

#### 종료 조건

- `cargo audit` + `cargo deny` green
- Dependabot PR이 자동 생성됨
- protected branch + CODEOWNERS + signed commits 정책 문서화
- production bootstrap without password impossible

### 4.7 관측성/운영 -- 7/10

#### 현재 상태

좋은 점:

- Prometheus exporter 존재
- health protocol 문서 존재
- panic crash dump 존재
- startup preflight 존재
- autostart runbook 존재
- `unhealthy` health state 존재
- alert rules와 Grafana dashboard starter artifact 존재

근거:

- [metrics exporter](../crates/ratatosk-server/src/metrics.rs#L1-L29)
- [health protocol](./operations.md)
- [panic hook and crash files](../crates/ratatosk-server/src/main.rs#L110-L180)
- [startup preflight](../crates/ratatosk-server/src/main.rs#L305-L420)

하지만:

- Alertmanager config(`monitoring/alertmanager/alertmanager.yml`)와 SLO/SLI 문서(`docs/SLO.md`)는 추가됐다.
- metrics와 `INFO` stats가 같은 truth source를 보지 않는다.

근거:

- [health status implementation](../crates/ratatosk-engine/src/command/cmd_server.rs#L1367-L1390)
- [health protocol note](./operations.md)
- [Prometheus instrumentation docs](https://prometheus.io/docs/practices/instrumentation/)
- [Prometheus alerting docs](https://prometheus.io/docs/practices/alerting/)
- [Alertmanager docs](https://prometheus.io/docs/alerting/latest/alertmanager/)

#### 10점 조건

- metrics, INFO, health가 같은 상태를 반영해야 한다.
- alert rules와 dashboard가 repo에 있어야 한다.
- SLO, paging policy, runbook link가 alert와 연결돼야 한다.

#### 계획

- stats truth source 단일화
- `unhealthy` health state 추가
- Prometheus alert rules 추가
- example Grafana dashboard 추가
- "무엇을 alert할지"와 "무엇은 page하지 않을지" 문서화
- blackbox monitoring path 추가

#### 종료 조건

- alert rules + dashboard + runbook PR merge
- one-node 운영에서 필요한 symptom-based alert set 존재
- health/INFO/Prometheus counters parity tests green

### 4.8 성능/용량 -- 7/10

#### 현재 상태

- benchmark baseline이 있다.
- perf guardrail checker가 있다.
- canonical log 기준 guardrail은 PASS다.

근거: [performance doc](./optimization.md)

#### 부족한 점

- perf gate가 CI required가 아니다.
- sustained load, p95/p99, max client envelope, memory growth envelope가 문서화되어 있지 않다.
- 현재 whole-command meta lock은 성능 ceiling을 낮출 수 있다.

#### 10점 조건

- release마다 성능 회귀가 자동 검출돼야 한다.
- latency/throughput/capacity envelope가 명시돼야 한다.
- architecture refactor의 효과가 측정돼야 한다.

#### 계획

- benchmark guardrail CI 추가
- load test scenario 정의
- p50/p95/p99, throughput, memory, connection cap, eviction behavior를 측정
- post-refactor lock wait / hold metrics 비교

#### 종료 조건

- perf regression CI green
- v1 capacity sheet 존재
- architecture refactor 후 benchmark evidence 확보

### 4.9 배포/롤백 -- 6/10

#### 현재 상태

- Nix build/run 지원
- systemd/macOS autostart 지원
- persistence/audit preflight 존재

근거:

- [README Nix section](../README.md#L57-L76)
- [ecosystem deployment baseline](./operations.md)
- [autostart runbook](../AUTOSTART_RUNBOOK_KO.md#L1-L176)

#### 부족한 점

- release artifact packaging 부족
- staged rollout / canary / rollback drill 부재
- 업그레이드 호환성 matrix 부재

#### 10점 조건

- release artifact, rollback path, upgrade path가 모두 문서와 자동화로 닫혀야 한다.
- canary 또는 RC soak 절차가 있어야 한다.

#### 계획

- release artifact policy 수립
- RC soak test 문서화
- rollback checklist 문서화
- persistence format compatibility matrix 문서화
- upgrade/rollback smoke script 추가

#### 종료 조건

- RC -> GA 승격 절차 문서화
- rollback rehearsal 완료
- persistence compatibility matrix 존재

### 4.10 릴리스 엔지니어링 -- 6/10

#### 현재 상태

- version은 아직 `0.1.0`
- release tag 없음
- changelog 존재
- semver/support policy 존재
- release workflow 존재
- recent commit hygiene 약함

근거:

- [workspace version](../Cargo.toml#L11-L16)
- [SemVer spec](https://semver.org/)
- [GitHub releases docs](https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository)

#### 10점 조건

- public API 정의
- semver policy
- tags + release notes + artifacts
- RC/GA discipline
- branch protection과 review ownership

#### 계획

- `CHANGELOG.md` 도입
- release tag policy 도입
- semver/support policy 문서 추가
- `v1.0.0-rc1` -> soak -> `v1.0.0` 절차 수립
- protected branches / required checks / signed commits / CODEOWNERS 설정 문서화

#### 종료 조건

- first tagged release published
- changelog and release notes automated
- branch protection and required checks live

## 5. 권장 마스터 로드맵

> **상세 실행 트래커는 [`docs/RELEASE_ROADMAP.md`](RELEASE_ROADMAP.md)** 를 참조한다.
> 아래는 고수준 요약이며, 태스크 단위 체크리스트·exit gate·검증 명령·의존성은 그 문서가 source of truth다.

권장 일정:

- 1명 기준: 대략 8~10주
- 2~3명 기준: 대략 4~6주
- 전제: 목표는 `single-node Ratatosk GA`, not `Redis drop-in GA`

### Phase 0. 제품 경계 고정 -- 2~3일

- v1 목표를 `single-node Ratatosk GA`로 고정한다.
- README, capability declarations, gap-ledger, `COMMAND DOCS`, website messaging를 같은 문장으로 맞춘다.
- 범위 밖 항목은 `unsupported` 또는 `experimental`로 재분류한다.

산출물:

- `PRODUCT_CONTRACT.md` 또는 README section
- capability tier policy
- v1 ship gate 정의

### Phase 1. 상태 모델 정리 -- 2주

- DB data/stats single source of truth를 확정한다.
- `ServerAccess` 재설계
- meta lock 범위를 축소
- docs-runtime mismatch 제거

산출물:

- state refactor PR
- architecture doc update
- contention benchmark

### Phase 2. stats/observability 정리 -- 1주

- INFO, Prometheus, health, cron가 같은 stats를 보게 만든다.
- `unhealthy` 상태 추가
- lock wait / hold metrics를 ship gate에 포함한다.

산출물:

- stats unification PR
- alert rules
- dashboard example

### Phase 3. 보안/공급망 정리 -- 1주

- production bootstrap auth flow
- `cargo deny` 복구
- Dependabot + CODEOWNERS + branch policy
- SBOM workflow 추가

산출물:

- `.github/dependabot.yml`
- `CODEOWNERS`
- SBOM action
- green security pipeline

### Phase 4. persistence/recovery 마감 -- 2주

- manifest switch/current-state rewrite 설계 마감
- crash/fault injection 추가
- backup/restore/rollback drill 자동화

산출물:

- persistence hardening PR
- recovery matrix
- smoke scripts 확장

### Phase 5. compatibility verification -- 2주

- supported command subset differential tests
- parser/persistence fuzzing
- blocking/tracking invariants tests

산출물:

- Redis differential harness
- fuzz targets
- failure-path tests

### Phase 6. perf/capacity formalization -- 1주

- benchmark gate를 CI required로 만든다.
- latency/throughput/memory envelope 측정
- architecture refactor 효과 측정

산출물:

- perf CI
- capacity report
- regression thresholds

### Phase 7. release engineering 마감 -- 3~4일

- changelog
- tags
- RC soak
- release notes
- artifact checksums

산출물:

- first `v1.0.0-rc1`
- GA checklist
- `v1.0.0`

## 6. 카테고리별 10점 달성 체크리스트

- 아키텍처: state/stats single source of truth, docs/runtime mismatch 0건
- 기능: v1 contract 내부 기능 100% 테스트/문서/운영 절차 확보
- Redis 계약: supported subset differential test green, unsupported subset explicit
- persistence: rewrite/manifest/failure-path matrix green
- 테스트: fuzz/property/concurrency/interop/perf gate 도입
- 보안: non-loopback secure-by-default, audit/deny/dependabot/SBOM green
- 운영: alert/dashboard/runbook/SLO 존재
- 성능: CI perf gate + capacity envelope 존재
- 배포: RC/canary/rollback rehearsal 완료
- release: semver, tags, changelog, release workflow live

## 7. 최종 ship gate

아래 조건을 모두 만족할 때만 v1 shipment를 허용한다.

- `cargo fmt/check/clippy/test/audit/deny` green
- supported command differential suite green
- persistence failure-path matrix green
- perf guardrail green
- Sev1/Sev2 open issue 0개
- docs/runtime mismatch 0개
- RC soak 완료
- backup/restore/rollback drill 완료
- signed/tagged release artifact 발행 가능

## 8. Stretch Goal: Redis Drop-In GA

이 문서의 기본 계획은 single-node GA다. 만약 목표가 Redis drop-in GA라면 아래가 추가된다.

- real network replication stream
- backlog + PSYNC
- WAIT/WAITAOF true blocking semantics
- Sentinel quorum/failover
- Redis Cluster slot ownership / MOVED / ASK / resharding
- richer client-side caching parity
- Functions lifecycle

공식 기준:

- [Redis replication](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/)
- [WAIT](https://redis.io/docs/latest/commands/wait/)
- [WAITAOF](https://redis.io/docs/latest/commands/waitaof/)
- [Sentinel](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/)
- [Cluster](https://redis.io/docs/latest/operate/oss_and_stack/management/scaling/)
- [Client-side caching](https://redis.io/docs/latest/develop/reference/client-side-caching/)

이 경로는 별도 프로그램으로 다루는 것이 맞다.

## 9. 외부 공식 자료

### Redis

- [Redis persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)
- [Redis replication](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/)
- [Redis Sentinel](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/)
- [Redis Cluster scaling](https://redis.io/docs/latest/operate/oss_and_stack/management/scaling/)
- [WAIT](https://redis.io/docs/latest/commands/wait/)
- [WAITAOF](https://redis.io/docs/latest/commands/waitaof/)
- [Redis security](https://redis.io/docs/latest/operate/oss_and_stack/management/security/)
- [Client-side caching](https://redis.io/docs/latest/develop/reference/client-side-caching/)

### Prometheus

- [Instrumentation](https://prometheus.io/docs/practices/instrumentation/)
- [Alerting](https://prometheus.io/docs/practices/alerting/)
- [Alertmanager](https://prometheus.io/docs/alerting/latest/alertmanager/)
- [Consoles and dashboards](https://prometheus.io/docs/practices/consoles/)

### Rust / Supply Chain / GitHub

- [cargo-deny advisories config](https://embarkstudios.github.io/cargo-deny/checks/advisories/cfg.html)
- [RustSec](https://rustsec.org/)
- [Semantic Versioning 2.0.0](https://semver.org/)
- [GitHub releases](https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository)
- [GitHub CODEOWNERS](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-code-owners)
- [GitHub protected branches](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/about-protected-branches)
- [Dependabot version updates](https://docs.github.com/en/code-security/how-tos/secure-your-supply-chain/secure-your-dependencies/configuring-dependabot-version-updates)
- [Export SBOM](https://docs.github.com/en/code-security/how-tos/secure-your-supply-chain/establish-provenance-and-integrity/exporting-a-software-bill-of-materials-for-your-repository)
