# Ratatosk Ship Readiness Master Plan

기준일: 2026-03-26

이 문서는 Ratatosk 코드베이스와 공식 외부 문서를 함께 대조해, 현재 상태를 ship-ready 관점에서 점수화하고, 모든 카테고리를 10/10까지 끌어올리기 위한 상세 계획을 정리한 것이다.

핵심 결론은 간단하다.

- 현재 Ratatosk는 `public GA` 기준으로는 아직 `NO-SHIP`이다.
- 하지만 `single-node durable cache + pub/sub + local persistence server`라는 현재 README 경계 안에서는 매우 강한 프리프로덕션 단계다.
- 가장 현실적인 v1 목표는 “Redis 드롭인 대체재”가 아니라 “single-node Ratatosk GA”다.
- 만약 목표를 “Redis drop-in GA”로 잡으면, replication, Sentinel, Cluster, WAIT/WAITAOF, client-side caching, Functions까지 포함된 멀티쿼터 프로그램으로 커진다.

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

### 4.1 아키텍처 정합성 — 6/10

#### 현재 상태

`SharedState` 바깥에 `data`와 `stats`가 있고, `ServerState` 안에도 `data`와 `stats`가 있다. 다만 `data`는 이제 outer/inner가 같은 `DataState` backing shard를 공유한다. 여기에 default-user ACL policy cache와 `PING` / `ECHO` / `TIME` / `DBSIZE` / string/hash/bitmap/zset/list의 일부 readonly 커맨드용 lock-free fast path가 추가됐다. 반면 일반 명령 실행은 여전히 `server_state.meta.lock().await`로 `ServerState` mutex를 잡은 뒤 `execute()`로 들어가며, `stats`도 아직 완전 단일 소스는 아니다.

근거:

- [SharedState](../crates/ratatosk-engine/src/keyspace.rs#L581-L640)
- [ServerState](../crates/ratatosk-engine/src/keyspace.rs#L813-L928)
- [execute path](../crates/ratatosk-server/src/client.rs#L375-L410)

문서 설명은 최근 수정으로 개선됐지만, 아직도 장기 설계와 현재 runtime path를 분리해 설명할 필요가 있다.

- capability 문서는 “state mutation은 serialized”라고 설명한다.
- 실제 코드는 현재 기준으로 이 설명에 더 가깝다.

근거:

- [architecture doc](./architecture-ratatosk.md#L83-L91)
- [capability doc](./capability-declarations.md#L85-L99)

#### 부족한 점

- DB data divergence 리스크는 줄었지만, authoritative access path가 하나로 정리되지는 않았다.
- stats는 여전히 dual-path다. 다만 cron과 client snapshot refresh가 outer atomic snapshot을 inner stats로 다시 흡수한다.
- authoritative boundary 설명이 아직 충분히 단순하지 않다.
- 장기적으로는 성능/정확성/운영 관측이 모두 이 구조에 발목 잡힌다.

#### 10점 조건

- DB data는 정확히 한 곳만 authoritative 해야 한다.
- stats도 정확히 한 곳만 authoritative 해야 한다.
- docs가 runtime path와 일치해야 한다.
- “different DB parallelism”이 실제 benchmark와 contention metric으로 입증되어야 한다.

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

### 4.2 선언 범위 기능 완성도 — 7/10

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
- [ecosystem status](./ecosystem.md#L8-L37)

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

### 4.3 Redis 계약 정합성 — 5/10

#### 현재 상태

gap-ledger 기준:

- total commands: 420
- `unsupported`: 64
- `syntax_only`: 30
- `baseline_local`: 45
- `behavioral_subset`: 281
- `distributed_parity`: 0

근거: [gap-ledger summary](./redis-gap-ledger.md#L11-L30)

특히 아래는 공식 Redis 계약과 차이가 크다.

- replication backlog / PSYNC 없음
- WAIT / WAITAOF blocking semantics 없음
- Sentinel 없음
- Cluster routing/MOVED/ASK 없음
- client-side caching / Functions는 부분 구현 또는 부재

근거:

- [capability declarations](./capability-declarations.md#L23-L79)
- [Redis replication docs](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/)
- [WAIT docs](https://redis.io/docs/latest/commands/wait/)
- [WAITAOF docs](https://redis.io/docs/latest/commands/waitaof/)
- [Sentinel docs](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/)
- [Cluster docs](https://redis.io/docs/latest/operate/oss_and_stack/management/scaling/)
- [Client-side caching docs](https://redis.io/docs/latest/develop/reference/client-side-caching/)

#### 부족한 점

- “명령 존재”와 “Redis 의미론 보장”이 섞여 보인다.
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

### 4.4 지속성/복구 — 7/10

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

- [persistence status](./persistence.md#L275-L328)
- [startup replay gate](../crates/ratatosk-server/src/persistence/aof.rs#L294-L351)
- [AOF bootstrap](../crates/ratatosk-server/src/persistence/aof.rs#L477-L513)

하지만 Redis 공식 persistence 모델과 비교하면 아직 차이가 있다.

- Redis는 background rewrite에서 minimal command set과 atomic manifest switch를 제공한다.
- Ratatosk는 manifest rewrite switch와 current-state materialization이 아직 부분 구현이다.

근거:

- [Ratatosk persistence status](./persistence.md#L277-L289)
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

### 4.5 테스트/검증 — 7/10

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

### 4.6 보안/공급망 — 7/10

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
- [security defaults doc](./ecosystem.md#L186-L192)
- [security workflow](../.github/workflows/security.yml#L1-L35)

주의할 점:

- loopback/개발 환경에서는 default ACL user가 여전히 `nopass` + full access다.
- built-in TLS가 없다.
- SBOM / provenance는 아직 없다.

근거:

- [default ACL user](../crates/ratatosk-engine/src/acl.rs#L21-L29)
- [TLS note](./ecosystem-ports.md#L21-L31)
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

### 4.7 관측성/운영 — 7/10

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
- [health protocol](./health-protocol.md#L54-L69)
- [panic hook and crash files](../crates/ratatosk-server/src/main.rs#L110-L180)
- [startup preflight](../crates/ratatosk-server/src/main.rs#L305-L420)

하지만:

- Alertmanager config, SLO/SLI 문서는 아직 없다.
- metrics와 `INFO` stats가 같은 truth source를 보지 않는다.

근거:

- [health status implementation](../crates/ratatosk-engine/src/command/cmd_server.rs#L1367-L1390)
- [health protocol note](./health-protocol.md#L62-L69)
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
- “무엇을 alert할지”와 “무엇은 page하지 않을지” 문서화
- blackbox monitoring path 추가

#### 종료 조건

- alert rules + dashboard + runbook PR merge
- one-node 운영에서 필요한 symptom-based alert set 존재
- health/INFO/Prometheus counters parity tests green

### 4.8 성능/용량 — 7/10

#### 현재 상태

- benchmark baseline이 있다.
- perf guardrail checker가 있다.
- canonical log 기준 guardrail은 PASS다.

근거: [performance doc](./performance.md#L1-L56)

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

### 4.9 배포/롤백 — 6/10

#### 현재 상태

- Nix build/run 지원
- systemd/macOS autostart 지원
- persistence/audit preflight 존재

근거:

- [README Nix section](../README.md#L57-L76)
- [ecosystem deployment baseline](./ecosystem.md#L80-L112)
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

### 4.10 릴리스 엔지니어링 — 6/10

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

권장 일정:

- 1명 기준: 대략 8~10주
- 2~3명 기준: 대략 4~6주
- 전제: 목표는 `single-node Ratatosk GA`, not `Redis drop-in GA`

### Phase 0. 제품 경계 고정 — 2~3일

- v1 목표를 `single-node Ratatosk GA`로 고정한다.
- README, capability declarations, gap-ledger, `COMMAND DOCS`, website messaging를 같은 문장으로 맞춘다.
- 범위 밖 항목은 `unsupported` 또는 `experimental`로 재분류한다.

산출물:

- `PRODUCT_CONTRACT.md` 또는 README section
- capability tier policy
- v1 ship gate 정의

### Phase 1. 상태 모델 정리 — 2주

- DB data/stats single source of truth를 확정한다.
- `ServerAccess` 재설계
- meta lock 범위를 축소
- docs-runtime mismatch 제거

산출물:

- state refactor PR
- architecture doc update
- contention benchmark

### Phase 2. stats/observability 정리 — 1주

- INFO, Prometheus, health, cron가 같은 stats를 보게 만든다.
- `unhealthy` 상태 추가
- lock wait / hold metrics를 ship gate에 포함한다.

산출물:

- stats unification PR
- alert rules
- dashboard example

### Phase 3. 보안/공급망 정리 — 1주

- production bootstrap auth flow
- `cargo deny` 복구
- Dependabot + CODEOWNERS + branch policy
- SBOM workflow 추가

산출물:

- `.github/dependabot.yml`
- `CODEOWNERS`
- SBOM action
- green security pipeline

### Phase 4. persistence/recovery 마감 — 2주

- manifest switch/current-state rewrite 설계 마감
- crash/fault injection 추가
- backup/restore/rollback drill 자동화

산출물:

- persistence hardening PR
- recovery matrix
- smoke scripts 확장

### Phase 5. compatibility verification — 2주

- supported command subset differential tests
- parser/persistence fuzzing
- blocking/tracking invariants tests

산출물:

- Redis differential harness
- fuzz targets
- failure-path tests

### Phase 6. perf/capacity formalization — 1주

- benchmark gate를 CI required로 만든다.
- latency/throughput/memory envelope 측정
- architecture refactor 효과 측정

산출물:

- perf CI
- capacity report
- regression thresholds

### Phase 7. release engineering 마감 — 3~4일

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
