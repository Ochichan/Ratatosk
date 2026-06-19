# Ratatosk 릴리즈 로드맵 — v1.0.0 GA

> **Live execution tracker.** `docs/operations.md §5`의 고수준 "권장 마스터 로드맵"을
> 태스크 단위로 확장한 단일 추적 문서다. Phase 구조(0–7)는 operations.md와 호환되며,
> 여기서는 각 Phase를 체크 가능한 작업·exit gate·검증 명령·파일 타깃·의존성으로 분해한다.
>
> 분석 근거: `docs/architecture.md`(상태/persistence/capability), `docs/operations.md`(contract·ship gate·평가),
> `docs/optimization.md`(perf/memory), `docs/redis-gap-ledger.json`(명령 tier 원천).

---

## 0. 이 트래커 사용법

**상태 범례**

- `[ ]` todo · `[~]` in-progress · `[x]` done · `[-]` dropped / out-of-scope · `[?]` 확인 필요(코드 대조)

**갱신 규칙**

1. 태스크 완료 시 체크하고 끝에 `(PR #123 / commit abc1234)`를 단다.
2. Phase의 **모든 exit-gate 조건**이 green이면 Phase 헤더에 ✅를 표시한다.
3. 명령 의미가 바뀌면 **반드시** `scripts/redis_gap_ledger.py`로 ledger를 갱신한다(ledger가 source of truth).
4. 이 트래커 · 코드 · gap-ledger 3자 불일치 = 0 이 ship gate 조건이다.

**진실의 원천**

- 코드 + `docs/redis-gap-ledger.json` + 이 트래커. 셋이 어긋나면 ship 불가.
- 마지막 코드/문서 동기화 스냅샷: **2026-06-02**.

---

## 1. 현재 상태 스냅샷 (2026-06-02 검증)

**Release readiness:** 평균 **6.6/10**. public GA 기준 **NO-SHIP**.
단, 현재 README 경계(`single-node durable cache + Pub/Sub + local persistence`) 안에서는 강한 pre-production 단계.

**명령 표면 (gap ledger 기준, 검증됨):** 420 entries 전부 `status=done` — 그러나 **parity tier는 계층별로 다름**.

| tier | 개수 | 의미 |
|---|---:|---|
| `behavioral_subset` | 275 | Redis 의미론에 근접, differential 대상 |
| `baseline_local` | 76 | 로컬 단일노드 한정 동작 |
| `syntax_only` | 6 | 문법/arity만 검증, 의미 미보장 ⚠️ |
| `unsupported` | 63 | 미지원 (distributed/functions/단일노드에서 항상 에러) |
| `distributed_parity` | 0 | 분산 parity 없음 |

> _2026-06-02 갱신: 코드↔ledger tier를 명령별로 audit·reconcile하여 분포가 30/45/64 →_
> _6/76/63으로 정정됨(working 명령이 `syntax_only`로 과분류돼 있었음). 이제
> `capability_tier_matches_gap_ledger_for_every_spec` 테스트가 코드=ledger를 잠금._

> ⚠️ 핵심 리스크: `done=420`을 "Redis와 동일"로 마케팅하면 안 된다. `syntax_only` + no-op admin이
> 클라이언트/툴에 거짓 성공을 준다 → **strict mode(Phase 0)** 로 막는 것이 v1 안전성의 핵심.

**구현 마일스톤:** m0 107 · m1 64 · m2 102 · m3 43 · m3-persist 2 · m4 22 · m5 80.

**구현 완료 (검증된 강점) ✅**

- 메모리: `StoredValue` 24B 레이아웃, incremental memory tracking(eviction CPU −99%), expires side-index(active expiry CPU −90%), `StringInt`, `SetInt`, mimalloc/jemalloc 선택 가능(opt-in feature; 기본 빌드는 시스템 할당자).
- 동시성: per-DB `parking_lot::RwLock`, atomic stats(요청당 lock 취득 ~9개 감소), arc_swap config 캐시.
- persistence: RDB(per-DB read-lock clone), AOF manifest 구조 + recovery 순서, fsync 정책(Always/EverySec/No), `BGREWRITEAOF`(stream rewrite), shutdown AOF flush, RDB clone 제거(optimization Phase 0).
- 보안/공급망: non-loopback bind 시 `RATATOSK_ALLOW_INSECURE_BIND` + password 필수, AUTH brute-force 완화(progressive delay + per-IP `AuthRateLimiter`), audit log + chain checkpoint, `cargo audit` green, `cargo deny check` green, Dependabot, CODEOWNERS, gitleaks workflow.
- 관측성: Prometheus exporter(`127.0.0.1:9090`), `health_status` / `PING HEALTH`, alert 시작팩(`monitoring/prometheus/ratatosk-alerts.yml`), Grafana 대시보드(`monitoring/grafana/...json`), `bridge_contract_version`.
- 호환성 메타: **`COMMAND DOCS`가 `ratatosk_capability_tier`를 이미 노출** (← 정직한 호환성 메시징의 토대, 이미 존재).
- 운영: `--check-config` startup preflight, systemd/launchd autostart, Nix flake, release workflow skeleton, perf-guardrail workflow, gap-ledger workflow, redis-interop workflow.

**미완 / 갭 (검증됨) ⛔ — 이 로드맵의 작업 대상**

| 영역 | 갭 |
|---|---|
| 아키텍처 | meta lock serialization 잔존, stats/INFO/Prometheus/health **truth-source 불일치** |
| persistence | AOF **BASE materialization + atomic manifest switch 미완**, LZF 미구현, crash/fault matrix 부재 |
| 호환성 검증 | supported-subset **differential suite 부재**, parser/persistence **fuzz 부재**, blocking/tracking invariants 테스트 부재 |
| 호환성 안전장치 | **strict compatibility mode 미구현**, no-op/`syntax_only`가 거짓 성공 |
| 관측성 | **SLO/SLI 문서 부재**, **Alertmanager config 부재**, lock wait/hold·memory drift·AOF queue lag 메트릭 미정형 |
| 보안 | default user `nopass`(production profile 금지 미강제), **TLS 레시피 부재**, **SBOM 부재**, **signed tags/checksums 미발행** |
| 성능 | perf gate가 CI required 아님, **capacity envelope 부재** |
| 릴리즈 | tagged release 체계·provenance·**RC/rollback/restore drill** 부재 |
| 신규 차별화 | `ratatosk compat`(분석기) 미구현, `ratatosk doctor` 미구현, capability helper crate 미구현, reliability report 자동화 미구현 |

### 2026-06-02 구현 패스 결과 (이 스냅샷 이후 변경)

**완료 + 검증:**

- **Phase 0 전체 ✅** — strict compatibility mode(코드+config 배선), `PRODUCT_CONTRACT.md`, README/CLI/conf 메시징 동기화. **명령별 tier audit로 코드↔ledger 39건 drift 해소 + parity 테스트로 잠금**(ship-gate #4 closed). 테스트 7종: strict 차단/허용/토글, 전수 enumeration, tier 완전성, **code=ledger parity** — 모두 green.
- **관측성** — `docs/SLO.md`, `monitoring/alertmanager/`; 차별화 메트릭(lock wait/hold·memory drift·AOF queue lag·blocked waiter)이 이미 노출됨을 확인.
- **persistence** — `scripts/recovery_matrix.sh`(7/7 로컬 pass), `scripts/backup_restore_drill.sh`(5/5 stage pass), durability contract 표(operations.md §4).
- **보안/공급망** — SBOM workflow(`sbom.yml`), TLS 레시피(operations.md §3), checksums(`.sha256`) + **keyless cosign 서명 준비완료**(`release.yml`, OIDC opt-in).
- **호환성 검증** — strict 전수 enumeration 테스트 + 코드↔ledger parity 테스트(ship-gate #4 잠금).
- **perf/릴리즈** — `scripts/capacity_envelope.sh`, `scripts/reliability_report.sh`, CHANGELOG `[Unreleased]` 정리.
- **Phase 2 코드 ✅** — `PING HEALTH` `reasons:` 사유 노출(사람이 읽는 degraded/unhealthy 원인), `INFO server` feature flags(`ratatosk_compatibility_mode`/`ratatosk_protected_mode`), audit chain status가 `PING HEALTH`+`INFO persistence`에 노출됨 확인.
- **Phase 3 코드 ✅** — 일급 `protected-mode` 디렉티브(default `yes`) 전 계층 배선(engine `ConfigState`/server `ServerConfig`/`CONFIG GET·SET·REWRITE`) + non-loopback nopass startup 강제. 테스트 3종(protected_mode round-trip·bootstrap·health reasons) green.
- **품질 게이트** — fmt/clippy(-D warnings)/test/ledger green 재확인(engine 217·server 85 테스트, 0 fail).

**여전히 미완 (외부 의존/위험으로 이 패스 밖):**

- Phase 1 상태모델 refactor(2주 고위험), Phase 4 AOF BASE materialization + atomic manifest switch(고위험), Phase 5 Redis/Valkey differential(외부 redis/valkey 바이너리)·RESP fuzz(cargo-fuzz/nightly), perf gate CI-required·branch protection·tag 서명·SLSA provenance(저장소 admin), RC soak(24–72h). _준비물(스크립트/워크플로/문서)은 완비, 실행만 operator/외부 환경 대기._

---

## 2. 제품 계약 — 모든 surface에서 같은 문장 (북극성)

README · website · `COMMAND DOCS` · gap-ledger · CLI help · Docker description · release notes에서
**동일 문장**을 반복한다.

> **EN:** Ratatosk is a single-node, Redis-compatible, RESP2/RESP3 in-memory server for cache, Pub/Sub,
> and local durability. It is **not** a Redis Cluster, Sentinel, or replication-compatible drop-in replacement.
> Every command exposes a capability tier; the supported subset is tested against Redis/Valkey.

> **KO:** Ratatosk는 캐시 · Pub/Sub · 로컬 지속성을 위한 단일 노드 Redis 호환 RESP2/RESP3 인메모리 서버다.
> Redis Cluster · Sentinel · replication 호환 drop-in 대체재가 **아니다**. 모든 명령은 capability tier를
> 노출하며, 지원 subset은 Redis/Valkey와 대조 검증된다.

**계약 강제 표 (Phase 0에서 실행)**

| 항목 | 지금 위험 | 조치 |
|---|---|---|
| `done=420` 표현 | "Redis와 동일"로 오해 | "command surface 420, supported semantics vary by tier"로 표기 |
| `syntax_only` 6개 | client/tool이 성공으로 오판 | strict mode에서 ERR, 기본 모드에서도 docs에 명시 |
| Cluster/Sentinel helper | cluster 지원처럼 보임 | cluster-aware client는 unsupported 명시 |
| `WAIT`/`WAITAOF` | metadata가 durability처럼 보임 | replica-backed 보장 아님 명시, strict에서 ERR |
| Lua/Functions | feature-gated와 ledger 상태 혼재 | 빌드 feature별 capability ledger 생성 |
| no-op admin | 운영 자동화가 성공 오판 | "acknowledged but no operational effect" 금지 또는 docs 굵게 |

---

## 3. 릴리즈 트레인 & 크리티컬 패스

**트레인**

| 마일스톤 | 게이트 | 내용 |
|---|---|---|
| `v1.0.0-rc1` | Phase 0–7 (RC track) 전부 exit-gate green | 첫 RC, soak 시작 |
| `v1.0.0-rc.N` | Sev1/Sev2 = 0, soak 회차 | 안정화 반복 |
| **`v1.0.0` GA** | **§7 최종 ship gate 전부 green** | 정식 출시 |
| `v1.1.0` | P1 차별화 (Phase 8) | compat 분석기·Listpack·doctor·helper |
| 별도 프로그램 | — | replication/cluster/sentinel/functions (Phase 9, out-of-scope) |

**크리티컬 패스 (의존성)**

```
P0 제품경계 ──▶ P1 상태모델 ──▶ ┬─▶ P2 관측성 ─┐
 (2~3d)        (2wk, 최장/최위험) │            │
   │                              ├─▶ P6 perf  ─┼─▶ P7 릴리즈 ──▶ rc1 ──▶ soak ──▶ GA
   │                              │            │   (3~4d)
   ├─▶ P3 보안 (1wk, 병렬) ───────┘            │
   └─▶ P5 호환성검증 (2wk, P0 후 시작) ─────────┘
       P4 persistence (2wk, 병렬, 고위험) ──────┘
```

- **크리티컬 패스 = P0 → P1 → (P2/P5/P6) → P7 → soak.** P3·P4는 병렬화 가능.
- P1(상태모델)이 최장·최고위험 → 가장 먼저 착수. P4(persistence)는 독립 진행 가능하지만 fault matrix가 길다.

**일정 (예시, solo, T0 = 2026-06-02)**

| 주차 | 기간(예시) | Phase |
|---|---|---|
| W1 | 06-02 ~ 06-06 | P0 완료 + P1 착수 + P3 병렬 착수 |
| W2–W3 | 06-09 ~ 06-20 | P1 (상태모델) · P4(persistence) 병렬 |
| W4 | 06-23 ~ 06-27 | P2 (관측성, P1 후) · P5 착수 |
| W5 | 06-30 ~ 07-04 | P5 (호환성 검증) · P6(perf) |
| W6 | 07-07 ~ 07-11 | P7 (릴리즈 엔지니어링) → **rc1** |
| W7–W8 | 07-14 ~ 07-25 | soak · drill · Sev 수정 → **GA** |

- **solo: 8~10주 → GA 목표 ~2026-07말/08초.** 2~3명: 4~6주 → ~2026-07중.
- 위 날짜는 예시다. 실제 T0(킥오프)에 맞춰 재계산할 것.

---

## Phase 0 — 제품 경계 고정 + strict mode 골격  ✅  ⏱ 2~3일  ·  의존성: 없음  ·  위험: 낮음

**Exit gate:** README/CLI/Docker/COMMAND DOCS/ledger가 동일 contract 문장을 노출 · `compatibility-mode` 설정이 존재하고 strict에서 대표 명령군(WAIT/WAITAOF/CLUSTER */SENTINEL */FUNCTION *) ERR · `PRODUCT_CONTRACT.md` 발행. → **달성 (2026-06-02 구현 패스)**.

- [x] **제품 계약 문서화** — `docs/PRODUCT_CONTRACT.md` 발행(§1 contract 문장 + tier 정책 + strict/compat + 강제 매트릭스). README 상단도 동일 문장 채택.
- [x] **메시징 동기화** — README 첫 화면, `CLI --help`(`main.rs` `long_about`), `ratatosk.conf` 주석에 동일 contract 문장 반영. _(Dockerfile/release-notes 템플릿은 해당 파일 부재로 N/A; 생성 시 같은 문장 적용.)_
- [x] **COMMAND DOCS tier 노출 검증** — `ratatosk_capability_tier` 노출(`command/mod.rs`). 전 명령 누락 없음 스냅샷 테스트 추가(`command_docs_exposes_capability_tier_for_every_command`). _검증:_ `cargo test -p ratatosk-engine`
- [x] **범위 밖 재분류 + tier audit** — 코드↔ledger를 명령별 대조해 39건 drift 발견·해소(핸들러 실측 audit). 코드 4건 수정(DEBUG/FAILOVER/SHUTDOWN→unsupported, SFLUSH→syntax_only), ledger 35건 정정. 분포 6/76/63으로 갱신, markdown 재생성. _검증:_ `redis_gap_ledger.py check` green + `capability_tier_matches_gap_ledger_for_every_spec` 테스트 green.
- [x] **strict compatibility mode 골격** — config `compatibility-mode strict|compat`(기본 `compat`). strict에서 `unsupported`/`syntax_only` tier + `WAIT`/`WAITAOF`가 구조화 ERR 반환. 핵심 명령(GET/SET/PING/INFO/CONFIG/CLIENT LIST/COMMAND DOCS/CLUSTER INFO)은 비차단.
  - 구현: `cmd_command_metadata.rs`(`strict_mode_error`/`resolve_leading_spec`), `mod.rs`(dispatch gate), `cmd_server_config.rs`(CONFIG GET/SET/REWRITE), `config.rs`(engine+server), `event_loop.rs`(seed), `ratatosk.conf`(샘플).
  - ERR 포맷: `ERR command WAIT is not supported in Ratatosk strict compatibility mode; reason=requires replica-backed acknowledgement that a single-node server cannot provide`
  - _검증:_ `cargo test -p ratatosk-engine -- strict_mode` (5 테스트 신규, green)
- [x] **v1 ship gate 정의 고정** — 본 문서 §4를 단일 ship gate로 채택, operations.md §5/§7과 정합.

**산출물:** `PRODUCT_CONTRACT.md` ✅ · capability tier policy ✅ · strict-mode config + 5 테스트 ✅ · ledger 일치 복구 ✅.

---

## Phase 1 — 상태 모델 / lock 구조 정리  ⏱ 2주  ·  의존성: P0  ·  위험: **높음 (최장 경로)**

**목표:** "완전한 멀티코어 DB"가 아니라 **관측 가능하고 설명 가능한 동시성 모델**. Dragonfly식 shared-nothing 추격이 아니라 정직한 single-node envelope.

**Exit gate:** DB data/stats single source of truth 확정 · docs-runtime mismatch 0 · meta lock wait/hold 메트릭 노출 · same-DB vs cross-DB latency 비교 리포트 존재.

- [ ] **DB data authoritative path 단일화** — outer/inner data handle 혼선 제거(현재 동일 shard 공유까지는 진행됨). `ServerAccess` 재설계로 접근 경로 1개로.
  - 파일: `crates/ratatosk-engine/src/state/` (DataState/DbShard/ServerState).
  - _수용 기준:_ architecture.md "Core State Model" 서술과 코드가 1:1 일치.
- [ ] **stats source 단일화** — `INFO`, Prometheus, `PING HEALTH`, `server_cron`이 같은 stats 구조체를 읽도록.
  - _수용 기준:_ 동일 워크로드 후 `INFO` 필드 = Prometheus 게이지 = health payload (parity 테스트).
  - _검증:_ `cargo test -p ratatosk-engine stats_parity` (신규)
- [ ] **meta lock 범위 축소** — 일반 명령 경로의 mutex hold 구간 최소화, readonly fast-path 경계 명문화.
- [x] **lock wait/hold 메트릭 추가** — `ratatosk_server_state_lock_wait_ms` / `ratatosk_server_state_lock_hold_ms` 노출 확인(`metrics.rs`). SLO 문서(`docs/SLO.md`)에 lock-health SLI로 연결. _(state-model refactor 전이라 의미는 현 lock 구조 기준.)_
- [ ] **readonly fast-path coverage 문서화** — 어떤 명령이 fast-path인지 `COMMAND DOCS` 또는 docs에 표기.
- [ ] **cross-DB parallel benchmark** — per-DB RwLock의 실제 가치 입증: same-DB vs cross-DB latency 비교 리포트.
  - _검증:_ `./scripts/bench_baseline.sh` 확장 + 리포트 산출물.

**산출물:** state refactor PR · architecture.md 갱신 · contention/cross-DB benchmark 리포트 · lock 메트릭.

---

## Phase 2 — stats / observability 정리  ⏱ 1주  ·  의존성: P1(stats 단일화)  ·  위험: 중

**목표:** "명령이 많다"가 아니라 **"문제 시 즉시 원인을 안다"**를 제품 차별점으로.

**Exit gate:** `INFO`·Prometheus·`PING HEALTH` 동일 상태 보고 · `unhealthy`/`degraded` 사유 노출 · Alertmanager config 존재 · SLO/SLI 문서 존재 · runbook이 alert와 1:1 연결.

- [x] **`unhealthy`/`degraded` 상태 + 사유** — `PING HEALTH`가 사람이 읽을 수 있는 degraded 원인을 `reasons:` 필드로 반환. unhealthy 사유: memory critical / persistence dir·AOF not writable / AOF write latched. degraded 사유: last RDB save·AOF rewrite failed / low disk / audit chain dirty. healthy 시 `reasons:none`. 상태 임계값은 불변(순수 가산). _검증:_ `ping_health_reports_unhealthy_when_aof_is_latched`, `ping_health_reports_no_reasons_when_healthy`.
- [~] **필수 INFO 필드 보강** — 표 기준 보강 진행:
  - `INFO server`: `health_status` ✅, version ✅, build ✅, contract version ✅, **feature flags ✅**(`ratatosk_compatibility_mode`/`ratatosk_protected_mode` 추가).
  - `INFO persistence`: AOF latch ✅, last RDB save ✅, last AOF rewrite ✅, audit dirty ✅, recovery status ✅ — **완료**.
  - `INFO clients`: connected ✅, blocked ✅, tracking ✅. _잔여:_ pubsub/output-buffer는 새 accessor 필요(후속).
  - `INFO memory`: **전용 섹션 신규** ✅ — `used_memory`(logical estimate)·`used_memory_human`·`maxmemory`·`maxmemory_human`·`maxmemory_policy`·`mem_used_memory_source:logical_estimate`·**`mem_estimate_age_ticks`(estimate drift)** ✅. estimate drift는 cron이 Prometheus로 export하던 `memory_estimate_age_ticks`를 stats에 stamp→`HotStatsSnapshot`→INFO로 배선(INFO==Prometheus 동일값). _잔여:_ allocator mem·fragmentation만 RSS 리더(Linux `/proc/self/statm`·macOS `task_info`) 선행 필요 → 후속(부정확 보고 대신 의도적 생략).
- [x] **Prometheus 차별화 메트릭** — 노출 확인: command latency histogram(`ratatosk_command_duration_seconds`), lock wait/hold(`ratatosk_server_state_lock_{wait,hold}_ms`), memory drift(`ratatosk_memory_estimate_age_ticks`), AOF queue lag(`ratatosk_aof_queue_depth`), blocked waiter(`ratatosk_blocking_retry_*`), eviction(`ratatosk_eviction_keys_total`), AOF errors(`ratatosk_aof_write_errors_total`).
- [x] **Alertmanager config 작성** — `monitoring/alertmanager/alertmanager.yml` + `README.md` 신규. severity=page/ticket 라우팅 트리 + inhibit 룰, 기존 alert 룰과 연결.
- [x] **SLO/SLI 문서** — `docs/SLO.md` 신규: availability·p99 latency·durability 목표, 실제 메트릭 기반 SLI, error budget, SLO→alert→runbook 매핑.
- [~] **runbook ↔ alert 매핑** — `docs/SLO.md §3`에 SLO/alert/runbook 매핑 표 작성. alert별 symptom/impact/mitigation/verification/rollback 5요소 상세 runbook은 후속.
  - _검증:_ `promtool check rules monitoring/prometheus/ratatosk-alerts.yml` (promtool 설치 환경에서)

**산출물:** stats unification PR · Alertmanager config · SLO 문서 · runbook 세트.

---

## Phase 3 — 보안 / 공급망 (production profile 기본값)  ⏱ 1주  ·  의존성: P0  ·  위험: 중  ·  **병렬 가능**

**목표:** "보안은 나중에"가 아니라 **"작은 배포에서 실수하지 않게 하는 기본값"**.

**Exit gate:** production profile에서 `nopass` 불가 · non-loopback bind는 인증 없이 startup fail · SBOM 생성 · signed tags + checksum 발행 가능 · security 파이프라인 green.

- [x] `cargo audit` green · `cargo deny check` green · Dependabot · CODEOWNERS · gitleaks workflow (검증됨, 유지)
- [x] **production auth bootstrap** — 일급 Redis 호환 `protected-mode` 디렉티브 추가(default `yes`). engine `ConfigState` + server `ServerConfig`(file/env `RATATOSK_PROTECTED_MODE`/CLI) + `CONFIG GET/SET/REWRITE` 전 계층 배선. `protected-mode yes`에서 non-loopback bind + nopass default user는 startup fail(`bootstrap_default_user_for_bind`); `RATATOSK_DEFAULT_USER_PASSWORD`/`_HASH`로 부트스트랩하거나 `protected-mode no`(= `RATATOSK_ALLOW_DEFAULT_USER_NOPASS=true`)로 명시적 opt-out. env/file/secret bootstrap 경로는 README §Security·`ratatosk.conf`·`docs/operations.md`에 문서화.
  - 파일: `crates/ratatosk-engine/src/config.rs`·`command/cmd_server_config.rs`, `crates/ratatosk-server/src/{config.rs,event_loop.rs}`, `ratatosk.conf`.
  - _수용 기준 충족:_ protected-mode yes + non-loopback + 무인증 → startup fail. _검증:_ `non_loopback_bind_requires_bootstrap_password_by_default`, `protected_mode_no_allows_nopass_default_user_on_non_loopback_bind`, `config_get_set_protected_mode_round_trips`.
- [x] **SBOM workflow** — `.github/workflows/sbom.yml` 신규: CycloneDX(`cargo cyclonedx`) 생성 → `sbom/` 수집 → artifact 업로드 → `v*` 태그 시 release 첨부.
- [~] **signed tags + checksums + provenance** — checksums ✅(`release.yml`이 아카이브별 `.sha256` 생성·발행), **keyless cosign 서명 준비완료**(GitHub OIDC, 장기 키 불필요 — repo var `RATATOSK_ENABLE_SIGNING=true`로 opt-in 시 `.sig`/`.crt` 발행), SBOM ✅(`sbom.yml`). _잔여:_ tag 서명·build provenance(SLSA)는 operator/저장소 설정.
- [x] **TLS 레시피** — `docs/operations.md §3`에 stunnel/nginx(stream)/Envoy termination 레시피 + 예시 config + hardening 체크리스트.
- [x] **audit log → health 연결** — tamper-evident chain status 노출 완료: `INFO persistence`의 `audit_chain_dirty`/`audit_recovery_status`, `INFO server`의 `health_status`(audit dirty 반영), `PING HEALTH`의 `audit_chain_dirty`/`audit_recovery_status` 필드 + dirty 시 사람이 읽는 `reasons:` 사유.
- [ ] **branch protection / required checks** — protected `main`, required: rust-ci·security·gap-ledger·perf-guardrail.

**산출물:** secure-by-default PR · SBOM action · 서명 릴리즈 파이프라인 · TLS 레시피 · branch policy.

---

## Phase 4 — persistence / recovery 마감  ⏱ 2주  ·  의존성: 없음(독립)  ·  위험: **높음**  ·  **병렬 가능**

**목표:** local durability를 제품 신뢰의 핵심으로. 장애 시 "무엇이 살아남는가"를 표로 못 박는다.

**Exit gate:** AOF current-state materialization 기반 compact rewrite + atomic manifest switch 완료(또는 명확히 제외 선언) · crash/fault matrix 자동화 green · backup/restore/rollback drill 스크립트화 · fsync 정책별 durability contract 문서화.

- [ ] **AOF current-state materialization rewrite** — BGREWRITEAOF를 stream replay가 아닌 현재 상태 직렬화 기반 compact rewrite로.
  - 파일: `crates/ratatosk-persist/src/aof/`.
- [ ] **atomic manifest switch** — BASE materialization + full atomic manifest switch 완료. (현재 candidate validation까지 진행, BASE/atomic switch 미완)
  - _대안 결정:_ v1에 못 넣으면 **명확히 제외 선언** + strict-mode/docs 반영.
- [x] **crash / fault injection matrix** — `scripts/recovery_matrix.sh` 신규. **로컬 실행 검증 완료(7/7 pass)**: clean RDB restart, AOF clean restart, kill-9(always-fsync 무손실), kill-9 중 BGREWRITEAOF(무손상), truncated AOF(consistent-prefix 복구), missing manifest(segment 재구성, silent loss 없음), repeated rewrite(안정). disk full은 constrained-FS 필요로 명시 skip(`ratatosk_aof_write_errors_total` alert로 커버).
- [x] **durability contract 표** — `docs/operations.md §4`에 fsync 정책(Always/EverySec/No)별 acknowledged-write 손실 윈도우 + recovery invariants 표. `recovery_matrix.sh`가 표를 강제.
- [x] **backup / restore / rollback drill 스크립트** — `scripts/backup_restore_drill.sh` 신규. **로컬 실행 검증 완료(5/5 stage pass)**: seed→backup→loss→restore→mutate→rollback, 결정적 dataset 검증.
- [ ] **RDB memory pressure 마감** — optimization Phase 5 Step 2(`Arc<StoredValue>`) 또는 Step 3(streaming) 중 1개 마감(BGSAVE peak −50~95%). v1 필수 여부는 capacity 측정 후 결정 가능.
- [-] LZF 압축 — v1 제외(out-of-scope), v1.1 후보.

**산출물:** persistence hardening PR · recovery matrix 자동화 · durability contract 표 · drill 스크립트.

---

## Phase 5 — compatibility verification (마케팅 기능화)  ⏱ 2주  ·  의존성: P0(+P1 권장)  ·  위험: 중

**목표:** "지원한다고 말한 것은 테스트로 증명한다." 단순 품질 게이트가 아니라 **제품 포지션**.

**Exit gate:** supported subset이 Redis/Valkey와 byte-level differential green · RESP parser/encoder fuzz 타깃 존재 · persistence property 테스트(roundtrip/truncation/corruption) green · blocking/tracking invariants 테스트 green · strict-mode가 unsupported/`syntax_only`를 조용히 성공시키지 않음(테스트로 보장).

- [ ] **Redis/Valkey differential harness** — v1 supported(`behavioral_subset`) 명령을 실제 Redis/Valkey와 byte-level 비교.
  - 파일: 기존 `redis-interop.yml` 확장, `crates/ratatosk-server/tests/` 또는 `tests/interop/`.
  - _검증:_ `.github/workflows/redis-interop.yml` (matrix: redis 7.x / valkey 8.x).
- [~] **RESP fuzz** — cargo-fuzz 타깃 **스캐폴드 완비** ✅: `crates/ratatosk-resp/fuzz/`(별도 workspace로 격리 → stable 게이트 무영향) + `fuzz_targets/resp_parse.rs`. 실제 불변식 로직은 `ratatosk_resp::fuzz_support::drain_and_roundtrip`(no-panic · 진행성 · encode∘parse round-trip)에 두어 **stable 빌드에서 unit-test로 검증**(`cargo test -p ratatosk-resp fuzz_support`, 2종 green). _잔여(operator/외부):_ 실제 캠페인 실행은 nightly + `cargo install cargo-fuzz` 필요 — 실행법 `crates/ratatosk-resp/fuzz/README.md`. (사용자 승인 "준비물만 만들고 문서화"에 따름.)
  - 파일: `crates/ratatosk-resp/fuzz/`.
- [~] **persistence property 테스트** — RDB/AOF roundtrip·truncation·corruption invariant은 `scripts/recovery_matrix.sh`(통합 레벨)로 일부 커버. crate-level proptest는 후속.
- [ ] **blocking invariants** — BLPOP/BZPOP/XREAD wakeup·fairness 테스트(waiter registry 강점 살리되 fairness gap 명시).
- [ ] **client tracking matrix** — REDIRECT/BCAST/PREFIX/NOLOOP/OPTIN/OPTOUT 조합 테스트. 미지원 조합은 즉시 ERR.
- [x] **strict-mode 보장 테스트** — 전 명령 **전수** enumeration 테스트(`strict_mode_blocks_every_unsupported_and_syntax_only_command`): 401 spec 전체에 대해 strict가 정확히 `unsupported`/`syntax_only` tier + WAIT/WAITAOF만 차단하고 나머지는 허용함을 검증. 대표 차단/허용 + 런타임 토글 테스트 포함, 모두 green.
- [ ] **coverage report 공개** — release page에 differential coverage report 산출(아래 reliability report와 통합).

**산출물:** Redis differential harness · fuzz 타깃 · failure-path/invariant 테스트 · coverage report.

---

## Phase 6 — perf / capacity 정형화  ⏱ 1주  ·  의존성: P1  ·  위험: 낮음

**목표:** "세계 최고"가 아니라 **"이 workload에서 이 envelope까지 안전"**.

**Exit gate:** perf-guardrail이 CI **required** · latency/throughput/memory **capacity envelope 리포트** 존재 · P1 refactor 효과 측정.

- [x] perf guardrail baseline 존재 — set@256 ≤ 110µs, ping@256 ≤ 35µs (canonical PASS 2026-02-08). 유지.
- [ ] **perf gate를 CI required로** — `perf-guardrail.yml`을 branch protection required check에 포함, CI fixture 안정화.
- [~] **capacity envelope 측정** — `scripts/capacity_envelope.sh` 신규: SET/GET/PING × pipeline depth 1·16 latency/throughput + 키 개수별 RAM(used_memory & `ratatosk_memory_used_bytes`) → markdown 리포트. bash 구문/shellcheck green; 전체 실행은 `redis-benchmark` 설치 환경 필요.
  - _검증:_ `./scripts/capacity_envelope.sh` (redis-benchmark 있는 환경) · `./scripts/bench_baseline.sh` + `python3 scripts/perf_guardrail_check.py`.
- [ ] **P1 refactor 효과 리포트** — state model 정리 전/후 same-DB vs cross-DB 비교.
- [ ] **hot path quick win** — GET/SET/INCR allocation 정리(잔여), memory estimate drift 메트릭(P2와 연계).
- [ ] **allocator stats feature 명확화** — jemalloc/mimalloc feature별 진단 노출.
- [-] Listpack / SkipList — v1 제외(Phase 8). v1은 현재 인코딩 envelope를 정직하게 공개.

**산출물:** perf CI(required) · capacity envelope 리포트 · regression thresholds 공개.

---

## Phase 7 — release engineering 마감  ⏱ 3~4일  ·  의존성: P0–P6  ·  위험: 낮음

**Exit gate:** §7 최종 ship gate 전부 green이 가능한 상태 · `v1.0.0-rc1` 발행 · reliability/benchmark/recovery 리포트 자동 생성.

- [~] **CHANGELOG 마감** — `[Unreleased]`에 strict mode·PRODUCT_CONTRACT·SLO·recovery/drill/capacity/reliability 스크립트·SBOM·TLS·durability 표 + migration/rollback note 정리. `v1.0.0` 최종 cut은 릴리즈 시점.
- [ ] **release 산출물** — `release.yml` 확장:
  - `ratatosk-linux-x86_64.tar.gz` + checksum
  - `ratatosk-macos-arm64.tar.gz` + checksum
  - Docker image + SBOM
  - example `ratatosk.conf`
  - docs bundle (offline)
- [ ] **자동 reliability report** — 릴리즈마다 생성:
  ```
  Ratatosk v1.0.0-rc1 Reliability Report
  - Redis differential supported subset: PASS
  - RESP fuzz: N cases
  - AOF truncation recovery / missing manifest startup fail / disk full / crash during BGREWRITEAOF: PASS
  - backup/restore drill / rollback drill: PASS
  - Perf guardrail (set@256, ping@256): PASS
  ```
  - 소스: Phase 4 matrix + Phase 5 differential + Phase 6 perf.
  - **구현:** `scripts/reliability_report.sh` 신규 — fmt/clippy/test/audit/deny/ledger/strict-mode/recovery-matrix/drill/perf 게이트를 PASS/FAIL/SKIP로 집계해 `benchmarks/reliability-report-*.md` 생성. SKIP은 외부 도구 부재 시 명시(silent 아님).
- [~] **benchmark report** + **recovery report** 산출물화 — recovery report는 `recovery_matrix.sh`, 통합 report는 `reliability_report.sh`가 생성. differential/fuzz는 외부 도구(redis/valkey, cargo-fuzz) 환경에서 채워짐.
- [ ] **RC 발행 + soak** — `v1.0.0-rc1` 태그(서명), soak(24–72h) 시작.
- [ ] **GA 체크리스트** — §7 전체 통과 확인 → `v1.0.0`.

**산출물:** `v1.0.0-rc1` → soak → `v1.0.0` · reliability/benchmark/recovery report · signed/tagged artifacts.

---

## Phase 8 — P1 차별화 (GA 직후 / v1.1)  ·  의존성: GA

GA를 막지는 않지만 경쟁 우위를 만드는 기능. v1.1 타깃.

- [ ] **`ratatosk compat` 마이그레이션 분석기** — AOF/monitor-log/command-list 입력 → tier·v1 지원·risk·action 리포트.
  ```bash
  ratatosk compat --aof appendonly.aof
  ratatosk compat --commands commands.txt --target v1
  ```
  출력: command / tier / v1 support / risk / action 표. (gap-ledger를 데이터 소스로 재사용)
- [ ] **`ratatosk doctor`** — `--check-config`(이미 존재) 확장: 운영 진단(포트/권한/디스크/AOF 상태/메모리).
- [ ] **capability helper crate** — Redis client 위에서 tier 메타 활용:
  ```rust
  let caps = ratatosk_capabilities(&mut conn).await?;
  caps.require("GET", Capability::BehavioralSubset)?;
  caps.warn_if("WAIT", Capability::BaselineLocal)?;
  ```
- [ ] **Listpack compact encoding** — small Hash/Set/ZSet 메모리 2.9~4.9x 절감(optimization Phase 4C, ~5d, Miri/ASan 필요).
- [ ] **Pub/Sub subscribed-state parity 정리** — client 호환성.
- [ ] **blocking fairness / `CLIENT UNBLOCK`** — Redis semantic gap 축소.
- [ ] **Lua scripting supported/experimental 결정** — differential 결과로 v1 supported 여부 확정.
- [ ] **SortedSet SkipList** — rank O(N)→O(log N), 메모리 절감(optimization Phase 7, unsafe + Miri/ASan, 4C 의존).

---

## Phase 9 — v1 이후 별도 프로그램 (out-of-scope, 명시적 제외)  `[-]`

v1에서 구현하지 않는다. 제품 페이지에서 **숨기지 않고** 명시하는 것이 신뢰를 만든다.

| 영역 | v1 결정 | 이유 |
|---|---|---|
| Real replication + PSYNC | 미구현, metadata/strict ERR | durability semantics와 결합, 복잡도 높음 |
| Sentinel | 미구현, HELP 외 ERR | replication 없이는 의미 약함 |
| Cluster bus / MOVED / ASK | slot helper만 baseline_local | 제품 범위가 완전히 달라짐 |
| Redis Functions | unsupported 유지 | scripting + persistence/replication semantics 필요 |
| Redis Search/JSON/Vector | 미구현 | 별도 제품급 범위 |
| Garnet/Dragonfly식 multi-core engine | 미구현 | 아키텍처 재설계급 |

근거 링크: [Redis replication](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/) · [WAIT](https://redis.io/docs/latest/commands/wait/) · [Sentinel](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/) · [Cluster](https://redis.io/docs/latest/operate/oss_and_stack/management/scaling/) · [Valkey cluster-spec](https://valkey.io/topics/cluster-spec/)

---

## 4. 최종 Ship Gate (v1.0.0 GA 허용 조건)

아래가 **전부 참**일 때만 GA. (operations.md §7 + 분석 §11 통합)

- [x] `cargo fmt --all --check` green (2026-06-02 검증)
- [x] `cargo check --workspace --quiet` green (2026-06-02 검증)
- [x] `cargo clippy --workspace --all-targets -- -D warnings` green (2026-06-02 검증)
- [x] `cargo test --workspace --quiet` green (2026-06-02 검증, strict-mode 5 테스트 포함)
- [x] `cargo audit` green · `cargo deny check` green (Phase 3 기 검증, 유지)
- [ ] supported subset Redis/Valkey **differential suite** green _(Phase 5, 외부 redis/valkey 필요)_
- [x] persistence **failure-path matrix** green (`scripts/recovery_matrix.sh` 7/7 로컬 검증)
- [ ] perf guardrail green (CI required) _(baseline green, branch-protection required 미설정)_
- [ ] Sev1/Sev2 open issue **0개** _(이슈 트래커 N/A)_
- [x] **docs/runtime/ledger mismatch 0개** — 명령별 audit로 39건 drift 해소(코드 4건 수정 + ledger 35건 정정), `capability_tier_matches_gap_ledger_for_every_spec` 테스트가 **모든 spec에서 코드=ledger를 강제**(green). `redis_gap_ledger.py check`도 green. _잔여:_ ledger에만 있고 코드가 개별 spec으로 등록 안 한 19개 서브명령(CLIENT ID/COMMAND COUNT 등; 컨테이너 핸들러로 동작)은 spec 등록 여부를 별도 추적(관측 표면은 일치).
- [ ] RC soak 완료 _(릴리즈 후 24–72h)_
- [x] backup/restore/rollback drill 완료 (`scripts/backup_restore_drill.sh` 5/5 로컬 검증)
- [~] signed/tagged release artifact + checksums + SBOM 발행 가능 — checksums ✅(`.sha256`) + SBOM ✅(`sbom.yml`) + keyless cosign 서명 준비완료(opt-in); tag 서명·provenance는 operator 설정 잔여

**+ 분석 §11의 서술형 10조건(요지):**
1. README 첫 화면만으로 single-node 제품임이 명확
2. `COMMAND DOCS` = gap-ledger = 실제 runtime behavior 일치
3. v1 supported subset이 Redis/Valkey differential 통과
4. unsupported/`syntax_only`가 strict에서 조용히 성공하지 않음
5. AOF/RDB recovery가 crash/fault matrix로 검증
6. `INFO`/Prometheus/`PING HEALTH`가 같은 상태 보고
7. production non-loopback bind는 인증 없이는 불가
8. release artifact는 signed/tagged/checksum/SBOM 보유
9. rollback/restore drill이 문서+스크립트로 재현
10. 성능 주장이 "세계 최고"가 아니라 "이 workload에서 이 envelope까지 안전"

---

## 5. Gate → 검증 명령 / 워크플로 매핑

| 게이트 | 로컬 명령 | CI 워크플로 |
|---|---|---|
| 포맷/빌드/lint/test | `cargo fmt --all --check` / `cargo check --workspace` / `cargo clippy --workspace --all-targets -- -D warnings` / `cargo test --workspace` | `rust-ci.yml` |
| 공급망 | `cargo audit` / `cargo deny check` | `security.yml` |
| 명령 ledger 일치 | `python3 scripts/redis_gap_ledger.py` | `gap-ledger.yml` |
| 호환성 differential | (interop harness) | `redis-interop.yml` |
| perf guardrail | `./scripts/bench_baseline.sh` → `python3 scripts/perf_guardrail_check.py` | `perf-guardrail.yml` |
| persistence recovery | `./scripts/smoke_bgrewriteaof.sh` + `scripts/recovery_matrix.sh`(신규) | (신규 워크플로) |
| 릴리즈 산출물 | — | `release.yml` |
| 워크스페이스 검증 | `./scripts/verify_workspace.sh` | — |
| alert 룰 | `promtool check rules monitoring/prometheus/ratatosk-alerts.yml` | (security/ci에 추가) |

---

## 6. 핵심 메시지 (마케팅 고정)

- **A. Honest Redis compatibility** — 모든 명령이 capability tier를 노출하고, supported subset은 Redis/Valkey로 검증된다.
- **B. Single-node durability without operational mystery** — RDB·AOF·manifest recovery gate·shutdown flush·crash drill이 릴리즈 계약의 일부.
- **C. Small, safe, observable cache/event bus** — cache·Pub/Sub·TTL·rate limit·session·local workflow state를 위한 컴팩트 Rust 서버 (Conductor/Ironclaw/command-center/Muninn 통합).
- **D. Strict mode prevents compatibility footguns** — 미지원/`syntax_only` 명령은 거짓 성공 대신 명확히 실패한다.

---

## 7. 변경 이력

| 날짜 | 변경 |
|---|---|
| 2026-06-02 | 초안. operations.md §5 고수준 로드맵을 실행형 트래커로 확장. 분석 기반 strict mode·compat 분석기·reliability report·관측성 차별화 추가. 코드 대조 검증(420 tier 분포, COMMAND DOCS tier 노출, --check-config 존재, strict/compat 미구현). |
| 2026-06-02 | **구현 패스.** Phase 0 전체 완료(strict mode + PRODUCT_CONTRACT + 메시징 동기화 + tier 완전성 테스트 + ledger 일치). Phase 2(SLO.md·Alertmanager), Phase 3(SBOM workflow·TLS 레시피), Phase 4(recovery_matrix 7/7·backup/restore/rollback drill 5/5·durability 표), Phase 6(capacity_envelope), Phase 7(reliability_report·CHANGELOG)의 검증 가능한 산출물 완료. Ship gate: fmt/check/clippy/test/ledger/persistence-matrix/drill green. 잔여: P1 상태모델·P4 AOF materialization(고위험), differential/fuzz(외부도구), 서명/soak/branch-protection(admin). |
| 2026-06-02 | **2차 패스(audit + 잠금).** 코드↔ledger tier **명령별 audit**로 39건 drift 해소(코드 4건+ledger 35건), `capability_tier_matches_gap_ledger_for_every_spec` 테스트로 잠금 → **ship-gate #4 closed**. 분포 정정(syntax_only 30→6, baseline_local 45→76, behavioral_subset 281→275, unsupported 64→63) 후 전 docs 동기화. strict 전수 enumeration 테스트 추가. release.yml에 keyless cosign 서명(OIDC opt-in) 추가. engine 215 테스트 green. |
| 2026-06-02 | **3차 패스(Phase 2·3 코드 마감).** Phase 2: `PING HEALTH` `reasons:` 사유 필드 + `INFO server` feature flags + audit→health 연결 확정. Phase 3: 일급 `protected-mode` 디렉티브(default `yes`) 전 계층 배선(engine `ConfigState`·server `ServerConfig`·`CONFIG GET/SET/REWRITE`·`ratatosk.conf`·env) + non-loopback nopass startup 강제. 신규 테스트 4종 추가(engine 215→217, server 84→85), fmt/clippy(-D warnings)/test/ledger/parity 전부 green. README·operations.md·PRODUCT_CONTRACT.md·CHANGELOG 동기화. INFO memory(allocator/fragmentation=RSS 리더 필요)·pubsub/output-buffer clients 필드는 [~] 후속으로 명시. |
