# Ratatosk Product Contract (v1)

기준일: 2026-03-26

이 문서는 Ratatosk v1이 무엇을 보장하고, 무엇을 보장하지 않는지 명확히 선언한다.
목표는 “single-node Ratatosk GA”이며, “Redis drop-in distributed replacement”가 아니다.

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

근거: `README.md`, `docs/capability-declarations.md`, `docs/redis-gap-ledger.md`

## 2. 지원 계약

Ratatosk v1은 아래만 “지원 계약”에 포함한다.

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

## 4. 지속성 계약

- RDB는 point-in-time snapshot이다
- AOF는 local durability mechanism이다
- durability는 distribution을 의미하지 않는다
- startup replay 순서는 RDB -> AOF다
- persistence format migration이나 rollback safety에 예외 플래그가 필요하면, 그 환경은 ship-ready로 간주하지 않는다

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
