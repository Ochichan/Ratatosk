# Ratatosk Observability Guide

기준일: 2026-03-26

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
