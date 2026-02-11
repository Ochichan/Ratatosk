# Performance Baseline

Ratatosk 성능 회귀 판단 기준 문서.
이 문서는 2026-02-10 기준으로 정리되었다.

## Quick Workflow

```bash
# 1) baseline 측정 (default allocator)
./scripts/bench_baseline.sh

# 2) 선택: mimalloc 비교
WITH_MIMALLOC=1 ./scripts/bench_baseline.sh

# 3) guardrail 검증
python3 scripts/perf_guardrail_check.py --log <benchmark_log>
```

## Bench Scope

- bench target: `cargo bench -p ratatosk-server --bench pipeline`
- metrics:
  - `pipeline_set_parse_execute_encode`
  - `pipeline_ping_parse_execute_encode`
- pipeline lengths: `1`, `32`, `256`, `1024`

## Guardrail Defaults

- `pipeline_set_parse_execute_encode/256` upper <= `110 us`
- `pipeline_ping_parse_execute_encode/256` upper <= `35 us`
- checker: `scripts/perf_guardrail_check.py`

## Reference Logs

| Log | set@256 upper | ping@256 upper | Guardrail Result | Classification |
| --- | ---: | ---: | --- | --- |
| `benchmarks/baseline-default-20260208-185945.log` | 101.510 us | 29.338 us | PASS | canonical baseline |
| `benchmarks/baseline-default-20260208-191454.log` | 101.610 us | 30.074 us | PASS | compatible baseline |
| `benchmarks/baseline-default-20260208-195805.log` | 103.610 us | 77.826 us | FAIL (ping) | high-variance outlier |

운영 규칙:
- PASS 로그만 guardrail 업데이트 후보로 사용한다.
- FAIL/고분산 로그는 회귀 원인 조사 참고용으로만 보관한다.

## Allocator A/B Policy

- 현재 기본 allocator를 유지한다.
- `mimalloc`은 feature flag(`--features mimalloc`)로 재측정할 수 있다.
- allocator 전환은 최소 2회 이상 일관된 개선 결과가 있을 때만 검토한다.

## Record Template

| Date | Commit | Allocator | set@256 upper (us) | ping@256 upper (us) | Guardrail | Notes |
| --- | --- | --- | ---: | ---: | --- | --- |
| YYYY-MM-DD | <sha> | default |  |  | PASS/FAIL |  |
| YYYY-MM-DD | <sha> | mimalloc |  |  | PASS/FAIL |  |
