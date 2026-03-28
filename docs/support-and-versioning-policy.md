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
