# Redis Gap Review (2026-03-12)

이 문서는 2026년 3월 12일 기준 Ratatosk 코드와 Redis 공식 문서를 대조해, `redis-gap-ledger`가 가리는 실제 호환성 갭을 정리한다.

핵심 결론은 단순하다. Ratatosk는 많은 Redis 명령 이름을 수용하지만, Redis의 분산 계약과 운영 계약까지 구현한 것은 아니다. 특히 replication, cluster, sentinel, monitor, client-tracking, 일부 server-admin 표면은 "문법 수용" 또는 "standalone baseline" 수준에 머물러 있다.

## 범위

- Redis 공식 문서 기준 비교 대상
  - Replication / PSYNC / REPLICAOF / ROLE / WAIT / WAITAOF
  - Cluster / CLUSTER family / READONLY / ASKING
  - Sentinel
  - Server-admin surface 중 MONITOR, CLIENT 관리, INFO 운영 단면
- Ratatosk 근거
  - `crates/ratatosk-engine/src/command/*.rs`
  - `crates/ratatosk-server/src/client.rs`
  - `crates/ratatosk-server/src/persistence.rs`
  - `crates/ratatosk-engine/src/keyspace.rs`

## 왜 기존 `redis-gap-ledger`와 결론이 다른가

`docs/redis-gap-ledger.md`는 현재 status 기준으로는 `420 / 420 done`이지만, 이제는 별도 `capability_tier`를 같이 기록한다. 현재 tier summary는 `unsupported=64`, `syntax_only=30`, `baseline_local=45`, `behavioral_subset=281`, `distributed_parity=0`이다. 실제 Redis 호환성은 다음 두 층에서 갈린다.

1. 명령 이름과 문법을 받아들이는가
2. Redis가 약속하는 런타임 계약, 상태 전이, 네트워크 부작용까지 수행하는가

Ratatosk는 1번은 넓게 커버하지만, 2번은 특히 분산/운영 영역에서 크게 비어 있다.

## Executive Summary

- Ratatosk는 현재 실질적으로 `standalone in-memory server with Redis-like command surface`에 가깝다.
- Replication 관련 명령은 대부분 상태 변화 없이 `OK`, 고정 `FULLRESYNC`, 또는 `0`을 반환한다.
- Cluster 관련 명령은 슬롯 해시 계산 정도만 실제 구현되어 있고, 나머지는 `cluster support disabled` 에러 또는 무상태 `OK`다.
- Sentinel은 `HELP`만 구현되어 있고 나머지는 모두 "Sentinel 아님" 오류를 반환한다.
- `CLIENT TRACKING`은 이제 direct invalidation push를 넘어 `BCAST`/`PREFIX`/`NOLOOP`까지 처리하지만, `CLIENT PAUSE`, `CLIENT UNBLOCK`, `MONITOR` 같은 운영 명령은 여전히 실제 서버 동작을 충분히 제어하지 않는다.
- `INFO`도 Redis 운영자가 기대하는 replication/cluster/sentinel 관찰면을 제공하지 않는다.

## Structural Findings

### 1. Replication 명령군은 실제 복제 상태 기계를 만들지 않는다

Redis 공식 문서상 `REPLICAOF`, `PSYNC`, `ROLE`, `WAIT`, `WAITAOF`는 단순 조회 명령이 아니라, 복제 토폴로지와 durability를 구성하거나 관측하는 핵심 계약이다.

- Redis 문서
  - Replication overview: <https://redis.io/docs/latest/operate/oss_and_stack/management/replication/>
  - `PSYNC`: <https://redis.io/docs/latest/commands/psync/>
  - `REPLICAOF`: <https://redis.io/docs/latest/commands/replicaof/>
  - `ROLE`: <https://redis.io/docs/latest/commands/role/>
  - `WAIT`: <https://redis.io/docs/latest/commands/wait/>
  - `WAITAOF`: <https://redis.io/docs/latest/commands/waitaof/>

- Ratatosk 코드 근거
  - `MONITOR`, `ROLE`, `REPLCONF`, `SYNC`, `PSYNC`, `REPLICAOF`: `crates/ratatosk-engine/src/command/cmd_server.rs:106-225`
  - `WAIT`, `WAITAOF`: `crates/ratatosk-engine/src/command/cmd_generic.rs:95-136`
  - `BGSAVE` 실제 비동기 실행 연결: `crates/ratatosk-server/src/client.rs:504-517`
  - 실제 background snapshot / AOF rewrite worker: `crates/ratatosk-server/src/persistence.rs:511-615`

- 관찰
  - `ROLE`는 이제 master/replica 모드, logical repl offset, 등록된 replica list를 반영한다.
  - `PSYNC`는 입력 replid/offset을 검증하고 현재 replid/offset 기준 `FULLRESYNC`를 반환하면서 replica handshake 상태를 남긴다.
  - `SYNC`는 아예 standalone 모드 unsupported 에러다.
  - `REPLCONF`는 LISTENING-PORT/CAPA/IP-ADDRESS/ACK/GETACK를 per-client replica metadata에 저장한다. `GETACK *`도 현재 logical offset을 돌려준다.
  - `REPLICAOF host port`와 `REPLICAOF NO ONE`은 standalone role transition을 실제 상태로 남긴다.
  - `WAIT`와 `WAITAOF`는 tracked replica ACK/local AOF health를 즉시 반영하지만, 아직 timeout 동안 실제 ACK를 기다리지는 않는다.

- 갭의 의미
  - Redis에서 이 명령들은 "복제가 존재한다"는 전제 위에서 의미가 생긴다.
  - Ratatosk는 이제 replication offset, replica handshake metadata, ACK 수집의 local skeleton은 갖췄지만, backlog/partial resync/network streaming/failover 전이는 여전히 없다.
  - 따라서 Redis 클라이언트나 운영 도구가 이 응답을 보고 full durability/replication을 신뢰하면 여전히 오판할 수 있다.

- 분류
  - 상태: `parse/ack only`
  - 위험도: 높음

### 2. Cluster는 "슬롯 해시 계산기 + help text" 수준이며 Redis Cluster 계약과 다르다

Redis Cluster는 슬롯 매핑, `MOVED`/`ASK` redirection, master/replica role, slot migration, cluster bus, 단일 DB 제약을 포함한 별도 분산 프로토콜이다.

- Redis 문서
  - Cluster spec: <https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/>
  - `CLUSTER SLOTS`: <https://redis.io/docs/latest/commands/cluster-slots/>
  - `ASKING`: <https://redis.io/docs/latest/commands/asking/>
  - `READONLY`: <https://redis.io/docs/latest/commands/readonly/>

- Ratatosk 코드 근거
  - `CLUSTER` dispatch 및 stub: `crates/ratatosk-engine/src/command/cmd_cluster.rs:14-31`, `248-251`
  - 실제 구현된 하위 명령은 `INFO`, `MYID`, `KEYSLOT`, `COUNTKEYSINSLOT`, `GETKEYSINSLOT`, `HELP`뿐: `crates/ratatosk-engine/src/command/cmd_cluster.rs:23-30`
  - `READONLY`, `READWRITE`, `ASKING`은 모두 무상태 `OK`: `crates/ratatosk-engine/src/command/cmd_cluster.rs:258-276`
  - 서버는 기본 16 DB를 가진다: `crates/ratatosk-engine/src/keyspace.rs:17`, `1914-1915`
  - `COUNTKEYSINSLOT`/`GETKEYSINSLOT`도 현재 `selected_db`만 조회한다: `crates/ratatosk-engine/src/command/cmd_cluster.rs:90-141`

- 관찰
  - `CLUSTER HELP`는 매우 많은 Redis subcommand를 나열하지만, 실제 dispatch는 대부분 `cluster_stub()`로 떨어져 "cluster support disabled"를 반환한다.
  - `CLUSTER INFO`도 `cluster_enabled:0`, `cluster_known_nodes:1`, `cluster_slots_assigned:0` 같은 고정 문자열이다.
  - `READONLY`, `READWRITE`, `ASKING`은 연결 상태를 바꾸지 않으며, 이후 라우팅이나 key access 정책에 영향을 주지 않는다.
  - Redis Cluster는 단일 DB 모델인데, Ratatosk cluster 관련 조회는 `selected_db`를 그대로 사용한다.
  - `MOVED`/`ASK` redirection, slot ownership map, migration/importing state, node discovery, replica read routing이 전혀 없다.

- 갭의 의미
  - 현재 구현은 "키를 Redis Cluster와 동일한 방식으로 해시 슬롯에 매핑하는 유틸" 정도는 제공한다.
  - 하지만 Redis Cluster 클라이언트가 기대하는 분산 라우팅 계약은 사실상 전무하다.
  - 명령 카탈로그상 `CLUSTER SLOTS`, `CLUSTER SHARDS`, `CLUSTER NODES`, `CLUSTER FAILOVER`가 done으로 보이는 것은 실제 동작 수준을 과대표시한다.

- 분류
  - 상태: `mostly unsupported with a few local helpers`
  - 위험도: 높음

### 3. Sentinel은 기능이 아니라 명령 이름만 노출한다

Redis Sentinel은 모니터링, 알림, 자동 failover, configuration provider 역할을 수행한다.

- Redis 문서
  - Sentinel overview: <https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/>

- Ratatosk 코드 근거
  - `SENTINEL` 구현 전체: `crates/ratatosk-engine/src/command/cmd_sentinel.rs:11-123`

- 관찰
  - 실제 동작하는 subcommand는 `HELP`뿐이다.
  - `SENTINEL MASTERS`, `MASTER`, `REPLICAS`, `GET-MASTER-ADDR-BY-NAME`, `CKQUORUM`, `FAILOVER`, `MONITOR` 등은 help 텍스트에만 존재한다.
  - 실행 시 `HELP`를 제외한 모든 subcommand는 `"ERR This instance is not configured as a Sentinel"`로 끝난다.

- 갭의 의미
  - Sentinel은 Redis 배포에서 외부 클라이언트가 master 주소를 발견하고 장애 조치를 수행하는 핵심 컴포넌트다.
  - Ratatosk는 현재 Sentinel 기능이 전혀 없으므로, Sentinel ecosystem과의 호환을 주장할 수 없다.

- 분류
  - 상태: `unsupported`
  - 위험도: 높음

### 4. Client-side caching / MONITOR / client control은 대부분 로컬 플래그 수준이다

Redis의 운영 표면에는 단순 응답 이상의 부작용이 있다. `MONITOR`는 실시간 명령 스트림을 흘려야 하고, `CLIENT TRACKING`은 invalidation을 발행해야 하며, `CLIENT PAUSE`/`UNBLOCK`은 실제 연결 흐름을 바꿔야 한다.

- Redis 문서
  - `MONITOR`: <https://redis.io/docs/latest/commands/monitor/>
  - `CLIENT TRACKING`: <https://redis.io/docs/latest/commands/client-tracking/>
  - Client-side caching reference: <https://redis.io/docs/latest/develop/reference/client-side-caching/>

- Ratatosk 코드 근거
  - `MONITOR`: `crates/ratatosk-engine/src/command/cmd_server.rs:106-112`
  - `CLIENT` command family: `crates/ratatosk-engine/src/command/cmd_client.rs:15-418`
  - client tracking state fields는 `ClientState`에만 존재: `crates/ratatosk-engine/src/command/mod.rs:3380-3456`
  - runtime은 live client snapshot registry를 유지하고 `INFO Clients`/`CLIENT LIST`가 이를 읽는다: `crates/ratatosk-engine/src/keyspace.rs`, `crates/ratatosk-server/src/client.rs`, `crates/ratatosk-engine/src/command/cmd_client.rs`

- 관찰
  - `MONITOR`는 아무 monitor 모드 전환 없이 단순 `OK`를 반환한다.
  - `CLIENT TRACKING`은 direct-key invalidation registry에 더해 `BCAST`/`PREFIX`/`NOLOOP` registry와 async invalidate push를 사용한다.
  - `CLIENT TRACKINGINFO`는 redirect, flags, prefix 목록을 실제 tracking state로 보여준다.
  - `CLIENT CACHING YES|NO`는 `OPTIN`/`OPTOUT` 모드에서 다음 read 1회에만 적용되는 gating으로 동작한다.
  - redirect target은 이제 notifier 기반으로 깨어나 invalidate push를 받을 수 있고, 연결되지 않은 target은 `REDIRECT` 단계에서 거부된다.
  - dead target으로의 stale invalidate enqueue는 막혔고, target disconnect 뒤에는 `broken_redirect` marking과 RESP3 `tracking-redir-broken` push, tracker direct fallback이 동작한다.
  - 다만 unsupported 조합 처리와 fallback semantics 전체까지 Redis full contract는 아니다.
  - `CLIENT PAUSE`/`UNPAUSE`는 입력 검증 후 `OK`만 반환한다.
  - `CLIENT UNBLOCK`은 대상 client를 찾거나 상태를 바꾸지 않고 항상 `0`을 반환한다.
  - `CLIENT KILL`도 사실상 현재 client id와 일치하는지 정도만 본다.
  - `CLIENT LIST`와 `INFO Clients`는 이제 live connection inventory, blocked client count, tracking client count를 반영한다. 다만 Redis의 전체 필드 충실도에는 아직 못 미친다.
  - list/sorted-set/stream blocking commands는 이제 blocked wait registry와 producer-side wakeup을 사용하지만, `CLIENT UNBLOCK`나 Redis식 fairness scheduler까지는 올라오지 않았다.

- 갭의 의미
  - Redis tooling은 이 명령들을 운영 제어면으로 사용한다.
  - Ratatosk에서는 이 명령들이 관찰/제어를 실제로 수행하지 않기 때문에, 운영 자동화가 성공처럼 보이면서 아무 일도 일어나지 않는 문제가 생긴다.

- 분류
  - 상태: `local-state only / no-op`
  - 위험도: 중간 이상

## Tactical Findings

### 5. `INFO`는 운영자가 기대하는 replication/cluster/sentinel 관찰면을 제공하지 않는다

- Ratatosk 코드 근거
  - `INFO` 섹션 선택은 `SERVER`, `CLIENTS`, `STATS`, `KEYSPACE`, `PERSISTENCE`만 지원: `crates/ratatosk-engine/src/command/cmd_server.rs:46-103`
  - 구현 섹션 본문: `crates/ratatosk-engine/src/command/cmd_server.rs:959-1090`

- 관찰
  - `INFO replication`, `INFO cluster`, `INFO modules`, `INFO cpu`, `INFO commandstats`, `INFO errorstats` 등 Redis 운영에서 자주 보는 섹션이 없다.
- `INFO Clients`는 이제 실제 blocked/tracking client 수를 반영하지만, pause/unblock/monitor 같은 더 강한 운영 제어까지는 아니다.
  - `INFO Persistence`도 `aof_current_size:0`, `aof_base_size:0` 같은 고정값을 쓴다.

- 영향
  - Ratatosk를 Redis-compatible monitoring target으로 붙이면 대시보드와 알람이 중요한 상태 변화를 관찰하지 못한다.

### 6. `FLUSHDB/FLUSHALL ASYNC`는 실제 async free 의미가 약하다

- Ratatosk 코드 근거
  - `SYNC|ASYNC` 파싱만 수행: `crates/ratatosk-engine/src/command/cmd_server.rs:1227-1239`
  - `FLUSHDB`/`FLUSHALL`은 mode 문자열만 기록하고 즉시 clear 수행: `crates/ratatosk-engine/src/command/cmd_server.rs:1241-1322`

- 관찰
  - Redis에서는 `ASYNC`가 lazy freeing과 연결되지만, 여기서는 실질적으로 mode 태그만 남기고 즉시 비운다.

- 영향
  - 대형 dataset에서 Redis와 같은 지연 특성을 기대하면 어긋난다.

### 7. `BGSAVE`와 `BGREWRITEAOF`는 실제 비동기 작업이 연결되어 있지만, Redis 배포 계약 전체를 대체하지는 못한다

- Ratatosk 코드 근거
  - 명령 ack: `crates/ratatosk-engine/src/command/cmd_server.rs:1154-1194`
  - 실제 worker 시작: `crates/ratatosk-server/src/client.rs:504-517`
  - background task 구현: `crates/ratatosk-server/src/persistence.rs:511-615`

- 관찰
  - 이 둘은 "순수 no-op"은 아니다. 실제 background snapshot / rewrite task가 존재한다.
  - 하지만 이 persistence 구현이 replication, failover, Sentinel, cluster state 저장을 대신해주지는 않는다.

- 영향
  - 운영 표면 중 persistence는 비교적 실체가 있지만, 분산 Redis 운영 계약과 혼동하면 안 된다.

## Dependency Graph Issues

- `CLIENT TRACKING`은 direct invalidation push를 넘어 `BCAST`/`PREFIX`/`NOLOOP`까지 올라왔지만, `PAUSE`, `UNBLOCK`, `MONITOR`는 여전히 per-client 로컬 상태 또는 immediate reply로 끝난다
  - 관련 코드: `crates/ratatosk-engine/src/command/cmd_client.rs:212-377`, `crates/ratatosk-engine/src/command/cmd_server.rs:106-112`
  - 서버 런타임과 연결된 제어 경로가 없다.

- 분산 기능이 필요한데 전체 상태 모델은 단일 `Arc<Mutex<ServerState>>`에 묶여 있다
  - 관련 코드: `crates/ratatosk-server/src/client.rs:40`, `crates/ratatosk-server/src/client.rs` 전반의 `server_state.lock().await`
  - 이 구조는 standalone 일관성에는 단순하지만 replica link, cluster bus, Sentinel peer state처럼 독립적인 네트워크 주체를 확장하기 어렵다.

- `ServerState`는 기본 16 DB를 전제로 유지되며, cluster helper도 `selected_db`를 읽는다
  - 관련 코드: `crates/ratatosk-engine/src/keyspace.rs:17`, `crates/ratatosk-engine/src/keyspace.rs:1914-1915`, `crates/ratatosk-engine/src/command/cmd_cluster.rs:90-141`
  - Redis Cluster의 단일 DB 제약과 모델이 다르다.

## Recommended Positioning

현재 코드 상태에서 가장 정직한 포지셔닝은 아래 둘 중 하나다.

1. "Redis protocol-compatible standalone cache/database"
2. "Redis command-surface emulator for a subset of operational tooling"

반대로 아래 주장은 현재 코드와 맞지 않는다.

- "Redis cluster-compatible"
- "Redis Sentinel-compatible"
- "Redis replication-compatible"
- "Redis client-side caching-compatible"
- "Redis operational parity"

## Suggested Refactoring Sequence

1. `docs/redis-gap-ledger.md`의 `done` 정의를 재설계한다.
   - `syntax_only`, `baseline_local`, `behavioral_subset`, `distributed_parity` 같은 단계가 필요하다.
2. 분산 기능을 목표로 할지, standalone 고도화를 목표로 할지 먼저 결정한다.
   - 둘을 동시에 잡으면 문서와 구현이 계속 어긋난다.
3. standalone 전략이면 다음을 명확히 한다.
   - cluster / sentinel은 `unsupported`로 낮추고, replication / client tracking은 standalone-local tier로 문서화한다.
   - misleading help text와 command metadata를 줄인다.
4. distributed 전략이면 다음 순서가 맞다.
   - replication state machine
   - replication backlog / offsets / ACK accounting
   - ROLE/WAIT/WAITAOF 진실화
   - cluster slot ownership + MOVED/ASK
   - replica read routing + ASKING/READONLY 상태
   - Sentinel 또는 외부 failover provider 연동

## Next Workstreams

### 1. Redirect wakeup / tracking delivery

- 권장 순서:
  1. redirect lifecycle 표를 먼저 고정
     - target disconnect 시 auto-detach
     - reconnect 시 explicit rebind only
     - stale pending invalidation drop
  2. tracker config와 target delivery capability를 server-wide registry로 정규화
  3. direct/bcast/prefix/noloop/optin/optout/redirect 조합별 behavior matrix를 테스트와 에러 메시지까지 같이 고정
  4. `TRACKINGINFO`, `CLIENT LIST`, disconnect cleanup을 같은 source-of-truth로 묶기
- release gate:
  - redirect target disconnect에서 stale state가 남지 않아야 한다.
  - redirected invalidation이 polling tick 의존 없이 도착해야 한다.
  - unsupported 조합은 문서와 에러 메시지가 일치해야 한다.
  - reconnect한 새 client id는 이전 redirect binding을 암묵적으로 승계하지 않아야 한다.

### 2. Persistence/runtime redesign

- 권장 순서:
  1. `ratatosk-server`와 `ratatosk-persist`의 ownership 경계 문서화
  2. full clone snapshot을 iterable serialization view로 치환
  3. AOF rewrite를 current-state materialization + incremental tail로 교체
  4. multipart manifest의 rewrite switch/rotation을 atomic transaction으로 연결
  5. `INFO persistence`와 background job observability 보강
- release gate:
  - runtime이 단일 `appendonly.aof` 경로 가정을 직접 들고 있지 않아야 한다.
  - `BGSAVE`/`BGREWRITEAOF` 시작 시 메모리 증폭과 pause cost가 측정 가능해야 한다.
  - crash/restart/recovery integration test가 manifest swap까지 고정해야 한다.
  - failed switch 이후에도 startup이 old manifest 또는 validated candidate 중 하나로만 부팅되어야 한다.
  - server crate가 filename/rotation 정책을 직접 소유하지 않아야 한다.
  - `ratatosk-persist`의 manifest candidate validation / cleanup helper가 BASE materialization + full switch transaction까지 확장되어야 한다.

## Bottom Line

Ratatosk는 현재 "Redis command catalog coverage"는 넓지만, Redis의 분산 시스템 계약은 거의 구현하지 않았다. 문서와 메타데이터가 이 차이를 숨기면 사용자와 운영자가 가장 위험한 부분에서 잘못된 신뢰를 갖게 된다.

따라서 다음 릴리스 전에는 최소한 아래 둘 중 하나가 필요하다.

- 분산/운영 명령을 정직하게 `unsupported` 또는 `syntax-only`로 재분류
- 아니면 replication/cluster/sentinel 동작을 실제 계약 수준까지 채우는 대형 아키텍처 작업 착수

## Source Links

- Redis Replication: <https://redis.io/docs/latest/operate/oss_and_stack/management/replication/>
- Redis Sentinel: <https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/>
- Redis Cluster specification: <https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/>
- `PSYNC`: <https://redis.io/docs/latest/commands/psync/>
- `REPLICAOF`: <https://redis.io/docs/latest/commands/replicaof/>
- `ROLE`: <https://redis.io/docs/latest/commands/role/>
- `WAIT`: <https://redis.io/docs/latest/commands/wait/>
- `WAITAOF`: <https://redis.io/docs/latest/commands/waitaof/>
- `CLUSTER SLOTS`: <https://redis.io/docs/latest/commands/cluster-slots/>
- `ASKING`: <https://redis.io/docs/latest/commands/asking/>
- `READONLY`: <https://redis.io/docs/latest/commands/readonly/>
- `MONITOR`: <https://redis.io/docs/latest/commands/monitor/>
- `CLIENT TRACKING`: <https://redis.io/docs/latest/commands/client-tracking/>
- Client-side caching reference: <https://redis.io/docs/latest/develop/reference/client-side-caching/>
