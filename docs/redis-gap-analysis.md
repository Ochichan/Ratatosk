# Redis Gap Analysis

기준일: 2026-03-12

이 문서는 현재 Ratatosk 코드베이스와 Redis 공식 문서를 대조해, "명령 이름이 존재하는가"가 아니라 "Redis가 약속하는 동작 계약과 운영 모델을 얼마나 실제로 충족하는가"를 정리한다.

비교 기준:

- Ratatosk: 현재 저장소 작업 트리
- Redis: 공식 문서 `latest` 경로(2026-03-12 시점)
- 초점: standalone key-value 서버 이상의 호환성, 운영 특성, 확장 경로

이 문서는 [`docs/redis-gap-ledger.md`](./redis-gap-ledger.md)를 대체하지 않는다. 오히려 그 문서의 status와 `capability_tier`를 해석하는 보완 문서다.

현재 런타임도 `COMMAND DOCS` 응답에 `ratatosk_capability_tier`를 포함해, 클라이언트가 메타데이터 단계에서 구현 수준을 구분할 수 있게 했다.

## Executive Summary

Ratatosk는 이미 다음 영역에서는 꽤 많이 진척되어 있다.

- RESP 파싱/인코딩
- 주요 자료구조(string/hash/list/set/zset/stream)
- expiry/eviction/lazy free
- RDB/AOF 기본 경로
- ACL, Pub/Sub, 일부 운영 명령의 형태적 호환성

하지만 Redis를 실제 운영에 투입할 때 중요한 다음 축에서는 아직 큰 갭이 있다.

1. 복제, Sentinel, Cluster가 "시스템"으로 존재하지 않는다.
2. Lua scripting / Functions가 사실상 비어 있다.
3. blocking command, client-side caching, Pub/Sub delivery가 Redis 내부 모델과 다르다.
4. persistence background work가 Redis의 fork/COW, multipart AOF 모델과 다르다.
5. runtime이 전역 `Mutex<ServerState>` 직렬화에 의존해 확장성과 tail latency 특성이 다르다.
6. 여러 server/client/admin 명령이 실제 동작보다 "syntax-compatible shell"에 가깝다.

현재 ledger tier summary:

- `unsupported=64`
- `syntax_only=30`
- `baseline_local=45`
- `behavioral_subset=281`
- `distributed_parity=0`

결론적으로 현재 Ratatosk는 "Redis 명령을 많이 이해하는 standalone 메모리 서버"로는 설명될 수 있지만, Redis를 대체하는 드롭인 시스템이라고 보기에는 이른 상태다. 특히 replication, failover, cluster redirection, client-side caching invalidation, scripting 생태계에 의존하는 워크로드는 그대로 이식되기 어렵다.

## Method

내부 근거는 다음 파일을 중심으로 확인했다.

- `crates/ratatosk-engine/src/command/cmd_server.rs`
- `crates/ratatosk-engine/src/command/cmd_cluster.rs`
- `crates/ratatosk-engine/src/command/cmd_sentinel.rs`
- `crates/ratatosk-engine/src/command/cmd_client.rs`
- `crates/ratatosk-engine/src/command/cmd_script.rs`
- `crates/ratatosk-engine/src/command/cmd_generic.rs`
- `crates/ratatosk-engine/src/command/cmd_list.rs`
- `crates/ratatosk-engine/src/command/cmd_stream.rs`
- `crates/ratatosk-engine/src/keyspace.rs`
- `crates/ratatosk-server/src/client.rs`
- `crates/ratatosk-server/src/event_loop.rs`
- `crates/ratatosk-server/src/persistence.rs`
- `crates/ratatosk-persist/src/aof/manifest.rs`

외부 근거는 Redis 공식 문서를 사용했다.

- Persistence: <https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/>
- Replication: <https://redis.io/docs/latest/operate/oss_and_stack/management/replication/>
- Sentinel: <https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/>
- Scaling / Cluster: <https://redis.io/docs/latest/operate/oss_and_stack/management/scaling/>
- Client-side caching: <https://redis.io/docs/latest/develop/reference/client-side-caching/>
- Lua scripting: <https://redis.io/docs/latest/develop/programmability/eval-intro/>
- Redis Functions: <https://redis.io/docs/latest/develop/programmability/functions-intro/>
- WAIT: <https://redis.io/docs/latest/commands/wait/>
- WAITAOF: <https://redis.io/docs/latest/commands/waitaof/>
- MONITOR: <https://redis.io/docs/latest/commands/monitor/>
- ROLE: <https://redis.io/docs/latest/commands/role/>
- XREAD: <https://redis.io/docs/latest/commands/xread/>

## Structural Findings

### 1. Replication / HA 계층이 없다

Redis 공식 문서는 replication을 비동기 복제와 partial resynchronization(PSYNC) 기반으로 설명하고, `WAIT`/`WAITAOF`는 write durability 또는 replica acknowledgement를 기다리는 동기화 장치로 둔다. Ratatosk는 2026-03-12 기준으로 standalone-local replication metadata skeleton을 갖췄지만, 실제 네트워크 복제 스트림과 backlog는 아직 없다.

현재 코드:

- `MONITOR`는 여전히 그냥 `OK`를 반환한다: `crates/ratatosk-engine/src/command/cmd_server.rs`
- `ROLE`은 이제 master/replica 모드, logical replication offset, 등록된 replica 목록을 반영한다: `crates/ratatosk-engine/src/command/cmd_server.rs`
- `REPLCONF`는 LISTENING-PORT/CAPA/IP-ADDRESS/ACK/GETACK를 per-client replica metadata에 연결한다: `crates/ratatosk-engine/src/command/cmd_server.rs`
- `PSYNC`는 현재 replid/offset 기준 `FULLRESYNC` 응답과 replica handshake 완료 표시를 남긴다: `crates/ratatosk-engine/src/command/cmd_server.rs`
- `REPLICAOF`/`SLAVEOF NO ONE`는 standalone role transition을 남긴다: `crates/ratatosk-engine/src/command/cmd_server.rs`
- `WAIT`/`WAITAOF`는 tracked replica ACK offset과 local AOF health를 즉시 반영하지만, 아직 blocking wait registry는 없다: `crates/ratatosk-engine/src/command/cmd_generic.rs`

실제 영향:

- Redis 클라이언트나 운영 도구는 이제 최소한 replication progress와 replica lag의 local snapshot은 읽을 수 있다.
- 하지만 `WAIT`/`WAITAOF`는 여전히 non-blocking immediate accounting이므로, Redis처럼 timeout 동안 실제 ACK를 기다리지는 못한다.
- Redis ecosystem에서 흔한 "primary + replica + Sentinel" 운영 모델과 직접 호환되지 않는다.

미래 파손 시나리오:

- 쓰기 직후 `WAIT 1 1000`은 이미 ACK된 replica 수는 반영하지만, timeout 동안 새 ACK를 기다리지는 못해 Redis보다 일찍 0 또는 부분 결과를 반환할 수 있다.
- 재시작/장애 전환 시 replica catch-up 또는 partial sync 같은 개념이 없으므로 운영 자동화가 구성되지 않는다.

권장 순서:

1. 완료: `ROLE`, `INFO replication`, replication offsets를 standalone-local 상태로 연결
2. 다음: `REPLICAOF`/`PSYNC`에 실제 network replication stream과 backlog를 붙일 것
3. 그 후에만 `WAIT`/`WAITAOF`를 blocking semantics까지 승격할 것

Migration cost: high

### 2. Sentinel과 Cluster는 명령 표면만 있고 시스템은 없다

Redis 공식 문서에서 Sentinel은 분산 감시/쿼럼/자동 failover 시스템이고, Cluster는 16384 hash slot, node metadata, redirect, gossip, failover를 포함한 분산 데이터 시스템이다. Ratatosk는 cluster hash slot 계산 일부와 help text는 있지만, 실제 cluster bus, node table, redirect, Sentinel state machine이 없다.

현재 코드:

- `CLUSTER`는 `INFO`, `MYID`, `KEYSLOT`, `COUNTKEYSINSLOT`, `GETKEYSINSLOT`, `HELP`만 처리하고 나머지는 disabled 에러다: `crates/ratatosk-engine/src/command/cmd_cluster.rs:14-31`, `:248-252`
- `CLUSTER INFO`도 `cluster_enabled:0`과 zeroed counters를 돌려준다: `crates/ratatosk-engine/src/command/cmd_cluster.rs:39-58`
- `READONLY`, `READWRITE`, `ASKING`은 모두 단순 `OK`: `crates/ratatosk-engine/src/command/cmd_cluster.rs:258-277`
- `SENTINEL`은 `HELP` 외에는 항상 "not configured as a Sentinel" 에러다: `crates/ratatosk-engine/src/command/cmd_sentinel.rs:11-20`
- 그런데 help text에는 Redis Sentinel의 전체 운영 서브커맨드가 나열돼 있다: `crates/ratatosk-engine/src/command/cmd_sentinel.rs:23-123`

실제 영향:

- slot 계산이나 slot별 key 조회는 가능해도, MOVED/ASK redirect가 없으므로 cluster-aware client는 정상 동작하지 않는다.
- Sentinel discovery endpoint를 기대하는 서비스 디스커버리 또는 failover automation은 Ratatosk를 대상으로 붙을 수 없다.
- "명령이 있다"와 "분산 시스템이 있다"가 완전히 분리되어 있다.

미래 파손 시나리오:

- cluster client가 slot map을 얻고 redirect를 기대하는 순간, Ratatosk는 standalone semantics만 제공해 라우팅 계약이 깨진다.
- Sentinel-aware connection bootstrap이 master 주소를 질의하면 아예 운영 정보가 나오지 않는다.

권장 순서:

1. standalone only 제품으로 계속 갈지, replication+Sentinel, 혹은 cluster까지 갈지 먼저 제품 경계를 결정할 것
2. 그 전까지는 cluster/sentinel 명령을 "지원"으로 마케팅하거나 ledger에서 `done`으로 표시하지 않는 편이 안전하다

Migration cost: high

### 3. Scripting / Functions가 생태계 수준으로 비어 있다

Redis 공식 문서는 `EVAL`의 atomic execution과 Lua integration을, Functions 문서는 library lifecycle과 server-side programmability를 전제로 한다. Ratatosk는 `SCRIPT LOAD/EXISTS/FLUSH` 수준의 캐시 조작만 있고, 실제 실행 엔진이 없다.

현재 코드:

- `EVAL`은 바로 unsupported 에러를 낸다: `crates/ratatosk-engine/src/command/cmd_script.rs:13-18`
- `EVALSHA`는 `NOSCRIPT`만 반환한다: `crates/ratatosk-engine/src/command/cmd_script.rs:20-25`
- `SCRIPT LOAD/EXISTS/FLUSH`는 SHA 캐시 조작만 한다: `crates/ratatosk-engine/src/command/cmd_script.rs:39-120`
- `FCALL`은 항상 function not found: `crates/ratatosk-engine/src/command/cmd_script.rs:149-158`
- `FUNCTION LOAD/DELETE/RESTORE`는 unsupported, `LIST`는 빈 배열, `DUMP`는 null, `STATS`는 빈 엔진 목록이다: `crates/ratatosk-engine/src/command/cmd_script.rs:160-260`

실제 영향:

- Redis에서 흔한 Lua-based atomic business logic, custom compare-and-set, rate limiting, migration scripts를 그대로 옮길 수 없다.
- Redis Functions를 사용하는 최신 Redis 배포 패턴과는 호환되지 않는다.

미래 파손 시나리오:

- 애플리케이션이 `EVALSHA`를 정상적인 hot path로 쓰면 Ratatosk는 성공 경로가 아예 없다.
- 운영자가 Redis Functions로 배포한 서버측 로직을 Ratatosk에선 재현할 수 없다.

권장 순서:

1. Lua engine을 실제로 넣을지
2. 아니면 scripting/function 계열을 명시적으로 "not supported" 범주로 재분류할지

Migration cost: high

### 4. Blocking command 모델이 Redis의 blocked-client scheduler가 아니라 polling retry다

Redis 공식 문서의 blocking commands는 서버가 blocked clients를 관리하고 조건이 충족되면 깨워서 응답하는 형태다. Ratatosk는 한 번 실행해서 결과가 없으면 `CommandOutcome::blocking(...)`을 반환하고, client loop가 sleep/backoff 후 재실행하는 구조다.

현재 코드:

- `BLPOP`/`BRPOP`는 1회 probe 후 blocking outcome을 만든다: `crates/ratatosk-engine/src/command/cmd_list.rs:164-197`
- `XREAD BLOCK`도 결과가 없으면 blocking outcome을 만든다: `crates/ratatosk-engine/src/command/cmd_stream.rs:280-429`
- 실제 대기는 client loop의 exponential backoff 재시도다: `crates/ratatosk-server/src/client.rs:334-452`

실제 영향:

- waiter registration, producer-side wakeup, fair ordering 같은 Redis blocked-client 특성이 없다.
- 대기 중인 클라이언트 수가 늘수록 불필요한 재실행과 락 경합이 늘어난다.
- tail latency가 producer event보다 polling interval/backoff 정책에 좌우된다.

미래 파손 시나리오:

- 많은 consumer가 `BLPOP`/`XREAD BLOCK`을 쓰는 큐 워크로드에서 CPU 소모와 락 경쟁이 급증한다.
- 낮은 레이턴시를 기대하는 stream consumer group 패턴이 Redis와 다른 성능 곡선을 보인다.

권장 순서:

1. blocking wait registry를 서버 상태에 추가
2. key/list/stream 변경 시 waiter를 직접 wakeup
3. polling retry 경로는 fallback 으로만 남길 것

Migration cost: high

### 5. Persistence background work가 Redis와 다른 비용 모델을 가진다

Redis persistence 문서는 RDB background save와 AOF rewrite가 copy-on-write/fork와 background I/O를 활용하고, AOF rewrite는 현재 데이터셋을 재구성하는 최소 명령 집합을 만드는 방향이다. Ratatosk는 snapshot clone과 single-file rewrite에 의존한다.

현재 코드:

- `snapshot_dbs()`는 전체 DB를 그대로 clone한다: `crates/ratatosk-engine/src/keyspace.rs:1965-1967`
- `BGSAVE` 시작 시 global lock 안에서 snapshot clone을 만든다: `crates/ratatosk-server/src/persistence.rs:511-519`
- synchronous `SAVE`도 동일하게 clone 기반이다: `crates/ratatosk-server/src/persistence.rs:679-686`
- runtime은 단일 `appendonly.aof` 파일명을 하드코딩한다: `crates/ratatosk-server/src/persistence.rs:21`, `:67-85`
- `BGREWRITEAOF`는 기존 AOF를 읽어서 `SELECT`를 건너뛰고 동일 command stream을 다시 append한다: `crates/ratatosk-server/src/persistence.rs:163-177`, `:180-260`
- `AofManifest`는 구현돼 있지만 runtime 경로에 연결되지 않는다: `crates/ratatosk-persist/src/aof/manifest.rs:1-80`

실제 영향:

- 큰 데이터셋에서 `SAVE`/`BGSAVE` 시작 순간 메모리 사용량과 pause cost가 Redis보다 나빠질 수 있다.
- AOF rewrite가 "현재 상태 compact"가 아니라 "기존 로그 재작성"에 가깝기 때문에 로그 정리 효과가 제한적이다.
- Redis 7+ multipart AOF 운영 모델과 맞지 않는다.

미래 파손 시나리오:

- 데이터셋이 커질수록 BGSAVE 트리거 순간 clone cost가 latency spike와 memory spike로 나타난다.
- 긴 수명의 write-heavy 시스템에서 BGREWRITEAOF 이후에도 파일 압축 효과가 충분하지 않을 수 있다.

권장 순서:

1. persistence snapshot abstraction을 `ServerState` clone에서 분리
2. 최소한 keyspace serialization 전용 snapshot view를 만들 것
3. AOF rewrite는 current state materialization 기반으로 다시 설계할 것
4. multipart AOF manifest를 runtime에 실제로 연결할 것

Migration cost: high

### 6. Runtime concurrency 모델이 Redis와도 다르고, 멀티코어 확장 모델과도 다르다

Ratatosk는 per-client tokio task를 띄우지만, 실제 명령 실행은 전역 `Arc<Mutex<ServerState>>` 하나를 잠그고 진행한다. Redis의 전통적인 장점은 단일 event loop 위에서 명확한 순서를 유지하는 데 있고, 최근 버전은 I/O thread 등 경계를 명확히 둔다. Ratatosk는 그 중간 형태라서 장점보다 락 기반 병목이 먼저 드러날 가능성이 높다.

현재 코드:

- shared state type alias: `Arc<Mutex<ServerState>>`: `crates/ratatosk-server/src/client.rs:40`
- 일반 명령 실행 직전마다 전역 락을 잡는다: `crates/ratatosk-server/src/client.rs:300-317`
- blocking 재시도도 동일한 락을 반복 획득한다: `crates/ratatosk-server/src/client.rs:404-452`
- accept loop는 클라이언트별 task를 무제한 생성하는 구조다: `crates/ratatosk-server/src/event_loop.rs:281-520`
- `IoThreadPool`은 placeholder다: `crates/ratatosk-server/src/io_thread.rs:1-8`

실제 영향:

- 읽기/쓰기 혼합 부하에서 task 수는 늘어나지만 state execution은 하나의 락으로 수렴한다.
- blocked command polling, Pub/Sub polling, cron, persistence bookkeeping이 모두 같은 상태 경계에 몰린다.
- "async 서버인데 실제 state path는 serial mutex"라는 형태 때문에 성능 분석과 최적화가 더 어렵다.

미래 파손 시나리오:

- 연결 수가 많을수록 tokio task overhead와 mutex contention이 누적된다.
- tail latency가 command complexity보다 lock wait time에 더 민감해질 수 있다.

권장 순서:

1. 제품 전략을 "single-threaded core + background I/O"로 갈지, "sharded state"로 갈지 먼저 고를 것
2. 그 전까지는 global mutex path의 observability를 더 강화할 것

Migration cost: high

## Tactical Findings

### 1. CLIENT TRACKING은 invalidation system이 아니라 connection-local flag 저장이다

Redis client-side caching 문서는 server-side key tracking과 invalidation delivery를 전제로 한다. Ratatosk는 `CLIENT TRACKING` 옵션을 `ClientState`에 저장하지만, 실제 key invalidation path가 없다.

현재 코드:

- tracking flags는 client state 필드일 뿐이다: `crates/ratatosk-engine/src/command/mod.rs:3389-3456`
- `CLIENT TRACKING`은 local flags만 토글한다: `crates/ratatosk-engine/src/command/cmd_client.rs:277-324`
- `TRACKINGINFO`는 empty prefixes를 돌려준다: `crates/ratatosk-engine/src/command/cmd_client.rs:326-345`

영향:

- Redis client-side caching을 기대하는 클라이언트는 invalidation push를 받지 못한다.
- `BCAST`, `OPTIN`, `OPTOUT`, `NOLOOP`, `PREFIX`는 대부분 syntax acceptance 수준이다.

Fix:

- key access/write 경로에 tracking registry와 invalidation publisher를 추가할 것

### 2. CLIENT PAUSE/UNPAUSE/UNBLOCK/SETINFO/REPLY는 대부분 no-op 또는 local-only다

현재 코드:

- `CLIENT PAUSE` / `UNPAUSE`는 실제 dispatch에 영향 없이 `OK`: `crates/ratatosk-engine/src/command/cmd_client.rs:212-243`
- `CLIENT UNBLOCK`은 항상 `0`: `crates/ratatosk-engine/src/command/cmd_client.rs:245-266`
- `CLIENT SETINFO`는 검증 후 `OK`: `crates/ratatosk-engine/src/command/cmd_client.rs:367-378`
- `CLIENT REPLY`는 field만 바꾸고 사용처가 없다: `crates/ratatosk-engine/src/command/cmd_client.rs:402-417`

영향:

- 운영 중 pause/unblock 제어를 쓰는 도구와 맞지 않는다.
- library metadata나 reply suppression semantics를 기대하는 클라이언트가 오판할 수 있다.

Fix:

- 지원하지 않을 기능은 명시적 unsupported error로 바꾸고, 지원할 기능만 실제 경로에 연결할 것

### 3. CLIENT LIST/INFO가 실제 connection inventory를 노출하지 않는다

현재 코드:

- `CLIENT LIST`는 현재 connection 하나만 조건부로 렌더링한다: `crates/ratatosk-engine/src/command/cmd_client.rs:58-70`, `:420-452`
- formatter는 `addr=127.0.0.1:0`, `fd=-1`, `sub=0`, `psub=0`, `ssub=0`, `redir=-1` 같은 고정 값을 쓴다: `crates/ratatosk-engine/src/command/cmd_client.rs:455-500`

영향:

- 운영자가 실제 client table을 볼 수 없다.
- Redis tooling이나 diagnostics가 connection metadata를 신뢰할 수 없다.

Fix:

- server-wide client registry를 만들고 live socket metadata를 채울 것

### 4. Pub/Sub는 pending queue polling 기반이며 subscribed-state 제약도 약하다

현재 코드:

- server는 client별 pending queue를 유지하고 4096개에서 overflow 처리한다: `crates/ratatosk-engine/src/keyspace.rs:632-875`
- subscribed client는 20ms polling으로 pending queue를 확인한다: `crates/ratatosk-server/src/client.rs:29`, `:710-753`, `:867-890`
- subscription 상태여도 일반 command pipeline이 계속 동작한다: `crates/ratatosk-server/src/client.rs:786-834`

영향:

- Redis의 push-heavy event loop delivery보다 coarse하다.
- subscribed-state에서 허용 명령 집합 차이로 일부 클라이언트 가정이 깨질 수 있다.

Fix:

- subscribed client state와 allowed command subset을 명시화하고, 가능하면 wakeup 기반 delivery로 바꿀 것

### 5. MONITOR, MEMORY, LATENCY 일부 응답은 baseline diagnostics다

현재 코드:

- `MONITOR`는 단순 `OK`: `crates/ratatosk-engine/src/command/cmd_server.rs:106-112`
- `LATENCY DOCTOR/GRAPH/HISTOGRAM`은 coarse summary다: `crates/ratatosk-engine/src/command/cmd_server.rs:233-368`
- `MEMORY DOCTOR/MALLOC-STATS/PURGE`는 baseline text 또는 `OK`를 돌려준다: `crates/ratatosk-engine/src/command/cmd_server.rs:929-951`

영향:

- 운영자가 Redis 수준의 introspection을 기대하면 과신하기 쉽다.

Fix:

- 진짜 지표를 채우거나, 최소한 help/ledger/status에서 baseline semantics를 더 강하게 표기할 것

## Dependency Graph Issues

### 1. `ratatosk-persist`에 있는 multipart AOF abstraction이 runtime 경로로 올라오지 못한다

- 문제: `AofManifest`는 `ratatosk-persist`에 존재하지만 `ratatosk-server` runtime은 단일 `appendonly.aof`와 로컬 rewrite 함수에 직접 결합돼 있다.
- 증거: `crates/ratatosk-persist/src/aof/manifest.rs:1-80`, `crates/ratatosk-server/src/persistence.rs:21`, `:67-85`, `:180-260`
- 영향: Redis 7+ persistence evolution을 따라가야 할 때 abstraction reuse가 안 되고, server crate에 persistence policy가 새어 나온다.
- fix: rewrite lifecycle과 file rotation 정책을 `ratatosk-persist` 쪽으로 끌어내릴 것

### 2. Command surface가 실제 subsystem readiness보다 앞서 있다

- 문제: `cmd_cluster`, `cmd_sentinel`, `cmd_client`, `cmd_script`가 많은 명령명을 외부 계약으로 노출하지만, 내부 subsystem이 그만큼 존재하지 않는다.
- 영향: crate 경계상 `command` layer가 capability advertisement를 과도하게 담당한다.
- 진행 상태(2026-03-12): `COMMAND DOCS`가 `ratatosk_capability_tier`와 tier별 summary를 노출하고, `CLUSTER HELP`/`SENTINEL HELP`가 standalone 한계를 명시하도록 보강됐다.
- 남은 작업: unsupported/no-op 계열을 `COMMAND INFO`, 기타 introspection surface, 문서 생성 파이프라인까지 일관되게 전파할 것

## Gap Priority

P0:

- Replication / PSYNC / ROLE / WAIT / WAITAOF를 실제 semantics로 만들지 않으면, Redis replacement로 포지셔닝하기 어렵다.
- Cluster / Sentinel은 help text보다 실제 subsystem 유무가 중요하므로, 제품 범위를 먼저 결정해야 한다.
- Scripting / Functions 부재는 많은 실사용 Redis 워크로드를 바로 막는다.

P1:

- Blocking commands를 waiter model로 전환
- client-side caching invalidation 추가
- persistence rewrite를 current-state compaction 모델로 재설계

P2:

- CLIENT LIST/INFO 실상화
- Pub/Sub delivery path 개선
- MONITOR/MEMORY/LATENCY 운영 진단 개선

## Suggested Refactoring Sequence

1. 제품 경계 재정의

- "Redis-compatible standalone server"로 제한할지
- "replication + Sentinel까지" 갈지
- "cluster까지" 갈지 먼저 결정해야 한다

2. 허위 양성 제거

- 완료: `redis-gap-ledger`의 `done` 중 no-op/unsupported/baseline shell 항목을 capability tier로 재분류
- 완료: help text와 `COMMAND DOCS` metadata에서 unsupported 기능을 명확히 드러냄
- 잔여: `COMMAND INFO` 등 다른 introspection surface도 같은 tier semantics를 반영할 것

3. replication skeleton 우선 구축

- replica state
- replication offsets
- `ROLE`
- `INFO replication`
- backlog
- `WAIT`

4. blocking/client-tracking/runtime 정비

- blocked client registry
- invalidation registry
- client registry
- lock contention observability

5. persistence 재설계

- snapshot clone 제거 또는 축소
- AOF rewrite를 current dataset materialization으로 전환
- multipart AOF manifest 실제 연동

6. 그 다음에만 Sentinel/Cluster 판단

- replication이 없는 상태에서 Sentinel/Cluster를 확장하는 것은 순서가 뒤집혀 있다

## Bottom Line

지금 Ratatosk의 가장 큰 문제는 "구현된 명령 수"가 아니라 "Redis가 제공하는 시스템 계약이 실제로 존재하는가"다. 현재 상태를 가장 정확하게 표현하면:

- standalone data server: 상당히 진척
- Redis command vocabulary: 넓음
- Redis operational/distributed semantics: 아직 큰 갭

따라서 다음 단계의 핵심은 새 명령을 더 채우는 것이 아니라, 이미 표면상 존재하는 명령들 중 replication, cluster, Sentinel, scripting, blocking, client tracking처럼 시스템 성격이 강한 영역을 실제 subsystem으로 승격하는 일이다.
