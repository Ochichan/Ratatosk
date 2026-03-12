# Redis Gap Analysis

기준일: 2026-03-13

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
- `WAIT`/`WAITAOF`는 tracked replica ACK offset과 local AOF health를 즉시 반영하지만, 여전히 timeout 동안 ACK를 기다리는 blocking semantics는 없다: `crates/ratatosk-engine/src/command/cmd_generic.rs`

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

### 4. Blocking command는 blocked wait registry를 갖췄지만 Redis scheduler parity에는 아직 못 미친다

Redis 공식 문서의 blocking commands는 서버가 blocked clients를 관리하고 조건이 충족되면 깨워서 응답하는 형태다. Ratatosk는 2026-03-12 기준으로 더 이상 polling-only는 아니다. 이제 한 번 실행해서 결과가 없으면 `CommandOutcome::blocking(...)`이 watched key 집합을 담아 반환되고, server state가 blocked wait registry를 유지하며 write path가 해당 key waiter를 깨운다. 다만 Redis의 공정한 blocked-client scheduler와 full command coverage까지 올라온 상태는 아니다.

현재 코드:

- `BLPOP`/`BRPOP`/`BLMOVE`/`BLMPOP`, `BZ*`, `XREAD BLOCK`/`XREADGROUP BLOCK`은 1회 probe 후 watched key를 담은 blocking outcome을 만든다: `crates/ratatosk-engine/src/command/cmd_list.rs`, `crates/ratatosk-engine/src/command/cmd_sorted_set.rs`, `crates/ratatosk-engine/src/command/cmd_stream.rs`
- server state는 blocked client별 watched key와 notifier를 registry로 유지한다: `crates/ratatosk-engine/src/keyspace.rs`
- write path는 변경된 key에 대해 blocked waiter를 먼저 notify하고, 그 다음 tracking invalidation을 발행한다: `crates/ratatosk-engine/src/command/mod.rs`
- runtime은 blocking 대기 중 `Notify`, timeout, disconnect를 동시에 기다리며 wakeup 시 즉시 재실행한다: `crates/ratatosk-server/src/client.rs`

실제 영향:

- list/sorted-set/stream blocking command는 이제 producer-side wakeup을 가지므로, matching write가 들어오면 polling interval을 기다리지 않고 다시 실행된다.
- blocked client inventory가 runtime registry에 반영돼 `CLIENT LIST`/`INFO clients`와도 일관성을 가진다.
- 다만 Redis처럼 command family 전반이 동일 scheduler를 공유하는 구조는 아니고, fairness/priority ordering, cross-command wakeup 정책, `CLIENT UNBLOCK` integration은 아직 없다.

미래 파손 시나리오:

- 많은 blocked consumer가 한 key set을 공유할 때 Redis와 같은 fairness나 starvation 방지가 없어 wakeup ordering이 다를 수 있다.
- `WAIT`/`WAITAOF`, Pub/Sub, redirect tracking 같은 인접 기능은 아직 별도 wakeup model을 쓰거나 polling에 의존하므로 운영 특성이 균일하지 않다.

권장 순서:

1. 완료: list/sorted-set/stream blocking command에 blocked wait registry와 producer-side wakeup 연결
2. 다음: fairness, `CLIENT UNBLOCK`, 추가 blocking families를 같은 scheduler 모델로 확장
3. 그 후: timeout/backoff 재시도는 strict fallback 경로로 더 축소

Migration cost: high

### 5. Persistence background work가 Redis와 다른 비용 모델을 가진다

Redis persistence 문서는 RDB background save와 AOF rewrite가 copy-on-write/fork와 background I/O를 활용하고, AOF rewrite는 현재 데이터셋을 재구성하는 최소 명령 집합을 만드는 방향이다. Ratatosk는 여전히 snapshot clone과 single-file rewrite에 크게 의존하지만, multipart AOF manifest의 bootstrap/startup recovery baseline은 이제 runtime에 연결됐다.

현재 코드:

- `snapshot_dbs()`는 전체 DB를 그대로 clone한다: `crates/ratatosk-engine/src/keyspace.rs:1965-1967`
- `BGSAVE` 시작 시 global lock 안에서 snapshot clone을 만든다: `crates/ratatosk-server/src/persistence.rs:511-519`
- synchronous `SAVE`도 동일하게 clone 기반이다: `crates/ratatosk-server/src/persistence.rs:679-686`
- runtime은 legacy single-file 경로를 compatibility fallback으로 유지하지만, appendonly bootstrap은 manifest를 우선 사용한다: `crates/ratatosk-server/src/persistence.rs`
- `BGREWRITEAOF`는 여전히 기존 AOF를 읽어서 `SELECT`를 건너뛰고 동일 command stream을 다시 append한다: `crates/ratatosk-server/src/persistence.rs`
- `AofManifest`는 save/load, bootstrap, startup recovery baseline에 더해 manifest-backed rewrite 후 새 incr 회전과 manifest commit baseline까지 runtime 경로에 연결됐다. 다만 BASE file materialization과 full atomic switch transaction은 아직 없다: `crates/ratatosk-persist/src/aof/manifest.rs`, `crates/ratatosk-persist/src/aof/switch.rs`, `crates/ratatosk-server/src/persistence.rs`

실제 영향:

- 큰 데이터셋에서 `SAVE`/`BGSAVE` 시작 순간 메모리 사용량과 pause cost가 Redis보다 나빠질 수 있다.
- AOF rewrite가 "현재 상태 compact"가 아니라 "기존 로그 재작성"에 가깝기 때문에 로그 정리 효과가 제한적이다.
- Redis 7+ multipart AOF의 bootstrap/recovery/rewrite-rotation baseline에는 가까워졌지만, BASE materialization과 current-state compaction 모델은 아직 맞지 않는다.

미래 파손 시나리오:

- 데이터셋이 커질수록 BGSAVE 트리거 순간 clone cost가 latency spike와 memory spike로 나타난다.
- 긴 수명의 write-heavy 시스템에서 BGREWRITEAOF 이후에도 파일 압축 효과가 충분하지 않을 수 있다.

권장 순서:

1. persistence snapshot abstraction을 `ServerState` clone에서 분리
2. 최소한 keyspace serialization 전용 snapshot view를 만들 것
3. AOF rewrite는 current state materialization 기반으로 다시 설계할 것
4. multipart AOF manifest switch와 rewrite rotation까지 runtime/persist 경계에 맞춰 완성할 것

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

### 1. CLIENT TRACKING은 direct-key invalidation skeleton까지는 올라왔지만 Redis full tracking과는 아직 거리가 있다

Redis client-side caching 문서는 server-side key tracking과 invalidation delivery를 전제로 한다. Ratatosk는 2026-03-13 기준으로 direct-key invalidation registry와 async invalidate push를 넘어, `BCAST`/`PREFIX`/`NOLOOP`와 `OPTIN`/`OPTOUT` one-shot gating, redirect target notifier wakeup, connected-target validation, dead-target drop, target disconnect 시 `broken_redirect` marking, RESP3 `tracking-redir-broken` push, tracker direct fallback까지 server-side/runtime path에 연결했다. 다만 unsupported 조합 처리와 Redis full tracking contract 전체는 아직 남아 있다.

현재 코드:

- key access/write 경로가 tracking registry, broadcast registry, invalidate push를 함께 건드린다: `crates/ratatosk-engine/src/command/mod.rs`, `crates/ratatosk-engine/src/keyspace.rs`
- `CLIENT TRACKING`은 ON/OFF/REDIRECT와 함께 `BCAST`/`PREFIX`/`NOLOOP`/`OPTIN`/`OPTOUT`를 registry reset과 함께 처리한다: `crates/ratatosk-engine/src/command/cmd_client.rs`
- runtime은 client별 async-push notifier와 pending queue를 함께 사용해 invalidation을 실제 전달한다: `crates/ratatosk-server/src/client.rs`
- `TRACKINGINFO`와 `GETREDIR`는 configured redirect를 유지하고, breakage는 `broken_redirect` flag로 별도 노출한다: `crates/ratatosk-engine/src/command/cmd_client.rs`
- `CLIENT CACHING YES|NO`는 `OPTIN`/`OPTOUT` 모드에서 다음 read 1회에만 적용되는 gating으로 동작한다: `crates/ratatosk-engine/src/command/cmd_client.rs`, `crates/ratatosk-engine/src/command/mod.rs`
- `REDIRECT`는 이제 연결된 target client만 수용하고, disconnect된 target으로는 pending invalidation을 쌓지 않는다. RESP3 tracker에는 `tracking-redir-broken` push를 보내고, delivery는 tracker direct fallback으로 전환된다: `crates/ratatosk-engine/src/command/cmd_client.rs`, `crates/ratatosk-engine/src/keyspace.rs`, `crates/ratatosk-server/src/client.rs`

영향:

- direct-key tracking과 broadcast-prefix tracking을 쓰는 클라이언트는 invalidate push를 받을 수 있다.
- self-write invalidation은 `NOLOOP`로 억제할 수 있다.
- assisted caching `OPTIN`/`OPTOUT`도 이제 next-command gating까지 baseline 수준으로 동작한다.
- redirect target도 이제 polling tick을 기다리지 않고 notifier로 깨어난다.
- dead target으로의 stale enqueue도 이제 막힌다.
- target disconnect 뒤에도 tracker는 configured redirect id를 유지하고, `TRACKINGINFO flags`에 `broken_redirect`가 붙는다.
- RESP3 tracker는 `tracking-redir-broken` push를 받고, 이후 tracked key invalidation은 tracker 자신에게 직접 돌아온다.
- 하지만 unsupported 조합 처리와 fallback semantics 전체가 Redis와 완전히 같지는 않다.

Fix:

- 남은 작업: unsupported 조합 처리와 redirect breakage 이후 fallback semantics를 Redis 계약에 더 가깝게 만들 것

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

- runtime은 live socket metadata를 server-wide client registry에 밀어 넣고, `CLIENT LIST`/`CLIENT INFO`/`INFO clients`가 이를 읽는다: `crates/ratatosk-server/src/client.rs`, `crates/ratatosk-engine/src/keyspace.rs`, `crates/ratatosk-engine/src/command/cmd_client.rs`, `crates/ratatosk-engine/src/command/cmd_server.rs`
- formatter는 여전히 `fd`, memory counters, 일부 subscription 세부 필드를 baseline 수준으로만 채운다.

영향:

- 운영자는 이제 실제 connected/blocked/tracking inventory를 볼 수 있다.
- 다만 Redis tooling이 기대하는 전체 필드 충실도와 server-wide control plane은 아직 부족하다.

Fix:

- 남은 작업: Redis가 노출하는 전체 client metadata와 pause/unblock/kill semantics를 registry에 연결할 것

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

### 1. `ratatosk-persist`의 multipart AOF abstraction이 runtime 전체 lifecycle을 아직 소유하지 못한다

- 문제: `AofManifest` save/load, startup recovery, rewrite 후 incr rotation/manifest commit baseline은 올라왔지만, BASE materialization과 full atomic switch transaction은 아직 `ratatosk-server` orchestration과 기존 single-file rewrite 모델에 묶여 있다.
- 증거: `crates/ratatosk-persist/src/aof/manifest.rs`, `crates/ratatosk-persist/src/aof/rewrite.rs`, `crates/ratatosk-server/src/persistence.rs`
- 영향: Redis 7+ persistence evolution을 따라갈 때 crate 경계가 다시 흐려지고, multipart 운영 규칙이 runtime 정책과 섞인다.
- fix: rewrite lifecycle과 file rotation 정책을 `ratatosk-persist` 쪽으로 더 끌어내릴 것

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
- client-side caching invalidation을 redirect wakeup까지 확장
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

- blocked scheduler parity(`CLIENT UNBLOCK`, fairness, 추가 blocking families)
- invalidation registry 확장(redirect wakeup)
- client registry
- lock contention observability

5. persistence 재설계

- snapshot clone 제거 또는 축소
- AOF rewrite를 current dataset materialization으로 전환
- multipart AOF manifest switch와 rewrite rotation 완성

6. 그 다음에만 Sentinel/Cluster 판단

- replication이 없는 상태에서 Sentinel/Cluster를 확장하는 것은 순서가 뒤집혀 있다

## Detailed Execution Plans

### Workstream A: client-side caching redirect wakeup 정합화

목표:

- `CLIENT TRACKING REDIRECT <id>`가 단순 target accounting이 아니라 실제 delivery/wakeup 계약까지 갖도록 만든다.
- direct tracking, `BCAST`, `PREFIX`, `NOLOOP`, `OPTIN`, `OPTOUT`가 redirect target에서도 같은 규칙으로 동작하게 만든다.
- invalidation delivery가 현재의 polling 의존 경로에서 event-driven 경로로 더 이동하도록 만든다.

권장 범위:

- 이번 workstream은 Redis full monitor/pubsub overhaul이 아니다.
- 범위는 `CLIENT TRACKING`, invalidate push delivery, redirect target lifecycle, wakeup observability로 한정한다.

Phase A0. 계약 표 확정

1. direct tracking / broadcast tracking / redirect 조합별 기대 동작 표를 문서에 먼저 고정한다.
2. `REDIRECT` 대상이 없을 때, 끊겼을 때, tracker와 target이 같을 때, target이 blocked 상태일 때 동작을 결정한다.
3. `NOLOOP`의 기준이 tracker client인지 target client인지 명시한다.
4. RESP2/RESP3 연결에서 invalidate push 허용 조건을 명시한다.

성공 기준:

- `docs/redis-gap-analysis.md`와 `docs/redis-gap-review-2026-03-12.md`에 behavior matrix가 생긴다.
- 구현 전후에 어떤 케이스가 바뀌는지 reviewer가 문서만 보고 판단할 수 있다.

Phase A1. 상태 모델 정규화

1. `ClientState`의 tracking 설정과 runtime delivery capability를 분리한다.
2. `ServerState`에 tracker config, redirect target liveness, pending invalidate delivery state를 한곳에서 볼 수 있는 registry를 둔다.
3. disconnect, `RESET`, `CLIENT TRACKING OFF`, redirect target 교체 시 정리 순서를 명시한다.
4. `CLIENT LIST`/`CLIENT INFO`/`TRACKINGINFO`가 같은 source-of-truth를 읽게 만든다.

대상 파일:

- `crates/ratatosk-engine/src/keyspace.rs`
- `crates/ratatosk-engine/src/command/mod.rs`
- `crates/ratatosk-engine/src/command/cmd_client.rs`
- `crates/ratatosk-server/src/client.rs`

성공 기준:

- tracker/target disconnect에서 stale redirect entry가 남지 않는다.
- `GETREDIR`/`TRACKINGINFO`/runtime delivery state가 서로 어긋나지 않는다.

Phase A2. delivery notifier 도입

1. 완료: invalidate queue enqueue와 동시에 target client notifier를 깨우는 경로를 추가했다.
2. 완료: 현재 pubsub/tracking polling loop와 공존시키되, wakeup path를 우선 사용하고 polling은 fallback으로 남겼다.
3. blocked client, subscribed client, redirected tracking target이 notifier를 공유할지 분리할지 결정한다.
4. notifier fan-out이 과도한 락 경쟁을 만들지 않도록 wakeup granularity를 측정한다.

성공 기준:

- redirected target이 invalidation enqueue 직후 다음 polling tick을 기다리지 않고 깨어난다.
- notifier 추가로 disconnect/shutdown path가 꼬이지 않는다.

Phase A3. command semantics 정합화

1. `CLIENT TRACKING ON REDIRECT <id>`에서 target 존재성 검증 시점을 정한다.
2. target이 현재 push 수신이 불가능하면 명시적 에러를 낼지, best-effort queueing을 할지 결정한다.
3. `CLIENT CACHING YES|NO`의 one-shot gating이 redirect target delivery와 충돌하지 않도록 정리한다.
4. `CLIENT UNBLOCK`, reply mode, Pub/Sub subscribed-state와의 상호작용을 최소 계약 수준으로 문서화한다.

성공 기준:

- direct mode와 redirect mode의 invalidation selection 결과가 tracker 설정만으로 설명된다.
- unsupported인 조합은 명시적으로 문서와 에러 경로에 반영된다.

Phase A4. observability와 회귀 테스트

1. engine unit test:
   - direct + redirect
   - `BCAST + PREFIX + REDIRECT`
   - `NOLOOP + REDIRECT`
   - `OPTIN/OPTOUT + REDIRECT`
2. TCP integration test:
   - tracker/target/writer 3-connection 시나리오
   - target disconnect/reconnect
   - blocked target, subscribed target
3. 운영 가시성:
   - `INFO clients` 또는 별도 stats에 tracking redirect delivery counters 추가 검토
   - dropped invalidation, redirected wake count, target missing count 추가 검토

검증 명령:

- `cargo test -p ratatosk-engine client_tracking_`
- `cargo test -p ratatosk-server client_tracking_`
- `cargo test -p ratatosk-server client_registry_reports_blocked_and_tracking_clients`
- `cargo clippy --workspace --all-targets -- -D warnings`

종료 조건:

- redirect target wakeup이 polling-only가 아니고 notifier 기반으로 동작한다.
- tracking 관련 남은 문서 갭이 "Redis full contract 대비 세부 차이" 수준으로 좁혀진다.

Phase A5. redirect lifecycle / reconnect policy 고정

1. redirect binding의 source-of-truth를 `tracker_id -> target_id` 단일 매핑이 아니라 "tracker config + target liveness" 두 층으로 나눈다.
2. 권장 정책:
   - target disconnect 시 모든 tracker의 active redirect binding을 즉시 해제한다.
   - tracker의 tracking 자체는 유지하되 `redirect=-1`로 떨어뜨리고, 이후 invalidation은 direct tracker connection으로만 전달한다.
   - reconnect한 새 client id는 이전 binding을 자동 승계하지 않는다. explicit `CLIENT TRACKING ... REDIRECT <newid>`만 허용한다.
3. `RESET`, `CLIENT ID` 재할당 없음, connection close, target replacement 순서에서 cleanup 순서를 문서와 코드에 동시에 고정한다.
4. target missing, tracker missing, tracker==target, dead target with pending queue를 각각 state transition table로 정리한다.

상태 전이 표(제안 계약):

| 이벤트 | tracker tracking | redirect field | pending invalidation | expected action |
|------|------|------|------|------|
| tracker enables redirect to live target | 유지 | `target_id` | 새 queue 가능 | bind + notifier armed |
| target disconnect | 유지 | `-1` | target queue drop | trackers auto-detach |
| tracker disconnect | 제거 | n/a | tracker-owned entries drop | registry cleanup |
| tracker sends `TRACKING OFF` | 제거 | `-1` | tracker/redirect queue drop | full cleanup |
| target reconnects with new id | 유지 | `-1` | none | explicit rebind required |

성공 기준:

- reconnect 이후 stale target id로 invalidation이 재개되지 않는다.
- `TRACKINGINFO`, `CLIENT LIST`, delivery registry가 같은 detach 결과를 보여준다.

Phase A6. unsupported combination matrix와 에러 계약

1. Redis 문서를 다시 대조해 `REDIRECT`, `BCAST`, `PREFIX`, `NOLOOP`, `OPTIN`, `OPTOUT`, RESP2/RESP3, subscribed-state, blocked-state 조합을 허용/거부/미지원 세 그룹으로 나눈다.
2. "아직 구현하지 않은데 조용히 accept"하는 조합을 없애고, 허용되지 않은 조합은 명시적 `ERR`로 고정한다.
3. 조합별 ownership을 나눈다.
   - parser/validation: `cmd_client.rs`
   - access marking / one-shot gating: `command/mod.rs`
   - registry state invariants: `keyspace.rs`
   - network delivery preconditions: `client.rs`
4. 문서에 behavior matrix를 넣고, test 이름도 matrix row와 일치시키는 규칙을 만든다.

조합 매트릭스 초안(문서화 대상):

| 조합 축 | 허용 여부 | 구현 책임 | 메모 |
|------|------|------|------|
| `REDIRECT + disconnected target` | 거부 | `cmd_client.rs` | 이미 connected-target validation 있음 |
| `REDIRECT + target disconnect after bind` | baseline broken-redirect + fallback | `keyspace.rs` / `client.rs` | unsupported 조합과 exact fallback semantics gap이 남음 |
| `BCAST + PREFIX + REDIRECT` | 허용 | `mod.rs` / `client.rs` | 회귀 테스트 고정 필요 |
| `NOLOOP + REDIRECT` | 허용 | `mod.rs` | loop 기준 명확화 필요 |
| `OPTIN/OPTOUT + REDIRECT` | 조건부 허용 | `cmd_client.rs` / `mod.rs` | gating과 delivery 순서 문서화 필요 |
| `TRACKING + subscribed/blocking target` | baseline/local only | `client.rs` | unsupported 면적을 명시해야 함 |

성공 기준:

- parser acceptance와 실제 delivery behavior가 분리되지 않는다.
- unsupported 조합은 모두 문서, runtime error, 테스트 이름이 같은 표현을 쓴다.

권장 구현 순서:

1. A5 state transition table을 문서와 코드에 먼저 고정
2. disconnect cleanup regression 추가
3. A6 조합별 parser/runtime error normalization
4. 마지막에 observability counter와 `TRACKINGINFO` 노출 정합화

### Workstream B: persistence/runtime 재설계

목표:

- snapshot clone과 single-file rewrite 중심 구현을, current-state materialization과 `ratatosk-persist` 중심 orchestration으로 바꾼다.
- runtime이 persistence policy를 직접 들고 있는 구조를 줄인다.
- 큰 데이터셋에서 `SAVE`/`BGSAVE`/`BGREWRITEAOF`의 pause cost와 memory amplification을 낮춘다.

권장 전략:

- 현재 코드베이스 기준으로는 "single-threaded core + background I/O"를 명시적 선택지로 고정하는 편이 안전하다.
- sharded state 전환은 별도 대형 프로젝트로 분리하고, 이번 재설계는 현재 전역 상태 모델 위에서 persistence 경계와 비용 모델을 먼저 바로잡는 것이 맞다.

Phase B0. 아키텍처 결정 기록

1. `ratatosk-server`는 orchestration만 맡고, 파일 포맷/manifest/rewrite lifecycle은 `ratatosk-persist`가 소유한다는 원칙을 문서화한다.
2. snapshot source는 "global clone"에서 "iterable consistent view"로 옮기는 것을 목표로 명시한다.
3. AOF는 "기존 로그 재기록"이 아니라 "현재 상태 materialization + incremental tail" 모델로 바꾸겠다고 선언한다.

성공 기준:

- 아키텍처 문서가 현재와 목표 경계를 명확히 구분한다.
- 이후 코드 리뷰에서 crate ownership을 기준으로 판단할 수 있다.

Phase B1. persistence 경계 분리

1. 진행 중: `ratatosk-server/src/persistence.rs`의 single-file rewrite algorithm과 filename primitive를 `ratatosk-persist`로 이동했고, manifest serialization/bootstrap/startup recovery baseline도 연결했다. 여기에 manifest candidate validation과 best-effort cleanup을 위한 `aof::switch` helper, manifest-backed rewrite 후 새 incr rotation/manifest commit baseline도 추가됐다. BASE materialization과 full switch transaction은 아직 남아 있다.
2. runtime은 job 시작/취소/상태 조회만 수행하고, 실제 serialization 계획은 persist crate API를 호출하게 바꾼다.
3. background task input/output 타입을 명시적 job spec으로 정의한다.
4. 권장 ownership 분리:
   - `ratatosk-server`: worker spawning, shutdown/drain, config slice 전달
   - `ratatosk-persist::aof::manifest`: manifest load/save/name/layout
   - `ratatosk-persist::aof::rewrite`: rewrite transaction orchestration
   - 신규 제안 `ratatosk-persist::aof::materialize`: current-state exporter
   - 신규 제안 `ratatosk-persist::aof::switch`: atomic manifest swap / rollback rules

대상 파일:

- `crates/ratatosk-server/src/persistence.rs`
- `crates/ratatosk-persist/src/aof/manifest.rs`
- `crates/ratatosk-persist/src/aof/rewrite.rs`
- `crates/ratatosk-persist/src/aof/materialize.rs` (신규 제안)
- `crates/ratatosk-persist/src/aof/switch.rs` (신규 제안)
- `crates/ratatosk-persist/src/*`

성공 기준:

- server crate가 `appendonly.aof` 파일명, rewrite policy, base/incr rotation 규칙을 직접 결정하지 않는다.

Phase B2. snapshot 비용 모델 교체

1. `snapshot_dbs()` 전체 clone 호출 지점을 조사해 read-only serialization view로 대체 가능한 경로부터 옮긴다.
2. DB 단위 또는 key chunk 단위 iterator를 만들고, background writer가 chunk를 순차 소비하게 만든다.
3. 긴 작업 동안 global lock 점유 시간을 측정하고 상한을 문서화한다.
4. 실패 시점 중간 산출물 정리 정책을 넣는다.
5. 권장 인터페이스:
   - engine 쪽: `SnapshotCursor` 또는 `IterableSnapshotView`
   - persist 쪽: `RdbExportPlan`, `AofMaterializationPlan`
   - runtime 쪽: "짧은 lock으로 chunk 확보 -> lock 해제 -> background write" 파이프라인

세부 단계:

1. `snapshot_dbs()` 호출 site inventory를 만든다.
2. RDB saver부터 chunked exporter를 붙이고, 같은 abstraction을 AOF materializer가 재사용하게 만든다.
3. iteration 단위는 "DB 단위 우선, 필요하면 key chunk" 순서로 늘린다.
4. 각 단계마다 `lock_hold_ms`, `snapshot_bytes_estimate`, `rewrite_temp_bytes`를 측정한다.
5. fallback 경로는 당분간 유지하되 feature flag가 아니라 runtime strategy enum으로 분기한다.

성공 기준:

- `BGSAVE` 시작 시점의 clone burst가 사라지거나 크게 줄어든다.
- 최소한 메모리 증폭이 "dataset full clone"보다 낮아졌음을 benchmark로 설명할 수 있다.

Phase B3. AOF rewrite를 current-state materialization으로 전환

1. 현재 DB 상태를 순회해 최소 command set 또는 base AOF snapshot을 쓰는 writer를 만든다.
2. rewrite 중 들어오는 신규 write는 incremental tail로 분리 기록한다.
3. 완료 시 manifest swap을 atomic하게 처리한다.
4. recovery는 base + incr 조합을 읽도록 바꾼다.
5. 권장 산출물:
   - `base.aof` 또는 `appendonly.aof.<seq>.base.aof`: current-state materialized snapshot
   - `appendonly.aof.<seq>.incr.aof`: rewrite 중 tail append를 받는 active file
   - rewrite completion record: manifest switch에 필요한 metadata 묶음

세부 단계:

1. "현재 상태 -> 최소 명령 집합" 규칙을 타입별로 정의한다.
   - string/hash/list/set/zset/stream
   - expiry 포함 여부와 DB 경계 표현
2. rewrite 시작 시 active incr tail을 rotate해서 "rewrite-sealed old incr"와 "new live incr"를 분리한다.
3. materializer는 sealed state view만 읽고, live write는 새 incr로만 흐르게 한다.
4. rewrite 결과물과 새 incr를 묶어 manifest candidate를 만든 뒤, switch 단계로 넘긴다.
5. recovery integration test는 base-only, base+single-incr, base+multi-incr, stale base + fresh incr 네 가지를 고정한다.

성공 기준:

- rewrite 결과물이 기존 command history 재기록이 아니라 현재 상태를 반영한다.
- log compaction 효과가 문서상/실측상 둘 다 설명 가능하다.

Phase B4. multipart AOF manifest 연동

1. 완료: `AofManifest`를 runtime bootstrap 경로와 startup recovery 경로에 실제 연결했다.
2. 진행 중: manifest discovery, base/incr 파일 검증, missing file handling, 손상 복구 규칙을 정리한다.
3. 진행 중: `BGREWRITEAOF` 완료 후 active manifest 전환 절차를 정리한다. 현재는 `ratatosk-persist::aof::switch`를 통해 manifest-backed rewrite 후 새 incr rotation + manifest commit baseline이 runtime에 연결됐다. 다만 BASE file 도입과 temp manifest 기반 full atomic switch 절차는 아직 없다.
4. 운영 문서에 파일 레이아웃, 장애 복구 절차를 추가한다.
5. atomic switch transaction을 별도 설계 항목으로 분리한다.

atomic switch 절차(제안 계약):

1. 새 base/incr 파일을 temp 이름으로 write + fsync
2. manifest candidate를 temp manifest로 atomic write
3. temp file 존재, size, header, replay sanity를 검증
4. candidate manifest rename
5. directory fsync
6. old manifest / old files cleanup는 best-effort 후행 작업으로 분리

crash matrix(문서에 반드시 유지할 것):

| crash point | startup expected behavior | cleanup rule |
|------|------|------|
| before candidate manifest write | old manifest 사용 | temp files 제거 |
| after candidate manifest write, before rename | old manifest 사용 | temp manifest 무시 |
| after manifest rename, before old file cleanup | new manifest 사용 | orphan old files 재정리 |
| after new base write, before new incr open | startup fail 금지 | old manifest fallback 또는 candidate 검증 실패 처리 |

성공 기준:

- startup recovery에서 `appendonly.aof` 단일 경로 가정이 사라진다.
- manifest 기반 recovery가 integration test로 고정된다.
- manifest switch가 crash-safe 절차와 test matrix로 설명된다.

Phase B5. 운영/관측/테스트 정비

1. `INFO persistence`에 실제 current size, base size, rewrite progress, last manifest switch 결과를 반영한다.
2. background job status를 runtime metric/log에 남긴다.
3. soak test:
   - large dataset `BGSAVE`
   - rewrite 중 write flood
   - crash/restart/recovery
4. 회귀 test:
   - RDB/AOF recovery matrix
   - partial file / stale manifest / failed swap
   - orphan temp base/incr
   - manifest points to missing sealed incr
   - switch success 후 old file cleanup 실패

릴리스 게이트(세분화):

1. correctness:
   - restart 후 데이터셋이 base + incr 조합과 일관되게 복구된다.
   - failed switch가 startup hard-fail이나 silent data loss를 만들지 않는다.
2. architecture:
   - `ratatosk-server`가 AOF filename/rotation 규칙을 직접 소유하지 않는다.
   - current-state exporter와 manifest switch 로직이 `ratatosk-persist`에 모인다.
3. observability:
   - `INFO persistence`와 log에서 active manifest, current incr, last switch status를 볼 수 있다.
4. performance:
   - rewrite 시작 순간의 lock hold / memory burst가 기존 clone 모델보다 줄거나 최소한 측정 가능하다.

검증 명령:

- `cargo test -p ratatosk-persist`
- `cargo test -p ratatosk-server persistence`
- `cargo clippy --workspace --all-targets -- -D warnings`

종료 조건:

- persistence policy가 `ratatosk-persist` 중심으로 정리된다.
- `SAVE`/`BGSAVE`/`BGREWRITEAOF` 비용 모델이 문서와 실제 동작에서 Redis 방향으로 한 단계 가까워진다.

실행 순서 권고:

1. Workstream A에서는 먼저 reconnect/detach policy를 고정하고 unsupported 조합 acceptance를 끊는다.
2. Workstream B에서는 `materialize` / `switch` ownership을 `ratatosk-persist`에 먼저 세운다.
3. 그 다음 B2 snapshot view, B3 rewrite materialization, B4 atomic switch 순으로 간다.
4. A와 B 모두 "state transition table + crash/cleanup matrix + integration test row"를 같은 문서 형식으로 유지한다.
5. 두 workstream이 끝난 뒤에만 `CLIENT UNBLOCK`, Pub/Sub wakeup generalization, replication `WAIT` blocking semantics로 확장한다.

## Bottom Line

지금 Ratatosk의 가장 큰 문제는 "구현된 명령 수"가 아니라 "Redis가 제공하는 시스템 계약이 실제로 존재하는가"다. 현재 상태를 가장 정확하게 표현하면:

- standalone data server: 상당히 진척
- Redis command vocabulary: 넓음
- Redis operational/distributed semantics: 아직 큰 갭

따라서 다음 단계의 핵심은 새 명령을 더 채우는 것이 아니라, 이미 표면상 존재하는 명령들 중 replication, cluster, Sentinel, scripting, blocking, client tracking처럼 시스템 성격이 강한 영역을 실제 subsystem으로 승격하는 일이다.
