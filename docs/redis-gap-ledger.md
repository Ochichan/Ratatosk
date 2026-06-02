# Redis Gap Ledger

Redis 명령 카탈로그 대비 Ratatosk 구현 상태 추적표.
원본은 `docs/redis-gap-ledger.json`, 이 문서는 `scripts/redis_gap_ledger.py`로 생성된다.

상태(`status`)와 동작 등급(`capability_tier`)은 다르다.

- `status`: 구현 추적 상태 (`planned`, `partial`, `done` 등)
- `capability_tier`: Redis 의미론 대비 수준 (`unsupported`, `syntax_only`, `baseline_local`, `behavioral_subset`, `distributed_parity`)

## Summary

| Metric | Value |
| --- | ---: |
| Total commands | 420 |
| planned | 0 |
| in_progress | 0 |
| partial | 0 |
| done | 420 |
| excluded | 0 |

## Capability Tier Summary

| Tier | Value |
| --- | ---: |
| unsupported | 63 |
| syntax_only | 6 |
| baseline_local | 76 |
| behavioral_subset | 275 |
| distributed_parity | 0 |

## Group Progress

| Group | done | partial | in_progress | planned | excluded | total |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| bitmap | 7 | 0 | 0 | 0 | 0 | 7 |
| cluster | 35 | 0 | 0 | 0 | 0 | 35 |
| connection | 26 | 0 | 0 | 0 | 0 | 26 |
| generic | 34 | 0 | 0 | 0 | 0 | 34 |
| geo | 10 | 0 | 0 | 0 | 0 | 10 |
| hash | 28 | 0 | 0 | 0 | 0 | 28 |
| hyperloglog | 5 | 0 | 0 | 0 | 0 | 5 |
| list | 22 | 0 | 0 | 0 | 0 | 22 |
| pubsub | 15 | 0 | 0 | 0 | 0 | 15 |
| scripting | 23 | 0 | 0 | 0 | 0 | 23 |
| sentinel | 22 | 0 | 0 | 0 | 0 | 22 |
| server | 83 | 0 | 0 | 0 | 0 | 83 |
| set | 17 | 0 | 0 | 0 | 0 | 17 |
| sorted_set | 35 | 0 | 0 | 0 | 0 | 35 |
| stream | 28 | 0 | 0 | 0 | 0 | 28 |
| string | 25 | 0 | 0 | 0 | 0 | 25 |
| transactions | 5 | 0 | 0 | 0 | 0 | 5 |

## Group Capability Tiers

| Group | distributed_parity | behavioral_subset | baseline_local | syntax_only | unsupported | total |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| bitmap | 0 | 7 | 0 | 0 | 0 | 7 |
| cluster | 0 | 0 | 10 | 3 | 22 | 35 |
| connection | 0 | 11 | 12 | 1 | 2 | 26 |
| generic | 0 | 32 | 2 | 0 | 0 | 34 |
| geo | 0 | 10 | 0 | 0 | 0 | 10 |
| hash | 0 | 28 | 0 | 0 | 0 | 28 |
| hyperloglog | 0 | 5 | 0 | 0 | 0 | 5 |
| list | 0 | 22 | 0 | 0 | 0 | 22 |
| pubsub | 0 | 15 | 0 | 0 | 0 | 15 |
| scripting | 0 | 1 | 11 | 0 | 11 | 23 |
| sentinel | 0 | 0 | 0 | 1 | 21 | 22 |
| server | 0 | 34 | 41 | 1 | 7 | 83 |
| set | 0 | 17 | 0 | 0 | 0 | 17 |
| sorted_set | 0 | 35 | 0 | 0 | 0 | 35 |
| stream | 0 | 28 | 0 | 0 | 0 | 28 |
| string | 0 | 25 | 0 | 0 | 0 | 25 |
| transactions | 0 | 5 | 0 | 0 | 0 | 5 |

## Command Ledger

| Command | Group | Since | Status | Tier | Milestone | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| `ACL` | server | 6.0.0 | done | behavioral_subset | m0-foundation | M0 ACL baseline implemented with central subcommand dispatch and stateful user/log management. |
| `ACL CAT` | server | 6.0.0 | done | behavioral_subset | m0-foundation | M0 ACL baseline implemented (category and category-filter list responses). |
| `ACL DELUSER` | server | 6.0.0 | done | behavioral_subset | m0-foundation | M0 ACL baseline implemented (multi-user delete, default user protected). |
| `ACL DRYRUN` | server | 7.0.0 | done | baseline_local | m0-foundation | M0 ACL baseline implemented (syntax/arity validation with standalone OK simulation). |
| `ACL GENPASS` | server | 6.0.0 | done | behavioral_subset | m0-foundation | M0 ACL baseline implemented (bit-length parsing and deterministic hex password generation). |
| `ACL GETUSER` | server | 6.0.0 | done | behavioral_subset | m0-foundation | M0 ACL baseline implemented (flags/passwords/commands/keys/channels/selectors map response). |
| `ACL HELP` | server | 6.0.0 | done | behavioral_subset | m0-foundation | M0 ACL baseline implemented (help text). |
| `ACL LIST` | server | 6.0.0 | done | behavioral_subset | m0-foundation | M0 ACL baseline implemented (user rule lines). |
| `ACL LOAD` | server | 6.0.0 | done | baseline_local | m0-foundation | M0 ACL baseline implemented (standalone no-op OK). |
| `ACL LOG` | server | 6.0.0 | done | behavioral_subset | m0-foundation | M0 ACL baseline implemented (count retrieval + RESET). |
| `ACL SAVE` | server | 6.0.0 | done | baseline_local | m0-foundation | M0 ACL baseline implemented (standalone no-op OK). |
| `ACL SETUSER` | server | 6.0.0 | done | behavioral_subset | m0-foundation | M0 ACL baseline implemented (on/off/nopass/resetpass/password and rule-token parsing). |
| `ACL USERS` | server | 6.0.0 | done | behavioral_subset | m0-foundation | M0 ACL baseline implemented (sorted user list). |
| `ACL WHOAMI` | server | 6.0.0 | done | behavioral_subset | m0-foundation | M0 ACL baseline implemented (current authenticated ACL user). |
| `APPEND` | string | 2.0.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `ASKING` | cluster | 3.0.0 | done | syntax_only | m5-advanced |  |
| `AUTH` | connection | 1.0.0 | done | behavioral_subset | m0-foundation | M0 compatibility baseline implemented (accepts AUTH <username> <password>). |
| `BGREWRITEAOF` | server | 1.0.0 | done | behavioral_subset | m3-persistence | Async AOF worker rewrite path wired (start/in-progress gate/shutdown drain). |
| `BGSAVE` | server | 1.0.0 | done | behavioral_subset | m3-persistence | M3 baseline implemented (non-blocking acknowledgement path with timestamp update). |
| `BITCOUNT` | bitmap | 2.6.0 | done | behavioral_subset | m4-extended-types |  |
| `BITFIELD` | bitmap | 3.2.0 | done | behavioral_subset | m4-extended-types |  |
| `BITFIELD_RO` | bitmap | 6.0.0 | done | behavioral_subset | m4-extended-types |  |
| `BITOP` | bitmap | 2.6.0 | done | behavioral_subset | m4-extended-types |  |
| `BITPOS` | bitmap | 2.8.7 | done | behavioral_subset | m4-extended-types |  |
| `BLMOVE` | list | 6.2.0 | done | behavioral_subset | m2-collections | M2 blocking semantics implemented (blocked wait registry + producer wakeup with timeout fallback; nil on timeout). |
| `BLMPOP` | list | 7.0.0 | done | behavioral_subset | m2-collections | M2 blocking semantics implemented (blocked wait registry + producer wakeup with timeout fallback; nil on timeout). |
| `BLPOP` | list | 2.0.0 | done | behavioral_subset | m2-collections | M2 blocking semantics implemented (blocked wait registry + producer wakeup with timeout fallback; nil on timeout). |
| `BRPOP` | list | 2.0.0 | done | behavioral_subset | m2-collections | M2 blocking semantics implemented (blocked wait registry + producer wakeup with timeout fallback; nil on timeout). |
| `BRPOPLPUSH` | list | 2.2.0 | done | behavioral_subset | m2-collections | M2 blocking semantics implemented (blocked wait registry + producer wakeup with timeout fallback; nil on timeout). |
| `BZMPOP` | sorted_set | 7.0.0 | done | behavioral_subset | m2-collections | M2 blocking semantics implemented (blocked wait registry + producer wakeup with timeout fallback). |
| `BZPOPMAX` | sorted_set | 5.0.0 | done | behavioral_subset | m2-collections | M2 blocking semantics implemented (blocked wait registry + producer wakeup with timeout fallback). |
| `BZPOPMIN` | sorted_set | 5.0.0 | done | behavioral_subset | m2-collections | M2 blocking semantics implemented (blocked wait registry + producer wakeup with timeout fallback). |
| `CLIENT` | connection | 2.4.0 | done | baseline_local | m0-foundation | M0 compatibility baseline implemented with HELP/ID/GETNAME/SETNAME/INFO/LIST. |
| `CLIENT CACHING` | connection | 6.0.0 | done | baseline_local | m0-foundation | M0 client-tracking baseline implemented: YES/NO parsing and per-client state toggle. |
| `CLIENT GETNAME` | connection | 2.6.9 | done | behavioral_subset | m0-foundation | M0 compatibility baseline implemented. |
| `CLIENT GETREDIR` | connection | 6.0.0 | done | baseline_local | m0-foundation | M0 client-tracking baseline implemented: returns configured tracking redirect id (`0` for self-redirection while enabled, `-1` when tracking is off) and cooperates with `broken_redirect` tracking state. |
| `CLIENT HELP` | connection | 5.0.0 | done | behavioral_subset | m0-foundation | M0 compatibility baseline implemented. |
| `CLIENT ID` | connection | 5.0.0 | done | behavioral_subset | m0-foundation | M0 compatibility baseline implemented. |
| `CLIENT INFO` | connection | 6.2.0 | done | baseline_local | m0-foundation | M0 compatibility baseline implemented (single-connection info string). |
| `CLIENT KILL` | connection | 2.4.0 | done | baseline_local | m0-foundation | M0 client-admin baseline implemented: legacy addr form + ID-filter parsing with deterministic kill count. |
| `CLIENT LIST` | connection | 2.4.0 | done | baseline_local | m0-foundation | M0 compatibility baseline implemented (single-connection list with TYPE/ID filtering baseline). |
| `CLIENT NO-EVICT` | connection | 7.0.0 | done | baseline_local | m0-foundation | M0 client baseline implemented: ON/OFF parsing and local state toggle. |
| `CLIENT NO-TOUCH` | connection | 7.2.0 | done | baseline_local | m0-foundation | M0 client baseline implemented: ON/OFF parsing and local state toggle. |
| `CLIENT PAUSE` | connection | 3.0.0 | done | unsupported | m0-foundation | M0 client-admin baseline implemented: timeout/mode parsing with no-op pause semantics. |
| `CLIENT REPLY` | connection | 3.2.0 | done | baseline_local | m0-foundation | M0 client baseline implemented: ON/OFF/SKIP parsing and local reply mode state. |
| `CLIENT SETINFO` | connection | 7.2.0 | done | baseline_local | m0-foundation | M0 client baseline implemented: LIB-NAME/LIB-VER metadata accepted (no-op). |
| `CLIENT SETNAME` | connection | 2.6.9 | done | behavioral_subset | m0-foundation | M0 compatibility baseline implemented. |
| `CLIENT TRACKING` | connection | 6.0.0 | done | baseline_local | m0-foundation | M0 client-tracking baseline implemented with direct-key invalidation, BCAST/PREFIX/NOLOOP registry, OPTIN/OPTOUT next-command gating, async invalidate push, connected-target REDIRECT validation, target wakeup delivery, `broken_redirect` marking, and RESP3 `tracking-redir-broken` push. |
| `CLIENT TRACKINGINFO` | connection | 6.2.0 | done | baseline_local | m0-foundation | M0 client-tracking baseline implemented: flags/redirect/prefix state plus current direct-key/BCAST tracking metadata surface, configured redirect visibility, and `broken_redirect` state reporting. |
| `CLIENT UNBLOCK` | connection | 5.0.0 | done | syntax_only | m0-foundation | M0 client-admin baseline implemented: ID/mode parsing with deterministic no-op unblock result. |
| `CLIENT UNPAUSE` | connection | 6.2.0 | done | unsupported | m0-foundation | M0 client-admin baseline implemented: explicit unpause no-op semantics. |
| `CLUSTER` | cluster | 3.0.0 | done | baseline_local | m5-advanced |  |
| `CLUSTER ADDSLOTS` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER ADDSLOTSRANGE` | cluster | 7.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER BUMPEPOCH` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER COUNT-FAILURE-REPORTS` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER COUNTKEYSINSLOT` | cluster | 3.0.0 | done | baseline_local | m5-advanced |  |
| `CLUSTER DELSLOTS` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER DELSLOTSRANGE` | cluster | 7.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER FAILOVER` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER FLUSHSLOTS` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER FORGET` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER GETKEYSINSLOT` | cluster | 3.0.0 | done | baseline_local | m5-advanced |  |
| `CLUSTER HELP` | cluster | 5.0.0 | done | baseline_local | m5-advanced |  |
| `CLUSTER INFO` | cluster | 3.0.0 | done | baseline_local | m5-advanced |  |
| `CLUSTER KEYSLOT` | cluster | 3.0.0 | done | baseline_local | m5-advanced |  |
| `CLUSTER LINKS` | cluster | 7.0.0 | done | baseline_local | m5-advanced |  |
| `CLUSTER MEET` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER MIGRATION` | cluster | 8.4.0 | done | unsupported | m5-advanced |  |
| `CLUSTER MYID` | cluster | 3.0.0 | done | baseline_local | m5-advanced |  |
| `CLUSTER MYSHARDID` | cluster | 7.2.0 | done | unsupported | m5-advanced |  |
| `CLUSTER NODES` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER REPLICAS` | cluster | 5.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER REPLICATE` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER RESET` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER SAVECONFIG` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER SET-CONFIG-EPOCH` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER SETSLOT` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER SHARDS` | cluster | 7.0.0 | done | baseline_local | m5-advanced |  |
| `CLUSTER SLAVES` | cluster | 3.0.0 | done | unsupported | m5-advanced |  |
| `CLUSTER SLOT-STATS` | cluster | 8.2.0 | done | unsupported | m5-advanced |  |
| `CLUSTER SLOTS` | cluster | 3.0.0 | done | baseline_local | m5-advanced |  |
| `CLUSTER SYNCSLOTS` | cluster | 8.4.0 | done | unsupported | m5-advanced |  |
| `COMMAND` | server | 2.8.13 | done | behavioral_subset | m0-foundation | M0 baseline implemented with COUNT/LIST/INFO/HELP and root reply. |
| `COMMAND COUNT` | server | 2.8.13 | done | behavioral_subset | m0-foundation | M0 baseline implemented. |
| `COMMAND DOCS` | server | 7.0.0 | done | behavioral_subset | m0-foundation | M0 command-metadata baseline implemented (returns per-command docs map with summary/arity/flags). |
| `COMMAND GETKEYS` | server | 2.8.13 | done | behavioral_subset | m0-foundation | M0 compatibility baseline implemented. |
| `COMMAND GETKEYSANDFLAGS` | server | 7.0.0 | done | behavioral_subset | m0-foundation | M0 compatibility baseline implemented. |
| `COMMAND HELP` | server | 5.0.0 | done | behavioral_subset | m0-foundation | M0 baseline implemented. |
| `COMMAND INFO` | server | 2.8.13 | done | behavioral_subset | m0-foundation | M0 baseline implemented. |
| `COMMAND LIST` | server | 7.0.0 | done | behavioral_subset | m0-foundation | M0 baseline implemented. |
| `CONFIG` | server | 2.0.0 | done | baseline_local | m0-foundation | M0 operational baseline implemented (GET/SET/HELP/RESETSTAT subset). |
| `CONFIG GET` | server | 2.0.0 | done | baseline_local | m0-foundation | M0 operational baseline implemented (glob pattern matching over core params). |
| `CONFIG HELP` | server | 5.0.0 | done | baseline_local | m0-foundation | M0 operational baseline implemented. |
| `CONFIG RESETSTAT` | server | 2.0.0 | done | baseline_local | m0-foundation | M0 operational baseline implemented. |
| `CONFIG REWRITE` | server | 2.8.0 | done | baseline_local | m0-foundation | M0 operational baseline implemented (in-memory acknowledge path). |
| `CONFIG SET` | server | 2.0.0 | done | baseline_local | m0-foundation | M0 operational baseline implemented (timeout/appendonly/save/slowlog params). |
| `COPY` | generic | 6.2.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented (DB/REPLACE options). |
| `DBSIZE` | server | 1.0.0 | done | behavioral_subset | m0-foundation | M0 compatibility baseline implemented. |
| `DEBUG` | server | 1.0.0 | done | unsupported | m0-foundation | M0 admin baseline implemented (HELP + unsupported subcommand response). |
| `DECR` | string | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `DECRBY` | string | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `DEL` | generic | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `DELEX` | string | 8.4.0 | done | behavioral_subset | m1-kv-core | M1 baseline implemented: DELEX key + IFEQ/IFNE/IFDEQ/IFDNE conditions for string values. |
| `DIGEST` | string | 8.4.0 | done | behavioral_subset | m1-kv-core | M1 baseline implemented: DIGEST returns deterministic signed integer hash string for string values. |
| `DISCARD` | transactions | 2.0.0 | done | behavioral_subset | m1-kv-core | M1 transaction baseline implemented. |
| `DUMP` | generic | 2.6.0 | done | behavioral_subset | m1-kv-core | M1 baseline implemented: RATSK1 internal payload serialization for string/hash/list/set. |
| `ECHO` | connection | 1.0.0 | done | behavioral_subset | m0-foundation | M0 implemented and tested. |
| `EVAL` | scripting | 2.6.0 | done | unsupported | m5-advanced |  |
| `EVALSHA` | scripting | 2.6.0 | done | unsupported | m5-advanced |  |
| `EVALSHA_RO` | scripting | 7.0.0 | done | unsupported | m5-advanced |  |
| `EVAL_RO` | scripting | 7.0.0 | done | unsupported | m5-advanced |  |
| `EXEC` | transactions | 1.2.0 | done | behavioral_subset | m1-kv-core | M1 transaction baseline implemented with WATCH conflict abort (null reply) and EXECABORT on queue-time errors. |
| `EXISTS` | generic | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `EXPIRE` | generic | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented (EXPIRE [NX\|XX\|GT\|LT]). |
| `EXPIREAT` | generic | 1.2.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `EXPIRETIME` | generic | 7.0.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `FAILOVER` | server | 6.2.0 | done | unsupported | m0-foundation | M0 standalone baseline implemented (returns unsupported-in-standalone error). |
| `FCALL` | scripting | 7.0.0 | done | unsupported | m5-advanced |  |
| `FCALL_RO` | scripting | 7.0.0 | done | unsupported | m5-advanced |  |
| `FLUSHALL` | server | 1.0.0 | done | behavioral_subset | m0-foundation | M0 compatibility baseline implemented (SYNC/ASYNC accepted). |
| `FLUSHDB` | server | 1.0.0 | done | behavioral_subset | m0-foundation | M0 compatibility baseline implemented. |
| `FUNCTION` | scripting | 7.0.0 | done | baseline_local | m5-advanced |  |
| `FUNCTION DELETE` | scripting | 7.0.0 | done | unsupported | m5-advanced |  |
| `FUNCTION DUMP` | scripting | 7.0.0 | done | baseline_local | m5-advanced |  |
| `FUNCTION FLUSH` | scripting | 7.0.0 | done | baseline_local | m5-advanced |  |
| `FUNCTION HELP` | scripting | 7.0.0 | done | baseline_local | m5-advanced |  |
| `FUNCTION KILL` | scripting | 7.0.0 | done | behavioral_subset | m5-advanced |  |
| `FUNCTION LIST` | scripting | 7.0.0 | done | baseline_local | m5-advanced |  |
| `FUNCTION LOAD` | scripting | 7.0.0 | done | unsupported | m5-advanced |  |
| `FUNCTION RESTORE` | scripting | 7.0.0 | done | unsupported | m5-advanced |  |
| `FUNCTION STATS` | scripting | 7.0.0 | done | baseline_local | m5-advanced |  |
| `GEOADD` | geo | 3.2.0 | done | behavioral_subset | m4-extended-types |  |
| `GEODIST` | geo | 3.2.0 | done | behavioral_subset | m4-extended-types |  |
| `GEOHASH` | geo | 3.2.0 | done | behavioral_subset | m4-extended-types |  |
| `GEOPOS` | geo | 3.2.0 | done | behavioral_subset | m4-extended-types |  |
| `GEORADIUS` | geo | 3.2.0 | done | behavioral_subset | m4-extended-types |  |
| `GEORADIUSBYMEMBER` | geo | 3.2.0 | done | behavioral_subset | m4-extended-types |  |
| `GEORADIUSBYMEMBER_RO` | geo | 3.2.10 | done | behavioral_subset | m4-extended-types |  |
| `GEORADIUS_RO` | geo | 3.2.10 | done | behavioral_subset | m4-extended-types |  |
| `GEOSEARCH` | geo | 6.2.0 | done | behavioral_subset | m4-extended-types |  |
| `GEOSEARCHSTORE` | geo | 6.2.0 | done | behavioral_subset | m4-extended-types |  |
| `GET` | string | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `GETBIT` | bitmap | 2.2.0 | done | behavioral_subset | m4-extended-types |  |
| `GETDEL` | string | 6.2.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `GETEX` | string | 6.2.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `GETRANGE` | string | 2.4.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `GETSET` | string | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `HDEL` | hash | 2.0.0 | done | behavioral_subset | m2-collections | M2 hash core baseline implemented. |
| `HELLO` | connection | 6.0.0 | done | behavioral_subset | m0-foundation | M0 parity baseline implemented (proto negotiation + AUTH/SETNAME syntax + NOPROTO handling). |
| `HEXISTS` | hash | 2.0.0 | done | behavioral_subset | m2-collections | M2 hash core baseline implemented. |
| `HEXPIRE` | hash | 7.4.0 | done | behavioral_subset | m2-collections |  |
| `HEXPIREAT` | hash | 7.4.0 | done | behavioral_subset | m2-collections |  |
| `HEXPIRETIME` | hash | 7.4.0 | done | behavioral_subset | m2-collections |  |
| `HGET` | hash | 2.0.0 | done | behavioral_subset | m2-collections | M2 hash core baseline implemented. |
| `HGETALL` | hash | 2.0.0 | done | behavioral_subset | m2-collections | M2 hash core baseline implemented. |
| `HGETDEL` | hash | 8.0.0 | done | behavioral_subset | m2-collections |  |
| `HGETEX` | hash | 8.0.0 | done | behavioral_subset | m2-collections |  |
| `HINCRBY` | hash | 2.0.0 | done | behavioral_subset | m2-collections | Batch-7 hash extended baseline implemented. |
| `HINCRBYFLOAT` | hash | 2.6.0 | done | behavioral_subset | m2-collections | Batch-7 hash extended baseline implemented. |
| `HKEYS` | hash | 2.0.0 | done | behavioral_subset | m2-collections | Batch-5 baseline implemented (ordered 1->2 execution). |
| `HLEN` | hash | 2.0.0 | done | behavioral_subset | m2-collections | M2 hash core baseline implemented. |
| `HMGET` | hash | 2.0.0 | done | behavioral_subset | m2-collections | M2 hash core baseline implemented. |
| `HMSET` | hash | 2.0.0 | done | behavioral_subset | m2-collections | Batch-7 hash extended baseline implemented. |
| `HOTKEYS` | server | 8.6.0 | done | baseline_local | m0-foundation | M0 admin baseline implemented (GET/RESET/START/STOP/HELP container). |
| `HOTKEYS GET` | server | 8.6.0 | done | baseline_local | m0-foundation | M0 admin baseline implemented (returns empty sample list). |
| `HOTKEYS RESET` | server | 8.6.0 | done | baseline_local | m0-foundation | M0 admin baseline implemented (no-op OK). |
| `HOTKEYS START` | server | 8.6.0 | done | baseline_local | m0-foundation | M0 admin baseline implemented (no-op OK). |
| `HOTKEYS STOP` | server | 8.6.0 | done | baseline_local | m0-foundation | M0 admin baseline implemented (no-op OK). |
| `HPERSIST` | hash | 7.4.0 | done | behavioral_subset | m2-collections |  |
| `HPEXPIRE` | hash | 7.4.0 | done | behavioral_subset | m2-collections |  |
| `HPEXPIREAT` | hash | 7.4.0 | done | behavioral_subset | m2-collections |  |
| `HPEXPIRETIME` | hash | 7.4.0 | done | behavioral_subset | m2-collections |  |
| `HPTTL` | hash | 7.4.0 | done | behavioral_subset | m2-collections |  |
| `HRANDFIELD` | hash | 6.2.0 | done | behavioral_subset | m2-collections | Batch-7 hash extended baseline implemented. |
| `HSCAN` | hash | 2.8.0 | done | behavioral_subset | m2-collections | M2 scan family baseline + cursor progression semantics refined. |
| `HSET` | hash | 2.0.0 | done | behavioral_subset | m2-collections | M2 hash core baseline implemented. |
| `HSETEX` | hash | 8.0.0 | done | behavioral_subset | m2-collections |  |
| `HSETNX` | hash | 2.0.0 | done | behavioral_subset | m2-collections | Batch-7 hash extended baseline implemented. |
| `HSTRLEN` | hash | 3.2.0 | done | behavioral_subset | m2-collections | Batch-7 hash extended baseline implemented. |
| `HTTL` | hash | 7.4.0 | done | behavioral_subset | m2-collections |  |
| `HVALS` | hash | 2.0.0 | done | behavioral_subset | m2-collections | Batch-5 baseline implemented (ordered 1->2 execution). |
| `INCR` | string | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `INCRBY` | string | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `INCRBYFLOAT` | string | 2.6.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `INFO` | server | 1.0.0 | done | baseline_local | m0-foundation | M0 compatibility baseline implemented for SERVER/CLIENTS/STATS/KEYSPACE sections. |
| `KEYS` | generic | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `LASTSAVE` | server | 1.0.0 | done | behavioral_subset | m0-foundation | M0 compatibility baseline implemented. |
| `LATENCY` | server | 2.8.13 | done | baseline_local | m0-foundation | M0 latency baseline implemented with in-memory sample tracking and LATENCY subcommand dispatch. |
| `LATENCY DOCTOR` | server | 2.8.13 | done | baseline_local | m0-foundation | M0 latency baseline implemented (human-readable diagnostics). |
| `LATENCY GRAPH` | server | 2.8.13 | done | baseline_local | m0-foundation | M0 latency baseline implemented (event summary graph text). |
| `LATENCY HELP` | server | 2.8.13 | done | baseline_local | m0-foundation | M0 latency baseline implemented (HELP text). |
| `LATENCY HISTOGRAM` | server | 7.0.0 | done | baseline_local | m0-foundation | M0 latency baseline implemented (coarse latency bucket output). |
| `LATENCY HISTORY` | server | 2.8.13 | done | baseline_local | m0-foundation | M0 latency baseline implemented (timestamp/latency sample rows). |
| `LATENCY LATEST` | server | 2.8.13 | done | baseline_local | m0-foundation | M0 latency baseline implemented (event/latest/max tuple rows). |
| `LATENCY RESET` | server | 2.8.13 | done | baseline_local | m0-foundation | M0 latency baseline implemented (event/all reset with removed count). |
| `LCS` | string | 7.0.0 | done | behavioral_subset | m1-kv-core | M1 baseline implemented: LCS key1 key2 + LEN option for string values. |
| `LINDEX` | list | 1.0.0 | done | behavioral_subset | m2-collections |  |
| `LINSERT` | list | 2.2.0 | done | behavioral_subset | m2-collections |  |
| `LLEN` | list | 1.0.0 | done | behavioral_subset | m2-collections | M2 list core baseline implemented. |
| `LMOVE` | list | 6.2.0 | done | behavioral_subset | m2-collections | M2 set/list extension baseline implemented. |
| `LMPOP` | list | 7.0.0 | done | behavioral_subset | m2-collections | M2 list extension baseline implemented. |
| `LOLWUT` | server | 5.0.0 | done | baseline_local | m0-foundation | M0 informational baseline implemented (static ascii-text response with VERSION option). |
| `LPOP` | list | 1.0.0 | done | behavioral_subset | m2-collections | M2 list core baseline implemented. |
| `LPOS` | list | 6.0.6 | done | behavioral_subset | m2-collections | M2 set/list extension baseline implemented. |
| `LPUSH` | list | 1.0.0 | done | behavioral_subset | m2-collections | M2 list core baseline implemented. |
| `LPUSHX` | list | 2.2.0 | done | behavioral_subset | m2-collections | M2 set/list extension baseline implemented. |
| `LRANGE` | list | 1.0.0 | done | behavioral_subset | m2-collections | M2 list core baseline implemented. |
| `LREM` | list | 1.0.0 | done | behavioral_subset | m2-collections | M2 set/list extension baseline implemented. |
| `LSET` | list | 1.0.0 | done | behavioral_subset | m2-collections | Batch-5 baseline implemented (ordered 1->2 execution). |
| `LTRIM` | list | 1.0.0 | done | behavioral_subset | m2-collections | Batch-5 baseline implemented (ordered 1->2 execution). |
| `MEMORY` | server | 4.0.0 | done | baseline_local | m0-foundation | M0 operational baseline implemented (USAGE/HELP subset). |
| `MEMORY DOCTOR` | server | 4.0.0 | done | baseline_local | m0-foundation | M0 operational baseline implemented. |
| `MEMORY HELP` | server | 4.0.0 | done | baseline_local | m0-foundation | M0 operational baseline implemented. |
| `MEMORY MALLOC-STATS` | server | 4.0.0 | done | baseline_local | m0-foundation | M0 operational baseline implemented. |
| `MEMORY PURGE` | server | 4.0.0 | done | baseline_local | m0-foundation | M0 operational baseline implemented. |
| `MEMORY STATS` | server | 4.0.0 | done | baseline_local | m0-foundation | M0 operational baseline implemented. |
| `MEMORY USAGE` | server | 4.0.0 | done | baseline_local | m0-foundation | M0 operational baseline implemented (SAMPLES syntax accepted). |
| `MGET` | string | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `MIGRATE` | generic | 2.6.0 | done | behavioral_subset | m1-kv-core | M1 baseline implemented: standalone-compatible MIGRATE returns NOKEY (no remote transfer). |
| `MODULE` | server | 4.0.0 | done | baseline_local | m0-foundation | M0 module baseline implemented (HELP/LIST supported, load/unload return unsupported). |
| `MODULE HELP` | server | 5.0.0 | done | baseline_local | m0-foundation | M0 module baseline implemented (help text). |
| `MODULE LIST` | server | 4.0.0 | done | baseline_local | m0-foundation | M0 module baseline implemented (returns empty list). |
| `MODULE LOAD` | server | 4.0.0 | done | unsupported | m0-foundation | M0 module baseline implemented (unsupported in this build). |
| `MODULE LOADEX` | server | 7.0.0 | done | unsupported | m0-foundation | M0 module baseline implemented (unsupported in this build). |
| `MODULE UNLOAD` | server | 4.0.0 | done | unsupported | m0-foundation | M0 module baseline implemented (unsupported in this build). |
| `MONITOR` | server | 1.0.0 | done | baseline_local | m0-foundation | M0 baseline implemented: MONITOR command accepted with standalone OK response. |
| `MOVE` | generic | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `MSET` | string | 1.0.1 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `MSETEX` | string | 8.4.0 | done | behavioral_subset | m1-kv-core | M1 baseline implemented: numkeys KV block + NX/XX + EX/PX shared expiration, atomic all-or-nothing. |
| `MSETNX` | string | 1.0.1 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `MULTI` | transactions | 1.2.0 | done | behavioral_subset | m1-kv-core | M1 transaction baseline implemented. |
| `OBJECT` | generic | 2.2.3 | done | behavioral_subset | m1-kv-core | M1 generic/object baseline implemented (HELP and key introspection subcommands). |
| `OBJECT ENCODING` | generic | 2.2.3 | done | behavioral_subset | m1-kv-core | M1 generic/object baseline implemented. |
| `OBJECT FREQ` | generic | 4.0.0 | done | behavioral_subset | m1-kv-core | M1 generic/object baseline implemented. |
| `OBJECT HELP` | generic | 6.2.0 | done | behavioral_subset | m1-kv-core | M1 generic/object baseline implemented. |
| `OBJECT IDLETIME` | generic | 2.2.3 | done | behavioral_subset | m1-kv-core | M1 generic/object baseline implemented. |
| `OBJECT REFCOUNT` | generic | 2.2.3 | done | behavioral_subset | m1-kv-core | M1 generic/object baseline implemented. |
| `PERSIST` | generic | 2.2.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `PEXPIRE` | generic | 2.6.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `PEXPIREAT` | generic | 2.6.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `PEXPIRETIME` | generic | 7.0.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `PFADD` | hyperloglog | 2.8.9 | done | behavioral_subset | m4-extended-types |  |
| `PFCOUNT` | hyperloglog | 2.8.9 | done | behavioral_subset | m4-extended-types |  |
| `PFDEBUG` | hyperloglog | 2.8.9 | done | behavioral_subset | m4-extended-types |  |
| `PFMERGE` | hyperloglog | 2.8.9 | done | behavioral_subset | m4-extended-types |  |
| `PFSELFTEST` | hyperloglog | 2.8.9 | done | behavioral_subset | m4-extended-types |  |
| `PING` | connection | 1.0.0 | done | behavioral_subset | m0-foundation | M0 implemented and tested. |
| `PSETEX` | string | 2.6.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `PSUBSCRIBE` | pubsub | 2.0.0 | done | behavioral_subset | m3-events | M3 events baseline implemented with pattern fanout and async push delivery. |
| `PSYNC` | server | 2.8.0 | done | baseline_local | m0-foundation | M0 replication-control baseline implemented with stateful FULLRESYNC handshake over the current replid/offset. |
| `PTTL` | generic | 2.6.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `PUBLISH` | pubsub | 2.0.0 | done | behavioral_subset | m3-events | M3 events baseline implemented with receiver counting and server-side fanout queue. |
| `PUBSUB` | pubsub | 2.8.0 | done | behavioral_subset | m3-events | M3 pubsub baseline implemented with CHANNELS/NUMSUB/NUMPAT/HELP dispatch. |
| `PUBSUB CHANNELS` | pubsub | 2.8.0 | done | behavioral_subset | m3-events | M3 pubsub baseline implemented (optional glob filter). |
| `PUBSUB HELP` | pubsub | 6.2.0 | done | behavioral_subset | m3-events | M3 pubsub baseline implemented (help text). |
| `PUBSUB NUMPAT` | pubsub | 2.8.0 | done | behavioral_subset | m3-events | M3 pubsub baseline implemented (unique pattern count). |
| `PUBSUB NUMSUB` | pubsub | 2.8.0 | done | behavioral_subset | m3-events | M3 pubsub baseline implemented (channel subscriber count pairs). |
| `PUBSUB SHARDCHANNELS` | pubsub | 7.0.0 | done | behavioral_subset | m3-events | M3 sharded pubsub baseline implemented (active shard channel listing with optional pattern). |
| `PUBSUB SHARDNUMSUB` | pubsub | 7.0.0 | done | behavioral_subset | m3-events | M3 sharded pubsub baseline implemented (per-channel shard subscriber counts). |
| `PUNSUBSCRIBE` | pubsub | 2.0.0 | done | behavioral_subset | m3-events | M3 pubsub baseline implemented: pattern unsubscribe and no-arg unsubscribe-all behavior. |
| `QUIT` | connection | 1.0.0 | done | behavioral_subset | m0-foundation | M0 implemented and tested. |
| `RANDOMKEY` | generic | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `READONLY` | cluster | 3.0.0 | done | syntax_only | m5-advanced |  |
| `READWRITE` | cluster | 3.0.0 | done | syntax_only | m5-advanced |  |
| `RENAME` | generic | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `RENAMENX` | generic | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `REPLCONF` | server | 3.0.0 | done | baseline_local | m0-foundation | M0 replication-control baseline implemented with LISTENING-PORT/CAPA/ACK/GETACK/IP-ADDRESS subset backed by per-client replica metadata. |
| `REPLICAOF` | server | 5.0.0 | done | baseline_local | m0-foundation | M0 replication-control baseline implemented with standalone role transition between master and configured upstream replica. |
| `RESET` | connection | 6.2.0 | done | behavioral_subset | m0-foundation | M0 compatibility baseline implemented. |
| `RESTORE` | generic | 2.6.0 | done | behavioral_subset | m1-kv-core | M1 baseline implemented: RESTORE payload import with REPLACE/ABSTTL support and BUSYKEY handling. |
| `RESTORE-ASKING` | server | 3.0.0 | done | behavioral_subset | m0-foundation | M0 replication-control baseline implemented as RESTORE alias behavior. |
| `ROLE` | server | 2.8.12 | done | baseline_local | m0-foundation | M0 replication-control baseline implemented with master/replica role reporting, replica list, and logical replication offsets. |
| `RPOP` | list | 1.0.0 | done | behavioral_subset | m2-collections | M2 list core baseline implemented. |
| `RPOPLPUSH` | list | 1.2.0 | done | behavioral_subset | m2-collections | M2 list extension baseline implemented. |
| `RPUSH` | list | 1.0.0 | done | behavioral_subset | m2-collections | M2 list core baseline implemented. |
| `RPUSHX` | list | 2.2.0 | done | behavioral_subset | m2-collections | M2 set/list extension baseline implemented. |
| `SADD` | set | 1.0.0 | done | behavioral_subset | m2-collections | M2 set core baseline implemented. |
| `SAVE` | server | 1.0.0 | done | behavioral_subset | m0-foundation | M0 compatibility baseline implemented (in-memory no-op save + LASTSAVE timestamp update). |
| `SCAN` | generic | 2.8.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline + m2 scan semantics refined (cursor progression with MATCH/TYPE/COUNT). |
| `SCARD` | set | 1.0.0 | done | behavioral_subset | m2-collections | M2 set core baseline implemented. |
| `SCRIPT` | scripting | 2.6.0 | done | baseline_local | m5-advanced |  |
| `SCRIPT DEBUG` | scripting | 3.2.0 | done | unsupported | m5-advanced |  |
| `SCRIPT EXISTS` | scripting | 2.6.0 | done | baseline_local | m5-advanced |  |
| `SCRIPT FLUSH` | scripting | 2.6.0 | done | baseline_local | m5-advanced |  |
| `SCRIPT HELP` | scripting | 5.0.0 | done | baseline_local | m5-advanced |  |
| `SCRIPT KILL` | scripting | 2.6.0 | done | unsupported | m5-advanced |  |
| `SCRIPT LOAD` | scripting | 2.6.0 | done | baseline_local | m5-advanced |  |
| `SDIFF` | set | 1.0.0 | done | behavioral_subset | m2-collections | M2 set algebra baseline implemented. |
| `SDIFFSTORE` | set | 1.0.0 | done | behavioral_subset | m2-collections | M2 set algebra baseline implemented. |
| `SELECT` | connection | 1.0.0 | done | behavioral_subset | m0-foundation | M1 baseline implemented for client DB switching. |
| `SENTINEL` | sentinel | 2.8.4 | done | unsupported | m5-advanced |  |
| `SENTINEL CKQUORUM` | sentinel | 2.8.4 | done | unsupported | m5-advanced |  |
| `SENTINEL CONFIG` | sentinel | 6.2.0 | done | unsupported | m5-advanced |  |
| `SENTINEL DEBUG` | sentinel | 7.0.0 | done | unsupported | m5-advanced |  |
| `SENTINEL FAILOVER` | sentinel | 2.8.4 | done | unsupported | m5-advanced |  |
| `SENTINEL FLUSHCONFIG` | sentinel | 2.8.4 | done | unsupported | m5-advanced |  |
| `SENTINEL GET-MASTER-ADDR-BY-NAME` | sentinel | 2.8.4 | done | unsupported | m5-advanced |  |
| `SENTINEL HELP` | sentinel | 6.2.0 | done | syntax_only | m5-advanced |  |
| `SENTINEL INFO-CACHE` | sentinel | 3.2.0 | done | unsupported | m5-advanced |  |
| `SENTINEL IS-MASTER-DOWN-BY-ADDR` | sentinel | 2.8.4 | done | unsupported | m5-advanced |  |
| `SENTINEL MASTER` | sentinel | 2.8.4 | done | unsupported | m5-advanced |  |
| `SENTINEL MASTERS` | sentinel | 2.8.4 | done | unsupported | m5-advanced |  |
| `SENTINEL MONITOR` | sentinel | 2.8.4 | done | unsupported | m5-advanced |  |
| `SENTINEL MYID` | sentinel | 6.2.0 | done | unsupported | m5-advanced |  |
| `SENTINEL PENDING-SCRIPTS` | sentinel | 2.8.4 | done | unsupported | m5-advanced |  |
| `SENTINEL REMOVE` | sentinel | 2.8.4 | done | unsupported | m5-advanced |  |
| `SENTINEL REPLICAS` | sentinel | 5.0.0 | done | unsupported | m5-advanced |  |
| `SENTINEL RESET` | sentinel | 2.8.4 | done | unsupported | m5-advanced |  |
| `SENTINEL SENTINELS` | sentinel | 2.8.4 | done | unsupported | m5-advanced |  |
| `SENTINEL SET` | sentinel | 2.8.4 | done | unsupported | m5-advanced |  |
| `SENTINEL SIMULATE-FAILURE` | sentinel | 3.2.0 | done | unsupported | m5-advanced |  |
| `SENTINEL SLAVES` | sentinel | 2.8.0 | done | unsupported | m5-advanced |  |
| `SET` | string | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented (SET [NX\|XX]). |
| `SETBIT` | bitmap | 2.2.0 | done | behavioral_subset | m4-extended-types |  |
| `SETEX` | string | 2.0.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `SETNX` | string | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `SETRANGE` | string | 2.2.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `SFLUSH` | server | 8.0.0 | done | syntax_only | m0-foundation | M0 admin baseline implemented (SYNC/ASYNC option parse + OK no-op). |
| `SHUTDOWN` | server | 1.0.0 | done | unsupported | m0-foundation | M0 standalone baseline implemented (returns unsupported-in-build error). |
| `SINTER` | set | 1.0.0 | done | behavioral_subset | m2-collections | M2 set algebra baseline implemented. |
| `SINTERCARD` | set | 7.0.0 | done | behavioral_subset | m2-collections | M2 set algebra baseline implemented (numkeys/LIMIT parser + cardinality-only path). |
| `SINTERSTORE` | set | 1.0.0 | done | behavioral_subset | m2-collections | M2 set algebra baseline implemented. |
| `SISMEMBER` | set | 1.0.0 | done | behavioral_subset | m2-collections | M2 set core baseline implemented. |
| `SLAVEOF` | server | 1.0.0 | done | baseline_local | m0-foundation | M0 replication-control baseline implemented as REPLICAOF alias. |
| `SLOWLOG` | server | 2.2.12 | done | behavioral_subset | m0-foundation | M0 operational baseline implemented (GET/LEN/RESET/HELP subset). |
| `SLOWLOG GET` | server | 2.2.12 | done | behavioral_subset | m0-foundation | M0 operational baseline implemented. |
| `SLOWLOG HELP` | server | 6.2.0 | done | behavioral_subset | m0-foundation | M0 operational baseline implemented. |
| `SLOWLOG LEN` | server | 2.2.12 | done | behavioral_subset | m0-foundation | M0 operational baseline implemented. |
| `SLOWLOG RESET` | server | 2.2.12 | done | behavioral_subset | m0-foundation | M0 operational baseline implemented. |
| `SMEMBERS` | set | 1.0.0 | done | behavioral_subset | m2-collections | M2 set core baseline implemented. |
| `SMISMEMBER` | set | 6.2.0 | done | behavioral_subset | m2-collections | M2 set/list extension baseline implemented. |
| `SMOVE` | set | 1.0.0 | done | behavioral_subset | m2-collections | Batch-5 baseline implemented (ordered 1->2 execution). |
| `SORT` | generic | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented (list/set sorting with ASC/DESC, ALPHA, LIMIT, STORE). |
| `SORT_RO` | generic | 7.0.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented (list/set sorting with ASC/DESC, ALPHA, LIMIT). |
| `SPOP` | set | 1.0.0 | done | behavioral_subset | m2-collections | M2 set/list extension baseline implemented. |
| `SPUBLISH` | pubsub | 7.0.0 | done | behavioral_subset | m3-events | M3 sharded pubsub baseline implemented with shard-channel fanout and receiver counting. |
| `SRANDMEMBER` | set | 1.0.0 | done | behavioral_subset | m2-collections | M2 set/list extension baseline implemented. |
| `SREM` | set | 1.0.0 | done | behavioral_subset | m2-collections | M2 set core baseline implemented. |
| `SSCAN` | set | 2.8.0 | done | behavioral_subset | m2-collections | M2 scan family baseline + cursor progression semantics refined. |
| `SSUBSCRIBE` | pubsub | 7.0.0 | done | behavioral_subset | m3-events | M3 sharded pubsub baseline implemented with per-client shard subscriptions and RESP subscribe ack. |
| `STRLEN` | string | 2.2.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented. |
| `SUBSCRIBE` | pubsub | 2.0.0 | done | behavioral_subset | m3-events | M3 events baseline implemented with cross-client fanout and async push delivery. |
| `SUBSTR` | string | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 string baseline implemented as GETRANGE alias. |
| `SUNION` | set | 1.0.0 | done | behavioral_subset | m2-collections | M2 set algebra baseline implemented. |
| `SUNIONSTORE` | set | 1.0.0 | done | behavioral_subset | m2-collections | M2 set algebra baseline implemented. |
| `SUNSUBSCRIBE` | pubsub | 7.0.0 | done | behavioral_subset | m3-events | M3 sharded pubsub baseline implemented with explicit/all unsubscribe behavior and RESP unsubscribe ack. |
| `SWAPDB` | server | 4.0.0 | done | behavioral_subset | m0-foundation | M0 admin baseline implemented (DB payload swap with range checks). |
| `SYNC` | server | 1.0.0 | done | unsupported | m0-foundation | M0 replication-control baseline implemented (standalone unsupported error baseline). |
| `TIME` | server | 2.6.0 | done | behavioral_subset | m0-foundation | M0 compatibility baseline implemented. |
| `TOUCH` | generic | 3.2.1 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `TRIMSLOTS` | server | 8.4.0 | done | baseline_local | m0-foundation | M0 admin baseline implemented (integer argument parse + no-op 0 result). |
| `TTL` | generic | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `TYPE` | generic | 1.0.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented. |
| `UNLINK` | generic | 4.0.0 | done | behavioral_subset | m1-kv-core | M1 generic baseline implemented (synchronous fallback). |
| `UNSUBSCRIBE` | pubsub | 2.0.0 | done | behavioral_subset | m3-events | M3 pubsub baseline implemented: unsubscribe channel/all with remaining-subscription counts. |
| `UNWATCH` | transactions | 2.2.0 | done | behavioral_subset | m1-kv-core | M1 transaction baseline implemented. |
| `WAIT` | generic | 3.0.0 | done | baseline_local | m1-kv-core | M1 baseline implemented: immediate standalone WAIT result from tracked replica ACK offsets without blocking timeout semantics. |
| `WAITAOF` | generic | 7.2.0 | done | baseline_local | m1-kv-core | M1 baseline implemented: immediate standalone WAITAOF vector from local AOF health and tracked replica ACK offsets. |
| `WATCH` | transactions | 2.2.0 | done | behavioral_subset | m1-kv-core | M1 transaction baseline implemented with key-version tracking. |
| `XACK` | stream | 5.0.0 | done | behavioral_subset | m3-events | M3 stream group baseline implemented. |
| `XACKDEL` | stream | 8.2.0 | done | behavioral_subset | m3-events | Batch-6 stream extended deletion/config baseline implemented. |
| `XADD` | stream | 5.0.0 | done | behavioral_subset | m3-events | M3 stream core baseline implemented (auto-ID and explicit ID checks, field/value append). |
| `XAUTOCLAIM` | stream | 6.2.0 | done | behavioral_subset | m3-events | Batch-5 baseline implemented (ordered 1->2 execution). |
| `XCFGSET` | stream | 8.6.0 | done | behavioral_subset | m3-events | Batch-6 stream extended deletion/config baseline implemented. |
| `XCLAIM` | stream | 5.0.0 | done | behavioral_subset | m3-events | Batch-5 baseline implemented (ordered 1->2 execution). |
| `XDEL` | stream | 5.0.0 | done | behavioral_subset | m3-events | Batch-5 baseline implemented (ordered 1->2 execution). |
| `XDELEX` | stream | 8.2.0 | done | behavioral_subset | m3-events | Batch-6 stream extended deletion/config baseline implemented. |
| `XGROUP` | stream | 5.0.0 | done | behavioral_subset | m3-events | M3 stream group baseline implemented (CREATE/DESTROY/SETID/CREATECONSUMER/DELCONSUMER/HELP dispatch). |
| `XGROUP CREATE` | stream | 5.0.0 | done | behavioral_subset | m3-events | M3 stream group baseline implemented (MKSTREAM + BUSYGROUP + id/$ support). |
| `XGROUP CREATECONSUMER` | stream | 6.2.0 | done | behavioral_subset | m3-events | M3 stream group baseline implemented. |
| `XGROUP DELCONSUMER` | stream | 5.0.0 | done | behavioral_subset | m3-events | M3 stream group baseline implemented (consumer pending removal count). |
| `XGROUP DESTROY` | stream | 5.0.0 | done | behavioral_subset | m3-events | M3 stream group baseline implemented. |
| `XGROUP HELP` | stream | 5.0.0 | done | behavioral_subset | m3-events | M3 stream group baseline implemented. |
| `XGROUP SETID` | stream | 5.0.0 | done | behavioral_subset | m3-events | M3 stream group baseline implemented. |
| `XINFO` | stream | 5.0.0 | done | behavioral_subset | m3-events | Batch-5 baseline implemented (ordered 1->2 execution). |
| `XINFO CONSUMERS` | stream | 5.0.0 | done | behavioral_subset | m3-events | Batch-5 baseline implemented (ordered 1->2 execution). |
| `XINFO GROUPS` | stream | 5.0.0 | done | behavioral_subset | m3-events | Batch-5 baseline implemented (ordered 1->2 execution). |
| `XINFO HELP` | stream | 5.0.0 | done | behavioral_subset | m3-events | Batch-5 baseline implemented (ordered 1->2 execution). |
| `XINFO STREAM` | stream | 5.0.0 | done | behavioral_subset | m3-events | Batch-5 baseline implemented (ordered 1->2 execution). |
| `XLEN` | stream | 5.0.0 | done | behavioral_subset | m3-events | M3 stream core baseline implemented. |
| `XPENDING` | stream | 5.0.0 | done | behavioral_subset | m3-events | M3 stream group baseline implemented (summary + range forms). |
| `XRANGE` | stream | 5.0.0 | done | behavioral_subset | m3-events | M3 stream core baseline implemented (inclusive range + COUNT option). |
| `XREAD` | stream | 5.0.0 | done | behavioral_subset | m3-events | M3 stream core baseline implemented (COUNT/BLOCK parse + STREAMS read, blocked wait registry + producer wakeup with timeout fallback). |
| `XREADGROUP` | stream | 5.0.0 | done | behavioral_subset | m3-events | M3 stream group baseline implemented (GROUP/COUNT/BLOCK/NOACK parse + STREAMS read path with blocked wait registry + producer wakeup fallback). |
| `XREVRANGE` | stream | 5.0.0 | done | behavioral_subset | m3-events | M3 stream core baseline implemented (reverse inclusive range + COUNT option). |
| `XSETID` | stream | 5.0.0 | done | behavioral_subset | m3-events | Batch-5 baseline implemented (ordered 1->2 execution). |
| `XTRIM` | stream | 5.0.0 | done | behavioral_subset | m3-events | Batch-5 baseline implemented (ordered 1->2 execution). |
| `ZADD` | sorted_set | 1.2.0 | done | behavioral_subset | m2-collections |  |
| `ZCARD` | sorted_set | 1.2.0 | done | behavioral_subset | m2-collections |  |
| `ZCOUNT` | sorted_set | 2.0.0 | done | behavioral_subset | m2-collections |  |
| `ZDIFF` | sorted_set | 6.2.0 | done | behavioral_subset | m2-collections |  |
| `ZDIFFSTORE` | sorted_set | 6.2.0 | done | behavioral_subset | m2-collections |  |
| `ZINCRBY` | sorted_set | 1.2.0 | done | behavioral_subset | m2-collections |  |
| `ZINTER` | sorted_set | 6.2.0 | done | behavioral_subset | m2-collections |  |
| `ZINTERCARD` | sorted_set | 7.0.0 | done | behavioral_subset | m2-collections |  |
| `ZINTERSTORE` | sorted_set | 2.0.0 | done | behavioral_subset | m2-collections |  |
| `ZLEXCOUNT` | sorted_set | 2.8.9 | done | behavioral_subset | m2-collections |  |
| `ZMPOP` | sorted_set | 7.0.0 | done | behavioral_subset | m2-collections |  |
| `ZMSCORE` | sorted_set | 6.2.0 | done | behavioral_subset | m2-collections |  |
| `ZPOPMAX` | sorted_set | 5.0.0 | done | behavioral_subset | m2-collections |  |
| `ZPOPMIN` | sorted_set | 5.0.0 | done | behavioral_subset | m2-collections |  |
| `ZRANDMEMBER` | sorted_set | 6.2.0 | done | behavioral_subset | m2-collections |  |
| `ZRANGE` | sorted_set | 1.2.0 | done | behavioral_subset | m2-collections |  |
| `ZRANGEBYLEX` | sorted_set | 2.8.9 | done | behavioral_subset | m2-collections |  |
| `ZRANGEBYSCORE` | sorted_set | 1.0.5 | done | behavioral_subset | m2-collections |  |
| `ZRANGESTORE` | sorted_set | 6.2.0 | done | behavioral_subset | m2-collections |  |
| `ZRANK` | sorted_set | 2.0.0 | done | behavioral_subset | m2-collections |  |
| `ZREM` | sorted_set | 1.2.0 | done | behavioral_subset | m2-collections |  |
| `ZREMRANGEBYLEX` | sorted_set | 2.8.9 | done | behavioral_subset | m2-collections |  |
| `ZREMRANGEBYRANK` | sorted_set | 2.0.0 | done | behavioral_subset | m2-collections |  |
| `ZREMRANGEBYSCORE` | sorted_set | 1.2.0 | done | behavioral_subset | m2-collections |  |
| `ZREVRANGE` | sorted_set | 1.2.0 | done | behavioral_subset | m2-collections |  |
| `ZREVRANGEBYLEX` | sorted_set | 2.8.9 | done | behavioral_subset | m2-collections |  |
| `ZREVRANGEBYSCORE` | sorted_set | 2.2.0 | done | behavioral_subset | m2-collections |  |
| `ZREVRANK` | sorted_set | 2.0.0 | done | behavioral_subset | m2-collections |  |
| `ZSCAN` | sorted_set | 2.8.0 | done | behavioral_subset | m2-collections |  |
| `ZSCORE` | sorted_set | 1.2.0 | done | behavioral_subset | m2-collections |  |
| `ZUNION` | sorted_set | 6.2.0 | done | behavioral_subset | m2-collections |  |
| `ZUNIONSTORE` | sorted_set | 2.0.0 | done | behavioral_subset | m2-collections |  |
