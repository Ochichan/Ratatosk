use bytes::Bytes;

use hashbrown::{HashMap, HashSet};
use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{
    ServerState, StoredValue, StreamConsumer, StreamEntry, StreamGroup, StreamId,
    StreamPendingEntry, purge_expired_key,
};

use super::cmd_stream::{
    AddTrimArgs, IntervalEdge, TrimStrategy, invalid_stream_id, parse_add_or_trim_args,
    parse_interval_id, parse_strict_stream_id, stream_entry_frame, stream_id_to_bytes,
    stream_nogroup_error,
};
use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, parse_usize, wrong_arity,
    wrong_type_response,
};

fn stream_contains_id(stream: &[StreamEntry], id: StreamId) -> bool {
    stream.binary_search_by_key(&id, |entry| entry.id).is_ok()
}

/// Drops removed IDs from every group's pending lists. For each group the
/// cheaper side is walked: the removed IDs (hash lookups) when they are fewer
/// than the group's pending entries, otherwise the pending IDs (binary
/// searches in the sorted removed IDs).
pub(super) fn prune_stream_removed_ids(entry: &mut StoredValue, removed_ids: &[StreamId]) {
    if removed_ids.is_empty() {
        return;
    }
    let Some(groups) = entry.as_stream_groups_mut() else {
        return;
    };
    let sorted;
    let removed = if removed_ids.is_sorted() {
        removed_ids
    } else {
        let mut copy = removed_ids.to_vec();
        copy.sort_unstable();
        sorted = copy;
        &sorted
    };

    for group in groups.values_mut() {
        if group.pending.is_empty() {
            continue;
        }
        let doomed = if removed.len() <= group.pending.len() {
            removed
                .iter()
                .copied()
                .filter(|id| group.pending.contains_key(id))
                .collect::<Vec<_>>()
        } else {
            group
                .pending
                .keys()
                .copied()
                .filter(|id| removed.binary_search(id).is_ok())
                .collect::<Vec<_>>()
        };
        for id in doomed {
            if let Some(pending) = group.pending.remove(&id) {
                if let Some(consumer) = group.consumers.get_mut(&pending.consumer) {
                    consumer.pending.remove(&id);
                }
            }
        }
    }
}

/// Drops `id` from the group's pending list and its owner's.
fn remove_pending_id(group: &mut StreamGroup, id: StreamId) {
    if let Some(pending) = group.pending.remove(&id) {
        if let Some(consumer) = group.consumers.get_mut(&pending.consumer) {
            consumer.pending.remove(&id);
        }
    }
}

/// Makes `consumer_name` the owner of `id`, as Redis's XCLAIM and XAUTOCLAIM
/// do: the delivery time is `delivery_ms`, and the delivery count is
/// `retry_count` when given, else one more than `base` unless `justid`.
#[derive(Clone, Copy)]
struct ClaimTerms {
    delivery_ms: i64,
    retry_count: Option<i64>,
    justid: bool,
    base: i64,
}

fn assign_pending_id(
    group: &mut StreamGroup,
    consumer_name: &Bytes,
    id: StreamId,
    terms: &ClaimTerms,
) {
    let ClaimTerms {
        delivery_ms,
        retry_count,
        justid,
        base,
    } = *terms;
    if let Some(previous) = group
        .pending
        .get(&id)
        .map(|pending| pending.consumer.clone())
    {
        if previous != *consumer_name {
            if let Some(previous) = group.consumers.get_mut(&previous) {
                previous.pending.remove(&id);
            }
        }
    }
    let deliveries = match retry_count {
        Some(count) => count,
        None if justid => base,
        None => base.saturating_add(1),
    };
    group.pending.insert(
        id,
        StreamPendingEntry {
            consumer: consumer_name.clone(),
            deliveries,
            last_delivered_ms: delivery_ms,
        },
    );
    group
        .consumers
        .entry(consumer_name.clone())
        .or_insert_with(|| StreamConsumer {
            seen_time_ms: delivery_ms,
            pending: HashSet::new(),
        })
        .pending
        .insert(id);
}

/// The consumer exists after XCLAIM and XAUTOCLAIM even when nothing is
/// claimed, and its seen time moves to now.
fn touch_consumer(group: &mut StreamGroup, consumer_name: &Bytes, now: i64) {
    group
        .consumers
        .entry(consumer_name.clone())
        .or_insert_with(|| StreamConsumer {
            seen_time_ms: now,
            pending: HashSet::new(),
        })
        .seen_time_ms = now;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StreamDeleteCondition {
    KeepRef,
    DelRef,
    Acked,
}

pub(super) fn parse_stream_delete_condition(raw: &Bytes) -> Option<StreamDeleteCondition> {
    if raw.eq_ignore_ascii_case(b"KEEPREF") {
        Some(StreamDeleteCondition::KeepRef)
    } else if raw.eq_ignore_ascii_case(b"DELREF") {
        Some(StreamDeleteCondition::DelRef)
    } else if raw.eq_ignore_ascii_case(b"ACKED") {
        Some(StreamDeleteCondition::Acked)
    } else {
        None
    }
}

pub(super) fn parse_stream_ids_block(
    args: &[Bytes],
    idx: usize,
) -> Result<Vec<StreamId>, RespFrame> {
    if idx >= args.len() || !args[idx].eq_ignore_ascii_case(b"IDS") {
        return Err(err("ERR syntax error"));
    }

    let Some(num_ids_raw) = args.get(idx + 1) else {
        return Err(err("ERR syntax error"));
    };
    let Some(num_ids) = parse_usize(num_ids_raw) else {
        return Err(err("ERR value is not an integer or out of range"));
    };

    let start = idx.saturating_add(2);
    let end = start.saturating_add(num_ids);
    if end != args.len() {
        return Err(err("ERR syntax error"));
    }

    let mut ids = Vec::with_capacity(num_ids);
    for raw_id in &args[start..end] {
        let Some(id) = parse_strict_stream_id(raw_id) else {
            return Err(err(
                "ERR Invalid stream ID specified as stream command argument",
            ));
        };
        ids.push(id);
    }

    Ok(ids)
}

pub(super) fn stream_has_pending_references(
    groups: &HashMap<Bytes, StreamGroup>,
    id: StreamId,
) -> bool {
    groups.values().any(|group| group.pending.contains_key(&id))
}

pub(super) fn purge_stream_pending_id(groups: &mut HashMap<Bytes, StreamGroup>, id: StreamId) {
    for group in groups.values_mut() {
        group.pending.remove(&id);
        for consumer in group.consumers.values_mut() {
            consumer.pending.remove(&id);
        }
    }
}

/// Entries per radix node in Redis (`stream-node-max-entries` default).
const APPROX_TRIM_NODE_ENTRIES: usize = 100;

/// What a `~` trim without `LIMIT` may remove at most, Redis's default of 100
/// nodes.
const APPROX_TRIM_DEFAULT_LIMIT: usize = 100 * APPROX_TRIM_NODE_ENTRIES;

/// How many entries a `~` trim removes, modelling Redis's `streamTrim` on
/// nodes of [`APPROX_TRIM_NODE_ENTRIES`] entries counted from the front (the
/// last node may be partial, and is the whole stream when it is short). Whole
/// leading nodes go while removing one keeps the stream at or above MAXLEN, or
/// for MINID while the node's last ID is below it, and the trim stops before a
/// node that would take the total past `limit` (`None` is no cap).
fn approx_trim_count(
    entries: &[StreamEntry],
    strategy: TrimStrategy,
    limit: Option<usize>,
) -> usize {
    let length = entries.len();
    let mut removed = 0usize;
    while removed < length {
        let node = APPROX_TRIM_NODE_ENTRIES.min(length - removed);
        let eligible = match strategy {
            TrimStrategy::MaxLen(max_len) => {
                let max_len = usize::try_from(max_len).unwrap_or(usize::MAX);
                if length - removed <= max_len {
                    break;
                }
                length - removed - node >= max_len
            }
            TrimStrategy::MinId(min_id) => entries[removed + node - 1].id < min_id,
        };
        if limit.is_some_and(|cap| removed + node > cap) || !eligible {
            break;
        }
        removed += node;
    }
    removed
}

/// Trims the stream at `entry` as `options` ask and returns how many entries
/// went.
///
/// An exact trim removes everything past the threshold, up to `LIMIT` when
/// one is given (`LIMIT 0` is no cap). A `~` trim removes whole nodes as
/// [`approx_trim_count`] describes, with `LIMIT` defaulting to
/// [`APPROX_TRIM_DEFAULT_LIMIT`]. The logged form of the command is the
/// exact result, so replay stays deterministic. Under AOF replay `~` is exact
/// and `LIMIT` is the plain cap older versions applied. Like Redis, trimming
/// leaves `max_deleted_id` alone. Group pending lists drop the removed IDs.
pub(super) fn apply_trim(entry: &mut StoredValue, options: &AddTrimArgs) -> usize {
    let Some(strategy) = options.strategy else {
        return 0;
    };
    let Some(stream) = entry.as_stream_entries_mut() else {
        return 0;
    };
    let replay = super::cmd_stream::replay_mode();

    let cap = if replay {
        options.limit
    } else {
        match options.limit {
            Some(0) => None,
            Some(cap) => Some(cap),
            None if options.approx => Some(APPROX_TRIM_DEFAULT_LIMIT),
            None => None,
        }
    };
    let mut remove_count = if options.approx && !replay {
        approx_trim_count(stream, strategy, cap)
    } else {
        let mut count = match strategy {
            TrimStrategy::MaxLen(max_len) => stream
                .len()
                .saturating_sub(usize::try_from(max_len).unwrap_or(usize::MAX)),
            TrimStrategy::MinId(min_id) => stream.partition_point(|item| item.id < min_id),
        };
        if let Some(cap) = cap {
            count = count.min(cap);
        }
        count
    };
    remove_count = remove_count.min(stream.len());
    if remove_count == 0 {
        return 0;
    }

    let removed_ids = stream.remove_front(remove_count);
    prune_stream_removed_ids(entry, &removed_ids);
    removed_ids.len()
}

pub(super) fn cmd_xtrim(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 3 {
        return wrong_arity("xtrim");
    }

    let key = &args[0];
    let options = match parse_add_or_trim_args(args, false) {
        Ok(options) => options,
        Err(reply) => return CommandOutcome::reply(reply),
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    if !entry.is_stream() {
        return wrong_type_response();
    }

    let removed = apply_trim(entry, &options);
    CommandOutcome::reply(RespFrame::Integer(removed as i64))
}

pub(super) fn cmd_xdel(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, ids @ ..] = args else {
        return wrong_arity("xdel");
    };
    if ids.is_empty() {
        return wrong_arity("xdel");
    }

    let mut id_set = HashSet::new();
    for raw_id in ids {
        let Some(id) = parse_strict_stream_id(raw_id) else {
            return CommandOutcome::reply(err(
                "ERR Invalid stream ID specified as stream command argument",
            ));
        };
        id_set.insert(id);
    }

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(stream) = entry.as_stream_entries_mut() else {
        return wrong_type_response();
    };

    let mut removed_ids = Vec::new();
    stream.retain(|item| {
        if id_set.remove(&item.id) {
            removed_ids.push(item.id);
            false
        } else {
            true
        }
    });

    prune_stream_removed_ids(entry, &removed_ids);
    record_deleted_ids(entry, removed_ids.iter().copied());
    CommandOutcome::reply(RespFrame::Integer(removed_ids.len() as i64))
}

/// Raise the stream's max deleted ID to cover `deleted`, as Redis does for
/// XDEL, XDELEX and XACKDEL. XTRIM leaves it unchanged.
fn record_deleted_ids(entry: &mut StoredValue, deleted: impl IntoIterator<Item = StreamId>) {
    if let Some(meta) = entry.as_stream_meta_mut() {
        if let Some(max) = deleted.into_iter().max() {
            meta.max_deleted_id = meta.max_deleted_id.max(max);
        }
    }
}

/// Looks up the stream and group like Redis's xclaimCommand and
/// xautoclaimCommand do, before anything else is judged.
fn require_group(
    server: &mut ServerState,
    client: &ClientState,
    key: &Bytes,
    group_name: &Bytes,
    now: i64,
) -> Option<CommandOutcome> {
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);
    let Some(entry) = db.get(key) else {
        return Some(stream_nogroup_error(key, group_name));
    };
    if !entry.is_stream() {
        return Some(wrong_type_response());
    }
    if !entry
        .as_stream_groups()
        .is_some_and(|groups| groups.contains_key(group_name))
    {
        return Some(stream_nogroup_error(key, group_name));
    }
    None
}

fn entry_exists(entries: &[StreamEntry], id: StreamId) -> bool {
    entries.binary_search_by_key(&id, |entry| entry.id).is_ok()
}

fn claimed_reply(entries: &[StreamEntry], id: StreamId, justid: bool) -> Option<RespFrame> {
    if justid {
        return Some(RespFrame::BulkString(Some(stream_id_to_bytes(id))));
    }
    entries
        .binary_search_by_key(&id, |entry| entry.id)
        .ok()
        .map(|at| stream_entry_frame(&entries[at]))
}

/// Whether an XCLAIM replayed from the AOF is a record an earlier version
/// logged as sent. Those versions had none of the options below, and this
/// version logs only commands that carry one, so a record without them keeps
/// the semantics it was written with.
fn is_earlier_xclaim_record(args: &[Bytes]) -> bool {
    super::cmd_stream::replay_mode()
        && !args[4..].iter().any(|arg| {
            [&b"TIME"[..], b"RETRYCOUNT", b"FORCE", b"LASTID", b"IDLE"]
                .iter()
                .any(|option| arg.eq_ignore_ascii_case(option))
        })
}

/// Replays an XCLAIM as earlier versions ran it: JUSTID anywhere among the
/// IDs, every claim adds a delivery (JUSTID included), nothing checks that the
/// stream entry exists, and the consumer only appears when it claims.
fn replay_earlier_xclaim(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let key = &args[0];
    let group_name = &args[1];
    let consumer_name = &args[2];
    let Some(min_idle) = parse_i64(&args[3]) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    let mut ids = Vec::new();
    for arg in &args[4..] {
        if arg.eq_ignore_ascii_case(b"JUSTID") {
            continue;
        }
        let Some(id) = parse_strict_stream_id(arg) else {
            return CommandOutcome::reply(err("ERR syntax error"));
        };
        ids.push(id);
    }

    let now = now_ms();
    if let Some(reply) = require_group(server, client, key, group_name, now) {
        return reply;
    }
    let mut db = server.db_mut(client.selected_db);
    let Some(group) = db
        .get_mut(key)
        .and_then(|entry| entry.as_stream_groups_mut())
        .and_then(|groups| groups.get_mut(group_name))
    else {
        return stream_nogroup_error(key, group_name);
    };
    for id in ids {
        let Some(pending) = group.pending.get(&id) else {
            continue;
        };
        if now.saturating_sub(pending.last_delivered_ms) < min_idle {
            continue;
        }
        let terms = ClaimTerms {
            delivery_ms: now,
            retry_count: None,
            justid: false,
            base: pending.deliveries,
        };
        assign_pending_id(group, consumer_name, id, &terms);
        touch_consumer(group, consumer_name, now);
    }
    CommandOutcome::reply(RespFrame::Array(vec![]))
}

/// Replays an XAUTOCLAIM as earlier versions ran it: any COUNT (0 claims one
/// entry), no cap on the entries looked at, every claim adds a delivery, and
/// the consumer only appears when it claims.
fn replay_earlier_xautoclaim(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let key = &args[0];
    let group_name = &args[1];
    let consumer_name = &args[2];
    let Some(min_idle) = parse_i64(&args[3]) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    let start = match parse_interval_id(&args[4], IntervalEdge::Start) {
        Ok(id) => id,
        Err(reply) => return CommandOutcome::reply(reply),
    };
    let mut count = 100usize;
    let mut idx = 5usize;
    while idx < args.len() {
        if args[idx].eq_ignore_ascii_case(b"COUNT") {
            let Some(parsed) = args.get(idx + 1).and_then(parse_usize) else {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            };
            count = parsed;
            idx += 2;
        } else if args[idx].eq_ignore_ascii_case(b"JUSTID") {
            idx += 1;
        } else {
            return CommandOutcome::reply(err("ERR syntax error"));
        }
    }

    let now = now_ms();
    if let Some(reply) = require_group(server, client, key, group_name, now) {
        return reply;
    }
    let mut db = server.db_mut(client.selected_db);
    let Some(group) = db
        .get_mut(key)
        .and_then(|entry| entry.as_stream_groups_mut())
        .and_then(|groups| groups.get_mut(group_name))
    else {
        return stream_nogroup_error(key, group_name);
    };
    let mut pending_ids = group.pending.keys().copied().collect::<Vec<_>>();
    pending_ids.sort_unstable();
    let mut claimed = 0usize;
    for id in pending_ids {
        if id < start {
            continue;
        }
        let Some(pending) = group.pending.get(&id) else {
            continue;
        };
        if now.saturating_sub(pending.last_delivered_ms) < min_idle {
            continue;
        }
        let terms = ClaimTerms {
            delivery_ms: now,
            retry_count: None,
            justid: false,
            base: pending.deliveries,
        };
        assign_pending_id(group, consumer_name, id, &terms);
        touch_consumer(group, consumer_name, now);
        claimed += 1;
        if claimed >= count {
            break;
        }
    }
    CommandOutcome::reply(RespFrame::Array(vec![]))
}

pub(super) fn cmd_xclaim(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 5 {
        return wrong_arity("xclaim");
    }
    if is_earlier_xclaim_record(args) {
        return replay_earlier_xclaim(args, server, client);
    }

    let key = &args[0];
    let group_name = &args[1];
    let consumer_name = &args[2];
    let now = now_ms();

    // Redis's order: the key and group, then the minimum idle time, the IDs
    // and the options.
    if let Some(reply) = require_group(server, client, key, group_name, now) {
        return reply;
    }
    let Some(min_idle) = parse_i64(&args[3]) else {
        return CommandOutcome::reply(err("ERR Invalid min-idle-time argument for XCLAIM"));
    };
    let min_idle = min_idle.max(0);

    // The IDs are the run of arguments that parse as strict IDs and what
    // follows are options.
    let mut ids = Vec::new();
    let mut idx = 4usize;
    while let Some(id) = args.get(idx).and_then(|raw| parse_strict_stream_id(raw)) {
        ids.push(id);
        idx += 1;
    }
    let mut justid = false;
    let mut force = false;
    let mut delivery_time: Option<i64> = None;
    let mut retry_count: Option<i64> = None;
    let mut last_id = StreamId { ms: 0, seq: 0 };
    while idx < args.len() {
        let more = args.len() - 1 - idx;
        let option = &args[idx];
        if option.eq_ignore_ascii_case(b"FORCE") {
            force = true;
        } else if option.eq_ignore_ascii_case(b"JUSTID") {
            justid = true;
        } else if option.eq_ignore_ascii_case(b"IDLE") && more > 0 {
            idx += 1;
            let Some(idle) = parse_i64(&args[idx]) else {
                return CommandOutcome::reply(err("ERR Invalid IDLE option argument for XCLAIM"));
            };
            delivery_time = Some(now.saturating_sub(idle));
        } else if option.eq_ignore_ascii_case(b"TIME") && more > 0 {
            idx += 1;
            let Some(time) = parse_i64(&args[idx]) else {
                return CommandOutcome::reply(err("ERR Invalid TIME option argument for XCLAIM"));
            };
            delivery_time = Some(time);
        } else if option.eq_ignore_ascii_case(b"RETRYCOUNT") && more > 0 {
            idx += 1;
            let Some(count) = parse_i64(&args[idx]) else {
                return CommandOutcome::reply(err(
                    "ERR Invalid RETRYCOUNT option argument for XCLAIM",
                ));
            };
            // A negative count is the same as not giving one.
            retry_count = (count >= 0).then_some(count);
        } else if option.eq_ignore_ascii_case(b"LASTID") && more > 0 {
            idx += 1;
            let Some(id) = parse_strict_stream_id(&args[idx]) else {
                return CommandOutcome::reply(invalid_stream_id());
            };
            last_id = id;
        } else {
            return CommandOutcome::reply(err(&format!(
                "ERR Unrecognized XCLAIM option '{}'",
                String::from_utf8_lossy(option)
            )));
        }
        idx += 1;
    }

    // A time that is in the future or negative falls back to now, as Redis
    // does, since clients derive it from their own clocks.
    let delivery_ms = match delivery_time {
        Some(time) if (0..=now).contains(&time) => time,
        _ => now,
    };

    let mut db = server.db_mut(client.selected_db);
    let Some(entry) = db.get_mut(key) else {
        return stream_nogroup_error(key, group_name);
    };
    let Some((entries, groups, _)) = entry.as_stream_parts_mut() else {
        return wrong_type_response();
    };
    let Some(group) = groups.get_mut(group_name) else {
        return stream_nogroup_error(key, group_name);
    };

    if last_id > group.last_delivered_id {
        group.last_delivered_id = last_id;
    }
    touch_consumer(group, consumer_name, now);

    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        let pending = group
            .pending
            .get(&id)
            .map(|pending| (pending.deliveries, pending.last_delivered_ms));
        // An entry that is gone leaves the pending list instead of moving.
        if !entry_exists(entries, id) {
            remove_pending_id(group, id);
            continue;
        }
        let base = match pending {
            Some((deliveries, last_delivered_ms)) => {
                if min_idle > 0 && now.saturating_sub(last_delivered_ms) < min_idle {
                    continue;
                }
                deliveries
            }
            // FORCE creates the pending entry of an entry that exists.
            None if force => 1,
            None => continue,
        };
        let terms = ClaimTerms {
            delivery_ms,
            retry_count,
            justid,
            base,
        };
        assign_pending_id(group, consumer_name, id, &terms);
        out.extend(claimed_reply(entries, id, justid));
    }

    CommandOutcome::reply(RespFrame::Array(out))
}

/// The largest COUNT Redis accepts for XAUTOCLAIM, `LONG_MAX` over the size of
/// a stream ID (16 bytes, more than the attempts factor of 10).
const XAUTOCLAIM_MAX_COUNT: i64 = i64::MAX / 16;
const XAUTOCLAIM_ATTEMPTS_FACTOR: usize = 10;

pub(super) fn cmd_xautoclaim(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 5 {
        return wrong_arity("xautoclaim");
    }
    // This version never logs XAUTOCLAIM as sent, so under replay it is an
    // earlier record.
    if super::cmd_stream::replay_mode() {
        return replay_earlier_xautoclaim(args, server, client);
    }

    let key = &args[0];
    let group_name = &args[1];
    let consumer_name = &args[2];

    let Some(min_idle) = parse_i64(&args[3]) else {
        return CommandOutcome::reply(err("ERR Invalid min-idle-time argument for XAUTOCLAIM"));
    };
    let min_idle = min_idle.max(0);

    // The start is an interval bound, as in Redis: `-`, `+`, a bare
    // millisecond value and the `(` exclusive prefix are accepted.
    let start = match parse_interval_id(&args[4], IntervalEdge::Start) {
        Ok(id) => id,
        Err(reply) => return CommandOutcome::reply(reply),
    };

    let mut count = 100usize;
    let mut justid = false;
    let mut idx = 5usize;
    while idx < args.len() {
        let more = args.len() - 1 - idx;
        if args[idx].eq_ignore_ascii_case(b"COUNT") && more > 0 {
            let parsed = parse_i64(&args[idx + 1])
                .filter(|count| (1..=XAUTOCLAIM_MAX_COUNT).contains(count));
            let Some(parsed) = parsed else {
                return CommandOutcome::reply(err("ERR COUNT must be > 0"));
            };
            count = usize::try_from(parsed).unwrap_or(usize::MAX);
            idx += 2;
        } else if args[idx].eq_ignore_ascii_case(b"JUSTID") {
            justid = true;
            idx += 1;
        } else {
            return CommandOutcome::reply(err("ERR syntax error"));
        }
    }

    let now = now_ms();
    if let Some(reply) = require_group(server, client, key, group_name, now) {
        return reply;
    }
    let mut db = server.db_mut(client.selected_db);
    let Some(entry) = db.get_mut(key) else {
        return stream_nogroup_error(key, group_name);
    };
    let Some((entries, groups, _)) = entry.as_stream_parts_mut() else {
        return wrong_type_response();
    };
    let Some(group) = groups.get_mut(group_name) else {
        return stream_nogroup_error(key, group_name);
    };
    touch_consumer(group, consumer_name, now);

    let mut pending_ids = group
        .pending
        .keys()
        .copied()
        .filter(|id| *id >= start)
        .collect::<Vec<_>>();
    pending_ids.sort_unstable();

    // As Redis: at most COUNT claimed or deleted entries, and at most ten
    // times that many pending entries looked at. The cursor is the next
    // pending ID, 0-0 at the end.
    let mut attempts = count.saturating_mul(XAUTOCLAIM_ATTEMPTS_FACTOR);
    let mut remaining = count;
    let mut claimed = Vec::new();
    let mut deleted = Vec::new();
    let mut examined = 0usize;
    while attempts > 0 && remaining > 0 && examined < pending_ids.len() {
        attempts -= 1;
        let id = pending_ids[examined];
        examined += 1;

        if !entry_exists(entries, id) {
            remove_pending_id(group, id);
            deleted.push(id);
            remaining -= 1;
            continue;
        }
        let Some(pending) = group.pending.get(&id) else {
            continue;
        };
        if min_idle > 0 && now.saturating_sub(pending.last_delivered_ms) < min_idle {
            continue;
        }
        let terms = ClaimTerms {
            delivery_ms: now,
            retry_count: None,
            justid,
            base: pending.deliveries,
        };
        assign_pending_id(group, consumer_name, id, &terms);
        claimed.extend(claimed_reply(entries, id, justid));
        remaining -= 1;
    }
    let next_cursor = pending_ids
        .get(examined)
        .copied()
        .unwrap_or(StreamId { ms: 0, seq: 0 });

    CommandOutcome::reply(RespFrame::Array(vec![
        RespFrame::BulkString(Some(stream_id_to_bytes(next_cursor))),
        RespFrame::Array(claimed),
        RespFrame::Array(
            deleted
                .into_iter()
                .map(|id| RespFrame::BulkString(Some(stream_id_to_bytes(id))))
                .collect(),
        ),
    ]))
}

pub(super) fn cmd_xackdel(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 5 {
        return wrong_arity("xackdel");
    }

    let key = &args[0];
    let group_name = &args[1];

    let mut idx = 2usize;
    let mut condition = StreamDeleteCondition::KeepRef;
    if idx < args.len() && !args[idx].eq_ignore_ascii_case(b"IDS") {
        let Some(parsed) = parse_stream_delete_condition(&args[idx]) else {
            return CommandOutcome::reply(err("ERR syntax error"));
        };
        condition = parsed;
        idx += 1;
    }

    let ids = match parse_stream_ids_block(args, idx) {
        Ok(ids) => ids,
        Err(response) => return CommandOutcome::reply(response),
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::Array(
            ids.into_iter()
                .map(|_| RespFrame::Integer(-1))
                .collect::<Vec<_>>(),
        ));
    };
    if !entry.is_stream() {
        return wrong_type_response();
    }
    let Some(groups) = entry.as_stream_groups() else {
        return stream_nogroup_error(key, group_name);
    };
    if !groups.contains_key(group_name) {
        return stream_nogroup_error(key, group_name);
    }

    let mut out = Vec::with_capacity(ids.len());
    let mut removed_ids = HashSet::new();
    for id in ids {
        let exists = entry
            .as_stream_entries()
            .is_some_and(|stream| stream_contains_id(stream, id));
        if !exists {
            out.push(RespFrame::Integer(-1));
            continue;
        }

        let acked = if let Some(groups_mut) = entry.as_stream_groups_mut() {
            if let Some(group) = groups_mut.get_mut(group_name) {
                if let Some(pending) = group.pending.remove(&id) {
                    if let Some(consumer) = group.consumers.get_mut(&pending.consumer) {
                        consumer.pending.remove(&id);
                    }
                    true
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };

        if !acked {
            out.push(RespFrame::Integer(-1));
            continue;
        }

        let has_pending_elsewhere = entry
            .as_stream_groups()
            .is_some_and(|groups_ref| stream_has_pending_references(groups_ref, id));

        match condition {
            StreamDeleteCondition::KeepRef => {
                removed_ids.insert(id);
                out.push(RespFrame::Integer(if has_pending_elsewhere {
                    2
                } else {
                    1
                }));
            }
            StreamDeleteCondition::DelRef => {
                removed_ids.insert(id);
                if let Some(groups_mut) = entry.as_stream_groups_mut() {
                    purge_stream_pending_id(groups_mut, id);
                }
                out.push(RespFrame::Integer(1));
            }
            StreamDeleteCondition::Acked => {
                if has_pending_elsewhere {
                    out.push(RespFrame::Integer(2));
                } else {
                    removed_ids.insert(id);
                    out.push(RespFrame::Integer(1));
                }
            }
        }
    }

    if !removed_ids.is_empty() {
        let mut deleted = Vec::new();
        if let Some(stream) = entry.as_stream_entries_mut() {
            stream.retain(|item| {
                let keep = !removed_ids.contains(&item.id);
                if !keep {
                    deleted.push(item.id);
                }
                keep
            });
        }
        record_deleted_ids(entry, deleted);
    }

    CommandOutcome::reply(RespFrame::Array(out))
}

pub(super) fn cmd_xdelex(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 4 {
        return wrong_arity("xdelex");
    }

    let key = &args[0];

    let mut idx = 1usize;
    let mut condition = StreamDeleteCondition::KeepRef;
    if idx < args.len() && !args[idx].eq_ignore_ascii_case(b"IDS") {
        let Some(parsed) = parse_stream_delete_condition(&args[idx]) else {
            return CommandOutcome::reply(err("ERR syntax error"));
        };
        condition = parsed;
        idx += 1;
    }

    let ids = match parse_stream_ids_block(args, idx) {
        Ok(ids) => ids,
        Err(response) => return CommandOutcome::reply(response),
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::Array(
            ids.into_iter()
                .map(|_| RespFrame::Integer(-1))
                .collect::<Vec<_>>(),
        ));
    };
    if !entry.is_stream() {
        return wrong_type_response();
    }

    let mut out = Vec::with_capacity(ids.len());
    let mut removed_ids = HashSet::new();
    for id in ids {
        let exists = entry
            .as_stream_entries()
            .is_some_and(|stream| stream_contains_id(stream, id));
        if !exists {
            out.push(RespFrame::Integer(-1));
            continue;
        }

        let has_pending = entry
            .as_stream_groups()
            .is_some_and(|groups| stream_has_pending_references(groups, id));

        match condition {
            StreamDeleteCondition::KeepRef => {
                removed_ids.insert(id);
                out.push(RespFrame::Integer(if has_pending { 2 } else { 1 }));
            }
            StreamDeleteCondition::DelRef => {
                removed_ids.insert(id);
                if let Some(groups_mut) = entry.as_stream_groups_mut() {
                    purge_stream_pending_id(groups_mut, id);
                }
                out.push(RespFrame::Integer(1));
            }
            StreamDeleteCondition::Acked => {
                if has_pending {
                    out.push(RespFrame::Integer(2));
                } else {
                    removed_ids.insert(id);
                    out.push(RespFrame::Integer(1));
                }
            }
        }
    }

    if !removed_ids.is_empty() {
        let mut deleted = Vec::new();
        if let Some(stream) = entry.as_stream_entries_mut() {
            stream.retain(|item| {
                let keep = !removed_ids.contains(&item.id);
                if !keep {
                    deleted.push(item.id);
                }
                keep
            });
        }
        record_deleted_ids(entry, deleted);
    }

    CommandOutcome::reply(RespFrame::Array(out))
}
