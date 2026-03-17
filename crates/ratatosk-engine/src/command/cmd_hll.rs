use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::hll;
use crate::keyspace::{ServerState, StoredValue, purge_expired_key};

use super::{ClientState, CommandOutcome, err, now_ms, wrong_arity, wrong_type_response};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract HLL bytes from a stored value, verifying the string type and magic.
/// Returns `Ok(bytes_ref)` on success, or an appropriate error `CommandOutcome`.
#[allow(clippy::result_large_err)]
fn extract_hll_bytes(entry: &StoredValue) -> Result<&Bytes, CommandOutcome> {
    let Some(s) = entry.as_string() else {
        return Err(wrong_type_response());
    };
    if !hll::hll_is_valid(s) {
        return Err(CommandOutcome::reply(err(
            "WRONGTYPE Key is not a valid HyperLogLog string value.",
        )));
    }
    Ok(s)
}

// ---------------------------------------------------------------------------
// PFADD key [element ...]
// ---------------------------------------------------------------------------

pub(super) fn cmd_pfadd(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("pfadd");
    }

    let key = &args[0];
    let elements = &args[1..];
    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    // Fetch existing HLL data and its TTL, or start fresh.
    let (mut hll_data, expire_at_ms) = if let Some(entry) = db.get(key) {
        let hll_bytes = match extract_hll_bytes(entry) {
            Ok(b) => b,
            Err(outcome) => return outcome,
        };
        (hll_bytes.to_vec(), entry.expire_at_ms)
    } else {
        (Vec::new(), None)
    };

    if elements.is_empty() {
        // No elements: ensure the key exists as HLL, return 1 if created.
        if hll_data.is_empty() {
            let created = hll::hll_create();
            db.insert(key.clone(), StoredValue::string(Bytes::from(created), None));
            return CommandOutcome::reply(RespFrame::Integer(1));
        }
        return CommandOutcome::reply(RespFrame::Integer(0));
    }

    // Ensure we have a valid HLL buffer (may be empty if key was absent).
    if hll_data.len() != hll::HLL_SIZE {
        hll_data = hll::hll_create();
    }

    let mut changed = false;
    for elem in elements {
        if hll::hll_add(&mut hll_data, elem) {
            changed = true;
        }
    }

    db.insert(
        key.clone(),
        StoredValue::string(Bytes::from(hll_data), expire_at_ms),
    );

    CommandOutcome::reply(RespFrame::Integer(if changed { 1 } else { 0 }))
}

// ---------------------------------------------------------------------------
// PFCOUNT key [key ...]
// ---------------------------------------------------------------------------

pub(super) fn cmd_pfcount(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("pfcount");
    }

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);

    if args.len() == 1 {
        // Single key: return (and cache) the cardinality directly.
        let key = &args[0];
        purge_expired_key(&mut db, key, now);

        let Some(entry) = db.get(key) else {
            return CommandOutcome::reply(RespFrame::Integer(0));
        };
        let hll_bytes = match extract_hll_bytes(entry) {
            Ok(b) => b,
            Err(outcome) => return outcome,
        };

        let mut data = hll_bytes.to_vec();
        let count = hll::hll_count_and_cache(&mut data);

        // Write the updated cache back only when needed.
        let expire_at_ms = entry.expire_at_ms;
        if hll_bytes.as_ref() != data.as_slice() {
            db.insert(
                key.clone(),
                StoredValue::string(Bytes::from(data), expire_at_ms),
            );
        }

        CommandOutcome::reply(RespFrame::Integer(count))
    } else {
        // Multiple keys: merge into a temporary HLL, then count.
        let mut tmp = hll::hll_create();

        for key in args {
            purge_expired_key(&mut db, key, now);
            let Some(entry) = db.get(key) else {
                continue;
            };
            let hll_bytes = match extract_hll_bytes(entry) {
                Ok(b) => b,
                Err(outcome) => return outcome,
            };
            hll::hll_merge(&mut tmp, &[hll_bytes.as_ref()]);
        }

        let count = hll::hll_count(&tmp);
        CommandOutcome::reply(RespFrame::Integer(count))
    }
}

// ---------------------------------------------------------------------------
// PFMERGE destkey sourcekey [sourcekey ...]
// ---------------------------------------------------------------------------

pub(super) fn cmd_pfmerge(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("pfmerge");
    }

    let dest_key = &args[0];
    let source_keys = &args[1..];
    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);

    // Start from the destination's existing HLL (if any).
    purge_expired_key(&mut db, dest_key, now);
    let (mut dest_data, dest_expire) = if let Some(entry) = db.get(dest_key) {
        let hll_bytes = match extract_hll_bytes(entry) {
            Ok(b) => b,
            Err(outcome) => return outcome,
        };
        (hll_bytes.to_vec(), entry.expire_at_ms)
    } else {
        (hll::hll_create(), None)
    };

    if dest_data.len() != hll::HLL_SIZE {
        dest_data = hll::hll_create();
    }

    // Collect source HLL blobs (validated).
    let mut source_vecs: Vec<Vec<u8>> = Vec::with_capacity(source_keys.len());
    for key in source_keys {
        purge_expired_key(&mut db, key, now);
        let Some(entry) = db.get(key) else {
            continue;
        };
        let hll_bytes = match extract_hll_bytes(entry) {
            Ok(b) => b,
            Err(outcome) => return outcome,
        };
        source_vecs.push(hll_bytes.to_vec());
    }

    let source_slices: Vec<&[u8]> = source_vecs.iter().map(|v| v.as_slice()).collect();
    hll::hll_merge(&mut dest_data, &source_slices);

    db.insert(
        dest_key.clone(),
        StoredValue::string(Bytes::from(dest_data), dest_expire),
    );

    CommandOutcome::reply(RespFrame::ok())
}

// ---------------------------------------------------------------------------
// PFDEBUG subcommand key
// ---------------------------------------------------------------------------

pub(super) fn cmd_pfdebug(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("pfdebug");
    }

    let key = &args[1];
    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(err("ERR The specified key does not exist"));
    };
    let _hll_bytes = match extract_hll_bytes(entry) {
        Ok(b) => b,
        Err(outcome) => return outcome,
    };

    CommandOutcome::reply(RespFrame::Array(vec![
        RespFrame::BulkString(Some(Bytes::from_static(b"encoding"))),
        RespFrame::BulkString(Some(Bytes::from_static(b"dense"))),
        RespFrame::BulkString(Some(Bytes::from_static(b"registers"))),
        RespFrame::Integer(16384),
    ]))
}

// ---------------------------------------------------------------------------
// PFSELFTEST
// ---------------------------------------------------------------------------

pub(super) fn cmd_pfselftest(
    args: &[Bytes],
    _server: &mut ServerState,
    _client: &ClientState,
) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("pfselftest");
    }

    // Basic self-test: add known elements, verify cardinality is reasonable.
    let mut blob = hll::hll_create();

    // Phase 1: single element.
    hll::hll_add(&mut blob, b"selftest-element-0");
    let count = hll::hll_count(&blob);
    if !(1..=2).contains(&count) {
        return CommandOutcome::reply(err(
            "ERR PFSELFTEST failed: single element count out of range",
        ));
    }

    // Phase 2: add 100 distinct elements.
    for i in 0_u32..100 {
        let elem = i.to_le_bytes();
        hll::hll_add(&mut blob, &elem);
    }
    let count = hll::hll_count(&blob);
    // With 101 distinct elements (1 from phase 1 + 100), we accept 80..130.
    if !(80..=130).contains(&count) {
        return CommandOutcome::reply(err(
            "ERR PFSELFTEST failed: 100-element count out of acceptable range",
        ));
    }

    // Phase 3: merge test.
    let mut a = hll::hll_create();
    let mut b = hll::hll_create();
    for i in 0_u32..500 {
        hll::hll_add(&mut a, &i.to_le_bytes());
    }
    for i in 500_u32..1000 {
        hll::hll_add(&mut b, &i.to_le_bytes());
    }
    let mut merged = hll::hll_create();
    hll::hll_merge(&mut merged, &[&a, &b]);
    let count = hll::hll_count(&merged);
    // 1000 distinct elements, accept 850..1150 (15% tolerance).
    if !(850..=1150).contains(&count) {
        return CommandOutcome::reply(err(
            "ERR PFSELFTEST failed: merge count out of acceptable range",
        ));
    }

    CommandOutcome::reply(RespFrame::ok())
}
