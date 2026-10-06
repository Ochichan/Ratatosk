use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ServerState, StoredValue, purge_expired_key};
use crate::object::{PROTO_MAX_BULK_LEN, STRING_TOO_LONG_ERR, normalize_string_range};

use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, wrong_arity, wrong_type_response,
};

pub(super) fn cmd_append(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, append] = args else {
        return wrong_arity("append");
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(new_len) = db.mutate_string(key, |buf| {
        let new_len = buf.len() + append.len();
        if new_len > PROTO_MAX_BULK_LEN {
            return (None, false);
        }
        crate::keyspace::reserve_string_growth(buf, new_len);
        buf.extend_from_slice(append);
        (Some(new_len), true)
    }) else {
        return wrong_type_response();
    };
    let Some(new_len) = new_len else {
        return CommandOutcome::reply(err(STRING_TOO_LONG_ERR));
    };

    CommandOutcome::reply(RespFrame::Integer(new_len as i64))
}

pub(super) fn cmd_strlen(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key] = args else {
        return wrong_arity("strlen");
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(s) = entry.as_string_bytes() else {
        return wrong_type_response();
    };

    CommandOutcome::reply(RespFrame::Integer(s.len() as i64))
}

pub(super) fn cmd_getrange(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, start_raw, end_raw] = args else {
        return wrong_arity("getrange");
    };

    let Some(start) = parse_i64(start_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    let Some(end) = parse_i64(end_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::bulk_str(""));
    };
    let Some(s) = entry.as_string_bytes() else {
        return wrong_type_response();
    };

    let bytes = s.as_ref();
    let Some((range_start, range_end)) = normalize_string_range(bytes.len(), start, end) else {
        return CommandOutcome::reply(RespFrame::bulk_str(""));
    };

    CommandOutcome::reply(RespFrame::BulkString(Some(Bytes::copy_from_slice(
        &bytes[range_start..=range_end],
    ))))
}

pub(super) fn cmd_setrange(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, offset_raw, value] = args else {
        return wrong_arity("setrange");
    };

    let Some(offset_i64) = parse_i64(offset_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    if offset_i64 < 0 {
        return CommandOutcome::reply(err("ERR offset is out of range"));
    }
    let Ok(offset) = usize::try_from(offset_i64) else {
        return CommandOutcome::reply(err("ERR offset is out of range"));
    };
    // Checked before any allocation: an unchecked offset would otherwise
    // resize the value to an arbitrary size and abort the process. Like
    // Redis, an empty value writes nothing and skips the check.
    if !value.is_empty() && offset.saturating_add(value.len()) > PROTO_MAX_BULK_LEN {
        return CommandOutcome::reply(err(STRING_TOO_LONG_ERR));
    }

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    if value.is_empty() {
        let Some(entry) = db.get(key) else {
            return CommandOutcome::reply(RespFrame::Integer(0));
        };
        let Some(s) = entry.as_string_bytes() else {
            return wrong_type_response();
        };
        return CommandOutcome::reply(RespFrame::Integer(s.len() as i64));
    }

    let Some(required_len) = offset.checked_add(value.len()) else {
        return CommandOutcome::reply(err(STRING_TOO_LONG_ERR));
    };
    let Some(new_len) = db.mutate_string(key, |base| {
        if base.len() < required_len {
            crate::keyspace::reserve_string_growth(base, required_len);
            base.resize(required_len, 0);
        }
        base[offset..required_len].copy_from_slice(value);
        (base.len(), true)
    }) else {
        return wrong_type_response();
    };

    CommandOutcome::reply(RespFrame::Integer(new_len as i64))
}

pub(super) fn cmd_getset(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, value] = args else {
        return wrong_arity("getset");
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    if db.get(key).is_some_and(|entry| !entry.is_string()) {
        return wrong_type_response();
    }

    let previous = db.insert(key.clone(), StoredValue::string(value.clone(), None));

    CommandOutcome::reply(RespFrame::BulkString(
        previous.and_then(|entry| entry.as_string_bytes()),
    ))
}

pub(super) fn cmd_setnx(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, value] = args else {
        return wrong_arity("setnx");
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    if db.contains_key(key) {
        return CommandOutcome::reply(RespFrame::Integer(0));
    }

    db.insert(key.clone(), StoredValue::string(value.clone(), None));
    CommandOutcome::reply(RespFrame::Integer(1))
}

pub(super) fn cmd_mget(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("mget");
    }

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    let mut out = Vec::with_capacity(args.len());

    for key in args {
        purge_expired_key(&mut db, key, now);
        let value = db.get(key).and_then(|entry| entry.as_string_bytes());
        out.push(RespFrame::BulkString(value));
    }

    CommandOutcome::reply(RespFrame::Array(out))
}

pub(super) fn cmd_mset(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 || args.len() % 2 != 0 {
        return wrong_arity("mset");
    }

    let mut db = server.db_mut(client.selected_db);
    let mut idx = 0usize;
    while idx < args.len() {
        db.insert(
            args[idx].clone(),
            StoredValue::string(args[idx + 1].clone(), None),
        );
        idx += 2;
    }

    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn cmd_msetnx(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 || args.len() % 2 != 0 {
        return wrong_arity("msetnx");
    }

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);

    let mut idx = 0usize;
    while idx < args.len() {
        purge_expired_key(&mut db, &args[idx], now);
        if db.contains_key(&args[idx]) {
            return CommandOutcome::reply(RespFrame::Integer(0));
        }
        idx += 2;
    }

    let mut idx = 0usize;
    while idx < args.len() {
        db.insert(
            args[idx].clone(),
            StoredValue::string(args[idx + 1].clone(), None),
        );
        idx += 2;
    }

    CommandOutcome::reply(RespFrame::Integer(1))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use ratatosk_resp::frame::RespFrame;

    use crate::keyspace::ServerState;

    use super::super::ClientState;

    use super::super::execute;

    fn cmd(parts: &[&str]) -> RespFrame {
        RespFrame::Array(parts.iter().map(|part| RespFrame::bulk_str(part)).collect())
    }

    fn run(parts: &[&str], server: &mut ServerState, client: &mut ClientState) -> RespFrame {
        let mut access = super::super::ServerAccess::new_inline(server);
        execute(cmd(parts), &mut access, client).response
    }

    fn bulk(reply: &RespFrame) -> &[u8] {
        match reply {
            RespFrame::BulkString(Some(value)) => value.as_ref(),
            other => panic!("expected bulk string, got {other:?}"),
        }
    }

    fn integer(reply: &RespFrame) -> i64 {
        match reply {
            RespFrame::Integer(value) => *value,
            other => panic!("expected integer, got {other:?}"),
        }
    }

    fn assert_wrongtype(reply: &RespFrame) {
        match reply {
            RespFrame::Error(message) => {
                assert!(std::str::from_utf8(message).unwrap().contains("WRONGTYPE"))
            }
            other => panic!("expected WRONGTYPE, got {other:?}"),
        }
    }

    #[test]
    fn numeric_string_supports_string_mutations_ttl_and_type_rejection() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["SET", "value", "42"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["TYPE", "value"], &mut server, &mut client),
            RespFrame::SimpleString(Bytes::from_static(b"string"))
        );
        assert_eq!(
            integer(&run(&["APPEND", "value", "."], &mut server, &mut client)),
            3
        );
        assert_eq!(
            bulk(&run(&["GET", "value"], &mut server, &mut client)),
            b"42."
        );
        assert_eq!(
            integer(&run(&["STRLEN", "value"], &mut server, &mut client)),
            3
        );
        assert_eq!(
            bulk(&run(
                &["GETRANGE", "value", "0", "-1"],
                &mut server,
                &mut client
            )),
            b"42."
        );
        assert_eq!(
            integer(&run(
                &["SETRANGE", "value", "3", "x"],
                &mut server,
                &mut client
            )),
            4
        );
        assert_eq!(
            bulk(&run(&["GET", "value"], &mut server, &mut client)),
            b"42.x"
        );

        assert_eq!(
            run(
                &["SET", "min", "-9223372036854775808", "PX", "5000"],
                &mut server,
                &mut client
            ),
            RespFrame::ok()
        );
        assert_eq!(
            integer(&run(&["APPEND", "min", "!"], &mut server, &mut client)),
            21
        );
        assert_eq!(
            bulk(&run(&["GET", "min"], &mut server, &mut client)),
            b"-9223372036854775808!"
        );
        let ttl = integer(&run(&["PTTL", "min"], &mut server, &mut client));
        assert!((0..=5000).contains(&ttl));

        assert_eq!(
            run(&["RPUSH", "list", "a"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["HSET", "hash", "field", "a"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_wrongtype(&run(&["APPEND", "list", "b"], &mut server, &mut client));
        assert_wrongtype(&run(
            &["SETBIT", "hash", "0", "1"],
            &mut server,
            &mut client,
        ));
    }

    /// OBJECT ENCODING reports "raw" for every string, so read the stored
    /// encoding tag and payload variant directly.
    fn encoding_of(key: &str, server: &mut ServerState, _client: &mut ClientState) -> String {
        let shard = server.data.read_db(0);
        let value = shard.data.get(key.as_bytes()).expect("key present");
        let is_int = value.as_int().is_some();
        let tag = value.encoding();
        assert_eq!(is_int, tag == crate::keyspace::Encoding::Int);
        if is_int { "int" } else { "raw" }.to_string()
    }

    fn stored_bytes(server: &ServerState, key: &str) -> Bytes {
        let shard = server.data.read_db(0);
        shard
            .data
            .get(key.as_bytes())
            .and_then(|v| v.as_string_bytes())
            .expect("string key")
    }

    #[test]
    fn in_place_mutations_match_fresh_set_semantics() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        // Missing keys are created, with zero padding where the offset leaves a gap.
        assert_eq!(
            integer(&run(&["APPEND", "a", "hi"], &mut server, &mut client)),
            2
        );
        assert_eq!(
            integer(&run(&["SETRANGE", "r", "3", "x"], &mut server, &mut client)),
            4
        );
        assert_eq!(
            bulk(&run(&["GET", "r"], &mut server, &mut client)),
            b"\0\0\0x"
        );
        assert_eq!(
            integer(&run(&["SETBIT", "b", "17", "1"], &mut server, &mut client)),
            0
        );
        assert_eq!(
            bulk(&run(&["GET", "b"], &mut server, &mut client)),
            b"\0\0\x40"
        );
        assert_eq!(
            run(
                &["BITFIELD", "bf", "SET", "u8", "8", "255"],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![RespFrame::Integer(0)])
        );
        assert_eq!(
            bulk(&run(&["GET", "bf"], &mut server, &mut client)),
            b"\0\xff"
        );
        // A GET-only BITFIELD on a missing key creates nothing.
        run(
            &["BITFIELD", "nokey", "GET", "u8", "0"],
            &mut server,
            &mut client,
        );
        assert_eq!(
            run(&["EXISTS", "nokey"], &mut server, &mut client),
            RespFrame::Integer(0)
        );

        // Integer-encoded value: "12" + "3" stays an int, "12" + "x" becomes raw.
        run(&["SET", "n", "12"], &mut server, &mut client);
        assert_eq!(encoding_of("n", &mut server, &mut client), "int");
        run(&["APPEND", "n", "3"], &mut server, &mut client);
        assert_eq!(bulk(&run(&["GET", "n"], &mut server, &mut client)), b"123");
        assert_eq!(encoding_of("n", &mut server, &mut client), "int");
        run(&["APPEND", "n", "x"], &mut server, &mut client);
        assert_eq!(encoding_of("n", &mut server, &mut client), "raw");
        // SETRANGE that turns a raw value back into an integer string.
        run(&["SETRANGE", "n", "3", "4"], &mut server, &mut client);
        run(&["SETRANGE", "n", "4", ""], &mut server, &mut client);
        assert_eq!(bulk(&run(&["GET", "n"], &mut server, &mut client)), b"1234");
        assert_eq!(encoding_of("n", &mut server, &mut client), "int");

        // Encoding always equals what a fresh SET of the same bytes gets.
        run(&["SET", "g", "1"], &mut server, &mut client);
        let big = "z".repeat(60);
        run(&["APPEND", "g", &big], &mut server, &mut client);
        run(&["SET", "g2", &format!("1{big}")], &mut server, &mut client);
        assert_eq!(
            encoding_of("g", &mut server, &mut client),
            encoding_of("g2", &mut server, &mut client)
        );
        // SETBIT can produce digit bytes: 0x31 = "1".
        for off in [2, 3, 7] {
            run(
                &["SETBIT", "d", &off.to_string(), "1"],
                &mut server,
                &mut client,
            );
        }
        assert_eq!(bulk(&run(&["GET", "d"], &mut server, &mut client)), b"1");
        assert_eq!(encoding_of("d", &mut server, &mut client), "int");
        assert_eq!(
            run(
                &["BITFIELD", "d", "SET", "u8", "0", "50"],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![RespFrame::Integer(49)])
        );
        assert_eq!(bulk(&run(&["GET", "d"], &mut server, &mut client)), b"2");
        assert_eq!(encoding_of("d", &mut server, &mut client), "int");
    }

    #[test]
    fn in_place_mutations_keep_ttl_and_reject_other_types() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        for (name, args) in [
            ("append", vec!["APPEND", "t", "b"]),
            ("setrange", vec!["SETRANGE", "t", "1", "b"]),
            ("setbit", vec!["SETBIT", "t", "9", "1"]),
            ("bitfield", vec!["BITFIELD", "t", "SET", "u8", "8", "1"]),
        ] {
            run(&["SET", "t", "a", "PX", "100000"], &mut server, &mut client);
            run(&args, &mut server, &mut client);
            let ttl = integer(&run(&["PTTL", "t"], &mut server, &mut client));
            assert!(
                (1..=100_000).contains(&ttl),
                "{name} must keep the TTL, got {ttl}"
            );
        }

        run(&["RPUSH", "l", "a"], &mut server, &mut client);
        assert_wrongtype(&run(&["APPEND", "l", "b"], &mut server, &mut client));
        assert_wrongtype(&run(&["SETRANGE", "l", "0", "b"], &mut server, &mut client));
        assert_wrongtype(&run(&["SETBIT", "l", "0", "1"], &mut server, &mut client));
        assert_wrongtype(&run(
            &["BITFIELD", "l", "SET", "u8", "0", "1"],
            &mut server,
            &mut client,
        ));
    }

    #[test]
    fn in_place_mutations_move_memory_estimate_by_the_length_delta() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        run(&["SET", "m", &"a".repeat(100)], &mut server, &mut client);
        let before = server.data.estimated_memory();
        run(&["APPEND", "m", &"b".repeat(50)], &mut server, &mut client);
        assert_eq!(server.data.estimated_memory(), before + 50);
        run(&["SETRANGE", "m", "10", "zz"], &mut server, &mut client);
        assert_eq!(server.data.estimated_memory(), before + 50);
        run(&["SETRANGE", "m", "200", "z"], &mut server, &mut client);
        assert_eq!(server.data.estimated_memory(), before + 101);
        run(&["SETBIT", "m", "8000", "1"], &mut server, &mut client);
        assert_eq!(server.data.estimated_memory(), before + 901);
        run(
            &["BITFIELD", "m", "SET", "u8", "8000", "7"],
            &mut server,
            &mut client,
        );
        assert_eq!(server.data.estimated_memory(), before + 901);
        run(
            &["BITFIELD", "m", "SET", "u8", "8200", "7"],
            &mut server,
            &mut client,
        );
        assert_eq!(server.data.estimated_memory(), before + 926);
        let total = {
            let shard = server.data.read_db(0);
            let key = Bytes::from_static(b"m");
            crate::eviction::estimate_object_memory(&key, shard.data.get(&key).unwrap())
        };
        assert_eq!(server.data.estimated_memory(), total);
    }

    #[test]
    fn in_place_mutations_never_change_a_clone_held_elsewhere() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        for (args, expected_len) in [
            (vec!["APPEND", "c", "!"], 9usize),
            (vec!["SETRANGE", "c", "0", "X"], 8),
            (vec!["SETBIT", "c", "1", "0"], 8),
            (vec!["BITFIELD", "c", "SET", "u8", "0", "0"], 8),
        ] {
            run(&["SET", "c", "abcdefgh"], &mut server, &mut client);
            let held = stored_bytes(&server, "c");
            let reply = run(&["GET", "c"], &mut server, &mut client);
            run(&args, &mut server, &mut client);
            assert_eq!(held.as_ref(), b"abcdefgh", "{args:?} changed a held clone");
            assert_eq!(bulk(&reply), b"abcdefgh", "{args:?} changed a GET reply");
            let now = stored_bytes(&server, "c");
            assert_eq!(now.len(), expected_len);
            assert_ne!(now.as_ref(), b"abcdefgh");
        }
    }

    #[test]
    fn mutate_string_creates_a_missing_key_only_when_modified() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();
        let key = Bytes::from_static(b"k");
        {
            let mut db = server.db_mut(0);
            assert_eq!(db.mutate_string(&key, |buf| (buf.len(), false)), Some(0));
        }
        assert_eq!(
            run(&["EXISTS", "k"], &mut server, &mut client),
            RespFrame::Integer(0)
        );
        {
            let mut db = server.db_mut(0);
            assert_eq!(
                db.mutate_string(&key, |buf| {
                    buf.extend_from_slice(b"7");
                    ((), true)
                }),
                Some(())
            );
        }
        assert_eq!(bulk(&run(&["GET", "k"], &mut server, &mut client)), b"7");
        assert_eq!(encoding_of("k", &mut server, &mut client), "int");
    }
}
