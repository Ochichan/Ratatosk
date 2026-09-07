#!/usr/bin/env python3
"""Real-process Ratatosk persistence tests beyond the Redis 7 reference surface.

Uses the same redis-py dependency and isolated process fixture as verify_preuse.
These results are Ratatosk-specific checks, not Redis compatibility comparisons.
"""
import argparse
import json
from pathlib import Path
import time
import traceback

from verify_preuse import Server, binary, equal, wait_rewrite


def relative_expiry(s):
    with s.client() as c:
        c.execute_command("MSETEX", 1, "multi-expiry", "v", "PX", 200)
        c.set("source", "payload")
        c.restore("restored", 200, c.dump("source"))
        c.hset("hash", mapping={"expired": "v", "extended": "keep"})
        equal(c.execute_command("HPEXPIRE", "hash", 200, "FIELDS", 2, "expired", "extended"), [1, 1])
        c.execute_command("HPERSIST", "hash", "FIELDS", 1, "extended")
        with c.pipeline() as transaction:
            transaction.execute_command("MSETEX", 1, "tx-expiry", "v", "PX", 200)
            transaction.execute()
        time.sleep(0.25)
    s.restart(kill=True)
    with s.client() as c:
        equal(c.mget("multi-expiry", "restored", "tx-expiry"), [None, None, None])
        equal(c.hgetall("hash"), {b"extended": b"keep"})


def hash_snapshot(s, mode):
    with s.client() as c:
        c.hset("hash", mapping={"expiring": "v", "permanent": "keep"})
        equal(c.execute_command("HPEXPIRE", "hash", 600000, "FIELDS", 1, "expiring"), [1])
        deadline = c.execute_command("HPEXPIRETIME", "hash", "FIELDS", 1, "expiring")
        if mode == "rewrite":
            c.bgrewriteaof()
            wait_rewrite(c)
        elif mode == "enable":
            c.config_set("appendonly", "yes")
            s.aof = True
        else:
            c.save()
    s.restart(kill=True)
    with s.client() as c:
        equal(c.execute_command("HPEXPIRETIME", "hash", "FIELDS", 1, "expiring"), deadline)
        equal(c.hgetall("hash"), {b"expiring": b"v", b"permanent": b"keep"})


def headerless_opt_in(s):
    s.stop(kill=True)
    path = next(s.directory.glob("*.incr.aof"))
    legacy = b"*3\r\n$3\r\nSET\r\n$6\r\nlegacy\r\n$4\r\nkept\r\n"
    path.write_bytes(legacy)
    try:
        s.start()
    except RuntimeError:
        pass
    else:
        raise AssertionError("headerless AOF bypassed explicit opt-in")
    equal(path.read_bytes(), legacy)
    s.extra["RATATOSK_ALLOW_LEGACY_AOF"] = "true"
    s.start()
    with s.client() as c:
        equal(c.get("legacy"), b"kept")
        c.set("new", "write")
    s.restart(kill=True)
    with s.client() as c:
        equal(c.mget("legacy", "new"), [b"kept", b"write"])


CASES = [("relative_expiry", True, relative_expiry),
         ("headerless_opt_in", True, headerless_opt_in),
         ("hash_rewrite", True, lambda s: hash_snapshot(s, "rewrite")),
         ("hash_runtime_enable", False, lambda s: hash_snapshot(s, "enable")),
         ("hash_rdb", False, lambda s: hash_snapshot(s, "rdb"))]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ratatosk", required=True, type=binary)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    results = []
    for name, aof, function in CASES:
        row = {"server": "ratatosk", "case": name, "status": "PASS"}
        try:
            with Server(args.ratatosk, "ratatosk", args.output_dir, aof=aof) as s:
                function(s)
            row["artifacts"] = str(s.directory)
        except Exception as error:
            row.update(status="FAIL", error=str(error), traceback=traceback.format_exc())
        results.append(row)
        print(json.dumps(row), flush=True)
    (args.output_dir / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    return int(any(row["status"] == "FAIL" for row in results))


if __name__ == "__main__":
    raise SystemExit(main())
