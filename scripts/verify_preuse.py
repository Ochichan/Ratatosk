#!/usr/bin/env python3
"""Exercise the standalone contract against real Ratatosk and Redis processes.

Requires redis-py 5.2.1 or later. Both binaries are required; absence is an error,
never a skipped comparison. All processes use private data directories and
loopback ports. Only processes launched by this script are stopped.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import random
import shutil
import socket
import subprocess
import tempfile
import time
import traceback

try:
    import redis
except ImportError as error:
    raise SystemExit("redis-py is required: install redis==5.2.1 in a virtualenv") from error


def equal(actual, expected):
    if type(actual) is not type(expected) or actual != expected:
        raise AssertionError(f"expected {expected!r}, got {actual!r}")


class Server:
    def __init__(self, binary: Path, kind: str, output: Path, *, aof=False, extra=None,
                 unixsocket: Path | None = None):
        self.binary, self.kind, self.aof = binary, kind, aof
        self.extra = extra or {}
        self.unixsocket = unixsocket
        self.directory = Path(tempfile.mkdtemp(prefix=f"{kind}-", dir=output))
        self.process = None
        self.log = None
        self.port = 0

    def start(self):
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            self.port = reservation.getsockname()[1]
        environment = {k: v for k, v in os.environ.items() if not k.startswith("RATATOSK_")}
        if self.kind == "ratatosk":
            environment.update(
                RATATOSK_BIND="127.0.0.1",
                RATATOSK_PORT=str(self.port),
                RATATOSK_DIR=str(self.directory),
                RATATOSK_DISABLE_CONFIG_AUTOLOAD="true",
                RATATOSK_APPENDONLY=str(self.aof).lower(),
                RATATOSK_APPENDFSYNC="always",
                RATATOSK_METRICS_BIND="127.0.0.1:0",
                RATATOSK_ALLOW_NO_METRICS="true",
                RATATOSK_AUDIT_LOG=str(self.directory / "audit.log"),
                RATATOSK_AUDIT_CHAIN_STATE=str(self.directory / "audit.state"),
                RATATOSK_CONN_RATE_LIMIT_MAX_ATTEMPTS="100000",
                RATATOSK_SHUTDOWN_GRACE_MS="1000",
            )
            if self.unixsocket:
                environment["RATATOSK_UNIXSOCKET"] = str(self.unixsocket)
            environment.update(self.extra)
            command = [str(self.binary), "--no-config-autoload"]
        else:
            command = [str(self.binary), "--bind", "127.0.0.1", "--port", str(self.port),
                       "--dir", str(self.directory), "--save", "", "--appendonly",
                       "yes" if self.aof else "no", "--appendfsync", "always"]
        self.log = (self.directory / "server.log").open("ab")
        self.process = subprocess.Popen(command, cwd=self.directory, env=environment,
                                        stdout=self.log, stderr=self.log)
        deadline = time.monotonic() + 10
        try:
            while time.monotonic() < deadline:
                if self.process.poll() is not None:
                    raise RuntimeError(f"server exited {self.process.returncode}; {self.directory}")
                try:
                    with self.client() as client:
                        if client.ping():
                            return self
                except (redis.ConnectionError, redis.TimeoutError, OSError):
                    time.sleep(0.02)
            raise TimeoutError(f"startup deadline: {self.directory}")
        except BaseException:
            self.stop(kill=True)
            raise

    def stop(self, *, kill=False):
        try:
            if self.process and self.process.poll() is None:
                if kill:
                    self.process.kill()
                else:
                    self.process.terminate()
                try:
                    code = self.process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait()
                    raise
                if not kill and code != 0:
                    raise AssertionError(f"graceful shutdown returned {code}: {self.directory}")
        finally:
            if self.log:
                self.log.close()
                self.log = None

    def restart(self, *, kill=False):
        self.stop(kill=kill)
        return self.start()

    def client(self, protocol=2, db=0):
        if self.kind == "ratatosk" and self.unixsocket:
            return redis.Redis(unix_socket_path=str(self.unixsocket), db=db, protocol=protocol,
                               socket_connect_timeout=2, socket_timeout=3)
        return redis.Redis(host="127.0.0.1", port=self.port, db=db, protocol=protocol,
                           socket_connect_timeout=2, socket_timeout=3)

    def raw_connection(self):
        if self.kind == "ratatosk" and self.unixsocket:
            connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            connection.settimeout(3)
            connection.connect(str(self.unixsocket))
            return connection
        return socket.create_connection(("127.0.0.1", self.port), timeout=3)

    def __enter__(self):
        return self.start()

    def __exit__(self, *_):
        self.stop()


def wait_rewrite(client):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        info = client.info("persistence")
        if not info["aof_rewrite_in_progress"] and not info.get("aof_rewrite_scheduled", 0):
            status = info.get("aof_last_bgrewrite_status", info.get("aof_last_rewrite_status"))
            if status != "ok":
                raise AssertionError(f"AOF rewrite failed: {info}")
            return
        time.sleep(0.02)
    raise TimeoutError("AOF rewrite did not complete")


def cache(s):
    with s.client() as c:
        value = "검색 결과".encode() + b"\x00\xff\r\n"
        equal(c.set("cache", value, px=150), True)
        equal(c.get("cache"), value)
        time.sleep(0.2)
        equal(c.get("cache"), None)
        c.set("session", "old", px=5000)
        equal(c.set("session", "bad", nx=True), None)
        equal(c.set("session", "new", xx=True, keepttl=True), True)
        if not 0 < c.pttl("session") <= 5000:
            raise AssertionError("KEEPTTL lost deadline")


def concurrent_counter(s):
    def increment(_):
        with s.client() as c:
            return [c.incr("counter") for _ in range(100)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        actual = sum(pool.map(increment, range(12)), [])
    equal(sorted(actual), list(range(1, 1201)))


def numeric_strings(s):
    values = [b"0", b"42", b"-123", b"-9223372036854775808", b"9223372036854775807"]
    with s.client() as c:
        for i, value in enumerate(values):
            key = f"number:{i}"
            c.set(key, value, px=600000)
            deadline = c.pexpiretime(key)
            equal(c.strlen(key), len(value))
            equal(c.getrange(key, 0, -1), value)
            equal(c.bitcount(key), sum(byte.bit_count() for byte in value))
            equal(c.getbit(key, 0), 0)
            equal(c.execute_command("BITFIELD_RO", key, "GET", "u8", 0), [value[0]])
            equal(c.append(key, b"!"), len(value) + 1)
            equal(c.get(key), value + b"!")
            equal(c.pexpiretime(key), deadline)
        c.set("bits", "255", px=600000)
        deadline = c.pexpiretime("bits")
        equal(c.setbit("bits", 8, 1), 0)
        equal(c.get("bits"), b"2\xb55")
        equal(c.pexpiretime("bits"), deadline)
        expected = dataset(c)
    s.restart(kill=True)
    with s.client() as c:
        equal(dataset(c), expected)


def blocking_pop(s):
    with s.client() as consumer, s.client() as producer:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(consumer.blpop, "jobs", 2)
            time.sleep(0.05)
            equal(producer.rpush("jobs", "job"), 1)
            equal(pending.result(timeout=3), (b"jobs", b"job"))
    s.restart()
    with s.client() as c:
        equal(c.llen("jobs"), 0)


def transaction(s):
    with s.client() as c:
        with c.pipeline() as p:
            p.set("committed", "value")
            p.incr("counter")
            equal(p.execute(), [True, 1])
    s.restart(kill=True)
    with s.client() as c:
        equal(c.get("committed"), b"value")
        equal(c.get("counter"), b"1")


def transaction_error(s):
    with s.client() as c:
        c.set("wrongtype", "string")
        with c.pipeline() as p:
            p.set("good", "before")
            p.lpush("wrongtype", "bad")
            p.set("good", "after")
            replies = p.execute(raise_on_error=False)
            if not isinstance(replies[1], redis.ResponseError):
                raise AssertionError(replies)
    s.restart()
    with s.client() as c:
        equal(c.get("good"), b"after")
        equal(c.get("wrongtype"), b"string")


def transaction_databases(s):
    with s.client() as c:
        with c.pipeline() as p:
            p.set("db-key", "zero")
            p.execute_command("SELECT", 3)
            p.set("db-key", "three")
            p.execute_command("SELECT", 1)
            p.incr("counter")
            equal(p.execute(), [True, True, True, True, 1])
    s.restart(kill=True)
    with s.client() as zero, s.client(db=3) as three, s.client(db=1) as one:
        equal(zero.get("db-key"), b"zero")
        equal(three.get("db-key"), b"three")
        equal(one.get("counter"), b"1")
        zero.set("after-restart", "zero")
    s.restart(kill=True)
    with s.client() as zero, s.client(db=1) as one:
        equal(zero.get("after-restart"), b"zero")
        equal(one.get("after-restart"), None)


def torn_transaction(s):
    with s.client() as c:
        c.set("baseline", "present")
        with c.pipeline() as p:
            p.set("uncommitted-one", "one")
            p.set("uncommitted-two", "two")
            p.execute()
    s.stop()
    # Remove only the final commit frame from our private AOF, simulating a
    # transaction interrupted before its commit reached storage.
    commit = b"*1\r\n$4\r\nEXEC\r\n"
    candidates = [path for path in s.directory.rglob("*.aof")
                  if path.read_bytes().endswith(commit)]
    equal(len(candidates), 1)
    path = candidates[0]
    path.write_bytes(path.read_bytes()[:-len(commit)])
    s.start()
    with s.client() as c:
        equal(c.get("baseline"), b"present")
        equal(c.mget("uncommitted-one", "uncommitted-two"), [None, None])
        c.set("after-repair", "present")
        with c.pipeline() as p:
            p.set("next-transaction", "present")
            p.execute()
    s.restart(kill=True)
    with s.client() as c:
        equal(c.mget("uncommitted-one", "uncommitted-two"), [None, None])
        equal(c.mget("after-repair", "next-transaction"), [b"present", b"present"])


def concurrent_recovery(s):
    def append(worker):
        with s.client() as c:
            for i in range(50):
                c.rpush("ordered", f"{worker}:{i}")
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(append, range(8)))
    with s.client() as c:
        expected = c.lrange("ordered", 0, -1)
        equal(len(expected), 400)
    s.restart(kill=True)
    with s.client() as c:
        equal(c.lrange("ordered", 0, -1), expected)


def watch_abort(s):
    with s.client() as c, c.pipeline() as p:
        p.watch("watched")
        c.set("watched", "other")
        p.multi()
        p.set("aborted", "must-not-exist")
        try:
            p.execute()
        except redis.WatchError:
            pass
        else:
            raise AssertionError("WATCH conflict must raise WatchError")
    s.restart()
    with s.client() as c:
        equal(c.get("aborted"), None)


def ttl_recovery(s):
    with s.client() as c:
        c.set("short", "expired", px=150)
        c.set("long", "value", px=5000)
        deadline = c.pexpiretime("long")
    s.stop()
    time.sleep(0.2)
    s.start()
    with s.client() as c:
        equal(c.get("short"), None)
        equal(c.pexpiretime("long"), deadline)


def expiry_variants(s):
    with s.client() as c:
        c.psetex("psetex", 150, "expired")
        c.set("expire", "expired")
        c.pexpire("expire", 150)
        c.set("getex", "expired")
        equal(c.getex("getex", px=150), b"expired")
    s.stop()
    time.sleep(0.2)
    s.start()
    with s.client() as c:
        equal(c.mget("psetex", "expire", "getex"), [None, None, None])


def streams(s):
    with s.client() as c:
        first = c.xadd("events", {"event": "ready"})
        second = c.xadd("events", {"event": "next"}, id="9999999999999-*")
    s.restart()
    with s.client() as c:
        equal([entry[0] for entry in c.xrange("events")], [first, second])


def snapshot_overlap(s):
    with s.client() as c:
        equal(c.incr("counter"), 1)
        equal(c.rpush("queue", "task"), 1)
        c.save()
    for _ in range(2):
        s.restart()
        with s.client() as c:
            equal(c.get("counter"), b"1")
            equal(c.lrange("queue", 0, -1), [b"task"])


def rewrite(s):
    with s.client() as c:
        c.incr("counter")
        c.set("expired", "old", px=150)
        identifier = c.xadd("events", {"a": "b"})
        c.bgrewriteaof()
        wait_rewrite(c)
        c.incr("counter")
        c.save()
    s.stop()
    time.sleep(0.2)
    s.start()
    with s.client() as c:
        equal(c.get("counter"), b"2")
        equal(c.get("expired"), None)
        equal(c.xrange("events"), [(identifier, {b"a": b"b"})])


def concurrent_rewrite(s):
    def append(worker):
        with s.client() as c:
            for i in range(100):
                c.rpush("ordered", f"{worker}:{i}")
    with s.client() as c:
        c.set("payload", b"x" * 1_000_000)
        for _ in range(3):
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                pending = [pool.submit(append, worker) for worker in range(4)]
                c.bgrewriteaof()
                for future in pending:
                    future.result(timeout=30)
                wait_rewrite(c)
        expected = c.lrange("ordered", 0, -1)
        equal(len(expected), 1200)
    s.restart(kill=True)
    with s.client() as c:
        equal(c.lrange("ordered", 0, -1), expected)


def runtime_enable(s):
    with s.client() as c:
        c.set("before", "value")
        equal(c.config_set("appendonly", "yes"), True)
        equal(c.info("persistence")["aof_enabled"], 1)
        c.set("after", "value")
        wait_rewrite(c)
    s.stop()
    s.aof = True
    s.start()
    with s.client() as c:
        equal(c.mget("before", "after"), [b"value", b"value"])


def runtime_disable_enable(s):
    with s.client() as c:
        c.set("state", "old")
        equal(c.config_set("appendonly", "no"), True)
        equal(c.info("persistence")["aof_enabled"], 0)
        c.set("state", "new")
        equal(c.config_set("appendonly", "yes"), True)
        wait_rewrite(c)
    s.restart()
    with s.client() as c:
        equal(c.get("state"), b"new")


def everysec_idle(s):
    s.stop()
    s.extra["RATATOSK_APPENDFSYNC"] = "everysec"
    s.start()
    with s.client() as c:
        c.config_set("appendfsync", "everysec")
        c.set("idle-write", "durable")
    time.sleep(1.4)
    s.restart(kill=True)
    with s.client() as c:
        equal(c.get("idle-write"), b"durable")


def runtime_enable_shutdown(s):
    with s.client() as c:
        c.config_set("appendfsync", "everysec")
        c.config_set("appendonly", "yes")
        wait_rewrite(c)
        c.set("shutdown-write", "durable")
    s.stop()
    s.aof = True
    s.start()
    with s.client() as c:
        equal(c.get("shutdown-write"), b"durable")


def dataset(client):
    result = {}
    for key in sorted(client.keys("*")):
        kind = client.type(key)
        if kind == b"string":
            value = client.get(key)
        elif kind == b"hash":
            value = sorted(client.hgetall(key).items())
        elif kind == b"list":
            value = client.lrange(key, 0, -1)
        elif kind == b"set":
            value = sorted(client.smembers(key))
        elif kind == b"zset":
            value = client.zrange(key, 0, -1, withscores=True)
        elif kind == b"stream":
            value = client.xrange(key)
        else:
            raise AssertionError(f"unexpected key type {kind!r}")
        result[key] = (kind, value, client.pexpiretime(key))
    return result


def mixed_recovery(s):
    rng = random.Random(20260906)
    with s.client() as c:
        c.sadd("set", *(str(i) for i in range(100)))
        for i in range(300):
            index = rng.randrange(9)
            if index == 0:
                c.set("string", str(i), px=600000, nx=bool(i % 2))
            elif index == 1:
                c.append("string", ".")
            elif index == 2:
                c.hset("hash", str(i % 7), str(i))
            elif index == 3:
                c.hdel("hash", str(i % 7))
            elif index == 4:
                c.rpush("list", str(i))
            elif index == 5:
                c.lpop("list")
            elif index == 6:
                c.spop("set")
            elif index == 7:
                c.zadd("sorted", {str(i % 11): i})
            else:
                c.xadd("events", {"sequence": str(i)})
        expected = dataset(c)
    s.restart(kill=True)
    with s.client() as c:
        equal(dataset(c), expected)


def hello2(s):
    with s.client() as c:
        response = c.execute_command("HELLO", 2)
        if not isinstance(response, list):
            raise AssertionError(response)
        equal(c.ping(), True)


def hash3(s):
    with s.client(protocol=3) as c:
        c.hset("hash", "a", "b")
        equal(c.hgetall("hash"), {b"a": b"b"})
        with c.pipeline() as p:
            p.hgetall("hash")
            equal(p.execute(), [{b"a": b"b"}])


def pubsub(s, protocol):
    with s.client(protocol=protocol) as c, c.pubsub() as subscriber:
        subscriber.subscribe("one", "two")
        replies = [subscriber.get_message(timeout=2), subscriber.get_message(timeout=2)]
        equal([r["channel"] if r else None for r in replies], [b"one", b"two"])
        for i in range(100):
            equal(c.publish("one", str(i)), 1)
        for i in range(100):
            reply = subscriber.get_message(timeout=2)
            equal(reply["data"] if reply else None, str(i).encode())
        subscriber.unsubscribe("one", "two")
        replies = [subscriber.get_message(timeout=2), subscriber.get_message(timeout=2)]
        equal([r["type"] if r else None for r in replies], ["unsubscribe", "unsubscribe"])


def pubsub_transaction(s):
    # Redis keeps one EXEC slot per command while SUBSCRIBE writes one ACK per
    # channel. Exercise the actual wire output, including this special case.
    expected = (b"+OK\r\n+QUEUED\r\n*1\r\n"
                b"*3\r\n$9\r\nsubscribe\r\n$1\r\na\r\n:1\r\n"
                b"*3\r\n$9\r\nsubscribe\r\n$1\r\nb\r\n:2\r\n")
    with s.raw_connection() as connection:
        connection.sendall(b"MULTI\r\nSUBSCRIBE a b\r\nEXEC\r\n")
        with connection.makefile("rb") as reader:
            equal(reader.read(len(expected)), expected)


def pubsub_counts(s):
    checks = [(b"SUBSCRIBE a", b"subscribe", b"a", 1),
              (b"PSUBSCRIBE p", b"psubscribe", b"p", 2),
              (b"SSUBSCRIBE s", b"ssubscribe", b"s", 1),
              (b"UNSUBSCRIBE", b"unsubscribe", b"a", 1),
              (b"UNSUBSCRIBE", b"unsubscribe", None, 1),
              (b"PUNSUBSCRIBE", b"punsubscribe", b"p", 0),
              (b"SUNSUBSCRIBE", b"sunsubscribe", b"s", 0)]
    def bulk(value):
        return b"$-1\r\n" if value is None else b"$%d\r\n%s\r\n" % (len(value), value)
    with s.raw_connection() as connection:
        with connection.makefile("rb") as reader:
            for command, kind, target, count in checks:
                expected = b"*3\r\n" + bulk(kind) + bulk(target) + b":%d\r\n" % count
                connection.sendall(command + b"\r\n")
                equal(reader.read(len(expected)), expected)


def expiry_dependencies(s):
    with s.client() as c:
        c.set("expired-counter", 1, px=300)
        equal(c.incr("expired-counter"), 2)
        c.hset("extended-hash", "a", 1)
        c.pexpire("extended-hash", 300)
        c.hset("extended-hash", "b", 2)
        c.persist("extended-hash")
        c.rpush("extended-list", "a")
        c.pexpire("extended-list", 300)
        c.rpush("extended-list", "b")
        c.pexpire("extended-list", 600000)
        c.set("reused", 99, px=100)
        time.sleep(0.15)
        equal(c.incr("reused"), 1)
        time.sleep(0.2)
    s.restart(kill=True)
    with s.client() as c:
        equal(c.get("expired-counter"), None)
        equal(c.hgetall("extended-hash"), {b"a": b"1", b"b": b"2"})
        equal(c.lrange("extended-list", 0, -1), [b"a", b"b"])
        equal(c.get("reused"), b"1")


def runtime_exec_enable(s):
    with s.client() as c:
        c.set("counter", 1)
        with c.pipeline() as transaction:
            transaction.incr("counter")
            transaction.config_set("appendonly", "yes")
            transaction.config_set("appendfsync", "always")
            transaction.incr("counter")
            equal(transaction.execute(), [2, True, True, 3])
        equal(c.info("persistence")["aof_enabled"], 1)
        wait_rewrite(c)
    s.aof = True
    s.restart(kill=True)
    with s.client() as c:
        equal(c.get("counter"), b"3")


def snapshot_stream_groups(s):
    with s.client() as c:
        first = c.xadd("stream", {"field": "one"})
        second = c.xadd("stream", {"field": "two"})
        c.xgroup_create("stream", "group", "0")
        c.xreadgroup("group", "consumer", {"stream": ">"}, count=1)
        c.xgroup_createconsumer("stream", "group", "idle-consumer")
        before = c.xpending("stream", "group")
        c.bgrewriteaof()
        wait_rewrite(c)
    s.restart(kill=True)
    with s.client() as c:
        equal(c.xpending("stream", "group"), before)
        equal(sorted(item["name"] for item in c.xinfo_consumers("stream", "group")),
              [b"consumer", b"idle-consumer"])
        equal(c.xreadgroup("group", "consumer", {"stream": "0"}),
              [[b"stream", [(first, {b"field": b"one"})]]])
        equal(c.xreadgroup("group", "consumer", {"stream": ">"}),
              [[b"stream", [(second, {b"field": b"two"})]]])
        equal(c.xack("stream", "group", first, second), 2)


CASES = [
    ("cache", False, cache),
    ("concurrent_counter", False, concurrent_counter),
    ("numeric_strings", True, numeric_strings),
    ("blocking_pop", True, blocking_pop),
    ("transaction", True, transaction),
    ("transaction_error", True, transaction_error),
    ("transaction_databases", True, transaction_databases),
    ("torn_transaction", True, torn_transaction),
    ("concurrent_recovery", True, concurrent_recovery),
    ("watch_abort", True, watch_abort),
    ("ttl_recovery", True, ttl_recovery),
    ("expiry_variants", True, expiry_variants),
    ("expiry_dependencies", True, expiry_dependencies),
    ("snapshot_stream_groups", True, snapshot_stream_groups),
    ("streams", True, streams),
    ("snapshot_overlap", True, snapshot_overlap),
    ("rewrite", True, rewrite),
    ("concurrent_rewrite", True, concurrent_rewrite),
    ("runtime_enable", False, runtime_enable),
    ("runtime_exec_enable", False, runtime_exec_enable),
    ("runtime_disable_enable", True, runtime_disable_enable),
    ("everysec_idle", True, everysec_idle),
    ("runtime_enable_shutdown", False, runtime_enable_shutdown),
    ("mixed_recovery", True, mixed_recovery),
    ("hello2", False, hello2),
    ("hash3", False, hash3),
    ("pubsub2", False, lambda s: pubsub(s, 2)),
    ("pubsub3", False, lambda s: pubsub(s, 3)),
    ("pubsub_transaction", False, pubsub_transaction),
    ("pubsub_counts", False, pubsub_counts),
]


def binary(value):
    path = Path(shutil.which(value) or value).resolve()
    if not path.is_file() or not os.access(path, os.X_OK):
        raise argparse.ArgumentTypeError(f"executable not found: {value}")
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ratatosk", required=True, type=binary)
    parser.add_argument("--redis-server", required=True, type=binary)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument(
        "--unixsocket",
        type=Path,
        help="Connect to Ratatosk over this Unix socket while Redis continues to use TCP.",
    )
    parser.add_argument("--case", nargs="+", choices=[name for name, _, _ in CASES])
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    ratatosk_unixsocket = args.unixsocket.resolve() if args.unixsocket else None
    results = []
    for kind, executable in [("redis", args.redis_server), ("ratatosk", args.ratatosk)]:
        for name, aof, function in CASES:
            if args.case and name not in args.case:
                continue
            row = {"server": kind, "case": name, "status": "PASS"}
            try:
                unixsocket = ratatosk_unixsocket if kind == "ratatosk" else None
                with Server(executable, kind, args.output_dir, aof=aof,
                            unixsocket=unixsocket) as server:
                    function(server)
                row["artifacts"] = str(server.directory)
            except Exception as error:
                row.update(status="FAIL", error=str(error), traceback=traceback.format_exc())
            results.append(row)
            print(json.dumps(row), flush=True)
    (args.output_dir / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    return int(any(row["status"] == "FAIL" for row in results))


if __name__ == "__main__":
    raise SystemExit(main())
