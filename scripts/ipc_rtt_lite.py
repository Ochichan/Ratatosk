"""Minimal lockstep PING round-trip latency probe (Python stdlib only).

Usage:
    python3 scripts/ipc_rtt_lite.py tcp 127.0.0.1:6380 [samples]
    python3 scripts/ipc_rtt_lite.py unix /path/to/ratatosk.sock [samples]

Prints one JSON line with p50/p95/p99/p99.9/max in microseconds. It is a quick
sanity check; `cargo run -p ratatosk-ipc-bench` is the real harness.
"""

import json
import socket
import sys
import time

REQUEST = b"*1\r\n$4\r\nPING\r\n"
REPLY = b"+PONG\r\n"
WARMUP = 2000


def connect(mode: str, target: str) -> socket.socket:
    if mode == "tcp":
        host, port = target.rsplit(":", 1)
        sock = socket.create_connection((host, int(port)))
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return sock
    if mode == "unix":
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(target)
        return sock
    raise SystemExit(f"unknown mode {mode!r}; expected 'tcp' or 'unix'")


def round_trip(sock: socket.socket) -> None:
    sock.sendall(REQUEST)
    reply = b""
    while not reply.endswith(b"\r\n"):
        chunk = sock.recv(64)
        if not chunk:
            raise SystemExit("server closed the connection")
        reply += chunk
    if reply != REPLY:
        raise SystemExit(f"unexpected reply {reply!r}")


def main() -> None:
    if len(sys.argv) < 3:
        raise SystemExit(__doc__)
    mode, target = sys.argv[1], sys.argv[2]
    samples = int(sys.argv[3]) if len(sys.argv) > 3 else 20000

    with connect(mode, target) as sock:
        for _ in range(WARMUP):
            round_trip(sock)
        latencies = []
        for _ in range(samples):
            started = time.perf_counter_ns()
            round_trip(sock)
            latencies.append(time.perf_counter_ns() - started)

    latencies.sort()

    def quantile(p: float) -> float:
        return latencies[min(len(latencies) - 1, int(p * len(latencies)))] / 1e3

    print(
        json.dumps(
            {
                "mode": mode,
                "n": samples,
                "p50_us": quantile(0.5),
                "p95_us": quantile(0.95),
                "p99_us": quantile(0.99),
                "p999_us": quantile(0.999),
                "max_us": latencies[-1] / 1e3,
            }
        )
    )


if __name__ == "__main__":
    main()
