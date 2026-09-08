#!/usr/bin/env python3
"""Check Ratatosk benchmark logs against guardrails."""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from pathlib import Path

SET_BENCH = "pipeline_set_parse_execute_encode/256"
PING_BENCH = "pipeline_ping_parse_execute_encode/256"

IPC_DEFAULT_P99_US = {
    ("ping", "tcp"): 60.0,
    ("ping", "unix"): 50.0,
    ("ping", "shm"): 20.0,
}
IPC_TRANSPORT_FALLBACK_P99_US = {"shm": 40.0}
IPC_FALLBACK_P99_US = 100.0

TIME_RE = re.compile(r"time:\s*\[\s*([0-9.]+)\s*([a-zµ]+)\s+[0-9.]+\s*[a-zµ]+\s+([0-9.]+)\s*([a-zµ]+)\s*\]")


def to_us(value: float, unit: str) -> float:
    if unit == "ns":
        return value / 1000.0
    if unit in {"µs", "us"}:
        return value
    if unit == "ms":
        return value * 1000.0
    raise ValueError(f"unsupported unit: {unit}")


def parse_upper_us(log_text: str, bench_name: str) -> float:
    idx = log_text.find(bench_name)
    if idx < 0:
        raise ValueError(f"benchmark not found: {bench_name}")

    chunk = log_text[idx : idx + 500]
    m = TIME_RE.search(chunk)
    if not m:
        raise ValueError(f"time range not found for benchmark: {bench_name}")

    upper = float(m.group(3))
    unit = m.group(4)
    return to_us(upper, unit)


def ipc_guard_us(payload: str, transport: str, override: float | None) -> float:
    if override is not None:
        return override
    return IPC_DEFAULT_P99_US.get(
        (payload, transport),
        IPC_TRANSPORT_FALLBACK_P99_US.get(transport, IPC_FALLBACK_P99_US),
    )


def ipc_rows(document: object) -> list[dict[str, object]]:
    if not isinstance(document, dict):
        raise ValueError("IPC artifact must be a JSON object")

    rows = document.get("results")
    if not isinstance(rows, list):
        # Accept the legacy nested rows shape for locally saved pre-contract artifacts.
        rows = document.get("rows")
    if not isinstance(rows, list):
        raise ValueError("IPC artifact must contain a 'results' array")

    parsed_rows: list[dict[str, object]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(f"IPC row {index} must be an object")
        parsed_rows.append(row)
    return parsed_rows


def ipc_row_values(row: dict[str, object], index: int) -> tuple[str, str, float]:
    config = row.get("config")
    measurements = row.get("results")
    if isinstance(config, dict):
        transport = config.get("transport")
        payload = config.get("payload")
        p99 = measurements.get("p99_us") if isinstance(measurements, dict) else None
    else:
        transport = row.get("transport")
        payload = row.get("payload")
        p99 = row.get("p99_us")

    if not isinstance(transport, str) or not transport:
        raise ValueError(f"IPC row {index} has no transport")
    if not isinstance(payload, str) or not payload:
        raise ValueError(f"IPC row {index} has no payload")
    if isinstance(p99, bool) or not isinstance(p99, (int, float)) or not math.isfinite(p99):
        raise ValueError(f"IPC row {index} has no finite p99_us")

    return transport, payload, float(p99)


def ipc_row_concurrency_value(row: dict[str, object]) -> object:
    config = row.get("config")
    return config.get("concurrency") if isinstance(config, dict) else row.get("concurrency")


def ipc_row_concurrency(row: dict[str, object], index: int) -> int:
    concurrency = ipc_row_concurrency_value(row)
    if isinstance(concurrency, bool) or not isinstance(concurrency, int) or concurrency < 1:
        raise ValueError(f"IPC row {index} has no positive integer concurrency")
    return concurrency


def ipc_concurrency_label(row: dict[str, object]) -> str:
    concurrency = ipc_row_concurrency_value(row)
    if isinstance(concurrency, int) and not isinstance(concurrency, bool):
        return str(concurrency)
    return "invalid"


def check_ipc(path: Path, override: float | None) -> int:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
        rows = ipc_rows(document)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        print(f"[perf] FAIL: invalid IPC artifact: {exc}", file=sys.stderr)
        return 1

    ok = True
    gated_rows = 0
    for index, row in enumerate(rows):
        try:
            concurrency = ipc_row_concurrency(row, index)
        except ValueError as exc:
            print(
                f"[perf] FAIL: invalid IPC artifact row={index} "
                f"conns={ipc_concurrency_label(row)}: {exc}",
                file=sys.stderr,
            )
            return 1

        status = row.get("status", "ok")
        if status == "skipped":
            reason = row.get("reason", "no reason given")
            print(f"[perf] ipc row={index} conns={concurrency} skipped: {reason}")
            continue
        if status != "ok":
            print(
                f"[perf] FAIL: IPC row={index} conns={concurrency} "
                f"has unsupported status: {status}"
            )
            ok = False
            continue

        try:
            transport, payload, p99 = ipc_row_values(row, index)
        except ValueError as exc:
            print(
                f"[perf] FAIL: invalid IPC artifact row={index} conns={concurrency}: {exc}",
                file=sys.stderr,
            )
            return 1

        server = row.get("server", "unknown")
        if concurrency != 1:
            print(
                f"[perf] ipc server={server} payload={payload} transport={transport} "
                f"conns={concurrency} p99={p99:.3f}us guard=not-gated"
            )
            continue

        guard = ipc_guard_us(payload, transport, override)
        print(
            f"[perf] ipc server={server} payload={payload} transport={transport} "
            f"conns={concurrency} p99={p99:.3f}us guard={guard:.3f}us"
        )
        gated_rows += 1
        if p99 > guard:
            print(
                f"[perf] FAIL: IPC {server} {payload}/{transport} conns={concurrency} "
                "p99 above guardrail"
            )
            ok = False

    if gated_rows == 0:
        print("[perf] ipc: no completed single-connection rows to check")
    if ok:
        print("[perf] OK")
        return 0
    return 1


def main() -> int:
    parser = argparse.ArgumentParser()
    input_mode = parser.add_mutually_exclusive_group(required=True)
    input_mode.add_argument("--log", help="Path to Criterion benchmark log file")
    input_mode.add_argument("--ipc", help="Path to ratatosk-ipc-bench JSON artifact")
    parser.add_argument("--set-guard-us", type=float, default=110.0)
    parser.add_argument("--ping-guard-us", type=float, default=35.0)
    parser.add_argument(
        "--ipc-p99-us-max",
        type=float,
        help="Override every IPC p99 guardrail in microseconds",
    )
    args = parser.parse_args()

    if args.ipc is not None:
        if (
            args.ipc_p99_us_max is not None
            and (not math.isfinite(args.ipc_p99_us_max) or args.ipc_p99_us_max < 0.0)
        ):
            parser.error("--ipc-p99-us-max must be a finite non-negative number")
        return check_ipc(Path(args.ipc), args.ipc_p99_us_max)

    text = Path(args.log).read_text(encoding="utf-8")
    set_upper = parse_upper_us(text, SET_BENCH)
    ping_upper = parse_upper_us(text, PING_BENCH)

    print(f"[perf] set@256 upper={set_upper:.3f}us guard={args.set_guard_us:.3f}us")
    print(f"[perf] ping@256 upper={ping_upper:.3f}us guard={args.ping_guard_us:.3f}us")

    ok = True
    if set_upper > args.set_guard_us:
        print("[perf] FAIL: set@256 above guardrail")
        ok = False
    if ping_upper > args.ping_guard_us:
        print("[perf] FAIL: ping@256 above guardrail")
        ok = False

    if ok:
        print("[perf] OK")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
