#!/usr/bin/env python3
"""Check Ratatosk benchmark logs against guardrails."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

SET_BENCH = "pipeline_set_parse_execute_encode/256"
PING_BENCH = "pipeline_ping_parse_execute_encode/256"

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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", required=True, help="Path to benchmark log file")
    parser.add_argument("--set-guard-us", type=float, default=110.0)
    parser.add_argument("--ping-guard-us", type=float, default=35.0)
    args = parser.parse_args()

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
