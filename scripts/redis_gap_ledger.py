#!/usr/bin/env python3
"""Generate and validate Redis command gap ledger artifacts.

This tool uses Redis' `src/commands/*.json` metadata to build a stable command
catalog, then merges that catalog with Ratatosk's implementation-tracking ledger.
The JSON ledger is the source of truth; Markdown is derived output.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from difflib import unified_diff
from pathlib import Path
from typing import Any


VALID_STATUSES = (
    "planned",
    "in_progress",
    "partial",
    "done",
    "excluded",
)
VALID_CAPABILITY_TIERS = (
    "unsupported",
    "syntax_only",
    "baseline_local",
    "behavioral_subset",
    "distributed_parity",
)

DEFAULT_STATUS = "planned"
DEFAULT_CAPABILITY_TIER = "unsupported"
DEFAULT_MILESTONE = "backlog"

LEDGER_SCHEMA_VERSION = 2


@dataclass(frozen=True)
class CatalogCommand:
    name: str
    group: str
    since: str
    arity: int | None
    path: str
    summary: str

    def to_json(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "group": self.group,
            "since": self.since,
            "arity": self.arity,
            "path": self.path,
            "summary": self.summary,
        }


def _json_load(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _json_dump(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=False)
        f.write("\n")


def _normalize_full_name(name: str, container: str | None) -> str:
    if container:
        full = f"{container.strip()} {name.strip()}"
    else:
        full = name.strip()
    return " ".join(full.split()).upper()


def _load_redis_commands(redis_dir: Path) -> list[CatalogCommand]:
    commands_dir = redis_dir / "src" / "commands"
    if not commands_dir.is_dir():
        # Allow passing the commands directory directly.
        commands_dir = redis_dir
    if not commands_dir.is_dir():
        raise FileNotFoundError(
            f"Redis commands directory not found: {redis_dir}"
        )

    files = sorted(commands_dir.glob("*.json"))
    if not files:
        raise FileNotFoundError(f"No command JSON files found in {commands_dir}")

    out: list[CatalogCommand] = []
    for file_path in files:
        payload = _json_load(file_path)
        if not isinstance(payload, dict):
            raise ValueError(f"Expected object in {file_path}")

        for name, spec in payload.items():
            if not isinstance(spec, dict):
                raise ValueError(f"Invalid command spec for {name} in {file_path}")

            container = spec.get("container")
            full_name = _normalize_full_name(str(name), str(container) if container else None)
            group = str(spec.get("group", "unknown"))
            since = str(spec.get("since", "unknown"))
            arity_raw = spec.get("arity")
            arity = int(arity_raw) if isinstance(arity_raw, int) else None
            summary = str(spec.get("summary", ""))

            out.append(
                CatalogCommand(
                    name=full_name,
                    group=group,
                    since=since,
                    arity=arity,
                    path=file_path.name,
                    summary=summary,
                )
            )

    dedup: dict[str, CatalogCommand] = {}
    duplicates: list[str] = []
    for item in out:
        if item.name in dedup:
            duplicates.append(item.name)
            continue
        dedup[item.name] = item
    if duplicates:
        dup_list = ", ".join(sorted(set(duplicates)))
        raise ValueError(f"Duplicate command names after normalization: {dup_list}")

    return sorted(dedup.values(), key=lambda c: c.name)


def _validate_catalog(catalog_data: dict[str, Any]) -> list[dict[str, Any]]:
    commands = catalog_data.get("commands")
    if not isinstance(commands, list):
        raise ValueError("Catalog must contain `commands` array")

    names: set[str] = set()
    for item in commands:
        if not isinstance(item, dict):
            raise ValueError("Catalog command entry must be object")
        for key in ("name", "group", "since", "path", "summary"):
            if key not in item:
                raise ValueError(f"Catalog command missing `{key}`")
        name = str(item["name"])
        if name in names:
            raise ValueError(f"Duplicate command in catalog: {name}")
        names.add(name)

    return sorted(commands, key=lambda x: str(x["name"]))


def _validate_ledger(ledger_data: dict[str, Any]) -> list[dict[str, Any]]:
    schema_version = ledger_data.get("schema_version")
    if schema_version not in (1, LEDGER_SCHEMA_VERSION):
        raise ValueError(
            f"Unsupported ledger schema_version={schema_version}; expected 1 or {LEDGER_SCHEMA_VERSION}"
        )

    commands = ledger_data.get("commands")
    if not isinstance(commands, list):
        raise ValueError("Ledger must contain `commands` array")

    names: set[str] = set()
    for item in commands:
        if not isinstance(item, dict):
            raise ValueError("Ledger command entry must be object")
        for key in ("name", "group", "since", "status", "milestone"):
            if key not in item:
                raise ValueError(f"Ledger command missing `{key}`")
        name = str(item["name"])
        if name in names:
            raise ValueError(f"Duplicate command in ledger: {name}")
        names.add(name)

        status = str(item["status"])
        if status not in VALID_STATUSES:
            raise ValueError(f"Invalid status `{status}` for command `{name}`")

        if not str(item["milestone"]).strip():
            raise ValueError(f"Empty milestone for command `{name}`")

        capability_tier = str(
            item.get(
                "capability_tier",
                _infer_capability_tier(status, name, str(item.get("notes", ""))),
            )
        )
        if capability_tier not in VALID_CAPABILITY_TIERS:
            raise ValueError(
                f"Invalid capability_tier `{capability_tier}` for command `{name}`"
            )

    return sorted(commands, key=lambda x: str(x["name"]))


def _is_prefixed_command(name: str, prefix: str) -> bool:
    return name == prefix or name.startswith(f"{prefix} ")


def _infer_capability_tier(status: str, name: str, notes: str) -> str:
    if status != "done":
        return DEFAULT_CAPABILITY_TIER

    upper_name = " ".join(name.split()).upper()
    lower_notes = notes.lower()

    unsupported_commands = {
        "SYNC",
        "SENTINEL",
        "EVAL",
        "EVALSHA",
        "EVAL_RO",
        "EVALSHA_RO",
        "FCALL",
        "FCALL_RO",
        "FUNCTION LOAD",
        "FUNCTION DELETE",
        "FUNCTION RESTORE",
    }
    syntax_only_commands = {
        "ASKING",
        "READONLY",
        "READWRITE",
        "MONITOR",
        "ROLE",
        "WAIT",
        "WAITAOF",
        "CLIENT PAUSE",
        "CLIENT UNPAUSE",
        "CLIENT UNBLOCK",
        "CLIENT SETINFO",
        "CLIENT REPLY",
        "HOTKEYS",
        "HOTKEYS GET",
        "HOTKEYS RESET",
        "HOTKEYS START",
        "HOTKEYS STOP",
        "FUNCTION",
        "FUNCTION HELP",
        "FUNCTION LIST",
        "FUNCTION DUMP",
        "FUNCTION FLUSH",
        "FUNCTION STATS",
        "SCRIPT",
        "SCRIPT HELP",
        "SCRIPT FLUSH",
        "ACL LOAD",
        "ACL SAVE",
        "ACL DRYRUN",
        "LOLWUT",
        "TRIMSLOTS",
    }
    baseline_local_commands = {
        "REPLCONF",
        "PSYNC",
        "REPLICAOF",
        "SLAVEOF",
        "CLIENT",
        "CLIENT CACHING",
        "CLIENT GETREDIR",
        "CLIENT INFO",
        "CLIENT KILL",
        "CLIENT LIST",
        "CLIENT NO-EVICT",
        "CLIENT NO-TOUCH",
        "CLIENT TRACKING",
        "CLIENT TRACKINGINFO",
        "CLUSTER",
        "CLUSTER COUNTKEYSINSLOT",
        "CLUSTER GETKEYSINSLOT",
        "CLUSTER HELP",
        "CLUSTER INFO",
        "CLUSTER KEYSLOT",
        "CLUSTER MYID",
        "CONFIG",
        "CONFIG GET",
        "CONFIG HELP",
        "CONFIG RESETSTAT",
        "CONFIG REWRITE",
        "CONFIG SET",
        "INFO",
        "LATENCY",
        "LATENCY DOCTOR",
        "LATENCY GRAPH",
        "LATENCY HELP",
        "LATENCY HISTOGRAM",
        "LATENCY HISTORY",
        "LATENCY LATEST",
        "LATENCY RESET",
        "MEMORY",
        "MEMORY DOCTOR",
        "MEMORY HELP",
        "MEMORY MALLOC-STATS",
        "MEMORY PURGE",
        "MEMORY STATS",
        "MEMORY USAGE",
    }
    behavioral_subset_commands = {
        "BGSAVE",
        "BGREWRITEAOF",
        "BLMOVE",
        "BLMPOP",
        "BLPOP",
        "BRPOP",
        "BRPOPLPUSH",
        "FLUSHALL",
        "FLUSHDB",
        "FUNCTION KILL",
        "SAVE",
        "XREAD",
        "XREADGROUP",
    }

    if upper_name in unsupported_commands:
        return "unsupported"
    if _is_prefixed_command(upper_name, "SENTINEL"):
        return "syntax_only" if upper_name == "SENTINEL HELP" else "unsupported"
    if _is_prefixed_command(upper_name, "CLUSTER") and upper_name not in {
        "CLUSTER COUNTKEYSINSLOT",
        "CLUSTER GETKEYSINSLOT",
        "CLUSTER HELP",
        "CLUSTER INFO",
        "CLUSTER KEYSLOT",
        "CLUSTER MYID",
    }:
        return "unsupported"
    if upper_name in syntax_only_commands:
        return "syntax_only"
    if upper_name in baseline_local_commands:
        return "baseline_local"
    if upper_name in behavioral_subset_commands:
        return "behavioral_subset"

    unsupported_markers = (
        "not supported",
        "unsupported",
        "support disabled",
        "not configured as a sentinel",
    )
    syntax_only_markers = (
        "no-op",
        "deterministic 0",
        "standalone wait parsing",
        "standalone waitaof parsing",
        "syntax/arity validation",
        "static ascii-text",
        "returns empty sample list",
        "coarse latency",
        "event summary graph",
        "single-connection",
    )
    baseline_markers = (
        "local state toggle",
        "client-tracking baseline",
        "standalone baseline",
        "in-memory acknowledge path",
        "single-connection info string",
    )
    subset_markers = (
        "polling",
        "core baseline",
        "background",
        "fanout",
        "implemented.",
    )

    if any(marker in lower_notes for marker in unsupported_markers):
        return "unsupported"
    if any(marker in lower_notes for marker in syntax_only_markers):
        return "syntax_only"
    if any(marker in lower_notes for marker in baseline_markers):
        return "baseline_local"
    if any(marker in lower_notes for marker in subset_markers):
        return "behavioral_subset"

    return "behavioral_subset"


def _split_markdown_row(line: str) -> list[str]:
    if not line.startswith("|") or not line.endswith("|"):
        raise ValueError(f"Invalid markdown table row: {line}")

    cells: list[str] = []
    current: list[str] = []
    escaped = False
    for ch in line[1:-1]:
        if escaped:
            current.append(ch)
            escaped = False
            continue
        if ch == "\\":
            escaped = True
            continue
        if ch == "|":
            cells.append("".join(current).strip())
            current = []
            continue
        current.append(ch)
    cells.append("".join(current).strip())
    return cells


def _parse_markdown_ledger(markdown_path: Path) -> dict[str, Any]:
    lines = markdown_path.read_text(encoding="utf-8").splitlines()
    header_kind: str | None = None
    start = 0

    for idx, line in enumerate(lines):
        if line.startswith("| Command | Group | Since | Status | Milestone | Notes |"):
            header_kind = "legacy"
            start = idx + 2
            break
        if line.startswith(
            "| Command | Group | Since | Status | Tier | Milestone | Notes |"
        ):
            header_kind = "tiered"
            start = idx + 2
            break

    if header_kind is None:
        raise ValueError(
            f"Could not find command ledger table in markdown file {markdown_path}"
        )

    commands: list[dict[str, Any]] = []
    for line in lines[start:]:
        if not line.startswith("|"):
            break
        cells = _split_markdown_row(line)
        if header_kind == "legacy":
            if len(cells) != 6:
                raise ValueError(f"Expected 6 cells in legacy row: {line}")
            command, group, since, status, milestone, notes = cells
            capability_tier = _infer_capability_tier(status, command.strip("`"), notes)
        else:
            if len(cells) != 7:
                raise ValueError(f"Expected 7 cells in tiered row: {line}")
            command, group, since, status, capability_tier, milestone, notes = cells

        commands.append(
            {
                "name": command.strip().strip("`"),
                "group": group,
                "since": since,
                "status": status,
                "capability_tier": capability_tier,
                "milestone": milestone,
                "owner": "",
                "notes": notes.replace("<br>", "\n"),
            }
        )

    return {
        "schema_version": LEDGER_SCHEMA_VERSION,
        "status_values": list(VALID_STATUSES),
        "capability_tiers": list(VALID_CAPABILITY_TIERS),
        "commands": commands,
    }


def _merge_ledger(
    catalog_commands: list[dict[str, Any]],
    existing_ledger: dict[str, Any] | None,
) -> dict[str, Any]:
    existing_map: dict[str, dict[str, Any]] = {}
    if existing_ledger:
        for entry in _validate_ledger(existing_ledger):
            existing_map[str(entry["name"])] = entry

    merged_commands: list[dict[str, Any]] = []
    for command in catalog_commands:
        name = str(command["name"])
        prev = existing_map.get(name, {})

        status = str(prev.get("status", DEFAULT_STATUS))
        if status not in VALID_STATUSES:
            status = DEFAULT_STATUS
        notes = str(prev.get("notes", ""))
        capability_tier = str(
            prev.get(
                "capability_tier",
                _infer_capability_tier(status, name, notes),
            )
        )
        if capability_tier not in VALID_CAPABILITY_TIERS:
            capability_tier = _infer_capability_tier(status, name, notes)

        merged_commands.append(
            {
                "name": name,
                "group": str(command["group"]),
                "since": str(command["since"]),
                "arity": command.get("arity"),
                "path": str(command["path"]),
                "summary": str(command["summary"]),
                "status": status,
                "capability_tier": capability_tier,
                "milestone": str(prev.get("milestone", DEFAULT_MILESTONE)),
                "owner": str(prev.get("owner", "")),
                "notes": notes,
            }
        )

    return {
        "schema_version": LEDGER_SCHEMA_VERSION,
        "status_values": list(VALID_STATUSES),
        "capability_tiers": list(VALID_CAPABILITY_TIERS),
        "commands": sorted(merged_commands, key=lambda x: str(x["name"])),
    }


def _esc_md_cell(value: Any) -> str:
    text = str(value)
    text = text.replace("|", "\\|")
    text = text.replace("\n", "<br>")
    return text


def _render_markdown(ledger_data: dict[str, Any]) -> str:
    commands = _validate_ledger(ledger_data)
    total = len(commands)

    by_status = Counter(str(c["status"]) for c in commands)
    by_tier = Counter(str(c["capability_tier"]) for c in commands)
    by_group: dict[str, Counter[str]] = defaultdict(Counter)
    by_group_tier: dict[str, Counter[str]] = defaultdict(Counter)
    for c in commands:
        by_group[str(c["group"])][str(c["status"])] += 1
        by_group_tier[str(c["group"])][str(c["capability_tier"])] += 1

    lines: list[str] = []
    lines.append("# Redis Gap Ledger")
    lines.append("")
    lines.append("Redis 명령 카탈로그 대비 Ratatosk 구현 상태 추적표.")
    lines.append(
        "원본은 `docs/redis-gap-ledger.json`, 이 문서는 `scripts/redis_gap_ledger.py`로 생성된다."
    )
    lines.append("")
    lines.append("상태(`status`)와 동작 등급(`capability_tier`)은 다르다.")
    lines.append("")
    lines.append("- `status`: 구현 추적 상태 (`planned`, `partial`, `done` 등)")
    lines.append(
        "- `capability_tier`: Redis 의미론 대비 수준 (`unsupported`, `syntax_only`, `baseline_local`, `behavioral_subset`, `distributed_parity`)"
    )
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("| --- | ---: |")
    lines.append(f"| Total commands | {total} |")
    for status in VALID_STATUSES:
        lines.append(f"| {status} | {by_status.get(status, 0)} |")
    lines.append("")
    lines.append("## Capability Tier Summary")
    lines.append("")
    lines.append("| Tier | Value |")
    lines.append("| --- | ---: |")
    for tier in VALID_CAPABILITY_TIERS:
        lines.append(f"| {tier} | {by_tier.get(tier, 0)} |")
    lines.append("")
    lines.append("## Group Progress")
    lines.append("")
    lines.append("| Group | done | partial | in_progress | planned | excluded | total |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
    for group in sorted(by_group):
        counter = by_group[group]
        group_total = sum(counter.values())
        lines.append(
            f"| {_esc_md_cell(group)} | {counter.get('done', 0)} | "
            f"{counter.get('partial', 0)} | {counter.get('in_progress', 0)} | "
            f"{counter.get('planned', 0)} | {counter.get('excluded', 0)} | {group_total} |"
        )
    lines.append("")
    lines.append("## Group Capability Tiers")
    lines.append("")
    lines.append(
        "| Group | distributed_parity | behavioral_subset | baseline_local | syntax_only | unsupported | total |"
    )
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
    for group in sorted(by_group_tier):
        counter = by_group_tier[group]
        group_total = sum(counter.values())
        lines.append(
            f"| {_esc_md_cell(group)} | {counter.get('distributed_parity', 0)} | "
            f"{counter.get('behavioral_subset', 0)} | {counter.get('baseline_local', 0)} | "
            f"{counter.get('syntax_only', 0)} | {counter.get('unsupported', 0)} | {group_total} |"
        )
    lines.append("")
    lines.append("## Command Ledger")
    lines.append("")
    lines.append("| Command | Group | Since | Status | Tier | Milestone | Notes |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- |")
    for cmd in commands:
        lines.append(
            f"| `{_esc_md_cell(cmd['name'])}` | {_esc_md_cell(cmd['group'])} | "
            f"{_esc_md_cell(cmd['since'])} | {_esc_md_cell(cmd['status'])} | "
            f"{_esc_md_cell(cmd['capability_tier'])} | {_esc_md_cell(cmd['milestone'])} | "
            f"{_esc_md_cell(cmd.get('notes', ''))} |"
        )
    lines.append("")
    return "\n".join(lines)


def _diff_strings(expected: str, actual: str, from_name: str, to_name: str) -> str:
    diff = unified_diff(
        actual.splitlines(keepends=True),
        expected.splitlines(keepends=True),
        fromfile=from_name,
        tofile=to_name,
    )
    return "".join(diff)


def cmd_snapshot(args: argparse.Namespace) -> int:
    redis_dir = Path(args.redis_dir).expanduser().resolve()
    catalog_path = Path(args.catalog).resolve()

    commands = _load_redis_commands(redis_dir)
    payload = {
        "schema_version": 1,
        "source": "redis/src/commands/*.json",
        "commands": [c.to_json() for c in commands],
    }
    _json_dump(catalog_path, payload)

    print(f"[snapshot] wrote {len(commands)} commands to {catalog_path}")
    return 0


def cmd_sync(args: argparse.Namespace) -> int:
    catalog_path = Path(args.catalog).resolve()
    ledger_path = Path(args.ledger).resolve()
    markdown_path = Path(args.markdown).resolve()

    catalog_data = _json_load(catalog_path)
    catalog_commands = _validate_catalog(catalog_data)

    existing_ledger: dict[str, Any] | None = None
    if ledger_path.exists():
        existing_ledger = _json_load(ledger_path)
    elif markdown_path.exists():
        existing_ledger = _parse_markdown_ledger(markdown_path)

    merged_ledger = _merge_ledger(catalog_commands, existing_ledger)
    markdown_text = _render_markdown(merged_ledger)

    _json_dump(ledger_path, merged_ledger)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(markdown_text, encoding="utf-8")

    status_counts = Counter(str(c["status"]) for c in merged_ledger["commands"])
    tier_counts = Counter(str(c["capability_tier"]) for c in merged_ledger["commands"])
    summary = ", ".join(
        f"{status}={status_counts.get(status, 0)}" for status in VALID_STATUSES
    )
    tier_summary = ", ".join(
        f"{tier}={tier_counts.get(tier, 0)}" for tier in VALID_CAPABILITY_TIERS
    )
    print(
        f"[sync] commands={len(merged_ledger['commands'])} "
        f"ledger={ledger_path} markdown={markdown_path}"
    )
    print(f"[sync] status_counts: {summary}")
    print(f"[sync] capability_tiers: {tier_summary}")
    return 0


def cmd_check(args: argparse.Namespace) -> int:
    catalog_path = Path(args.catalog).resolve()
    ledger_path = Path(args.ledger).resolve()
    markdown_path = Path(args.markdown).resolve()

    catalog_data = _json_load(catalog_path)
    ledger_data = _json_load(ledger_path)

    catalog_commands = _validate_catalog(catalog_data)
    ledger_commands = _validate_ledger(ledger_data)

    catalog_names = {str(c["name"]) for c in catalog_commands}
    ledger_names = {str(c["name"]) for c in ledger_commands}

    missing_in_ledger = sorted(catalog_names - ledger_names)
    extra_in_ledger = sorted(ledger_names - catalog_names)
    if missing_in_ledger or extra_in_ledger:
        if missing_in_ledger:
            print("[check] commands missing in ledger:")
            for name in missing_in_ledger:
                print(f"  - {name}")
        if extra_in_ledger:
            print("[check] extra commands in ledger not present in catalog:")
            for name in extra_in_ledger:
                print(f"  - {name}")
        return 1

    expected_markdown = _render_markdown(ledger_data)
    actual_markdown = markdown_path.read_text(encoding="utf-8")
    if expected_markdown != actual_markdown:
        print("[check] markdown is out of date. Re-run sync:")
        print(
            "  python3 scripts/redis_gap_ledger.py sync "
            f"--catalog {catalog_path} --ledger {ledger_path} --markdown {markdown_path}"
        )
        diff = _diff_strings(
            expected=expected_markdown,
            actual=actual_markdown,
            from_name=str(markdown_path),
            to_name=f"{markdown_path} (expected)",
        )
        print(diff)
        return 1

    status_counts = Counter(str(c["status"]) for c in ledger_commands)
    tier_counts = Counter(str(c["capability_tier"]) for c in ledger_commands)
    summary = ", ".join(
        f"{status}={status_counts.get(status, 0)}" for status in VALID_STATUSES
    )
    tier_summary = ", ".join(
        f"{tier}={tier_counts.get(tier, 0)}" for tier in VALID_CAPABILITY_TIERS
    )
    print(f"[check] OK: commands={len(ledger_commands)}; {summary}; {tier_summary}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_snapshot = sub.add_parser(
        "snapshot", help="Create command catalog from Redis command JSON files."
    )
    p_snapshot.add_argument(
        "--redis-dir",
        required=True,
        help="Path to Redis repo root (or directly to src/commands directory).",
    )
    p_snapshot.add_argument(
        "--catalog",
        default="docs/redis-command-catalog.json",
        help="Output catalog path.",
    )
    p_snapshot.set_defaults(func=cmd_snapshot)

    p_sync = sub.add_parser(
        "sync", help="Merge catalog into ledger and regenerate markdown."
    )
    p_sync.add_argument(
        "--catalog",
        default="docs/redis-command-catalog.json",
        help="Input command catalog path.",
    )
    p_sync.add_argument(
        "--ledger",
        default="docs/redis-gap-ledger.json",
        help="Ledger source-of-truth JSON path.",
    )
    p_sync.add_argument(
        "--markdown",
        default="docs/redis-gap-ledger.md",
        help="Generated markdown report path.",
    )
    p_sync.set_defaults(func=cmd_sync)

    p_check = sub.add_parser(
        "check", help="Validate catalog/ledger consistency and markdown freshness."
    )
    p_check.add_argument(
        "--catalog",
        default="docs/redis-command-catalog.json",
        help="Input command catalog path.",
    )
    p_check.add_argument(
        "--ledger",
        default="docs/redis-gap-ledger.json",
        help="Ledger source-of-truth JSON path.",
    )
    p_check.add_argument(
        "--markdown",
        default="docs/redis-gap-ledger.md",
        help="Generated markdown report path.",
    )
    p_check.set_defaults(func=cmd_check)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return int(args.func(args))
    except Exception as exc:  # pragma: no cover - CLI guard
        print(f"[error] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
