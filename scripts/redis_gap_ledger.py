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

DEFAULT_STATUS = "planned"
DEFAULT_MILESTONE = "backlog"

LEDGER_SCHEMA_VERSION = 1


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
    if schema_version != LEDGER_SCHEMA_VERSION:
        raise ValueError(
            f"Unsupported ledger schema_version={schema_version}; expected {LEDGER_SCHEMA_VERSION}"
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

    return sorted(commands, key=lambda x: str(x["name"]))


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

        merged_commands.append(
            {
                "name": name,
                "group": str(command["group"]),
                "since": str(command["since"]),
                "arity": command.get("arity"),
                "path": str(command["path"]),
                "summary": str(command["summary"]),
                "status": status,
                "milestone": str(prev.get("milestone", DEFAULT_MILESTONE)),
                "owner": str(prev.get("owner", "")),
                "notes": str(prev.get("notes", "")),
            }
        )

    return {
        "schema_version": LEDGER_SCHEMA_VERSION,
        "status_values": list(VALID_STATUSES),
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
    by_group: dict[str, Counter[str]] = defaultdict(Counter)
    for c in commands:
        by_group[str(c["group"])][str(c["status"])] += 1

    lines: list[str] = []
    lines.append("# Redis Gap Ledger")
    lines.append("")
    lines.append("Redis 명령 카탈로그 대비 Ratatosk 구현 상태 추적표.")
    lines.append(
        "원본은 `docs/redis-gap-ledger.json`, 이 문서는 `scripts/redis_gap_ledger.py`로 생성된다."
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
    lines.append("## Command Ledger")
    lines.append("")
    lines.append("| Command | Group | Since | Status | Milestone | Notes |")
    lines.append("| --- | --- | --- | --- | --- | --- |")
    for cmd in commands:
        lines.append(
            f"| `{_esc_md_cell(cmd['name'])}` | {_esc_md_cell(cmd['group'])} | "
            f"{_esc_md_cell(cmd['since'])} | {_esc_md_cell(cmd['status'])} | "
            f"{_esc_md_cell(cmd['milestone'])} | {_esc_md_cell(cmd.get('notes', ''))} |"
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

    merged_ledger = _merge_ledger(catalog_commands, existing_ledger)
    markdown_text = _render_markdown(merged_ledger)

    _json_dump(ledger_path, merged_ledger)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(markdown_text, encoding="utf-8")

    status_counts = Counter(str(c["status"]) for c in merged_ledger["commands"])
    summary = ", ".join(
        f"{status}={status_counts.get(status, 0)}" for status in VALID_STATUSES
    )
    print(
        f"[sync] commands={len(merged_ledger['commands'])} "
        f"ledger={ledger_path} markdown={markdown_path}"
    )
    print(f"[sync] status_counts: {summary}")
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
    summary = ", ".join(
        f"{status}={status_counts.get(status, 0)}" for status in VALID_STATUSES
    )
    print(f"[check] OK: commands={len(ledger_commands)}; {summary}")
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
