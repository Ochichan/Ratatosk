#!/usr/bin/env python3
"""Strict structural lint for SKILL.md-based skills."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

MAX_SKILL_LINES = 500
REQUIRED_FRONTMATTER_KEYS = {"name", "description"}
BANNED_AUX_FILES = {
    "README.md",
    "CHANGELOG.md",
    "QUICK_REFERENCE.md",
    "INSTALLATION_GUIDE.md",
}
VAGUE_PHRASES = (
    "do your best",
    "as needed",
    "if possible",
    "might want to",
    "usually",
)


def _parse_frontmatter(skill_text: str) -> tuple[dict[str, str], str, list[str]]:
    errors: list[str] = []
    if not skill_text.startswith("---\n"):
        return {}, skill_text, ["SKILL.md must start with YAML frontmatter block."]

    end = skill_text.find("\n---\n", 4)
    if end < 0:
        return {}, skill_text, ["YAML frontmatter block is not closed with '---'."]

    fm_text = skill_text[4:end]
    body = skill_text[end + 5 :]
    data: dict[str, str] = {}
    for raw in fm_text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if ":" not in line:
            errors.append(f"Invalid frontmatter line: {raw}")
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()
        if value.startswith('"') and value.endswith('"') and len(value) >= 2:
            value = value[1:-1]
        data[key] = value
    return data, body, errors


def _extract_local_markdown_links(body: str) -> list[str]:
    links = re.findall(r"\[[^\]]+\]\(([^)]+)\)", body)
    local_links: list[str] = []
    for link in links:
        link = link.strip()
        if not link or link.startswith("#"):
            continue
        if "//" in link:
            continue
        local_links.append(link)
    return local_links


def lint_skill_dir(skill_dir: Path) -> dict[str, object]:
    errors: list[str] = []
    warnings: list[str] = []

    if not skill_dir.exists() or not skill_dir.is_dir():
        return {
            "status": "FAIL",
            "errors": [f"Skill directory not found: {skill_dir}"],
            "warnings": [],
            "checked_files": [],
        }

    checked_files: list[str] = []
    skill_file = skill_dir / "SKILL.md"
    if not skill_file.exists():
        return {
            "status": "FAIL",
            "errors": [f"Missing required file: {skill_file}"],
            "warnings": [],
            "checked_files": checked_files,
        }

    checked_files.append(str(skill_file))
    content = skill_file.read_text(encoding="utf-8")
    frontmatter, body, fm_errors = _parse_frontmatter(content)
    errors.extend(fm_errors)

    if frontmatter:
        missing = REQUIRED_FRONTMATTER_KEYS - set(frontmatter)
        extra = set(frontmatter) - REQUIRED_FRONTMATTER_KEYS
        if missing:
            errors.append(f"Frontmatter missing required keys: {sorted(missing)}")
        if extra:
            errors.append(f"Frontmatter has unsupported keys: {sorted(extra)}")

        name = frontmatter.get("name", "")
        expected = skill_dir.name
        if name and name != expected:
            errors.append(
                f"Frontmatter name '{name}' must match folder name '{expected}'."
            )

        description = frontmatter.get("description", "")
        if description and "use when" not in description.lower():
            errors.append("Description must include concrete trigger phrase: 'Use when ...'.")
        if description and len(description) < 120:
            warnings.append("Description is short (<120 chars); trigger context may be weak.")

    if len(content.splitlines()) > MAX_SKILL_LINES:
        errors.append(f"SKILL.md exceeds {MAX_SKILL_LINES} lines.")

    if "## Workflow" not in body:
        errors.append("SKILL.md must include a '## Workflow' section.")
    if "## Quality Gate" not in body:
        errors.append("SKILL.md must include a '## Quality Gate' section.")

    numbered_steps = re.findall(r"(?m)^\d+\.\s+", body)
    if len(numbered_steps) < 4:
        errors.append("Workflow must contain at least 4 ordered steps.")

    lower_body = body.lower()
    for phrase in VAGUE_PHRASES:
        if phrase in lower_body:
            warnings.append(f"Vague phrase detected: '{phrase}'")

    for aux in BANNED_AUX_FILES:
        aux_path = skill_dir / aux
        if aux_path.exists():
            errors.append(f"Remove auxiliary file not allowed in skills: {aux}")

    for link in _extract_local_markdown_links(body):
        path = (skill_dir / link).resolve()
        if not path.exists():
            errors.append(f"Broken local link in SKILL.md: {link}")

    status = "PASS" if not errors else "FAIL"
    return {
        "status": status,
        "errors": errors,
        "warnings": warnings,
        "checked_files": checked_files,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Lint a SKILL.md-based skill folder.")
    parser.add_argument("skill_dir", type=Path, help="Path to skill directory")
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args()

    result = lint_skill_dir(args.skill_dir)
    if args.as_json:
        print(json.dumps(result, ensure_ascii=True, indent=2))
    else:
        print(f"status: {result['status']}")
        for err in result["errors"]:
            print(f"ERROR: {err}")
        for warning in result["warnings"]:
            print(f"WARN: {warning}")

    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
