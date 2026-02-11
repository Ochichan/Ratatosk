#!/usr/bin/env python3
"""Weighted scoring gate for SKILL.md-based skills."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

from lint_skill import lint_skill_dir

PASS_THRESHOLD = 90


def _read_skill(skill_dir: Path) -> tuple[dict[str, str], str]:
    skill_file = skill_dir / "SKILL.md"
    text = skill_file.read_text(encoding="utf-8")

    frontmatter: dict[str, str] = {}
    body = text
    if text.startswith("---\n"):
        end = text.find("\n---\n", 4)
        if end >= 0:
            for line in text[4:end].splitlines():
                if ":" not in line:
                    continue
                key, value = line.split(":", 1)
                frontmatter[key.strip()] = value.strip().strip('"')
            body = text[end + 5 :]
    return frontmatter, body


def _count_ordered_steps(body: str) -> int:
    return len(re.findall(r"(?m)^\d+\.\s+", body))


def _has_commands(body: str) -> bool:
    return "```bash" in body or "```sh" in body


def _has_section(body: str, heading: str) -> bool:
    return heading in body


def _link_count(body: str) -> int:
    return len(re.findall(r"\[[^\]]+\]\(([^)]+)\)", body))


def score_skill_dir(skill_dir: Path) -> dict[str, object]:
    lint = lint_skill_dir(skill_dir)
    fatal_violations: list[str] = []
    if lint["status"] != "PASS":
        fatal_violations.extend(lint["errors"])  # type: ignore[arg-type]

    if not (skill_dir / "SKILL.md").exists():
        fatal_violations.append("Missing SKILL.md")

    frontmatter, body = _read_skill(skill_dir)
    description = frontmatter.get("description", "")

    trigger_precision = 0
    if description:
        if "use when" in description.lower():
            trigger_precision += 8
        if len(description) >= 140:
            trigger_precision += 6
        if any(token in description.lower() for token in ("create", "update", "validate", "trigger")):
            trigger_precision += 6

    workflow_determinism = 0
    step_count = _count_ordered_steps(body)
    if step_count >= 6:
        workflow_determinism += 12
    elif step_count >= 4:
        workflow_determinism += 8
    if _has_commands(body):
        workflow_determinism += 8
    if _has_section(body, "## Workflow"):
        workflow_determinism += 5

    resource_reuse = 0
    if "scripts/" in body:
        resource_reuse += 8
    if "references/" in body:
        resource_reuse += 6
    if _link_count(body) >= 2:
        resource_reuse += 6

    safety_guardrails = 0
    if _has_section(body, "## Quality Gate"):
        safety_guardrails += 8
    if "fails immediately" in body.lower() or "fatal" in body.lower():
        safety_guardrails += 4
    if "threshold" in body.lower():
        safety_guardrails += 3

    progressive_disclosure = 0
    if "## References" in body:
        progressive_disclosure += 5
    if "move dense detail into `references/`" in body.lower():
        progressive_disclosure += 5

    concision = 10
    line_count = len((skill_dir / "SKILL.md").read_text(encoding="utf-8").splitlines())
    if line_count > 350:
        concision -= 6
    elif line_count > 250:
        concision -= 3

    category_scores = {
        "trigger_precision": min(trigger_precision, 20),
        "workflow_determinism": min(workflow_determinism, 25),
        "resource_reuse_design": min(resource_reuse, 20),
        "safety_guardrails": min(safety_guardrails, 15),
        "progressive_disclosure": min(progressive_disclosure, 10),
        "concision_clarity": max(min(concision, 10), 0),
    }
    total_score = sum(category_scores.values())

    status = "PASS" if total_score >= PASS_THRESHOLD and not fatal_violations else "FAIL"
    return {
        "status": status,
        "threshold": PASS_THRESHOLD,
        "total_score": total_score,
        "category_scores": category_scores,
        "fatal_violations": fatal_violations,
        "lint_warnings": lint["warnings"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Score a SKILL.md-based skill folder.")
    parser.add_argument("skill_dir", type=Path, help="Path to skill directory")
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args()

    result = score_skill_dir(args.skill_dir)
    if args.as_json:
        print(json.dumps(result, ensure_ascii=True, indent=2))
    else:
        print(f"status: {result['status']}")
        print(f"score: {result['total_score']}/{100} (threshold {result['threshold']})")
        for category, score in result["category_scores"].items():
            print(f"{category}: {score}")
        for violation in result["fatal_violations"]:
            print(f"FATAL: {violation}")
        for warning in result["lint_warnings"]:
            print(f"WARN: {warning}")

    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    sys.exit(main())
