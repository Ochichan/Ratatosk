---
id: _skill-quality-gate-rubric
title: Skill Quality Gate Rubric
type: include
enabled: true
---

# Skill Rubric

Use this rubric to score a candidate skill out of 100.

## Pass Criteria

- `PASS`: total score >= 90 and zero fatal violations.
- `FAIL`: total score < 90 or at least one fatal violation.

## Fatal Violations

- Missing `SKILL.md`.
- Invalid frontmatter block or missing `name`/`description`.
- Description missing concrete trigger phrase (`Use when ...`).
- Missing ordered workflow instructions.
- Broken local markdown links in SKILL.md.

## Weighted Categories

1. Trigger Precision (20)
- Full points: description names scope, triggers, and user contexts.
- Deduct when description is vague or generic.

2. Workflow Determinism (25)
- Full points: explicit numbered steps, command examples, deterministic order.
- Deduct when sequencing is unclear or optionality is overused.

3. Resource Reuse Design (20)
- Full points: scripts/references/assets are used only when they reduce repetition.
- Deduct when resources are missing for repetitive tasks, or unnecessary files are added.

4. Safety and Guardrails (15)
- Full points: hard-fail conditions and validation gates are explicit.
- Deduct when safety checks are implied but not enforceable.

5. Progressive Disclosure (10)
- Full points: SKILL.md stays concise and links to focused references.
- Deduct when SKILL.md is overloaded or references are unstructured.

6. Concision and Clarity (10)
- Full points: concise directives, low ambiguity, no redundant prose.
- Deduct for verbosity without added instruction value.
