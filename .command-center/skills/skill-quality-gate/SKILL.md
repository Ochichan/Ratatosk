---
name: skill-quality-gate
description: Enforce a strict quality gate for creating or updating SKILL.md-based skills. Use when Codex must produce a high-rigor skill with explicit trigger metadata, deterministic workflow steps, reference-backed instructions, and scriptable validation before accepting the result.
---

# Skill Quality Gate

Create or upgrade skills that are stricter than baseline review prompts by enforcing measurable quality criteria and fail-fast checks.

## Core Rules

- Treat every skill draft as invalid until it passes both lint and score checks.
- Use deterministic instructions; replace vague language with explicit commands.
- Keep SKILL.md focused on workflow; move dense detail into `references/`.
- Reject auxiliary docs (`README.md`, `CHANGELOG.md`, `QUICK_REFERENCE.md`) inside the skill folder.
- Require runnable validation commands before marking work complete.

## Workflow

1. Define concrete triggers and expected user requests.
2. Create or update the skill folder and required files (`SKILL.md`, `agents/openai.yaml`).
3. Add reusable resources (`scripts/`, `references/`, `assets/`) only when they remove repeated work.
4. Run `scripts/lint_skill.py <skill_dir>` and fix all reported errors.
5. Run `scripts/score_skill.py <skill_dir>` and raise score to pass threshold.
6. Repeat steps 4-5 until gate status is `PASS` with no fatal violations.

## Quality Gate

A skill fails immediately when any of these are true:

- SKILL.md frontmatter is missing `name` or `description`.
- Description does not state concrete trigger context (`Use when ...`).
- Workflow is not explicit and ordered.
- Referenced local files are missing.
- Lint status is `FAIL`.
- Score is below threshold.

Default threshold: `90/100`.

## Commands

```bash
python3 scripts/lint_skill.py .command-center/skills/<candidate>
python3 scripts/score_skill.py .command-center/skills/<candidate>
```

## References

- Use `references/rubric.md` to apply weighted scoring and pass/fail policy.
- Use `references/anti-patterns.md` to detect and remove weak patterns.

## Output Contract

When finishing a skill task, report:

- Final score and pass/fail status.
- Fatal violations (if any).
- Exact files changed.
- Remaining risk or follow-up actions.
