---
id: _skill-quality-gate-anti-patterns
title: Skill Quality Gate Anti-patterns
type: include
enabled: true
---

# Anti-Patterns

Reject or rewrite these patterns.

## Trigger Metadata Anti-Patterns

- Description only says what the skill is, not when to use it.
- Description lacks concrete contexts or trigger phrases.
- Frontmatter includes unrelated keys that do not affect triggering.

## Instruction Anti-Patterns

- Vague directives (`do your best`, `as needed`, `if possible`).
- Unordered steps for order-sensitive workflows.
- Hidden critical requirements buried in long paragraphs.
- Conflicting rules across sections.

## Resource Anti-Patterns

- Rewriting long procedures repeatedly instead of scripting.
- Creating broad references with no loading guidance.
- Adding docs that are not part of skill execution.

## Validation Anti-Patterns

- Declaring completion without running lint/score.
- Reporting only prose summary without measurable gate results.
- Passing with known fatal violations.
