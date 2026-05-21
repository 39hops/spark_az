# CLAUDE.md — Project Operating Doc

> Read `AGENTS.md` first. It is the honesty contract. This file is the
> project-specific operating guide — tech stack, build/run commands, style
> choices, current state. Where this file and `AGENTS.md` agree on honesty,
> `AGENTS.md` is canonical. The operator's latest instruction outranks both.

## What this project is

<!-- One paragraph. Replace this when you fill in the repo. -->

`<one-line elevator pitch>`. `<one paragraph: what it is, who it's for, what
the current scope is, what makes it interesting>`.

## What this project is NOT

<!-- The capability ceiling. Be explicit so honesty rules can hold. -->

- `<thing it isn't trying to be>` — and why.
- `<another thing>` — and why.

## Build / Run

<!-- Concrete commands. Mark each VERIFIED <date> or UNVERIFIED. -->

```sh
# Setup
scripts/setup.sh                # one-shot project setup

# Build
scripts/build.sh                # placeholder; fill in for your stack

# Test
scripts/test.sh                 # placeholder; fill in for your stack
```

## Tech stack

<!-- Language version, build system, test framework, lint/format, key deps. -->

- Language: `<TBD>`
- Build: `<TBD>`
- Test: `<TBD>`
- Lint/format: `<TBD>`
- Key dependencies: `<TBD>`

## Style

<!-- Naming conventions, file size targets, code organization. -->

- Naming: `<convention>` (e.g. `snake_case` / `camelCase` / `PascalCase`).
- File size: target `<N>`–`<M>` lines per file; hard cap `<X>`.
- Tests: `<convention for test files and locations>`.

## Current state

<!-- Honest implemented-vs-planned. AGENTS.md §1: features count only when
     both implemented and verified. -->

| Phase | Scope | Status |
|---|---|---|
| 0 | Scaffolding | **NOT STARTED** |

## Docs

- `AGENTS.md` — honesty contract (read first).
- `docs/CAPABILITIES.md` — honest can/cannot.
- `docs/ARCHITECTURE.md` — design and interfaces.
- `docs/ROADMAP.md` — future, separated from implemented.
- `docs/superpowers/specs/` — brainstormed design docs.
- `docs/superpowers/plans/` — implementation plans.
- `handoffs/` — session-to-session notes.

## Handoffs convention

`handoffs/` holds session-to-session notes. Each file starts with a single-line
`Status:` marker:

- `Status: ACTIVE` — work in this handoff is still owed. A new session reads
  the file and executes it (or pushes back if the plan is wrong).
- `Status: COMPLETED <date> — <commit-sha>` — the recommended work shipped in
  that commit. The file is preserved as historical context only.

When you finish work a handoff describes, update its status line to COMPLETED
with the closing commit's SHA. Don't delete old handoffs — the historical
record is the point.

## Specs and plans convention

- `docs/superpowers/specs/YYYY-MM-DD-<topic>-design.md` — brainstormed design,
  one per feature, locked-decisions table at top, scope boundary at bottom.
- `docs/superpowers/plans/YYYY-MM-DD-<topic>-plan.md` — task-by-task
  implementation plan an engineer or agent can execute. References its spec.
