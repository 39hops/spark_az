# Contributing

Agents and humans both contribute to this codebase. The same rules apply.

## Read first

- `AGENTS.md` — honesty contract.
- `CLAUDE.md` — project operating doc and current state.

## Before opening a PR

1. The change does what its title says — nothing extra, nothing missing.
2. Tests pass locally. Paste the real output if claiming green.
3. Docs that mention the changed code still match it (CLAUDE.md phase
   table, `docs/ARCHITECTURE.md`, `docs/CAPABILITIES.md` — whichever apply).
4. Commit messages explain the *why*. Reference any spec or plan filename
   the change executes.

## What good agent-driven contributions look like

- Each commit ships one logical change.
- Designs that are non-trivial start in `docs/superpowers/specs/` before
  code.
- Implementation plans live in `docs/superpowers/plans/` and reference
  their spec.
- Session-to-session work is recorded in `handoffs/`.
- "Done" claims include the real command output.

## What to avoid

- Confident statements that aren't backed by a run.
- "This should work" — either run it or label it unverified.
- Tests that mock the thing they're claiming to test.
- Fixing unrelated stuff in a focused PR (do it in a separate one).
- Bypassing pre-commit hooks, CI checks, or linters without explicit
  operator authorization. If a check fails, fix the underlying issue.
