# .claude/

Project-level Claude Code settings live here. This file documents what's in
the directory and what's expected.

## Files

- `settings.json` — **committed**. Shared, project-level Claude Code config.
  Defaults are intentionally minimal (just a schema reference); add hooks,
  permissions, env vars, and other shared settings here.
- `settings.local.json` — **gitignored** (see `.gitignore`). Per-machine,
  per-operator config. Add your local permissions, hooks, or API tweaks here
  without polluting the team's shared settings.

## Common things to put in `settings.json`

- Project-wide tool permissions (allowlists).
- Hooks that should fire for everyone working in this repo.
- Project-wide env vars Claude Code should read.

If you're not sure whether something belongs in `settings.json` or
`settings.local.json`: would you want a teammate cloning the repo to inherit
it? If yes → `settings.json`. If it's about your machine or your preferences
→ `settings.local.json`.
