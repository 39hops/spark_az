# agentic-coding-skeleton

A starter repo for projects that will be worked on alongside coding agents
(Claude Code, Cursor, etc.). Clone or "Use this template," tell the agent
what you want to build, and the bare-bones conventions in this repo keep
the work organized and honest from turn one.

## What you get

- **`AGENTS.md`** — an operating contract for agents that's honest but not
  rigid. Hard rules around fake output, verification, and capability claims.
  Open judgment everywhere else.
- **`CLAUDE.md`** — a project operating doc template with placeholders for
  your stack, style, and phase status.
- **`docs/superpowers/{specs,plans}/`** — places for brainstormed designs and
  the implementation plans that execute them.
- **`handoffs/`** — session-to-session notes convention.
- **`skills/` + `plugins/`** — drop-in points if your project uses an agent
  skill/plugin system.
- **`.github/workflows/`** — minimal CI placeholders.
- **`.claude/settings.json`** — Claude Code settings stub.
- **`scripts/{setup,build,test}.sh`** — placeholder entry points (they print
  "configure me").

## How to use

1. Clone or click "Use this template" on GitHub.
2. Replace the `<TBD>` and `<...>` placeholders in `README.md`, `CLAUDE.md`,
   and the `docs/` skeleton files with your project specifics.
3. Fill in `scripts/{setup,build,test}.sh` for your stack.
4. Tell your agent: "Read AGENTS.md and CLAUDE.md. The task is …"

The agent now has a coherent baseline: honesty floor from `AGENTS.md`,
project context from `CLAUDE.md`, and conventions for design/plan/handoff
documents already in place.

## What this is NOT

- Not a framework. It's a directory layout plus two opinion documents.
- Not language-specific. The placeholders adapt to anything.
- Not magic. The agent still has to do the work — this just keeps it
  organized.

## License

Apache-2.0. See `LICENSE`.
