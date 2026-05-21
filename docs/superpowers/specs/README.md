# docs/superpowers/specs/

Brainstormed design docs. One file per feature.

## Naming

`YYYY-MM-DD-<topic>-design.md`. The date is when the brainstorm happened;
the topic is a short kebab-case slug.

## Shape

Each spec should include:

- **Status** marker at the top (`APPROVED-DESIGN`, `IN-PROGRESS`,
  `SUPERSEDED-BY: <other-file>`).
- **Prerequisite reading** — the other docs and code paths the spec
  assumes.
- **Motivation** — why this work exists. What problem it solves.
- **Locked decisions table** — the choices the brainstorm settled. A future
  session executing the spec can read this table and proceed without
  re-deriving everything.
- **Architecture** — interfaces, components, data flow.
- **Scope boundary** — what is NOT in v1.
- **Open questions** — things still designable, with bias toward shipping
  v1 without resolving them.

## Lifecycle

A spec lives forever once committed. If a later spec changes the design,
add a `SUPERSEDED-BY:` line at the top of the older one; don't delete it.
The historical record is the point.
