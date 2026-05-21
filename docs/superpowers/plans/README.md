# docs/superpowers/plans/

Implementation plans. One file per spec being executed (or per discrete
chunk of execution if the spec is multi-phase).

## Naming

`YYYY-MM-DD-<topic>-plan.md`. The date is when the plan was written; the
topic matches the spec it executes.

## Shape

Each plan should include:

- **Goal** — one sentence.
- **Architecture summary** — two or three sentences.
- **Prerequisite reading** — the spec it executes and any code paths the
  executor must understand first.
- **File structure** — which files get created, modified, or deleted. Each
  file should have one clear responsibility.
- **Tasks** — bite-sized (2–5 minutes each) steps in TDD shape where it
  fits: write the failing test, run it to fail, implement, run to pass,
  commit. Exact file paths and commands. Complete code snippets, no
  placeholders.
- **Done definition** — verifiable criteria for "this plan is finished."

## Granularity

Plans should be detailed enough that an engineer (or agent) with zero
context can execute them end-to-end. No `TBD`, no `add appropriate error
handling`, no `similar to Task N`. The plan is the briefing.

## Lifecycle

A plan is consumed by execution. Once executed, the plan stays as a
historical record of what was built and how. Mark the plan as complete in
its header rather than deleting it.
