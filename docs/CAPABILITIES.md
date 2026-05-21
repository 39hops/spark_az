# CAPABILITIES.md — Honest Can / Cannot

> This document is binding. Per `AGENTS.md` §1 (no fake output), no code,
> comment, README, or agent response may contradict it. If reality diverges
> from this doc, fix the doc in the same change.

## One-paragraph truth

`spark_az` runs a sequence of Azure Synapse Spark child notebooks from
one orchestrator notebook via `mssparkutils.notebook.run`, captures each
child's status / duration / exit value / exception, and writes one
queryable Delta row per child. It is a small library (single
`pipeline_logger.py` module plus a Spark-session helper) intentionally
scoped for the v1 surface. Not a replacement for Synapse pipeline JSON
authoring, not a parallel orchestrator, not a step-grained event logger.

## What it CAN do now

- **Sequential child orchestration.** `run_pipeline(children, *,
  log_table, pipeline_name, fail_fast=True, default_timeout_seconds=1800,
  write_log=True)` runs each child via
  `mssparkutils.notebook.run(path, timeout_seconds, args)`.
- **Per-child Delta logging.** One row per child, columns:
  `pipeline_run_id`, `pipeline_name`, `child_index`, `notebook_path`,
  `status` (`ok` | `failed` | `timeout` | `skipped`), `started_at`,
  `finished_at`, `duration_ms`, `exit_value`, `args_json`,
  `error_class`, `error_message`, `error_traceback`,
  `orchestrator_notebook`, `audited_at`. One batched Delta commit per
  `run_pipeline` call.
- **Idempotent log-table creation.** `ensure_log_table(table)` creates
  the managed Delta table on first use; safe to call repeatedly.
- **Fail-fast with durable post-mortem.** When `fail_fast=True` and a
  child fails, the remaining children are recorded as `"skipped"` rows,
  the full log batch is written, then the orchestrator notebook
  re-raises so the upstream Synapse pipeline activity sees failure. The
  log table survives.
- **Run-all (continue past failures).** With `fail_fast=False`, every
  child runs and failures land as rows; the call returns normally.
- **Status mapping.** Any exception from `mssparkutils.notebook.run`
  whose message contains `"timeout"` or `"timed out"` (case-insensitive
  substring) is recorded as `status="timeout"`; any other exception as
  `status="failed"`. Tracebacks are truncated to 16 KB, messages to 4 KB.
- **stdlib logging.** Per-child output goes through
  `logging.getLogger("spark_az.pipeline_logger")` at INFO. Attach an
  Azure Application Insights handler to fan out without library changes.
- **Two deployment formats.** An installable wheel
  (`scripts/build.sh`) and a self-contained Synapse notebook
  (`notebooks/pipeline_logger_inline.ipynb`) that `%run` can pull into
  any other notebook with zero install. The inline notebook is also
  directly runnable as a Synapse pipeline activity (parameters cell at
  the top).
- **Local testing without Synapse.** `tests/conftest.py` provides a
  `fake_mssparkutils` fixture and a local Delta-enabled `SparkSession`
  fixture; 30 unit tests + 4 Delta integration tests = 34 tests passing.

## What is partially implemented or roadmap

- **App Insights fan-out.** Stdlib logger is wired; bolting on an
  `AzureMonitorLogExporter` handler is one line at the notebook top.
  The library doesn't manage the connection string itself; the user
  attaches the handler from their notebook.
- **Inline-notebook drift detection.** The hand-maintained
  `notebooks/pipeline_logger_inline.py` is in sync with the library as
  of commit `87d8762`. There is no automated check; drift is the
  acceptable tradeoff for the simpler maintenance contract.

## What it CANNOT do

- **Parallel child orchestration.** v1 is sequential.
  `mssparkutils.notebook.run` is blocking; a future
  `run_pipeline_parallel` would wrap it in a thread pool with FAIR-pool
  support, but is not implemented.
- **Step-grained logging inside children.** One row per child notebook,
  not one per phase inside a child. A future spec may add a `step()`
  context manager that emits step rows keyed by the same
  `pipeline_run_id`, but the library does not provide this today.
- **Synapse pipeline JSON authoring.** The orchestrator runs the
  children itself; users still hand-author or click-author their
  Synapse pipeline JSON. A Python builder for that JSON layer is a
  separate future spec.
- **`pipeline_run_id` propagation from a Synapse pipeline.** Today the
  UUID is generated inside `run_pipeline()`. v2 will accept an injected
  `pipeline_run_id` from `@pipeline().RunId` so the same ID joins the
  Synapse pipeline activity log with our Delta log.
- **Retries inside `run_pipeline`.** v1 surfaces failure; the pipeline
  JSON above is responsible for retrying the orchestrator notebook if
  desired.
- **Pluggable sinks (Azure Monitor as primary, etc.).** The library
  writes to one Delta table. App Insights is opt-in via a stdlib
  logging handler; it isn't a structured sink. If a second concrete
  sink becomes a real need, a `Sink` protocol can be added then.

## Performance

- **Spark startup in tests:** ~9 s for the session-scoped fixture; full
  suite runs in ~10–15 s after warm-up.
- **Per-child log overhead:** dominated by Delta commit time; one
  commit per `run_pipeline` call, not per child. Measured against a
  local Delta table, two-row append + `current_timestamp` stamping
  completes well under one second.

No production-scale numbers are estimated here. If a benchmark is run,
attach the command and its real output before quoting a number.

## Known failure modes

- **Outside Synapse, `_nbutils()` raises `RuntimeError`.** Any call to
  `run_pipeline` or `run_child` fails before the loop. Tests use the
  `fake_mssparkutils` fixture to stand in.
- **Timeout substring matching is permissive.** An exception message
  containing `"timeout"` or `"timed out"` anywhere is recorded as
  `status="timeout"`. Other exceptions whose messages happen to mention
  those words will be misclassified. The error row's full
  `error_class` / `error_message` / `error_traceback` are intact for
  forensics; only the `status` discriminator is affected.
- **`BaseException` caught.** `run_child` catches `BaseException` (per
  spec: "never raises"). A `KeyboardInterrupt` during a child run is
  recorded as `status="failed", error_class="KeyboardInterrupt"`
  instead of propagating. Defensible but worth knowing.
- **Inline-notebook drift.** Hand-maintained; if the library changes
  and someone forgets to copy the new bodies into
  `notebooks/pipeline_logger_inline.py`, behavior between the two
  delivery formats diverges. The maintenance note at the bottom of the
  inline notebook spells out the protocol.

## Scope boundary

Implemented features and roadmap features are tracked separately in
`CLAUDE.md` (current state table) and `docs/ROADMAP.md`. A feature is
"real" only when it's both implemented and verified.
