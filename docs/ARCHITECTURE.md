# ARCHITECTURE.md

## Overview

`spark_az` is a single-purpose orchestration + logging layer for Azure
Synapse Spark notebooks. One notebook (the *orchestrator*) calls many
others (the *children*) via `mssparkutils.notebook.run`, captures each
child's outcome, and writes one structured Delta row per child. The
library has no other concerns — no I/O facade, no transform helpers,
no schema management. It is intentionally small.

## Component layers

```
+----------------------------------------------------------+
| Synapse Pipeline (JSON, hand-authored or generated)      |
|   Notebook activity → orchestrator notebook              |
+----------------------------------------------------------+
                            |
                            v
+----------------------------------------------------------+
| Orchestrator notebook                                    |
|   - notebooks/_logging/lgr.ipynb (thin wrapper, uses  |
|     installed `spark_az` wheel), OR                      |
|   - notebooks/_logging/lgr_inline.ipynb (entire       |
|     library inline; %run-able or pipeline-activity-able) |
+----------------------------------------------------------+
                            |
              run_pipeline(specs, log_table=..., ...)
                            v
+----------------------------------------------------------+
| spark_az.lgr                                 |
|   - run_pipeline → run_child per spec → _append_rows     |
|   - ensure_log_table (idempotent Delta create)           |
|   - _print_line via logging.Logger                       |
+----------------------------------------------------------+
       |                              |
       v                              v
+----------------+        +-------------------------------+
| mssparkutils   |        | Delta managed table           |
|   .notebook    |        |   <db>.__pipeline_runlog      |
|   .run         |        |   (one row per child)         |
+----------------+        +-------------------------------+
       |
       v
+----------------+
| Child notebook |
|   (unchanged)  |
+----------------+
```

## Key interfaces

| Interface | Contract | Code |
|---|---|---|
| `run_pipeline(children, *, log_table, pipeline_name, fail_fast=True, default_timeout_seconds=1800, write_log=True) -> List[ChildResult]` | Sequentially run children; batch one Delta append at end; re-raise on first failure when `fail_fast=True`. Returns full result list otherwise. | `src/spark_az/lgr.py` |
| `run_child(spec, *, pipeline_run_id, pipeline_name, child_index, default_timeout_seconds=1800) -> ChildResult` | Run a single child via `mssparkutils.notebook.run`; never raises; map status to `ok` / `failed` / `timeout`. | `src/spark_az/lgr.py` |
| `ensure_log_table(table) -> None` | Idempotent Delta-table creation with the canonical schema. | `src/spark_az/lgr.py` |
| `ChildSpec` | TypedDict: `path` (required) + optional `timeout_seconds`, `args`, `name`. | `src/spark_az/lgr.py` |
| `ChildResult` | TypedDict: every column of the log table except `audited_at` (stamped at write time). | `src/spark_az/lgr.py` |
| `set_spark(session)` / `get_spark() -> SparkSession` | Module-singleton SparkSession lookup. Never calls `SparkSession.builder.getOrCreate()`. | `src/spark_az/session.py` |
| `log: logging.Logger` | Module-level `"spark_az.lgr"` logger with idempotent handler setup. Attach `AzureLogHandler` to fan out. | `src/spark_az/lgr.py` |
| `mssparkutils.notebook.run(path, timeout_seconds, args)` | External boundary; resolved via `_nbutils()` which tries `notebookutils.mssparkutils` then bare `mssparkutils`. Stubbed in tests via `fake_mssparkutils`. | `src/spark_az/lgr.py` |

## Data flow

1. **Caller builds `List[ChildSpec]`** — either from a hard-coded list
   in the orchestrator notebook, or from a Synapse pipeline activity
   that passes a JSON array as the `notebooks` parameter.
2. **`run_pipeline()` generates one `pipeline_run_id`** (UUID4) for the
   whole call. This ID is stamped on every row written by this call so
   a single SQL query can reconstruct the full pipeline run.
3. **Per child, in spec order:**
   1. If a prior child failed and `fail_fast=True`, build a
      `_skipped_result` and continue. No `mssparkutils.notebook.run`.
   2. Otherwise, `run_child()` calls
      `mssparkutils.notebook.run(spec["path"], timeout, args)`. The
      function never raises — exceptions become `status="failed"` or
      `status="timeout"` rows with full `error_class`, `error_message`,
      `error_traceback`.
   3. `_print_line()` emits one human-readable log line via
      `logging.Logger`.
4. **At the end (in a `try…finally`),** when `write_log=True`:
   1. `ensure_log_table(log_table)` — creates the Delta table if it
      doesn't exist (idempotent).
   2. `_append_rows(log_table, results)` — one batched Delta append for
      all `ChildResult` rows accumulated. ISO-string `started_at` /
      `finished_at` are coerced to timestamps via
      `pyspark.sql.functions.to_timestamp` at write time; `audited_at`
      is stamped server-side with `current_timestamp()`.
5. **If `fail_fast=True` and any child failed,** a `RuntimeError`
   carrying the first failing child's `error_class` and `error_message`
   is raised AFTER the log write, so the orchestrator notebook itself
   fails in Synapse. The log table survives for post-mortem.

## File map

```
src/spark_az/
├── __init__.py          # public surface re-exports
├── session.py           # get_spark / set_spark
├── lgr.py   # ChildSpec, ChildResult, run_child, run_pipeline,
│                        # ensure_log_table, _append_rows, helpers
└── py.typed             # PEP 561 marker

tests/
├── conftest.py                            # fake_mssparkutils + local Spark fixtures
├── test_session.py                        # 4 unit tests
├── test_lgr.py                # 26 unit tests with fake_mssparkutils
└── test_lgr_delta.py          # 4 integration tests with local Delta

notebooks/
├── lgr.{py,ipynb}             # thin wrapper, imports installed library
└── lgr_inline.{py,ipynb}      # entire library inline, %run-able
```

## Design rationale (pointers)

- Why the package never calls `SparkSession.builder.getOrCreate()`:
  see `docs/superpowers/specs/2026-05-21-pipeline-logger-design.md`
  and `src/spark_az/session.py` docstring.
- Why one batched Delta append per `run_pipeline` call instead of one
  per child: spec section "Delta schema" and rationale in
  `_append_rows`.
- Why two notebook artifacts side-by-side (thin wrapper + inline):
  the user asked for both. The wheel pathway is the "proper" Python
  packaging story; the inline notebook is the zero-install delivery
  vehicle for ad-hoc use. See the inline notebook's top-of-file
  markdown for the maintenance contract.

## What this document is NOT

- Not a tutorial. See `README.md` for usage examples.
- Not a roadmap. See `docs/ROADMAP.md`.
- Not a design doc. See
  `docs/superpowers/specs/2026-05-21-pipeline-logger-design.md` for the
  locked design decisions that produced this architecture.
