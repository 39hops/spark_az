# Pipeline Logger — Design Spec

**Date:** 2026-05-21
**Status:** Locked
**Author:** Artin (with Claude)
**Reference:** Mirrors style and conventions of
[`github.com/39hops/spark_lib`](https://github.com/39hops/spark_lib).

## Locked decisions

| Decision | Choice | Rationale |
| --- | --- | --- |
| Pattern | **B — orchestrator notebook runs children** | One notebook drives many via `mssparkutils.notebook.run`. Children unchanged. Zero migration cost. |
| Log grain | **One row per child notebook invocation** | Coarse but covers the "what happened in this pipeline run?" question without notebook-side changes. |
| Run identity | **One UUID per `run_pipeline()` call** | No Synapse pipeline JSON contract required in v1. The orchestrator notebook *is* the pipeline. |
| Sink | **Stdout + Delta table** | Pretty cell output for the human; durable table for replay and dashboards. |
| Module shape | **Single module** (`src/spark_az/lgr.py`) | YAGNI. Refactor when it grows past ~400 lines. |
| Source format | **Jupytext "percent" `.py` + generated `.ipynb`** | `.py` is the source of truth, testable with `pytest`. `.ipynb` is the Synapse-deliverable. Both committed. |
| Write timing | **One batched append per `run_pipeline()` call** | Same trick as `SyncState.upsert_all` in spark_lib — one Delta commit, not N. |
| Fail-fast | **Default `True`; failed run still writes the log, then re-raises** | The Synapse pipeline activity wrapping the orchestrator needs to see it fail; the log table survives so we can post-mortem. |
| Concurrency | **Sequential only in v1** | `mssparkutils.notebook.run` is blocking. Parallel is a v2 — `run_parallel`-shape thread pool over `run_child`. |
| Style | **Strict `typing` module + `from __future__ import annotations` + docstrings only** | Matches spark_lib. Portable across Synapse Python versions. |

## Context

The repo's goal is "spark_az": Azure Synapse Spark helpers, follow-on to
`spark_lib`. The first piece is a notebook-driven orchestrator that runs other
notebooks and writes one structured row per child to a Delta table.

This replaces the pattern of editing Synapse pipeline JSON in Studio for simple
sequential fan-outs. The orchestrator notebook can itself be invoked from a
single-activity Synapse pipeline, keeping the JSON layer trivial.

## Architecture

```
src/spark_az/
├── __init__.py             # re-exports the public surface
├── session.py              # get_spark / set_spark (carried from spark_lib)
└── lgr.py      # orchestration + Delta logging

notebooks/
├── lgr.py      # jupytext "percent" source of truth
└── lgr.ipynb   # generated, committed

scripts/
└── build_notebooks.sh      # jupytext --to ipynb wrapper

tests/
├── conftest.py             # mssparkutils stub, local Spark fixture
├── test_lgr.py # pure-Python unit tests
└── test_lgr_delta.py  # local-Spark schema roundtrip
```

### Public surface

Re-exported from `spark_az.__init__`:

```python
from spark_az import (
    run_pipeline,
    run_child,
    ChildSpec,
    ChildResult,
    ensure_log_table,
    get_spark,
    set_spark,
)
```

### `session.py`

Verbatim port of `spark_lib.session`: module-level `_spark`, `set_spark`,
`get_spark` falling through registered → `SparkSession.getActiveSession()` →
`RuntimeError`. Never calls `getOrCreate`. Tests reset `_spark` via a fixture.

## Public API

### `ChildSpec` (TypedDict, total=False)

```python
from typing import Any, Dict, TypedDict


class ChildSpec(TypedDict, total=False):
    """One child notebook to run.

    Fields:
        path: Required. Synapse workspace path (or notebook name) passed to
            ``mssparkutils.notebook.run``.
        timeout_seconds: Optional. Defaults to ``default_timeout_seconds``
            from ``run_pipeline`` (1800).
        args: Optional. Arguments forwarded to the child notebook.
        name: Optional display name for stdout. Defaults to the basename of
            ``path``.
    """
    path: str
    timeout_seconds: int
    args: Dict[str, Any]
    name: str
```

### `ChildResult` (TypedDict, total=True)

```python
from typing import TypedDict


class ChildResult(TypedDict):
    """One row written to the log table per child invocation.

    Field semantics:
        status: ``"ok"`` | ``"failed"`` | ``"timeout"`` | ``"skipped"``.
        exit_value: Whatever the child returned via
            ``mssparkutils.notebook.exit(...)``. Stored as a string for
            forward compatibility with arbitrary payloads.
        args_json: ``json.dumps(args, default=str)`` for reproducibility.
        error_class / error_message / error_traceback: Populated when
            ``status != "ok"``. Empty strings otherwise. Traceback truncated
            to 16 KB, message to 4 KB.
    """
    pipeline_run_id: str
    pipeline_name: str
    child_index: int
    notebook_path: str
    status: str
    started_at: str
    finished_at: str
    duration_ms: int
    exit_value: str
    args_json: str
    error_class: str
    error_message: str
    error_traceback: str
    orchestrator_notebook: str
```

### `run_pipeline`

```python
from typing import Iterable, List


def run_pipeline(
    children: Iterable[ChildSpec],
    *,
    log_table: str,
    pipeline_name: str,
    fail_fast: bool = True,
    default_timeout_seconds: int = 1800,
    write_log: bool = True,
) -> List[ChildResult]:
    """Run a sequence of child notebooks and log each one.

    Generates one ``pipeline_run_id`` (UUID4) for the call. For each child:

    1. Calls ``mssparkutils.notebook.run(path, timeout_seconds, args)``.
    2. Captures wall time, exit value, and exception (if any).
    3. Prints one stdout line.
    4. Appends a :class:`ChildResult` to the in-memory result list.

    After the loop, the full result list is written to ``log_table`` in one
    Delta append (when ``write_log=True``). If ``fail_fast=True`` and any
    child failed, the captured exception is re-raised AFTER the log is
    written, so the orchestrator notebook itself fails in Synapse and the
    log table is durable for post-mortem.

    Args:
        children: Iterable of :class:`ChildSpec` entries.
        log_table: Fully-qualified managed Delta table for the log rows.
            Created via :func:`ensure_log_table` if missing.
        pipeline_name: Caller-supplied label stamped on every row.
        fail_fast: When ``True`` (default), the first failure marks remaining
            children as ``status="skipped"`` and the call re-raises after the
            log write. When ``False``, every child is attempted and the call
            returns normally with failures captured as rows.
        default_timeout_seconds: Used when a :class:`ChildSpec` does not
            specify its own ``timeout_seconds``.
        write_log: When ``False``, prints stdout but skips the Delta write.
            Used by tests and dry runs.

    Returns:
        When ``fail_fast=False`` OR all children succeeded: the full
        ``List[ChildResult]`` in input order, including ``"skipped"`` rows
        if any. When ``fail_fast=True`` AND a child failed: the function
        re-raises instead of returning — the full list is durable in
        ``log_table`` for the caller to query via SQL.

    Raises:
        RuntimeError: ``mssparkutils`` / ``notebookutils`` not importable
            (i.e. not running in Synapse). Raised before the child loop.
        RuntimeError: Re-raised after the log write when ``fail_fast=True``
            and any child failed. The exception carries the first failing
            child's ``error_class`` and ``error_message`` in its message.

    Examples:
        Sequential run with fail-fast:

        >>> results = run_pipeline(
        ...     [
        ...         {"path": "/notebooks/extract", "args": {"date": "2026-05-21"}},
        ...         {"path": "/notebooks/transform"},
        ...         {"path": "/notebooks/load"},
        ...     ],
        ...     log_table="_meta.__pipeline_runlog",
        ...     pipeline_name="nightly_lab_refresh",
        ... )

        Run-all (continue past failures):

        >>> results = run_pipeline(specs, log_table=t, pipeline_name=p, fail_fast=False)
        >>> failed = [r for r in results if r["status"] != "ok"]
    """
```

### `run_child`

```python
def run_child(
    spec: ChildSpec,
    *,
    pipeline_run_id: str,
    pipeline_name: str,
    child_index: int,
    default_timeout_seconds: int = 1800,
) -> ChildResult:
    """Run one child notebook and return a :class:`ChildResult`.

    Never raises. Any exception from ``mssparkutils.notebook.run`` is
    captured into the result. Decision to re-raise lives in
    :func:`run_pipeline` so this function stays composable for future
    parallel orchestration.

    Status mapping:

    - Returns normally → ``"ok"``; ``exit_value`` set to ``str(returned)``.
    - Raises with ``"timeout"`` in the exception message → ``"timeout"``.
    - Any other exception → ``"failed"`` with ``error_class``,
      ``error_message``, ``error_traceback`` populated.

    Args:
        spec: The child to run.
        pipeline_run_id: UUID shared across one ``run_pipeline()`` call.
        pipeline_name: Caller-supplied label.
        child_index: Zero-based position in the input sequence.
        default_timeout_seconds: Used when ``spec["timeout_seconds"]`` is
            absent.

    Returns:
        A :class:`ChildResult` describing the outcome.

    Examples:
        >>> result = run_child(
        ...     {"path": "/notebooks/extract", "args": {"date": "2026-05-21"}},
        ...     pipeline_run_id="...",
        ...     pipeline_name="nightly",
        ...     child_index=0,
        ... )
    """
```

### `ensure_log_table`

```python
def ensure_log_table(table: str) -> None:
    """Create the log Delta table if it does not exist.

    Idempotent. Mirrors :meth:`SyncState.ensure` in spark_lib: checks
    ``spark.catalog.tableExists(table)``, otherwise writes an empty
    DataFrame with the standard log schema and saves as a managed Delta
    table.

    Args:
        table: Fully-qualified managed Delta table name.

    Examples:
        >>> ensure_log_table("_meta.__pipeline_runlog")
    """
```

## Delta schema

| Column | Type | Notes |
| --- | --- | --- |
| `pipeline_run_id` | string (not null) | One UUID per `run_pipeline()` call. |
| `pipeline_name` | string (not null) | Caller-supplied label. |
| `child_index` | int (not null) | Position in the input list (0-based). |
| `notebook_path` | string (not null) | Forwarded to `mssparkutils.notebook.run`. |
| `status` | string (not null) | `"ok"` \| `"failed"` \| `"timeout"` \| `"skipped"`. |
| `started_at` | timestamp (not null) | Set right before the `notebook.run` call. |
| `finished_at` | timestamp (not null) | Set in the `finally`. |
| `duration_ms` | long (not null) | `finished_at - started_at` in milliseconds. |
| `exit_value` | string (not null) | Whatever the child passed to `notebook.exit(...)`. Empty string if none. |
| `args_json` | string (not null) | `json.dumps(args, default=str)`. |
| `error_class` | string (not null) | `type(exc).__name__` or `""`. |
| `error_message` | string (not null) | `str(exc)` or `""`. Truncated to 4 KB. |
| `error_traceback` | string (not null) | `traceback.format_exc()` or `""`. Truncated to 16 KB. |
| `orchestrator_notebook` | string (not null) | Best-effort from `mssparkutils.runtime.context`. Empty if unavailable. |
| `audited_at` | timestamp (not null) | `current_timestamp()` at write time. |

`status="skipped"` rows appear only when `fail_fast=True` and a prior child
failed. Every child in the input list always produces a row — the table by
itself answers "what was supposed to run, what ran, what didn't."

For skipped rows: `started_at = finished_at = current_timestamp()` at the
moment the skip is recorded, `duration_ms = 0`, `exit_value = ""`,
`args_json = json.dumps(spec.get("args", {}))`, `error_class = ""`,
`error_message = ""`, `error_traceback = ""`. No nulls.

## Stdout format

One line per child, printed as the child completes (not at end-of-pipeline):

```
[14:02:11] [OK]     extract           1.83s  exit=42rows
[14:02:13] [FAIL]   transform         0.42s  ValueError: missing column 'id'
[14:02:13] [SKIP]   load                     (fail_fast)
```

- Timestamp: local wall time, `%H:%M:%S`.
- Status: fixed-width 6 chars, uppercase.
- Display name: padded to 18 chars.
- Duration: `%.2fs` (omitted on `SKIP`).
- Suffix: `exit=<value>` on success (truncated to 40 chars), `<error_class>: <message>` on failure (message truncated to 80 chars), `(fail_fast)` on skip.

Plain `print()` — no `logging` handler setup. The audience is the Synapse cell
output, which already shows everything written to stdout.

## Failure semantics

```
run_pipeline:
  1. ensure_log_table(log_table)               # idempotent
  2. for i, spec in enumerate(specs):
       if a fatal child has occurred AND fail_fast:
         append a "skipped" ChildResult; print SKIP line; continue
       result = run_child(spec, ...)            # never raises
       append result; print line
       if result["status"] != "ok" AND fail_fast:
         remember the first failure as "fatal"
  3. write all results in one batched append    # finally — runs even on failure path
  4. if fatal: re-raise a reconstructed RuntimeError carrying error_class + message
```

We re-raise a reconstructed `RuntimeError` rather than the original
`Py4JJavaError` because the original lives only inside `run_child`'s
try/except and is not preserved on the `ChildResult`. The point of re-raising
is to fail the orchestrator notebook so the Synapse pipeline above sees
failure — exception identity is not load-bearing.

## Testing strategy

| Layer | Spark needed? | What it covers |
| --- | --- | --- |
| Unit (`test_lgr.py`) | No | Status mapping, `args_json` serialization, `error_traceback` truncation, `fail_fast` re-raise + log-write order, `ChildResult` shape. `mssparkutils.notebook.run` is stubbed via `conftest.py` to return a value, raise a `RuntimeError`, or raise with `"timeout"` in the message. |
| Integration (`test_lgr_delta.py`) | Local Spark + `delta-spark` | `ensure_log_table` is idempotent. Writing a synthetic `ChildResult` list round-trips through the Delta schema unchanged. Catches schema drift. |
| Smoke | Synapse (manual) | Two no-op child notebooks in `notebooks/_smoke/`. Run-on-deploy check, not in CI. |

`conftest.py` carries:

```python
import pytest

@pytest.fixture
def fake_mssparkutils(monkeypatch):
    """Inject a configurable stand-in for mssparkutils.notebook.run."""
    # Implementation in tests/conftest.py.
```

`pyproject.toml` `[project.optional-dependencies]`:

```toml
spark = ["pyspark", "delta-spark"]
test  = ["pyspark", "delta-spark", "pytest"]
dev   = ["jupytext"]
```

`dependencies = []` at top level. Matches spark_lib.

## `.py` → `.ipynb` build

`notebooks/_logging/lgr.py` is authored in jupytext "percent" format:

```python
# %% [markdown]
# # lgr
# Orchestrates child notebooks via mssparkutils.notebook.run and writes a
# Delta log row per child.

# %% tags=["parameters"]
from typing import Any, Dict, List

notebooks: List[Dict[str, Any]] = []
log_table: str = "_meta.__pipeline_runlog"
pipeline_name: str = ""
fail_fast: bool = True

# %%
from spark_az.lgr import run_pipeline

run_pipeline(
    notebooks,
    log_table=log_table,
    pipeline_name=pipeline_name,
    fail_fast=fail_fast,
)
```

`scripts/build_notebooks.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail
jupytext --to ipynb notebooks/*.py
```

CI verifies `.py` and `.ipynb` stay in sync via `jupytext --test
notebooks/*.py`. The `tags=["parameters"]` cell is recognized by both Synapse
pipeline notebook activities and Papermill, so the same notebook is
parametrizable from a Synapse pipeline activity or a local test driver.

## File-by-file punch list

| File | Purpose |
| --- | --- |
| `src/spark_az/__init__.py` | Re-export `run_pipeline`, `run_child`, `ChildSpec`, `ChildResult`, `ensure_log_table`, `get_spark`, `set_spark`. |
| `src/spark_az/session.py` | `get_spark` / `set_spark` ported from spark_lib. |
| `src/spark_az/lgr.py` | `LOG_SCHEMA`, `ChildSpec`, `ChildResult`, `ensure_log_table`, `run_child`, `run_pipeline`, `_print_line`, `_append_rows`, `_skipped_result`, `_truncate`, `_nbutils`. |
| `notebooks/_logging/lgr.py` | Jupytext-format orchestrator notebook. |
| `notebooks/_logging/lgr.ipynb` | Generated; checked in. |
| `scripts/build_notebooks.sh` | Wraps `jupytext --to ipynb`. |
| `tests/conftest.py` | `fake_mssparkutils` fixture; local `SparkSession` fixture (delta-spark configured). |
| `tests/test_lgr.py` | Pure-Python unit tests. |
| `tests/test_lgr_delta.py` | Local-Spark schema roundtrip. |
| `pyproject.toml` | `dependencies = []`; `[project.optional-dependencies]` for `spark` / `test` / `dev`; `[tool.pytest.ini_options]` and packages-find as in spark_lib. |

## Scope boundary

**In v1:**

- Sequential orchestration via `mssparkutils.notebook.run`.
- One Delta row per child, batched single-append per `run_pipeline` call.
- Stdout cell output.
- `fail_fast=True` default with reconstructed-exception re-raise.

**Explicitly out of v1 (future specs):**

- **Parallel orchestration.** A `run_pipeline_parallel` over a thread pool
  with FAIR-pool support, mirroring `spark_lib.cleanup.run_parallel`.
- **Step-grain logging inside children.** A `%run`-able mini-library exposing
  a `step()` context manager that emits step rows keyed by the same
  `pipeline_run_id`. Children would opt in; the schema would gain a
  `child_index` foreign key.
- **Synapse pipeline JSON authoring.** A Python builder that emits Synapse
  pipeline JSON with retries, dependencies, and notebook activities. The
  orchestrator notebook stays the unit Synapse calls; the JSON layer just
  parameterizes which orchestrator notebook to run.
- **`pipeline_run_id` propagation from a Synapse pipeline.** Today we
  generate the UUID in the orchestrator. v2 will accept an injected
  `pipeline_run_id` from `@pipeline().RunId` so the same ID joins the
  Synapse pipeline activity log with our Delta log.
- **Retry semantics inside `run_pipeline`.** v1 surfaces failure; the pipeline
  JSON above is responsible for retrying the whole orchestrator notebook.

**Not pursuing:**

- A `Sink` protocol with pluggable destinations (Azure Monitor, Log
  Analytics). Add when a second concrete sink is genuinely needed.
- Capturing stdlib `logging` records emitted inside children. The child's own
  cell output is preserved by Synapse; duplicating it as Delta rows is more
  data than value at v1.
