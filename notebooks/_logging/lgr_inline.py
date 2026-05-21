# ---
# jupyter:
#   jupytext:
#     formats: py:percent,ipynb
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# # `lgr_inline`
#
# A single self-contained Synapse notebook that bundles the entire
# `spark_az.lgr` library. No `pip install spark-az`, no wheel
# upload to ADLS, no bootstrap dance — drop this notebook into your
# Synapse workspace and you have a structured pipeline-logging layer
# available immediately.
#
# It's both a library notebook AND a runnable notebook:
#
# - **Library mode** — `%run ./lgr_inline` from any other
#   Synapse notebook. Every public name lands in the caller's scope.
#   With default parameters (`notebooks=[]`) the run cell at the bottom
#   is a no-op, so `%run` has zero side effects.
# - **Run mode** — set the parameter cell from a Synapse pipeline
#   notebook activity (or by editing in place), run all cells. The
#   bottom cell calls `run_pipeline(...)` and writes one Delta row per
#   child notebook executed.

# %% [markdown]
# ## What this notebook gives you
#
# | Symbol | Purpose |
# | --- | --- |
# | `run_pipeline(children, *, log_table, pipeline_name, …)` | Sequentially run a list of child notebooks via `mssparkutils.notebook.run`, capture exit values and exceptions, write one Delta row per child, and re-raise on first failure (unless `fail_fast=False`). |
# | `run_child(spec, *, pipeline_run_id, pipeline_name, child_index, …)` | Run a single child notebook. Never raises — returns a `ChildResult`. |
# | `ensure_log_table(table)` | Idempotently create the Delta log table with the canonical schema. |
# | `ChildSpec` | `TypedDict` describing one child to run: `path` (required), `args`, `timeout_seconds`, `name` (optional). |
# | `ChildResult` | `TypedDict` describing one row written to the log: status, durations, error class/message/traceback, etc. |
# | `set_spark(spark)` / `get_spark()` | Register or retrieve the active `SparkSession`. Synapse normally provides one automatically. |
# | `log` | `logging.Logger` named `spark_az.lgr`. Attach an `AzureLogHandler` to fan out to Application Insights. |
#
# **Status values written to the Delta log:** `"ok"`, `"failed"`,
# `"timeout"`, `"skipped"`. Every child in your input list always
# produces a row — the table by itself answers "what was supposed to
# run, what ran, what didn't."

# %% [markdown]
# ## Example — library mode
#
# In a separate Synapse notebook:
#
# ```python
# %run ./lgr_inline
#
# results = run_pipeline(
#     [
#         {"path": "/notebooks/extract",   "args": {"date": "2026-05-21"}},
#         {"path": "/notebooks/transform"},
#         {"path": "/notebooks/load",      "timeout_seconds": 3600},
#     ],
#     log_table="lab.__pipeline_runlog",
#     pipeline_name="nightly_lab_refresh",
# )
#
# import logging
# logging.getLogger("spark_az.lgr").setLevel(logging.INFO)
# ```
#
# Query the log later with SQL:
#
# ```sql
# SELECT pipeline_run_id, child_index, notebook_path, status,
#        duration_ms / 1000 AS seconds, error_class, error_message
# FROM   lab.__pipeline_runlog
# WHERE  pipeline_name = 'nightly_lab_refresh'
# ORDER  BY pipeline_run_id DESC, child_index;
# ```

# %% [markdown]
# ## Example — run mode (pipeline activity)
#
# Configure a Synapse pipeline **Notebook activity** to point at this
# notebook and pass JSON arguments:
#
# ```json
# {
#   "notebooks": [
#     {"path": "/notebooks/extract"},
#     {"path": "/notebooks/transform"},
#     {"path": "/notebooks/load"}
#   ],
#   "log_table": "lab.__pipeline_runlog",
#   "pipeline_name": "nightly_lab_refresh",
#   "fail_fast": true
# }
# ```
#
# Synapse replaces the values in the parameter cell below at runtime.
# The final cell calls `run_pipeline(...)` and the orchestrator notebook
# either succeeds or raises — failure propagates to the Synapse
# pipeline so it can retry or alert.

# %% [markdown]
# ## Parameters
#
# Synapse will replace these defaults at runtime when the notebook is
# invoked from a pipeline activity. Empty `notebooks` makes the run
# cell at the bottom a no-op, so `%run` users in library mode are
# unaffected.

# %% tags=["parameters"]
from __future__ import annotations

from typing import Any, Dict, List

notebooks: "List[Dict[str, Any]]" = []
log_table: str = "lab.__pipeline_runlog"
pipeline_name: str = ""
fail_fast: bool = True
default_timeout_seconds: int = 1800

# %% [markdown]
# ## Session helpers
#
# Look up the active `SparkSession` without ever calling
# `SparkSession.builder.getOrCreate()`. Synapse pre-creates the
# session; calling `getOrCreate` from notebook code can fight the
# runtime. `set_spark()` is for local scripts and tests where Spark
# isn't pre-active.

# %%
import json
import logging
import os
import time
import traceback
import uuid
from datetime import datetime, timezone
from typing import (
    TYPE_CHECKING,
    Iterable,
    Optional,
    Tuple,
    TypedDict,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType  # noqa: F401 - used in annotations

_spark: Optional["SparkSession"] = None


def set_spark(session: "SparkSession") -> None:
    """Register the SparkSession used by the inline lgr.

    Use this in local scripts/tests or any runtime where Spark does not
    expose an active session. Synapse notebooks usually do not need it
    because Spark is already active before user code runs.

    Args:
        session: A live ``SparkSession``.
    """
    global _spark
    _spark = session


def get_spark() -> "SparkSession":
    """Return the registered or active SparkSession.

    This intentionally avoids ``SparkSession.builder.getOrCreate()`` so
    imports do not mutate the runtime or fight Synapse's pre-created
    session.

    Returns:
        The active SparkSession.

    Raises:
        RuntimeError: No registered session and no active session.
    """
    if _spark is not None:
        return _spark

    active: Optional["SparkSession"] = _active_spark_session()
    if active is not None:
        return active

    raise RuntimeError(
        "No active SparkSession found. In Synapse this should usually be "
        "available automatically; otherwise call set_spark(spark) once "
        "before reading, writing, or running Spark jobs."
    )


def _active_spark_session() -> Optional["SparkSession"]:
    """Return Spark's active session without creating one."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        return None
    return SparkSession.getActiveSession()


# %% [markdown]
# ## Logger setup
#
# A module-scoped `logging.Logger` named `spark_az.lgr`.
# Idempotent handler setup with a `propagate=False` so we don't double-log
# through pytest's root capture during tests. Per-child stdout lines go
# here via `log.info(...)` — attach an `AzureLogHandler` to fan out to
# Application Insights without changing any code in this notebook.

# %%
_HANDLER_NAME: str = "spark_az.lgr.default"

log: logging.Logger = logging.getLogger("spark_az.lgr")
if not any(h.get_name() == _HANDLER_NAME for h in log.handlers):
    _handler: logging.Handler = logging.StreamHandler()
    _handler.set_name(_HANDLER_NAME)
    _handler.setFormatter(
        logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        )
    )
    log.addHandler(_handler)
    log.setLevel(logging.INFO)
    log.propagate = False

# %% [markdown]
# ## Schema and TypedDicts
#
# `LOG_SCHEMA_FIELDS` is the single source of truth for the Delta log
# table's columns. `ChildSpec` describes one child to run; `ChildResult`
# describes one row written to the log per child invocation. Note that
# `ChildSpec.path` is required (enforced via a `TypedDict` mixin) while
# the other keys are optional.

# %%
LOG_SCHEMA_FIELDS: List[Tuple[str, str]] = [
    ("pipeline_run_id", "string"),
    ("pipeline_name", "string"),
    ("child_index", "long"),
    ("notebook_path", "string"),
    ("status", "string"),
    ("started_at", "timestamp"),
    ("finished_at", "timestamp"),
    ("duration_ms", "long"),
    ("exit_value", "string"),
    ("args_json", "string"),
    ("error_class", "string"),
    ("error_message", "string"),
    ("error_traceback", "string"),
    ("orchestrator_notebook", "string"),
    ("audited_at", "timestamp"),
]


class _ChildSpecRequired(TypedDict):
    """Required fields of :class:`ChildSpec`. Internal helper."""

    path: str


class ChildSpec(_ChildSpecRequired, total=False):
    """One child notebook to run.

    Fields:
        path: Required. Synapse workspace path passed to
            ``mssparkutils.notebook.run``.
        timeout_seconds: Optional. Defaults to the caller-supplied
            ``default_timeout_seconds``.
        args: Optional. Arguments forwarded to the child notebook.
        name: Optional display name for stdout. Defaults to the basename
            of ``path``.

    Examples:
        >>> spec: ChildSpec = {
        ...     "path": "/notebooks/extract",
        ...     "args": {"date": "2026-05-21"},
        ...     "timeout_seconds": 600,
        ... }
    """

    timeout_seconds: int
    args: Dict[str, Any]
    name: str


class ChildResult(TypedDict):
    """One row written to the log table per child invocation.

    Field semantics:
        status: ``"ok"`` | ``"failed"`` | ``"timeout"`` | ``"skipped"``.
        exit_value: Whatever the child returned via
            ``mssparkutils.notebook.exit(...)``. Stored as a string for
            forward compatibility.
        args_json: ``json.dumps(args, default=str)`` for reproducibility.
        error_class / error_message / error_traceback: Populated when
            ``status != "ok"``. Empty strings otherwise.
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

# %% [markdown]
# ## Spark / Synapse helpers
#
# `_log_schema()` lazy-builds the Spark `StructType` so the notebook
# parses without `pyspark` installed (Synapse provides it at runtime).
# `_nbutils()` resolves `mssparkutils` through whichever import path
# the runtime offers. `ensure_log_table()` creates the Delta log table
# if it doesn't already exist.

# %%
def _log_schema() -> "StructType":
    """Build the Spark schema for the log table.

    Returns:
        ``StructType`` with all columns ``nullable=False``.
    """
    from pyspark.sql.types import (
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    type_map: Dict[str, Any] = {
        "string": StringType(),
        "long": LongType(),
        "timestamp": TimestampType(),
    }
    return StructType(
        [
            StructField(name, type_map[type_name], False)
            for name, type_name in LOG_SCHEMA_FIELDS
        ]
    )


def _nbutils() -> Any:
    """Return Synapse's ``mssparkutils`` regardless of which path imports it.

    Returns:
        The ``mssparkutils`` module-like object.

    Raises:
        RuntimeError: Neither ``notebookutils.mssparkutils`` nor
            ``mssparkutils`` is importable.
    """
    try:
        from notebookutils import mssparkutils

        return mssparkutils
    except ImportError:
        try:
            import mssparkutils

            return mssparkutils
        except ImportError:
            raise RuntimeError(
                "mssparkutils / notebookutils not importable; "
                "lgr_inline must run inside Azure Synapse."
            )


def ensure_log_table(table: str) -> None:
    """Create the log Delta table if it does not exist.

    Idempotent. Checks ``spark.catalog.tableExists(table)``; otherwise
    writes an empty DataFrame with :func:`_log_schema` as a managed Delta
    table.

    Args:
        table: Fully-qualified managed Delta table name.

    Examples:
        >>> ensure_log_table("lab.__pipeline_runlog")
    """
    spark: Any = get_spark()
    if spark.catalog.tableExists(table):
        return
    (
        spark.createDataFrame([], _log_schema())
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable(table)
    )

# %% [markdown]
# ## Small utilities
#
# String truncation, timestamp helpers, args JSON serialization, and the
# `_skipped_result` builder used to record children that didn't get to
# run because an earlier `fail_fast=True` failure cancelled them.

# %%
def _truncate(text: str, *, limit: int) -> str:
    """Truncate ``text`` to at most ``limit`` chars + a marker suffix.

    The marker suffix is intentionally outside the limit budget so callers
    can reason about the leading content unambiguously.
    """
    if len(text) <= limit:
        return text
    return text[:limit] + "…[truncated]"


def _now_iso() -> str:
    """Return the current UTC time as an ISO 8601 string with microseconds."""
    return datetime.now(timezone.utc).isoformat()


def _serialize_args(args: Optional[Dict[str, Any]]) -> str:
    """Serialize a child's args dict for storage in ``args_json``.

    Uses ``default=str`` so unusual values (datetimes, paths) survive
    without raising.
    """
    return json.dumps(args or {}, default=str, sort_keys=True)


def _skipped_result(
    spec: ChildSpec,
    *,
    pipeline_run_id: str,
    pipeline_name: str,
    child_index: int,
    orchestrator_notebook: str,
) -> ChildResult:
    """Build a :class:`ChildResult` for a child that was skipped by ``fail_fast``."""
    now: str = _now_iso()
    return {
        "pipeline_run_id": pipeline_run_id,
        "pipeline_name": pipeline_name,
        "child_index": child_index,
        "notebook_path": spec["path"],
        "status": "skipped",
        "started_at": now,
        "finished_at": now,
        "duration_ms": 0,
        "exit_value": "",
        "args_json": _serialize_args(spec.get("args")),
        "error_class": "",
        "error_message": "",
        "error_traceback": "",
        "orchestrator_notebook": orchestrator_notebook,
    }

# %% [markdown]
# ## Stdout formatting
#
# `_print_line` emits one human-readable line per child via the module
# logger. Output looks like:
#
# ```
# 14:02:11 [INFO] spark_az.lgr: [14:02:11] [OK]     extract           1.83s  exit=42rows
# 14:02:13 [INFO] spark_az.lgr: [14:02:13] [FAIL]   transform         0.42s  ValueError: missing column 'id'
# 14:02:13 [INFO] spark_az.lgr: [14:02:13] [SKIP]   load                     (fail_fast)
# ```

# %%
_STATUS_BADGE: Dict[str, str] = {
    "ok": "OK",
    "failed": "FAIL",
    "timeout": "TIME",
    "skipped": "SKIP",
}

_STDOUT_NAME_WIDTH: int = 18
_STDOUT_BADGE_WIDTH: int = 6
_STDOUT_EXIT_MAX: int = 40
_STDOUT_ERROR_MAX: int = 80


def _print_line(result: ChildResult, *, display_name: str) -> None:
    """Emit one human-readable log line for a finished child.

    Logged at ``INFO`` to ``spark_az.lgr`` so users can attach
    additional handlers (e.g. ``AzureLogHandler``) without changes here.
    """
    badge: str = _STATUS_BADGE.get(result["status"], result["status"].upper())
    badge_field: str = f"[{badge}]".ljust(_STDOUT_BADGE_WIDTH + 2)
    name_field: str = display_name[:_STDOUT_NAME_WIDTH].ljust(_STDOUT_NAME_WIDTH)
    clock: str = _now_clock()

    if result["status"] == "skipped":
        suffix: str = "(fail_fast)"
        duration_field: str = " " * 7
    else:
        duration_field = f"{result['duration_ms'] / 1000:>6.2f}s"
        if result["status"] == "ok":
            exit_text: str = _truncate(result["exit_value"], limit=_STDOUT_EXIT_MAX)
            suffix = f"exit={exit_text}" if exit_text else ""
        else:
            message: str = _truncate(
                result["error_message"], limit=_STDOUT_ERROR_MAX
            )
            suffix = f"{result['error_class']}: {message}".strip(": ")

    log.info(
        "[%s] %s %s %s  %s",
        clock,
        badge_field,
        name_field,
        duration_field,
        suffix,
    )


def _now_clock() -> str:
    """Return the current local wall clock as ``HH:MM:SS``."""
    return datetime.now().strftime("%H:%M:%S")

# %% [markdown]
# ## `run_child` — execute one child notebook
#
# Calls `mssparkutils.notebook.run(path, timeout_seconds, args)`, times
# it, and maps the outcome to a `ChildResult`. Never raises — exceptions
# become `status="failed"` rows (or `status="timeout"` if the exception
# message contains a timeout hint). Decision to re-raise lives in
# `run_pipeline`.

# %%
_TIMEOUT_HINTS: List[str] = ["timeout", "timed out"]


def _orchestrator_notebook_name() -> str:
    """Best-effort lookup of the calling notebook's name from runtime context.

    Returns an empty string if unavailable. Errors are swallowed because
    the field is decorative — losing it must not fail a run.
    """
    try:
        nb: Any = _nbutils()
        context: Any = getattr(getattr(nb, "runtime", None), "context", None)
        if context is None:
            return ""
        name: Any = context["currentNotebookName"]
        return str(name) if name else ""
    except Exception:
        return ""


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
    captured into the result.

    Status mapping:

    - Returns normally → ``"ok"``; ``exit_value`` is ``str(returned)``.
    - Raises with ``"timeout"`` or ``"timed out"`` in the exception
      message → ``"timeout"``.
    - Any other exception → ``"failed"``.
    """
    nb: Any = _nbutils()
    timeout: int = int(spec.get("timeout_seconds", default_timeout_seconds))
    args: Dict[str, Any] = dict(spec.get("args", {}))
    args_json: str = _serialize_args(args)
    orchestrator: str = _orchestrator_notebook_name()

    started_iso: str = _now_iso()
    started_mono: float = time.monotonic()
    try:
        returned: Any = nb.notebook.run(spec["path"], timeout, args)
    except BaseException as exc:
        finished_iso: str = _now_iso()
        duration_ms: int = int((time.monotonic() - started_mono) * 1000)
        message: str = str(exc)
        status: str = (
            "timeout"
            if any(h in message.lower() for h in _TIMEOUT_HINTS)
            else "failed"
        )
        return {
            "pipeline_run_id": pipeline_run_id,
            "pipeline_name": pipeline_name,
            "child_index": child_index,
            "notebook_path": spec["path"],
            "status": status,
            "started_at": started_iso,
            "finished_at": finished_iso,
            "duration_ms": duration_ms,
            "exit_value": "",
            "args_json": args_json,
            "error_class": type(exc).__name__,
            "error_message": _truncate(message, limit=4096),
            "error_traceback": _truncate(
                traceback.format_exc(), limit=16384
            ),
            "orchestrator_notebook": orchestrator,
        }

    finished_iso = _now_iso()
    duration_ms = int((time.monotonic() - started_mono) * 1000)
    return {
        "pipeline_run_id": pipeline_run_id,
        "pipeline_name": pipeline_name,
        "child_index": child_index,
        "notebook_path": spec["path"],
        "status": "ok",
        "started_at": started_iso,
        "finished_at": finished_iso,
        "duration_ms": duration_ms,
        "exit_value": str(returned) if returned is not None else "",
        "args_json": args_json,
        "error_class": "",
        "error_message": "",
        "error_traceback": "",
        "orchestrator_notebook": orchestrator,
    }

# %% [markdown]
# ## Delta writer
#
# `_append_rows` batches every `ChildResult` from one `run_pipeline()`
# call into a single Delta append. One commit per call, not one per
# row. `audited_at` is stamped server-side with `current_timestamp()`.

# %%
def _append_rows(table: str, results: List[ChildResult]) -> None:
    """Append a batch of :class:`ChildResult` rows to ``table``.

    Stamps ``audited_at = current_timestamp()`` at write time. One Delta
    commit per call regardless of batch size.
    """
    if not results:
        return
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    spark: Any = get_spark()
    write_schema: StructType = StructType(
        [
            StructField(name, _string_or_long(type_name), False)
            for name, type_name in LOG_SCHEMA_FIELDS
            if name not in {"audited_at", "started_at", "finished_at"}
        ]
        + [
            StructField("started_at", StringType(), False),
            StructField("finished_at", StringType(), False),
        ]
    )
    column_order: List[str] = [f.name for f in write_schema.fields]
    rows: List[Dict[str, Any]] = [
        {name: r[name] for name in column_order} for r in results
    ]
    df = spark.createDataFrame(rows, write_schema).select(
        *[
            F.to_timestamp(F.col(c)).alias(c)
            if c in {"started_at", "finished_at"}
            else F.col(c)
            for c in column_order
        ]
    ).withColumn("audited_at", F.current_timestamp())
    df.write.format("delta").mode("append").saveAsTable(table)


def _string_or_long(type_name: str) -> Any:
    """Map our schema type names to Spark types for the staging frame."""
    from pyspark.sql.types import LongType, StringType

    if type_name == "long":
        return LongType()
    return StringType()

# %% [markdown]
# ## `run_pipeline` — orchestrator entry point
#
# Generates one `pipeline_run_id` (UUID4) for the call. Runs each child
# sequentially via `run_child`. Prints a line per child. Batches all
# log rows into one Delta append at the end. Re-raises (after the
# append) on first failure when `fail_fast=True`.

# %%
def _display_name(spec: ChildSpec) -> str:
    """Resolve the display name for log lines.

    Returns the explicit ``name`` from the spec if provided, otherwise
    the basename of ``path``.
    """
    explicit: Optional[str] = spec.get("name")
    if explicit:
        return str(explicit)
    return os.path.basename(str(spec["path"]).rstrip("/")) or str(spec["path"])


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

    Generates one ``pipeline_run_id`` (UUID4) for the call. For each
    child: calls ``mssparkutils.notebook.run``, captures wall time +
    exit value + exception, emits one log line, appends a
    :class:`ChildResult` to the in-memory list.

    After the loop, the full result list is written to ``log_table`` in
    one Delta append (when ``write_log=True``). If ``fail_fast=True``
    and any child failed, the captured failure is re-raised AFTER the
    log write so the orchestrator notebook itself fails in Synapse and
    the log table is durable for post-mortem.

    Args:
        children: Iterable of :class:`ChildSpec` entries.
        log_table: Fully-qualified managed Delta table for the log rows.
            Created via :func:`ensure_log_table` if missing.
        pipeline_name: Caller-supplied label stamped on every row.
        fail_fast: When ``True`` (default), the first failure marks
            remaining children as ``status="skipped"`` and the call
            re-raises after the log write. When ``False``, every child
            is attempted and the call returns normally with failures
            captured as rows.
        default_timeout_seconds: Used when a :class:`ChildSpec` does not
            specify its own ``timeout_seconds``.
        write_log: When ``False``, emits stdout but skips the Delta
            write. Used by tests and dry runs.

    Returns:
        When ``fail_fast=False`` or all children succeeded: the full
        ``List[ChildResult]`` in input order, including ``"skipped"``
        rows if any. When ``fail_fast=True`` and a child failed, the
        function re-raises instead of returning.

    Raises:
        RuntimeError: ``mssparkutils`` / ``notebookutils`` not importable
            (not running in Synapse). Raised before the child loop.
        RuntimeError: Re-raised after the log write when
            ``fail_fast=True`` and any child failed. The exception
            carries the first failing child's ``error_class`` and
            ``error_message``.
    """
    _nbutils()

    pipeline_run_id: str = str(uuid.uuid4())
    orchestrator: str = _orchestrator_notebook_name()
    spec_list: List[ChildSpec] = list(children)
    results: List[ChildResult] = []
    first_failure: Optional[ChildResult] = None

    try:
        for i, spec in enumerate(spec_list):
            if first_failure is not None and fail_fast:
                skipped: ChildResult = _skipped_result(
                    spec,
                    pipeline_run_id=pipeline_run_id,
                    pipeline_name=pipeline_name,
                    child_index=i,
                    orchestrator_notebook=orchestrator,
                )
                results.append(skipped)
                _print_line(skipped, display_name=_display_name(spec))
                continue
            result: ChildResult = run_child(
                spec,
                pipeline_run_id=pipeline_run_id,
                pipeline_name=pipeline_name,
                child_index=i,
                default_timeout_seconds=default_timeout_seconds,
            )
            results.append(result)
            _print_line(result, display_name=_display_name(spec))
            if result["status"] != "ok" and first_failure is None:
                first_failure = result
    finally:
        if write_log:
            ensure_log_table(log_table)
            _append_rows(log_table, results)

    if first_failure is not None and fail_fast:
        raise RuntimeError(
            f"child {first_failure['child_index']} "
            f"({first_failure['notebook_path']}) "
            f"{first_failure['status']}: "
            f"{first_failure['error_class']}: "
            f"{first_failure['error_message']}"
        )
    return results


__public_api__: List[str] = [
    "ChildResult",
    "ChildSpec",
    "ensure_log_table",
    "get_spark",
    "log",
    "run_child",
    "run_pipeline",
    "set_spark",
]

__all__: List[str] = list(__public_api__)

# %% [markdown]
# ## Run
#
# This cell calls `run_pipeline(...)` if `notebooks` is non-empty.
# That makes the notebook safe to `%run` for library mode (empty
# default → no-op) and useful for direct-run mode (set parameters
# from a Synapse pipeline activity, run all → executes children).
#
# Results are exposed as the variable `pipeline_results` for downstream
# cells or for the caller to inspect.

# %%
from typing import cast as _cast

pipeline_results: "List[ChildResult]" = []
if notebooks:
    pipeline_results = run_pipeline(
        _cast("List[ChildSpec]", notebooks),
        log_table=log_table,
        pipeline_name=pipeline_name,
        fail_fast=fail_fast,
        default_timeout_seconds=default_timeout_seconds,
    )

# %% [markdown]
# ## Maintenance
#
# This notebook is **hand-maintained** in sync with the canonical
# library in `src/spark_az/session.py` and `src/spark_az/lgr.py`.
# When the library changes, regenerate this notebook by copying the
# new bodies in, then run `bash scripts/build_notebooks.sh` to rebuild
# the `.ipynb` from the `.py`.
