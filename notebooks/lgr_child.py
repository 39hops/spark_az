# ---
# jupyter:
#   jupytext:
#     formats: py:percent,ipynb
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.3
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# # lgr_child — drop-in child logger
# `%run` this at the very top of a Synapse child notebook to pull JSON
# logging, `step()`, and `notebook_exit()` into scope. There is no
# parameters cell here on purpose, so `%run` never overwrites the caller's
# pipeline-injected parameters. Setup and pipeline wiring live in the README.

# %% [markdown]
# ## Library
# Hand-synced from `src/spark_az/` via `scripts/inline_lgr_child_notebook.py`. Don't edit here.

# %%
from __future__ import annotations

"""SparkSession lookup helpers.

The package never creates a SparkSession. Synapse notebooks already provide
one, and local PySpark jobs should register or activate one explicitly.
"""

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_spark: Optional["SparkSession"] = None


def set_spark(session: "SparkSession") -> None:
    """Register the SparkSession used by spark_az.

    Use this in local scripts/tests or any runtime where Spark does not
    expose an active session. Synapse notebooks usually do not need it
    because Spark is already active before user code runs.

    Args:
        session: A live ``SparkSession``.

    Examples:
        In a Synapse notebook (rarely needed):

        >>> from spark_az import set_spark
        >>> set_spark(spark)

        In a local script:

        >>> from pyspark.sql import SparkSession
        >>> set_spark(SparkSession.builder.getOrCreate())
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

    Examples:
        >>> spark = get_spark()
        >>> spark.sql("SELECT 1").collect()
        [Row(1=1)]
    """
    if _spark is not None:
        return _spark

    active: Optional["SparkSession"] = _active_spark_session()
    if active is not None:
        return active

    raise RuntimeError(
        "No active SparkSession found. In Synapse this should usually be "
        "available automatically; otherwise call spark_az.set_spark(spark) "
        "once before reading, writing, or running Spark jobs."
    )


def _active_spark_session() -> Optional["SparkSession"]:
    """Return Spark's active session without creating one."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        return None
    return SparkSession.getActiveSession()


__all__ = ["get_spark", "set_spark"]

"""Synapse orchestrator + Delta logging.

Run a sequence of child notebooks via ``mssparkutils.notebook.run`` from one
orchestrator notebook. Write one structured Delta row per child describing
status, duration, exit value, and any captured exception.

Public API
----------
- :class:`ChildSpec` — describes one child notebook to run.
- :class:`ChildResult` — describes one row written to the log table.
- :class:`PipelineParams` — validated Synapse-passed parameter bag.
- :func:`ensure_log_table` — idempotent log-table creation.
- :func:`read_pipeline_params` — validate + build a ``PipelineParams``.
- :func:`run_child` — run one child; never raises.
- :func:`run_pipeline` — run many in sequence with batched logging.
- :func:`step` — context manager for in-orchestrator structured timing.
- :class:`JsonFormatter` + :func:`set_json_formatter` — opt-in JSON
  log lines on stdout.
- :func:`enable_app_insights` — opt-in Azure App Insights / Log
  Analytics fan-out via ``azure-monitor-opentelemetry``.
- :func:`set_active_run_id` / :func:`get_active_run_id` — module
  singleton consulted by ``step`` and set automatically by
  ``run_pipeline``.

Conventions
-----------
- The log table is a managed Delta table (e.g. ``"_meta.__pipeline_runlog"``).
- Per-child output goes through ``logging.Logger`` ``spark_az.lgr``
  at ``INFO``; attach an ``AzureLogHandler`` (or call
  :func:`enable_app_insights`) to fan out without touching this module.
- ``mssparkutils.notebook.run`` is blocking; orchestration is sequential.
"""

import json
import logging
import os
import time
import traceback
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    TypedDict,
)


if TYPE_CHECKING:
    from pyspark.sql.types import StructType


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


def _log_schema() -> "StructType":
    """Build the Spark schema for the log table.

    Returns:
        ``StructType`` with all columns ``nullable=False``.

    Examples:
        >>> _log_schema().fieldNames()[:2]
        ['pipeline_run_id', 'pipeline_name']
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


def _truncate(text: str, *, limit: int) -> str:
    """Truncate ``text`` to at most ``limit`` chars + a marker suffix.

    The marker suffix is intentionally outside the limit budget so callers
    can reason about the leading content unambiguously.

    Args:
        text: Source string. May be empty.
        limit: Maximum number of source characters to keep.

    Returns:
        ``text`` itself if shorter than ``limit``; otherwise the first
        ``limit`` characters followed by ``"…[truncated]"``.

    Examples:
        >>> _truncate("hello", limit=10)
        'hello'
        >>> _truncate("x" * 50, limit=3)
        'xxx…[truncated]'
    """
    if len(text) <= limit:
        return text
    return text[:limit] + "…[truncated]"


def _nbutils() -> Any:
    """Return Synapse's ``mssparkutils`` regardless of which path imports it.

    Returns:
        The ``mssparkutils`` module-like object (real in Synapse, stubbed
        in tests).

    Raises:
        RuntimeError: Neither ``notebookutils.mssparkutils`` nor
            ``mssparkutils`` is importable. This happens when run outside
            Synapse without the test fake installed.
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
                "spark_az.lgr must run inside Azure Synapse."
            )


def ensure_log_table(table: str) -> None:
    """Create the log Delta table (and its database) if missing.

    Idempotent. Mirrors :meth:`SyncState.ensure` in spark_lib: checks
    ``spark.catalog.tableExists(table)``; otherwise runs
    ``CREATE DATABASE IF NOT EXISTS <db>`` for any database prefix on
    ``table`` then writes an empty DataFrame with :func:`_log_schema`
    as a managed Delta table.

    Args:
        table: Fully-qualified managed Delta table name
            (e.g. ``"_meta.__pipeline_runlog"``). A bare table name
            without a database prefix is also accepted.

    Examples:
        >>> ensure_log_table("_meta.__pipeline_runlog")
    """
    spark: Any = get_spark()
    if spark.catalog.tableExists(table):
        return
    if "." in table:
        db: str = table.split(".", 1)[0]
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    (
        spark.createDataFrame([], _log_schema())
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable(table)
    )


def _now_iso() -> str:
    """Return the current UTC time as an ISO 8601 string with microseconds.

    Returns:
        ``"YYYY-MM-DDTHH:MM:SS.ffffff+00:00"``.
    """
    return datetime.now(timezone.utc).isoformat()


def _serialize_args(args: Optional[Dict[str, Any]]) -> str:
    """Serialize a child's args dict for storage in ``args_json``.

    Uses ``default=str`` so unusual values (datetimes, paths) survive
    without raising.

    Args:
        args: The child's args dict. ``None`` and ``{}`` both round-trip
            to ``"{}"``.

    Returns:
        A compact JSON string.
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
    """Build a ``ChildResult`` for a child that was skipped by ``fail_fast``.

    Every NOT NULL log column gets a sensible default. ``started_at`` and
    ``finished_at`` are set to the moment the skip is recorded — they are
    not real durations.

    Args:
        spec: The child that did not run.
        pipeline_run_id: UUID shared across the ``run_pipeline()`` call.
        pipeline_name: Caller-supplied label.
        child_index: Zero-based position in the input list.
        orchestrator_notebook: Best-effort notebook name from runtime
            context. Empty string if unavailable.

    Returns:
        A ``ChildResult`` with ``status="skipped"``.

    Examples:
        >>> r = _skipped_result(
        ...     {"path": "/x"},
        ...     pipeline_run_id="r", pipeline_name="p",
        ...     child_index=0, orchestrator_notebook="",
        ... )
        >>> r["status"]
        'skipped'
    """
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

    Format::

        [hh:mm:ss] [STATUS] <name 18ch> <duration>  <suffix>

    - Duration is omitted for ``skipped``.
    - Suffix is ``exit=<value>`` on ok, ``<error_class>: <message>`` on
      failed/timeout, ``(fail_fast)`` on skipped.

    Args:
        result: The :class:`ChildResult` being reported.
        display_name: Pre-resolved display name (caller chooses spec
            ``name`` or basename of ``path``).
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
    captured into the result. The decision to re-raise lives in
    :func:`run_pipeline` so this function stays composable for future
    parallel orchestration.

    Status mapping:

    - Returns normally → ``"ok"``; ``exit_value`` is ``str(returned)``.
    - Raises with ``"timeout"`` or ``"timed out"`` in the exception message
      → ``"timeout"``.
    - Any other exception → ``"failed"``.

    Args:
        spec: The child to run.
        pipeline_run_id: UUID shared across one ``run_pipeline()`` call.
        pipeline_name: Caller-supplied label.
        child_index: Zero-based position in the input list.
        default_timeout_seconds: Used when ``spec["timeout_seconds"]`` is
            absent.

    Returns:
        A :class:`ChildResult` describing the outcome.

    Examples:
        >>> result = run_child(
        ...     {"path": "/notebooks/extract", "args": {"date": "2026-05-21"}},
        ...     pipeline_run_id="r",
        ...     pipeline_name="p",
        ...     child_index=0,
        ... )
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


def _append_rows(table: str, results: List[ChildResult]) -> None:
    """Append a batch of :class:`ChildResult` rows to ``table``.

    Stamps ``audited_at = current_timestamp()`` at write time. One Delta
    commit per call regardless of batch size — same trick as
    ``SyncState.upsert_all`` in spark_lib.

    Args:
        table: Fully-qualified managed Delta table name.
        results: Rows to append. Empty list is a no-op.

    Examples:
        >>> _append_rows("_meta.__pipeline_runlog", [...])
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


def _display_name(spec: ChildSpec) -> str:
    """Resolve the display name for log lines.

    Returns the explicit ``name`` from the spec if provided, otherwise the
    basename of ``path``. Falls back to the raw path when basename is
    empty (e.g. paths ending in a slash).
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
    pipeline_run_id: Optional[str] = None,
) -> List[ChildResult]:
    """Run a sequence of child notebooks and log each one.

    Generates one ``pipeline_run_id`` (UUID4) for the call. For each child:

    1. Calls ``mssparkutils.notebook.run(path, timeout_seconds, args)``.
    2. Captures wall time, exit value, and exception (if any).
    3. Emits one stdout line via the module logger.
    4. Appends a :class:`ChildResult` to the in-memory result list.

    After the loop, the full result list is written to ``log_table`` in
    one Delta append (when ``write_log=True``). If ``fail_fast=True`` and
    any child failed, the captured failure is re-raised AFTER the log
    write so the orchestrator notebook itself fails in Synapse and the
    log table is durable for post-mortem.

    Args:
        children: Iterable of :class:`ChildSpec` entries.
        log_table: Fully-qualified managed Delta table for the log rows.
            Created via :func:`ensure_log_table` if missing.
        pipeline_name: Caller-supplied label stamped on every row.
        fail_fast: When ``True`` (default), the first failure marks
            remaining children as ``status="skipped"`` and the call
            re-raises after the log write. When ``False``, every child is
            attempted and the call returns normally with failures captured
            as rows.
        default_timeout_seconds: Used when a :class:`ChildSpec` does not
            specify its own ``timeout_seconds``.
        write_log: When ``False``, emits stdout but skips the Delta
            write. Used by tests and dry runs.

    Returns:
        When ``fail_fast=False`` or all children succeeded: the full
        ``List[ChildResult]`` in input order, including ``"skipped"``
        rows if any. When ``fail_fast=True`` and a child failed, the
        function re-raises instead of returning — the full list is
        durable in ``log_table`` for the caller to query.

    Raises:
        RuntimeError: ``mssparkutils`` / ``notebookutils`` not importable
            (not running in Synapse). Raised before the child loop.
        RuntimeError: Re-raised after the log write when ``fail_fast=True``
            and any child failed. The exception carries the first failing
            child's ``error_class`` and ``error_message``.

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

        >>> results = run_pipeline(
        ...     specs,
        ...     log_table="_meta.__pipeline_runlog",
        ...     pipeline_name="p",
        ...     fail_fast=False,
        ... )
        >>> failed = [r for r in results if r["status"] != "ok"]
    """
    _nbutils()

    resolved_run_id: str = pipeline_run_id or str(uuid.uuid4())
    set_active_run_id(resolved_run_id)
    orchestrator: str = _orchestrator_notebook_name()
    spec_list: List[ChildSpec] = list(children)
    results: List[ChildResult] = []
    first_failure: Optional[ChildResult] = None

    try:
        for i, spec in enumerate(spec_list):
            if first_failure is not None and fail_fast:
                skipped: ChildResult = _skipped_result(
                    spec,
                    pipeline_run_id=resolved_run_id,
                    pipeline_name=pipeline_name,
                    child_index=i,
                    orchestrator_notebook=orchestrator,
                )
                results.append(skipped)
                _print_line(skipped, display_name=_display_name(spec))
                continue
            result: ChildResult = run_child(
                spec,
                pipeline_run_id=resolved_run_id,
                pipeline_name=pipeline_name,
                child_index=i,
                default_timeout_seconds=default_timeout_seconds,
            )
            results.append(result)
            _print_line(result, display_name=_display_name(spec))
            if result["status"] != "ok" and first_failure is None:
                first_failure = result
    finally:
        set_active_run_id(None)
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


_active_run_id: Optional[str] = None


def set_active_run_id(run_id: Optional[str]) -> None:
    """Set or clear the ``pipeline_run_id`` auto-attached to step records.

    :func:`run_pipeline` sets this for the duration of its call so any
    :func:`step` invocations inside children automatically carry the
    same run id. Direct callers (orchestrator notebooks that use
    :func:`step` without :func:`run_pipeline`) can set it themselves.

    Args:
        run_id: A UUID string, or ``None`` to clear.
    """
    global _active_run_id
    _active_run_id = run_id


def get_active_run_id() -> Optional[str]:
    """Return the pipeline_run_id currently attached to step records.

    Returns:
        The active run id, or ``None`` if none is set.
    """
    return _active_run_id


_JSON_RECORD_KEYS: Tuple[str, ...] = (
    "pipeline_run_id",
    "pipeline_name",
    "child_index",
    "step",
    "phase",
    "duration_ms",
    "error_class",
    "error_message",
)


class JsonFormatter(logging.Formatter):
    """:class:`logging.Formatter` that emits one JSON object per record.

    Always-present fields: ``ts`` (ISO 8601 UTC), ``level``, ``logger``,
    ``msg``. Any of ``pipeline_run_id``, ``pipeline_name``,
    ``child_index``, ``step``, ``phase``, ``duration_ms``,
    ``error_class``, ``error_message`` attached to the record via
    ``log.info(..., extra=...)`` are included when present. Exception
    tracebacks land under ``exc_info``. Arbitrary extra keys flow
    through as-is so callers can attach custom structured fields.

    Examples:
        >>> import logging
        >>> handler = logging.StreamHandler()
        >>> handler.setFormatter(JsonFormatter())
        >>> log.addHandler(handler)
    """

    def format(self, record: logging.LogRecord) -> str:
        """Serialize ``record`` as a single-line JSON object.

        Args:
            record: The log record from the standard logging machinery.

        Returns:
            A JSON string (no trailing newline; the handler adds one).
        """
        payload: Dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key in _JSON_RECORD_KEYS:
            value: Any = getattr(record, key, None)
            if value is not None:
                payload[key] = value
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def set_json_formatter(level: int = logging.INFO) -> None:
    """Swap the default ``spark_az.lgr`` handler to :class:`JsonFormatter`.

    Idempotent. Targets only the handler installed by this module
    (identified by name) so other handlers attached by the user — caplog,
    App Insights — are not touched.

    Args:
        level: Optional ``log.setLevel`` adjustment.

    Examples:
        At the top of an orchestrator notebook:

        >>> set_json_formatter()
        >>> log.info("hello", extra={"step": "boot"})
    """
    log.setLevel(level)
    for h in log.handlers:
        if h.get_name() == _HANDLER_NAME:
            h.setFormatter(JsonFormatter())
            return


_APP_INSIGHTS_ENABLED: bool = False


def enable_app_insights(connection_string: str) -> None:
    """Wire the module logger to Azure App Insights / Log Analytics.

    Requires the optional ``azure-monitor-opentelemetry`` dependency.
    Idempotent: successive calls are no-ops in the same process.

    Args:
        connection_string: App Insights / Log Analytics connection
            string (``"InstrumentationKey=...;..."``).

    Raises:
        ImportError: ``azure.monitor.opentelemetry`` is not installed.
            The message names the package to install.

    Examples:
        >>> enable_app_insights("InstrumentationKey=...;IngestionEndpoint=...")
    """
    global _APP_INSIGHTS_ENABLED
    if _APP_INSIGHTS_ENABLED:
        return
    try:
        from azure.monitor.opentelemetry import configure_azure_monitor
    except ImportError as exc:
        raise ImportError(
            "enable_app_insights requires the optional "
            "`azure-monitor-opentelemetry` dependency. "
            "Install with: pip install azure-monitor-opentelemetry"
        ) from exc
    configure_azure_monitor(
        connection_string=connection_string,
        logger_name="spark_az.lgr",
    )
    _APP_INSIGHTS_ENABLED = True


class _StepContext:
    """The object yielded by :func:`step` — exposes ``metric(key, value)``."""

    def __init__(self) -> None:
        self.metrics: Dict[str, Any] = {}

    def metric(self, key: str, value: Any) -> None:
        """Record a metric to attach to the step's success log record.

        Args:
            key: Field name in the emitted JSON.
            value: Any JSON-serializable value (or anything ``str()``-able
                — :class:`JsonFormatter` uses ``default=str``).

        Examples:
            >>> with step("extract") as s:
            ...     s.metric("rows_in", 1234)
        """
        self.metrics[key] = value


@contextmanager
def step(name: str, **attrs: Any) -> Iterator[_StepContext]:
    """Time a logical step and emit structured log records.

    Logs three records to ``spark_az.lgr``:

    - INFO on entry: ``{"step": name, "phase": "start", **attrs}``.
    - INFO on success exit: ``{"step": name, "phase": "ok",
      "duration_ms": ..., **attrs, **metrics}``.
    - ERROR on exception: ``{"step": name, "phase": "failed",
      "duration_ms": ..., "error_class": ..., "error_message": ...,
      **attrs}``. The exception is then re-raised.

    The active ``pipeline_run_id`` (set via :func:`set_active_run_id` or
    automatically by :func:`run_pipeline`) is attached to every record.

    Args:
        name: Step name. Free-form.
        **attrs: Extra structured fields attached to every record for
            this step.

    Yields:
        A :class:`_StepContext` exposing ``metric(key, value)`` to
        accumulate counts/values into the success log record.

    Examples:
        >>> with step("extract", source="lab.raw") as s:
        ...     df = source.read()
        ...     s.metric("rows_in", df.count())
    """
    ctx: _StepContext = _StepContext()
    base: Dict[str, Any] = {"step": name, **attrs}
    if _active_run_id is not None:
        base["pipeline_run_id"] = _active_run_id

    started: float = time.monotonic()
    log.info("step start: %s", name, extra={**base, "phase": "start"})

    try:
        yield ctx
    except BaseException as exc:
        duration_ms: int = int((time.monotonic() - started) * 1000)
        log.error(
            "step failed: %s (%s)",
            name,
            type(exc).__name__,
            extra={
                **base,
                "phase": "failed",
                "duration_ms": duration_ms,
                "error_class": type(exc).__name__,
                "error_message": _truncate(str(exc), limit=4096),
            },
        )
        raise

    duration_ms = int((time.monotonic() - started) * 1000)
    log.info(
        "step ok: %s (%dms)",
        name,
        duration_ms,
        extra={**base, "phase": "ok", "duration_ms": duration_ms, **ctx.metrics},
    )


class PipelineParams(TypedDict, total=False):
    """Standard Synapse pipeline parameters consumed by the starter notebook.

    Fields:
        pipeline_run_id: Optional Synapse-injected run id (e.g. from
            ``@pipeline().RunId``). When present,
            :func:`run_pipeline` uses it instead of generating a UUID.
        pipeline_name: Caller-supplied label stamped on every row.
        log_table: Fully-qualified Delta log table name.
        fail_fast: Whether to stop the orchestrator on first failure.
        default_timeout_seconds: Per-child timeout default.
        notebooks: List of :class:`ChildSpec`-shaped dicts.
        extras: Free-form caller-defined bag, not interpreted by
            :func:`run_pipeline` but available downstream.

    Examples:
        >>> params: PipelineParams = read_pipeline_params(
        ...     pipeline_name="nightly",
        ...     log_table="_meta.__pipeline_runlog",
        ...     notebooks=[{"path": "/x"}],
        ... )
    """

    pipeline_run_id: str
    pipeline_name: str
    log_table: str
    fail_fast: bool
    default_timeout_seconds: int
    notebooks: List[Dict[str, Any]]
    extras: Dict[str, Any]


def read_pipeline_params(
    *,
    pipeline_name: str,
    log_table: str,
    notebooks: List[Dict[str, Any]],
    fail_fast: bool = True,
    default_timeout_seconds: int = 1800,
    pipeline_run_id: Optional[str] = None,
    extras: Optional[Dict[str, Any]] = None,
) -> PipelineParams:
    """Build a validated :class:`PipelineParams` from raw Synapse-passed args.

    Validates the minimum invariants the orchestrator expects so
    misconfigured pipelines fail fast at the parameter cell, not deep
    inside :func:`run_pipeline`.

    Args:
        pipeline_name: Required. Non-empty caller label.
        log_table: Required. Non-empty fully-qualified Delta table name.
        notebooks: Required. List of dicts; each must contain a
            non-empty string ``"path"``.
        fail_fast: Defaults to ``True``.
        default_timeout_seconds: Defaults to 1800.
        pipeline_run_id: Optional Synapse-injected run id.
        extras: Optional free-form bag.

    Returns:
        A validated :class:`PipelineParams`.

    Raises:
        ValueError: ``pipeline_name`` empty; ``log_table`` empty;
            ``notebooks`` not a list of dicts each containing a
            non-empty string ``path``.

    Examples:
        >>> params = read_pipeline_params(
        ...     pipeline_name="nightly",
        ...     log_table="_meta.__pipeline_runlog",
        ...     notebooks=[
        ...         {"path": "/notebooks/extract"},
        ...         {"path": "/notebooks/load", "timeout_seconds": 3600},
        ...     ],
        ...     pipeline_run_id="r-123",
        ... )
    """
    if not pipeline_name:
        raise ValueError("pipeline_name must be non-empty")
    if not log_table:
        raise ValueError("log_table must be non-empty")
    if not isinstance(notebooks, list):
        raise ValueError("notebooks must be a list")
    for i, child in enumerate(notebooks):
        if not isinstance(child, dict):
            raise ValueError(
                f"notebooks[{i}] must be a dict, got {type(child).__name__}"
            )
        path: Any = child.get("path")
        if not isinstance(path, str) or not path:
            raise ValueError(
                f"notebooks[{i}] missing required non-empty string field 'path'"
            )

    params: PipelineParams = {
        "pipeline_name": pipeline_name,
        "log_table": log_table,
        "notebooks": list(notebooks),
        "fail_fast": fail_fast,
        "default_timeout_seconds": default_timeout_seconds,
    }
    if pipeline_run_id:
        params["pipeline_run_id"] = pipeline_run_id
    if extras:
        params["extras"] = dict(extras)
    return params


__all__ = [
    "ChildResult",
    "ChildSpec",
    "JsonFormatter",
    "LOG_SCHEMA_FIELDS",
    "PipelineParams",
    "enable_app_insights",
    "ensure_log_table",
    "get_active_run_id",
    "log",
    "read_pipeline_params",
    "set_active_run_id",
    "set_json_formatter",
    "step",
    "run_child",
    "run_pipeline",
]

"""Child-side logging and structured pipeline exit for ``%run`` use.

``%run`` the generated ``notebooks/lgr_child.ipynb`` at the very top of any
Synapse child notebook to pull JSON logging, the :func:`spark_az.lgr.step`
timer, and :func:`notebook_exit` into the child's own namespace with zero
install::

    %run /Shared/lgr_child

    with step("write_orders"):
        write_target()

    notebook_exit(
        "ok",
        log_table=log_table,
        pipeline_run_id=pipeline_run_id,
        target="lake.orders",
    )

:func:`notebook_exit` JSON-encodes its payload and hands it to the pipeline
via ``mssparkutils.notebook.exit``. The pipeline reads it back as
``@activity('<name>').output.status.Output.result.exitValue`` and can branch
on any field. When ``write_log`` is set, the same call also appends one row
describing this notebook to the Delta runlog the orchestrator uses, keyed by
``pipeline_run_id`` so self-logged children join the orchestrator's table.

Module-import time is captured so a self-logged row's ``duration_ms``
measures the child's own wall time from the ``%run`` onward.
"""

import json
import time
import traceback
from typing import Any, Dict, Optional


_EXIT_VALUE_MAX: int = 8192

_child_started_mono: float = time.monotonic()
_child_started_iso: str = _now_iso()


def _format_error(error: Optional[BaseException]) -> Dict[str, str]:
    """Return the ``error_*`` fields for an optional caught exception.

    Args:
        error: A caught exception, or ``None``.

    Returns:
        A dict with ``error_class``, ``error_message``, ``error_traceback``;
        all empty strings when ``error`` is ``None``.
    """
    if error is None:
        return {"error_class": "", "error_message": "", "error_traceback": ""}
    return {
        "error_class": type(error).__name__,
        "error_message": str(error),
        "error_traceback": "".join(
            traceback.format_exception(type(error), error, error.__traceback__)
        ),
    }


def build_exit_payload(
    status: str,
    *,
    pipeline_run_id: str = "",
    error: Optional[BaseException] = None,
    fields: Optional[Dict[str, Any]] = None,
) -> str:
    """Build the JSON string handed to the pipeline as the exit value.

    The Synapse pipeline can only receive a string from a notebook, so
    structured information is JSON-encoded here and parsed back in the
    pipeline with ``@json(...)``.

    Args:
        status: The child's outcome (``"ok"``, ``"failed"``, ...).
        pipeline_run_id: Stamped into the payload when non-empty so the
            pipeline can correlate the result with its run.
        error: Optional caught exception; its class and message are added.
        fields: Arbitrary JSON-serialisable values to include (row counts,
            dataset names, downstream-branching flags).

    Returns:
        A compact, key-sorted JSON string.

    Examples:
        >>> build_exit_payload("ok", fields={"rows": 10})
        '{"finished_at": "...", "rows": 10, "status": "ok"}'
    """
    payload: Dict[str, Any] = {"status": status, "finished_at": _now_iso()}
    if pipeline_run_id:
        payload["pipeline_run_id"] = pipeline_run_id
    if error is not None:
        payload["error_class"] = type(error).__name__
        payload["error_message"] = str(error)
    if fields:
        payload.update(fields)
    return json.dumps(payload, default=str, sort_keys=True)


def _self_result(
    *,
    status: str,
    pipeline_run_id: str,
    pipeline_name: str,
    child_index: int,
    exit_value: str,
    args: Optional[Dict[str, Any]],
    error: Dict[str, str],
) -> ChildResult:
    """Build a :class:`spark_az.lgr.ChildResult` row describing THIS notebook.

    Pure function — no Spark — so it is unit-testable without a session.
    ``notebook_path`` is the current notebook name from runtime context;
    ``orchestrator_notebook`` is left empty because a self-logging child has
    no orchestrator above it. ``started_at`` is the moment ``%run`` brought
    this module in; ``duration_ms`` is measured from that point.

    Args:
        status: The child's outcome.
        pipeline_run_id: Synapse run id so the row joins the orchestrator's.
        pipeline_name: Caller-supplied label.
        child_index: ``-1`` marks a self-logged row not sequenced by an
            orchestrator; a pipeline may pass a real index.
        exit_value: The JSON payload string handed to the pipeline.
        args: The child's parameters, serialised into ``args_json``.
        error: Output of :func:`_format_error`.

    Returns:
        A fully-populated :class:`spark_az.lgr.ChildResult`.
    """
    return {
        "pipeline_run_id": pipeline_run_id,
        "pipeline_name": pipeline_name,
        "child_index": child_index,
        "notebook_path": _orchestrator_notebook_name(),
        "status": status,
        "started_at": _child_started_iso,
        "finished_at": _now_iso(),
        "duration_ms": int((time.monotonic() - _child_started_mono) * 1000),
        "exit_value": _truncate(exit_value, limit=_EXIT_VALUE_MAX),
        "args_json": _serialize_args(args),
        "error_class": error["error_class"],
        "error_message": _truncate(error["error_message"], limit=4096),
        "error_traceback": _truncate(error["error_traceback"], limit=16384),
        "orchestrator_notebook": "",
    }


def notebook_exit(
    status: str = "ok",
    *,
    log_table: Optional[str] = None,
    pipeline_run_id: str = "",
    pipeline_name: str = "",
    child_index: int = -1,
    args: Optional[Dict[str, Any]] = None,
    error: Optional[BaseException] = None,
    write_log: bool = True,
    **fields: Any,
) -> None:
    """Return structured info to the pipeline, optionally self-logging first.

    Builds a JSON payload from ``status`` + ``**fields`` (plus any ``error``),
    appends one self-describing row to ``log_table`` when ``write_log`` is
    set, then calls ``mssparkutils.notebook.exit`` with the JSON string. The
    Delta write happens BEFORE the exit because ``notebook.exit`` ends the
    notebook.

    Run this via ``%run`` at the top of a child, then pass the
    pipeline-injected ``pipeline_run_id`` and ``log_table`` through from the
    child's own parameters cell.

    Args:
        status: ``"ok"`` | ``"failed"`` | ``"timeout"`` | free-form label.
        log_table: Fully-qualified Delta runlog table. Required when
            ``write_log`` is True.
        pipeline_run_id: ``@pipeline().RunId``, so the self-row joins the
            orchestrator's rows and the payload carries the correlation id.
        pipeline_name: Caller label stamped on the self-row.
        child_index: Position if known; ``-1`` (default) marks a self-logged
            row not sequenced by an orchestrator.
        args: The child's parameters, recorded in ``args_json``.
        error: A caught exception to record on the row and in the payload.
        write_log: When True (default), append one self-row before exiting.
            Set False to return a payload without touching Delta.
        **fields: Arbitrary JSON-serialisable values added to the payload —
            dataset names, row counts, or flags downstream activities branch
            on. For row counts, prefer Delta write metrics over a costly
            ``count()``.

    Raises:
        ValueError: ``write_log`` is True but ``log_table`` is empty.
        RuntimeError: ``mssparkutils`` is unavailable (not in Synapse).

    Examples:
        >>> notebook_exit(
        ...     "ok",
        ...     log_table="_meta.__pipeline_runlog",
        ...     pipeline_run_id="run-1",
        ...     rows=42,
        ... )
    """
    payload_json: str = build_exit_payload(
        status,
        pipeline_run_id=pipeline_run_id,
        error=error,
        fields=fields or None,
    )

    if write_log:
        if not log_table:
            raise ValueError(
                "notebook_exit: log_table is required when write_log=True"
            )
        result: ChildResult = _self_result(
            status=status,
            pipeline_run_id=pipeline_run_id,
            pipeline_name=pipeline_name,
            child_index=child_index,
            exit_value=payload_json,
            args=args,
            error=_format_error(error),
        )
        ensure_log_table(log_table)
        _append_rows(log_table, [result])
        log.info("self-logged status=%s to %s", status, log_table)

    _nbutils().notebook.exit(payload_json)


__all__ = ["build_exit_payload", "notebook_exit"]

# %% [markdown]
# ## Setup
# Turn on JSON-structured stdout logging for the child.

# %%
set_json_formatter()

# %% [markdown]
# ## Usage (reference — this runs in the child, not here)
# In the child notebook, AFTER its own parameters cell:
#
# ```python
# %run /Shared/lgr_child
#
# with step("write_orders"):
#     write_target()
#
# notebook_exit(
#     "ok",
#     log_table=log_table,
#     pipeline_run_id=pipeline_run_id,
#     target="lake.orders",
# )
# ```
#
# Attach any JSON-serialisable fields you want the pipeline to see; for row
# counts read Delta write metrics, not a costly `count()`. Wrap risky work to
# self-log a failure row and still hand the pipeline a structured result:
#
# ```python
# try:
#     run_the_work()
#     notebook_exit("ok", log_table=log_table, pipeline_run_id=pipeline_run_id)
# except Exception as exc:
#     notebook_exit("failed", log_table=log_table,
#                   pipeline_run_id=pipeline_run_id, error=exc)
# ```
#
# The pipeline reads the structured result back with:
# `@json(activity('<child>').output.status.Output.result.exitValue).status`
