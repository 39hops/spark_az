"""Synapse orchestrator + Delta logging.

Run a sequence of child notebooks via ``mssparkutils.notebook.run`` from one
orchestrator notebook. Write one structured Delta row per child describing
status, duration, exit value, and any captured exception.

Public API
----------
- :class:`ChildSpec` — describes one child notebook to run.
- :class:`ChildResult` — describes one row written to the log table.
- :func:`ensure_log_table` — idempotent log-table creation.
- :func:`run_child` — run one child; never raises.
- :func:`run_pipeline` — run many in sequence with batched logging.

Conventions
-----------
- The log table is a managed Delta table (e.g. ``"lab.__pipeline_runlog"``).
- Per-child output goes through ``logging.Logger`` ``spark_az.pipeline_logger``
  at ``INFO``; attach an ``AzureLogHandler`` to fan out to Application
  Insights without touching this module.
- ``mssparkutils.notebook.run`` is blocking; orchestration is sequential
  in v1.
"""
from __future__ import annotations

import json
import logging
import os
import time
import traceback
import uuid
from datetime import datetime, timezone
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    TypedDict,
)

from .session import get_spark

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


_HANDLER_NAME: str = "spark_az.pipeline_logger.default"

log: logging.Logger = logging.getLogger("spark_az.pipeline_logger")
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
                "spark_az.pipeline_logger must run inside Azure Synapse."
            )


def ensure_log_table(table: str) -> None:
    """Create the log Delta table if it does not exist.

    Idempotent. Mirrors :meth:`SyncState.ensure` in spark_lib: checks
    ``spark.catalog.tableExists(table)``; otherwise writes an empty
    DataFrame with :func:`_log_schema` as a managed Delta table.

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

    Logged at ``INFO`` to ``spark_az.pipeline_logger`` so users can attach
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
        >>> _append_rows("lab.__pipeline_runlog", [...])
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
        ...     log_table="lab.__pipeline_runlog",
        ...     pipeline_name="nightly_lab_refresh",
        ... )

        Run-all (continue past failures):

        >>> results = run_pipeline(
        ...     specs,
        ...     log_table="lab.__pipeline_runlog",
        ...     pipeline_name="p",
        ...     fail_fast=False,
        ... )
        >>> failed = [r for r in results if r["status"] != "ok"]
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


__all__ = [
    "ChildResult",
    "ChildSpec",
    "LOG_SCHEMA_FIELDS",
    "ensure_log_table",
    "log",
    "run_child",
    "run_pipeline",
]
