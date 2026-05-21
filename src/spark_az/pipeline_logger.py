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
- Stdout is plain ``print()`` — audience is the Synapse cell output.
- ``mssparkutils.notebook.run`` is blocking; orchestration is sequential
  in v1.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    TypedDict,
)

from .session import get_spark

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


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


class ChildSpec(TypedDict, total=False):
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

    path: str
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


__all__ = [
    "ChildResult",
    "ChildSpec",
    "LOG_SCHEMA_FIELDS",
    "ensure_log_table",
]
