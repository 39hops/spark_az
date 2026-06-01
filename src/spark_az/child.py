"""Child-side logging and structured pipeline exit for ``%run`` use.

``%run`` the generated ``notebooks/lgr_child.ipynb`` at the very top of any
Synapse child notebook to pull JSON logging, the :func:`spark_az.lgr.step`
timer, and :func:`notebook_exit` into the child's own namespace with zero
install::

    %run /Shared/lgr_child

    with step("extract") as s:
        df = read_source()
        s.metric("rows", df.count())

    notebook_exit(
        "ok",
        log_table=log_table,
        pipeline_run_id=pipeline_run_id,
        rows=df.count(),
        watermark="2026-06-01",
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
from __future__ import annotations

import json
import time
import traceback
from typing import Any, Dict, Optional

from .lgr import (
    ChildResult,
    _append_rows,
    _nbutils,
    _now_iso,
    _orchestrator_notebook_name,
    _serialize_args,
    _truncate,
    ensure_log_table,
    log,
)

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
            watermarks, downstream-branching flags).

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
            row counts, watermarks, or flags downstream activities branch on.

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
