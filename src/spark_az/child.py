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
from __future__ import annotations

import json
import time
import traceback
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Optional

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


_logged_outcome: bool = False
_hook_registered: bool = False


def _ipython() -> Any:
    """Return the active IPython shell, or ``None`` outside IPython."""
    try:
        from IPython import get_ipython
    except Exception:
        return None
    return get_ipython()


def _ns_value(key: str, default: str) -> str:
    """Read ``key`` from the notebook namespace, else ``default``.

    The child's own parameters cell defines ``log_table`` /
    ``pipeline_run_id`` / ``pipeline_name`` as globals, so :func:`log_done`
    and the failure hook pick them up with no arguments.
    """
    shell: Any = _ipython()
    if shell is not None:
        value: Any = shell.user_ns.get(key, default)
        return str(value) if value else default
    return default


def _log_outcome(
    status: str,
    *,
    error: Optional[BaseException] = None,
    log_table: Optional[str] = None,
    pipeline_run_id: Optional[str] = None,
    fields: Optional[Dict[str, Any]] = None,
) -> None:
    """Append one self-row describing THIS notebook's outcome.

    ``log_table`` / ``pipeline_run_id`` default to the notebook-namespace
    values (then the standard defaults) so callers can pass nothing.

    Args:
        status: ``"ok"`` | ``"failed"`` | free-form.
        error: Optional caught exception recorded on the row.
        log_table: Override the runlog table.
        pipeline_run_id: Override the run id.
        fields: Optional extra JSON fields recorded in the exit payload.
    """
    global _logged_outcome
    table: str = log_table or _ns_value("log_table", "_meta.__pipeline_runlog")
    run_id: str = (
        pipeline_run_id
        if pipeline_run_id is not None
        else _ns_value("pipeline_run_id", "")
    )
    result: ChildResult = _self_result(
        status=status,
        pipeline_run_id=run_id,
        pipeline_name=_ns_value("pipeline_name", ""),
        child_index=-1,
        exit_value=build_exit_payload(
            status, pipeline_run_id=run_id, error=error, fields=fields
        ),
        args=None,
        error=_format_error(error),
    )
    ensure_log_table(table)
    _append_rows(table, [result])
    _logged_outcome = True
    log.info("logged status=%s to %s", status, table)


def log_done(**fields: Any) -> None:
    """Bottom line: record that this notebook finished OK.

    Logs one ``status="ok"`` row (notebook name, duration,
    ``pipeline_run_id``) to the runlog table, reading ``log_table`` /
    ``pipeline_run_id`` from the notebook's parameters cell so the common
    case takes no arguments. A no-op if an outcome was already logged (e.g.
    the failure hook fired first).

    Args:
        **fields: Optional extra JSON fields to record on the row.

    Examples:
        >>> log_done()
        >>> log_done(target="lake.orders")
    """
    if _logged_outcome:
        return
    _log_outcome("ok", fields=fields or None)


def _on_cell(result: Any) -> None:
    """``post_run_cell`` callback: log a failed row on the first cell error.

    Observational only — it never swallows the exception, so the notebook
    still fails and the pipeline still sees it. Its own errors are swallowed
    so logging can never mask the original exception.

    Args:
        result: The IPython ``ExecutionResult``; ``error_in_exec`` holds the
            cell's exception or ``None``.
    """
    if _logged_outcome:
        return
    error: Optional[BaseException] = getattr(result, "error_in_exec", None)
    if error is None:
        return
    try:
        _log_outcome("failed", error=error)
    except Exception as exc:
        log.warning("auto-log of failure could not write: %s", exc)


def install_logging() -> None:
    """Top line: start the timer and arm automatic failure logging.

    Records the run start (durations measure from here) and, under
    IPython/Synapse, registers a ``post_run_cell`` hook that logs a
    ``status="failed"`` row if any later cell raises — the case a bottom-line
    call can't catch, since the crash stops the notebook before it. Outside
    IPython (plain scripts / tests) it only resets the timer.

    Idempotent: the hook registers at most once per session. Called for you
    from ``lgr_child``'s ``%run`` setup cell, so a notebook's two lines are
    ``%run`` at the top and :func:`log_done` at the end.
    """
    global _hook_registered, _logged_outcome
    global _child_started_mono, _child_started_iso
    _logged_outcome = False
    _child_started_mono = time.monotonic()
    _child_started_iso = _now_iso()
    shell: Any = _ipython()
    if shell is None:
        return
    if not _hook_registered:
        shell.events.register("post_run_cell", _on_cell)
        _hook_registered = True


@contextmanager
def log_run(
    log_table: Optional[str] = None,
    pipeline_run_id: Optional[str] = None,
) -> Iterator[None]:
    """Wrap a notebook body to log ok/failed + duration whether it crashes.

    The robust alternative to the ``%run`` + :func:`log_done` hook when the
    IPython hook can't be used: plain ``try`` / re-raise semantics that work
    in any runtime. Logs ``status="ok"`` on a clean exit, ``status="failed"``
    with the error on an exception (then re-raises so the notebook fails).

    Args:
        log_table: Optional runlog-table override.
        pipeline_run_id: Optional run-id override.

    Examples:
        >>> with log_run():
        ...     do_work()
    """
    global _logged_outcome, _child_started_mono, _child_started_iso
    _logged_outcome = False
    _child_started_mono = time.monotonic()
    _child_started_iso = _now_iso()
    try:
        yield
    except BaseException as exc:
        _log_outcome(
            "failed",
            error=exc,
            log_table=log_table,
            pipeline_run_id=pipeline_run_id,
        )
        raise
    _log_outcome("ok", log_table=log_table, pipeline_run_id=pipeline_run_id)


__all__ = [
    "build_exit_payload",
    "install_logging",
    "log_done",
    "log_run",
    "notebook_exit",
]
