"""Unit tests for spark_az.pipeline_logger."""
from __future__ import annotations

from typing import Any, Dict, List, Set, get_type_hints

import sys

import pytest


def test_log_schema_fields_are_complete() -> None:
    """LOG_SCHEMA_FIELDS must list every column in the spec."""
    import spark_az.pipeline_logger as pl

    expected_names: List[str] = [
        "pipeline_run_id",
        "pipeline_name",
        "child_index",
        "notebook_path",
        "status",
        "started_at",
        "finished_at",
        "duration_ms",
        "exit_value",
        "args_json",
        "error_class",
        "error_message",
        "error_traceback",
        "orchestrator_notebook",
        "audited_at",
    ]
    actual_names: List[str] = [name for name, _ in pl.LOG_SCHEMA_FIELDS]
    assert actual_names == expected_names


def test_log_schema_fields_use_known_types() -> None:
    """Every column type must be one of the documented spark type names."""
    import spark_az.pipeline_logger as pl

    allowed: Set[str] = {"string", "long", "timestamp"}
    types_used: Set[str] = {t for _, t in pl.LOG_SCHEMA_FIELDS}
    assert types_used <= allowed


def test_childresult_keys_match_audit_columns_minus_audited_at() -> None:
    """ChildResult covers every log column except audited_at."""
    import spark_az.pipeline_logger as pl

    schema_names: List[str] = [name for name, _ in pl.LOG_SCHEMA_FIELDS]
    childresult_keys: List[str] = list(
        get_type_hints(pl.ChildResult).keys()
    )
    assert childresult_keys == [n for n in schema_names if n != "audited_at"]


def test_childspec_total_false() -> None:
    """ChildSpec is a partial TypedDict (total=False)."""
    import spark_az.pipeline_logger as pl

    spec: pl.ChildSpec = {"path": "/x"}
    assert spec["path"] == "/x"


def test_truncate_under_limit_returns_input() -> None:
    from spark_az.pipeline_logger import _truncate

    assert _truncate("hello", limit=100) == "hello"


def test_truncate_over_limit_appends_marker() -> None:
    from spark_az.pipeline_logger import _truncate

    out: str = _truncate("x" * 50, limit=20)
    assert out.startswith("x" * 20)
    assert out.endswith("…[truncated]")
    assert len(out) <= 20 + len("…[truncated]")


def test_truncate_empty_string_passes_through() -> None:
    from spark_az.pipeline_logger import _truncate

    assert _truncate("", limit=10) == ""


def test_nbutils_returns_module_when_notebookutils_present(
    fake_mssparkutils: Any,
) -> None:
    from spark_az.pipeline_logger import _nbutils

    nb: Any = _nbutils()
    assert nb is fake_mssparkutils


def test_nbutils_raises_when_neither_module_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "notebookutils", None)
    monkeypatch.setitem(sys.modules, "mssparkutils", None)
    from spark_az.pipeline_logger import _nbutils

    with pytest.raises(RuntimeError, match="mssparkutils"):
        _nbutils()


def test_skipped_result_has_expected_fields() -> None:
    from spark_az.pipeline_logger import ChildSpec, _skipped_result

    spec: ChildSpec = {"path": "/notebooks/load", "args": {"k": "v"}}
    result = _skipped_result(
        spec,
        pipeline_run_id="run-1",
        pipeline_name="nightly",
        child_index=2,
        orchestrator_notebook="/notebooks/orch",
    )

    assert result["status"] == "skipped"
    assert result["pipeline_run_id"] == "run-1"
    assert result["pipeline_name"] == "nightly"
    assert result["child_index"] == 2
    assert result["notebook_path"] == "/notebooks/load"
    assert result["duration_ms"] == 0
    assert result["exit_value"] == ""
    assert result["args_json"] == '{"k": "v"}'
    assert result["error_class"] == ""
    assert result["error_message"] == ""
    assert result["error_traceback"] == ""
    assert result["orchestrator_notebook"] == "/notebooks/orch"
    assert result["started_at"] == result["finished_at"]
    assert result["started_at"] != ""


import logging


def _result_template() -> Dict[str, Any]:
    return {
        "pipeline_run_id": "r",
        "pipeline_name": "p",
        "child_index": 0,
        "notebook_path": "/notebooks/extract",
        "status": "ok",
        "started_at": "2026-05-21T12:00:00+00:00",
        "finished_at": "2026-05-21T12:00:01.830000+00:00",
        "duration_ms": 1830,
        "exit_value": "42rows",
        "args_json": "{}",
        "error_class": "",
        "error_message": "",
        "error_traceback": "",
        "orchestrator_notebook": "",
    }


def test_module_logger_is_configured() -> None:
    from spark_az.pipeline_logger import log

    assert log.name == "spark_az.pipeline_logger"
    assert log.propagate is False
    assert any(isinstance(h, logging.StreamHandler) for h in log.handlers)


def test_print_line_ok_status(caplog: pytest.LogCaptureFixture) -> None:
    from spark_az.pipeline_logger import _print_line, log

    result: Dict[str, Any] = _result_template()
    with caplog.at_level(logging.INFO, logger=log.name):
        _print_line(result, display_name="extract")

    messages: List[str] = [r.getMessage() for r in caplog.records]
    assert any("[OK]" in m for m in messages)
    assert any("extract" in m for m in messages)
    assert any("1.83s" in m for m in messages)
    assert any("exit=42rows" in m for m in messages)


def test_print_line_failed_status(caplog: pytest.LogCaptureFixture) -> None:
    from spark_az.pipeline_logger import _print_line, log

    result: Dict[str, Any] = _result_template()
    result["status"] = "failed"
    result["duration_ms"] = 420
    result["exit_value"] = ""
    result["error_class"] = "ValueError"
    result["error_message"] = "missing column 'id'"
    with caplog.at_level(logging.INFO, logger=log.name):
        _print_line(result, display_name="transform")

    messages: List[str] = [r.getMessage() for r in caplog.records]
    assert any("[FAIL]" in m for m in messages)
    assert any("transform" in m for m in messages)
    assert any("0.42s" in m for m in messages)
    assert any("ValueError: missing column 'id'" in m for m in messages)


def test_print_line_skipped_status(caplog: pytest.LogCaptureFixture) -> None:
    from spark_az.pipeline_logger import _print_line, log

    result: Dict[str, Any] = _result_template()
    result["status"] = "skipped"
    result["duration_ms"] = 0
    result["exit_value"] = ""
    with caplog.at_level(logging.INFO, logger=log.name):
        _print_line(result, display_name="load")

    messages: List[str] = [r.getMessage() for r in caplog.records]
    assert any("[SKIP]" in m for m in messages)
    assert any("load" in m for m in messages)
    assert any("(fail_fast)" in m for m in messages)


def test_print_line_timeout_status(caplog: pytest.LogCaptureFixture) -> None:
    from spark_az.pipeline_logger import _print_line, log

    result: Dict[str, Any] = _result_template()
    result["status"] = "timeout"
    result["duration_ms"] = 1800000
    result["error_class"] = "RuntimeError"
    result["error_message"] = "notebook timed out after 1800 seconds"
    with caplog.at_level(logging.INFO, logger=log.name):
        _print_line(result, display_name="extract")

    messages: List[str] = [r.getMessage() for r in caplog.records]
    assert any("[TIME]" in m for m in messages)
    assert any("1800.00s" in m for m in messages)
    assert any("timed out" in m for m in messages)
