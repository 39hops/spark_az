"""Unit tests for spark_az.child (no Spark required).

The Delta self-logging path (``notebook_exit(write_log=True)``) is exercised
in ``tests/test_child_delta.py``; everything here runs without a session.
"""
from __future__ import annotations

import json
from typing import Any, Dict, Set

import pytest


def test_build_exit_payload_basic() -> None:
    from spark_az.child import build_exit_payload

    payload: Dict[str, Any] = json.loads(
        build_exit_payload("ok", fields={"rows": 10, "watermark": "2026-06-01"})
    )
    assert payload["status"] == "ok"
    assert payload["rows"] == 10
    assert payload["watermark"] == "2026-06-01"
    assert "finished_at" in payload


def test_build_exit_payload_omits_empty_run_id() -> None:
    from spark_az.child import build_exit_payload

    payload: Dict[str, Any] = json.loads(build_exit_payload("ok"))
    assert "pipeline_run_id" not in payload


def test_build_exit_payload_includes_run_id_and_error() -> None:
    from spark_az.child import build_exit_payload

    payload: Dict[str, Any] = json.loads(
        build_exit_payload("failed", pipeline_run_id="r1", error=ValueError("bad thing"))
    )
    assert payload["status"] == "failed"
    assert payload["pipeline_run_id"] == "r1"
    assert payload["error_class"] == "ValueError"
    assert "bad thing" in payload["error_message"]


def test_notebook_exit_write_log_false_calls_exit(fake_mssparkutils: Any) -> None:
    from spark_az.child import notebook_exit

    notebook_exit("ok", write_log=False, rows=5, watermark="2026-06-01")

    raw: Any = fake_mssparkutils.notebook.exit_value
    assert raw is not None
    payload: Dict[str, Any] = json.loads(raw)
    assert payload["status"] == "ok"
    assert payload["rows"] == 5
    assert payload["watermark"] == "2026-06-01"


def test_notebook_exit_requires_log_table_when_writing(
    fake_mssparkutils: Any,
) -> None:
    from spark_az.child import notebook_exit

    with pytest.raises(ValueError, match="log_table"):
        notebook_exit("ok", write_log=True)
    assert fake_mssparkutils.notebook.exit_value is None


def test_self_result_has_childresult_shape() -> None:
    from spark_az.child import _format_error, _self_result
    from spark_az.lgr import LOG_SCHEMA_FIELDS

    result = _self_result(
        status="ok",
        pipeline_run_id="r1",
        pipeline_name="nightly",
        child_index=-1,
        exit_value='{"status": "ok"}',
        args={"d": 1},
        error=_format_error(None),
    )

    keys: Set[str] = set(result.keys())
    expected: Set[str] = {n for n, _ in LOG_SCHEMA_FIELDS if n != "audited_at"}
    assert keys == expected
    assert result["child_index"] == -1
    assert result["pipeline_run_id"] == "r1"
    assert result["status"] == "ok"
    assert result["args_json"] == '{"d": 1}'
    assert result["error_class"] == ""
    assert result["started_at"] != ""
    assert result["duration_ms"] >= 0


def test_self_result_records_error() -> None:
    from spark_az.child import _format_error, _self_result

    result = _self_result(
        status="failed",
        pipeline_run_id="r1",
        pipeline_name="p",
        child_index=-1,
        exit_value="{}",
        args=None,
        error=_format_error(ValueError("missing column 'id'")),
    )
    assert result["status"] == "failed"
    assert result["error_class"] == "ValueError"
    assert "missing column" in result["error_message"]
    assert "ValueError" in result["error_traceback"]
