"""Unit tests for spark_az.child (no Spark required).

The Delta self-logging path (``notebook_exit(write_log=True)``) is exercised
in ``tests/test_child_delta.py``; everything here runs without a session.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Set

import pytest


def test_build_exit_payload_basic() -> None:
    from spark_az.child import build_exit_payload

    payload: Dict[str, Any] = json.loads(
        build_exit_payload("ok", fields={"rows": 10, "target": "lake.orders"})
    )
    assert payload["status"] == "ok"
    assert payload["rows"] == 10
    assert payload["target"] == "lake.orders"
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

    notebook_exit("ok", write_log=False, rows=5, target="lake.orders")

    raw: Any = fake_mssparkutils.notebook.exit_value
    assert raw is not None
    payload: Dict[str, Any] = json.loads(raw)
    assert payload["status"] == "ok"
    assert payload["rows"] == 5
    assert payload["target"] == "lake.orders"


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


def test_install_logging_noop_without_ipython(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import spark_az.child as ch

    monkeypatch.setattr(ch, "_ipython", lambda: None)
    ch.install_logging()
    assert ch._hook_registered is False


def test_log_done_writes_ok_row(monkeypatch: pytest.MonkeyPatch) -> None:
    import spark_az.child as ch

    captured: Dict[str, Any] = {}
    monkeypatch.setattr(ch, "_ipython", lambda: None)
    monkeypatch.setattr(
        ch, "ensure_log_table", lambda t: captured.__setitem__("table", t)
    )
    monkeypatch.setattr(
        ch, "_append_rows", lambda t, rows: captured.__setitem__("rows", rows)
    )

    ch.log_done(target="lake.orders")

    assert captured["table"] == "_meta.__pipeline_runlog"
    row: Dict[str, Any] = captured["rows"][0]
    assert row["status"] == "ok"
    assert row["child_index"] == -1
    assert row["duration_ms"] >= 0
    assert json.loads(row["exit_value"])["target"] == "lake.orders"


def test_failure_hook_logs_failed_row(monkeypatch: pytest.MonkeyPatch) -> None:
    import types

    import spark_az.child as ch

    captured: Dict[str, Any] = {}
    monkeypatch.setattr(ch, "_ipython", lambda: None)
    monkeypatch.setattr(ch, "ensure_log_table", lambda t: None)
    monkeypatch.setattr(
        ch, "_append_rows", lambda t, rows: captured.__setitem__("rows", rows)
    )

    ch._on_cell(types.SimpleNamespace(error_in_exec=ValueError("boom")))

    row: Dict[str, Any] = captured["rows"][0]
    assert row["status"] == "failed"
    assert row["error_class"] == "ValueError"
    assert "boom" in row["error_message"]


def test_failure_hook_ignores_clean_cell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import types

    import spark_az.child as ch

    calls: List[int] = []
    monkeypatch.setattr(ch, "ensure_log_table", lambda t: None)
    monkeypatch.setattr(ch, "_append_rows", lambda t, rows: calls.append(1))

    ch._on_cell(types.SimpleNamespace(error_in_exec=None))

    assert calls == []


def test_outcome_logged_at_most_once(monkeypatch: pytest.MonkeyPatch) -> None:
    import types

    import spark_az.child as ch

    calls: List[int] = []
    monkeypatch.setattr(ch, "_ipython", lambda: None)
    monkeypatch.setattr(ch, "ensure_log_table", lambda t: None)
    monkeypatch.setattr(ch, "_append_rows", lambda t, rows: calls.append(1))

    ch.log_done()
    ch._on_cell(types.SimpleNamespace(error_in_exec=ValueError("x")))
    ch.log_done()

    assert calls == [1]


def test_log_run_logs_ok_then_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    import spark_az.child as ch

    rows: List[Any] = []
    monkeypatch.setattr(ch, "_ipython", lambda: None)
    monkeypatch.setattr(ch, "ensure_log_table", lambda t: None)
    monkeypatch.setattr(ch, "_append_rows", lambda t, r: rows.extend(r))

    with ch.log_run():
        pass
    assert rows[-1]["status"] == "ok"

    with pytest.raises(ValueError, match="nope"):
        with ch.log_run():
            raise ValueError("nope")
    assert rows[-1]["status"] == "failed"
    assert rows[-1]["error_class"] == "ValueError"
