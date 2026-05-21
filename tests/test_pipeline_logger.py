"""Unit tests for spark_az.pipeline_logger."""
from __future__ import annotations

from typing import Any, Dict, List, Set, get_type_hints

import json
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

from spark_az.pipeline_logger import ChildResult as _ChildResult


def _result_template() -> "_ChildResult":
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


def test_run_child_success(fake_mssparkutils: Any) -> None:
    from spark_az.pipeline_logger import ChildSpec, run_child

    fake_mssparkutils.notebook.handler = lambda path, t, args: "42rows"
    spec: ChildSpec = {
        "path": "/notebooks/extract",
        "args": {"date": "2026-05-21"},
        "timeout_seconds": 600,
    }

    result = run_child(
        spec,
        pipeline_run_id="run-1",
        pipeline_name="nightly",
        child_index=0,
    )

    assert result["status"] == "ok"
    assert result["exit_value"] == "42rows"
    assert result["notebook_path"] == "/notebooks/extract"
    assert result["pipeline_run_id"] == "run-1"
    assert result["pipeline_name"] == "nightly"
    assert result["child_index"] == 0
    assert result["duration_ms"] >= 0
    assert result["error_class"] == ""
    assert result["error_message"] == ""
    assert result["error_traceback"] == ""
    assert result["args_json"] == '{"date": "2026-05-21"}'
    call = fake_mssparkutils.notebook.calls[0]
    assert call == {
        "path": "/notebooks/extract",
        "timeout": 600,
        "args": {"date": "2026-05-21"},
    }


def test_run_child_uses_default_timeout(fake_mssparkutils: Any) -> None:
    from spark_az.pipeline_logger import ChildSpec, run_child

    fake_mssparkutils.notebook.handler = lambda path, t, args: ""
    spec: ChildSpec = {"path": "/notebooks/x"}

    run_child(
        spec,
        pipeline_run_id="r",
        pipeline_name="p",
        child_index=0,
        default_timeout_seconds=900,
    )

    assert fake_mssparkutils.notebook.calls[0]["timeout"] == 900


def test_run_child_failure_captures_traceback(fake_mssparkutils: Any) -> None:
    """run_child must record status=failed and capture error details on exception."""
    from spark_az.pipeline_logger import ChildSpec, run_child

    def boom(path: str, timeout: int, args: Dict[str, Any]) -> Any:
        raise ValueError("missing column 'id'")

    fake_mssparkutils.notebook.handler = boom
    spec: ChildSpec = {"path": "/notebooks/transform"}

    result = run_child(
        spec,
        pipeline_run_id="r",
        pipeline_name="p",
        child_index=1,
    )

    assert result["status"] == "failed"
    assert result["error_class"] == "ValueError"
    assert "missing column" in result["error_message"]
    assert "ValueError" in result["error_traceback"]
    assert result["exit_value"] == ""


def test_run_child_timeout_routes_to_timeout_status(
    fake_mssparkutils: Any,
) -> None:
    """run_child must set status=timeout when the notebook raises a timeout RuntimeError."""
    from spark_az.pipeline_logger import ChildSpec, run_child

    def slow(path: str, timeout: int, args: Dict[str, Any]) -> Any:
        raise RuntimeError("notebook timed out after 1800 seconds")

    fake_mssparkutils.notebook.handler = slow
    spec: ChildSpec = {"path": "/notebooks/load"}

    result = run_child(
        spec,
        pipeline_run_id="r",
        pipeline_name="p",
        child_index=2,
    )

    assert result["status"] == "timeout"
    assert result["error_class"] == "RuntimeError"
    assert "timed out" in result["error_message"]


def test_run_child_truncates_giant_traceback(
    fake_mssparkutils: Any,
) -> None:
    """run_child must truncate oversized error_message and error_traceback fields."""
    from spark_az.pipeline_logger import ChildSpec, run_child

    def boom(path: str, timeout: int, args: Dict[str, Any]) -> Any:
        raise RuntimeError("x" * 50000)

    fake_mssparkutils.notebook.handler = boom
    spec: ChildSpec = {"path": "/notebooks/x"}

    result = run_child(
        spec,
        pipeline_run_id="r",
        pipeline_name="p",
        child_index=0,
    )

    assert result["error_message"].endswith("…[truncated]")
    assert len(result["error_message"]) <= 4096 + len("…[truncated]")
    assert result["error_traceback"].endswith("…[truncated]")


def test_run_pipeline_all_pass_returns_results(
    fake_mssparkutils: Any,
) -> None:
    from spark_az.pipeline_logger import ChildSpec, run_pipeline

    responses: Dict[str, str] = {
        "/notebooks/extract": "10rows",
        "/notebooks/transform": "10rows",
        "/notebooks/load": "ok",
    }
    fake_mssparkutils.notebook.handler = (
        lambda path, t, args: responses[path]
    )

    specs: List[ChildSpec] = [
        {"path": "/notebooks/extract"},
        {"path": "/notebooks/transform"},
        {"path": "/notebooks/load"},
    ]

    results = run_pipeline(
        specs,
        log_table="ignored",
        pipeline_name="nightly",
        write_log=False,
    )

    assert [r["status"] for r in results] == ["ok", "ok", "ok"]
    assert [r["child_index"] for r in results] == [0, 1, 2]
    assert {r["pipeline_run_id"] for r in results} == {results[0]["pipeline_run_id"]}
    assert results[0]["pipeline_name"] == "nightly"


def test_run_pipeline_outside_synapse_raises_before_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "notebookutils", None)
    monkeypatch.setitem(sys.modules, "mssparkutils", None)
    from spark_az.pipeline_logger import run_pipeline

    with pytest.raises(RuntimeError, match="mssparkutils"):
        run_pipeline(
            [{"path": "/x"}],
            log_table="t",
            pipeline_name="p",
            write_log=False,
        )


def test_run_pipeline_fail_fast_skips_remaining_and_raises(
    fake_mssparkutils: Any,
) -> None:
    from spark_az.pipeline_logger import ChildSpec, run_pipeline

    def handler(path: str, t: int, args: Dict[str, Any]) -> Any:
        if path == "/notebooks/transform":
            raise ValueError("bad data")
        return "ok"

    fake_mssparkutils.notebook.handler = handler
    specs: List[ChildSpec] = [
        {"path": "/notebooks/extract"},
        {"path": "/notebooks/transform"},
        {"path": "/notebooks/load"},
    ]

    with pytest.raises(RuntimeError, match="ValueError: bad data"):
        run_pipeline(
            specs,
            log_table="ignored",
            pipeline_name="nightly",
            write_log=False,
            fail_fast=True,
        )

    paths_called: List[str] = [
        c["path"] for c in fake_mssparkutils.notebook.calls
    ]
    assert paths_called == ["/notebooks/extract", "/notebooks/transform"]


def test_run_pipeline_fail_fast_writes_log_before_raising(
    fake_mssparkutils: Any, registered_spark: Any
) -> None:
    """The Delta log must be durable even on fail_fast re-raise."""
    from spark_az.pipeline_logger import ChildSpec, run_pipeline

    spark: Any = registered_spark
    table: str = "default.test_runpipeline_failfast"
    spark.sql(f"DROP TABLE IF EXISTS {table}")

    def handler(path: str, t: int, args: Dict[str, Any]) -> Any:
        if path == "/notebooks/x2":
            raise ValueError("nope")
        return "ok"

    fake_mssparkutils.notebook.handler = handler
    specs: List[ChildSpec] = [
        {"path": "/notebooks/x1"},
        {"path": "/notebooks/x2"},
        {"path": "/notebooks/x3"},
    ]

    with pytest.raises(RuntimeError):
        run_pipeline(
            specs,
            log_table=table,
            pipeline_name="p",
            fail_fast=True,
        )

    rows = spark.table(table).orderBy("child_index").collect()
    assert [r["status"] for r in rows] == ["ok", "failed", "skipped"]
    assert rows[1]["error_class"] == "ValueError"
    assert rows[2]["notebook_path"] == "/notebooks/x3"


def test_run_pipeline_fail_fast_false_runs_everything(
    fake_mssparkutils: Any,
) -> None:
    from spark_az.pipeline_logger import ChildSpec, run_pipeline

    def handler(path: str, t: int, args: Dict[str, Any]) -> Any:
        if path == "/notebooks/middle":
            raise ValueError("bad")
        return "ok"

    fake_mssparkutils.notebook.handler = handler
    specs: List[ChildSpec] = [
        {"path": "/notebooks/first"},
        {"path": "/notebooks/middle"},
        {"path": "/notebooks/last"},
    ]

    results = run_pipeline(
        specs,
        log_table="ignored",
        pipeline_name="p",
        write_log=False,
        fail_fast=False,
    )

    assert [r["status"] for r in results] == ["ok", "failed", "ok"]
    paths_called: List[str] = [
        c["path"] for c in fake_mssparkutils.notebook.calls
    ]
    assert paths_called == [
        "/notebooks/first",
        "/notebooks/middle",
        "/notebooks/last",
    ]


def test_public_api_reexported_from_package_root() -> None:
    import spark_az

    expected: List[str] = [
        "ChildResult",
        "ChildSpec",
        "JsonFormatter",
        "PipelineParams",
        "enable_app_insights",
        "ensure_log_table",
        "get_active_run_id",
        "get_spark",
        "log",
        "read_pipeline_params",
        "run_child",
        "run_pipeline",
        "set_active_run_id",
        "set_json_formatter",
        "set_spark",
        "step",
    ]
    for name in expected:
        assert hasattr(spark_az, name), f"spark_az missing {name}"
    assert set(spark_az.__all__) == set(expected)


def test_json_formatter_basic_fields() -> None:
    from spark_az.pipeline_logger import JsonFormatter

    fmt: JsonFormatter = JsonFormatter()
    record = logging.LogRecord(
        name="test", level=logging.INFO, pathname="", lineno=0,
        msg="hello", args=(), exc_info=None,
    )
    payload: Dict[str, Any] = json.loads(fmt.format(record))
    assert payload["msg"] == "hello"
    assert payload["level"] == "INFO"
    assert payload["logger"] == "test"
    assert "ts" in payload


def test_json_formatter_includes_extras() -> None:
    from spark_az.pipeline_logger import JsonFormatter

    fmt: JsonFormatter = JsonFormatter()
    record = logging.LogRecord(
        name="test", level=logging.INFO, pathname="", lineno=0,
        msg="step done", args=(), exc_info=None,
    )
    record.step = "extract"
    record.duration_ms = 1230
    record.pipeline_run_id = "r-1"
    payload: Dict[str, Any] = json.loads(fmt.format(record))
    assert payload["step"] == "extract"
    assert payload["duration_ms"] == 1230
    assert payload["pipeline_run_id"] == "r-1"


def test_set_json_formatter_swaps_default_handler() -> None:
    from spark_az.pipeline_logger import (
        JsonFormatter,
        _HANDLER_NAME,
        log,
        set_json_formatter,
    )

    set_json_formatter()
    default_handlers = [h for h in log.handlers if h.get_name() == _HANDLER_NAME]
    assert len(default_handlers) == 1
    assert isinstance(default_handlers[0].formatter, JsonFormatter)


def test_set_json_formatter_idempotent() -> None:
    from spark_az.pipeline_logger import _HANDLER_NAME, log, set_json_formatter

    set_json_formatter()
    set_json_formatter()
    handlers = [h for h in log.handlers if h.get_name() == _HANDLER_NAME]
    assert len(handlers) == 1


def test_enable_app_insights_missing_dep_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import builtins as _builtins

    real_import = _builtins.__import__

    def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name.startswith("azure.monitor.opentelemetry"):
            raise ImportError(f"No module named {name!r}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(_builtins, "__import__", fake_import)
    from spark_az import pipeline_logger as pl

    monkeypatch.setattr(pl, "_APP_INSIGHTS_ENABLED", False, raising=False)
    with pytest.raises(ImportError, match="azure-monitor-opentelemetry"):
        pl.enable_app_insights("InstrumentationKey=fake")


def test_step_emits_start_and_ok(caplog: pytest.LogCaptureFixture) -> None:
    from spark_az.pipeline_logger import log, step

    with caplog.at_level(logging.INFO, logger=log.name):
        with step("extract", source="lab.raw") as s:
            s.metric("rows_in", 100)

    records = [r for r in caplog.records if getattr(r, "step", None) == "extract"]
    phases = [getattr(r, "phase", "") for r in records]
    assert "start" in phases
    assert "ok" in phases
    ok = next(r for r in records if getattr(r, "phase", "") == "ok")
    assert getattr(ok, "rows_in", None) == 100
    assert getattr(ok, "duration_ms", -1) >= 0


def test_step_emits_failed_and_reraises(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from spark_az.pipeline_logger import log, step

    with caplog.at_level(logging.INFO, logger=log.name):
        with pytest.raises(ValueError, match="bad"):
            with step("transform"):
                raise ValueError("bad")

    records = [r for r in caplog.records if getattr(r, "step", None) == "transform"]
    phases = [getattr(r, "phase", "") for r in records]
    assert "failed" in phases
    failed = next(r for r in records if getattr(r, "phase", "") == "failed")
    assert getattr(failed, "error_class", "") == "ValueError"
    assert "bad" in getattr(failed, "error_message", "")


def test_step_uses_active_run_id(caplog: pytest.LogCaptureFixture) -> None:
    from spark_az.pipeline_logger import log, set_active_run_id, step

    set_active_run_id("r-test")
    with caplog.at_level(logging.INFO, logger=log.name):
        with step("noop"):
            pass

    recs = [r for r in caplog.records if getattr(r, "step", None) == "noop"]
    assert recs
    assert all(getattr(r, "pipeline_run_id", None) == "r-test" for r in recs)


def test_read_pipeline_params_happy_path() -> None:
    from spark_az.pipeline_logger import read_pipeline_params

    params = read_pipeline_params(
        pipeline_name="nightly",
        log_table="lab.__pipeline_runlog",
        notebooks=[{"path": "/x"}, {"path": "/y", "args": {"d": 1}}],
        pipeline_run_id="r-1",
        extras={"job_id": "j-7"},
    )
    assert params["pipeline_name"] == "nightly"
    assert params["log_table"] == "lab.__pipeline_runlog"
    assert params["notebooks"] == [{"path": "/x"}, {"path": "/y", "args": {"d": 1}}]
    assert params["pipeline_run_id"] == "r-1"
    assert params["extras"] == {"job_id": "j-7"}
    assert params["fail_fast"] is True


def test_read_pipeline_params_raises_on_empty_pipeline_name() -> None:
    from spark_az.pipeline_logger import read_pipeline_params

    with pytest.raises(ValueError, match="pipeline_name"):
        read_pipeline_params(pipeline_name="", log_table="t", notebooks=[])


def test_read_pipeline_params_raises_on_bad_notebooks() -> None:
    from spark_az.pipeline_logger import read_pipeline_params

    with pytest.raises(ValueError, match="path"):
        read_pipeline_params(
            pipeline_name="p",
            log_table="t",
            notebooks=[{"path": "/x"}, {"timeout_seconds": 60}],
        )


def test_run_pipeline_accepts_injected_run_id(
    fake_mssparkutils: Any,
) -> None:
    from spark_az.pipeline_logger import ChildSpec, run_pipeline

    fake_mssparkutils.notebook.handler = lambda p, t, a: "ok"
    results = run_pipeline(
        [{"path": "/x"}],
        log_table="t",
        pipeline_name="p",
        write_log=False,
        pipeline_run_id="injected-run-id",
    )
    assert results[0]["pipeline_run_id"] == "injected-run-id"


def test_run_pipeline_sets_and_clears_active_run_id(
    fake_mssparkutils: Any,
) -> None:
    from spark_az.pipeline_logger import (
        ChildSpec,
        get_active_run_id,
        run_pipeline,
    )

    captured: List[Any] = []

    def handler(path: str, t: int, args: Dict[str, Any]) -> Any:
        captured.append(get_active_run_id())
        return "ok"

    fake_mssparkutils.notebook.handler = handler
    run_pipeline(
        [{"path": "/x"}],
        log_table="t",
        pipeline_name="p",
        write_log=False,
        pipeline_run_id="r-active",
    )
    assert captured == ["r-active"]
    assert get_active_run_id() is None
