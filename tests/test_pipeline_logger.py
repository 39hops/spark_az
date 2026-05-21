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
