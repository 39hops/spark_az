"""Integration tests that exercise a local Delta-enabled SparkSession."""
from __future__ import annotations

from typing import Any, List

import pytest


pytestmark = pytest.mark.usefixtures("registered_spark")


def test_ensure_log_table_creates_table(spark: Any) -> None:
    from spark_az.lgr import LOG_SCHEMA_FIELDS, ensure_log_table

    table: str = "default.test_ensure_creates"
    spark.sql(f"DROP TABLE IF EXISTS {table}")

    ensure_log_table(table)

    assert spark.catalog.tableExists(table)
    actual_cols: List[str] = [f.name for f in spark.table(table).schema.fields]
    expected_cols: List[str] = [name for name, _ in LOG_SCHEMA_FIELDS]
    assert actual_cols == expected_cols


def test_ensure_log_table_is_idempotent(spark: Any) -> None:
    from spark_az.lgr import ensure_log_table

    table: str = "default.test_ensure_idempotent"
    spark.sql(f"DROP TABLE IF EXISTS {table}")

    ensure_log_table(table)
    ensure_log_table(table)

    assert spark.catalog.tableExists(table)
    assert spark.table(table).count() == 0


def test_ensure_log_table_creates_missing_database(spark: Any) -> None:
    from spark_az.lgr import ensure_log_table

    db: str = "_lgr_test_meta"
    table: str = f"{db}.runlog_smoke"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(f"DROP DATABASE IF EXISTS {db} CASCADE")

    ensure_log_table(table)

    assert spark.catalog.tableExists(table)
    assert spark.catalog.databaseExists(db)


def test_append_rows_writes_all_columns_and_audited_at(spark: Any) -> None:
    from spark_az.lgr import (
        LOG_SCHEMA_FIELDS,
        ChildResult,
        _append_rows,
        ensure_log_table,
    )

    table: str = "default.test_append_rows"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    ensure_log_table(table)

    row: ChildResult = {
        "pipeline_run_id": "r1",
        "pipeline_name": "p",
        "child_index": 0,
        "notebook_path": "/n/x",
        "status": "ok",
        "started_at": "2026-05-21T12:00:00+00:00",
        "finished_at": "2026-05-21T12:00:01+00:00",
        "duration_ms": 1000,
        "exit_value": "v",
        "args_json": "{}",
        "error_class": "",
        "error_message": "",
        "error_traceback": "",
        "orchestrator_notebook": "",
    }

    _append_rows(table, [row, dict(row, child_index=1)])

    df = spark.table(table)
    assert df.count() == 2
    cols: List[str] = [f.name for f in df.schema.fields]
    expected: List[str] = [name for name, _ in LOG_SCHEMA_FIELDS]
    assert cols == expected
    audited = [r["audited_at"] for r in df.collect()]
    assert all(a is not None for a in audited)


def test_append_rows_empty_is_noop(spark: Any) -> None:
    from spark_az.lgr import _append_rows, ensure_log_table

    table: str = "default.test_append_empty"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    ensure_log_table(table)

    _append_rows(table, [])

    assert spark.table(table).count() == 0
