"""Integration tests that exercise a local Delta-enabled SparkSession."""
from __future__ import annotations

from typing import Any, List

import pytest


pytestmark = pytest.mark.usefixtures("registered_spark")


def test_ensure_log_table_creates_table(spark: Any) -> None:
    from spark_az.pipeline_logger import LOG_SCHEMA_FIELDS, ensure_log_table

    table: str = "default.test_ensure_creates"
    spark.sql(f"DROP TABLE IF EXISTS {table}")

    ensure_log_table(table)

    assert spark.catalog.tableExists(table)
    actual_cols: List[str] = [f.name for f in spark.table(table).schema.fields]
    expected_cols: List[str] = [name for name, _ in LOG_SCHEMA_FIELDS]
    assert actual_cols == expected_cols


def test_ensure_log_table_is_idempotent(spark: Any) -> None:
    from spark_az.pipeline_logger import ensure_log_table

    table: str = "default.test_ensure_idempotent"
    spark.sql(f"DROP TABLE IF EXISTS {table}")

    ensure_log_table(table)
    ensure_log_table(table)

    assert spark.catalog.tableExists(table)
    assert spark.table(table).count() == 0
