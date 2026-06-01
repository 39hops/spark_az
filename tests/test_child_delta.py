"""Delta integration tests for spark_az.child self-logging.

UNRUN in this dev env: pyspark 4.1 cannot start a JVM under Python 3.14 here
(JAVA_GATEWAY_EXITED), exactly as for ``tests/test_lgr_delta.py``. Run in
Synapse or on any box with a pyspark-compatible JDK to verify the
self-logged Delta row.
"""
from __future__ import annotations

import json
from typing import Any

import pytest

pytestmark = pytest.mark.usefixtures("registered_spark")


def test_notebook_exit_appends_self_row(spark: Any, fake_mssparkutils: Any) -> None:
    """A self-logging child writes exactly one row keyed by pipeline_run_id."""
    from spark_az.child import notebook_exit

    table: str = "default.test_child_self_log"
    spark.sql(f"DROP TABLE IF EXISTS {table}")

    notebook_exit(
        "ok",
        log_table=table,
        pipeline_run_id="run-9",
        pipeline_name="nightly",
        rows=7,
        target="lake.orders",
    )

    rows = spark.table(table).orderBy("child_index").collect()
    assert len(rows) == 1
    assert rows[0]["pipeline_run_id"] == "run-9"
    assert rows[0]["status"] == "ok"
    assert rows[0]["child_index"] == -1
    assert rows[0]["audited_at"] is not None
    payload = json.loads(rows[0]["exit_value"])
    assert payload["rows"] == 7
    assert payload["target"] == "lake.orders"
    assert fake_mssparkutils.notebook.exit_value is not None


def test_notebook_exit_failed_status_records_error(
    spark: Any, fake_mssparkutils: Any
) -> None:
    """A failed child records the error class and message on its row."""
    from spark_az.child import notebook_exit

    table: str = "default.test_child_self_log_failed"
    spark.sql(f"DROP TABLE IF EXISTS {table}")

    notebook_exit(
        "failed",
        log_table=table,
        pipeline_run_id="run-10",
        error=ValueError("missing column 'id'"),
    )

    row = spark.table(table).collect()[0]
    assert row["status"] == "failed"
    assert row["error_class"] == "ValueError"
    assert "missing column" in row["error_message"]
    payload = json.loads(fake_mssparkutils.notebook.exit_value)
    assert payload["error_class"] == "ValueError"


def test_log_run_appends_ok_row(spark: Any) -> None:
    """log_run logs one ok self-row on a clean exit."""
    from spark_az.child import log_run

    table: str = "default.test_child_log_run_ok"
    spark.sql(f"DROP TABLE IF EXISTS {table}")

    with log_run(log_table=table, pipeline_run_id="run-7"):
        pass

    rows = spark.table(table).collect()
    assert len(rows) == 1
    assert rows[0]["status"] == "ok"
    assert rows[0]["pipeline_run_id"] == "run-7"
    assert rows[0]["child_index"] == -1


def test_log_run_appends_failed_row_and_reraises(spark: Any) -> None:
    """log_run logs a failed self-row with the error, then re-raises."""
    from spark_az.child import log_run

    table: str = "default.test_child_log_run_failed"
    spark.sql(f"DROP TABLE IF EXISTS {table}")

    with pytest.raises(ValueError, match="kaboom"):
        with log_run(log_table=table, pipeline_run_id="run-8"):
            raise ValueError("kaboom")

    row = spark.table(table).collect()[0]
    assert row["status"] == "failed"
    assert row["error_class"] == "ValueError"
    assert "kaboom" in row["error_message"]
