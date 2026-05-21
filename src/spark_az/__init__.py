"""Azure Synapse Spark notebook orchestration + Delta logging."""
from __future__ import annotations

from .lgr import (
    ChildResult,
    ChildSpec,
    JsonFormatter,
    PipelineParams,
    enable_app_insights,
    ensure_log_table,
    get_active_run_id,
    log,
    read_pipeline_params,
    run_child,
    run_pipeline,
    set_active_run_id,
    set_json_formatter,
    step,
)
from .session import get_spark, set_spark

__all__ = [
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
