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
from .child import install_logging, log_done, log_run, notebook_exit

__all__ = [
    "ChildResult",
    "ChildSpec",
    "JsonFormatter",
    "PipelineParams",
    "enable_app_insights",
    "ensure_log_table",
    "get_active_run_id",
    "get_spark",
    "install_logging",
    "log",
    "log_done",
    "log_run",
    "notebook_exit",
    "read_pipeline_params",
    "run_child",
    "run_pipeline",
    "set_active_run_id",
    "set_json_formatter",
    "set_spark",
    "step",
]
