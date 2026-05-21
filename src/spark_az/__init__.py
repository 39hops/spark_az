"""Azure Synapse Spark notebook orchestration + Delta logging."""
from __future__ import annotations

from .pipeline_logger import (
    ChildResult,
    ChildSpec,
    ensure_log_table,
    run_child,
    run_pipeline,
)
from .session import get_spark, set_spark

__all__ = [
    "ChildResult",
    "ChildSpec",
    "ensure_log_table",
    "get_spark",
    "run_child",
    "run_pipeline",
    "set_spark",
]
