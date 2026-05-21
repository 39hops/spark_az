"""Azure Synapse Spark notebook orchestration + Delta logging."""
from __future__ import annotations

from .session import get_spark, set_spark

__all__ = ["get_spark", "set_spark"]
