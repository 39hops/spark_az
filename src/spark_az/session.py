"""SparkSession lookup helpers.

The package never creates a SparkSession. Synapse notebooks already provide
one, and local PySpark jobs should register or activate one explicitly.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_spark: Optional["SparkSession"] = None


def set_spark(session: "SparkSession") -> None:
    """Register the SparkSession used by spark_az.

    Use this in local scripts/tests or any runtime where Spark does not
    expose an active session. Synapse notebooks usually do not need it
    because Spark is already active before user code runs.

    Args:
        session: A live ``SparkSession``.

    Examples:
        In a Synapse notebook (rarely needed):

        >>> from spark_az import set_spark
        >>> set_spark(spark)

        In a local script:

        >>> from pyspark.sql import SparkSession
        >>> set_spark(SparkSession.builder.getOrCreate())
    """
    global _spark
    _spark = session


def get_spark() -> "SparkSession":
    """Return the registered or active SparkSession.

    This intentionally avoids ``SparkSession.builder.getOrCreate()`` so
    imports do not mutate the runtime or fight Synapse's pre-created
    session.

    Returns:
        The active SparkSession.

    Raises:
        RuntimeError: No registered session and no active session.

    Examples:
        >>> spark = get_spark()
        >>> spark.sql("SELECT 1").collect()
        [Row(1=1)]
    """
    if _spark is not None:
        return _spark

    active: Optional["SparkSession"] = _active_spark_session()
    if active is not None:
        return active

    raise RuntimeError(
        "No active SparkSession found. In Synapse this should usually be "
        "available automatically; otherwise call spark_az.set_spark(spark) "
        "once before reading, writing, or running Spark jobs."
    )


def _active_spark_session() -> Optional["SparkSession"]:
    """Return Spark's active session without creating one."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        return None
    return SparkSession.getActiveSession()


__all__ = ["get_spark", "set_spark"]
