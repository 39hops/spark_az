"""Shared pytest fixtures: fake mssparkutils + local Delta-enabled Spark."""
from __future__ import annotations

import sys
import types
from typing import Any, Callable, Dict, List, Optional

import pytest


class FakeNotebook:
    """Stand-in for ``mssparkutils.notebook``.

    Configurable per-test. ``handler`` is called with ``(path, timeout,
    args)`` and may return any value, raise ``RuntimeError("...timeout...")``
    to simulate timeout, or raise any other exception to simulate failure.
    """

    def __init__(self) -> None:
        self.calls: List[Dict[str, Any]] = []
        self.handler: Callable[..., Any] = lambda path, timeout, args: ""

    def run(
        self,
        path: str,
        timeout: int,
        args: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Record the call and delegate to the configured handler."""
        recorded: Dict[str, Any] = {
            "path": path,
            "timeout": timeout,
            "args": dict(args) if args else {},
        }
        self.calls.append(recorded)
        return self.handler(path, timeout, args or {})


class FakeRuntimeContext:
    """Stand-in for ``mssparkutils.runtime.context``."""

    def __init__(self, notebook_name: str = "") -> None:
        self._notebook_name: str = notebook_name

    def __getitem__(self, key: str) -> str:
        if key == "currentNotebookName":
            return self._notebook_name
        return ""


class FakeMssparkutils:
    """Top-level stand-in. Mirrors the ``mssparkutils`` import surface."""

    def __init__(self) -> None:
        self.notebook: FakeNotebook = FakeNotebook()
        self.runtime: types.SimpleNamespace = types.SimpleNamespace(
            context=FakeRuntimeContext()
        )


@pytest.fixture
def fake_mssparkutils(monkeypatch: pytest.MonkeyPatch) -> FakeMssparkutils:
    """Install a fake ``mssparkutils`` and ``notebookutils.mssparkutils``.

    Tests configure the handler via
    ``fake_mssparkutils.notebook.handler = ...``.
    """
    fake: FakeMssparkutils = FakeMssparkutils()

    notebookutils: types.ModuleType = types.ModuleType("notebookutils")
    setattr(notebookutils, "mssparkutils", fake)
    monkeypatch.setitem(sys.modules, "notebookutils", notebookutils)
    monkeypatch.setitem(sys.modules, "mssparkutils", fake)
    return fake


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Any:
    """Local Delta-enabled SparkSession for integration tests."""
    from delta.pip_utils import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    warehouse: str = str(tmp_path_factory.mktemp("spark-warehouse"))
    builder: Any = (
        SparkSession.builder.appName("spark_az-tests")
        .master("local[2]")
        .config("spark.sql.warehouse.dir", warehouse)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.ui.enabled", "false")
    )
    builder = configure_spark_with_delta_pip(builder)
    session: Any = builder.getOrCreate()
    try:
        yield session
    finally:
        session.stop()


@pytest.fixture(autouse=True)
def _reset_spark_registration(monkeypatch: pytest.MonkeyPatch) -> None:
    """Clear any package-level Spark registration between tests."""
    from spark_az import session as session_module

    monkeypatch.setattr(session_module, "_spark", None, raising=False)


@pytest.fixture(autouse=True)
def _propagate_pipeline_logger_to_caplog(
    caplog: pytest.LogCaptureFixture,
) -> Any:
    """Add caplog's handler directly to the pipeline_logger so that records
    are captured even though the logger has ``propagate=False``."""
    import logging

    logger: logging.Logger = logging.getLogger("spark_az.pipeline_logger")
    logger.addHandler(caplog.handler)
    try:
        yield
    finally:
        logger.removeHandler(caplog.handler)


@pytest.fixture
def registered_spark(spark: Any) -> Any:
    """Register the local Spark session with the package and yield it."""
    from spark_az.session import set_spark

    set_spark(spark)
    return spark
