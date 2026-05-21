# Pipeline Logger Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a Python package + Synapse notebook that runs a sequence of child notebooks via `mssparkutils.notebook.run`, writes one Delta log row per child, and re-raises on failure so the upstream Synapse pipeline sees the failure.

**Architecture:** Single module `src/spark_az/lgr.py` exposing `run_pipeline`, `run_child`, `ensure_log_table`, `ChildSpec`, `ChildResult`. Orchestrator is a jupytext "percent" `.py` in `notebooks/` that imports the library and is auto-converted to `.ipynb` for Synapse. Sequential by design; parallel is a v2.

**Tech Stack:** Python 3.9+, `typing` module (NOT PEP 585 builtin generics), `from __future__ import annotations` at top of every file, PySpark + `delta-spark` for the log sink, `pytest` for tests, `jupytext` for the notebook build step, `mssparkutils`/`notebookutils` at runtime only (in Synapse).

**Style contract (applies to every code block in this plan):**
- `from __future__ import annotations` at top of every `.py`.
- Imports from `typing` only (`List`, `Dict`, `Optional`, `Iterable`, `TypedDict`, `TYPE_CHECKING`). No `list[str]` / `dict[str, int]`.
- PySpark imports under `TYPE_CHECKING` at module level, real imports inside functions.
- Documentation in docstrings only. No inline `#` comments except for the unavoidable jupytext cell markers (`# %%`, `# %% [markdown]`, `# %% tags=["parameters"]`) and shebangs.
- Public symbols have Google-style docstrings with `Args:`, `Returns:`, `Raises:`, `Examples:` sections.

**Spec reference:** `docs/superpowers/specs/2026-05-21-pipeline-logger-design.md`

---

## File structure

| Path | Created in task | Responsibility |
|---|---|---|
| `pyproject.toml` | 1 | Package metadata, deps, optional extras, pytest config. |
| `scripts/setup.sh` | 1 | `pip install -e .[test,dev]`. |
| `scripts/build.sh` | 1 | Build wheel + run `jupytext --to ipynb`. |
| `scripts/test.sh` | 1 | Run pytest. |
| `src/spark_az/__init__.py` | 16 (re-exports), 1 (empty) | Public surface. |
| `src/spark_az/session.py` | 2 | `get_spark` / `set_spark`. |
| `src/spark_az/lgr.py` | 3, 5–15 | Schema, helpers, `run_child`, `run_pipeline`. |
| `src/spark_az/py.typed` | 1 | PEP 561 marker. |
| `tests/conftest.py` | 4 | `fake_mssparkutils` fixture + local `SparkSession`. |
| `tests/test_session.py` | 2 | Unit tests for session helpers. |
| `tests/test_lgr.py` | 3, 5, 8–11, 13–15 | Pure-Python unit tests. |
| `tests/test_lgr_delta.py` | 7, 12 | Local-Spark integration. |
| `notebooks/_logging/lgr.py` | 17 | Jupytext orchestrator. |
| `notebooks/_logging/lgr.ipynb` | 18 | Generated, committed. |
| `scripts/build_notebooks.sh` | 18 | `jupytext --to ipynb`. |

---

## Task 1: Project bootstrap (pyproject + scripts)

**Files:**
- Create: `pyproject.toml`, `src/spark_az/__init__.py`, `src/spark_az/py.typed`
- Modify: `scripts/setup.sh`, `scripts/build.sh`, `scripts/test.sh`
- Delete: `src/.gitkeep`, `tests/.gitkeep`

- [ ] **Step 1: Write `pyproject.toml`**

```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "spark-az"
version = "0.1.0"
description = "Azure Synapse Spark notebook orchestration + Delta logging."
readme = "README.md"
requires-python = ">=3.9"
license = "MIT"
authors = [{ name = "Artin" }]
dependencies = []

[project.optional-dependencies]
spark = ["pyspark", "delta-spark"]
test  = ["pyspark", "delta-spark", "pytest"]
dev   = ["jupytext"]

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-q"

[tool.setuptools.packages.find]
where = ["src"]
include = ["spark_az*"]

[tool.setuptools.package-data]
spark_az = ["py.typed"]
```

- [ ] **Step 2: Create empty `src/spark_az/__init__.py`**

```python
"""Azure Synapse Spark notebook orchestration + Delta logging."""
```

- [ ] **Step 3: Create empty marker `src/spark_az/py.typed`** (zero-byte file).

- [ ] **Step 4: Replace `scripts/setup.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

python -m pip install --upgrade pip
python -m pip install -e ".[test,dev]"
```

- [ ] **Step 5: Replace `scripts/build.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

python -m pip install --upgrade pip build
python -m build --wheel
```

- [ ] **Step 6: Replace `scripts/test.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

python -m pytest "$@"
```

- [ ] **Step 7: Delete `src/.gitkeep` and `tests/.gitkeep`** (no longer needed once real files live there).

```bash
rm -f src/.gitkeep tests/.gitkeep
```

- [ ] **Step 8: Verify install**

Run: `bash scripts/setup.sh`
Expected: ends with `Successfully installed ... spark-az-0.1.0 ...` and no errors.

- [ ] **Step 9: Verify import**

Run: `python -c "import spark_az; print(spark_az.__doc__)"`
Expected: `Azure Synapse Spark notebook orchestration + Delta logging.`

- [ ] **Step 10: Commit**

```bash
git add pyproject.toml src/spark_az/__init__.py src/spark_az/py.typed \
        scripts/setup.sh scripts/build.sh scripts/test.sh
git add -u src/.gitkeep tests/.gitkeep
git commit -m "chore: bootstrap python package + script entries"
```

---

## Task 2: `session.py` (port from spark_lib)

**Files:**
- Create: `src/spark_az/session.py`
- Create: `tests/test_session.py`
- Modify: `src/spark_az/__init__.py` (add re-export)

- [ ] **Step 1: Write the failing test**

`tests/test_session.py`:

```python
"""Tests for spark_az.session."""
from __future__ import annotations

from typing import Any

import pytest

from spark_az import session


def test_get_spark_raises_when_nothing_registered_and_no_active(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """get_spark must not call SparkSession.builder.getOrCreate()."""
    monkeypatch.setattr(session, "_spark", None, raising=False)
    monkeypatch.setattr(session, "_active_spark_session", lambda: None)
    with pytest.raises(RuntimeError, match="No active SparkSession"):
        session.get_spark()


def test_get_spark_returns_registered(monkeypatch: pytest.MonkeyPatch) -> None:
    sentinel: Any = object()
    monkeypatch.setattr(session, "_spark", sentinel, raising=False)
    assert session.get_spark() is sentinel


def test_set_spark_round_trips(monkeypatch: pytest.MonkeyPatch) -> None:
    sentinel: Any = object()
    monkeypatch.setattr(session, "_spark", None, raising=False)
    session.set_spark(sentinel)
    assert session.get_spark() is sentinel


def test_get_spark_falls_back_to_active_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sentinel: Any = object()
    monkeypatch.setattr(session, "_spark", None, raising=False)
    monkeypatch.setattr(session, "_active_spark_session", lambda: sentinel)
    assert session.get_spark() is sentinel
```

- [ ] **Step 2: Run test, expect failure**

Run: `python -m pytest tests/test_session.py -v`
Expected: 4 errors, all `ModuleNotFoundError: No module named 'spark_az.session'`.

- [ ] **Step 3: Implement `src/spark_az/session.py`**

```python
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
```

- [ ] **Step 4: Re-export from `src/spark_az/__init__.py`**

Replace the file:

```python
"""Azure Synapse Spark notebook orchestration + Delta logging."""
from __future__ import annotations

from .session import get_spark, set_spark

__all__ = ["get_spark", "set_spark"]
```

- [ ] **Step 5: Run tests, expect pass**

Run: `python -m pytest tests/test_session.py -v`
Expected: `4 passed`.

- [ ] **Step 6: Commit**

```bash
git add src/spark_az/session.py src/spark_az/__init__.py tests/test_session.py
git commit -m "feat(session): SparkSession lookup ported from spark_lib"
```

---

## Task 3: Schema constants + TypedDicts

**Files:**
- Create: `src/spark_az/lgr.py` (initial slice)
- Create: `tests/test_lgr.py` (initial slice)

- [ ] **Step 1: Write the failing test**

`tests/test_lgr.py`:

```python
"""Unit tests for spark_az.lgr."""
from __future__ import annotations

from typing import Any, Dict, List, Set, get_type_hints

import pytest


def test_log_schema_fields_are_complete() -> None:
    """LOG_SCHEMA_FIELDS must list every column in the spec."""
    from spark_az import lgr as pl

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
    from spark_az import lgr as pl

    allowed: Set[str] = {"string", "long", "timestamp"}
    types_used: Set[str] = {t for _, t in pl.LOG_SCHEMA_FIELDS}
    assert types_used <= allowed


def test_childresult_keys_match_audit_columns_minus_audited_at() -> None:
    """ChildResult covers every log column except audited_at."""
    from spark_az import lgr as pl

    schema_names: List[str] = [name for name, _ in pl.LOG_SCHEMA_FIELDS]
    childresult_keys: List[str] = list(
        get_type_hints(pl.ChildResult).keys()
    )
    assert childresult_keys == [n for n in schema_names if n != "audited_at"]


def test_childspec_total_false() -> None:
    """ChildSpec is a partial TypedDict (total=False)."""
    from spark_az import lgr as pl

    spec: pl.ChildSpec = {"path": "/x"}
    assert spec["path"] == "/x"
```

- [ ] **Step 2: Run test, expect failure**

Run: `python -m pytest tests/test_lgr.py -v`
Expected: errors importing `spark_az.lgr`.

- [ ] **Step 3: Implement initial slice of `src/spark_az/lgr.py`**

```python
"""Synapse orchestrator + Delta logging.

Run a sequence of child notebooks via ``mssparkutils.notebook.run`` from one
orchestrator notebook. Write one structured Delta row per child describing
status, duration, exit value, and any captured exception.

Public API
----------
- :class:`ChildSpec` — describes one child notebook to run.
- :class:`ChildResult` — describes one row written to the log table.
- :func:`ensure_log_table` — idempotent log-table creation.
- :func:`run_child` — run one child; never raises.
- :func:`run_pipeline` — run many in sequence with batched logging.

Conventions
-----------
- The log table is a managed Delta table (e.g. ``"_meta.__pipeline_runlog"``).
- Stdout is plain ``print()`` — audience is the Synapse cell output.
- ``mssparkutils.notebook.run`` is blocking; orchestration is sequential
  in v1.
"""
from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Tuple,
    TypedDict,
)

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


LOG_SCHEMA_FIELDS: List[Tuple[str, str]] = [
    ("pipeline_run_id", "string"),
    ("pipeline_name", "string"),
    ("child_index", "long"),
    ("notebook_path", "string"),
    ("status", "string"),
    ("started_at", "timestamp"),
    ("finished_at", "timestamp"),
    ("duration_ms", "long"),
    ("exit_value", "string"),
    ("args_json", "string"),
    ("error_class", "string"),
    ("error_message", "string"),
    ("error_traceback", "string"),
    ("orchestrator_notebook", "string"),
    ("audited_at", "timestamp"),
]


class ChildSpec(TypedDict, total=False):
    """One child notebook to run.

    Fields:
        path: Required. Synapse workspace path passed to
            ``mssparkutils.notebook.run``.
        timeout_seconds: Optional. Defaults to the caller-supplied
            ``default_timeout_seconds``.
        args: Optional. Arguments forwarded to the child notebook.
        name: Optional display name for stdout. Defaults to the basename
            of ``path``.

    Examples:
        >>> spec: ChildSpec = {
        ...     "path": "/notebooks/extract",
        ...     "args": {"date": "2026-05-21"},
        ...     "timeout_seconds": 600,
        ... }
    """
    path: str
    timeout_seconds: int
    args: Dict[str, Any]
    name: str


class ChildResult(TypedDict):
    """One row written to the log table per child invocation.

    Field semantics:
        status: ``"ok"`` | ``"failed"`` | ``"timeout"`` | ``"skipped"``.
        exit_value: Whatever the child returned via
            ``mssparkutils.notebook.exit(...)``. Stored as a string for
            forward compatibility.
        args_json: ``json.dumps(args, default=str)`` for reproducibility.
        error_class / error_message / error_traceback: Populated when
            ``status != "ok"``. Empty strings otherwise.
    """
    pipeline_run_id: str
    pipeline_name: str
    child_index: int
    notebook_path: str
    status: str
    started_at: str
    finished_at: str
    duration_ms: int
    exit_value: str
    args_json: str
    error_class: str
    error_message: str
    error_traceback: str
    orchestrator_notebook: str


def _log_schema() -> "StructType":
    """Build the Spark schema for the log table.

    Returns:
        ``StructType`` with all columns ``nullable=False``.

    Examples:
        >>> _log_schema().fieldNames()[:2]
        ['pipeline_run_id', 'pipeline_name']
    """
    from pyspark.sql.types import (
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    type_map: Dict[str, Any] = {
        "string": StringType(),
        "long": LongType(),
        "timestamp": TimestampType(),
    }
    return StructType(
        [
            StructField(name, type_map[type_name], False)
            for name, type_name in LOG_SCHEMA_FIELDS
        ]
    )


__all__ = [
    "ChildResult",
    "ChildSpec",
    "LOG_SCHEMA_FIELDS",
]
```

- [ ] **Step 4: Run tests, expect pass**

Run: `python -m pytest tests/test_lgr.py -v`
Expected: `4 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/spark_az/lgr.py tests/test_lgr.py
git commit -m "feat(lgr): log schema + ChildSpec/ChildResult"
```

---

## Task 4: Test infrastructure (`conftest.py`)

**Files:**
- Create: `tests/conftest.py`

- [ ] **Step 1: Write `tests/conftest.py`**

```python
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
    notebookutils.mssparkutils = fake
    monkeypatch.setitem(sys.modules, "notebookutils", notebookutils)
    monkeypatch.setitem(sys.modules, "mssparkutils", fake)
    return fake


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Any:
    """Local Delta-enabled SparkSession for integration tests."""
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


@pytest.fixture
def registered_spark(spark: Any) -> Any:
    """Register the local Spark session with the package and yield it."""
    from spark_az import set_spark

    set_spark(spark)
    return spark
```

- [ ] **Step 2: Smoke-test conftest is importable**

Run: `python -m pytest tests/test_session.py -v`
Expected: still `4 passed` (no regression from the new conftest).

- [ ] **Step 3: Commit**

```bash
git add tests/conftest.py
git commit -m "test: add fake_mssparkutils + local Spark fixtures"
```

---

## Task 5: `_truncate` helper

**Files:**
- Modify: `src/spark_az/lgr.py` (add helper)
- Modify: `tests/test_lgr.py` (add tests)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_lgr.py`:

```python
def test_truncate_under_limit_returns_input() -> None:
    from spark_az.lgr import _truncate

    assert _truncate("hello", limit=100) == "hello"


def test_truncate_over_limit_appends_marker() -> None:
    from spark_az.lgr import _truncate

    out: str = _truncate("x" * 50, limit=20)
    assert out.startswith("x" * 20)
    assert out.endswith("…[truncated]")
    assert len(out) <= 20 + len("…[truncated]")


def test_truncate_empty_string_passes_through() -> None:
    from spark_az.lgr import _truncate

    assert _truncate("", limit=10) == ""
```

- [ ] **Step 2: Run, expect failure**

Run: `python -m pytest tests/test_lgr.py -k truncate -v`
Expected: 3 errors, `ImportError: cannot import name '_truncate'`.

- [ ] **Step 3: Implement `_truncate` in `lgr.py`**

Append (before `__all__`):

```python
def _truncate(text: str, *, limit: int) -> str:
    """Truncate ``text`` to at most ``limit`` chars + a marker suffix.

    The marker suffix is intentionally outside the limit budget so callers
    can reason about the leading content unambiguously.

    Args:
        text: Source string. May be empty.
        limit: Maximum number of source characters to keep.

    Returns:
        ``text`` itself if shorter than ``limit``; otherwise the first
        ``limit`` characters followed by ``"…[truncated]"``.

    Examples:
        >>> _truncate("hello", limit=10)
        'hello'
        >>> _truncate("x" * 50, limit=3)
        'xxx…[truncated]'
    """
    if len(text) <= limit:
        return text
    return text[:limit] + "…[truncated]"
```

- [ ] **Step 4: Run, expect pass**

Run: `python -m pytest tests/test_lgr.py -k truncate -v`
Expected: `3 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/spark_az/lgr.py tests/test_lgr.py
git commit -m "feat(lgr): _truncate helper"
```

---

## Task 6: `_nbutils` helper

**Files:**
- Modify: `src/spark_az/lgr.py`
- Modify: `tests/test_lgr.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_lgr.py`:

```python
def test_nbutils_returns_module_when_notebookutils_present(
    fake_mssparkutils: Any,
) -> None:
    from spark_az.lgr import _nbutils

    nb: Any = _nbutils()
    assert nb is fake_mssparkutils


def test_nbutils_raises_when_neither_module_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "notebookutils", None)
    monkeypatch.setitem(sys.modules, "mssparkutils", None)
    from spark_az.lgr import _nbutils

    with pytest.raises(RuntimeError, match="mssparkutils"):
        _nbutils()
```

Also add to the imports at top of `tests/test_lgr.py` (after the existing imports):

```python
import sys
```

- [ ] **Step 2: Run, expect failure**

Run: `python -m pytest tests/test_lgr.py -k nbutils -v`
Expected: `ImportError: cannot import name '_nbutils'`.

- [ ] **Step 3: Implement `_nbutils` in `lgr.py`**

Append (before `__all__`):

```python
def _nbutils() -> Any:
    """Return Synapse's ``mssparkutils`` regardless of which path imports it.

    Returns:
        The ``mssparkutils`` module-like object (real in Synapse, stubbed
        in tests).

    Raises:
        RuntimeError: Neither ``notebookutils.mssparkutils`` nor
            ``mssparkutils`` is importable. This happens when run outside
            Synapse without the test fake installed.
    """
    try:
        from notebookutils import mssparkutils

        return mssparkutils
    except ImportError:
        try:
            import mssparkutils

            return mssparkutils
        except ImportError:
            raise RuntimeError(
                "mssparkutils / notebookutils not importable; "
                "spark_az.lgr must run inside Azure Synapse."
            )
```

- [ ] **Step 4: Run, expect pass**

Run: `python -m pytest tests/test_lgr.py -k nbutils -v`
Expected: `2 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/spark_az/lgr.py tests/test_lgr.py
git commit -m "feat(lgr): _nbutils Synapse runtime accessor"
```

---

## Task 7: `ensure_log_table`

**Files:**
- Modify: `src/spark_az/lgr.py`
- Create: `tests/test_lgr_delta.py`

- [ ] **Step 1: Write the failing integration test**

`tests/test_lgr_delta.py`:

```python
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
```

- [ ] **Step 2: Run, expect failure**

Run: `python -m pytest tests/test_lgr_delta.py -v`
Expected: `ImportError: cannot import name 'ensure_log_table'`.

- [ ] **Step 3: Implement `ensure_log_table` in `lgr.py`**

Add `from .session import get_spark` near the top with the other imports (after `TYPE_CHECKING` block). Then append before `__all__`:

```python
def ensure_log_table(table: str) -> None:
    """Create the log Delta table if it does not exist.

    Idempotent. Mirrors :meth:`SyncState.ensure` in spark_lib: checks
    ``spark.catalog.tableExists(table)``; otherwise writes an empty
    DataFrame with :func:`_log_schema` as a managed Delta table.

    Args:
        table: Fully-qualified managed Delta table name.

    Examples:
        >>> ensure_log_table("_meta.__pipeline_runlog")
    """
    spark: Any = get_spark()
    if spark.catalog.tableExists(table):
        return
    (
        spark.createDataFrame([], _log_schema())
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable(table)
    )
```

Also extend `__all__` to include `"ensure_log_table"`.

- [ ] **Step 4: Run, expect pass**

Run: `python -m pytest tests/test_lgr_delta.py -v`
Expected: `2 passed`. (First run will be slow — Spark startup.)

- [ ] **Step 5: Commit**

```bash
git add src/spark_az/lgr.py tests/test_lgr_delta.py
git commit -m "feat(lgr): ensure_log_table idempotent creator"
```

---

## Task 8: `_skipped_result` helper

**Files:**
- Modify: `src/spark_az/lgr.py`
- Modify: `tests/test_lgr.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_lgr.py`:

```python
def test_skipped_result_has_expected_fields() -> None:
    from spark_az.lgr import ChildSpec, _skipped_result

    spec: ChildSpec = {"path": "/notebooks/load", "args": {"k": "v"}}
    result = _skipped_result(
        spec,
        pipeline_run_id="run-1",
        pipeline_name="nightly",
        child_index=2,
        orchestrator_notebook="/notebooks/orch",
    )

    assert result["status"] == "skipped"
    assert result["pipeline_run_id"] == "run-1"
    assert result["pipeline_name"] == "nightly"
    assert result["child_index"] == 2
    assert result["notebook_path"] == "/notebooks/load"
    assert result["duration_ms"] == 0
    assert result["exit_value"] == ""
    assert result["args_json"] == '{"k": "v"}'
    assert result["error_class"] == ""
    assert result["error_message"] == ""
    assert result["error_traceback"] == ""
    assert result["orchestrator_notebook"] == "/notebooks/orch"
    assert result["started_at"] == result["finished_at"]
    assert result["started_at"] != ""
```

- [ ] **Step 2: Run, expect failure**

Run: `python -m pytest tests/test_lgr.py -k skipped -v`
Expected: `ImportError: cannot import name '_skipped_result'`.

- [ ] **Step 3: Implement `_skipped_result`**

Add `import json` and `from datetime import datetime, timezone` to the imports at the top of `lgr.py`. Then append before `__all__`:

```python
def _now_iso() -> str:
    """Return the current UTC time as an ISO 8601 string with microseconds.

    Returns:
        ``"YYYY-MM-DDTHH:MM:SS.ffffff+00:00"``.
    """
    return datetime.now(timezone.utc).isoformat()


def _serialize_args(args: Optional[Dict[str, Any]]) -> str:
    """Serialize a child's args dict for storage in ``args_json``.

    Uses ``default=str`` so unusual values (datetimes, paths) survive
    without raising.

    Args:
        args: The child's args dict. ``None`` and ``{}`` both round-trip
            to ``"{}"``.

    Returns:
        A compact JSON string.
    """
    return json.dumps(args or {}, default=str, sort_keys=True)


def _skipped_result(
    spec: ChildSpec,
    *,
    pipeline_run_id: str,
    pipeline_name: str,
    child_index: int,
    orchestrator_notebook: str,
) -> ChildResult:
    """Build a ``ChildResult`` for a child that was skipped by ``fail_fast``.

    Every NOT NULL log column gets a sensible default. ``started_at`` and
    ``finished_at`` are set to the moment the skip is recorded — they are
    not real durations.

    Args:
        spec: The child that did not run.
        pipeline_run_id: UUID shared across the ``run_pipeline()`` call.
        pipeline_name: Caller-supplied label.
        child_index: Zero-based position in the input list.
        orchestrator_notebook: Best-effort notebook name from runtime
            context. Empty string if unavailable.

    Returns:
        A ``ChildResult`` with ``status="skipped"``.

    Examples:
        >>> r = _skipped_result(
        ...     {"path": "/x"},
        ...     pipeline_run_id="r", pipeline_name="p",
        ...     child_index=0, orchestrator_notebook="",
        ... )
        >>> r["status"]
        'skipped'
    """
    now: str = _now_iso()
    return {
        "pipeline_run_id": pipeline_run_id,
        "pipeline_name": pipeline_name,
        "child_index": child_index,
        "notebook_path": spec["path"],
        "status": "skipped",
        "started_at": now,
        "finished_at": now,
        "duration_ms": 0,
        "exit_value": "",
        "args_json": _serialize_args(spec.get("args")),
        "error_class": "",
        "error_message": "",
        "error_traceback": "",
        "orchestrator_notebook": orchestrator_notebook,
    }
```

Also add `Optional` to the `typing` import line.

- [ ] **Step 4: Run, expect pass**

Run: `python -m pytest tests/test_lgr.py -k skipped -v`
Expected: `1 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/spark_az/lgr.py tests/test_lgr.py
git commit -m "feat(lgr): _skipped_result + ISO/args helpers"
```

---

## Task 9: `_print_line` stdout helper

**Files:**
- Modify: `src/spark_az/lgr.py`
- Modify: `tests/test_lgr.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_lgr.py`:

```python
def _result_template() -> Dict[str, Any]:
    return {
        "pipeline_run_id": "r",
        "pipeline_name": "p",
        "child_index": 0,
        "notebook_path": "/notebooks/extract",
        "status": "ok",
        "started_at": "2026-05-21T12:00:00+00:00",
        "finished_at": "2026-05-21T12:00:01.830000+00:00",
        "duration_ms": 1830,
        "exit_value": "42rows",
        "args_json": "{}",
        "error_class": "",
        "error_message": "",
        "error_traceback": "",
        "orchestrator_notebook": "",
    }


def test_print_line_ok_status(capsys: pytest.CaptureFixture) -> None:
    from spark_az.lgr import _print_line

    result: Dict[str, Any] = _result_template()
    _print_line(result, display_name="extract")

    captured: str = capsys.readouterr().out
    assert "[OK]" in captured
    assert "extract" in captured
    assert "1.83s" in captured
    assert "exit=42rows" in captured


def test_print_line_failed_status(capsys: pytest.CaptureFixture) -> None:
    from spark_az.lgr import _print_line

    result: Dict[str, Any] = _result_template()
    result["status"] = "failed"
    result["duration_ms"] = 420
    result["exit_value"] = ""
    result["error_class"] = "ValueError"
    result["error_message"] = "missing column 'id'"
    _print_line(result, display_name="transform")

    captured: str = capsys.readouterr().out
    assert "[FAIL]" in captured
    assert "transform" in captured
    assert "0.42s" in captured
    assert "ValueError: missing column 'id'" in captured


def test_print_line_skipped_status(capsys: pytest.CaptureFixture) -> None:
    from spark_az.lgr import _print_line

    result: Dict[str, Any] = _result_template()
    result["status"] = "skipped"
    result["duration_ms"] = 0
    result["exit_value"] = ""
    _print_line(result, display_name="load")

    captured: str = capsys.readouterr().out
    assert "[SKIP]" in captured
    assert "load" in captured
    assert "(fail_fast)" in captured


def test_print_line_timeout_status(capsys: pytest.CaptureFixture) -> None:
    from spark_az.lgr import _print_line

    result: Dict[str, Any] = _result_template()
    result["status"] = "timeout"
    result["duration_ms"] = 1800000
    result["error_class"] = "RuntimeError"
    result["error_message"] = "notebook timed out after 1800 seconds"
    _print_line(result, display_name="extract")

    captured: str = capsys.readouterr().out
    assert "[TIME]" in captured
    assert "1800.00s" in captured
    assert "timed out" in captured
```

- [ ] **Step 2: Run, expect failure**

Run: `python -m pytest tests/test_lgr.py -k print_line -v`
Expected: `4` failures, `ImportError: cannot import name '_print_line'`.

- [ ] **Step 3: Implement `_print_line`**

Append to `lgr.py` before `__all__`:

```python
_STATUS_BADGE: Dict[str, str] = {
    "ok": "OK",
    "failed": "FAIL",
    "timeout": "TIME",
    "skipped": "SKIP",
}

_STDOUT_NAME_WIDTH: int = 18
_STDOUT_BADGE_WIDTH: int = 6
_STDOUT_EXIT_MAX: int = 40
_STDOUT_ERROR_MAX: int = 80


def _print_line(result: ChildResult, *, display_name: str) -> None:
    """Print one human-readable stdout line for a finished child.

    Format::

        [hh:mm:ss] [STATUS] <name 18ch> <duration>  <suffix>

    - Duration is omitted for ``skipped``.
    - Suffix is ``exit=<value>`` on ok, ``<error_class>: <message>`` on
      failed/timeout, ``(fail_fast)`` on skipped.

    Args:
        result: The :class:`ChildResult` being reported.
        display_name: Pre-resolved display name (caller chooses spec
            ``name`` or basename of ``path``).
    """
    badge: str = _STATUS_BADGE.get(result["status"], result["status"].upper())
    badge_field: str = f"[{badge}]".ljust(_STDOUT_BADGE_WIDTH + 2)
    name_field: str = display_name[:_STDOUT_NAME_WIDTH].ljust(_STDOUT_NAME_WIDTH)
    clock: str = _now_clock()

    if result["status"] == "skipped":
        suffix: str = "(fail_fast)"
        duration_field: str = " " * 7
    else:
        duration_field = f"{result['duration_ms'] / 1000:>6.2f}s"
        if result["status"] == "ok":
            exit_text: str = _truncate(result["exit_value"], limit=_STDOUT_EXIT_MAX)
            suffix = f"exit={exit_text}" if exit_text else ""
        else:
            message: str = _truncate(
                result["error_message"], limit=_STDOUT_ERROR_MAX
            )
            suffix = f"{result['error_class']}: {message}".strip(": ")

    print(f"[{clock}] {badge_field} {name_field} {duration_field}  {suffix}")


def _now_clock() -> str:
    """Return the current local wall clock as ``HH:MM:SS``."""
    return datetime.now().strftime("%H:%M:%S")
```

- [ ] **Step 4: Run, expect pass**

Run: `python -m pytest tests/test_lgr.py -k print_line -v`
Expected: `4 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/spark_az/lgr.py tests/test_lgr.py
git commit -m "feat(lgr): _print_line stdout formatter"
```

---

## Task 10: `run_child` happy path

**Files:**
- Modify: `src/spark_az/lgr.py`
- Modify: `tests/test_lgr.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_lgr.py`:

```python
def test_run_child_success(fake_mssparkutils: Any) -> None:
    from spark_az.lgr import ChildSpec, run_child

    fake_mssparkutils.notebook.handler = lambda path, t, args: "42rows"
    spec: ChildSpec = {
        "path": "/notebooks/extract",
        "args": {"date": "2026-05-21"},
        "timeout_seconds": 600,
    }

    result = run_child(
        spec,
        pipeline_run_id="run-1",
        pipeline_name="nightly",
        child_index=0,
    )

    assert result["status"] == "ok"
    assert result["exit_value"] == "42rows"
    assert result["notebook_path"] == "/notebooks/extract"
    assert result["pipeline_run_id"] == "run-1"
    assert result["pipeline_name"] == "nightly"
    assert result["child_index"] == 0
    assert result["duration_ms"] >= 0
    assert result["error_class"] == ""
    assert result["error_message"] == ""
    assert result["error_traceback"] == ""
    assert result["args_json"] == '{"date": "2026-05-21"}'
    call = fake_mssparkutils.notebook.calls[0]
    assert call == {
        "path": "/notebooks/extract",
        "timeout": 600,
        "args": {"date": "2026-05-21"},
    }


def test_run_child_uses_default_timeout(fake_mssparkutils: Any) -> None:
    from spark_az.lgr import ChildSpec, run_child

    fake_mssparkutils.notebook.handler = lambda path, t, args: ""
    spec: ChildSpec = {"path": "/notebooks/x"}

    run_child(
        spec,
        pipeline_run_id="r",
        pipeline_name="p",
        child_index=0,
        default_timeout_seconds=900,
    )

    assert fake_mssparkutils.notebook.calls[0]["timeout"] == 900
```

- [ ] **Step 2: Run, expect failure**

Run: `python -m pytest tests/test_lgr.py -k run_child -v`
Expected: `ImportError: cannot import name 'run_child'`.

- [ ] **Step 3: Implement `run_child` (success path only for now)**

Add `import time` and `import traceback` to imports. Append to `lgr.py` before `__all__`:

```python
_TIMEOUT_HINTS: List[str] = ["timeout", "timed out"]


def _orchestrator_notebook_name() -> str:
    """Best-effort lookup of the calling notebook's name from runtime context.

    Returns an empty string if unavailable. Errors are swallowed because the
    field is decorative — losing it must not fail a run.
    """
    try:
        nb: Any = _nbutils()
        context: Any = getattr(getattr(nb, "runtime", None), "context", None)
        if context is None:
            return ""
        name: Any = context["currentNotebookName"]
        return str(name) if name else ""
    except Exception:
        return ""


def run_child(
    spec: ChildSpec,
    *,
    pipeline_run_id: str,
    pipeline_name: str,
    child_index: int,
    default_timeout_seconds: int = 1800,
) -> ChildResult:
    """Run one child notebook and return a :class:`ChildResult`.

    Never raises. Any exception from ``mssparkutils.notebook.run`` is
    captured into the result. The decision to re-raise lives in
    :func:`run_pipeline` so this function stays composable for future
    parallel orchestration.

    Status mapping:

    - Returns normally → ``"ok"``; ``exit_value`` is ``str(returned)``.
    - Raises with ``"timeout"`` or ``"timed out"`` in the exception message
      → ``"timeout"``.
    - Any other exception → ``"failed"``.

    Args:
        spec: The child to run.
        pipeline_run_id: UUID shared across one ``run_pipeline()`` call.
        pipeline_name: Caller-supplied label.
        child_index: Zero-based position in the input list.
        default_timeout_seconds: Used when ``spec["timeout_seconds"]`` is
            absent.

    Returns:
        A :class:`ChildResult` describing the outcome.

    Examples:
        >>> result = run_child(
        ...     {"path": "/notebooks/extract", "args": {"date": "2026-05-21"}},
        ...     pipeline_run_id="r",
        ...     pipeline_name="p",
        ...     child_index=0,
        ... )
    """
    nb: Any = _nbutils()
    timeout: int = int(spec.get("timeout_seconds", default_timeout_seconds))
    args: Dict[str, Any] = dict(spec.get("args", {}))
    args_json: str = _serialize_args(args)
    orchestrator: str = _orchestrator_notebook_name()

    started_iso: str = _now_iso()
    started_mono: float = time.monotonic()
    try:
        returned: Any = nb.notebook.run(spec["path"], timeout, args)
    except BaseException as exc:
        finished_iso: str = _now_iso()
        duration_ms: int = int((time.monotonic() - started_mono) * 1000)
        message: str = str(exc)
        status: str = (
            "timeout"
            if any(h in message.lower() for h in _TIMEOUT_HINTS)
            else "failed"
        )
        return {
            "pipeline_run_id": pipeline_run_id,
            "pipeline_name": pipeline_name,
            "child_index": child_index,
            "notebook_path": spec["path"],
            "status": status,
            "started_at": started_iso,
            "finished_at": finished_iso,
            "duration_ms": duration_ms,
            "exit_value": "",
            "args_json": args_json,
            "error_class": type(exc).__name__,
            "error_message": _truncate(message, limit=4096),
            "error_traceback": _truncate(
                traceback.format_exc(), limit=16384
            ),
            "orchestrator_notebook": orchestrator,
        }

    finished_iso = _now_iso()
    duration_ms = int((time.monotonic() - started_mono) * 1000)
    return {
        "pipeline_run_id": pipeline_run_id,
        "pipeline_name": pipeline_name,
        "child_index": child_index,
        "notebook_path": spec["path"],
        "status": "ok",
        "started_at": started_iso,
        "finished_at": finished_iso,
        "duration_ms": duration_ms,
        "exit_value": str(returned) if returned is not None else "",
        "args_json": args_json,
        "error_class": "",
        "error_message": "",
        "error_traceback": "",
        "orchestrator_notebook": orchestrator,
    }
```

Extend `__all__` to include `"run_child"`.

- [ ] **Step 4: Run, expect pass**

Run: `python -m pytest tests/test_lgr.py -k run_child -v`
Expected: `2 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/spark_az/lgr.py tests/test_lgr.py
git commit -m "feat(lgr): run_child happy path + status mapping"
```

---

## Task 11: `run_child` failure & timeout coverage

**Files:**
- Modify: `tests/test_lgr.py`

- [ ] **Step 1: Add the failure-path tests**

Append to `tests/test_lgr.py`:

```python
def test_run_child_failure_captures_traceback(fake_mssparkutils: Any) -> None:
    from spark_az.lgr import ChildSpec, run_child

    def boom(path: str, timeout: int, args: Dict[str, Any]) -> Any:
        raise ValueError("missing column 'id'")

    fake_mssparkutils.notebook.handler = boom
    spec: ChildSpec = {"path": "/notebooks/transform"}

    result = run_child(
        spec,
        pipeline_run_id="r",
        pipeline_name="p",
        child_index=1,
    )

    assert result["status"] == "failed"
    assert result["error_class"] == "ValueError"
    assert "missing column" in result["error_message"]
    assert "ValueError" in result["error_traceback"]
    assert result["exit_value"] == ""


def test_run_child_timeout_routes_to_timeout_status(
    fake_mssparkutils: Any,
) -> None:
    from spark_az.lgr import ChildSpec, run_child

    def slow(path: str, timeout: int, args: Dict[str, Any]) -> Any:
        raise RuntimeError("notebook timed out after 1800 seconds")

    fake_mssparkutils.notebook.handler = slow
    spec: ChildSpec = {"path": "/notebooks/load"}

    result = run_child(
        spec,
        pipeline_run_id="r",
        pipeline_name="p",
        child_index=2,
    )

    assert result["status"] == "timeout"
    assert result["error_class"] == "RuntimeError"
    assert "timed out" in result["error_message"]


def test_run_child_truncates_giant_traceback(
    fake_mssparkutils: Any,
) -> None:
    from spark_az.lgr import ChildSpec, run_child

    def boom(path: str, timeout: int, args: Dict[str, Any]) -> Any:
        raise RuntimeError("x" * 50000)

    fake_mssparkutils.notebook.handler = boom
    spec: ChildSpec = {"path": "/notebooks/x"}

    result = run_child(
        spec,
        pipeline_run_id="r",
        pipeline_name="p",
        child_index=0,
    )

    assert result["error_message"].endswith("…[truncated]")
    assert len(result["error_message"]) <= 4096 + len("…[truncated]")
    assert result["error_traceback"].endswith("…[truncated]")
```

- [ ] **Step 2: Run, expect pass**

Run: `python -m pytest tests/test_lgr.py -k run_child -v`
Expected: `5 passed` (the original two plus the new three).

- [ ] **Step 3: Commit**

```bash
git add tests/test_lgr.py
git commit -m "test(lgr): run_child failure + timeout coverage"
```

---

## Task 12: `_append_rows` batched Delta write

**Files:**
- Modify: `src/spark_az/lgr.py`
- Modify: `tests/test_lgr_delta.py`

- [ ] **Step 1: Write the failing integration test**

Append to `tests/test_lgr_delta.py`:

```python
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
```

- [ ] **Step 2: Run, expect failure**

Run: `python -m pytest tests/test_lgr_delta.py -k append_rows -v`
Expected: `ImportError: cannot import name '_append_rows'`.

- [ ] **Step 3: Implement `_append_rows`**

Append to `lgr.py` before `__all__`:

```python
def _append_rows(table: str, results: List[ChildResult]) -> None:
    """Append a batch of :class:`ChildResult` rows to ``table``.

    Stamps ``audited_at = current_timestamp()`` at write time. One Delta
    commit per call regardless of batch size — same trick as
    ``SyncState.upsert_all`` in spark_lib.

    Args:
        table: Fully-qualified managed Delta table name.
        results: Rows to append. Empty list is a no-op.

    Examples:
        >>> _append_rows("_meta.__pipeline_runlog", [...])
    """
    if not results:
        return
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType, LongType

    spark: Any = get_spark()
    write_schema: StructType = StructType(
        [
            StructField(name, _string_or_long(type_name), False)
            for name, type_name in LOG_SCHEMA_FIELDS
            if name not in {"audited_at", "started_at", "finished_at"}
        ]
        + [
            StructField("started_at", StringType(), False),
            StructField("finished_at", StringType(), False),
        ]
    )
    column_order: List[str] = [f.name for f in write_schema.fields]
    rows: List[Dict[str, Any]] = [
        {name: r[name] for name in column_order} for r in results
    ]
    df = spark.createDataFrame(rows, write_schema).select(
        *[
            F.to_timestamp(F.col(c)).alias(c)
            if c in {"started_at", "finished_at"}
            else F.col(c)
            for c in column_order
        ]
    ).withColumn("audited_at", F.current_timestamp())
    df.write.format("delta").mode("append").saveAsTable(table)


def _string_or_long(type_name: str) -> Any:
    """Map our schema type names to Spark types for the staging frame."""
    from pyspark.sql.types import LongType, StringType

    if type_name == "long":
        return LongType()
    return StringType()
```

- [ ] **Step 4: Run, expect pass**

Run: `python -m pytest tests/test_lgr_delta.py -v`
Expected: `4 passed` (2 from ensure + 2 from append).

- [ ] **Step 5: Commit**

```bash
git add src/spark_az/lgr.py tests/test_lgr_delta.py
git commit -m "feat(lgr): _append_rows batched Delta write"
```

---

## Task 13: `run_pipeline` — all-pass happy path

**Files:**
- Modify: `src/spark_az/lgr.py`
- Modify: `tests/test_lgr.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_lgr.py`:

```python
def test_run_pipeline_all_pass_returns_results(
    fake_mssparkutils: Any,
) -> None:
    from spark_az.lgr import ChildSpec, run_pipeline

    responses: Dict[str, str] = {
        "/notebooks/extract": "10rows",
        "/notebooks/transform": "10rows",
        "/notebooks/load": "ok",
    }
    fake_mssparkutils.notebook.handler = (
        lambda path, t, args: responses[path]
    )

    specs: List[ChildSpec] = [
        {"path": "/notebooks/extract"},
        {"path": "/notebooks/transform"},
        {"path": "/notebooks/load"},
    ]

    results = run_pipeline(
        specs,
        log_table="ignored",
        pipeline_name="nightly",
        write_log=False,
    )

    assert [r["status"] for r in results] == ["ok", "ok", "ok"]
    assert [r["child_index"] for r in results] == [0, 1, 2]
    assert {r["pipeline_run_id"] for r in results} == {results[0]["pipeline_run_id"]}
    assert results[0]["pipeline_name"] == "nightly"


def test_run_pipeline_outside_synapse_raises_before_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "notebookutils", None)
    monkeypatch.setitem(sys.modules, "mssparkutils", None)
    from spark_az.lgr import run_pipeline

    with pytest.raises(RuntimeError, match="mssparkutils"):
        run_pipeline(
            [{"path": "/x"}],
            log_table="t",
            pipeline_name="p",
            write_log=False,
        )
```

- [ ] **Step 2: Run, expect failure**

Run: `python -m pytest tests/test_lgr.py -k run_pipeline -v`
Expected: `ImportError: cannot import name 'run_pipeline'`.

- [ ] **Step 3: Implement `run_pipeline` (happy path + outside-Synapse guard)**

Add `import os` and `import uuid` to imports. Append to `lgr.py` before `__all__`:

```python
def _display_name(spec: ChildSpec) -> str:
    """Resolve the display name for stdout: explicit name → basename of path."""
    explicit: Optional[str] = spec.get("name")
    if explicit:
        return str(explicit)
    return os.path.basename(str(spec["path"]).rstrip("/")) or str(spec["path"])


def run_pipeline(
    children: Iterable[ChildSpec],
    *,
    log_table: str,
    pipeline_name: str,
    fail_fast: bool = True,
    default_timeout_seconds: int = 1800,
    write_log: bool = True,
) -> List[ChildResult]:
    """Run a sequence of child notebooks and log each one.

    Generates one ``pipeline_run_id`` (UUID4) for the call. For each child:

    1. Calls ``mssparkutils.notebook.run(path, timeout_seconds, args)``.
    2. Captures wall time, exit value, and exception (if any).
    3. Prints one stdout line.
    4. Appends a :class:`ChildResult` to the in-memory result list.

    After the loop, the full result list is written to ``log_table`` in one
    Delta append (when ``write_log=True``). If ``fail_fast=True`` and any
    child failed, the captured failure is re-raised AFTER the log write so
    the orchestrator notebook itself fails in Synapse and the log table is
    durable for post-mortem.

    Args:
        children: Iterable of :class:`ChildSpec` entries.
        log_table: Fully-qualified managed Delta table for the log rows.
            Created via :func:`ensure_log_table` if missing.
        pipeline_name: Caller-supplied label stamped on every row.
        fail_fast: When ``True`` (default), the first failure marks
            remaining children as ``status="skipped"`` and the call
            re-raises after the log write. When ``False``, every child is
            attempted and the call returns normally with failures captured
            as rows.
        default_timeout_seconds: Used when a :class:`ChildSpec` does not
            specify its own ``timeout_seconds``.
        write_log: When ``False``, prints stdout but skips the Delta
            write. Used by tests and dry runs.

    Returns:
        When ``fail_fast=False`` or all children succeeded: the full
        ``List[ChildResult]`` in input order. When ``fail_fast=True`` and
        a child failed, the function re-raises instead of returning.

    Raises:
        RuntimeError: ``mssparkutils`` not importable (not in Synapse).
            Raised before the child loop.
        RuntimeError: Re-raised after the log write when ``fail_fast=True``
            and any child failed. Message carries the first failing child's
            ``error_class`` and ``error_message``.

    Examples:
        Sequential run with fail-fast:

        >>> results = run_pipeline(
        ...     [
        ...         {"path": "/notebooks/extract", "args": {"date": "2026-05-21"}},
        ...         {"path": "/notebooks/transform"},
        ...         {"path": "/notebooks/load"},
        ...     ],
        ...     log_table="_meta.__pipeline_runlog",
        ...     pipeline_name="nightly_lab_refresh",
        ... )

        Run-all (continue past failures):

        >>> results = run_pipeline(
        ...     specs,
        ...     log_table="_meta.__pipeline_runlog",
        ...     pipeline_name="p",
        ...     fail_fast=False,
        ... )
        >>> failed = [r for r in results if r["status"] != "ok"]
    """
    _nbutils()

    pipeline_run_id: str = str(uuid.uuid4())
    orchestrator: str = _orchestrator_notebook_name()
    spec_list: List[ChildSpec] = list(children)
    results: List[ChildResult] = []
    first_failure: Optional[ChildResult] = None

    try:
        for i, spec in enumerate(spec_list):
            if first_failure is not None and fail_fast:
                skipped: ChildResult = _skipped_result(
                    spec,
                    pipeline_run_id=pipeline_run_id,
                    pipeline_name=pipeline_name,
                    child_index=i,
                    orchestrator_notebook=orchestrator,
                )
                results.append(skipped)
                _print_line(skipped, display_name=_display_name(spec))
                continue
            result: ChildResult = run_child(
                spec,
                pipeline_run_id=pipeline_run_id,
                pipeline_name=pipeline_name,
                child_index=i,
                default_timeout_seconds=default_timeout_seconds,
            )
            results.append(result)
            _print_line(result, display_name=_display_name(spec))
            if result["status"] != "ok" and first_failure is None:
                first_failure = result
    finally:
        if write_log:
            ensure_log_table(log_table)
            _append_rows(log_table, results)

    if first_failure is not None and fail_fast:
        raise RuntimeError(
            f"child {first_failure['child_index']} "
            f"({first_failure['notebook_path']}) "
            f"{first_failure['status']}: "
            f"{first_failure['error_class']}: "
            f"{first_failure['error_message']}"
        )
    return results
```

Extend `__all__` to include `"run_pipeline"`.

- [ ] **Step 4: Run, expect pass**

Run: `python -m pytest tests/test_lgr.py -k run_pipeline -v`
Expected: `2 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/spark_az/lgr.py tests/test_lgr.py
git commit -m "feat(lgr): run_pipeline happy path + Synapse guard"
```

---

## Task 14: `run_pipeline` — `fail_fast=True` failure path

**Files:**
- Modify: `tests/test_lgr.py`

- [ ] **Step 1: Add the failure tests**

Append to `tests/test_lgr.py`:

```python
def test_run_pipeline_fail_fast_skips_remaining_and_raises(
    fake_mssparkutils: Any,
) -> None:
    from spark_az.lgr import ChildSpec, run_pipeline

    def handler(path: str, t: int, args: Dict[str, Any]) -> Any:
        if path == "/notebooks/transform":
            raise ValueError("bad data")
        return "ok"

    fake_mssparkutils.notebook.handler = handler
    specs: List[ChildSpec] = [
        {"path": "/notebooks/extract"},
        {"path": "/notebooks/transform"},
        {"path": "/notebooks/load"},
    ]

    with pytest.raises(RuntimeError, match="ValueError: bad data"):
        run_pipeline(
            specs,
            log_table="ignored",
            pipeline_name="nightly",
            write_log=False,
            fail_fast=True,
        )

    paths_called: List[str] = [
        c["path"] for c in fake_mssparkutils.notebook.calls
    ]
    assert paths_called == ["/notebooks/extract", "/notebooks/transform"]


def test_run_pipeline_fail_fast_writes_log_before_raising(
    fake_mssparkutils: Any, registered_spark: Any
) -> None:
    """The Delta log must be durable even on fail_fast re-raise."""
    from spark_az.lgr import ChildSpec, run_pipeline

    spark: Any = registered_spark
    table: str = "default.test_runpipeline_failfast"
    spark.sql(f"DROP TABLE IF EXISTS {table}")

    def handler(path: str, t: int, args: Dict[str, Any]) -> Any:
        if path == "/notebooks/x2":
            raise ValueError("nope")
        return "ok"

    fake_mssparkutils.notebook.handler = handler
    specs: List[ChildSpec] = [
        {"path": "/notebooks/x1"},
        {"path": "/notebooks/x2"},
        {"path": "/notebooks/x3"},
    ]

    with pytest.raises(RuntimeError):
        run_pipeline(
            specs,
            log_table=table,
            pipeline_name="p",
            fail_fast=True,
        )

    rows = spark.table(table).orderBy("child_index").collect()
    assert [r["status"] for r in rows] == ["ok", "failed", "skipped"]
    assert rows[1]["error_class"] == "ValueError"
    assert rows[2]["notebook_path"] == "/notebooks/x3"
```

- [ ] **Step 2: Run, expect pass** (the implementation from Task 13 already handles this — these tests verify the full contract)

Run: `python -m pytest tests/test_lgr.py tests/test_lgr_delta.py -k run_pipeline -v`
Expected: All tests pass (including the two new ones).

If any fail, re-read Task 13's implementation against the failing assertion and fix before continuing.

- [ ] **Step 3: Commit**

```bash
git add tests/test_lgr.py
git commit -m "test(lgr): fail_fast skips + writes log before raise"
```

---

## Task 15: `run_pipeline` — `fail_fast=False`

**Files:**
- Modify: `tests/test_lgr.py`

- [ ] **Step 1: Add the test**

Append to `tests/test_lgr.py`:

```python
def test_run_pipeline_fail_fast_false_runs_everything(
    fake_mssparkutils: Any,
) -> None:
    from spark_az.lgr import ChildSpec, run_pipeline

    def handler(path: str, t: int, args: Dict[str, Any]) -> Any:
        if path == "/notebooks/middle":
            raise ValueError("bad")
        return "ok"

    fake_mssparkutils.notebook.handler = handler
    specs: List[ChildSpec] = [
        {"path": "/notebooks/first"},
        {"path": "/notebooks/middle"},
        {"path": "/notebooks/last"},
    ]

    results = run_pipeline(
        specs,
        log_table="ignored",
        pipeline_name="p",
        write_log=False,
        fail_fast=False,
    )

    assert [r["status"] for r in results] == ["ok", "failed", "ok"]
    paths_called: List[str] = [
        c["path"] for c in fake_mssparkutils.notebook.calls
    ]
    assert paths_called == [
        "/notebooks/first",
        "/notebooks/middle",
        "/notebooks/last",
    ]
```

- [ ] **Step 2: Run, expect pass**

Run: `python -m pytest tests/test_lgr.py -k fail_fast_false -v`
Expected: `1 passed`.

- [ ] **Step 3: Commit**

```bash
git add tests/test_lgr.py
git commit -m "test(lgr): fail_fast=False runs everything"
```

---

## Task 16: `__init__.py` re-exports + import smoke test

**Files:**
- Modify: `src/spark_az/__init__.py`
- Modify: `tests/test_lgr.py`

- [ ] **Step 1: Replace `src/spark_az/__init__.py`**

```python
"""Azure Synapse Spark notebook orchestration + Delta logging."""
from __future__ import annotations

from .lgr import (
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
```

- [ ] **Step 2: Add the smoke test**

Append to `tests/test_lgr.py`:

```python
def test_public_api_reexported_from_package_root() -> None:
    import spark_az

    expected: List[str] = [
        "ChildResult",
        "ChildSpec",
        "ensure_log_table",
        "get_spark",
        "run_child",
        "run_pipeline",
        "set_spark",
    ]
    for name in expected:
        assert hasattr(spark_az, name), f"spark_az missing {name}"
    assert set(spark_az.__all__) == set(expected)
```

- [ ] **Step 3: Run the whole suite**

Run: `python -m pytest -v`
Expected: every test passes (sessions + lgr unit + delta integration).

- [ ] **Step 4: Commit**

```bash
git add src/spark_az/__init__.py tests/test_lgr.py
git commit -m "feat(spark_az): re-export public API + smoke test"
```

---

## Task 17: `notebooks/_logging/lgr.py` (jupytext source)

**Files:**
- Create: `notebooks/_logging/lgr.py`

- [ ] **Step 1: Create the notebooks directory**

```bash
mkdir -p notebooks
```

- [ ] **Step 2: Write `notebooks/_logging/lgr.py`** (jupytext "percent" format)

```python
# ---
# jupyter:
#   jupytext:
#     formats: py:percent,ipynb
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# # lgr
#
# Orchestrates child Synapse notebooks via mssparkutils.notebook.run and
# writes a Delta log row per child. Invoke from a Synapse pipeline
# notebook activity with the `notebooks` parameter set.

# %% tags=["parameters"]
from __future__ import annotations

from typing import Any, Dict, List

notebooks: List[Dict[str, Any]] = []
log_table: str = "_meta.__pipeline_runlog"
pipeline_name: str = ""
fail_fast: bool = True
default_timeout_seconds: int = 1800

# %%
from spark_az import run_pipeline

run_pipeline(
    notebooks,
    log_table=log_table,
    pipeline_name=pipeline_name,
    fail_fast=fail_fast,
    default_timeout_seconds=default_timeout_seconds,
)
```

The `# %%` and `# %% tags=["parameters"]` lines are jupytext cell markers (not "comments" in the style sense) — they delimit notebook cells and must stay literal.

- [ ] **Step 3: Verify the file is valid Python**

Run: `python -c "import ast; ast.parse(open('notebooks/_logging/lgr.py').read()); print('ok')"`
Expected: `ok`.

- [ ] **Step 4: Commit**

```bash
git add notebooks/_logging/lgr.py
git commit -m "feat(notebook): jupytext orchestrator lgr.py"
```

---

## Task 18: `scripts/build_notebooks.sh` + generated `.ipynb`

**Files:**
- Create: `scripts/build_notebooks.sh`
- Create: `notebooks/_logging/lgr.ipynb` (generated)

- [ ] **Step 1: Write `scripts/build_notebooks.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

jupytext --to ipynb notebooks/*.py
```

- [ ] **Step 2: Mark executable**

```bash
chmod +x scripts/build_notebooks.sh
```

- [ ] **Step 3: Run the build**

Run: `bash scripts/build_notebooks.sh`
Expected: stdout contains `[jupytext] Writing notebooks/_logging/lgr.ipynb` (or similar), and the file `notebooks/_logging/lgr.ipynb` now exists.

- [ ] **Step 4: Verify the generated notebook is valid JSON and has a parameter cell**

Run: `python -c "import json,sys; nb=json.load(open('notebooks/_logging/lgr.ipynb')); tags=[c.get('metadata',{}).get('tags',[]) for c in nb['cells']]; print('ok' if any('parameters' in t for t in tags) else sys.exit('no parameter cell'))"`
Expected: `ok`.

- [ ] **Step 5: Round-trip check (`.py` and `.ipynb` stay in sync)**

Run: `jupytext --test notebooks/_logging/lgr.py`
Expected: exits 0 with no diff.

- [ ] **Step 6: Commit both**

```bash
git add scripts/build_notebooks.sh notebooks/_logging/lgr.ipynb
git commit -m "build(notebooks): jupytext build script + generated ipynb"
```

---

## Self-review (run before declaring the plan complete)

**Spec coverage:**

- ✅ `ChildSpec`, `ChildResult` TypedDicts → Task 3
- ✅ Delta schema (every column) → Task 3 (constant) + Task 7 (`ensure_log_table`) + Task 12 (`_append_rows`)
- ✅ `ensure_log_table` idempotent creation → Task 7
- ✅ `run_child` status mapping (ok/failed/timeout) → Tasks 10–11
- ✅ Truncation of `error_message` / `error_traceback` → Tasks 5, 11
- ✅ `args_json` serialization with `default=str` → Task 8
- ✅ `orchestrator_notebook` best-effort lookup → Task 10
- ✅ Stdout format (timestamp, badge, padded name, duration, suffix) → Task 9
- ✅ `run_pipeline` happy path + Synapse guard → Task 13
- ✅ `fail_fast=True` skip-remaining + re-raise + log-before-raise → Tasks 13–14
- ✅ `fail_fast=False` returns full list with failures captured → Task 15
- ✅ `write_log=False` for tests → Task 13
- ✅ Public API re-exports → Task 16
- ✅ Jupytext source + generated `.ipynb` + build script → Tasks 17–18
- ✅ `pyproject.toml` with empty `dependencies` and extras → Task 1

**Placeholder scan:** No "TBD", "TODO", or "similar to" references. Every code step has full code. Every command has expected output.

**Type / name consistency:**

- `LOG_SCHEMA_FIELDS` is the single source of truth for column names — used in Task 3 (definition), Task 7 (`ensure_log_table` test), Task 12 (`_append_rows` test).
- `_now_iso`, `_now_clock`, `_truncate`, `_serialize_args`, `_orchestrator_notebook_name`, `_print_line`, `_skipped_result`, `_append_rows`, `_display_name`, `_string_or_long`, `_nbutils` — every private helper introduced is used by at least one task that follows.
- `run_child` signature stays identical across Tasks 10, 11, 13.
- `run_pipeline` signature locked in Task 13; later tasks only add tests.
- Status values (`"ok"` / `"failed"` / `"timeout"` / `"skipped"`) consistent across helper, badge map, schema-default rows.

**Scope check:** This plan ships exactly the v1 surface described in the spec. v2 work (parallel orchestration, step-grain logging in children, Synapse pipeline JSON builder, `pipeline_run_id` propagation from `@pipeline().RunId`) is explicitly out — separate spec → plan cycles when those land.
