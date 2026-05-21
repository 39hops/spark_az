# Pipeline Starter Notebook — Design Spec (v2)

**Date:** 2026-05-21
**Status:** Locked
**Author:** Artin (with Claude)
**Builds on:** `2026-05-21-pipeline-logger-design.md` (v1).

## Locked decisions

| Decision | Choice |
| --- | --- |
| Deliverable | **One polished `notebooks/_logging/lgr_starter.ipynb`** (+ jupytext `.py`) that's the drop-in starting point for any new Synapse pipeline. |
| JSON logging | **Both paths.** Default formatter on the module logger is JSON-on-stdout; an opt-in `enable_app_insights(connection_string)` helper attaches an Azure Monitor OpenTelemetry handler when called. |
| In-notebook step grain | **`step(name, **attrs)` context manager** emits structured log records keyed by the active `pipeline_run_id`. Logs only in v2 — Delta table for steps is deferred to v3. |
| Pipeline params | **`PipelineParams` typed helper** that reads Synapse-passed args from the parameter cell, validates required keys, applies defaults, and exposes a typed object. |
| Synapse pipeline JSON | **Reference template at `synapse/lgr_starter_pipeline.json`** — importable into Synapse Studio with the parameter wiring done. |
| Library scope | **Additions land in `src/spark_az/lgr.py`** (same single module per v1 design). Refactor to submodules only if the file exceeds ~700 lines after this work. |
| Dependencies | `python-json-logger` and `azure-monitor-opentelemetry` join the `dev`/`spark`/`test` extras pattern — both **optional** so the base package stays zero-dep. |
| Notebook style | Same hand-maintained jupytext format as v1 inline notebook. Section-by-section MD documentation. |

## New library surface (additions only)

### `JsonFormatter(logging.Formatter)`

```python
from typing import Any, Dict


class JsonFormatter(logging.Formatter):
    """logging.Formatter that emits one JSON object per record.

    Fields:
        ts: ISO 8601 UTC timestamp.
        level: ``record.levelname``.
        logger: ``record.name``.
        msg: The formatted message.
        pipeline_run_id, pipeline_name, child_index, step: Pulled from
            ``record`` extras when set via ``logging.LoggerAdapter`` or
            ``log.info(..., extra=...)``.
        exc_info: Stringified traceback if present.

    Examples:
        >>> import logging
        >>> handler = logging.StreamHandler()
        >>> handler.setFormatter(JsonFormatter())
        >>> log.addHandler(handler)
    """
```

### `set_json_formatter(level: int = logging.INFO) -> None`

Idempotent. Swaps the default `spark_az.lgr` handler's formatter to `JsonFormatter`. Called at the top of `lgr_starter.ipynb`.

### `enable_app_insights(connection_string: str, level: int = logging.INFO) -> None`

If `azure-monitor-opentelemetry` is importable, attaches a `LoggingHandler` from `azure.monitor.opentelemetry` to the `spark_az.lgr` logger. Raises a clear `ImportError` with install instructions when the dep is missing. Idempotent (won't double-attach).

### `step(name: str, *, log: logging.Logger = ..., **attrs: Any)` context manager

```python
with step("extract", source="lab.raw"):
    df = source.read()

with step("transform") as s:
    out = clean(df)
    s.metric("rows_out", out.count())
```

Emits:
- One INFO record on entry: `{"step": name, "phase": "start", **attrs}`.
- One INFO record on success exit: `{"step": name, "phase": "ok", "duration_ms": N, **attrs, **metrics}`.
- One ERROR record on exception: `{"step": name, "phase": "failed", "duration_ms": N, "error_class": ..., "error_message": ...}`, then re-raises.

`s.metric(key, value)` accumulates into the exit record. `attrs` passed to `step(...)` carry through to all three records.

Step records SHARE the pipeline_run_id from the currently active `run_pipeline()` call when invoked inside `run_child` — but for v2's notebook-driven use (where the orchestrator notebook calls `step()` directly), the caller supplies a `pipeline_run_id` either via `step(... pipeline_run_id=...)` or via a module-level `set_active_run_id(...)` (set automatically by `run_pipeline` and exposed for direct callers).

### `PipelineParams` TypedDict + `read_pipeline_params()` helper

```python
class PipelineParams(TypedDict, total=False):
    """Standard Synapse pipeline parameters consumed by the starter notebook."""
    pipeline_run_id: str       # @pipeline().RunId, optional override
    pipeline_name: str         # @pipeline().Pipeline
    log_table: str             # e.g. _meta.__pipeline_runlog
    fail_fast: bool
    default_timeout_seconds: int
    notebooks: List[Dict[str, Any]]
    extras: Dict[str, Any]     # caller-defined free-form bag


def read_pipeline_params(
    *,
    pipeline_name: str,
    log_table: str,
    notebooks: List[Dict[str, Any]],
    fail_fast: bool = True,
    default_timeout_seconds: int = 1800,
    pipeline_run_id: Optional[str] = None,
    extras: Optional[Dict[str, Any]] = None,
) -> PipelineParams:
    """Build a validated PipelineParams from raw Synapse-passed args.

    Raises:
        ValueError: ``pipeline_name`` empty; ``log_table`` empty;
            ``notebooks`` not a list of dicts each containing ``path``.
    """
```

## New artifacts

### `notebooks/_logging/lgr_starter.py` (jupytext)

Single self-contained starter notebook. Cell layout:

1. Markdown — "# Pipeline starter"; what to fill in.
2. Markdown — "## How to use" (3 modes: Synapse pipeline activity, interactive run, library import).
3. Markdown — "## Parameters" (table of every param).
4. Parameter cell — `notebooks`, `log_table`, `pipeline_name`, `fail_fast`, `default_timeout_seconds`, `app_insights_connection_string` (optional).
5. Markdown — "## Setup".
6. Code — imports `spark_az` library symbols; `set_json_formatter()`; optional `if app_insights_connection_string: enable_app_insights(...)`.
7. Markdown — "## Pipeline definition".
8. Code — `params = read_pipeline_params(...)`.
9. Markdown — "## Optional: per-step work in the orchestrator".
10. Code — placeholder `with step("custom_prep"): pass` example.
11. Markdown — "## Run".
12. Code — `if params["notebooks"]: results = run_pipeline(...)`.
13. Markdown — "## Inspect" — sample SQL.

Same hand-maintained pattern as `lgr_inline.py` — uses the installed library, NOT inline. The starter is for users who already have spark_az installed on their pool. (Users who don't can still use `lgr_inline.ipynb`.)

### `synapse/lgr_starter_pipeline.json`

A Synapse pipeline JSON containing one Notebook activity that calls `lgr_starter` with all the parameters wired. User imports it into Synapse Studio, replaces the `referenceName` with their notebook's name, and ships.

Structure:

```json
{
  "name": "spark_az_starter_pipeline",
  "properties": {
    "activities": [
      {
        "name": "run_starter",
        "type": "SynapseNotebook",
        "typeProperties": {
          "notebook": {"referenceName": "lgr_starter", "type": "NotebookReference"},
          "parameters": {
            "pipeline_run_id":          {"value": "@pipeline().RunId", "type": "string"},
            "pipeline_name":            {"value": "@pipeline().Pipeline", "type": "string"},
            "log_table":                {"value": "_meta.__pipeline_runlog", "type": "string"},
            "notebooks":                {"value": "...", "type": "array"},
            "fail_fast":                {"value": "true", "type": "bool"},
            "default_timeout_seconds":  {"value": "1800", "type": "int"},
            "app_insights_connection_string": {"value": "", "type": "string"}
          }
        }
      }
    ]
  }
}
```

Doc note in `README.md` and the starter notebook explains how to import + customize.

## Tests

| Layer | What |
| --- | --- |
| `tests/test_lgr.py` | `JsonFormatter` produces parseable JSON with expected fields; `set_json_formatter` is idempotent; `step()` emits start/ok/failed records with timing; `step.metric()` accumulates; `read_pipeline_params` raises on invalid inputs; `enable_app_insights` raises informatively when the dep is missing (no need to install the heavy Azure dep to test the missing-import path). |
| `tests/test_lgr_delta.py` | Unchanged. |
| `tests/test_session.py` | Unchanged. |

Target: net +12–18 tests, full suite still green in under 20 s.

## Scope boundary

**In v2:**

- `JsonFormatter`, `set_json_formatter`, `enable_app_insights`, `step`, `read_pipeline_params`.
- `notebooks/lgr_starter.{py,ipynb}`.
- `synapse/lgr_starter_pipeline.json`.
- README + CAPABILITIES updates.

**Explicitly out of v2 (deferred):**

- **Delta table for `step` rows.** v2 logs steps as structured log records only. v3 adds a `step_log_table` and persists each step.
- **Pipeline JSON Python builder.** v2 ships a static reference template; a programmatic builder is its own future spec.
- **Retry-aware error reporting.** v2 surfaces failures; Synapse activity-level retry handling lands in v3+.

**Not pursuing:**

- Replacing the existing `_print_line` human-readable output. JSON is opt-in via `set_json_formatter`; the default stays human-readable for `%run` use.
- Auto-generation of the inline notebook (still hand-maintained).
