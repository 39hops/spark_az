# spark_az

Azure Synapse Spark notebook orchestration with structured Delta logging.

One orchestrator notebook runs a sequence of child notebooks via
`mssparkutils.notebook.run`, captures exit values and exceptions, and
writes one structured Delta row per child to a managed log table. The
entire library lives inline in a single notebook so there's no wheel
to build, no Spark-pool packages to register — drop the notebook in,
set parameters, run.

Style and conventions mirror the companion library
[`github.com/39hops/spark_lib`](https://github.com/39hops/spark_lib).

## Setup

1. **Upload** `notebooks/lgr.ipynb` into your Synapse workspace
   (Develop hub → `+` → Import).
2. **Attach** it to a Spark pool.
3. **Import** the reference pipeline at `synapse/lgr_pipeline.json`
   (Integrate hub → `+` → Pipeline → `{}` Code view → paste).
4. Customise the `notebooks` parameter on the pipeline activity with
   your child notebook paths. Publish.

The `_meta.__pipeline_runlog` Delta table is created automatically on
the first run.

## Pipeline parameters

| Parameter | Value | Notes |
| --- | --- | --- |
| `pipeline_run_id` | `@pipeline().RunId` | Synapse-injected; ties Delta rows to the pipeline run. |
| `pipeline_name` | `@pipeline().Pipeline` | Stamped on every row. |
| `log_table` | `_meta.__pipeline_runlog` | Managed Delta table. Database created on first run. |
| `notebooks` | `[{"path": "...", "args": {...}}, ...]` | List of children to orchestrate. |
| `fail_fast` | `true` (default) | Re-raises after writing the log on first failure. |
| `default_timeout_seconds` | `1800` | Per-child default; override per-spec via `timeout_seconds`. |
| `app_insights_connection_string` | `""` (default) | If set, fans logs out to App Insights. |

## Querying the log table

```sql
SELECT pipeline_run_id, child_index, notebook_path, status,
       duration_ms / 1000 AS seconds, error_class, error_message
FROM   _meta.__pipeline_runlog
WHERE  pipeline_name = '<your pipeline>'
ORDER  BY pipeline_run_id DESC, child_index;
```

Run summary across many pipeline executions:

```sql
SELECT pipeline_run_id, COUNT(*) AS children,
       SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
       MIN(started_at) AS run_start,
       MAX(finished_at) AS run_end
FROM   _meta.__pipeline_runlog
WHERE  pipeline_name = '<your pipeline>'
GROUP  BY pipeline_run_id
ORDER  BY run_start DESC
LIMIT  20;
```

## Failure semantics

- `fail_fast=True` (default): on the first failed child the remaining
  children are recorded as `status="skipped"` rows, the log table is
  written, then a `RuntimeError` is re-raised so the orchestrator
  notebook itself fails. Upstream Synapse pipeline activities see the
  failure.
- `fail_fast=False`: every child runs, failures are captured as rows,
  the call returns normally and the caller decides what to do.

## JSON logging + App Insights

JSON-structured stdout is on by default in `notebooks/lgr.ipynb` —
each `log.info(...)` is one JSON object:

```json
{"ts": "2026-05-21T14:02:11+00:00", "level": "INFO", "logger": "spark_az.lgr", "msg": "[OK] extract 1.83s", "pipeline_run_id": "...", "step": "extract", "duration_ms": 1830}
```

Synapse captures stdout into driver logs. To also fan out to Azure
Application Insights / Log Analytics, set the
`app_insights_connection_string` pipeline parameter to your connection
string. The setup cell calls `enable_app_insights(...)` automatically
when it's non-empty.

Requires the `azure-monitor-opentelemetry` package on the Spark pool.
Without it, `enable_app_insights` raises an `ImportError` with install
instructions.

## In-orchestrator step timing

`step()` is in scope inside the notebook. Use it around any work you
want timed and structured-logged:

```python
with step("preflight", pipeline=pipeline_name) as s:
    rows = source_count()
    s.metric("rows_seen", rows)

with step("aggregate"):
    publish_summary()
```

Each step emits start / ok / failed structured log records with the
active `pipeline_run_id` attached. Step records currently land in
stdout (and any attached handler) — they are not yet written to their
own Delta table.

## Workspace utilities

Two standalone tools under `tools/` for inspecting a whole Lake Database,
independent of the orchestrator and of any install — paste the file into a
Synapse notebook cell (or import `tools/db_search.ipynb`), set the parameters
at the top, and run. Both enumerate tables from the catalog and fan out across
them with a thread pool.

- **`tools/db.py`** — scans every database for column names containing
  characters outside `[a-zA-Z0-9_-]`, the ones that block Lake Database
  publish validation, and prints a report.
- **`tools/db_search.{py,ipynb}`** — searches every table for values. Set
  `SEARCH_WORDS` and `COND` (`OR`/`AND`); matching is type-aware per column
  (string substring, numeric exact, ISO-date match). Optionally scope by
  database (`SEARCH_DBS`) and by ingestion window (`DATE_RANGE` on
  `INGESTED_COL`). Matched rows are written to the abfss container as one
  Excel workbook — a summary sheet plus one sheet per table. Requires
  `pandas` + `openpyxl` on the Spark pool.

## Local development

The library lives at `src/spark_az/` with full pytest coverage.
`notebooks/lgr.py` is the inlined form, auto-generated from the
library by `scripts/inline_lgr_notebook.py`. Workflow:

```sh
scripts/setup.sh                # pip install -e .[test,dev] in your venv
scripts/test.sh                 # pytest — 48 tests, ~15 s
scripts/build_notebooks.sh      # rebuilds notebooks/lgr.{py,ipynb} from src/
```

When you change anything in `src/spark_az/`, re-run
`scripts/build_notebooks.sh` and commit both the source and the
regenerated notebook.

Tests use:

- `pytest` with a local Delta-enabled `SparkSession` fixture
  (`tests/conftest.py`), built via `configure_spark_with_delta_pip`.
- `fake_mssparkutils` fixture that installs configurable stand-ins
  for the `mssparkutils` and `notebookutils.mssparkutils` import paths
  via `monkeypatch.setitem(sys.modules, ...)`, so unit tests run
  without Synapse.

## Project layout

```
src/spark_az/
├── __init__.py             # public surface re-exports
├── session.py              # get_spark / set_spark — never calls getOrCreate
└── lgr.py                  # ChildSpec / ChildResult / run_child / run_pipeline / step / ...

notebooks/
└── lgr.{py,ipynb}          # all-in-one drop-in notebook (auto-inlined from src/)

synapse/
└── lgr_pipeline.json       # reference Synapse pipeline JSON wiring lgr

scripts/
├── setup.sh                # editable install of test+dev extras
├── test.sh                 # pytest wrapper
├── build_notebooks.sh      # inline_lgr_notebook.py + jupytext --sync
└── inline_lgr_notebook.py  # builds notebooks/lgr.py from src/spark_az/

tools/
├── db.py                   # audit workspace column names for bad characters
└── db_search.{py,ipynb}    # search every lake-database table for values

docs/superpowers/
├── specs/                  # locked-decision design docs (v1, v2)
└── plans/                  # task-by-task implementation plans
```

See `AGENTS.md` for the honesty contract every contributor (human or
agent) follows, `CLAUDE.md` for the project operating doc, and
`docs/ARCHITECTURE.md` for the interface and data-flow detail.

## License

MIT. See `LICENSE`.
