# spark_az

Azure Synapse Spark notebook orchestration with structured Delta logging.

A small Python library + a self-contained Synapse notebook that lets one
"orchestrator" notebook run a sequence of child notebooks via
`mssparkutils.notebook.run`, capture exit values and exceptions, and write
one structured Delta row per child to a managed log table.

Style and conventions mirror the companion library
[`github.com/39hops/spark_lib`](https://github.com/39hops/spark_lib).

## Two ways to use it

### 1. Zero-install — `%run` the inline notebook (recommended for first use)

Upload `notebooks/pipeline_logger_inline.ipynb` to your Synapse workspace,
then from any other notebook:

```python
%run "Shared/lib/pipeline_logger_inline"

results = run_pipeline(
    [
        {"path": "Shared/etl/extract",   "args": {"date": "2026-05-21"}},
        {"path": "Shared/etl/transform"},
        {"path": "Shared/etl/load",      "timeout_seconds": 3600},
    ],
    log_table="lab.__pipeline_runlog",
    pipeline_name="nightly_lab_refresh",
)
```

No wheel, no Spark-pool package management — `%run` brings every public
symbol (`run_pipeline`, `run_child`, `ChildSpec`, `ChildResult`,
`ensure_log_table`, `set_spark`, `get_spark`, `log`) into the caller's
scope. The same notebook also works as a Synapse pipeline notebook
activity: set the `notebooks` / `log_table` / `pipeline_name` parameters
from the activity, run all cells.

### 2. Install the wheel — for repeat use across many notebooks

```sh
scripts/build.sh                # builds dist/spark_az-0.1.0-py3-none-any.whl
```

Upload the wheel to ADLS, register it as a workspace package on your
Synapse Spark pool, then in any notebook on that pool:

```python
from spark_az import run_pipeline, ChildSpec

results = run_pipeline([...], log_table="lab.__pipeline_runlog", pipeline_name="...")
```

Use `notebooks/pipeline_logger.ipynb` (the thin orchestrator) the same
way as the inline version.

## What you get in the log table

One Delta row per child notebook invocation, plus a `"skipped"` row for
every child that didn't get to run when `fail_fast=True` halts the loop.
Columns: `pipeline_run_id`, `pipeline_name`, `child_index`,
`notebook_path`, `status` (`ok` | `failed` | `timeout` | `skipped`),
`started_at`, `finished_at`, `duration_ms`, `exit_value`, `args_json`,
`error_class`, `error_message`, `error_traceback`,
`orchestrator_notebook`, `audited_at`.

```sql
SELECT pipeline_run_id, child_index, notebook_path, status,
       duration_ms / 1000 AS seconds, error_class, error_message
FROM   lab.__pipeline_runlog
WHERE  pipeline_name = 'nightly_lab_refresh'
ORDER  BY pipeline_run_id DESC, child_index;
```

## Failure semantics

- `fail_fast=True` (default): on the first failed child the remaining
  children are recorded as `status="skipped"` rows, the log table is
  written, then a `RuntimeError` is re-raised so the orchestrator
  notebook itself fails. Upstream Synapse pipeline activities see the
  failure.
- `fail_fast=False`: every child runs, failures are captured as rows,
  the call returns normally and the caller decides what to do.

## App Insights

`pipeline_logger.log` is a stdlib `logging.Logger`. Bolt on an Azure
handler at notebook top to fan out:

```python
import logging
from azure.monitor.opentelemetry.exporter import AzureMonitorLogExporter
logging.getLogger("spark_az.pipeline_logger").addHandler(
    AzureMonitorLogExporter.from_connection_string("...")
)
```

No code change in the library.

## Local development

```sh
scripts/setup.sh                # pip install -e .[test,dev] in your venv
scripts/test.sh                 # pytest — 34 tests, ~10 s after first run
scripts/build.sh                # build the wheel
scripts/build_notebooks.sh      # regenerate notebooks/*.ipynb from *.py via jupytext
```

Tests use:

- `pytest` with a local Delta-enabled `SparkSession` fixture
  (`tests/conftest.py`), built via `configure_spark_with_delta_pip`.
- `fake_mssparkutils` fixture that installs configurable stand-ins for
  the `mssparkutils` and `notebookutils.mssparkutils` import paths via
  `monkeypatch.setitem(sys.modules, ...)`, so unit tests run without
  Synapse.

## Project layout

```
src/spark_az/
├── __init__.py             # public surface re-exports
├── session.py              # get_spark / set_spark — never calls getOrCreate
└── pipeline_logger.py      # ChildSpec / ChildResult / run_child / run_pipeline / ensure_log_table

notebooks/
├── pipeline_logger.{py,ipynb}          # thin wrapper, imports installed library
└── pipeline_logger_inline.{py,ipynb}   # entire library inline — %run-able with zero install

scripts/
├── setup.sh                # editable install of test+dev extras
├── build.sh                # wheel build
├── test.sh                 # pytest wrapper
└── build_notebooks.sh      # jupytext --to ipynb notebooks/*.py

docs/superpowers/
├── specs/2026-05-21-pipeline-logger-design.md   # locked-decision design
└── plans/2026-05-21-pipeline-logger.md          # task-by-task implementation plan
```

See `AGENTS.md` for the honesty contract every contributor (human or
agent) follows, `CLAUDE.md` for the project operating doc, and
`docs/ARCHITECTURE.md` for the interface and data-flow detail.

## License

MIT. See `LICENSE`.
