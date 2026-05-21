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
# # `pipeline_starter`
#
# A polished, drop-in Synapse notebook that orchestrates a sequence of
# child notebooks with structured JSON logging, optional Application
# Insights fan-out, and a clean pipeline-parameter contract.
#
# Copy this notebook into your Synapse workspace, hook it up to a
# pipeline activity (or run it interactively), and you have:
#
# - One Delta row per child notebook in `log_table`, including failed
#   and skipped children.
# - JSON-structured stdout that Synapse captures into driver logs and
#   any handler you attach (Application Insights / Log Analytics).
# - An optional `step()` context manager for finer-grained logging
#   inside the orchestrator itself.
# - Fail-fast that re-raises **after** the log write so the upstream
#   Synapse pipeline sees the failure with a durable post-mortem.
#
# This notebook depends on the `spark_az` wheel being installed on the
# Spark pool. If you can't install workspace packages, use
# `pipeline_logger_inline.ipynb` instead — same surface, entire library
# inlined.

# %% [markdown]
# ## How to use
#
# **Pipeline activity** (the primary mode): wire a Synapse pipeline
# Notebook activity at this notebook. Pass `notebooks`, `log_table`,
# `pipeline_name`, `pipeline_run_id` (use `@pipeline().RunId`),
# `fail_fast`, `default_timeout_seconds`, and optionally
# `app_insights_connection_string`. A reference pipeline JSON is at
# `synapse/pipeline_template.json` in the repo.
#
# **Interactive** (for debugging): edit the parameter cell below, then
# **Run All**. The `step()` block in the middle is optional.
#
# **Library mode**: do not use this notebook in `%run` mode — that's
# what `pipeline_logger_inline.ipynb` is for. This notebook is meant to
# *be* the orchestrator.

# %% [markdown]
# ## Parameters
#
# Synapse replaces these defaults at runtime when the notebook is
# invoked from a pipeline activity. Required fields are validated by
# `read_pipeline_params` below — if they're empty or malformed, the
# notebook fails at the parameters cell, not deep inside the run.

# %% tags=["parameters"]
from __future__ import annotations

from typing import Any, Dict, List

notebooks: "List[Dict[str, Any]]" = []
log_table: str = "lab.__pipeline_runlog"
pipeline_name: str = ""
pipeline_run_id: str = ""
fail_fast: bool = True
default_timeout_seconds: int = 1800
app_insights_connection_string: str = ""

# %% [markdown]
# ## Setup
#
# JSON logging on by default. App Insights only if a connection string
# was passed — `enable_app_insights` raises on missing dependency with
# install instructions.

# %%
from typing import cast as _cast

from spark_az import (
    ChildSpec,
    enable_app_insights,
    log,
    read_pipeline_params,
    run_pipeline,
    set_json_formatter,
    step,
)

set_json_formatter()

if app_insights_connection_string:
    enable_app_insights(app_insights_connection_string)

# %% [markdown]
# ## Pipeline definition
#
# Validate the Synapse-passed args. `read_pipeline_params` raises a
# clear `ValueError` if `pipeline_name`, `log_table`, or any
# `notebooks[i]["path"]` is empty/malformed. The returned
# `PipelineParams` is a `TypedDict` you can pass downstream if you want
# to thread it through further helpers.

# %%
params = read_pipeline_params(
    pipeline_name=pipeline_name,
    log_table=log_table,
    notebooks=notebooks,
    fail_fast=fail_fast,
    default_timeout_seconds=default_timeout_seconds,
    pipeline_run_id=pipeline_run_id or None,
)

log.info(
    "pipeline_starter configured",
    extra={
        "pipeline_name": params["pipeline_name"],
        "pipeline_run_id": params.get("pipeline_run_id"),
        "child_count": len(params["notebooks"]),
    },
)

# %% [markdown]
# ## Optional: per-step work in the orchestrator
#
# Use `step(name, **attrs)` to wrap any in-orchestrator work you want
# timed and structured-logged. Step records carry the active
# `pipeline_run_id` automatically.
#
# Common uses: pre-flight checks, post-run aggregation, ad-hoc data
# moves that don't deserve their own child notebook. Remove this cell
# if you have nothing of the kind.

# %%
with step("preflight", pipeline=params["pipeline_name"]) as s:
    s.metric("expected_children", len(params["notebooks"]))

# %% [markdown]
# ## Run
#
# Calls `run_pipeline(...)` only when `notebooks` is non-empty. This
# keeps the notebook safe to render-and-publish with default empty
# parameters (no children → no Delta writes → no errors).
#
# `pipeline_results` is left in scope for downstream cells (or any
# diagnostics you want to add below).

# %%
pipeline_results: "List[Any]" = []
if params["notebooks"]:
    pipeline_results = run_pipeline(
        _cast("List[ChildSpec]", params["notebooks"]),
        log_table=params["log_table"],
        pipeline_name=params["pipeline_name"],
        fail_fast=params["fail_fast"],
        default_timeout_seconds=params["default_timeout_seconds"],
        pipeline_run_id=params.get("pipeline_run_id"),
    )

# %% [markdown]
# ## Inspect
#
# Query the log table to see every row written by this run:
#
# ```sql
# SELECT child_index, notebook_path, status,
#        duration_ms / 1000 AS seconds,
#        error_class, error_message
# FROM   lab.__pipeline_runlog
# WHERE  pipeline_run_id = '<paste the run id from the cell output above>'
# ORDER  BY child_index;
# ```
#
# Or across the whole pipeline:
#
# ```sql
# SELECT pipeline_run_id, COUNT(*) AS children,
#        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
#        MIN(started_at) AS run_start,
#        MAX(finished_at) AS run_end
# FROM   lab.__pipeline_runlog
# WHERE  pipeline_name = 'nightly_lab_refresh'
# GROUP  BY pipeline_run_id
# ORDER  BY run_start DESC
# LIMIT  20;
# ```

# %% [markdown]
# ## Maintenance
#
# This notebook is hand-maintained in sync with the `spark_az` library.
# When the library API changes, update the imports and call sites here
# and rebuild via `bash scripts/build_notebooks.sh`.
