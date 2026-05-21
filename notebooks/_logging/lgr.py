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
log_table: str = "lab.__pipeline_runlog"
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
