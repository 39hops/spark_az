"""Build ``notebooks/lgr.py`` by inlining the spark_az library source.

The all-in-one Synapse notebook is the project's primary deliverable: a
single file the user drops into their workspace and runs. This script
keeps it in sync with ``src/spark_az/`` without manual copy-paste.

Run via ``scripts/build_notebooks.sh`` or directly:

    python scripts/inline_lgr_notebook.py
"""
from __future__ import annotations

from pathlib import Path


def main() -> None:
    """Assemble ``notebooks/lgr.py`` from the library source files."""
    repo: Path = Path(__file__).resolve().parent.parent
    session_src: str = (repo / "src" / "spark_az" / "session.py").read_text()
    lgr_src: str = (repo / "src" / "spark_az" / "lgr.py").read_text()

    session_src = session_src.replace(
        "from __future__ import annotations\n", "", 1
    )
    lgr_src = lgr_src.replace(
        "from __future__ import annotations\n", "", 1
    )
    lgr_src = lgr_src.replace(
        "from .session import get_spark\n", "", 1
    )

    notebook: str = (
        _JUPYTEXT_HEADER
        + _PARAMETERS_CELLS
        + "# %% [markdown]\n"
        + "# ## Library\n"
        + "# Hand-synced from `src/spark_az/` via `scripts/inline_lgr_notebook.py`. Don't edit here.\n\n"
        + "# %%\n"
        + session_src
        + "\n"
        + lgr_src
        + _SETUP_RUN_INSPECT_CELLS
    )

    out: Path = repo / "notebooks" / "lgr.py"
    out.write_text(notebook)


_JUPYTEXT_HEADER: str = """# ---
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
# Drop-in Synapse pipeline orchestrator. Edit Parameters, then Run All.
# Setup details and sample queries live in the repo README.

"""


_PARAMETERS_CELLS: str = """# %% [markdown]
# ## Parameters

# %% tags=["parameters"]
from __future__ import annotations

from typing import Any, Dict, List

notebooks: \"List[Dict[str, Any]]\" = []
log_table: str = \"_meta.__pipeline_runlog\"
pipeline_name: str = \"\"
pipeline_run_id: str = \"\"
fail_fast: bool = True
default_timeout_seconds: int = 1800
app_insights_connection_string: str = \"\"

"""


_SETUP_RUN_INSPECT_CELLS: str = """

# %% [markdown]
# ## Setup
# JSON logging on. Pass an App Insights connection string to fan out.

# %%
set_json_formatter()
if app_insights_connection_string:
    enable_app_insights(app_insights_connection_string)

# %% [markdown]
# ## Run
# No-op when `notebooks` is empty so `%run` from another notebook is safe.

# %%
from typing import cast as _cast

pipeline_results: \"List[ChildResult]\" = []
if notebooks:
    pipeline_results = run_pipeline(
        _cast(\"List[ChildSpec]\", notebooks),
        log_table=log_table,
        pipeline_name=pipeline_name or \"interactive\",
        fail_fast=fail_fast,
        default_timeout_seconds=default_timeout_seconds,
        pipeline_run_id=pipeline_run_id or None,
    )

# %% [markdown]
# ## Inspect
# Query `_meta.__pipeline_runlog` after a run. Sample SQL in the README.
"""


if __name__ == "__main__":
    main()
