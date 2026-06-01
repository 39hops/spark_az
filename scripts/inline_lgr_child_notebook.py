"""Build ``notebooks/lgr_child.py`` by inlining the spark_az library source.

The child-logger notebook is ``%run`` at the top of any Synapse child
notebook to pull JSON logging, ``step``, and ``notebook_exit`` into the
child's namespace with zero install. It inlines the same library as
``lgr.ipynb`` (session + lgr + child) but ships no parameters cell of its
own, so ``%run`` never clobbers the caller's pipeline-injected parameters.

Run via ``scripts/build_notebooks.sh`` or directly:

    python scripts/inline_lgr_child_notebook.py
"""
from __future__ import annotations

import re
from pathlib import Path


def main() -> None:
    """Assemble ``notebooks/lgr_child.py`` from the library source files."""
    repo: Path = Path(__file__).resolve().parent.parent
    session_src: str = (repo / "src" / "spark_az" / "session.py").read_text()
    lgr_src: str = (repo / "src" / "spark_az" / "lgr.py").read_text()
    child_src: str = (repo / "src" / "spark_az" / "child.py").read_text()

    session_src = session_src.replace(
        "from __future__ import annotations\n", "", 1
    )
    lgr_src = lgr_src.replace("from __future__ import annotations\n", "", 1)
    lgr_src = lgr_src.replace("from .session import get_spark\n", "", 1)
    child_src = child_src.replace(
        "from __future__ import annotations\n", "", 1
    )
    child_src = re.sub(r"from \.lgr import \([^)]*\)\n", "", child_src, count=1)

    notebook: str = (
        _JUPYTEXT_HEADER
        + "# %% [markdown]\n"
        + "# ## Library\n"
        + "# Hand-synced from `src/spark_az/` via `scripts/inline_lgr_child_notebook.py`. Don't edit here.\n\n"
        + "# %%\n"
        + "from __future__ import annotations\n\n"
        + session_src
        + "\n"
        + lgr_src
        + "\n"
        + child_src
        + _SETUP_USAGE_CELLS
    )

    out: Path = repo / "notebooks" / "lgr_child.py"
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
# # lgr_child — drop-in child logger
# `%run` this at the very top of a Synapse child notebook to pull JSON
# logging, `step()`, and `notebook_exit()` into scope. There is no
# parameters cell here on purpose, so `%run` never overwrites the caller's
# pipeline-injected parameters. Setup and pipeline wiring live in the README.

"""


_SETUP_USAGE_CELLS: str = '''
# %% [markdown]
# ## Setup
# Turn on JSON-structured stdout logging for the child.

# %%
set_json_formatter()

# %% [markdown]
# ## Usage (reference — this runs in the child, not here)
# In the child notebook, AFTER its own parameters cell:
#
# ```python
# %run /Shared/lgr_child
#
# with step("extract") as s:
#     df = read_source()
#     s.metric("rows", df.count())
#
# notebook_exit(
#     "ok",
#     log_table=log_table,
#     pipeline_run_id=pipeline_run_id,
#     rows=df.count(),
#     watermark="2026-06-01",
# )
# ```
#
# Wrap risky work to self-log a failure row and still hand the pipeline a
# structured result:
#
# ```python
# try:
#     run_the_work()
#     notebook_exit("ok", log_table=log_table, pipeline_run_id=pipeline_run_id)
# except Exception as exc:
#     notebook_exit("failed", log_table=log_table,
#                   pipeline_run_id=pipeline_run_id, error=exc)
# ```
#
# The pipeline reads the structured result back with:
# `@json(activity('<child>').output.status.Output.result.exitValue).rows`
'''


if __name__ == "__main__":
    main()
