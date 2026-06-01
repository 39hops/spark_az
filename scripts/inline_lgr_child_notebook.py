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
# Turn on JSON logging and arm automatic failure logging for this notebook.

# %%
set_json_formatter()
install_logging()

# %% [markdown]
# ## Usage (reference — this runs in the child, not here)
# Two lines per notebook. First, at the very top:
#
# ```python
# %run /Shared/lgr_child
# ```
#
# ...your existing cells, untouched... then as the LAST line:
#
# ```python
# log_done()
# ```
#
# `log_done()` writes one row to `_meta.__pipeline_runlog` with status,
# duration, and notebook name (plus `pipeline_run_id` if your parameters cell
# defines it). If any cell in between raises, a `failed` row with the error is
# logged automatically — you add nothing for that.
#
# Robust fallback, if the auto-capture hook ever misbehaves — wrap the body
# instead of calling log_done():
#
# ```python
# with log_run():
#     ...your work...
# ```
#
# Advanced — to also hand structured data back to the pipeline (read with
# `@json(activity('<child>').output.status.Output.result.exitValue).<field>`),
# end with notebook_exit() instead:
#
# ```python
# notebook_exit("ok", log_table=log_table, pipeline_run_id=pipeline_run_id,
#               target="lake.orders")
# ```
'''


if __name__ == "__main__":
    main()
