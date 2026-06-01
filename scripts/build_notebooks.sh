#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

python scripts/inline_lgr_notebook.py
python scripts/inline_lgr_child_notebook.py
jupytext --sync notebooks/lgr.py notebooks/lgr_child.py tools/db_search.py
