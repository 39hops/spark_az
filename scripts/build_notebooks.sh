#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

python scripts/inline_lgr_notebook.py
jupytext --sync notebooks/lgr.py tools/db_search.py
