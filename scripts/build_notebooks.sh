#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

python scripts/inline_lgr_notebook.py
find notebooks -name '*.py' -print0 | xargs -0 jupytext --to ipynb
