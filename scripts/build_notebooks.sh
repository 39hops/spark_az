#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

find notebooks -name '*.py' -print0 | xargs -0 jupytext --to ipynb
