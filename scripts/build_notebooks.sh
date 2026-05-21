#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

jupytext --to ipynb notebooks/*.py
