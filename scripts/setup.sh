#!/usr/bin/env bash
# scripts/setup.sh — one-shot project setup.
#
# Replace this placeholder with your real setup steps (install
# dependencies, configure the build, create local files, etc.) once
# the project picks a stack.
set -euo pipefail

cd "$(dirname "$0")/.."

echo "scripts/setup.sh — placeholder."
echo "Replace this script with your real setup commands."
echo
echo "Common setup steps to include:"
echo "  - Install language-level dependencies (cargo, npm, pip, uv, ...)"
echo "  - Initialize submodules if any"
echo "  - Create local config files (.env, ...)"
echo "  - Verify required external tools are installed"
echo
echo "Until configured, this exits 0 so CI doesn't break."
exit 0
