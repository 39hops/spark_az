#!/usr/bin/env bash
# scripts/build.sh — build the project.
#
# Replace this placeholder with your real build command(s).
set -euo pipefail

cd "$(dirname "$0")/.."

echo "scripts/build.sh — placeholder."
echo "Replace this script with your real build command for the chosen stack:"
echo "  - C++:        cmake -S . -B build && cmake --build build"
echo "  - Rust:       cargo build --release"
echo "  - Python:     uv build (or skip — interpreted)"
echo "  - TypeScript: npm run build"
echo "  - Go:         go build ./..."
echo
echo "Until configured, this exits 0 so CI doesn't break."
exit 0
