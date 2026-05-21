#!/usr/bin/env bash
# scripts/test.sh — run the project's test suite.
#
# Replace this placeholder with your real test command(s).
set -euo pipefail

cd "$(dirname "$0")/.."

echo "scripts/test.sh — placeholder."
echo "Replace this script with your real test command for the chosen stack:"
echo "  - C++:        ctest --test-dir build --output-on-failure"
echo "  - Rust:       cargo test"
echo "  - Python:     pytest"
echo "  - TypeScript: npm test"
echo "  - Go:         go test ./..."
echo
echo "Until configured, this exits 0 so CI doesn't break."
exit 0
