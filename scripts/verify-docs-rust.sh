#!/usr/bin/env bash
set -euo pipefail

if ! command -v jq >/dev/null 2>&1; then
  echo "Error: jq is required (install with your package manager)." >&2
  exit 1
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "Error: cargo is required to run Rustdoc checks." >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

bash "$SCRIPT_DIR/verify-docs.sh"

echo "Running Rust crate docs hardening checks for ddd_cqrs_es..."
cargo check -p ddd_cqrs_es
RUSTFLAGS="-D missing-docs" cargo doc --no-deps --lib -p ddd_cqrs_es
RUSTFLAGS="-D missing-docs" cargo doc --no-deps --lib -p ddd_cqrs_es --all-features

echo "docs/rust verification complete."
