#!/usr/bin/env bash
set -euo pipefail

log() {
  echo
  echo "==> $*"
}

command -v cargo >/dev/null 2>&1 || { echo "Error: cargo is required." >&2; exit 1; }

log "Installing Rust WASI target for example check"
rustup target add wasm32-wasip2

log "Running rustfmt"
cargo fmt --all -- --check

log "Compiling library crate"
cargo check --all-targets -p ddd_cqrs_es

log "Running unit and integration tests"
cargo test --all-targets --all-features -p ddd_cqrs_es

log "Running doc tests"
cargo test --doc --all-features -p ddd_cqrs_es

log "Running docs hardening checks"
bash scripts/verify-docs-rust.sh

log "Compiling counter-app example with sqlite"
make example-check

log "CI check suite complete"
