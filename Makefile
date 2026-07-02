.DEFAULT_GOAL := help
SHELL := /usr/bin/env bash

.PHONY: help version publish example example-check ci clean preflight check-tools check-example-runtime check-wasm-target

# Convenience aliases used by examples/counter-app passthrough.
EXAMPLE_RUNTIME := $(word 2,$(MAKECMDGOALS))
VALID_RUNTIMES := spin wasmtime run
EXAMPLE_TARGET := counter-app
EXAMPLE_CHECK_FEATURES := ssr,sqlite

# `make version` can take `make version X.Y.Z` or `VERSION=X.Y.Z make version`.
VERSION_ARG := $(if $(VERSION),$(VERSION),$(word 2,$(MAKECMDGOALS)))

# If the user invokes `make version <x.y.z>`, `make publish dry-run`,
# or `make example spin`, treat extra positional arguments as no-op targets.
VERSION_GOALS := $(if $(filter version,$(firstword $(MAKECMDGOALS))),$(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS)))
$(foreach goal,$(VERSION_GOALS),$(eval $(goal):;@:))

PUBLISH_MODE := $(if $(filter dry-run --dry-run,$(MAKECMDGOALS)),dry-run,publish)

# Keep no-op placeholders for positional arguments like version, publish modes,
# and example runtime aliases.
.PHONY: spin wasmtime run --dry-run dry-run

help:
	@echo "Usage:"
	@echo "  make version [<version>]            bump crate version (auto-increments patch if omitted)"
	@echo "  make publish [dry-run]              run crates.io publish flow (or: make publish -- --dry-run)"
	@echo "  make example <spin|wasmtime|run>    run counter-app example with db/realtime args"
	@echo "  make preflight                      check required local tools before running CI/release/example commands"
	@echo ""
	@echo "Examples:"
	@echo "  make version"
	@echo "  make version 0.2.1"
	@echo "  make publish"
	@echo "  make publish -- --dry-run"
	@echo "  make publish dry-run"
	@echo "  make example spin db=neon realtime=redis"
	@echo "  make example-check                  # Compile counter-app counterexample with sqlite feature"
	@echo "  make ci                             # Run full CI quality suite locally"

check-tools:
	@echo "Checking required tools..."
	@missing=0; \
	for bin in cargo rustup perl git; do \
		if ! command -v "$$bin" >/dev/null 2>&1; then \
			echo "Error: required command '$$bin' not found in PATH." >&2; \
			missing=1; \
		fi; \
	done; \
	if [ "$$missing" -ne 0 ]; then \
		exit 1; \
	fi

check-wasm-target:
	@echo "Checking wasm32-wasip2 target..."
	@rustup target list --installed | grep -qx 'wasm32-wasip2' || { \
		echo "Error: Rust target 'wasm32-wasip2' is required. Run: rustup target add wasm32-wasip2" >&2; \
		exit 1; \
	}

check-example-runtime:
	@if [ -z "$(EXAMPLE_RUNTIME)" ]; then \
		echo "Error: missing example runtime. Use: make example <spin|wasmtime|run>." >&2; \
		exit 2; \
	fi; \
	if [ "$(EXAMPLE_RUNTIME)" != "spin" ] && [ "$(EXAMPLE_RUNTIME)" != "wasmtime" ] && [ "$(EXAMPLE_RUNTIME)" != "run" ]; then \
		echo "Error: invalid runtime '$(EXAMPLE_RUNTIME)'. Valid options: $(VALID_RUNTIMES)." >&2; \
		exit 2; \
	fi

preflight: check-tools

version:
	@bash scripts/version.sh "$(VERSION_ARG)"

publish:
	@$(MAKE) preflight
	@if [ "$(PUBLISH_MODE)" = "publish" ] && [ -z "$${CARGO_REGISTRY_TOKEN:-}" ]; then \
		echo "Error: publish mode requires CARGO_REGISTRY_TOKEN environment variable." >&2; \
		exit 1; \
	fi; \
	echo "Starting release flow in '$(PUBLISH_MODE)' mode..."; \
	bash scripts/release-crates-io.sh "$(PUBLISH_MODE)"

example:
	@$(MAKE) preflight
	@$(MAKE) check-example-runtime EXAMPLE_RUNTIME="$(EXAMPLE_RUNTIME)"
	@$(MAKE) check-wasm-target
	@$(MAKE) -C examples/$(EXAMPLE_TARGET) $(EXAMPLE_RUNTIME) db="$(db)" realtime="$(realtime)"

example-check:
	@$(MAKE) check-tools check-wasm-target
	@cargo check --manifest-path examples/$(EXAMPLE_TARGET)/Cargo.toml --target wasm32-wasip2 --no-default-features --features $(EXAMPLE_CHECK_FEATURES)

ci:
	@$(MAKE) preflight
	bash scripts/ci-check.sh

spin wasmtime run --dry-run dry-run:
	@:

clean:
	@$(MAKE) -C examples/counter-app clean

.DEFAULT:
	@if [ "$(strip $(filter version publish example,$(MAKECMDGOALS)))" != "" ]; then \
		exit 0; \
	fi
	@echo "No rule to make target '$@'." >&2
	@exit 2
