.DEFAULT_GOAL := help
SHELL := /bin/bash

.PHONY: help version publish example clean ci
.PHONY: spin wasmtime run --dry-run dry-run

# Convenience aliases used by examples/counter-app passthrough.
EXAMPLE_RUNTIME := $(word 2,$(MAKECMDGOALS))

# `make version` can optionally take `make version X.Y.Z`.
VERSION_ARG := $(word 2,$(MAKECMDGOALS))

# `make publish` can optionally take `--dry-run` / `dry-run`.
PUBLISH_MODE_ARG := $(word 2,$(MAKECMDGOALS))
EXAMPLE_CHECK_TARGET := counter-app
EXAMPLE_CHECK_FEATURES := ssr,sqlite

help:
	@echo "Usage:"
	@echo "  make version [<version>]            bump crate version (auto-increments patch if omitted)"
	@echo "  make publish [dry-run]              run crates.io publish flow (or: make publish -- --dry-run)"
	@echo "  make example <spin|wasmtime|run>    run counter-app example with db/realtime args"
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

version:
	@bash scripts/version.sh $(VERSION_ARG)

publish:
	@if [ "$(PUBLISH_MODE_ARG)" = "--dry-run" ] || [ "$(PUBLISH_MODE_ARG)" = "dry-run" ]; then \
		target="dry-run"; \
	else \
		target="publish"; \
	fi; \
	bash scripts/release-crates-io.sh "$$target"

example:
	@if [ -z "$(EXAMPLE_RUNTIME)" ]; then \
		echo "Error: missing example runtime. Use: make example <spin|wasmtime|run>."; \
		exit 2; \
	fi; \
	$(MAKE) -C examples/counter-app $(EXAMPLE_RUNTIME) db="$(db)" realtime="$(realtime)"

example-check:
	cargo check --manifest-path examples/$(EXAMPLE_CHECK_TARGET)/Cargo.toml --target wasm32-wasip2 --no-default-features --features $(EXAMPLE_CHECK_FEATURES)

ci:
	bash scripts/ci-check.sh

# No-op placeholders so `make version X`, `make publish --dry-run`, and
# `make example spin` don't fail on positional arguments.
spin wasmtime run --dry-run dry-run:
	@:

.DEFAULT:
	@if [ "$(firstword $(MAKECMDGOALS))" = "version" ]; then \
		exit 0; \
	fi
	@if [ "$(firstword $(MAKECMDGOALS))" = "publish" ]; then \
		exit 0; \
	fi
	@if [ "$(firstword $(MAKECMDGOALS))" = "example" ]; then \
		exit 0; \
	fi
	@echo "No rule to make target '$@'." >&2
	@exit 2

clean:
	@$(MAKE) -C examples/counter-app clean
