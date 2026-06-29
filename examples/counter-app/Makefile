# Makefile for building app for different WASI runtimes
.PHONY: help spin wasmtime clean run fresh db-reset

# Load .env file and export variables if it exists first
# This provides the default settings which can be overridden via command-line db= parameter
ifneq (,$(wildcard .env))
  include .env
  export
endif

# Get db/realtime from command-line, fallback to .env, then defaults.

# Initialize db if not defined on command line.
db ?= $(if $(DATABASE_BACKEND),$(DATABASE_BACKEND),sqlite)
realtime ?= $(if $(REALTIME_BACKEND),$(REALTIME_BACKEND),off)

# Map database backend aliases to runtime backend names.
ifeq ($(db), sqlite)
  DATABASE_BACKEND = sqlite
endif
ifeq ($(db), postgres)
  DATABASE_BACKEND = postgres
endif
ifeq ($(db), psql)
  DATABASE_BACKEND = postgres
endif
ifeq ($(db), neon)
  DATABASE_BACKEND = neon
endif
ifeq ($(db), supabase)
  DATABASE_BACKEND = supabase
endif
ifeq ($(db), libsql)
  DATABASE_BACKEND = libsql
endif
ifeq ($(db), turso)
  DATABASE_BACKEND = turso
endif
ifeq ($(db), redis)
  DATABASE_BACKEND = redis
endif
ifeq ($(db), mysql)
  DATABASE_BACKEND = mysql
endif

ifeq ($(realtime), off)
  REALTIME_BACKEND = off
endif
ifeq ($(realtime), polling)
  REALTIME_BACKEND = polling
endif
ifeq ($(realtime), redis)
  REALTIME_BACKEND = redis
endif

# Map database backend to fine-grained Cargo features
CARGO_FEATURES := ssr
ifeq ($(DATABASE_BACKEND), sqlite)
  CARGO_FEATURES := ssr,sqlite
endif
ifeq ($(DATABASE_BACKEND), postgres)
  CARGO_FEATURES := ssr,postgres
endif
ifeq ($(DATABASE_BACKEND), neon)
  CARGO_FEATURES := ssr,neon
endif
ifeq ($(DATABASE_BACKEND), libsql)
  CARGO_FEATURES := ssr,libsql
endif
ifeq ($(DATABASE_BACKEND), turso)
  CARGO_FEATURES := ssr,libsql
endif
ifeq ($(DATABASE_BACKEND), supabase)
  CARGO_FEATURES := ssr,supabase
endif
ifeq ($(DATABASE_BACKEND), redis)
  CARGO_FEATURES := ssr,redis
endif
ifeq ($(DATABASE_BACKEND), mysql)
  CARGO_FEATURES := ssr,mysql
endif

SPIN_CARGO_FEATURES := $(CARGO_FEATURES)
WASMTIME_CARGO_FEATURES := $(CARGO_FEATURES)
ifneq (,$(filter redis,$(DATABASE_BACKEND) $(REALTIME_BACKEND)))
  SPIN_CARGO_FEATURES := $(CARGO_FEATURES),spin-redis
  WASMTIME_CARGO_FEATURES := $(CARGO_FEATURES),wasi-redis
endif
ifeq ($(DATABASE_BACKEND), mysql)
  SPIN_CARGO_FEATURES := $(SPIN_CARGO_FEATURES),spin-mysql
  WASMTIME_CARGO_FEATURES := $(WASMTIME_CARGO_FEATURES),wasi-mysql
endif

SPIN_MANIFEST := spin.toml
ifeq ($(REALTIME_BACKEND), redis)
  SPIN_MANIFEST := spin.redis.toml
endif

# Default target
help:
	@echo "Available targets:"
	@echo "  make spin     - Build and run with Spin"
	@echo "  make wasmtime - Build and run with Wasmtime"
	@echo "  make clean    - Clean build artifacts"
	@echo ""
	@echo "Database Selection Options:"
	@echo "  make wasmtime db=neon      - Run with Neon Postgres HTTP backend"
	@echo "  make wasmtime db=turso     - Run with Turso LibSQL HTTP backend"
	@echo "  make wasmtime db=mysql     - Run with raw TCP MySQL backend"
	@echo "  make wasmtime db=redis realtime=redis - Run with Redis event store and SSE notifications"
	@echo "  make spin db=postgres      - Run with Spin native Postgres backend"
	@echo "  make spin db=mysql realtime=polling - Run with Spin native MySQL and SSE polling"
	@echo "  make spin db=redis realtime=redis - Run with Spin Redis event store, SSE, and Redis trigger"
	@echo "  make db=neon fresh         - Reset active database tables without starting the app"

# Run target alias
run: wasmtime

# Reset active database tables and files
db-reset:
	@echo "Resetting database backend: $(DATABASE_BACKEND)..."
	@DATABASE_URL_VALUE=""; \
	DATABASE_AUTH_TOKEN_VALUE=""; \
	if [ "$(DATABASE_BACKEND)" = "mysql" ]; then \
		if [ -n "$$MYSQL_URL" ]; then \
			DATABASE_URL_VALUE="$$MYSQL_URL"; \
		elif [ -n "$$DATABASE_URL" ]; then \
			DATABASE_URL_VALUE="$$DATABASE_URL"; \
		else \
			echo "Error: Neither DATABASE_URL nor MYSQL_URL is set for MySQL reset."; \
			exit 1; \
		fi; \
	fi; \
	case "$(DATABASE_BACKEND)" in \
		postgres) DATABASE_URL_VALUE="$$POSTGRES_URL" ;; \
		neon) DATABASE_URL_VALUE="$$NEON_DB_URL" ;; \
		libsql|turso) DATABASE_URL_VALUE="$$TURSO_URL"; DATABASE_AUTH_TOKEN_VALUE="$$TURSO_AUTH_TOKEN" ;; \
		supabase) DATABASE_URL_VALUE="$$SUPABASE_URL"; DATABASE_AUTH_TOKEN_VALUE="$$SUPABASE_SECRET_KEY" ;; \
		redis) DATABASE_URL_VALUE="$$REDIS_URL" ;; \
	esac; \
	DATABASE_BACKEND=$(DATABASE_BACKEND) DATABASE_URL="$$DATABASE_URL_VALUE" DATABASE_AUTH_TOKEN="$$DATABASE_AUTH_TOKEN_VALUE" ./scripts/reset_db.sh

# fresh resets the database schema host-side and returns immediately without serving
fresh: db-reset

# Build and run with Spin
spin:
	@echo "Building for Spin runtime with DB: $(DATABASE_BACKEND), realtime: $(REALTIME_BACKEND)..."
	@mkdir -p .spin
	WASI_RUNTIME=spin DATABASE_BACKEND=$(DATABASE_BACKEND) REALTIME_BACKEND=$(REALTIME_BACKEND) cargo leptos build --release --lib-features hydrate --bin-features $(SPIN_CARGO_FEATURES)
	WASI_RUNTIME=spin DATABASE_BACKEND=$(DATABASE_BACKEND) REALTIME_BACKEND=$(REALTIME_BACKEND) LEPTOS_OUTPUT_NAME=counter_app cargo build --lib --target wasm32-wasip2 --release --no-default-features --features $(SPIN_CARGO_FEATURES)
	@if [ "$(REALTIME_BACKEND)" = "redis" ]; then \
		cargo build --manifest-path redis-trigger/Cargo.toml --target wasm32-wasip2 --release; \
	fi
	@echo "Built: target/wasm32-wasip2/release/counter_app.wasm (for Spin)"
	@echo "Starting Spin..."
	@DATABASE_URL_VALUE=""; \
	DATABASE_AUTH_TOKEN_VALUE=""; \
	REDIS_URL_VALUE="$${REDIS_URL:-redis://127.0.0.1:6379}"; \
	REDIS_CHANNEL_VALUE="$${REDIS_CHANNEL:-counter-events}"; \
	SPIN_VARIABLE_FLAGS=""; \
		case "$(DATABASE_BACKEND)" in \
			postgres) DATABASE_URL_VALUE="$$POSTGRES_URL" ;; \
			mysql) DATABASE_URL_VALUE="$${MYSQL_URL:-$$DATABASE_URL}" ;; \
			neon) DATABASE_URL_VALUE="$$NEON_DB_URL" ;; \
		libsql|turso) DATABASE_URL_VALUE="$$TURSO_URL"; DATABASE_AUTH_TOKEN_VALUE="$$TURSO_AUTH_TOKEN" ;; \
		supabase) DATABASE_URL_VALUE="$$SUPABASE_URL"; DATABASE_AUTH_TOKEN_VALUE="$$SUPABASE_SECRET_KEY" ;; \
		redis) DATABASE_URL_VALUE="$$REDIS_URL_VALUE" ;; \
	esac; \
	if [ "$(DATABASE_BACKEND)" = "mysql" ] && [ -z "$$DATABASE_URL_VALUE" ]; then \
		echo "Error: Neither DATABASE_URL nor MYSQL_URL is set for MySQL Spin."; \
		exit 1; \
	fi; \
	if [ "$(REALTIME_BACKEND)" = "redis" ]; then \
		SPIN_VARIABLE_FLAGS="--variable redis_url=$$REDIS_URL_VALUE --variable redis_channel=$$REDIS_CHANNEL_VALUE"; \
	fi; \
	spin up --from $(SPIN_MANIFEST) --env DATABASE_BACKEND=$(DATABASE_BACKEND) --env DATABASE_URL="$$DATABASE_URL_VALUE" --env DATABASE_AUTH_TOKEN="$$DATABASE_AUTH_TOKEN_VALUE" --env REALTIME_BACKEND=$(REALTIME_BACKEND) --env REDIS_URL="$$REDIS_URL_VALUE" --env REDIS_CHANNEL="$$REDIS_CHANNEL_VALUE" $$SPIN_VARIABLE_FLAGS

# Build and run with Wasmtime
wasmtime:
	@echo "Building for Wasmtime runtime with DB: $(DATABASE_BACKEND), realtime: $(REALTIME_BACKEND)..."
	WASI_RUNTIME=wasmtime DATABASE_BACKEND=$(DATABASE_BACKEND) REALTIME_BACKEND=$(REALTIME_BACKEND) cargo leptos build --release --lib-features hydrate --bin-features $(WASMTIME_CARGO_FEATURES)
	WASI_RUNTIME=wasmtime DATABASE_BACKEND=$(DATABASE_BACKEND) REALTIME_BACKEND=$(REALTIME_BACKEND) LEPTOS_OUTPUT_NAME=counter_app cargo build --lib --target wasm32-wasip2 --release --no-default-features --features $(WASMTIME_CARGO_FEATURES) --target-dir target/wasmtime
	@echo "Built: target/wasmtime/wasm32-wasip2/release/counter_app.wasm (for Wasmtime)"
	@echo "Starting Wasmtime serve..."
	@echo "Serving at http://127.0.0.1:3000"
	@mkdir -p ./data
	@DATABASE_URL_VALUE=""; \
	DATABASE_AUTH_TOKEN_VALUE=""; \
	REDIS_URL_VALUE="$${REDIS_URL:-redis://127.0.0.1:6379}"; \
	REDIS_CHANNEL_VALUE="$${REDIS_CHANNEL:-counter-events}"; \
	case "$(DATABASE_BACKEND)" in \
		postgres) DATABASE_URL_VALUE="$$POSTGRES_URL" ;; \
		mysql) DATABASE_URL_VALUE="$${MYSQL_URL:-$$DATABASE_URL}" ;; \
		neon) DATABASE_URL_VALUE="$$NEON_DB_URL" ;; \
		libsql|turso) DATABASE_URL_VALUE="$$TURSO_URL"; DATABASE_AUTH_TOKEN_VALUE="$$TURSO_AUTH_TOKEN" ;; \
		supabase) DATABASE_URL_VALUE="$$SUPABASE_URL"; DATABASE_AUTH_TOKEN_VALUE="$$SUPABASE_SECRET_KEY" ;; \
		redis) DATABASE_URL_VALUE="$$REDIS_URL_VALUE" ;; \
	esac; \
	if [ "$(DATABASE_BACKEND)" = "mysql" ] && [ -z "$$DATABASE_URL_VALUE" ]; then \
		echo "Error: Neither DATABASE_URL nor MYSQL_URL is set for MySQL Wasmtime."; \
		exit 1; \
	fi; \
	wasmtime serve \
		-W component-model-async=y \
		-S p3=y \
		-S cli=y \
		-S http=y \
		-S tcp=y \
		-S inherit-network=y \
		-S allow-ip-name-lookup=y \
		--dir=./target/site/pkg::/ \
		--dir=./data::/data \
		--env=LEPTOS_OUTPUT_NAME=counter_app \
		--env=LEPTOS_SITE_ROOT=/ \
		--env=LEPTOS_SITE_PKG_DIR=pkg \
		--env=DATABASE_BACKEND=$(DATABASE_BACKEND) \
		--env=DATABASE_URL="$$DATABASE_URL_VALUE" \
		--env=DATABASE_AUTH_TOKEN="$$DATABASE_AUTH_TOKEN_VALUE" \
		--env=REALTIME_BACKEND=$(REALTIME_BACKEND) \
		--env=REDIS_URL="$$REDIS_URL_VALUE" \
		--env=REDIS_CHANNEL="$$REDIS_CHANNEL_VALUE" \
		--addr 127.0.0.1:3000 \
		target/wasmtime/wasm32-wasip2/release/counter_app.wasm

# Clean target
clean:
	cargo clean
	rm -rf target/site target/wasmtime ./data
	rm -rf .spin
