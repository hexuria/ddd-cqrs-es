# Contributing Guide

## Environment and command wrapper

Repository AGENTS guidance requires workspace commands to be prefixed with `rtk`.
Use `rtk` for Makefile and command-line execution in this repo context.

## Core development workflow

- Start from `examples/counter-app` for runtime-focused changes.
- Use the Makefile as the backend command source of truth:
  - `rtk make help`
  - `rtk make help-db`
  - `rtk make help-realtime`
  - `rtk make help-matrix`
  - `rtk make help-env`

## Backend contract defaults

- Supported public backends are: `sqlite`, `postgres`, `neon`, `supabase`, `turso`, `mysql`, `redis`.
- `db=turso` is the supported public value for Turso/LibSQL.
- `libsql` is retained as an internal compatibility path in runtime/reset internals and should not be documented as a public `make db=<...>` option.
- Realtime modes: `off`, `polling`, `redis`.

## Reset semantics (important)

- `make db=<backend> fresh` is reset-only. It drops/recreates backend state and exits.
- `fresh` must never launch the app server.

## Required edits for backend/realtime changes

When updating backend or realtime behavior, keep docs in sync in all of these locations:

- `examples/counter-app/Makefile`
- `examples/counter-app/README.md`
- `examples/counter-app/.env.example`
- `docs/tutorial/leptos-ssr.md`
- `docs/production/redis.md`
- `.agents/skills/leptos-wasi-cqrs/SKILL.md`

## Documentation quality checks

Run this before docs-focused PRs:

- `rtk node -v` (environment sanity check, if needed)
- `rtk jq -r '.navigation.groups[].pages[]' docs/docs.json | sort` and compare against `docs/**/*.md`
- `rtk scripts/verify-docs.sh`
