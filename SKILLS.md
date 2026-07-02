# Skills Index

This repository ships one repository-local skill used by agent workflows.

## Available Skills

- `leptos-wasi-cqrs`
  - Location: `.agents/skills/leptos-wasi-cqrs/SKILL.md`
  - Scope: Build, integrate, and debug full-stack CQRS/Event Sourcing applications using `ddd_cqrs_es` with Leptos WASI/Spin/Wasmtime.
  - Use this for:
    - backend/realtime matrix changes in `examples/counter-app`
    - store/runtime wiring updates
    - migration and reset behavior for backends
    - Spin trigger and SSE/realtime behavior in examples

## When to consult

- Consult before editing counter-app backend dispatch, command contracts, or documentation that spans backends/realtime.
- Consult when publishing or validating contributor-facing workflows that depend on `make help`, `make db`, or `make realtime` behavior.

## Related source-of-truth files

- `examples/counter-app/Makefile`
- `examples/counter-app/README.md`
- `docs/docs.json`
- `docs/tutorial/leptos-ssr.md`
- `docs/production/redis.md`
