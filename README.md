# Domain Driven Design + CQRS + ES

[![ci](https://github.com/codeitlikemiley/ddd-cqrs-es/actions/workflows/ci.yml/badge.svg)](https://github.com/codeitlikemiley/ddd-cqrs-es/actions/workflows/ci.yml)

## Documentation

- **Live documentation:** [https://ddd-cqrs-es.goldcoders.dev](https://ddd-cqrs-es.goldcoders.dev)

<p align="center">
  <img src="./assets/readme-banner.png" alt="ddd_cqrs_es banner" width="100%" />
</p>

ddd_cqrs_es is a Rust-native **Domain-Driven Design + CQRS + Event Sourcing** framework designed to stay out of your way.

**What makes it different:**
- **Domain-first API design** so your aggregates/repositories/commands stay independent from transport and framework choices.
- **Single core, multi runtime** with native and WASI deployments for high-scale web APIs, web apps, and service workflows.
- **Production focus** with explicit durability model: checkpoints, snapshots, idempotency, and event-upgrade paths documented and versioned.
- **Built for AI Agents with [Skills](./SKILLS.md)** for automated workflows that stay aligned with stable domain primitives.

**Architecture + deployment story (what this crate is for):**

This is not an app framework. It is an architectural crate that turns domain models and business rules into a composable core you can deploy as:

- High-throughput APIs for web backends
- Real-time apps and interactive workflows
- Multi-runtime services (native + WASI targets)

The active stack we use includes:
- [Leptos](https://github.com/leptos-rs/leptos) for full-stack Rust UI and server functions
- [leptos_wasi](https://github.com/leptos-rs/leptos_wasi) for Leptos server-side in WASI environments (we maintain this)
- [leptos-spin](https://github.com/spinframework/leptos-spin) templates for Spin runtime apps (we maintain this)
- [Spin](https://github.com/spinframework/spin) for Wasm-first serverless execution
- [spin-operator](https://github.com/spinframework/spin-operator) and [SpinKube](https://www.spinkube.dev) for production deployment on Kubernetes at scale

---

## Installation

Add the crate as a dependency in your `Cargo.toml`:

```toml
[dependencies]
ddd_cqrs_es = "0.2.0"
```

To enable durable database adapters:
* **SQLite Support:** Enable the `"sqlite"` feature.
* **PostgreSQL Support:** Enable the `"postgres"` feature.
* **MySQL Support:** Enable the `"mysql"` feature.
* **WASI MySQL Helper:** Enable `"wasi-mysql"` for raw TCP MySQL query execution from generic Wasmtime/WASI runtimes.
* **Spin MySQL Helper:** Enable `"spin-mysql"` for Spin SDK MySQL query execution.
* **LibSQL Support:** Enable the `"wasi-libsql"` feature (for Turso or generic LibSQL).
* **Redis Support:** Enable the `"redis"` feature plus `"wasi-redis"` or `"spin-redis"` for experimental async Redis persistence and notification helpers.

#### 🗄️ Supported Database Matrix:
* **SQLite:** Fully supported via local embedded file database using the `"sqlite"` feature.
* **PostgreSQL:** Fully supported via stable high-performance relational database using the `"postgres"` feature.
* **LibSQL / Turso:** Supported via distributed SQL edge helpers using the `"wasi-libsql"` feature.
* **Redis:** Supported via async event store, checkpoints, and pub/sub notifications using the `"redis"` feature.
* **MySQL:** Supported on native targets via stable event/checkpoint/idempotency stores using the `"mysql"` feature. WASI examples can also use lower-level MySQL query helpers through `"wasi-mysql"` on Wasmtime or `"spin-mysql"` on Spin.

#### ⚡ Realtime and Notification Support:
The root crate provides durable stores, checkpoint stores, idempotency stores, and notification primitives. It does not own an HTTP, SSE, or WebSocket server.

* **PostgreSQL / SQLite / MySQL:** Use durable events and checkpoints to drive application-owned polling, SSE, WebSocket, or worker pipelines.
* **Redis:** Provides experimental async persistence plus `RedisPubSubPublisher` for notification-only wake messages. Clients should wake on notifications and replay durable events/checkpoints as the source of truth.
* **Counter app:** Demonstrates SSE polling and Redis wake queues for Spin and Wasmtime. That example-level delivery wiring is separate from the stable root API.

The stable SQL adapters are available through the SQL feature family:
SQLite, PostgreSQL, and MySQL with checkpoints, snapshots, idempotency, and projection support.
WASI, Spin, and Redis features provide runtime-specific helpers and async notification primitives for edge deployments.
See full runtime/feature details in the docs.

---
## You probably want this before shipping

- This is an **architectural crate**, not an end-user application template.
- The root crate is intentionally transport-agnostic; HTTP/SSE/WebSocket layers are owned by your app.
- For production-grade idempotency in request handling, prefer the native SQL atomic path.
- Feature flags are explicit and modular; enable only what your target runtime needs.
- API details, feature matrix, and deep implementation notes live in the docs.

## More docs

- [Live docs homepage](https://ddd-cqrs-es.goldcoders.dev)
- [docs/index.md](./docs/index.md)
- [docs/README.md](./docs/README.md)

## License

This project is licensed under the same terms as the repository root license file:
- [LICENSE-MIT](./LICENSE-MIT)

## Contributing

Contributions are welcome. See:
- [CONTRIBUTING.md](./CONTRIBUTING.md)
