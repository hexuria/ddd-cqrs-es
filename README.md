# ddd_cqrs_es

A lightweight, infrastructure-light Domain-Driven Design (DDD), CQRS, and Event Sourcing framework for Rust.

Decouple your core business logic completely from databases, serialization, web frameworks, and asynchronous runtimes. Design pure domain aggregates, enforce transactional consistency boundaries, and build rich read models with minimal friction.

---

## Installation

Add the crate as a dependency in your `Cargo.toml`:

```toml
[dependencies]
# From GitHub repository:
ddd_cqrs_es = { git = "https://github.com/codeitlikemiley/ddd-cqrs-es" }

# Or from crates.io (once published):
# ddd_cqrs_es = "0.2.0"
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

The stable built-in SQL adapters are `SqliteEventStore`, `PostgresEventStore`, `MySqlEventStore` (native only), `SqliteCheckpointStore`, `PostgresCheckpointStore`, `MySqlCheckpointStore` (native only), `SqliteIdempotencyStore`, `PostgresIdempotencyStore`, `MySqlIdempotencyStore` (native only), `SqliteSnapshotStore`, `PostgresSnapshotStore`, and `MySqlSnapshotStore`. SQL event stores also implement atomic idempotent append through `execute_idempotent_atomic`. WASI, Spin, Neon, LibSQL, Supabase, and MySQL runtime feature flags expose lower-level query helpers for examples and runtime experiments. Redis exposes an experimental async `RedisEventStore`, `RedisCheckpointStore`, and `RedisPubSubPublisher`; pub/sub is notification-only and durable event replay remains the source of truth. The counter example includes a separate Spin Redis Trigger sidecar for subscriber smoke testing; it is not part of the root library API. The WASI counter-app supports `db=mysql` on Wasmtime through raw TCP (`wasi-mysql`) and on Spin through `spin_sdk::mysql` (`spin-mysql`).

#### API Notes:
* Aggregates no longer expose `id()` through the trait; repositories use the external stream ID supplied by the caller.
* `EventType` is a small newtype; use `event_type.as_str()` or `event_type.into_string()` at string boundaries.
* `SqlSchemaConfig` validates table names through fallible `with_*_table(...)` builders.
* `ProcessManagerRunner` and `AsyncProcessManagerRunner` can dispatch process-manager commands through the command bus traits.
* `execute_idempotent(...)` is portable but not crash-atomic across separate stores. Use `execute_idempotent_atomic(...)` with the native SQL stores for production request idempotency.

---

## Detailed Conceptual Guides

Our documentation is structured around explaining core theoretical concepts and patterns before transitioning to code. The layout is divided into 5 learning modules:

### 1. The Patterns (Theory)
* [**1.1. Domain-Driven Design**](./docs/theory/ddd.md) — Ubiquitous Language, Entities, Value Objects, and Aggregate Root transactional boundaries.
* [**1.2. CQRS**](./docs/theory/cqrs.md) — Separating read vs write pipelines.
* [**1.3. Making Changes to State**](./docs/theory/state-changes.md) — Command handling validation vs deterministic event application.
* [**1.4. Queries**](./docs/theory/queries.md) — Pre-calculating read models for high-performance views.
* [**1.5. Event Sourcing**](./docs/theory/event-sourcing.md) — Historical fact logs, state reconstitution, and end-to-end command life cycle.

### 2. Getting Started (Tutorial)
* [**2.1. Add Commands**](./docs/tutorial/commands.md) — Define user intent enums.
* [**2.2. Add Domain Events**](./docs/tutorial/events.md) — Implement past-tense fact enums and the `DomainEvent` trait.
* [**2.3. Add an Error and Service**](./docs/tutorial/errors.md) — Handle validation failures with domain-specific errors.
* [**2.4. Add an Aggregate**](./docs/tutorial/aggregate.md) — Build the state struct and implement the `Aggregate` trait.

### 3. Domain Tests
* [**3.1. Adding More Complex Logic**](./docs/testing/complex-logic.md) — Unit test your aggregates deterministically in microseconds using the `AggregateFixture` BDD framework (`Given` -> `When` -> `Then`).

### 4. Configuring a (test) Application
* [**4.1. An Event Store**](./docs/config-app/event-store.md) — Initialize a thread-safe `InMemoryEventStore`.
* [**4.2. A Simple Query**](./docs/config-app/simple-query.md) — Build an in-memory `Projection` read view.
* [**4.3. Putting Everything Together**](./docs/config-app/assembly.md) — Tie the write-side repository and read-side projection runner into an executable entry point.

### 5. Building an Application
* [**5.0. Production Guarantees**](./docs/production/guarantees.md) — Distinguish portable APIs from transaction-aware SQL APIs, durable snapshots, atomic projections, and notification-only realtime.
* [**5.1. Persisted Event Store**](./docs/production/persisted-store.md) — Connect SQLite, PostgreSQL, and MySQL adapters with Optimistic Concurrency Control (OCC).
* [**5.2. Queries with Persisted Views**](./docs/production/persisted-views.md) — Manage multi-process projections asynchronously using checkpoint sequence offsets.
* [**5.3. Database Query Patterns**](./docs/production/db-query-patterns.md) — Match event-store, checkpoint, snapshot, and read-model queries to the right indexes and consistency boundaries.
* [**5.4. Redis Event Store and Realtime**](./docs/production/redis.md) — Use the experimental async Redis event store, checkpoint store, and notification-only pub/sub publisher.
* [**5.5. Including Metadata**](./docs/production/metadata.md) — Attach correlation, causation, actor, and tenant headers for enterprise audit tracing.
* [**5.6. Event Upcasters**](./docs/production/upcasters.md) — Handle live event schema evolution smoothly using `EventUpcaster` byte transforms.

For contributor-facing documentation, workflows, and available agent skills, see:
* [SKILLS.md](./SKILLS.md)
* [CONTRIBUTING.md](./CONTRIBUTING.md)

---

## Local Documentation Server

We utilize **Mintlify** to render a beautiful, modern documentation website. To preview the site locally with hot-reloading:

1. Navigate to the documentation directory:
   ```bash
   cd docs
   ```
2. Start the local preview:
   ```bash
   mint dev
   ```
3. To validate the configuration and check for broken links:
   ```bash
   mint validate
   mint broken-links
   ```
