# ddd_cqrs_es

A lightweight, infrastructure-light Domain-Driven Design (DDD), CQRS, and Event Sourcing framework for Rust.

Decouple your core business logic completely from databases, serialization, web frameworks, and asynchronous runtimes. Design pure domain aggregates, enforce transactional consistency boundaries, and build rich read models with minimal friction.

---

## Installation

Add the crate as a dependency in your `Cargo.toml`:

```toml
[dependencies]
ddd_cqrs_es = { path = "../ddd" }
```

To enable durable database adapters:
* **SQLite Support:** Enable the `"sqlite"` feature.
* **PostgreSQL Support:** Enable the `"postgres"` feature.

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
* [**5.1. Persisted Event Store**](./docs/production/persisted-store.md) — Connect SQLite and PostgreSQL adapters with Optimistic Concurrency Control (OCC).
* [**5.2. Queries with Persisted Views**](./docs/production/persisted-views.md) — Manage multi-process projections asynchronously using checkpoint sequence offsets.
* [**5.3. Including Metadata**](./docs/production/metadata.md) — Attach correlation, causation, actor, and tenant headers for enterprise audit tracing.
* [**5.4. Event Upcasters**](./docs/production/upcasters.md) — Handle live event schema evolution smoothly using `EventUpcaster` byte transforms.

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
