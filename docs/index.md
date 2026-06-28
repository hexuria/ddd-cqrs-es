---
title: Getting Started with ddd_cqrs_es
description: Welcome to the lightweight, infrastructure-light Domain-Driven Design (DDD), CQRS, and Event Sourcing framework for Rust.
---

Welcome to **ddd_cqrs_es**! This library is a lightweight, high-performance Rust framework designed to help you construct highly reliable, testable, and maintainable software systems using the combined power of **Domain-Driven Design (DDD)**, **Command Query Responsibility Segregation (CQRS)**, and **Event Sourcing (ES)**.

The distinguishing design philosophy of this framework is that it is completely **infrastructure-light**. Your core domain logic—the rules that govern how your business operates—is kept entirely free of dependencies on databases, serialization formats, web frameworks, or asynchronous runtimes.

---

## 🚀 Installation

Add the crate as a dependency in your `Cargo.toml`:

```toml
[dependencies]
ddd_cqrs_es = { path = "../ddd" }
```

### Feature Flags

Our framework is highly modular. You can enable specific adapters and engines depending on your production requirements:

The stable built-in durable adapters are SQLite and PostgreSQL. Runtime-specific
WASI, Spin, Neon, LibSQL, and Supabase flags expose lower-level query helpers
for examples and experiments until they implement the library's reusable event
store, checkpoint store, and idempotency store contracts. Redis exposes an
experimental async event store, checkpoint store, and pub/sub notification
surface.

| Feature | Description | Third-Party Dependencies |
| :--- | :--- | :--- |
| **`default`** | Standard local, thread-safe in-memory event store and memory projection runners. | None |
| **`sqlite`** | Stable SQLite event store, checkpoint store, and idempotency store. | `rusqlite` |
| **`postgres`** | Stable PostgreSQL event store, checkpoint store, and idempotency store. | `postgres` |
| **`redis`** | Experimental async Redis event store, checkpoint store, pub/sub publisher, and command executor trait. | None |
| **`wasi-redis`** | Experimental raw RESP Redis client for generic Wasmtime/WASI runtimes. | `redis` |
| **`spin-redis`** | Experimental Spin SDK Redis client. | `spin-sdk` |
| **`wasi-http`** | Experimental outbound HTTP helper foundation for WASI runtimes. | `wasip3`, `http`, `http-body-util`, `bytes` |
| **`wasi-neon`** | Experimental Neon HTTP SQL query helper. | `wasi-http` |
| **`wasi-libsql`** | Experimental LibSQL/Turso Hrana HTTP query helper. | `wasi-http` |
| **`wasi-postgres-tcp`** | Experimental raw PostgreSQL TCP query helper for WASI-style runtimes. | `md5`, `base64`, `pbkdf2`, `hmac`, `sha2`, `rustls` |
| **`spin-sqlite`** | Experimental Spin SQLite host-call query helper. | `spin-sdk` |
| **`spin-postgres`** | Experimental Spin PostgreSQL host-call query helper. | `spin-sdk` |
| **`wasi-supabase-rpc`** | Experimental Supabase RPC query helper. | `wasi-http` |

---

## ⚡ Quick 10-Second Example

Here is how simple it is to initialize our write path, tie it to an in-memory event ledger, and execute a transactional command:

```rust
use ddd_cqrs_es::{InMemoryEventStore, Repository, Metadata};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize a thread-safe, local, in-memory event ledger
    let store = InMemoryEventStore::<BankAccount>::new();
    
    // 2. Bind the ledger to the repository coordinator
    let repo = Repository::new(store);
    let account_id = "account_abc123".to_owned();

    // 3. Execute a transactional command with audit tracking metadata!
    repo.execute(
        &account_id,
        BankAccountCommand::DepositMoney { amount: 100 },
        Metadata::new().with_actor_id("user_alice"),
    )?;

    // 4. Rebuild aggregate state instantly by replaying its historical facts
    let loaded = repo.load(&account_id)?;
    assert_eq!(loaded.state.balance(), 100);
    
    println!("Transaction committed! Balance is: ${}", loaded.state.balance());
    Ok(())
}
```

---

## 🗺️ How to Navigate This Documentation

We structured our guides as a structured, chronological path designed to take you from a complete beginner to building full-scale, distributed production applications:

### Module 1: [The Patterns](./theory/ddd.md) (Theory)
* **What you'll learn:** The architectural foundations. Read about [Domain-Driven Design](./theory/ddd.md) (aggregate boundaries, Ubiquitous Language), [CQRS](./theory/cqrs.md) (separating write vs read pipelines), [State Changes](./theory/state-changes.md) (command validation vs event application), [Queries](./theory/queries.md) (read models), and [Event Sourcing mechanics](./theory/event-sourcing.md).

### Module 2: [Domain Modeling (Tutorial)](./tutorial/commands.md)
* **What you'll learn:** Build a fully validated Bank Account domain step-by-step. Implement [Commands](./tutorial/commands.md), [Events](./tutorial/events.md) (implementing `DomainEvent`), [Errors](./tutorial/errors.md) (handling invariants), and the core [Aggregate Root](./tutorial/aggregate.md) struct.

### Module 3: [Domain Tests](./testing/complex-logic.md)
* **What you'll learn:** Write bulletproof business validations in microseconds. Learn why Event Sourcing is a unit-testing superpower and write elegant Given-When-Then tests using the [Aggregate Test Fixture](./testing/complex-logic.md) API.

### Module 4: [Configuring an Application](./config-app/event-store.md)
* **What you'll learn:** Assemble your domain parts. Wire up the local [InMemoryEventStore](./config-app/event-store.md), write a custom [Query Projection](./config-app/simple-query.md), and [Assemble them together](./config-app/assembly.md) into a working execution loop.

### Module 5: [Building an Application](./production/persisted-store.md) (Production)
* **What you'll learn:** Move to production. Deploy durable [SQLite and PostgreSQL stores](./production/persisted-store.md), configure asynchronous [Projections with Checkpoint tracking](./production/persisted-views.md), use experimental [Redis persistence and realtime notifications](./production/redis.md), attach [Metadata trace headers](./production/metadata.md), write custom [Event Upcasters](./production/upcasters.md) for schema evolution, and [Integrate with Web Frameworks (Axum)](./production/axum-integration.md).

### Module 6: [Leptos WASM SSR + Spin SQLite CQRS](./tutorial/leptos-ssr.md) (Full-Stack Showcase)
* **What you'll learn:** Put everything together. Architect a full-stack, real-time-like reactive UI inside a WebAssembly server-side rendered (SSR) Leptos application deployed to Fermyon Spin. Learn how to write custom WASM SQLite store adapters, checkpointed projections, and reactive forms with optimistic updates.

### Module 7: [Runtimes & Cloud Deployment (Spin vs. Wasmtime)](./wasmtime-vs-spin-comparison.md)
* **What you'll learn:** Production runtimes and connection engineering. Understand the performance differences between Wasmtime CLI and Fermyon Spin connection pooling. Read our best practice architectural recommendations for database proximity, and learn how to deploy serverless WASM applications to Fermyon Cloud and SpinKube on AWS EKS, GCP GKE, and Azure AKS.
