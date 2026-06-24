---
title: 1.2. CQRS
description: Command-Query Responsibility Segregation separates read and write models.
---

Most traditional web applications use a single, unified database model to both write (mutate) and read (query) data. A user edits their profile, which issues an `UPDATE` statement to a table. A second later, the application issues a complex `SELECT` statement with several inner joins on that same table to display a user dashboard.

As applications scale, this single-model approach leads to severe performance bottlenecks, indexing conflicts, and schema design compromises. Write paths require strict normalization for transactional integrity, while read paths require highly denormalized views for fast, flexible querying.

**Command-Query Responsibility Segregation (CQRS)** solves this by dividing these responsibilities into completely separate pipelines:

```
                  ┌───────────────────────────────┐
                  │            Client             │
                  └───────┬───────────────▲───────┘
                          │               │
                 Commands │ (Write Path)  │ Queries (Read Path)
                          ▼               │
               ┌─────────────────────┐    │    ┌─────────────────────┐
               │    Write Model      │    │    │     Read Model      │
               │ (Aggregate/Store)   │    │    │    (Projections)    │
               └──────────┬──────────┘    │    └──────────▲──────────┘
                          │               │               │
                    Events│               └───────────────┤Replays
                          ▼                               │
               ┌─────────────────────┐                    │
               │     Event Store     ├────────────────────┘
               │    (Fact Ledger)    │
               └─────────────────────┘
```

---

## The Two Paths

### 1. The Write Path (Command Model)
The write path is optimized strictly for **enforcing business invariants** and appending new facts.
* It does not support complex search operations, arbitrary filters, or table joins.
* It only supports loading a single stream by its unique Aggregate ID, reconstituting the aggregate state in memory, executing the command validations, and appending new events.
* This ensures that write transactions are extremely lightweight, predictable, and scale-free.

### 2. The Read Path (Query Model)
The read path is optimized strictly for **high-speed, highly-customized query views** tailored to your user interface.
* Read models (often called **materialized views** or **projections**) consume committed events from the event store and update separate read-optimized databases.
* If a UI dashboard requires a list of accounts with their current balances, the read model pre-aggregates this data into a fast SQLite or PostgreSQL table.
* Queries are satisfied by reading directly from these views, avoiding any expensive on-the-fly calculations.

---

## Benefits of CQRS

By separating these models, we gain substantial advantages:

* **Independent Scaling:** Write workloads (which are transactional and require strict locking/ordering per stream) can scale independently of read workloads (which can be heavily cached or distributed across read-replicas).
* **Optimized Data Schemas:** Read models can be structured in any shape—SQL tables, document stores, key-value caches, or search indexes—completely decoupled from how the write path stores events.
* **Simplified Domain Logic:** The write-side domain logic is free of concerns about how data will be searched or displayed. It only focuses on validating business rules.
