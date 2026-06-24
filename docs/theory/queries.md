---
title: 1.4. Queries
description: Satisfy fast user queries using denormalized read model views.
---

Because the write path of an Event Sourced system only supports loading a single stream by its unique Aggregate ID, you cannot perform complex searches, aggregate operations, or joins directly against your event store.

To support high-performance, flexible querying, we use the read side of CQRS. This side is composed of **Projections** and **Read Models** (or Materialized Views).

---

## The Query Pipeline

```
[ Committed Events ]
        │
        ▼
[ Projection Runner ] (Polls/Listens sequentially)
        │
        ▼
[ Projection ] (Applies events to Read Model)
        │
        ▼
[ Read Model Database ] ◄─── [ User UI Queries ] (Fast SELECTs/Joins)
```

### 1. Read Models (Views)
A **Read Model** is a database table, collection, or cache optimized specifically for a particular UI screen or API endpoint.
* For example, if your application displays a list of the 10 richest accounts, your read model is a simple SQL table called `account_balances` with columns for `account_id`, `owner_name`, and `balance`, indexed on `balance DESC`.
* When a user queries this dashboard, the application issues a simple, fast `SELECT` query on this pre-calculated table, completely avoiding expensive on-the-fly calculations or joins.

### 2. Projections
A **Projection** is a background worker that listens to or polls committed events from the event store. 
* It receives event envelopes sequentially (e.g., `AccountOpened`, `MoneyDeposited`).
* For each event, it updates the corresponding read model table (e.g., executing an SQL `UPSERT` to set or add to the balance of that account).
* The projection does not participate in command validation. Its only job is to translate events into optimized read states.

---

## Eventual Consistency

Because projections execute *after* events are successfully written and committed to the event store, there is a tiny, often sub-millisecond delay between when a command succeeds and when the read model updates.

This state is called **Eventual Consistency**:

```
[ Command Committed ] ──(latency boundary)──► [ Projection Applied ] ──► [ UI View Updated ]
```

In practice, this sub-millisecond latency is invisible to users and represents a massive benefit:
* Your write operations are freed from maintaining complex, indexed query schemas during critical transactions.
* Your read queries are extremely fast because they are pre-computed, completely eliminating locking contention between reads and writes.
* If a projection fails or is paused, your write transactions are completely unaffected, ensuring high availability of the core system.
