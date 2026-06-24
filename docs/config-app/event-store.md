---
title: 4.1. An Event Store
description: Configure an in-memory event store for local testing.
---

To execute commands or rebuild state outside of unit test suites, we need to bind our aggregate to an event database. 

An **Event Store** is an append-only ledger optimized specifically for saving and loading sequential streams of event envelopes.

---

## The In-Memory Event Store

We provide a built-in, fully thread-safe in-memory adapter called `InMemoryEventStore`. It holds committed events inside an internal memory map protected by a reader-writer lock (`Arc<RwLock>`).

It is perfect for:
* Local development and rapid prototyping.
* Fast integration testing where you don't want the overhead of starting SQLite or PostgreSQL databases.
* Proving out core business workflows.

---

## Initializing the Store

To set up an in-memory event store for our `BankAccount` aggregate, import the module and initialize it:

```rust
use ddd_cqrs_es::{InMemoryEventStore, Repository};

fn main() {
    // 1. Initialize the thread-safe, local event store
    let store = InMemoryEventStore::<BankAccount>::new();
    
    // 2. Bind the store to the Repository coordinator
    let repo = Repository::new(store);
    
    println!("In-memory event store initialized successfully!");
}
```

---

## How It Operates

Under the hood, when a transaction occurs, the `Repository` requests the in-memory store to:
1. Load all committed event envelopes matching a specific Aggregate ID.
2. Verify if the stream's current in-memory revision matches what the client expects (Optimistic Concurrency Control).
3. If valid, append the new envelopes to the aggregate's vector of events inside the lock.

While the memory store does not persist events to disk between server restarts, it perfectly replicates the exact concurrency validations and sequence offsets of our real SQL adapters, making it an invaluable tool for test applications.
