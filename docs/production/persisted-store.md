---
title: 5.1. Persisted Event Store
description: Setup a persistent SQLite or PostgreSQL event store with Optimistic Concurrency Control.
---

When transitioning from local testing to a production system, we need to persist our event streams to a durable, high-performance database.

A durable event store acts as our application's **immutable ledger**. It is highly optimized to support two primary operations:
1. **Load Stream:** Fetch all committed event envelopes for a specific Aggregate ID, ordered sequentially by their stream version.
2. **Append Stream:** Append a block of new events to the stream in a single transaction.

---

## Optimistic Concurrency Control (OCC)

When multiple application server instances handle requests for the same aggregate instance simultaneously, they can cause race conditions. If Server A and Server B both load an account at revision `5`, execute validations, and attempt to append events, they could corrupt the aggregate state if both succeed.

To prevent this, our framework implements **Optimistic Concurrency Control**:
* When loading an aggregate, the repository tracks its current version (e.g., `ExpectedRevision::Exact(5)`).
* When appending events, the database adapter verifies that the current version of the stream in the database is still exactly `5`.
* If another request edited the stream first and advanced it to `6`, the transaction is rolled back and the append fails with a concurrency violation error (`RepositoryError::Concurrency`).

### OCC State Transition Flow

```mermaid
flowchart TD
    A[Client initiates Command Execution] --> B[Repository loads Aggregate state @ Revision N]
    B --> C[Aggregate validates Command & generates New Event]
    C --> D[Repository attempts to append event to stream]
    D --> E{EventStore checks current stream revision in DB}
    E -- Current revision is EXACTLY N --> F[1. Append event payload to ledger]
    F --> H[2. Increment stream revision to N + 1]
    H --> I[3. Return success / Transaction committed]
    E -- Current revision is NOT N --> G[1. Raise Concurrency Collision Error]
    G --> K[2. Abort transaction and rollback SQL]
    K --> L[3. Return RepositoryError::Concurrency]
    L --> M[4. Client loads updated history and retries]
```

---

## Standard Relational Database Schema

Our SQLite and PostgreSQL adapters share a unified table schema design. It enforces strict sequential versions per aggregate stream while tracking a global sequence sequence for asynchronous projection engines.

```sql
CREATE TABLE events (
    -- Unique monotonically increasing sequence number across all aggregate types.
    -- Used by asynchronous Projection Runners to poll for new events.
    sequence BIGSERIAL PRIMARY KEY,
    
    -- Universally unique identifier for this specific event instance.
    event_id TEXT NOT NULL UNIQUE,
    
    -- Unique identifier of the aggregate instance (e.g., account-123).
    aggregate_id TEXT NOT NULL,
    
    -- The type of aggregate (e.g., bank_account).
    aggregate_type TEXT NOT NULL,
    
    -- The sequential version number of this event inside its specific stream.
    revision BIGINT NOT NULL,
    
    -- Name of the event type (e.g., money_deposited).
    event_type TEXT NOT NULL,
    
    -- Schema version of this event payload.
    event_version INT NOT NULL,
    
    -- Actual domain event payload serialized as JSON text or JSONB.
    payload JSONB NOT NULL,
    
    -- Extensible envelope metadata (correlation ID, actor, tenancy) as JSONB.
    metadata JSONB NOT NULL,
    
    -- Unix epoch timestamp when this event was committed.
    recorded_at_ms BIGINT NOT NULL,
    
    -- Primary transaction guard: ensures no two events can occupy the same revision in a stream.
    UNIQUE (aggregate_type, aggregate_id, revision)
);
```

---

## Configuring Database Adapters

Our framework provides built-in adapters for both SQLite and PostgreSQL.

### 1. SQLite Store (Embedded File)
Perfect for edge applications, local databases, or desktop apps. Enable with the `"sqlite"` feature.

```rust
use ddd_cqrs_es::{SqliteEventStore, Repository};

fn setup_sqlite() -> Result<Repository<BankAccount, SqliteEventStore<BankAccount>>, Box<dyn std::error::Error>> {
    // 1. Establish a standard rusqlite connection
    let connection = rusqlite::Connection::open("local_events.db")?;
    
    // 2. Wrap connection in our SqliteEventStore adapter
    let store = SqliteEventStore::<BankAccount>::new(connection);
    
    // 3. Initialize the database schema if it doesn't exist
    store.initialize_schema()?;
    
    let repo = Repository::new(store);
    Ok(repo)
}
```

### 2. PostgreSQL Store (Production Microservice)
Designed for high-concurrency production microservices. Enable with the `"postgres"` feature.

```rust
use ddd_cqrs_es::{PostgresEventStore, Repository};

fn setup_postgres() -> Result<Repository<BankAccount, PostgresEventStore<BankAccount>>, Box<dyn std::error::Error>> {
    // 1. Connect to PostgreSQL using standard connection string
    let dsn = "host=localhost port=5432 user=postgres dbname=app_events sslmode=disable";
    let store = PostgresEventStore::<BankAccount>::connect(dsn)?;
    
    // 2. Initialize the physical database table structure
    store.initialize_schema()?;
    
    let repo = Repository::new(store);
    Ok(repo)
}
```
