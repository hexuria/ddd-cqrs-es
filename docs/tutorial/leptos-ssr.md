# Leptos WASM SSR + Spin SQLite CQRS: A Full-Stack Masterclass

In this advanced tutorial, we will design, build, and deploy a complete, production-ready, full-stack reactive application using **Leptos** (WebAssembly Server-Side Rendering) and **Fermyon Spin** (WASI SQLite) powered by our extensible `ddd_cqrs_es` framework.

By the end of this guide, you will understand how to model a domain using Event Sourcing, implement highly optimized read-model projections, overcome the compilation limits of WebAssembly inside sandboxed microservices, and deliver a zero-latency, reactive UI using optimistic updates and server actions.

---

## 🗺️ Architectural Blueprint

Before we dive into the code, let's look at the flow of a modern, full-stack CQRS and Event Sourced system. Here is how commands flow from the interactive Leptos UI on the client, get validated and processed on the server, persist in our event store, update projections sequentially, and hydrate the reactive client-side interface:

```mermaid
sequenceDiagram
    autonumber
    actor User as 🌐 User (Browser)
    participant Client as 🖥️ Leptos Client (WASM)
    participant Server as ⚙️ Leptos Server (WASI)
    participant Domain as 🧠 Aggregate (Counter)
    database EventDB as 🗄️ Event Store (SQLite)
    database ReadDB as 📊 Read Model (SQLite)

    %% 1. Command Dispatch
    User->>Client: Clicks "+1" button
    Note over Client: Optimistic Update:<br/>Increment display instantly (e.g. from 5 to 6)
    Client->>Server: HTTP POST /api/increment_count (Server Function)

    %% 2. Rehydration & Handling
    Server->>EventDB: Fetch historical events for Counter ID
    EventDB-->>Server: [Incremented { amount: 1 }]
    Server->>Domain: Replay events to rebuild current state (Value = 5)
    Server->>Domain: Handle Command: Increment { amount: 1 }
    Note over Domain: Check Invariants:<br/>1. Is amount > 0?<br/>2. Will it overflow i32?
    Domain-->>Server: Ok([Incremented { amount: 1 }])

    %% 3. Persistence & Projection
    Server->>EventDB: Append new event with revision tracking
    EventDB-->>Server: Commited sequence #43
    Server->>Server: Trigger projection runner (On-the-fly Checkpoint)
    Server->>ReadDB: Update flat read model: UPDATE counter_read_model SET value = 6
    Server->>ReadDB: Save Checkpoint sequence #43

    %% 4. Response & Hydration
    Server-->>Client: HTTP 200 OK (Sync complete)
    Client->>Server: Fetch current count (or read model)
    Server-->>Client: Returns count = 6
    Note over Client: Finalizes UI state.<br/>Sync indicator glows green!
```

---

## 1. Conceptualization & Domain Modeling

Let's model the **Counter** domain. A counter seems simple, but in an enterprise environment, every state change requires complete auditability, precise validation rules, and high scalability.

### Mapping the Domain Requirements
To model this domain, we map the requirements to core DDD and Event Sourcing patterns:

*   **Aggregate Root (`Counter`)**: The primary consistency boundary. It maintains the current counter value, tracks the stream revision (for optimistic concurrency), and ensures that all state mutations are applied sequentially.
*   **Value Object (`CounterId`)**: A type-safe newtype wrapper around a `String` representing the unique ID of our counter stream.
*   **Commands (`CounterCommand`)**: Intentions to change state. These represent the *write* operations:
    *   `Increment { amount: i32 }`: Requests to add a positive amount.
    *   `Decrement { amount: i32 }`: Requests to subtract a positive amount.
    *   `Reset`: Requests to reset the counter to zero.
*   **Events (`CounterEvent`)**: Historical, immutable facts that have occurred. These represent our *historical log*:
    *   `Incremented { amount: i32 }`
    *   `Decremented { amount: i32 }`
    *   `ResetPerformed { value: i32 }`

### Why This Matters
By separating commands (intentions) from events (facts), we separate validation from execution. 

> [!IMPORTANT]
> **Command Handling is Validative**: Commands can be rejected if they violate invariants.
> **Event Application is Infallible**: Events represent the past. Once an event is committed, it cannot be rejected or fail to apply; it must mutate the aggregate state without further checks.

---

## 2. Implementing the Pure Domain

Let's examine the full, pure domain implementation located inside `examples/counter-app/src/domain.rs`. Notice that this file has absolutely **no infrastructure dependencies** (no databases, no network frameworks). It is pure, highly testable Rust logic that implements our framework's `Aggregate` trait.

```rust
use std::fmt;
use serde::{Deserialize, Serialize};
use ddd_cqrs_es::{Aggregate, DomainEvent};

/// Type-safe newtype wrapper for the Counter Aggregate ID.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CounterId(pub String);

impl fmt::Display for CounterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for CounterId {
    fn from(id: String) -> Self {
        Self(id)
    }
}

impl From<&str> for CounterId {
    fn from(id: &str) -> Self {
        Self(id.to_string())
    }
}

/// Commands accepted by the Counter Aggregate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CounterCommand {
    Increment { amount: i32 },
    Decrement { amount: i32 },
    Reset,
}

/// Domain events emitted by the Counter Aggregate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CounterEvent {
    Incremented { amount: i32 },
    Decremented { amount: i32 },
    ResetPerformed { value: i32 },
}

impl DomainEvent for CounterEvent {
    fn event_type(&self) -> &'static str {
        match self {
            CounterEvent::Incremented { .. } => "incremented",
            CounterEvent::Decremented { .. } => "decremented",
            CounterEvent::ResetPerformed { .. } => "reset_performed",
        }
    }
}

/// The Counter Aggregate state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Counter {
    pub id: CounterId,
    pub value: i32,
    pub revision: u64,
}

impl Aggregate for Counter {
    type Id = CounterId;
    type Command = CounterCommand;
    type Event = CounterEvent;
    type Error = String;

    fn aggregate_type() -> &'static str {
        "counter"
    }

    fn id(&self) -> Option<&Self::Id> {
        if self.id.0.is_empty() {
            None
        } else {
            Some(&self.id)
        }
    }

    fn revision(&self) -> u64 {
        self.revision
    }

    fn new() -> Self {
        Self {
            id: CounterId(String::new()),
            value: 0,
            revision: 0,
        }
    }

    /// Infallibly mutates state based on a committed event fact.
    fn apply(&mut self, event: &Self::Event) {
        match event {
            CounterEvent::Incremented { amount } => {
                self.value = self.value.saturating_add(*amount);
            }
            CounterEvent::Decremented { amount } => {
                self.value = self.value.saturating_sub(*amount);
            }
            CounterEvent::ResetPerformed { value } => {
                self.value = *value;
            }
        }
        self.revision += 1;
    }

    /// Validates a command against current state. Returns a vector of events if successful.
    fn handle(&self, command: Self::Command) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            CounterCommand::Increment { amount } => {
                if amount <= 0 {
                    return Err("amount to increment must be positive".to_string());
                }
                if self.value.checked_add(amount).is_none() {
                    return Err("increment would overflow integer boundary".to_string());
                }
                Ok(vec![CounterEvent::Incremented { amount }])
            }
            CounterCommand::Decrement { amount } => {
                if_dbg!(amount <= 0);
                if amount <= 0 {
                    return Err("amount to decrement must be positive".to_string());
                }
                if self.value.checked_sub(amount).is_none() {
                    return Err("decrement would underflow integer boundary".to_string());
                }
                Ok(vec![CounterEvent::Decremented { amount }])
            }
            CounterCommand::Reset => {
                Ok(vec![CounterEvent::ResetPerformed { value: 0 }])
            }
        }
    }

    /// Rebuilds aggregate state by replaying envelopes sequentially.
    fn replay(events: &[ddd_cqrs_es::EventEnvelope<Self::Event, Self::Id>]) -> ddd_cqrs_es::LoadedAggregate<Self> {
        let mut state = Self::new();
        let mut revision = ddd_cqrs_es::INITIAL_REVISION;

        if let Some(first) = events.first() {
            state.id = first.aggregate_id.clone();
        }

        for envelope in events {
            state.apply(&envelope.payload);
            revision = envelope.revision;
        }

        ddd_cqrs_es::LoadedAggregate { state, revision }
    }
}
```

> [!TIP]
> Notice the usage of `checked_add` and `checked_sub` in `handle`. This ensures the aggregate defends its invariants *before* accepting changes, while `apply` uses `saturating_add` as a secondary safety mechanism when applying historical facts.

---

## 3. Custom SQLite & Checkpoint Storage on WASM/Spin

### The WASM Sandboxing & Native C Compilation Problem
When compiling standard Rust applications to the WebAssembly target `wasm32-wasip2`, you will quickly run into compile-time or runtime walls if you pull in traditional database engines like `rusqlite` or `diesel`. Why?

Standard native driver crates:
1.  **Rely on Native C libraries**: They expect to link dynamically to a local system C-library (`libsqlite3.so` or `libpq.dylib`), which is impossible inside a sandboxed WebAssembly container.
2.  **Require Raw POSIX Syscalls**: Traditional native drivers spawn threads and perform raw, blocking socket connections or open custom files descriptors—operations strictly blocked by the standard WASI sandbox.

### How We Solve This: Extensible Traits & Spin Host-Calls
To circumvent these limits, our framework provides clean, pluggable `EventStore` and `CheckpointStore` traits. 

In Fermyon Spin, the host runtime manages a native, high-performance SQLite database engine. WebAssembly components communicate with this host engine using highly optimized **WASM host-calls** defined via WIT. Spin's host SDK exposes this capability via `spin_sdk::sqlite::Connection`.

By creating a custom adapter inside `src/store.rs`, we can bridge Spin's host-supplied SQLite connection to our framework's traits. Let's see how this is implemented:

```rust
//! Custom Spin-compliant SQLite Store Adapters.
//! This bridges the Spin SDK host database interface with the ddd_cqrs_es traits.

use std::marker::PhantomData;
use serde::{Serialize, de::DeserializeOwned};
use spin_sdk::sqlite::{Connection, Value};

use ddd_cqrs_es::{
    Aggregate, EventStore, EventStream, EventEnvelope, EventId, NewEvent,
    ExpectedRevision, CheckpointStore, ConcurrencyError, EventStoreError, Metadata
};

// =========================================================================
// 1. Spin SQLite Event Store Adapter
// =========================================================================

pub struct SpinSqliteEventStore<A>
where
    A: Aggregate,
{
    connection_name: String,
    table_name: String,
    _marker: PhantomData<fn() -> A>,
}

impl<A> Clone for SpinSqliteEventStore<A>
where
    A: Aggregate,
{
    fn clone(&self) -> Self {
        Self {
            connection_name: self.connection_name.clone(),
            table_name: self.table_name.clone(),
            _marker: PhantomData,
        }
    }
}

impl<A> SpinSqliteEventStore<A>
where
    A: Aggregate,
{
    pub fn new(connection_name: impl Into<String>) -> Self {
        Self {
            connection_name: connection_name.into(),
            table_name: "events".to_string(),
            _marker: PhantomData,
        }
    }

    fn get_connection(&self) -> Connection {
        Connection::open(&self.connection_name)
            .expect("Failed to open Spin Host SQLite database connection")
    }

    /// Prepares database tables if they do not exist yet.
    pub fn initialize_schema(&self) {
        let conn = self.get_connection();
        let query = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table} (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                aggregate_id TEXT NOT NULL,
                aggregate_type TEXT NOT NULL,
                revision INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                metadata TEXT NOT NULL,
                recorded_at_ms INTEGER NOT NULL,
                UNIQUE (aggregate_type, aggregate_id, revision)
            );
            CREATE INDEX IF NOT EXISTS {table}_stream_idx
                ON {table} (aggregate_type, aggregate_id, revision);
            "#,
            table = self.table_name
        );
        conn.execute(&query, &[]).expect("Failed to initialize aggregate events schema");
    }
}

impl<A> EventStore<A> for SpinSqliteEventStore<A>
where
    A: Aggregate + 'static,
    A::Event: Serialize + DeserializeOwned,
    A::Id: Serialize + DeserializeOwned,
{
    type Error = String;

    fn load(&self, aggregate_id: &A::Id) -> Result<EventStream<A>, Self::Error> {
        let conn = self.get_connection();
        let id_str = serde_json::to_string(aggregate_id)
            .map_err(|e| format!("Failed to serialize aggregate ID: {}", e))?;

        let query = format!(
            "SELECT event_id, aggregate_id, aggregate_type, revision, sequence, event_type, \
             payload, metadata, recorded_at_ms FROM {table} \
             WHERE aggregate_type = ? AND aggregate_id = ? ORDER BY revision ASC",
            table = self.table_name
        );

        let row_set = conn.execute(
            &query,
            &[Value::Text(A::aggregate_type().to_string()), Value::Text(id_str)]
        ).map_err(|e| format!("Database read error: {:?}", e))?;

        let mut stream = Vec::new();
        for row in row_set.rows() {
            let event_id: String = row.get("event_id").ok_or("Missing event_id")?;
            let revision: i64 = row.get("revision").ok_or("Missing revision")?;
            let seq: i64 = row.get("sequence").ok_or("Missing sequence")?;
            let payload_str: String = row.get("payload").ok_or("Missing payload")?;
            let metadata_str: String = row.get("metadata").ok_or("Missing metadata")?;
            let recorded_at_ms: i64 = row.get("recorded_at_ms").ok_or("Missing recorded_at_ms")?;

            let payload: A::Event = serde_json::from_str(&payload_str)
                .map_err(|e| format!("Failed to deserialize event: {}", e))?;
            let metadata: Metadata = serde_json::from_str(&metadata_str)
                .map_err(|e| format!("Failed to deserialize metadata: {}", e))?;

            stream.push(EventEnvelope {
                event_id: EventId::from(event_id),
                aggregate_id: aggregate_id.clone(),
                aggregate_type: A::aggregate_type(),
                revision: revision as u64,
                sequence: Some(seq as u64),
                event_type: row.get("event_type").unwrap_or_default(),
                event_version: 1,
                payload,
                metadata,
                recorded_at: std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(recorded_at_ms as u64),
            });
        }

        Ok(stream)
    }

    fn append(
        &self,
        aggregate_id: &A::Id,
        expected_revision: ExpectedRevision,
        events: Vec<NewEvent<A::Event>>,
    ) -> Result<EventStream<A>, Self::Error> {
        let conn = self.get_connection();
        let id_str = serde_json::to_string(aggregate_id)
            .map_err(|e| format!("Failed to serialize aggregate ID: {}", e))?;

        // 1. Concurrency Check: Load current revision
        let count_query = format!(
            "SELECT COALESCE(MAX(revision), 0) as current FROM {table} WHERE aggregate_type = ? AND aggregate_id = ?",
            table = self.table_name
        );
        let res = conn.execute(
            &count_query,
            &[Value::Text(A::aggregate_type().to_string()), Value::Text(id_str.clone())]
        ).map_err(|e| format!("Query error: {:?}", e))?;
        
        let current_revision = if let Some(row) = res.rows().next() {
            let val: i64 = row.get("current").unwrap_or(0);
            val as u64
        } else {
            0
        };

        // Validate expectations (Optimistic Concurrency Control)
        match expected_revision {
            ExpectedRevision::NoStream if current_revision > 0 => {
                return Err(format!("Concurrency Error: Expected NoStream, found revision {}", current_revision));
            }
            ExpectedRevision::Exact(expected) if current_revision != expected => {
                return Err(format!("Concurrency Error: Expected revision {}, found {}", expected, current_revision));
            }
            ExpectedRevision::Any => {}
            _ => {}
        }

        let mut committed = Vec::new();
        let insert_query = format!(
            "INSERT INTO {table} (event_id, aggregate_id, aggregate_type, revision, event_type, payload, metadata, recorded_at_ms) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            table = self.table_name
        );

        // 2. Persist events sequentially
        for (idx, new_event) in events.into_iter().enumerate() {
            let next_rev = current_revision + 1 + idx as u64;
            let event_id = EventId::new();
            let payload_str = serde_json::to_string(&new_event.payload).unwrap();
            let metadata_str = serde_json::to_string(&new_event.metadata).unwrap();
            let timestamp_ms = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap().as_millis() as i64;

            conn.execute(
                &insert_query,
                &[
                    Value::Text(event_id.to_string()),
                    Value::Text(id_str.clone()),
                    Value::Text(A::aggregate_type().to_string()),
                    Value::Integer(next_rev as i64),
                    Value::Text(new_event.payload.event_type().to_string()),
                    Value::Text(payload_str),
                    Value::Text(metadata_str),
                    Value::Integer(timestamp_ms),
                ]
            ).map_err(|e| format!("Commit append failed: {:?}", e))?;

            // Fetch sequence of appended row to correctly return complete EventEnvelope
            let seq_query = "SELECT last_insert_rowid() as seq";
            let seq_res = conn.execute(seq_query, &[]).unwrap();
            let sequence_val = seq_res.rows().next().unwrap().get::<i64>("seq").unwrap() as u64;

            committed.push(EventEnvelope {
                event_id,
                aggregate_id: aggregate_id.clone(),
                aggregate_type: A::aggregate_type(),
                revision: next_rev,
                sequence: Some(sequence_val),
                event_type: new_event.payload.event_type().to_string(),
                event_version: 1,
                payload: new_event.payload,
                metadata: new_event.metadata,
                recorded_at: std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(timestamp_ms as u64),
            });
        }

        Ok(committed)
    }

    fn load_global_after(&self, sequence: Option<u64>) -> Result<EventStream<A>, Self::Error> {
        let conn = self.get_connection();
        let seq_val = sequence.unwrap_or(0) as i64;

        let query = format!(
            "SELECT event_id, aggregate_id, aggregate_type, revision, sequence, event_type, \
             payload, metadata, recorded_at_ms FROM {table} \
             WHERE aggregate_type = ? AND sequence > ? ORDER BY sequence ASC",
            table = self.table_name
        );

        let row_set = conn.execute(
            &query,
            &[Value::Text(A::aggregate_type().to_string()), Value::Integer(seq_val)]
        ).map_err(|e| format!("Database load global error: {:?}", e))?;

        let mut stream = Vec::new();
        for row in row_set.rows() {
            let event_id: String = row.get("event_id").unwrap();
            let aggregate_id_str: String = row.get("aggregate_id").unwrap();
            let revision: i64 = row.get("revision").unwrap();
            let seq: i64 = row.get("sequence").unwrap();
            let payload_str: String = row.get("payload").unwrap();
            let metadata_str: String = row.get("metadata").unwrap();
            let recorded_at_ms: i64 = row.get("recorded_at_ms").unwrap();

            let aggregate_id: A::Id = serde_json::from_str(&aggregate_id_str).unwrap();
            let payload: A::Event = serde_json::from_str(&payload_str).unwrap();
            let metadata: Metadata = serde_json::from_str(&metadata_str).unwrap();

            stream.push(EventEnvelope {
                event_id: EventId::from(event_id),
                aggregate_id,
                aggregate_type: A::aggregate_type(),
                revision: revision as u64,
                sequence: Some(seq as u64),
                event_type: row.get("event_type").unwrap_or_default(),
                event_version: 1,
                payload,
                metadata,
                recorded_at: std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(recorded_at_ms as u64),
            });
        }

        Ok(stream)
    }
}

// =========================================================================
// 2. Spin SQLite Checkpoint Store Adapter
// =========================================================================

pub struct SpinSqliteCheckpointStore {
    connection_name: String,
    table_name: String,
}

impl SpinSqliteCheckpointStore {
    pub fn new(connection_name: impl Into<String>) -> Self {
        Self {
            connection_name: connection_name.into(),
            table_name: "projection_checkpoints".to_string(),
        }
    }

    fn get_connection(&self) -> Connection {
        Connection::open(&self.connection_name).expect("Failed to open SQLite")
    }

    pub fn initialize_schema(&self) {
        let conn = self.get_connection();
        let query = format!(
            "CREATE TABLE IF NOT EXISTS {table} (projection_name TEXT PRIMARY KEY, sequence INTEGER NOT NULL)",
            table = self.table_name
        );
        conn.execute(&query, &[]).expect("Failed to initialize checkpoint schema");
    }
}

impl CheckpointStore for SpinSqliteCheckpointStore {
    type Error = String;

    fn load_checkpoint(&self, projection_name: &str) -> Result<Option<u64>, Self::Error> {
        let conn = self.get_connection();
        let query = format!(
            "SELECT sequence FROM {table} WHERE projection_name = ?",
            table = self.table_name
        );
        let res = conn.execute(&query, &[Value::Text(projection_name.to_string())])
            .map_err(|e| format!("{:?}", e))?;

        if let Some(row) = res.rows().next() {
            let seq: i64 = row.get("sequence").unwrap_or(0);
            Ok(Some(seq as u64))
        } else {
            Ok(None)
        }
    }

    fn save_checkpoint(&self, projection_name: &str, sequence: u64) -> Result<(), Self::Error> {
        let conn = self.get_connection();
        let query = format!(
            "INSERT INTO {table} (projection_name, sequence) VALUES (?, ?) \
             ON CONFLICT(projection_name) DO UPDATE SET sequence = excluded.sequence",
            table = self.table_name
        );
        conn.execute(&query, &[Value::Text(projection_name.to_string()), Value::Integer(sequence as i64)])
            .map_err(|e| format!("Save checkpoint failed: {:?}", e))?;
        Ok(())
    }
}
```

---

## 4. Asynchronous CQRS Projections & Checkpointing

A primary tenet of the CQRS pattern is the complete separation of your **Write Model** (optimized for committing atomic business facts) and **Read Model** (optimized for blazingly fast querying). 

Our Aggregate is the write model; it doesn't support list queries or range filter aggregates efficiently. To solve this, we stream committed events into a flat, denormalized read model table using a **Projection**.

### The Read Model Database Schema
For the counter UI, we need a flat table containing each counter's latest computed value:

```sql
CREATE TABLE IF NOT EXISTS counter_read_model (
    counter_id TEXT PRIMARY KEY,
    current_value INTEGER NOT NULL
);
```

### Implementing `CounterProjection`
The `CounterProjection` consumes the domain event envelopes and maintains this read model table:

```rust
use ddd_cqrs_es::{Projection, EventEnvelope};
use spin_sdk::sqlite::{Connection, Value};
use crate::domain::{CounterEvent, CounterId};

pub struct CounterProjection {
    connection_name: String,
}

impl CounterProjection {
    pub fn new(connection_name: impl Into<String>) -> Self {
        Self {
            connection_name: connection_name.into(),
        }
    }

    fn get_connection(&self) -> Connection {
        Connection::open(&self.connection_name).unwrap()
    }

    pub fn initialize_schema(&self) {
        let conn = self.get_connection();
        conn.execute(
            "CREATE TABLE IF NOT EXISTS counter_read_model (counter_id TEXT PRIMARY KEY, current_value INTEGER NOT NULL)",
            &[]
        ).expect("Failed to initialize counter read model table");
    }
}

impl Projection<CounterEvent, CounterId> for CounterProjection {
    type Error = String;

    fn name(&self) -> &'static str {
        "counter_projection"
    }

    fn apply(&mut self, event: &EventEnvelope<CounterEvent, CounterId>) -> Result<(), Self::Error> {
        let conn = self.get_connection();
        let id_str = serde_json::to_string(&event.aggregate_id).unwrap();

        match &event.payload {
            CounterEvent::Incremented { amount } => {
                let query = "INSERT INTO counter_read_model (counter_id, current_value) VALUES (?, ?) \
                             ON CONFLICT(counter_id) DO UPDATE SET current_value = current_value + ?";
                conn.execute(query, &[
                    Value::Text(id_str),
                    Value::Integer(*amount as i64),
                    Value::Integer(*amount as i64),
                ]).map_err(|e| format!("{:?}", e))?;
            }
            CounterEvent::Decremented { amount } => {
                let query = "INSERT INTO counter_read_model (counter_id, current_value) VALUES (?, ?) \
                             ON CONFLICT(counter_id) DO UPDATE SET current_value = current_value - ?";
                conn.execute(query, &[
                    Value::Text(id_str),
                    Value::Integer(-(*amount) as i64),
                    Value::Integer(*amount as i64),
                ]).map_err(|e| format!("{:?}", e))?;
            }
            CounterEvent::ResetPerformed { value } => {
                let query = "INSERT INTO counter_read_model (counter_id, current_value) VALUES (?, ?) \
                             ON CONFLICT(counter_id) DO UPDATE SET current_value = ?";
                conn.execute(query, &[
                    Value::Text(id_str),
                    Value::Integer(*value as i64),
                ]).map_err(|e| format!("{:?}", e))?;
            }
        }
        Ok(())
    }
}
```

### Driving Projections with `PersistedProjectionRunner`
To keep this read model updated sequentially, we use `PersistedProjectionRunner`. 

When a command is executed, new events are appended. We load our last processed projection sequence from `SpinSqliteCheckpointStore`, fetch globally newer events from `SpinSqliteEventStore`, apply them sequentially to our projection, and update the checkpoint after each successful event. The projection write and checkpoint write are not one transaction, so projection updates must be idempotent:

```rust
use ddd_cqrs_es::PersistedProjectionRunner;

pub fn sync_read_model(
    store: &SpinSqliteEventStore<Counter>,
    checkpoint_store: &SpinSqliteCheckpointStore,
    projection: &mut CounterProjection,
) -> Result<usize, String> {
    // 1. Wrap the projection and checkpoint tracker
    let mut runner = PersistedProjectionRunner::new(projection, checkpoint_store);
    
    // 2. Fetch checkpoint, pull pending events from the store, apply, and save progress!
    runner.run(store).map_err(|e| format!("Projection runner failed: {:?}", e))
}
```

---

## 5. Leptos SSR Server Functions Integration

Leptos utilizes Server Functions (`#[server]`) to elegantly bridge client components with backend resources. On the server side (`ssr` feature), we wire up our domain repositories, event store, checkpoint store, and projections.

### 5.1 Server Function Definitions (`src/app.rs`)

Here is how our server functions are constructed inside `/Users/uriah/Code/ddd/examples/counter-app/src/app.rs`. Notice how they isolate SSR execution from client-side WASM hydration compilation:

```rust
#[server(prefix = "/api")]
pub async fn get_count() -> Result<i32, ServerFnError> {
    #[cfg(feature = "ssr")]
    {
        get_count_db().await
    }
    #[cfg(not(feature = "ssr"))]
    {
        unreachable!()
    }
}

#[server(prefix = "/api")]
pub async fn increment_count(amount: i32) -> Result<(), ServerFnError> {
    #[cfg(feature = "ssr")]
    {
        if amount <= 0 {
            return Err(ServerFnError::new("Amount must be positive"));
        }
        run_cqrs_command(crate::domain::CounterCommand::Increment { amount })
    }
    #[cfg(not(feature = "ssr"))]
    {
        let _ = amount;
        unreachable!()
    }
}

#[server(prefix = "/api")]
pub async fn decrement_count(amount: i32) -> Result<(), ServerFnError> {
    #[cfg(feature = "ssr")]
    {
        if amount <= 0 {
            return Err(ServerFnError::new("Amount must be positive"));
        }
        run_cqrs_command(crate::domain::CounterCommand::Decrement { amount })
    }
    #[cfg(not(feature = "ssr"))]
    {
        let _ = amount;
        unreachable!()
    }
}

#[server(prefix = "/api")]
pub async fn reset_count() -> Result<(), ServerFnError> {
    #[cfg(feature = "ssr")]
    {
        run_cqrs_command(crate::domain::CounterCommand::Reset)
    }
    #[cfg(not(feature = "ssr"))]
    {
        unreachable!()
    }
}
```

### 5.2 Unified Server-Side Command Execution (`src/app.rs`)

Behind the scenes on the server, we initialize our event store and checkpoints, execute the command within aggregate consistency boundaries through the repository, and instantly advance our projection runner to synchronize the read model:

```rust
#[cfg(feature = "ssr")]
fn run_cqrs_command(command: crate::domain::CounterCommand) -> Result<(), ServerFnError> {
    use ddd_cqrs_es::{Repository, PersistedProjectionRunner};
    use crate::store::{SpinSqliteEventStore, SpinSqliteCheckpointStore, CounterProjection};
    use crate::domain::{Counter, CounterId};

    let event_store = SpinSqliteEventStore::<Counter>::new("default");
    
    // Ensure table schemas are initialized
    event_store.initialize_schema().map_err(|e| ServerFnError::new(e))?;

    let repo = Repository::new(event_store.clone());
    let aggregate_id = CounterId("global".to_string());

    // Execute the command through the repository (validating business invariants)
    repo.execute(&aggregate_id, command, ddd_cqrs_es::Metadata::default())
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    // Advance projection and save the checkpoint after successful projection writes.
    // Projection writes must be idempotent because checkpoint updates are separate.
    let checkpoint_store = SpinSqliteCheckpointStore::new("default");
    let projection = CounterProjection::new("default");
    let mut runner = PersistedProjectionRunner::new(projection, checkpoint_store);

    runner.run::<Counter, _>(&event_store)
        .map_err(|e| ServerFnError::new(format!("{:?}", e)))?;

    Ok(())
}
```

### 5.3 WASI Server Function Registration (`src/server.rs`)

To allow the Leptos WASI server router to map incoming client POST requests (like `/api/increment_count`) to their respective handler functions, we register each server function on the `Handler` builder inside `src/server.rs`:

```rust
use crate::app::{App, GetCount, IncrementCount, DecrementCount, ResetCount, GetLatestEvents};
use leptos_wasi::prelude::Handler;
use wasip3::http::types::{Request, Response, ErrorCode};

struct LeptosServer;

impl wasip3::exports::http::handler::Guest for LeptosServer {
    async fn handle(request: Request) -> Result<Response, ErrorCode> {
        let _ = init_wasip3_spawner();
        let conf = get_configuration(None).unwrap();
        let leptos_options = conf.leptos_options;

        let req = wasip3::http_compat::http_from_wasi_request(request)?;

        // Build handler and register each server function using .with_server_fn::<T>()
        let wasi_res = Handler::build(req).await
            .map_err(|e| ErrorCode::InternalError(None))?
            .static_files_handler("/pkg", serve_static_files)
            .with_server_fn::<GetCount>()
            .with_server_fn::<IncrementCount>()
            .with_server_fn::<DecrementCount>()
            .with_server_fn::<ResetCount>()
            .with_server_fn::<GetLatestEvents>()
            .generate_routes(App)
            .handle_with_context(move || shell(leptos_options.clone()), || {})
            .await
            .map_err(|e| ErrorCode::InternalError(None))?;

        Ok(wasi_res)
    }
}
```

---

## 6. Polished Premium UI Walkthrough

On the frontend, Leptos uses reactive signals, server actions, and forms to provide a snappy, fluid user interface. Let's inspect the `HomePage` view from our counter app to see how it binds server functions and implements optimistic states:

```rust
#[component]
fn HomePage() -> impl IntoView {
    // Action to trigger backend mutation
    let increment_action = ServerAction::<IncrementCount>::new();
    
    // Local signal holding optimistic display count
    let (optimistic_count, set_optimistic_count) = signal(None::<u32>);
    
    // Resource representing the server-synchronized data
    let count = Resource::new(
        move || increment_action.version().get(),
        |_| get_count()
    );

    // Effect: Synchronize the local optimistic count when server count updates
    Effect::new(move |_| {
        if let Some(Ok(server_count)) = count.get() {
            set_optimistic_count.set(Some(server_count));
        }
    });

    let display_count = move || {
        if let Some(opt_count) = optimistic_count.get() {
            opt_count.to_string()
        } else {
            "...".to_string()
        }
    };

    view! {
        <div class="min-h-screen bg-[#1a2332] flex items-center justify-center p-4">
            <div class="bg-[#263343] rounded-xl shadow-2xl p-8 md:p-12 max-w-md w-full border border-[#3a4a5c]">
                <div class="text-center space-y-8">
                    <div class="space-y-2">
                        <div class="flex items-center justify-center gap-3 mb-4">
                            <div class="w-10 h-10 bg-[#00d4aa] rounded-lg flex items-center justify-center">
                                <span class="text-[#1a2332] font-bold text-xl">L</span>
                            </div>
                            <h1 class="text-3xl md:text-4xl font-medium text-white">
                                "counter-app"
                            </h1>
                        </div>
                        <p class="text-[#8b9cb8] text-sm">
                            "Powered by Leptos + WASI SQLite CQRS"
                        </p>
                    </div>

                    <div class="relative">
                        <div class="bg-[#1a2332] rounded-lg p-8 border border-[#3a4a5c]">
                            <div class="text-5xl md:text-6xl font-light text-white tabular-nums">
                                {display_count}
                            </div>
                            <div class="text-[#8b9cb8] text-sm mt-2 uppercase tracking-wider">
                                "COUNT VALUE"
                            </div>
                        </div>

                        {/* Spinner layout shown when a server sync transaction is pending */}
                        <Show when=move || increment_action.pending().get()>
                            <div class="absolute inset-0 flex items-center justify-center bg-[#1a2332]/50 rounded-lg">
                                <div class="animate-spin rounded-full h-8 w-8 border-2 border-transparent border-t-[#00d4aa]"></div>
                            </div>
                        </Show>
                    </div>

                    <ActionForm action=increment_action>
                        <button
                            disabled=move || increment_action.pending().get()
                            on:click=move |_| {
                                // OPTIMISTIC UPDATE: Increment state on screen instantly
                                if let Some(current) = optimistic_count.get() {
                                    set_optimistic_count.set(Some(current + 1));
                                }
                            }
                            class="w-full rounded-lg bg-[#00d4aa] px-6 py-3 text-[#1a2332] font-medium transition-all duration-200 hover:bg-[#00b894] active:scale-[0.98] disabled:opacity-50 disabled:cursor-not-allowed"
                        >
                            {move || if increment_action.pending().get() {
                                "Syncing..."
                            } else {
                                "Increment Counter"
                            }}
                        </button>
                    </ActionForm>

                    <div class="flex items-center justify-center gap-2 text-xs">
                        <div class={move || {
                            if optimistic_count.get().is_none() {
                                "w-2 h-2 rounded-full bg-yellow-500 animate-pulse"
                            } else if increment_action.pending().get() {
                                "w-2 h-2 rounded-full bg-[#00d4aa] animate-pulse"
                            } else {
                                "w-2 h-2 rounded-full bg-[#00d4aa]"
                            }
                        }}></div>
                        <span class="text-[#8b9cb8] uppercase tracking-wider">
                            {move || {
                                if optimistic_count.get().is_none() {
                                    "Loading"
                                } else if increment_action.pending().get() {
                                    "Syncing"
                                } else {
                                    "Ready"
                                }
                            }}
                        </span>
                    </div>
                </div>
            </div>
        </div>
    }
}
```

### Hydration Mechanics & UI States
1.  **Server-Side Rendering (SSR)**: When the page is loaded, the server triggers `get_count()`, renders the HTML layout with the true count, and sends down static markup. The user sees a fully rendered page instantly.
2.  **Hydration**: The compiled Client WebAssembly binary is loaded by the browser, intercepts the static page, attaches event listeners, and initializes signals. The transition is completely invisible and painless.
3.  **Optimistic State Updates**: On button click, we don't wait for a round-trip network response. We instantly increment our optimistic signal `current + 1`, and the UI updates within a millisecond. In the background, Leptos posts to our command function. Once the command completes, the final, verified server-value is received and overrides the optimistic value. This eliminates perceived latency!

---

## 7. Getting Started & Execution Guide

Follow these steps to build, configure, and execute the application with various database engines across different WASI-compliant WebAssembly runtimes.

### ⚙️ Prerequisites

Ensure you have the following installed on your developer machine:
*   **Rust Toolchain**: Stable release (Rust 1.93.0+ or similar)
*   **WASM Target**: `rustup target add wasm32-wasip2`
*   **Fermyon Spin CLI** (for Spin runtime): `brew install fermyon/tap/spin`
*   **Wasmtime CLI** (for bare WASM runtime): `brew install wasmtime`
*   **cargo-leptos**: `cargo install cargo-leptos`

### 🔑 Environment Setup (`.env`)

Before running the application, configure your databases. We provide a complete template. Copy the example file to initialize your config:

```bash
cp examples/counter-app/.env.example examples/counter-app/.env
```

Open `examples/counter-app/.env` and inspect the configuration variables. This file is tracked by version control as a reference, enabling seamless collaboration and automated testing across local and cloud environments:

```ini
# Supported make backends: sqlite, postgres, neon, supabase, turso, redis
DATABASE_BACKEND=sqlite

# Realtime transport: off, polling, redis
REALTIME_BACKEND=off
REDIS_CHANNEL=counter-events

# make derives DATABASE_URL/DATABASE_AUTH_TOKEN from the backend-specific
# values below before launching Spin or Wasmtime.

# =========================================================================
# 1. PostgreSQL Settings (Local PostgreSQL)
# =========================================================================
POSTGRES_URL=postgresql://postgres:postgres@localhost:5432/postgres

# =========================================================================
# 2. Neon Settings
# =========================================================================
NEON_DB_URL=

# =========================================================================
# 3. Supabase Settings
# =========================================================================
SUPABASE_URL=
SUPABASE_SECRET_KEY=

# =========================================================================
# 4. LibSQL / Turso Settings (HTTP API)
# =========================================================================
TURSO_URL=
TURSO_AUTH_TOKEN=

# =========================================================================
# 5. Redis Settings (experimental event store and realtime notifications)
# =========================================================================
REDIS_URL=redis://127.0.0.1:6379
```

---

## 🔀 Advanced Enterprise Multi-Backend Architecture

Our Leptos application implements a state-of-the-art **Multi-Backend Persistence Engine** inside `src/store.rs`. 

Because our CQRS and Event Sourcing infrastructure depends strictly on framework traits (`EventStore`, `CheckpointStore`, `Projection`), we designed dynamic, runtime-routed wrappers—`MultiBackendEventStore<A>`, `MultiBackendCheckpointStore`, and `MultiBackendCounterProjection`—which inspect the environment at boot-time and execute the correct database operations without modifying a single line of business or component-rendering code.

```mermaid
flowchart TD
    %% Define styles
    classDef client fill:#1e1e38,stroke:#00d4aa,stroke-width:2px,color:#fff;
    classDef server fill:#2d1e38,stroke:#a855f7,stroke-width:2px,color:#fff;
    classDef routing fill:#1e293b,stroke:#f59e0b,stroke-width:2px,color:#fff;
    classDef storage fill:#0f172a,stroke:#64748b,stroke-width:1px,color:#fff;

    Client["🖥️ Leptos Frontend (WASM)"]:::client
    Server["⚙️ Server Functions (WASI)"]:::server
    Router["🔀 Multi-Backend Dispatcher"]:::routing
    
    %% Storage Backends
    SpinSqlite["🗄️ Spin SQLite (Host-calls)"]:::storage
    PostgresNative["🐘 Postgres Native (TCP)"]:::storage
    NeonPostgres["⚡ Neon Serverless (Outbound HTTP)"]:::storage
    SupabaseDB["⚡ Supabase REST (Outbound HTTP)"]:::storage
    TursoLibSql["🌀 Turso/LibSQL (Hrana HTTP)"]:::storage
    RedisStore["Redis Event Store + Notifications"]:::storage
    JsonFile["📂 JSON Flat-File (WASM Directory Mount)"]:::storage

    %% Relationships
    Client -->|"POST /api/increment_count"| Server
    Server -->|"Repository::execute()"| Router
    
    Router -->|"DATABASE_BACKEND = sqlite"| SpinSqlite
    Router -->|"DATABASE_BACKEND = postgres"| PostgresNative
    Router -->|"DATABASE_BACKEND = neon"| NeonPostgres
    Router -->|"DATABASE_BACKEND = supabase"| SupabaseDB
    Router -->|"DATABASE_BACKEND = libsql"| TursoLibSql
    Router -->|"DATABASE_BACKEND = redis"| RedisStore
    Router -->|"DATABASE_BACKEND = sqlite (under Wasmtime)"| JsonFile
```

### Supported Database Backends Matrix

| Backend Key (`db`) | Connection Model | Network Protocol | Target Runtime Compatibility | Use Cases | Realtime Support |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`sqlite`** | Local Host-Call | WASM Host Interface | **Fermyon Spin** only | Low-latency local dev, edge microservices | Yes (via SSE stream) |
| **`postgres`** | Direct Socket Pool | TCP Socket stream | **Fermyon Spin** (via outbound TCP) | Classic high-throughput self-hosted PG | Yes (via SSE stream) |
| **`neon`** | Stateless HTTP SQL | JSON over HTTP (WASIp3) | **Wasmtime** & **Fermyon Spin** | Serverless cloud databases with cold-start mitigation | No |
| **`supabase`** | Stateless REST | JSON REST over HTTP (WASIp3) | **Wasmtime** & **Fermyon Spin** | Rapid prototyping, managed Supabase database integration | No |
| **`libsql`** / **`turso`** | Hrana Protocol | Pipeline HTTP (WASIp3) | **Wasmtime** & **Fermyon Spin** | Globally distributed SQL, SQLite-at-the-edge (Turso) | No |
| **`redis`** | Async Redis commands | RESP TCP under Wasmtime, Spin Redis outbound and optional Redis Trigger under Spin | **Wasmtime** & **Fermyon Spin** | Experimental event persistence, checkpoints, and realtime notifications | Yes (via PubSub / SSE) |
| **`sqlite`** (Wasmtime) | JSON Flat-File Fallback | POSIX File I/O | **Wasmtime** (mounted volume) | Zero-dependency local testing without external servers | Yes (via SSE stream) |
| **`mysql`** | *None* | *None* | **Not Supported** | No MySQL support available | No |

---

## ⚡ Overcoming WASM Sandbox Limits: Stateless Outbound HTTP

When compiling applications to WebAssembly targets, traditional blocking socket connection pools (such as those used by `tokio-postgres` or standard native SQLite engines written in C) are strictly incompatible with the isolated, single-threaded sandboxed environment of a WASM component.

To solve this compilation and runtime block, our Multi-Backend Engine employs a stateless, highly optimized **Outbound HTTP Bridge** powered by the WASIp3 HTTP Component Model standards. 

When a database request is sent to Neon, Supabase, or Turso, the store adapter converts the SQL query and its parameterized arguments into a payload format (e.g., the JSON-based Hrana pipeline protocol for Turso/LibSQL or the HTTP SQL Endpoint format for Neon), dispatches it via a single non-blocking HTTP POST, collects the response, and translates the rows back into DDD event envelopes.

### Sequence Flow: Outbound HTTP Database Query

```mermaid
sequenceDiagram
    autonumber
    participant App as ⚙️ WASM Component
    participant SDK as 🔌 WASIp3 Outbound HTTP
    participant Host as 🌐 Runtime Host (Wasmtime/Spin)
    participant Cloud as ☁️ Database Cloud API (Neon/Turso)

    App->>App: Serialize query + params into JSON body
    App->>SDK: wasip3::http::client::send(wasi_req)
    Note over SDK,Host: Native host-call transitions<br/>out of the WASM sandbox
    Host->>Cloud: HTTP POST (TLS connection over internet)
    Cloud-->>Host: HTTP 200 OK (JSON Rows/Result)
    Host-->>SDK: Parse HTTP response stream
    SDK-->>App: Return raw response bytes
    App->>App: Deserialize bytes into EventEnvelope<A>
```

Here is a simplified look at how the `MultiBackendEventStore` leverages WASIp3 Outbound HTTP helpers to communicate with external SQL APIs:

```rust
// A look under the hood of src/store.rs:
pub struct MultiBackendEventStore<A> {
    _phantom: PhantomData<fn() -> A>,
}

impl<A> EventStore<A> for MultiBackendEventStore<A>
where
    A: Aggregate + 'static,
    A::Event: serde::Serialize + serde::de::DeserializeOwned,
    A::Id: serde::Serialize + serde::de::DeserializeOwned,
{
    type Error = EventStoreError;

    fn load(&self, aggregate_id: &A::Id) -> Result<Vec<EventEnvelope<A::Event, A::Id>>, Self::Error> {
        let backend = get_backend();
        
        match backend.as_str() {
            "sqlite" => {
                #[cfg(runtime_spin)] {
                    let store = SpinSqliteEventStore::<A>::new("default");
                    store.load(aggregate_id)
                }
                #[cfg(not(runtime_spin))] {
                    // Under Wasmtime, fallback to JSON Flat-File Store mounted at /data/
                    let store = JsonFileEventStore::<A>::new("/data");
                    store.load(aggregate_id)
                }
            }
            "postgres" => {
                #[cfg(feature = "postgres")] {
                    let store = PostgresEventStore::<A>::new(get_postgres_url());
                    store.load(aggregate_id)
                }
                #[cfg(not(feature = "postgres"))] {
                    Err(EventStoreError::Backend("Postgres feature not enabled".to_string()))
                }
            }
            "neon" => {
                // Execute stateless queries over Outbound HTTP SQL API
                let url = get_postgres_url();
                let sql = "SELECT ... FROM events WHERE aggregate_id = $1";
                let rows = block_on(execute_neon_query(&url, sql, vec![aggregate_id.to_string()]))?;
                deserialize_postgres_rows(rows)
            }
            "turso" | "libsql" => {
                // Execute Hrana pipeline over Outbound HTTP
                let url = get_turso_url();
                let token = get_turso_auth_token();
                let sql = "SELECT ... FROM events WHERE aggregate_id = ?";
                let result = block_on(execute_hrana_query(&url, token.as_deref(), sql, vec![aggregate_id.to_string()]))?;
                deserialize_sqlite_rows(result.rows)
            }
            _ => Err(EventStoreError::Backend(format!("Unsupported database backend: {}", backend))),
        }
    }
    
    // Similarly dispatched for `append()` and `load_global_after()`...
}
```

---

## 🛠️ Execution & Testing Playbook

We have provided a unified `Makefile` inside `examples/counter-app` to compile, package, and launch our Leptos WASM application using simple target flags. This shields you from compiling custom target configurations manually.

### 1. Build and Run under Wasmtime (Bare Component Runtime)

Running under Wasmtime is incredibly useful for standard system deployment, local orchestration, and target compatibility checks.

```bash
# Compile and run with the default local JSON Flat-File engine
# (Creates and writes to examples/counter-app/data/ folder automatically!)
make wasmtime

# Compile and run connected to Neon serverless Postgres via WASIp3 Outbound HTTP
make wasmtime db=neon

# Compile and run connected to Supabase REST database via WASIp3 Outbound HTTP
make wasmtime db=supabase

# Compile and run connected to Turso/LibSQL DB over Hrana HTTP
make wasmtime db=turso

# Compile and run with the experimental Redis event store and SSE notifications
make wasmtime db=redis realtime=redis
```

### 2. Build and Run under Fermyon Spin (Microservices Runtime)

Running under Fermyon Spin leverages the Spin-specific SQLite host engine or native Postgres connectivity.

```bash
# Compile and run with native Spin SQLite database host-calls
make spin

# Compile and run with native Spin PostgreSQL database connector
make spin db=postgres

# Compile and run with Spin Redis persistence and SSE notifications
make spin db=redis realtime=redis
```

Once launched, open your web browser to `http://127.0.0.1:3000` to interact with your secure, full-stack, optimistic-updating, Event-Sourced Leptos application!
---

## 💎 The Pure DDD & CQRS Advantage

Take a moment to step back and realize what we have accomplished.

By separating **Domain Logic** (commands, aggregate invariants, and events) from **Infrastructure Concerns** (SQLite, Postgres, HTTP API protocols, network sandboxing, and runtime-specific environments), we have made our application completely robust, future-proof, and flexible.

*   Want to run your microservice as a lightweight, zero-dependency serverless edge component? **Set `DATABASE_BACKEND=sqlite`**.
*   Need to scale to enterprise workloads on AWS with thousands of events per second? **Enable `DATABASE_BACKEND=postgres`**.
*   Want to run globally distributed edge containers with serverless SQL backends? **Set `DATABASE_BACKEND=neon` or `DATABASE_BACKEND=turso`**.
*   Want experimental Redis-backed event persistence and faster cross-client UI wakeups? **Set `DATABASE_BACKEND=redis` and `REALTIME_BACKEND=redis`**.

Your domain logic does not change by a single letter. That is the outstanding power of building enterprise-grade systems with clean, decoupled **Domain-Driven Design** and **Event Sourcing**!
