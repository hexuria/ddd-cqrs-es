# Persistence

`EventStore<A>` is the adapter boundary for persistence. Implementations must
store committed event envelopes and preserve optimistic concurrency semantics.

Required behavior:

- Load events by aggregate ID.
- Append events to a stream.
- Enforce `ExpectedRevision`.
- Preserve metadata.
- Assign stream revisions starting at `1`.
- Preserve event order in each stream.
- Return cloned envelopes without exposing mutable store internals.
- Support global reads after a sequence when the backend has global ordering.

The in-memory store uses `Arc<RwLock<...>>`, stores events per aggregate stream,
assigns global sequence numbers, and exposes `clear` for tests.

## SQLite Adapter

Enable with:

```toml
features = ["sqlite"]
```

`SqliteEventStore<A>` uses `rusqlite`, stores aggregate IDs, payloads, and
metadata as JSON text, and uses a unique `(aggregate_type, aggregate_id,
revision)` constraint for optimistic concurrency.

```rust
let store = ddd_cqrs_es::SqliteEventStore::<MyAggregate>::in_memory()?;
```

For file-backed stores, create a `rusqlite::Connection`, pass it to
`SqliteEventStore::new`, and call `initialize_schema`.

## PostgreSQL Adapter

Enable with:

```toml
features = ["postgres"]
```

`PostgresEventStore<A>` uses the synchronous `postgres` driver, stores payloads
and metadata as `JSONB`, assigns global sequence values through `BIGSERIAL`, and
uses a unique `(aggregate_type, aggregate_id, revision)` constraint.

```rust
let store = ddd_cqrs_es::PostgresEventStore::<MyAggregate>::connect(
    "host=localhost port=5432 user=uriah dbname=events"
)?;
store.initialize_schema()?;
```

The integration suite includes an opt-in live contract test. Set
`DDD_CQRS_ES_POSTGRES_URL` to run it:

```bash
DDD_CQRS_ES_POSTGRES_URL='host=localhost port=5432 user=uriah dbname=ddd_cqrs_es_live' \
  cargo test --features postgres postgres_store_passes_reusable_contract_when_url_is_provided
```

Durable adapters should map concurrency failures to `ConcurrencyError` and
preserve adapter-specific context in `EventStoreError` or their own associated
error type.

The current adapters use this logical SQL shape:

```sql
CREATE TABLE events (
    sequence BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    aggregate_id TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    revision BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    event_version INT NOT NULL,
    payload JSONB NOT NULL,
    metadata JSONB NOT NULL,
    recorded_at_ms BIGINT NOT NULL,
    UNIQUE (aggregate_type, aggregate_id, revision)
);
```
