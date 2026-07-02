---
title: 5.3. Database Query Patterns
description: Design event-store, checkpoint, snapshot, and read-model queries without accidental scans or misleading consistency claims.
---

Production CQRS systems are fast when each query has one clear job. The event
store is optimized for command execution and sequential replay. User-facing
screens should query read models that were built from the event log.

## Framework Query Shape

The SQL event-store adapters use these hot paths:

| Operation | Query shape | Required access path |
| :--- | :--- | :--- |
| Load one aggregate stream | `WHERE aggregate_type = ? AND aggregate_id = ? ORDER BY revision ASC` | `UNIQUE (aggregate_type, aggregate_id, revision)` |
| Check current stream revision | `MAX(revision)` for one `aggregate_type` and `aggregate_id` | Same stream uniqueness index |
| Replay new global events for one aggregate type | `WHERE aggregate_type = ? AND sequence > ? ORDER BY sequence ASC` | `INDEX (aggregate_type, sequence)` |
| Load latest global rows for a small ledger view | `ORDER BY sequence DESC LIMIT n` | Primary key on `sequence` |
| Load or save checkpoints | `projection_name` point lookup/upsert | Primary key on `projection_name` |
| Load or save idempotency state | `idempotency_key` point lookup/upsert | Primary key on `idempotency_key` |
| Load or save snapshots | `(aggregate_type, aggregate_id)` point lookup/upsert | Primary key on `(aggregate_type, aggregate_id)` |

The framework schema creates a global replay index on `(aggregate_type,
sequence)` because projection replay is aggregate-type scoped. The stream
uniqueness constraint already covers stream loads, so a second identical stream
index is unnecessary in fresh schemas. Schema migration v6 removes the old
duplicate `{events_table}_stream_idx` index when it exists:

- SQLite and PostgreSQL drop `{events_table}_stream_idx` directly with `DROP
  INDEX IF EXISTS`.
- MySQL discovers non-unique indexes whose ordered columns are exactly
  `(aggregate_type, aggregate_id, revision)` and drops only those duplicates,
  preserving the unique stream constraint and the global replay index.

## Bounded Projection Replay

`EventStore::load_global_after` and projection runner `run(...)` methods remain
available for compatibility, but they load the full backlog after the
checkpoint. Production workers should prefer the bounded APIs:

```rust
use ddd_cqrs_es::ProjectionBatchConfig;

let config = ProjectionBatchConfig::default();
let outcome = runner.run_batch::<BankAccount, _>(&event_store, config)?;

if !outcome.caught_up {
    // Schedule another batch immediately or let the worker loop continue.
}
```

The default batch size is `500`. SQL adapters apply `LIMIT`, Redis uses
`ZRANGEBYSCORE ... LIMIT`, and the in-memory store uses iterator `take`, so the
bounded path avoids fetching an unbounded tail in production adapters.

## Read Models Own Product Queries

Do not turn the event table into an ad hoc reporting database. Avoid these
patterns on hot paths:

- Filtering or sorting by fields inside `payload` JSON.
- Joining the event table directly into product screens.
- Replaying a full stream on every read request once the stream can grow large.
- Loading all global events just to show a small read-model value.

Instead, project the fields you query into application-owned read-model tables.
Index those tables for the UI access pattern, not for the event-store write
pattern.

For example, a dashboard should read:

```sql
SELECT balance, status, updated_at_ms
FROM account_read_model
WHERE account_id = ?;
```

It should not filter historical `payload` JSON to discover the current balance.

## Checkpoints Must Move Forward

Projection checkpoints represent the last durable sequence a projection
finished processing. Saving an older checkpoint can make a worker replay work it
already completed.

Use monotonic upserts:

```sql
-- PostgreSQL
INSERT INTO projection_checkpoints (projection_name, sequence)
VALUES ($1, $2)
ON CONFLICT (projection_name)
DO UPDATE SET sequence = GREATEST(projection_checkpoints.sequence, EXCLUDED.sequence);
```

The SQLite, PostgreSQL, and MySQL checkpoint stores apply this rule.

## Eventual Consistency

Eventual consistency is a tradeoff, not a defect and not a magic guarantee.

Advantages:

- Command transactions stay small: validate aggregate state, append events, and
  return.
- Read models can be rebuilt, scaled, denormalized, and indexed for each screen.
- Slow reporting queries do not block command writes.

Disadvantages:

- A read model can lag behind the command response.
- Realtime notifications can be duplicated, delayed, or missed.
- UIs must avoid rewinding optimistic state when an older read-model snapshot
  arrives.

For low-latency screens, return the authoritative write-side result from the
command handler, then reconcile read-model or SSE updates by sequence. Older
snapshots should not overwrite a newer visible sequence.

## Realtime Is A Wake Signal

SSE, WebSocket, polling, and Redis pub/sub are transport choices. They do not
replace durable event replay.

The safe pattern is:

1. Command appends events to the durable store.
2. Server publishes a wake notification.
3. Client or worker loads durable events after its last known `sequence`.
4. Client ignores duplicate or older sequences.

Redis pub/sub is useful for low-latency wakeups, but it should not be described
as exactly-once delivery.

## Query Review Checklist

Before adding a database query:

- Identify whether it belongs to the write model, event replay, checkpointing,
  idempotency, snapshots, or a read model.
- Check the `WHERE` and `ORDER BY` columns against an existing primary key,
  unique constraint, or index.
- Use a read model for product queries that filter by business fields.
- Keep projection catch-up bounded or run it outside the request path when
  backlogs can become large.
- Use `EXPLAIN` or the database query planner before claiming a query is
  optimized.
- Update this guide and the counter-app docs when a new query pattern becomes
  part of the recommended workflow.

## Verifying Plans

The test suite includes SQLite planner assertions by default when the `sqlite`
feature is enabled. PostgreSQL and MySQL plan tests are live-gated:

```bash
cargo test --all-features sqlite_query_plans_use_expected_access_paths
DDD_CQRS_ES_POSTGRES_URL=postgresql://localhost/ddd_test \
  cargo test --all-features postgres_query_plans_use_expected_indexes_when_url_is_provided
DDD_CQRS_ES_MYSQL_URL=mysql://root:password@127.0.0.1:3306/ddd_test \
  cargo test --all-features test_mysql_query_plans_and_v6_duplicate_index_cleanup
```

To inspect an existing database manually, list duplicate stream indexes before
and after running schema migration v6:

```sql
-- PostgreSQL
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = 'events';

-- MySQL
SELECT index_name, non_unique,
       GROUP_CONCAT(column_name ORDER BY seq_in_index) AS columns_in_order
FROM information_schema.statistics
WHERE table_schema = DATABASE()
  AND table_name = 'events'
GROUP BY index_name, non_unique;
```
