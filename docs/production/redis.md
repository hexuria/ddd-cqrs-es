---
title: 5.3. Redis Event Store and Realtime
description: Experimental async Redis persistence and notification support.
---

Redis support has two separate roles in this project:

1. **Experimental event persistence:** `RedisEventStore<A, C>` implements the async event-store contract.
2. **Realtime notification:** `RedisPubSubPublisher<C>` publishes wake-up messages after commands commit.

Redis pub/sub is never the source of truth. Clients should use notifications to
wake up, then read durable events, checkpoints, or read models.

For Spin, Redis support also has two separate runtime paths:

* **Outbound Redis:** the HTTP component opens `spin_sdk::redis::Connection`
  for persistence, queries, and publishing.
* **Redis Trigger:** a separate subscriber component is invoked when Redis
  publishes to the configured channel.

---

## Feature Flags

Enable the base async Redis API with `redis`, then choose the runtime client:

| Feature | Runtime | Purpose |
| :--- | :--- | :--- |
| `redis` | Any async Rust target | Enables `RedisEventStore`, `RedisCheckpointStore`, `RedisPubSubPublisher`, and the `RedisCommandExecutor` trait. |
| `wasi-redis` | Generic Wasmtime/WASI | Enables `WasiRedisClient`, a small raw RESP client for plain `redis://` TCP URLs. |
| `spin-redis` | Fermyon Spin | Enables `SpinRedisClient`, backed by `spin_sdk::redis::Connection`. |

`RedisEventStore` is async-only. It intentionally does not implement the sync
`EventStore` trait because the current host APIs used by Spin and the counter
example are async.

---

## Redis Event Store Schema

The adapter stores event data with a small key layout under a configurable
prefix. The default prefix is `ddd_cqrs_es`.

| Key | Purpose |
| :--- | :--- |
| `{prefix}:seq` | Global monotonic sequence counter. |
| `{prefix}:global` | Sorted set of all global sequences. |
| `{prefix}:revision:{aggregate_type_hex}:{aggregate_id_hex}` | Current revision for one aggregate stream. |
| `{prefix}:stream:{aggregate_type_hex}:{aggregate_id_hex}` | Sorted set of sequences for one aggregate stream, scored by stream revision. |
| `{prefix}:event:{sequence}` | Redis hash containing one event envelope. |
| `{prefix}:checkpoint:{projection_name_hex}` | Last processed global sequence for one projection. |

Append is performed by one Lua `EVAL` script. The script validates the expected
revision, allocates global sequence numbers, updates the stream revision, stores
event hashes, and updates stream/global indexes atomically.

---

## Basic Usage

```rust,no_run
use ddd_cqrs_es::{AsyncRepository, RedisEventStore, WasiRedisClient};

# async fn setup() -> Result<(), Box<dyn std::error::Error>> {
let client = WasiRedisClient::new("redis://127.0.0.1:6379");
let store = RedisEventStore::<BankAccount, _>::new(client);
let repo = AsyncRepository::new(store);
# Ok(())
# }
```

Use a custom prefix when multiple apps share one Redis database:

```rust,no_run
use ddd_cqrs_es::{RedisEventStore, WasiRedisClient};

# fn setup() -> Result<(), ddd_cqrs_es::EventStoreError> {
let client = WasiRedisClient::new("redis://127.0.0.1:6379");
let store = RedisEventStore::<BankAccount, _>::with_prefix(client, "my_app:v1")?;
# Ok(())
# }
```

---

## Checkpoints

`RedisCheckpointStore<C>` implements `AsyncCheckpointStore`.

```rust,no_run
use ddd_cqrs_es::{RedisCheckpointStore, WasiRedisClient};

# async fn checkpoint() -> Result<(), ddd_cqrs_es::EventStoreError> {
let client = WasiRedisClient::new("redis://127.0.0.1:6379");
let checkpoints = RedisCheckpointStore::new(client);

checkpoints.save_checkpoint("counter_projection", 42).await?;
let last = checkpoints.load_checkpoint("counter_projection").await?;
assert_eq!(last, Some(42));
# Ok(())
# }
```

Projection writes and checkpoint writes are still separate operations. Projection
handlers must be idempotent so a retry does not corrupt a read model.

---

## Pub/Sub Notifications

`RedisPubSubPublisher<C>` is notification-only. Publish after event append and
projection update succeeds.

```rust,no_run
use ddd_cqrs_es::{RedisPubSubPublisher, WasiRedisClient};
use serde::Serialize;

#[derive(Serialize)]
struct CounterMessage {
    last_sequence: u64,
}

# async fn publish() -> Result<(), ddd_cqrs_es::EventStoreError> {
let client = WasiRedisClient::new("redis://127.0.0.1:6379");
let publisher = RedisPubSubPublisher::new(client, "counter-events");

publisher
    .publish_json(&CounterMessage { last_sequence: 42 })
    .await?;
# Ok(())
# }
```

If notification publishing fails after a command has committed, do not roll back
the command. Log or emit telemetry, then allow clients to recover through
durable replay from their last seen sequence.

---

## Counter App Realtime

The counter example uses SSE/EventSource as the browser transport:

```bash
cd examples/counter-app
make db=redis fresh
make wasmtime db=redis realtime=redis
```

Spin uses the Spin Redis client:

```bash
make spin db=redis realtime=redis
```

When `realtime=redis`, the Spin example uses `spin.redis.toml` and starts a
separate Redis trigger component subscribed to `REDIS_CHANNEL`. The trigger
parses each `CounterRealtimeMessage` and records health markers in Redis:

| Key | Meaning |
| :--- | :--- |
| `counter:redis_trigger:last_sequence` | Last realtime sequence observed by the Spin Redis trigger. |
| `counter:redis_trigger:last_count` | Counter value from the last valid realtime message. |
| `counter:redis_trigger:received_count` | Number of valid realtime messages observed. |

The trigger does not update projections, checkpoints, event-store data, or the
browser SSE response. It is a smoke-testable subscriber that proves Spin Redis
Trigger wiring is active.

Environment variables:

| Variable | Default | Meaning |
| :--- | :--- | :--- |
| `DATABASE_BACKEND` | `sqlite` | Set to `redis` for Redis event persistence in the counter app. |
| `REALTIME_BACKEND` | `off` | `off`, `polling`, or `redis`. Non-`off` enables `/api/counter/stream`. |
| `REDIS_URL` | `redis://127.0.0.1:6379` | Redis connection URL. |
| `REDIS_CHANNEL` | `counter-events` | Channel used for Redis notification publishing. |

The SSE endpoint is:

```text
/api/counter/stream?last_sequence=0
```

It emits frames like:

```text
event: counter
data: {"view":{"count":1,"latest_events":[...],"last_sequence":1,"realtime_enabled":true},"last_sequence":1}
```

Do not set `Connection: keep-alive` manually on this endpoint. WASIp3 rejects
that hop-by-hop header during response conversion. The stream stays open because
the response body is streaming and the content type is `text/event-stream`.

With `REALTIME_BACKEND=redis`, the SSE route uses Redis-blocking long polling.
Each browser request registers a short-TTL Redis list queue, then the server
blocks on `BRPOP` for that queue. After commands commit and projections update,
the publisher fans one wake message out to every live queue. The SSE handler
treats that wake as notification-only and reads durable events after the
client's `last_sequence` before emitting one `counter` event and closing the
response.

Idle clients do not reconnect every few hundred milliseconds. When Redis has no
wake message, the handler waits inside `BRPOP` for up to 25 seconds, emits one
SSE comment keepalive with a 1 second EventSource retry interval, and closes.
That means an idle tab creates roughly one request every 26 seconds, and the
server is waiting on Redis rather than repeatedly querying the event store.

Redis publishing remains a notification hook. On Spin, the optional Redis
trigger sidecar observes the same pub/sub notifications and records health
markers, but browser delivery uses the per-connection Redis list queues because
the trigger cannot write into an already-open HTTP response owned by the HTTP
component. The HTTP route does not perform a blocking Redis `SUBSCRIBE`; it uses
`BRPOP` through the existing outbound Redis command path.

For the counter app's Redis backend, read-model updates and checkpoint updates
are applied together with one Lua command per event. The generic projection
runner contract remains store-agnostic and still requires idempotent projection
handlers.

---

## Current Limitations

Redis support is marked experimental until broader live contract coverage proves
ordering, recovery, and operational behavior under production traffic.

Known boundaries:

* `WasiRedisClient` supports plain `redis://` TCP URLs. It does not implement TLS, Sentinel, Cluster, or RESP3-specific behavior.
* Redis pub/sub is lossy notification, not durable delivery.
* Counter SSE wake queues are best-effort notification. Durable events remain the source of truth, and clients recover through `last_sequence` replay.
* The event store is async-only.
* Generic projection writes and checkpoint writes are not one transaction unless an adapter or application adds a transaction-aware runner.
* The counter app HTTP SSE route uses Redis `BRPOP` wake queues instead of Redis `SUBSCRIBE`, because Spin outbound Redis exposes command execution while Redis Trigger runs as a separate component. It is a Redis-blocking long poll, not a permanent multi-chunk WebSocket-style stream.
