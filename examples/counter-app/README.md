# counter-app

This example is a Leptos WASI counter app backed by the `ddd_cqrs_es` command,
event store, projection, checkpoint, and realtime notification APIs. It runs
under both raw Wasmtime and Fermyon Spin.

## Prerequisites

- **Rust Toolchain:** Version 1.93.0 or later (required by `spin-sdk` v6.0.0).
- **Rust target:** `rustup target add wasm32-wasip2`
- **Cargo Leptos:** `cargo install --locked cargo-leptos`
- **Spin CLI:** Version 4.0.0 or later.
- **Wasmtime CLI:** Version 45.0.0 or later.
- **Redis CLI/server:** Optional, only needed for `db=redis` or `realtime=redis`.
- **MySQL server/client:** Optional, only needed for `db=mysql`.

## Build and Run

Default Wasmtime run:

```bash
make wasmtime
```

Default Spin run:

```bash
make spin
```

Reset the active backend without starting the app:

```bash
make db=sqlite fresh
```

Clean local build and storage files:

```bash
make clean
```

Once running, access the application at `http://127.0.0.1:3000`.

## Storage Backends

The Makefile selects storage with `db=<backend>` and derives the runtime
environment variables for you.

| Command | Backend |
| :--- | :--- |
| `make wasmtime` | Local JSON-file fallback mounted at `/data`. |
| `make spin` | Spin SQLite host-call store. |
| `make spin db=postgres` | Spin PostgreSQL connector. |
| `make wasmtime db=neon` | Neon HTTP SQL helper. |
| `make wasmtime db=supabase` | Supabase RPC helper. |
| `make wasmtime db=turso` | Turso/LibSQL Hrana HTTP helper. |
| `make wasmtime db=mysql` | Raw TCP MySQL helper using `wasi-mysql`. |
| `make spin db=mysql realtime=polling` | Spin SDK MySQL helper using `spin-mysql`. |
| `make wasmtime db=redis realtime=redis` | Experimental Redis event store with SSE notifications. |
| `make spin db=redis realtime=redis` | Experimental Spin Redis event store with SSE notifications. |

`db=mysql` uses `MYSQL_URL` first, then falls back to `DATABASE_URL`.

```bash
make db=mysql fresh
make wasmtime db=mysql
make spin db=mysql realtime=polling
```

`db=redis` uses `REDIS_URL`, defaulting to `redis://127.0.0.1:6379`.

```bash
redis-cli -u redis://127.0.0.1:6379 ping
make db=redis fresh
make wasmtime db=redis realtime=redis
```

## Realtime Updates

The browser uses SSE/EventSource, not WebSocket, for realtime updates in this
version. The stream endpoint is:

```text
/api/counter/stream?last_sequence=0
```

You can inspect it directly:

```bash
curl -N 'http://127.0.0.1:3000/api/counter/stream?last_sequence=0'
```

Server actions return the updated `CounterViewDto`, so the client updates the
count and latest events from the mutation response immediately. The SSE stream
then keeps other browser sessions current without forcing a refetch after every
local button click.

Redis pub/sub is notification-only. The durable event store, projection read
model, and checkpoint state remain the source of truth. If a publish fails
after a command commits, clients recover by replaying durable events from the
last seen sequence.

With `realtime=redis`, the SSE route uses Redis-blocking long polling. Each
browser request registers a short-TTL Redis list queue, then the server blocks
on `BRPOP` for that queue. After a command commits and projections update, the
publisher fans a wake message out to every live queue. The stream then re-reads
durable state after `last_sequence`, sends one `counter` event, and closes.

Idle streams do not reconnect repeatedly. When Redis has no wake message, the
handler waits inside `BRPOP` for up to 25 seconds, emits one SSE comment
keepalive with a 1 second EventSource retry interval, and closes. That keeps the
Network tab quiet and keeps the server waiting on Redis instead of repeatedly
checking the event store.
The route intentionally does not run a blocking Redis `SUBSCRIBE`; it uses
`BRPOP` through the existing outbound Redis command path so Spin and Wasmtime can
share the same browser delivery behavior.

On Spin, `make spin realtime=redis` uses `spin.redis.toml` and starts a
separate Redis trigger component subscribed to `REDIS_CHANNEL`. The trigger
validates realtime JSON and writes smoke-test health markers:

| Key | Meaning |
| :--- | :--- |
| `counter:redis_trigger:last_sequence` | Last sequence observed by the Redis trigger |
| `counter:redis_trigger:last_count` | Counter value from the last valid trigger message |
| `counter:redis_trigger:received_count` | Number of valid trigger messages observed |

The trigger does not update projections, checkpoints, event-store data, or the
browser stream. It proves Spin Redis Trigger wiring is active while browser SSE
delivery uses the per-connection Redis wake queues described above.

For `db=redis`, counter read-model updates and projection checkpoint updates are
applied together with one Lua command per event. Other backends use the generic
projection runner, so projection handlers still need to be idempotent.

Do not add a manual `Connection: keep-alive` response header to the SSE route.
WASIp3 treats that as a forbidden hop-by-hop header; the stream stays open
because the response body is streaming and the content type is
`text/event-stream`.

## Environment

Copy `.env.example` if you want persistent local settings:

```bash
cp .env.example .env
```

Important variables:

| Variable | Values |
| :--- | :--- |
| `DATABASE_BACKEND` | `sqlite`, `postgres`, `mysql`, `neon`, `supabase`, `turso`, `redis` |
| `REALTIME_BACKEND` | `off`, `polling`, `redis` |
| `MYSQL_URL` | MySQL URL, for example `mysql://user:password@127.0.0.1:3306/counter_app` |
| `REDIS_URL` | Redis URL, default `redis://127.0.0.1:6379` |
| `REDIS_CHANNEL` | Redis notification channel, default `counter-events` |

`make db=<backend>` and `make realtime=<mode>` override `.env` for one command.

## Runtime Notes

Static files from `./target/site/pkg/` are mapped by Wasmtime to `/` inside the
guest. The app implements the WASI HTTP handler and runs as a WebAssembly
component using the Preview 3 async ABI.
