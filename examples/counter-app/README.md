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

Run these commands from `examples/counter-app`.

Show the supported targets, backend examples, reset commands, and environment
variables:

```bash
make help
make help topic=db
make help topic=realtime
make help-matrix
```

Default Wasmtime run:

```bash
make wasmtime
```

Default Spin run:

```bash
make spin
```

Reset the active backend without starting the app. `fresh` is reset-only; it
does not build or serve the application:

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
| `make wasmtime db=postgres` | PostgreSQL over TCP. |
| `make spin db=postgres` | Spin PostgreSQL connector. |
| `make wasmtime db=postgres realtime=redis` | PostgreSQL durable store with Redis wake notifications. |
| `make spin db=postgres realtime=redis` | Spin PostgreSQL connector with Redis wake notifications. |
| `make wasmtime db=neon` | Neon HTTP SQL helper. |
| `make wasmtime db=neon realtime=redis` | Neon durable store with Redis wake notifications. |
| `make spin db=neon realtime=redis` | Spin-hosted app using Neon durable store with Redis wake notifications. |
| `make wasmtime db=supabase` | Supabase RPC helper. |
| `make spin db=supabase` | Spin-hosted app using the Supabase RPC helper. |
| `make wasmtime db=supabase realtime=redis` | Supabase durable store with Redis wake notifications. |
| `make spin db=supabase realtime=redis` | Spin-hosted app using Supabase with Redis wake notifications. |
| `make wasmtime db=turso` | Turso/LibSQL Hrana HTTP helper. |
| `make wasmtime db=turso realtime=redis` | Turso durable store with Redis wake notifications. |
| `make spin db=turso realtime=redis` | Spin-hosted app using Turso durable store with Redis wake notifications. |
| `make wasmtime db=mysql` | Raw TCP MySQL helper using `wasi-mysql`. |
| `make spin db=mysql realtime=polling` | Spin SDK MySQL helper using `spin-mysql`. |
| `make wasmtime db=mysql realtime=redis` | Raw TCP MySQL with Redis wake notifications. |
| `make spin db=mysql realtime=redis` | Spin SDK MySQL with Redis wake notifications. |
| `make wasmtime db=redis realtime=redis` | Experimental Redis event store with SSE notifications. |
| `make spin db=redis realtime=redis` | Experimental Spin Redis event store with SSE notifications. |

Reset examples:

```bash
make db=sqlite fresh
make db=postgres fresh
make db=neon fresh
make db=supabase fresh
make db=turso fresh
make db=mysql fresh
make db=redis fresh
```

`db=mysql` requires `MYSQL_URL`. `DATABASE_URL` is an internal runtime value
derived by the Makefile, not a public fallback.

```bash
make db=mysql fresh
make wasmtime db=mysql
make spin db=mysql realtime=polling
make spin db=mysql realtime=redis
```

`db=redis` uses `REDIS_URL`, defaulting to `redis://127.0.0.1:6379`.

```bash
redis-cli -u redis://127.0.0.1:6379 ping
make db=redis fresh
make wasmtime db=redis realtime=redis
```

## Realtime Modes

The Makefile accepts `realtime=off`, `realtime=polling`, and `realtime=redis`.
Use `make help-realtime` to print the current list.

`realtime=redis` is a wake/notification transport. It is supported with every
supported `db` backend:

```bash
make wasmtime db=sqlite realtime=redis
make spin db=sqlite realtime=redis
make wasmtime db=postgres realtime=redis
make spin db=postgres realtime=redis
make wasmtime db=neon realtime=redis
make spin db=neon realtime=redis
make wasmtime db=supabase realtime=redis
make spin db=supabase realtime=redis
make wasmtime db=turso realtime=redis
make spin db=turso realtime=redis
make wasmtime db=mysql realtime=redis
make spin db=mysql realtime=redis
make wasmtime db=redis realtime=redis
make spin db=redis realtime=redis
```

`db=redis` is different: it uses Redis as the durable event store, checkpoint
store, and read-model store. `make ... db=redis realtime=redis` uses Redis for
both persistence and wake notifications.

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
last seen sequence. Redis wake delivery is not an exactly-once delivery
guarantee; duplicate or missed wake messages must be harmless because the SSE
route always replays durable state by sequence.

With `realtime=redis`, the SSE route uses Redis as the wake transport. Each
browser request registers a short-TTL Redis list queue. After a command commits
and projections update, the publisher sends a notification to `REDIS_CHANNEL`
and fans a wake message out to every live queue. The stream then re-reads
durable state after `last_sequence`, emits one `counter` event, and keeps the
SSE connection open for later events.

Idle streams do not reconnect repeatedly. On Spin, the handler waits inside
`BRPOP` for up to 25 seconds, emits one SSE comment keepalive with a 1 second
EventSource retry interval, and continues waiting. On Wasmtime, the handler
uses `RPOP` with WASI async sleeps so the component can continue serving normal
HTTP requests while waiting for Redis wake messages. The route intentionally
does not run a blocking Redis `SUBSCRIBE`; it uses the existing outbound Redis
command path so Spin and Wasmtime can share the same browser delivery behavior.

On Spin, any `make spin ... realtime=redis` command uses `spin.redis.toml` and
starts a separate Redis trigger component subscribed to `REDIS_CHANNEL`. The
trigger validates realtime JSON and writes smoke-test health markers:

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
| `POSTGRES_URL` | PostgreSQL URL for `db=postgres` |
| `NEON_DB_URL` | Neon Postgres HTTP SQL URL for `db=neon` |
| `SUPABASE_URL` | Supabase project API URL for `db=supabase` |
| `SUPABASE_SECRET_KEY` | Supabase service role key for `db=supabase` |
| `TURSO_URL` | Turso remote database or local sqld HTTP URL for `db=turso` |
| `TURSO_AUTH_TOKEN` | Turso auth token; leave empty for local sqld development |
| `MYSQL_URL` | MySQL URL, for example `mysql://user:password@127.0.0.1:3306/counter_app` |
| `REDIS_URL` | Redis URL, default `redis://127.0.0.1:6379` |
| `REDIS_CHANNEL` | Redis notification channel, default `counter-events` |

`make db=<backend>` and `make realtime=<mode>` override `.env` for one
command. The Makefile derives the internal `DATABASE_URL` and
`DATABASE_AUTH_TOKEN` values from the backend-specific variables above. Do not
set `DATABASE_URL` or `DATABASE_AUTH_TOKEN` in `.env` for normal counter-app
workflows.

## Runtime Setup Details

The Makefile is the supported entrypoint and sets these values for you. If you
copy this example into another Spin or Wasmtime project, mirror the runtime
boundary below.

Backend URL derivation:

| `db` | Public variable | Runtime values passed to the component |
| :--- | :--- | :--- |
| `sqlite` | none | Wasmtime mounts `./data`; Spin uses the `default` SQLite store. |
| `postgres` | `POSTGRES_URL` | `DATABASE_URL=$POSTGRES_URL` |
| `neon` | `NEON_DB_URL` | `DATABASE_URL=$NEON_DB_URL` |
| `supabase` | `SUPABASE_URL`, `SUPABASE_SECRET_KEY` | `DATABASE_URL=$SUPABASE_URL`, `DATABASE_AUTH_TOKEN=$SUPABASE_SECRET_KEY` |
| `turso` | `TURSO_URL`, `TURSO_AUTH_TOKEN` | `DATABASE_URL=$TURSO_URL`, `DATABASE_AUTH_TOKEN=$TURSO_AUTH_TOKEN` |
| `mysql` | `MYSQL_URL` | `DATABASE_URL=$MYSQL_URL` |
| `redis` | `REDIS_URL` | `REDIS_URL=$REDIS_URL` |

For `realtime=redis`, also pass `REALTIME_BACKEND=redis`, `REDIS_URL`, and
`REDIS_CHANNEL`. Redis realtime is notification-only unless `db=redis` is also
selected.

Spin setup:

- Use `spin.toml` for `realtime=off` or `realtime=polling`.
- Use `spin.redis.toml` for `realtime=redis`; it starts the HTTP component and
  the Redis trigger smoke-test component.
- The HTTP component needs outbound permission for every backend family it can
  call:

```toml
allowed_outbound_hosts = [
  "*://*.turso.io:*",
  "*://*.neon.tech:*",
  "*://*.supabase.co:*",
  "*://localhost:*",
  "*://127.0.0.1:*",
  "postgres://*:*",
  "postgresql://*:*",
  "mysql://*:*",
  "redis://*:*",
  "rediss://*:*",
]
```

Spin also needs the host stores declared for the default local paths:

```toml
key_value_stores = ["default"]
sqlite_databases = ["default"]
```

When `realtime=redis`, `spin.redis.toml` exposes Redis trigger variables:

```toml
[variables]
redis_url = { default = "redis://127.0.0.1:6379" }
redis_channel = { default = "counter-events" }

[application.trigger.redis]
address = "{{ redis_url }}"
```

Wasmtime setup:

- Mount the generated browser assets at `/`.
- Mount local JSON-file state at `/data` for `db=sqlite`.
- Enable Preview 3, HTTP, TCP, inherited networking, and DNS lookup.
- Pass the same runtime env vars that the Makefile derives.
- Add `DATABASE_URL` only for `db=postgres`, `db=neon`, `db=supabase`,
  `db=turso`, or `db=mysql`.
- Add `DATABASE_AUTH_TOKEN` only for Supabase or Turso when a token is needed.
- Add `REDIS_URL` and `REDIS_CHANNEL` only for `db=redis` or
  `realtime=redis`.

The Makefile runs the component with this shape:

```bash
wasmtime serve \
  -W component-model-async=y \
  -S p3=y \
  -S cli=y \
  -S http=y \
  -S tcp=y \
  -S inherit-network=y \
  -S allow-ip-name-lookup=y \
  --dir=./target/site/pkg::/ \
  --dir=./data::/data \
  --env=LEPTOS_OUTPUT_NAME=counter_app \
  --env=LEPTOS_SITE_ROOT=/ \
  --env=LEPTOS_SITE_PKG_DIR=pkg \
  --env=DATABASE_BACKEND=<sqlite|postgres|neon|supabase|turso|mysql|redis> \
  --env=REALTIME_BACKEND=<off|polling|redis> \
  --addr 127.0.0.1:3000 \
  target/wasmtime/wasm32-wasip2/release/counter_app.wasm
```

When bypassing Make, append the optional backend flags from the derivation table
above yourself.

## Runtime Notes

Static files from `./target/site/pkg/` are mapped by Wasmtime to `/` inside the
guest. The app implements the WASI HTTP handler and runs as a WebAssembly
component using the Preview 3 async ABI.
