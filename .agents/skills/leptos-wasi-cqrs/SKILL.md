---
name: leptos-wasi-cqrs
description: Guidance for building full-stack Event Sourced (CQRS) applications using ddd_cqrs_es and Leptos WASI (via leptos_wasi) on Fermyon Spin or generic Wasmtime runtimes.
---

# Leptos WASI + CQRS/ES Integration Skill

This skill provides step-by-step instructions and reference patterns for an AI agent to build, modify, debug, and expand full-stack Event Sourced (CQRS) applications integrating the `ddd_cqrs_es` framework with the `leptos_wasi` component model server on both **Fermyon Spin** and **generic Wasmtime** runtimes. The current counter app supports `sqlite`, `postgres`, `neon`, `supabase`, `turso`, `mysql`, and `redis` through the Makefile.

---

## 🗺️ High-Level Architectural Flow

In this architecture, incoming write operations (Commands) are dispatched via **Leptos Server Functions**, validated by the pure business domain (Aggregate), and persisted as immutable history (Events) in the Event Store. The server action returns a unified read view immediately, then projections and realtime notifications catch up from durable event history:

```mermaid
sequenceDiagram
    autonumber
    actor User as 🌐 Browser Client
    participant Client as 🖥️ Leptos Client (WASM)
    participant Server as ⚙️ Leptos Server (WASI)
    participant Domain as 🧠 Aggregate (Domain)
    database EventDB as 🗄️ Event Store
    database ReadDB as 📊 Read Model
    
    User->>Client: Clicks command button
    Client->>Server: HTTP POST /api/command_name (Server Function)
    Server->>EventDB: Fetch historical Event Stream for Aggregate ID
    EventDB-->>Server: [Historical Events...]
    Server->>Domain: Replay events to reconstruct current aggregate state
    Server->>Domain: Validate Command against current state invariants
    Domain-->>Server: Ok([New Committed Events])
    Server->>EventDB: Append new events (Optimistic Concurrency Control)
    Server->>Server: Build CounterViewDto from authoritative state
    Server-->>Client: HTTP 200 OK with updated view
    Server->>ReadDB: Catch up read model/checkpoint from durable events
    Server-->>Client: SSE wake/update for other sessions when realtime is enabled
```

---

## Concept Map for Agents

When working in this repo, keep these boundaries separate:

- `domain.rs`: pure commands, events, IDs, errors, and `Aggregate` implementations. No database, HTTP, Redis, Leptos, logging side effects, or runtime-specific APIs belong here.
- `app.rs`: Leptos UI, server functions, DTOs, optimistic UI reconciliation, and the server-side command orchestration that calls the repository.
- `store.rs`: backend selection, schema initialization, event-store/checkpoint/read-model adapters, projection catch-up, SSE stream response, and Redis wake publishing.
- `server.rs`: WASI HTTP routing, static file serving, one-time schema initialization before dynamic requests, manual `/api/counter/stream` routing, and Leptos server-function registration.
- `Makefile`: canonical build/run/reset entrypoint. Do not invent alternate public commands; validate with `make help`, `make help-db`, `make help-realtime`, and `make help-matrix`.

Current API names to prefer:

- Aggregate replay from envelopes: `Aggregate::replay`.
- Raw aggregate unit-test replay: `Aggregate::replay_raw_events_from_zero`.
- Command execution that returns updated aggregate state: `execute_returning_state`.
- Production SQL request idempotency: `execute_idempotent_atomic` / async `execute_idempotent_atomic` with SQLite, PostgreSQL, or MySQL event stores.
- Unified counter read call: `get_counter_view` / `CounterViewDto`.
- Realtime transport: SSE/EventSource in the browser; Redis is a wake/notification transport unless `db=redis` is selected.

Avoid stale APIs and command names:

- Do not implement `Aggregate::id()`; it was removed.
- Do not call `Aggregate::replay_events`; use `replay_raw_events_from_zero` for raw event tests.
- Do not use `NewEvent::from_domain_event`; use `NewEvent::new`.
- Do not treat `EventType` as a `String`; use `as_str()`, `into_string()`, `Display`, or `From<&str>` / `From<String>`.
- Do not document `db=libsql`, `db=psql`, `NEON_URL`, or `realime=redis`.

## 🧠 1. Pure Domain Design Pattern (`domain.rs`)

The business domain must remain completely decoupled from databases, networking, or frameworks. Define pure state-machine commands, events, and validations using the framework's `Aggregate` trait:

```rust
use serde::{Deserialize, Serialize};
use ddd_cqrs_es::{Aggregate, DomainEvent};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EntityId(pub String);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum EntityCommand {
    DoAction { arg: i32 },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum EntityEvent {
    ActionDone { arg: i32 },
}

impl DomainEvent for EntityEvent {
    fn event_type(&self) -> &'static str {
        match self {
            EntityEvent::ActionDone { .. } => "action_done",
        }
    }
}

pub struct EntityAggregate {
    pub id: EntityId,
    pub state_val: i32,
    pub revision: u64,
}

impl Aggregate for EntityAggregate {
    type Id = EntityId;
    type Command = EntityCommand;
    type Event = EntityEvent;
    type Error = String;

    fn aggregate_type() -> &'static str { "entity" }
    fn revision(&self) -> u64 { self.revision }
    fn new() -> Self {
        Self {
            id: EntityId(String::new()),
            state_val: 0,
            revision: 0,
        }
    }

    fn apply(&mut self, event: &Self::Event) {
        match event {
            EntityEvent::ActionDone { arg } => {
                self.state_val += arg;
            }
        }
        self.revision += 1;
    }

    fn handle(&self, command: Self::Command) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            EntityCommand::DoAction { arg } => {
                if arg <= 0 {
                    return Err("Argument must be positive".to_string());
                }
                Ok(vec![EntityEvent::ActionDone { arg }])
            }
        }
    }
}
```

Aggregate rules:

- `handle` validates current state and returns events; it must not mutate state or call infrastructure.
- `apply` mutates state from already-decided facts; it must be deterministic and side-effect free.
- `revision()` must reflect replayed stream revision. Increment it in `apply` when the aggregate stores a revision field.
- Aggregate identity is passed to the repository. The trait no longer asks the aggregate for an ID.
- `DomainEvent::event_type()` should return a stable wire/storage name. Persisted envelopes wrap it as the `EventType` newtype.

---

## 💾 2. Multi-Runtime WASI Persistence (`store.rs`)

The counter app uses `MultiBackendEventStore`, checkpoint helpers, and read-model helpers to select storage from runtime environment variables set by the Makefile. Backend-specific code must stay feature/cfg gated so both Spin and Wasmtime compile:

- `db=sqlite`: Spin SQLite host store on Spin; JSON files mounted at `/data` on Wasmtime.
- `db=postgres`: PostgreSQL URL from `POSTGRES_URL`, passed internally as `DATABASE_URL`.
- `db=neon`: Neon HTTP SQL URL from `NEON_DB_URL`.
- `db=supabase`: Supabase REST URL/key from `SUPABASE_URL` and `SUPABASE_SECRET_KEY`.
- `db=turso`: Turso/LibSQL Hrana URL/token from `TURSO_URL` and `TURSO_AUTH_TOKEN`; public command name remains `db=turso`.
- `db=mysql`: raw TCP MySQL on Wasmtime with `wasi-mysql`; Spin host MySQL with `spin-mysql`.
- `db=redis`: Redis is the durable event/checkpoint/read-model store.

`realtime=redis` is independent of the durable backend. It uses Redis as notification/wake transport, then the SSE route replays durable events after the client's `last_sequence`.

For production examples that use the native SQL adapters directly, prefer `Repository::execute_idempotent_atomic` or `AsyncRepository::execute_idempotent_atomic` over the portable `execute_idempotent` path. The portable API is still valid for demos and non-strict workloads, but it coordinates separate stores and should not be described as crash-atomic.

### 2.1 Database Query and Consistency Rules

Before adding or changing SQL, match each query to the access pattern documented in `docs/production/db-query-patterns.md`:

- Aggregate stream loads use `WHERE aggregate_type = ? AND aggregate_id = ? ORDER BY revision ASC`; the `UNIQUE (aggregate_type, aggregate_id, revision)` constraint is the required access path.
- Aggregate-scoped global replay uses `WHERE aggregate_type = ? AND sequence > ? ORDER BY sequence ASC`; schemas need an `(aggregate_type, sequence)` index.
- Point stores use primary keys: checkpoints by `projection_name`, idempotency by `idempotency_key`, snapshots by `(aggregate_type, aggregate_id)`, and read models by their application key.
- Do not query event `payload` JSON for product screens. Project business fields into a read model and index that read model for the UI query.
- Do not add duplicate indexes that mirror a unique constraint unless an actual query planner check proves a need.
- Checkpoint saves must be monotonic. A stale checkpoint write must not move `sequence` backward.
- Projection catch-up from durable events can be expensive. Production examples must use `run_batch(...)`, `ProjectionBatchConfig`, or `load_global_after_limited(...)`; reserve unbounded `run(...)` / `load_global_after(...)` for small tests or explicit maintenance jobs.

When explaining eventual consistency, state both sides plainly: it keeps command writes small and read models scalable, but read models and realtime notifications can lag, duplicate, or arrive out of order. Redis/SSE/WebSocket are wake transports; durable replay by sequence remains the truth. For optimistic UI, never let an older server-action or SSE snapshot rewind a newer visible sequence.

### 2.2 Event Store Implementation Pattern

```rust
pub struct MultiBackendEventStore<A> {
    _phantom: std::marker::PhantomData<fn() -> A>,
}

impl<A> MultiBackendEventStore<A>
where
    A: ddd_cqrs_es::Aggregate + 'static,
{
    pub fn new() -> Self {
        Self { _phantom: std::marker::PhantomData }
    }
}

// Dispatch in EventStore/AsyncEventStore impls by get_backend():
// sqlite, postgres, neon, supabase, turso, mysql, redis.
// Keep runtime-specific imports behind feature/cfg gates.
```

Schema initialization belongs in `initialize_schema_async()` and must be guarded by an async lock/once guard. Do not run migrations from every command handler. `make db=<backend> fresh` is reset-only and never starts the server.

---

## ⚡ 3. Server-Side Integration & Command Runner (`app.rs`)

Handle write commands atomically using a unified server-side handler. Use `AsyncRepository::execute_returning_state` so the command path loads the aggregate once, appends events, and returns the authoritative aggregate state for the response. The read model can catch up afterward from durable events.

In Leptos, protect server-only SSR imports behind `#[cfg(feature = "ssr")]`:

```rust
#[cfg(feature = "ssr")]
async fn run_cqrs_command(
    command: crate::domain::CounterCommand,
) -> Result<CounterViewDto, ServerFnError> {
    use crate::domain::{Counter, CounterId};
    use crate::store::MultiBackendEventStore;
    use ddd_cqrs_es::AsyncRepository;

    let event_store = MultiBackendEventStore::<Counter>::new();
    let repository = AsyncRepository::new(event_store);
    let aggregate_id = CounterId("global".to_string());

    const COMMAND_CONCURRENCY_RETRIES: usize = 6;
    let mut attempts = 0;
    let (loaded, committed_events) = loop {
        match repository
            .execute_returning_state(
                &aggregate_id,
                command.clone(),
                ddd_cqrs_es::Metadata::default(),
            )
            .await
        {
            Ok(outcome) => break outcome,
            Err(error)
                if attempts < COMMAND_CONCURRENCY_RETRIES
                    && is_retryable_counter_write_conflict(&error) =>
            {
                attempts += 1;
                wasip3::clocks::monotonic_clock::wait_for(attempts as u64 * 5_000_000).await;
            }
            Err(error) => return Err(ServerFnError::new(error.to_string())),
        }
    }

    let mut view = get_counter_view_db().await?;
    view.count = loaded.state.value;
    if let Some(last_sequence) = committed_events.last().and_then(|event| event.sequence) {
        view.last_sequence = last_sequence;
    }

    crate::store::publish_counter_realtime(&view).await;
    if let Err(error) = crate::store::catch_up_counter_projection().await {
        eprintln!("failed to catch up counter projection: {error}");
    }

    Ok(view)
}
```

Consolidate separate read calls into a single server function returning a unified view state to minimize browser network round-trips:

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventLogDto {
    pub sequence: u64,
    pub event_type: String,
    pub revision: u64,
    pub payload: String,
    pub recorded_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CounterViewDto {
    pub count: i32,
    pub latest_events: Vec<EventLogDto>,
    pub last_sequence: u64,
    pub realtime_enabled: bool,
}

#[server(prefix = "/api")]
pub async fn get_counter_view() -> Result<CounterViewDto, ServerFnError> {
    #[cfg(feature = "ssr")]
    {
        get_counter_view_db().await
    }
    #[cfg(not(feature = "ssr"))]
    {
        unreachable!()
    }
}
```

For optimistic UI, dispatch server actions directly from button handlers and update a local count immediately. Track `pending_until_sequence` so older server-action or SSE snapshots cannot rewind the visible count during bursty clicks. Buttons should not be disabled just because one command is in flight.

---

## 🌐 4. WASI Server Handler & Boot Migrations (`server.rs`)

Database schema creation statements (`CREATE TABLE IF NOT EXISTS`) are blocking and prone to transaction conflicts under concurrency. Keep migrations out of hot command handlers. Run `initialize_schema_async()` once for dynamic requests using the store-level async guard, and skip it for static `/pkg/` assets. The counter SSE route is not a Leptos server function; route `/api/counter/stream` to `counter_stream_response` before the Leptos handler.

```rust
use leptos_wasi::prelude::Handler;
use wasip3::http::types::{Request, Response, ErrorCode};
use crate::app::{shell, App, GetCounterView, IncrementCount, DecrementCount, ResetCount};

struct LeptosServer;

impl wasip3::exports::http::handler::Guest for LeptosServer {
    async fn handle(request: Request) -> Result<Response, ErrorCode> {
        let _ = init_wasip3_spawner();
        let req = wasip3::http_compat::http_from_wasi_request(request)?;
        let request_path = req.uri().path().to_string();

        if !request_path.starts_with("/pkg/")
            && let Err(e) = crate::store::initialize_schema_async().await {
                eprintln!("Error executing boot schema migrations: {:?}", e);
                return Err(ErrorCode::InternalError(None));
            }

        if request_path == "/api/counter/stream" {
            let response = crate::store::counter_stream_response(&req)
                .await
                .map_err(|e| {
                    eprintln!("Error building counter stream response: {:?}", e);
                    ErrorCode::InternalError(None)
                })?;
            return wasip3::http_compat::http_into_wasi_response(response);
        }

        let conf = get_configuration(None).unwrap();
        let leptos_options = conf.leptos_options;

        let wasi_res = Handler::build(req).await
            .map_err(|e| ErrorCode::InternalError(None))?
            .static_files_handler("/pkg", serve_static_files)
            .with_server_fn::<GetCounterView>()
            .with_server_fn::<IncrementCount>()
            .with_server_fn::<DecrementCount>()
            .with_server_fn::<ResetCount>()
            .generate_routes(App)
            .handle_with_context(move || shell(leptos_options.clone()), || {})
            .await
            .map_err(|e| ErrorCode::InternalError(None))?;

        Ok(wasi_res)
    }
}

wasip3::http::service::export!(LeptosServer);
```

SSE/Redis rules:

- Browser realtime uses SSE/EventSource at `/api/counter/stream?last_sequence=...`.
- `realtime=polling` uses durable event-store catch-up.
- `realtime=redis` uses Redis wake queues/pub/sub, then still replays durable events by sequence.
- Spin `realtime=redis` also starts a Redis trigger sidecar from `spin.redis.toml`; it is a smoke-test subscriber and does not own browser delivery or projections.
- Do not add a manual `Connection: keep-alive` header to WASI SSE responses.

---

## Agent Workflow Checklist

Before editing this app:

1. Read `examples/counter-app/Makefile`, `src/domain.rs`, `src/app.rs`, `src/store.rs`, and `src/server.rs` for current wiring.
2. Use `make help`, `make help-db`, `make help-realtime`, and `make help-matrix` as the public command source of truth.
3. If changing backend/realtime behavior, update `examples/counter-app/README.md`, `docs/tutorial/leptos-ssr.md`, `docs/production/redis.md`, and this skill together.
4. Validate docs commands against the Makefile's `validate-params` target rather than assuming a command is supported.
5. For runtime compile checks, match the Makefile shape with `WASI_RUNTIME=spin` or `WASI_RUNTIME=wasmtime`; one runtime cfg does not prove the other.

## 🛠️ 5. Build, Test & Execute Commands

Execute the following commands from `examples/counter-app` to compile, build, and run the microservices. Start with `make help`, `make help-db`, `make help-realtime`, or `make help-matrix` to see supported runtime/database/realtime combinations.

### 5.1 Fermyon Spin Runtime
Builds the CSS/WASM and runs the Spin host:
```bash
make spin
make spin db=postgres
make spin db=postgres realtime=redis
make spin db=neon realtime=redis
make spin db=supabase
make spin db=supabase realtime=redis
make spin db=turso realtime=redis
make spin db=mysql realtime=polling
make spin db=mysql realtime=redis
make spin db=redis realtime=redis
```

### 5.2 Generic Wasmtime Runtime
Builds CSS/WASM and serves using generic wasmtime sandbox capabilities:
```bash
make wasmtime
make wasmtime db=postgres
make wasmtime db=postgres realtime=redis
make wasmtime db=neon
make wasmtime db=neon realtime=redis
make wasmtime db=supabase
make wasmtime db=supabase realtime=redis
make wasmtime db=turso
make wasmtime db=turso realtime=redis
make wasmtime db=mysql
make wasmtime db=mysql realtime=redis
make wasmtime db=redis realtime=redis
```

`realtime=redis` is a wake/notification transport and can be paired with MySQL
or another durable backend. `db=redis` means Redis is also the durable
event/checkpoint/read-model store.

### 5.3 Reset Without Serving
Reset the selected backend schema and return without launching the app:
```bash
make db=sqlite fresh
make db=postgres fresh
make db=neon fresh
make db=supabase fresh
make db=turso fresh
make db=mysql fresh
make db=redis fresh
```
