use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;
#[cfg(runtime_wasmtime)]
use ddd_cqrs_es::AsyncCheckpointStore;
use ddd_cqrs_es::{Aggregate, EventEnvelope, EventId, ExpectedRevision, NewEvent};
use ddd_cqrs_es::error::EventStoreError;
use ddd_cqrs_es::async_api::AsyncEventStore;
use async_trait::async_trait;

// #[cfg(feature = "postgres")]
// pub use ddd_cqrs_es::{PostgresEventStore, PostgresCheckpointStore};

static SCHEMA_INITIALIZED: AtomicBool = AtomicBool::new(false);
static SCHEMA_INIT_LOCK: OnceLock<futures::lock::Mutex<()>> = OnceLock::new();

// =========================================================================
// ENVIRONMENT CONFIGURATION HELPERS
// =========================================================================

pub fn get_backend() -> String {
    std::env::var("DATABASE_BACKEND").unwrap_or_else(|_| "sqlite".to_string())
}

fn env_non_empty(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|s| !s.is_empty())
}

pub fn get_postgres_url() -> String {
    let backend = get_backend();
    match backend.as_str() {
        "supabase" => {
            return env_non_empty("SUPABASE_URL")
                .or_else(|| env_non_empty("DATABASE_URL"))
                .unwrap_or_default();
        }
        "neon" => {
            return env_non_empty("DATABASE_URL")
                .or_else(|| env_non_empty("NEON_DB_URL"))
                .unwrap_or_default();
        }
        _ => {}
    }

    env_non_empty("DATABASE_URL")
        .or_else(|| env_non_empty("POSTGRES_URL"))
        .unwrap_or_else(|| "postgresql://postgres:postgres@localhost:5432/postgres".to_string())
}

pub fn get_mysql_url() -> String {
    env_non_empty("MYSQL_URL")
        .or_else(|| env_non_empty("DATABASE_URL"))
        .unwrap_or_default()
}

pub fn get_supabase_secret_key() -> Option<String> {
    env_non_empty("SUPABASE_SECRET_KEY")
        .or_else(|| env_non_empty("DATABASE_AUTH_TOKEN"))
}

pub fn get_turso_url() -> String {
    env_non_empty("DATABASE_URL")
        .or_else(|| env_non_empty("TURSO_URL"))
        .unwrap_or_else(|| "http://127.0.0.1:8080".to_string())
}

pub fn get_turso_auth_token() -> Option<String> {
    env_non_empty("DATABASE_AUTH_TOKEN").or_else(|| env_non_empty("TURSO_AUTH_TOKEN"))
}

pub fn get_redis_url() -> String {
    env_non_empty("REDIS_URL").unwrap_or_else(|| "redis://127.0.0.1:6379".to_string())
}

pub fn get_redis_channel() -> String {
    env_non_empty("REDIS_CHANNEL").unwrap_or_else(|| "counter-events".to_string())
}

#[cfg(feature = "redis")]
#[allow(dead_code)]
fn redis_realtime_suffix() -> String {
    hex_encode(get_redis_channel().as_bytes())
}

#[cfg(feature = "redis")]
#[allow(dead_code)]
fn redis_realtime_subscribers_key() -> String {
    format!("counter:realtime:{}:subscribers", redis_realtime_suffix())
}

#[cfg(feature = "redis")]
#[allow(dead_code)]
fn redis_realtime_queue_key(subscriber_id: &str) -> String {
    format!(
        "counter:realtime:{}:queue:{}",
        redis_realtime_suffix(),
        subscriber_id
    )
}

#[cfg(feature = "redis")]
#[allow(dead_code)]
fn redis_realtime_alive_key(queue_key: &str) -> String {
    format!("{queue_key}:alive")
}

pub fn get_realtime_backend() -> String {
    env_non_empty("REALTIME_BACKEND").unwrap_or_else(|| "off".to_string())
}

#[cfg(all(feature = "spin-redis", runtime_spin))]
fn redis_client() -> ddd_cqrs_es::SpinRedisClient {
    ddd_cqrs_es::SpinRedisClient::new(get_redis_url())
}

#[cfg(all(feature = "wasi-redis", runtime_wasmtime))]
fn redis_client() -> ddd_cqrs_es::WasiRedisClient {
    ddd_cqrs_es::WasiRedisClient::new(get_redis_url())
}

#[allow(dead_code)]
fn redis_read_model_key(aggregate_id_json: &str) -> String {
    format!("counter:read_model:{}", hex_encode(aggregate_id_json.as_bytes()))
}

#[cfg(feature = "redis")]
#[allow(dead_code)]
fn redis_checkpoint_key(projection_name: &str) -> String {
    format!(
        "ddd_cqrs_es:checkpoint:{}",
        hex_encode(projection_name.as_bytes())
    )
}

#[cfg(feature = "redis")]
#[allow(dead_code)]
const REDIS_COUNTER_PROJECTION_LUA: &str = r#"
local sequence = tonumber(ARGV[1])
local operation = ARGV[2]
local amount = tonumber(ARGV[3])
local current = tonumber(redis.call('GET', KEYS[2]) or '0')

if sequence <= current then
    return {'SKIP', current}
end

if sequence ~= current + 1 then
    return {'ERR', 'checkpoint_gap', current}
end

if operation == 'incr' then
    redis.call('INCRBY', KEYS[1], amount)
elseif operation == 'set' then
    redis.call('SET', KEYS[1], amount)
else
    return {'ERR', 'unknown_operation', current}
end

redis.call('SET', KEYS[2], sequence)
return {'OK', sequence}
"#;

#[allow(dead_code)]
fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

// -------------------------------------------------------------------------
// ROUTED QUERY EXECUTOR
// -------------------------------------------------------------------------

#[allow(unused_variables)]
async fn execute_query_routed(
    sql_sqlite: &str,
    sql_postgres: &str,
    params: Vec<serde_json::Value>,
) -> Result<Vec<serde_json::Value>, String> {
    let backend = get_backend();

    if backend == "mysql" {
        #[cfg(runtime_spin)]
        {
            #[cfg(feature = "spin-mysql")]
            {
                let url = get_mysql_url();
                ddd_cqrs_es::adapters::execute_spin_mysql(&url, sql_sqlite, params).await
            }
            #[cfg(not(feature = "spin-mysql"))]
            {
                Err("spin-mysql feature is not enabled".to_string())
            }
        }
        #[cfg(runtime_wasmtime)]
        {
            #[cfg(feature = "wasi-mysql")]
            {
                let url = get_mysql_url();
                ddd_cqrs_es::adapters::execute_raw_tcp_mysql(&url, sql_sqlite, params)
            }
            #[cfg(not(feature = "wasi-mysql"))]
            {
                Err("wasi-mysql feature is not enabled".to_string())
            }
        }
    } else if backend == "libsql" || backend == "turso" {
        #[cfg(feature = "libsql")]
        {
            let url = get_turso_url();
            let auth = get_turso_auth_token();
            let res = ddd_cqrs_es::adapters::execute_libsql_query(&url, auth.as_deref(), sql_sqlite, params).await?;
            Ok(res.rows)
        }
        #[cfg(not(feature = "libsql"))]
        {
            Err("libsql feature is not enabled".to_string())
        }
    } else if backend == "supabase" {
        #[cfg(feature = "supabase")]
        {
            let url = get_postgres_url();
            let secret = get_supabase_secret_key();
            ddd_cqrs_es::adapters::execute_supabase_query(&url, secret.as_deref(), sql_postgres, params).await
        }
        #[cfg(not(feature = "supabase"))]
        {
            Err("supabase feature is not enabled".to_string())
        }
    } else if backend == "neon" {
        #[cfg(feature = "neon")]
        {
            let url = get_postgres_url();
            ddd_cqrs_es::adapters::execute_neon_query(&url, sql_postgres, params).await
        }
        #[cfg(not(feature = "neon"))]
        {
            Err("neon feature is not enabled".to_string())
        }
    } else {
        // Postgres TCP or Spin PG
        #[cfg(runtime_spin)]
        {
            #[cfg(feature = "postgres")]
            {
                let url = get_postgres_url();
                ddd_cqrs_es::adapters::execute_spin_pg(&url, sql_postgres, params).await
            }
            #[cfg(not(feature = "postgres"))]
            {
                Err("postgres feature is not enabled".to_string())
            }
        }
        #[cfg(runtime_wasmtime)]
        {
            #[cfg(feature = "postgres")]
            {
                let url = get_postgres_url();
                ddd_cqrs_es::adapters::execute_raw_tcp_postgres(&url, sql_postgres, params)
            }
            #[cfg(not(feature = "postgres"))]
            {
                Err("postgres feature is not enabled".to_string())
            }
        }
    }
}



// =========================================================================
// MIGRATIONS AT BOOT (ONCE)
// =========================================================================

pub async fn initialize_schema_async() -> Result<(), String> {
    if SCHEMA_INITIALIZED.load(Ordering::Acquire) {
        return Ok(());
    }

    let lock = SCHEMA_INIT_LOCK.get_or_init(|| futures::lock::Mutex::new(()));
    let _guard = lock.lock().await;

    if SCHEMA_INITIALIZED.load(Ordering::Acquire) {
        return Ok(());
    }

    let backend = get_backend();

    if backend == "redis" {
        #[cfg(feature = "redis")]
        {
            SCHEMA_INITIALIZED.store(true, Ordering::Release);
            return Ok(());
        }
        #[cfg(not(feature = "redis"))]
        {
            return Err("redis feature not enabled".to_string());
        }
    } else if backend == "sqlite" {
        #[cfg(runtime_spin)]
        {
            #[cfg(feature = "sqlite")]
            {
                let sql_events = ddd_cqrs_es::adapters::EVENTS_TABLE_SCHEMA_SQLITE;
                let sql_checkpoints = ddd_cqrs_es::adapters::CHECKPOINTS_TABLE_SCHEMA_SQLITE;
                let sql_read_model = "CREATE TABLE IF NOT EXISTS counter_read_model (id TEXT PRIMARY KEY, value INTEGER NOT NULL);";
                ddd_cqrs_es::adapters::execute_spin_sqlite(sql_events, Vec::new()).await.map_err(|e| e.to_string())?;
                ddd_cqrs_es::adapters::execute_spin_sqlite(sql_checkpoints, Vec::new()).await.map_err(|e| e.to_string())?;
                ddd_cqrs_es::adapters::execute_spin_sqlite(sql_read_model, Vec::new()).await.map_err(|e| e.to_string())?;
            }
            #[cfg(not(feature = "sqlite"))]
            {
                return Err("sqlite feature not enabled".to_string());
            }
        }
        #[cfg(runtime_wasmtime)]
        {
            std::fs::create_dir_all("/data").map_err(|e| e.to_string())?;
        }
    } else if backend == "mysql" {
        #[cfg(any(feature = "spin-mysql", feature = "wasi-mysql"))]
        {
            let sql_events = r#"
                    CREATE TABLE IF NOT EXISTS events (
                        sequence BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                        event_id VARCHAR(36) NOT NULL UNIQUE,
                        aggregate_id VARCHAR(255) NOT NULL,
                        aggregate_type VARCHAR(255) NOT NULL,
                        revision BIGINT UNSIGNED NOT NULL,
                        event_type VARCHAR(255) NOT NULL,
                        event_version INT UNSIGNED NOT NULL,
                        payload LONGTEXT NOT NULL,
                        metadata LONGTEXT NOT NULL,
                        recorded_at_ms BIGINT NOT NULL,
                        UNIQUE KEY idx_aggregate_revision (aggregate_type, aggregate_id, revision),
                        KEY idx_aggregate (aggregate_type, aggregate_id),
                        KEY idx_sequence (sequence)
                    );
                "#;
            let sql_checkpoints = "CREATE TABLE IF NOT EXISTS checkpoints (projection_name VARCHAR(255) PRIMARY KEY, last_sequence BIGINT UNSIGNED NOT NULL);";
            let sql_read_model = "CREATE TABLE IF NOT EXISTS counter_read_model (id VARCHAR(255) PRIMARY KEY, value BIGINT NOT NULL);";

            execute_query_routed(sql_events, sql_events, Vec::new()).await?;
            execute_query_routed(
                "ALTER TABLE events MODIFY payload LONGTEXT NOT NULL, MODIFY metadata LONGTEXT NOT NULL",
                "ALTER TABLE events MODIFY payload LONGTEXT NOT NULL, MODIFY metadata LONGTEXT NOT NULL",
                Vec::new(),
            )
            .await?;
            execute_query_routed(sql_checkpoints, sql_checkpoints, Vec::new()).await?;
            execute_query_routed(sql_read_model, sql_read_model, Vec::new()).await?;
        }
        #[cfg(not(any(feature = "spin-mysql", feature = "wasi-mysql")))]
        {
            return Err("mysql runtime feature not enabled".to_string());
        }
    } else {
        // Postgres or LibSQL
        let (sql_events, sql_checkpoints, sql_read_model) = if backend == "libsql" || backend == "turso" {
            (
                ddd_cqrs_es::adapters::EVENTS_TABLE_SCHEMA_SQLITE,
                ddd_cqrs_es::adapters::CHECKPOINTS_TABLE_SCHEMA_SQLITE,
                "CREATE TABLE IF NOT EXISTS counter_read_model (id TEXT PRIMARY KEY, value INTEGER NOT NULL);",
            )
        } else {
            (
                ddd_cqrs_es::adapters::EVENTS_TABLE_SCHEMA_POSTGRES,
                ddd_cqrs_es::adapters::CHECKPOINTS_TABLE_SCHEMA_POSTGRES,
                "CREATE TABLE IF NOT EXISTS counter_read_model (id VARCHAR(255) PRIMARY KEY, value BIGINT NOT NULL);",
            )
        };

        execute_query_routed(sql_events, sql_events, Vec::new()).await?;
        execute_query_routed(sql_checkpoints, sql_checkpoints, Vec::new()).await?;
        execute_query_routed(sql_read_model, sql_read_model, Vec::new()).await?;
    }

    SCHEMA_INITIALIZED.store(true, Ordering::Release);
    Ok(())
}

// =========================================================================
// MULTI-BACKEND EVENT STORE
// =========================================================================

pub struct MultiBackendEventStore<A> {
    _phantom: PhantomData<fn() -> A>,
}

impl<A> Clone for MultiBackendEventStore<A> {
    fn clone(&self) -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<A> Default for MultiBackendEventStore<A> {
    fn default() -> Self {
        Self::new()
    }
}

impl<A> MultiBackendEventStore<A> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<A> AsyncEventStore<A> for MultiBackendEventStore<A>
where
    A: Aggregate + Send + Sync + 'static,
    A::Event: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + Clone,
    A::Id: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + Clone + PartialEq + std::fmt::Display,
{
    type Error = EventStoreError;

    async fn load(&self, aggregate_id: &A::Id) -> Result<Vec<EventEnvelope<A::Event, A::Id>>, Self::Error> {
        let backend = get_backend();

        if backend == "redis" {
            #[cfg(feature = "redis")]
            {
                #[cfg(any(
                    all(feature = "spin-redis", runtime_spin),
                    all(feature = "wasi-redis", runtime_wasmtime)
                ))]
                {
                    let store = ddd_cqrs_es::RedisEventStore::<A, _>::new(redis_client());
                    return store.load(aggregate_id).await;
                }
                #[cfg(not(any(
                    all(feature = "spin-redis", runtime_spin),
                    all(feature = "wasi-redis", runtime_wasmtime)
                )))]
                {
                    return Err(EventStoreError::Backend(
                        "redis backend requires WASI_RUNTIME=spin or WASI_RUNTIME=wasmtime".to_string(),
                    ));
                }
            }
            #[cfg(not(feature = "redis"))]
            {
                return Err(EventStoreError::Backend("redis feature not enabled".to_string()));
            }
        }

        if backend == "sqlite" {
            #[cfg(runtime_spin)]
            {
                #[cfg(feature = "sqlite")]
                {
                    let query = "SELECT sequence, event_id, aggregate_id, aggregate_type, revision, event_type, event_version, payload, metadata, recorded_at_ms FROM events WHERE aggregate_type = ? AND aggregate_id = ? ORDER BY revision ASC";
                    let agg_id_str = serde_json::to_string(aggregate_id).map_err(|e| EventStoreError::Serialization(e.to_string()))?;
                    let params = vec![
                        serde_json::Value::String(A::aggregate_type().to_string()),
                        serde_json::Value::String(agg_id_str),
                    ];
                    let rows = ddd_cqrs_es::adapters::execute_spin_sqlite(query, params).await
                        .map_err(|e| EventStoreError::Backend(e))?;
                    let mut envelopes = Vec::new();
                    for r in rows {
                        envelopes.push(ddd_cqrs_es::adapters::row_to_envelope::<A::Event, A::Id>(&r).map_err(|e| EventStoreError::Deserialization(e))?);
                    }
                    return Ok(envelopes);
                }
                #[cfg(not(feature = "sqlite"))]
                {
                    return Err(EventStoreError::Backend("sqlite feature not enabled".to_string()));
                }
            }
            #[cfg(runtime_wasmtime)]
            {
                let store = ddd_cqrs_es::adapters::JsonFileEventStore::<A>::new("/data/events.json");
                return AsyncEventStore::load(&store, aggregate_id).await;
            }
        }

        let query_sqlite = if backend == "mysql" {
            "SELECT sequence, event_id, aggregate_id, aggregate_type, revision, event_type, event_version, CAST(payload AS CHAR(10000) CHARACTER SET utf8mb4) AS payload, CAST(metadata AS CHAR(10000) CHARACTER SET utf8mb4) AS metadata, recorded_at_ms FROM events WHERE aggregate_type = ? AND aggregate_id = ? ORDER BY revision ASC"
        } else {
            "SELECT sequence, event_id, aggregate_id, aggregate_type, revision, event_type, event_version, payload, metadata, recorded_at_ms FROM events WHERE aggregate_type = ? AND aggregate_id = ? ORDER BY revision ASC"
        };
        let query_postgres = "SELECT sequence, event_id, aggregate_id, aggregate_type, revision, event_type, event_version, payload, metadata, recorded_at_ms FROM events WHERE aggregate_type = $1 AND aggregate_id = $2 ORDER BY revision ASC";

        let agg_id_str = serde_json::to_string(aggregate_id).map_err(|e| EventStoreError::Serialization(e.to_string()))?;
        let params = vec![
            serde_json::Value::String(A::aggregate_type().to_string()),
            serde_json::Value::String(agg_id_str),
        ];

        let rows = execute_query_routed(query_sqlite, query_postgres, params).await
            .map_err(EventStoreError::Backend)?;

        let mut envelopes = Vec::new();
        for r in rows {
            envelopes.push(ddd_cqrs_es::adapters::row_to_envelope::<A::Event, A::Id>(&r).map_err(EventStoreError::Deserialization)?);
        }
        Ok(envelopes)
    }

    async fn append(
        &self,
        aggregate_id: &A::Id,
        expected_revision: ExpectedRevision,
        events: Vec<NewEvent<A::Event>>,
    ) -> Result<Vec<EventEnvelope<A::Event, A::Id>>, Self::Error> {
        let backend = get_backend();

        if backend == "redis" {
            #[cfg(feature = "redis")]
            {
                #[cfg(any(
                    all(feature = "spin-redis", runtime_spin),
                    all(feature = "wasi-redis", runtime_wasmtime)
                ))]
                {
                    let store = ddd_cqrs_es::RedisEventStore::<A, _>::new(redis_client());
                    return store.append(aggregate_id, expected_revision, events).await;
                }
                #[cfg(not(any(
                    all(feature = "spin-redis", runtime_spin),
                    all(feature = "wasi-redis", runtime_wasmtime)
                )))]
                {
                    return Err(EventStoreError::Backend(
                        "redis backend requires WASI_RUNTIME=spin or WASI_RUNTIME=wasmtime".to_string(),
                    ));
                }
            }
            #[cfg(not(feature = "redis"))]
            {
                return Err(EventStoreError::Backend("redis feature not enabled".to_string()));
            }
        }

        if backend == "sqlite" {
            #[cfg(runtime_spin)]
            {
                #[cfg(feature = "sqlite")]
                {
                    // In spin SQLite, we query current revision first
                    let query_rev = "SELECT COALESCE(MAX(revision), 0) as max_rev FROM events WHERE aggregate_type = ? AND aggregate_id = ?";
                    let agg_id_str = serde_json::to_string(aggregate_id).map_err(|e| EventStoreError::Serialization(e.to_string()))?;
                    let params_rev = vec![
                        serde_json::Value::String(A::aggregate_type().to_string()),
                        serde_json::Value::String(agg_id_str.clone()),
                    ];
                    let rows_rev = ddd_cqrs_es::adapters::execute_spin_sqlite(query_rev, params_rev).await
                        .map_err(|e| EventStoreError::Backend(e))?;
                    
                    let current_revision = rows_rev.first()
                        .and_then(|r| r.get("max_rev"))
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);

                    match expected_revision {
                        ExpectedRevision::Any => {}
                        ExpectedRevision::NoStream if current_revision == 0 => {}
                        ExpectedRevision::NoStream => {
                            return Err(EventStoreError::Concurrency(ddd_cqrs_es::ConcurrencyError::StreamAlreadyExists));
                        }
                        ExpectedRevision::Exact(expected) if expected == current_revision => {}
                        ExpectedRevision::Exact(_) => {
                            return Err(EventStoreError::Concurrency(ddd_cqrs_es::ConcurrencyError::WrongExpectedRevision {
                                expected: expected_revision,
                                actual: current_revision,
                            }));
                        }
                    }

                    let mut envelopes = Vec::new();
                    let now = std::time::SystemTime::now();
                    let now_ms = now.duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64;

                    for (i, event) in events.into_iter().enumerate() {
                        let revision = current_revision + i as u64 + 1;
                        let event_id = EventId::new();

                        let insert_query = "INSERT INTO events (event_id, aggregate_id, aggregate_type, revision, event_type, event_version, payload, metadata, recorded_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING sequence";
                        let payload_str = serde_json::to_string(&event.payload).map_err(|e| EventStoreError::Serialization(e.to_string()))?;
                        let metadata_str = serde_json::to_string(&event.metadata).map_err(|e| EventStoreError::Serialization(e.to_string()))?;
                        let params_insert = vec![
                            serde_json::Value::String(event_id.to_string()),
                            serde_json::Value::String(agg_id_str.clone()),
                            serde_json::Value::String(A::aggregate_type().to_string()),
                            serde_json::Value::Number(revision.into()),
                            serde_json::Value::String(event.event_type.clone()),
                            serde_json::Value::Number(event.event_version.into()),
                            serde_json::Value::String(payload_str),
                            serde_json::Value::String(metadata_str),
                            serde_json::Value::Number(now_ms.into()),
                        ];

                        let insert_rows = ddd_cqrs_es::adapters::execute_spin_sqlite(insert_query, params_insert).await
                            .map_err(|e| EventStoreError::Backend(e))?;

                        let sequence = insert_rows.first()
                            .and_then(|r| r.get("sequence"))
                            .and_then(|v| {
                                if let Some(u) = v.as_u64() {
                                    Some(u)
                                } else if let Some(i) = v.as_i64() {
                                    Some(i as u64)
                                } else {
                                    None
                                }
                            });

                        let envelope = EventEnvelope::new(
                            event_id.clone(),
                            aggregate_id.clone(),
                            A::aggregate_type().to_string(),
                            revision,
                            sequence,
                            event.event_type,
                            event.event_version,
                            event.payload,
                            event.metadata,
                            now,
                        );

                        envelopes.push(envelope);
                    }
                    return Ok(envelopes);
                }
                #[cfg(not(feature = "sqlite"))]
                {
                    return Err(EventStoreError::Backend("sqlite feature not enabled".to_string()));
                }
            }
            #[cfg(runtime_wasmtime)]
            {
                let store = ddd_cqrs_es::adapters::JsonFileEventStore::<A>::new("/data/events.json");
                return AsyncEventStore::append(&store, aggregate_id, expected_revision, events).await;
            }
        }

        let query_sqlite_rev = "SELECT COALESCE(MAX(revision), 0) as max_rev FROM events WHERE aggregate_type = ? AND aggregate_id = ?";
        let query_postgres_rev = "SELECT COALESCE(MAX(revision), 0) as max_rev FROM events WHERE aggregate_type = $1 AND aggregate_id = $2";

        let agg_id_str = serde_json::to_string(aggregate_id).map_err(|e| EventStoreError::Serialization(e.to_string()))?;
        let params_rev = vec![
            serde_json::Value::String(A::aggregate_type().to_string()),
            serde_json::Value::String(agg_id_str.clone()),
        ];

        let rows_rev = execute_query_routed(query_sqlite_rev, query_postgres_rev, params_rev).await
            .map_err(EventStoreError::Backend)?;

        let current_revision = rows_rev.first()
            .and_then(|r| r.get("max_rev"))
            .and_then(|v| {
                if let Some(u) = v.as_u64() {
                    Some(u)
                } else if let Some(i) = v.as_i64() {
                    Some(i as u64)
                } else if let Some(s) = v.as_str() {
                    s.parse::<u64>().ok()
                } else {
                    None
                }
            })
            .unwrap_or(0);

        match expected_revision {
            ExpectedRevision::Any => {}
            ExpectedRevision::NoStream if current_revision == 0 => {}
            ExpectedRevision::NoStream => {
                return Err(EventStoreError::Concurrency(ddd_cqrs_es::ConcurrencyError::StreamAlreadyExists));
            }
            ExpectedRevision::Exact(expected) if expected == current_revision => {}
            ExpectedRevision::Exact(_) => {
                return Err(EventStoreError::Concurrency(ddd_cqrs_es::ConcurrencyError::WrongExpectedRevision {
                    expected: expected_revision,
                    actual: current_revision,
                }));
            }
        }

        let mut envelopes = Vec::new();
        let now = std::time::SystemTime::now();
        let now_ms = now.duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64;

        for (i, event) in events.into_iter().enumerate() {
            let revision = current_revision + i as u64 + 1;
            let event_id = EventId::new();

            let sql_sqlite_insert = "INSERT INTO events (event_id, aggregate_id, aggregate_type, revision, event_type, event_version, payload, metadata, recorded_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING sequence";
            let sql_postgres_insert = "INSERT INTO events (event_id, aggregate_id, aggregate_type, revision, event_type, event_version, payload, metadata, recorded_at_ms) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING sequence";

            let payload_val = serde_json::to_value(&event.payload).map_err(|e| EventStoreError::Serialization(e.to_string()))?;
            let metadata_val = serde_json::to_value(&event.metadata).map_err(|e| EventStoreError::Serialization(e.to_string()))?;

            let params_insert = vec![
                serde_json::Value::String(event_id.to_string()),
                serde_json::Value::String(agg_id_str.clone()),
                serde_json::Value::String(A::aggregate_type().to_string()),
                serde_json::Value::Number(revision.into()),
                serde_json::Value::String(event.event_type.clone()),
                serde_json::Value::Number(event.event_version.into()),
                payload_val,
                metadata_val,
                serde_json::Value::Number(now_ms.into()),
            ];

            let insert_rows = execute_query_routed(sql_sqlite_insert, sql_postgres_insert, params_insert).await
                .map_err(EventStoreError::Backend)?;

            let sequence = insert_rows.first()
                .and_then(|r| r.get("sequence"))
                .and_then(|v| {
                    if let Some(u) = v.as_u64() {
                        Some(u)
                    } else if let Some(i) = v.as_i64() {
                        Some(i as u64)
                    } else if let Some(s) = v.as_str() {
                        s.parse::<u64>().ok()
                    } else {
                        None
                    }
                });

            let envelope = EventEnvelope::new(
                event_id.clone(),
                aggregate_id.clone(),
                A::aggregate_type().to_string(),
                revision,
                sequence,
                event.event_type,
                event.event_version,
                event.payload,
                event.metadata,
                now,
            );

            envelopes.push(envelope);
        }

        Ok(envelopes)
    }

    async fn load_global_after(&self, sequence: Option<u64>) -> Result<Vec<EventEnvelope<A::Event, A::Id>>, Self::Error> {
        let backend = get_backend();
        let seq = sequence.unwrap_or(0);

        if backend == "redis" {
            #[cfg(feature = "redis")]
            {
                #[cfg(any(
                    all(feature = "spin-redis", runtime_spin),
                    all(feature = "wasi-redis", runtime_wasmtime)
                ))]
                {
                    let store = ddd_cqrs_es::RedisEventStore::<A, _>::new(redis_client());
                    return store.load_global_after(sequence).await;
                }
                #[cfg(not(any(
                    all(feature = "spin-redis", runtime_spin),
                    all(feature = "wasi-redis", runtime_wasmtime)
                )))]
                {
                    return Err(EventStoreError::Backend(
                        "redis backend requires WASI_RUNTIME=spin or WASI_RUNTIME=wasmtime".to_string(),
                    ));
                }
            }
            #[cfg(not(feature = "redis"))]
            {
                return Err(EventStoreError::Backend("redis feature not enabled".to_string()));
            }
        }

        if backend == "sqlite" {
            #[cfg(runtime_spin)]
            {
                #[cfg(feature = "sqlite")]
                {
                    let query = "SELECT sequence, event_id, aggregate_id, aggregate_type, revision, event_type, event_version, payload, metadata, recorded_at_ms FROM events WHERE sequence > ? ORDER BY sequence ASC";
                    let params = vec![serde_json::Value::Number(seq.into())];
                    let rows = ddd_cqrs_es::adapters::execute_spin_sqlite(query, params).await
                        .map_err(|e| EventStoreError::Backend(e))?;
                    let mut envelopes = Vec::new();
                    for r in rows {
                        envelopes.push(ddd_cqrs_es::adapters::row_to_envelope::<A::Event, A::Id>(&r).map_err(|e| EventStoreError::Deserialization(e))?);
                    }
                    return Ok(envelopes);
                }
                #[cfg(not(feature = "sqlite"))]
                {
                    return Err(EventStoreError::Backend("sqlite feature not enabled".to_string()));
                }
            }
            #[cfg(runtime_wasmtime)]
            {
                let store = ddd_cqrs_es::adapters::JsonFileEventStore::<A>::new("/data/events.json");
                return AsyncEventStore::load_global_after(&store, Some(seq)).await;
            }
        }

        let query_sqlite = if backend == "mysql" {
            "SELECT sequence, event_id, aggregate_id, aggregate_type, revision, event_type, event_version, CAST(payload AS CHAR(10000) CHARACTER SET utf8mb4) AS payload, CAST(metadata AS CHAR(10000) CHARACTER SET utf8mb4) AS metadata, recorded_at_ms FROM events WHERE sequence > ? ORDER BY sequence ASC"
        } else {
            "SELECT sequence, event_id, aggregate_id, aggregate_type, revision, event_type, event_version, payload, metadata, recorded_at_ms FROM events WHERE sequence > ? ORDER BY sequence ASC"
        };
        let query_postgres = "SELECT sequence, event_id, aggregate_id, aggregate_type, revision, event_type, event_version, payload, metadata, recorded_at_ms FROM events WHERE sequence > $1 ORDER BY sequence ASC";

        let params = vec![serde_json::Value::Number(seq.into())];
        let rows = execute_query_routed(query_sqlite, query_postgres, params).await
            .map_err(EventStoreError::Backend)?;

        let mut envelopes = Vec::new();
        for r in rows {
            envelopes.push(ddd_cqrs_es::adapters::row_to_envelope::<A::Event, A::Id>(&r).map_err(EventStoreError::Deserialization)?);
        }
        Ok(envelopes)
    }
}

// =========================================================================
// CHECKPOINT STORE
// =========================================================================

#[derive(Clone)]
pub struct MultiBackendCheckpointStore;

impl Default for MultiBackendCheckpointStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MultiBackendCheckpointStore {
    pub fn new() -> Self {
        Self
    }

    pub async fn load_checkpoint_async(&self, projection_name: &str) -> Result<Option<u64>, EventStoreError> {
        let backend = get_backend();

        if backend == "redis" {
            #[cfg(feature = "redis")]
            {
                #[cfg(any(
                    all(feature = "spin-redis", runtime_spin),
                    all(feature = "wasi-redis", runtime_wasmtime)
                ))]
                {
                    let store = ddd_cqrs_es::RedisCheckpointStore::new(redis_client());
                    return store.load_checkpoint(projection_name).await;
                }
                #[cfg(not(any(
                    all(feature = "spin-redis", runtime_spin),
                    all(feature = "wasi-redis", runtime_wasmtime)
                )))]
                {
                    return Err(EventStoreError::Backend(
                        "redis checkpoint store requires WASI_RUNTIME=spin or WASI_RUNTIME=wasmtime".to_string(),
                    ));
                }
            }
            #[cfg(not(feature = "redis"))]
            {
                return Err(EventStoreError::Backend("redis feature not enabled".to_string()));
            }
        }

        if backend == "sqlite" {
            #[cfg(runtime_spin)]
            {
                #[cfg(feature = "sqlite")]
                {
                    let query = "SELECT last_sequence FROM checkpoints WHERE projection_name = ?";
                    let params = vec![serde_json::Value::String(projection_name.to_string())];
                    let rows = ddd_cqrs_es::adapters::execute_spin_sqlite(query, params).await
                        .map_err(|e| EventStoreError::Backend(e))?;
                    if let Some(r) = rows.first() {
                        if let Some(val) = r.get("last_sequence") {
                            if let Some(u) = val.as_u64() {
                                return Ok(Some(u));
                            }
                        }
                    }
                    return Ok(None);
                }
                #[cfg(not(feature = "sqlite"))]
                {
                    return Err(EventStoreError::Backend("sqlite feature not enabled".to_string()));
                }
            }
            #[cfg(runtime_wasmtime)]
            {
                let store = ddd_cqrs_es::adapters::JsonFileCheckpointStore::new("/data/checkpoints.json");
                return AsyncCheckpointStore::load_checkpoint(&store, projection_name).await;
            }
        }

        let query_sqlite = "SELECT last_sequence FROM checkpoints WHERE projection_name = ?";
        let query_postgres = "SELECT last_sequence FROM checkpoints WHERE projection_name = $1";

        let params = vec![serde_json::Value::String(projection_name.to_string())];
        let rows = execute_query_routed(query_sqlite, query_postgres, params).await
            .map_err(EventStoreError::Backend)?;

        if let Some(r) = rows.first()
            && let Some(val) = r.get("last_sequence") {
                if let Some(u) = val.as_u64() {
                    return Ok(Some(u));
                } else if let Some(i) = val.as_i64() {
                    return Ok(Some(i as u64));
                } else if let Some(s) = val.as_str()
                    && let Ok(u) = s.parse::<u64>() {
                        return Ok(Some(u));
                    }
            }

        Ok(None)
    }

    pub async fn save_checkpoint_async(&self, projection_name: &str, sequence: u64) -> Result<(), EventStoreError> {
        let backend = get_backend();

        if backend == "redis" {
            #[cfg(feature = "redis")]
            {
                #[cfg(any(
                    all(feature = "spin-redis", runtime_spin),
                    all(feature = "wasi-redis", runtime_wasmtime)
                ))]
                {
                    let store = ddd_cqrs_es::RedisCheckpointStore::new(redis_client());
                    return store.save_checkpoint(projection_name, sequence).await;
                }
                #[cfg(not(any(
                    all(feature = "spin-redis", runtime_spin),
                    all(feature = "wasi-redis", runtime_wasmtime)
                )))]
                {
                    return Err(EventStoreError::Backend(
                        "redis checkpoint store requires WASI_RUNTIME=spin or WASI_RUNTIME=wasmtime".to_string(),
                    ));
                }
            }
            #[cfg(not(feature = "redis"))]
            {
                return Err(EventStoreError::Backend("redis feature not enabled".to_string()));
            }
        }

        if backend == "sqlite" {
            #[cfg(runtime_spin)]
            {
                #[cfg(feature = "sqlite")]
                {
                    let query = "INSERT INTO checkpoints (projection_name, last_sequence) VALUES (?, ?) ON CONFLICT(projection_name) DO UPDATE SET last_sequence = excluded.last_sequence";
                    let params = vec![
                        serde_json::Value::String(projection_name.to_string()),
                        serde_json::Value::Number(sequence.into()),
                    ];
                    ddd_cqrs_es::adapters::execute_spin_sqlite(query, params).await
                        .map_err(|e| EventStoreError::Backend(e))?;
                    return Ok(());
                }
                #[cfg(not(feature = "sqlite"))]
                {
                    return Err(EventStoreError::Backend("sqlite feature not enabled".to_string()));
                }
            }
            #[cfg(runtime_wasmtime)]
            {
                let store = ddd_cqrs_es::adapters::JsonFileCheckpointStore::new("/data/checkpoints.json");
                return AsyncCheckpointStore::save_checkpoint(&store, projection_name, sequence).await;
            }
        }

        if backend == "mysql" {
            let sql_mysql = "INSERT INTO checkpoints (projection_name, last_sequence) VALUES (?, ?) ON DUPLICATE KEY UPDATE last_sequence = VALUES(last_sequence)";
            let params = vec![
                serde_json::Value::String(projection_name.to_string()),
                serde_json::Value::Number(sequence.into()),
            ];

            execute_query_routed(sql_mysql, sql_mysql, params).await
                .map_err(EventStoreError::Backend)?;

            return Ok(());
        }

        let sql_sqlite = "INSERT INTO checkpoints (projection_name, last_sequence) VALUES (?, ?) ON CONFLICT(projection_name) DO UPDATE SET last_sequence = excluded.last_sequence";
        let sql_postgres = "INSERT INTO checkpoints (projection_name, last_sequence) VALUES ($1, $2) ON CONFLICT(projection_name) DO UPDATE SET last_sequence = EXCLUDED.last_sequence";

        let params = vec![
            serde_json::Value::String(projection_name.to_string()),
            serde_json::Value::Number(sequence.into()),
        ];

        execute_query_routed(sql_sqlite, sql_postgres, params).await
            .map_err(EventStoreError::Backend)?;

        Ok(())
    }
}

// =========================================================================
// COUNTER-SPECIFIC READ MODEL & PROJECTION
// =========================================================================

pub struct MultiBackendCounterProjection;

impl Default for MultiBackendCounterProjection {
    fn default() -> Self {
        Self::new()
    }
}

impl MultiBackendCounterProjection {
    pub fn new() -> Self {
        Self
    }

    pub async fn apply_async(&mut self, envelope: &EventEnvelope<crate::domain::CounterEvent, crate::domain::CounterId>) -> Result<(), EventStoreError> {
        let aggregate_id_str = serde_json::to_string(&envelope.aggregate_id)
            .map_err(|e| EventStoreError::Serialization(e.to_string()))?;
            
        let backend = get_backend();

        if backend == "redis" {
            #[cfg(feature = "redis")]
            {
                #[cfg(any(
                    all(feature = "spin-redis", runtime_spin),
                    all(feature = "wasi-redis", runtime_wasmtime)
                ))]
                {
                    use ddd_cqrs_es::RedisCommandExecutor;

                    let client = redis_client();
                    let key = redis_read_model_key(&aggregate_id_str);
                    match envelope.payload {
                        crate::domain::CounterEvent::Incremented { amount } => {
                            client
                                .execute(
                                    "INCRBY",
                                    vec![key.into_bytes(), amount.to_string().into_bytes()],
                                )
                                .await
                                .map_err(|e| EventStoreError::Backend(e.to_string()))?;
                        }
                        crate::domain::CounterEvent::Decremented { amount } => {
                            let delta = -amount;
                            client
                                .execute(
                                    "INCRBY",
                                    vec![key.into_bytes(), delta.to_string().into_bytes()],
                                )
                                .await
                                .map_err(|e| EventStoreError::Backend(e.to_string()))?;
                        }
                        crate::domain::CounterEvent::ResetPerformed { value } => {
                            client
                                .execute(
                                    "SET",
                                    vec![key.into_bytes(), value.to_string().into_bytes()],
                                )
                                .await
                                .map_err(|e| EventStoreError::Backend(e.to_string()))?;
                        }
                    }
                    return Ok(());
                }
                #[cfg(not(any(
                    all(feature = "spin-redis", runtime_spin),
                    all(feature = "wasi-redis", runtime_wasmtime)
                )))]
                {
                    return Err(EventStoreError::Backend(
                        "redis projection requires WASI_RUNTIME=spin or WASI_RUNTIME=wasmtime".to_string(),
                    ));
                }
            }
            #[cfg(not(feature = "redis"))]
            {
                return Err(EventStoreError::Backend("redis feature not enabled".to_string()));
            }
        }
        
        if backend == "sqlite" {
            #[cfg(runtime_spin)]
            {
                #[cfg(feature = "sqlite")]
                {
                    let (sql, param_val) = match envelope.payload {
                        crate::domain::CounterEvent::Incremented { amount } => (
                            "INSERT INTO counter_read_model (id, value) VALUES (?, ?) ON CONFLICT(id) DO UPDATE SET value = value + excluded.value;",
                            amount,
                        ),
                        crate::domain::CounterEvent::Decremented { amount } => (
                            "INSERT INTO counter_read_model (id, value) VALUES (?, ?) ON CONFLICT(id) DO UPDATE SET value = value + excluded.value;",
                            -amount,
                        ),
                        crate::domain::CounterEvent::ResetPerformed { value } => (
                            "INSERT INTO counter_read_model (id, value) VALUES (?, ?) ON CONFLICT(id) DO UPDATE SET value = excluded.value;",
                            value,
                        ),
                    };
                    let params = vec![
                        serde_json::Value::String(aggregate_id_str),
                        serde_json::Value::Number(param_val.into()),
                    ];
                    ddd_cqrs_es::adapters::execute_spin_sqlite(sql, params).await
                        .map_err(|e| EventStoreError::Backend(e))?;
                    return Ok(());
                }
                #[cfg(not(feature = "sqlite"))]
                {
                    return Err(EventStoreError::Backend("sqlite feature not enabled".to_string()));
                }
            }
            #[cfg(runtime_wasmtime)]
            {
                // Local fs read model update
                use std::fs;
                use std::path::Path;
                let path = Path::new("/data/counter_read_model.json");
                let content = if path.exists() {
                    fs::read_to_string(path).map_err(|e| EventStoreError::Backend(e.to_string()))?
                } else {
                    "{}".to_string()
                };
                let mut map: std::collections::HashMap<String, i32> = serde_json::from_str(&content)
                    .map_err(|e| EventStoreError::Deserialization(e.to_string()))?;
                let current = map.get(&aggregate_id_str).copied().unwrap_or(0);
                let updated = match envelope.payload {
                    crate::domain::CounterEvent::Incremented { amount } => current + amount,
                    crate::domain::CounterEvent::Decremented { amount } => current - amount,
                    crate::domain::CounterEvent::ResetPerformed { value } => value,
                };
                map.insert(aggregate_id_str, updated);
                let new_content = serde_json::to_string(&map)
                    .map_err(|e| EventStoreError::Serialization(e.to_string()))?;
                fs::write(path, new_content).map_err(|e| EventStoreError::Backend(e.to_string()))?;
                return Ok(());
            }
        }

        if backend == "mysql" {
            let (sql_mysql, param_val) = match envelope.payload {
                crate::domain::CounterEvent::Incremented { amount } => (
                    "INSERT INTO counter_read_model (id, value) VALUES (?, ?) ON DUPLICATE KEY UPDATE value = value + VALUES(value);",
                    amount,
                ),
                crate::domain::CounterEvent::Decremented { amount } => (
                    "INSERT INTO counter_read_model (id, value) VALUES (?, ?) ON DUPLICATE KEY UPDATE value = value + VALUES(value);",
                    -amount,
                ),
                crate::domain::CounterEvent::ResetPerformed { value } => (
                    "INSERT INTO counter_read_model (id, value) VALUES (?, ?) ON DUPLICATE KEY UPDATE value = VALUES(value);",
                    value,
                ),
            };
            let params = vec![
                serde_json::Value::String(aggregate_id_str),
                serde_json::Value::Number(param_val.into()),
            ];

            execute_query_routed(sql_mysql, sql_mysql, params).await
                .map_err(EventStoreError::Backend)?;

            return Ok(());
        }

        let (sql_sqlite, sql_postgres, param_val) = match envelope.payload {
            crate::domain::CounterEvent::Incremented { amount } => (
                "INSERT INTO counter_read_model (id, value) VALUES (?, ?) ON CONFLICT(id) DO UPDATE SET value = value + excluded.value;",
                "INSERT INTO counter_read_model (id, value) VALUES ($1, $2) ON CONFLICT(id) DO UPDATE SET value = counter_read_model.value + EXCLUDED.value;",
                amount,
            ),
            crate::domain::CounterEvent::Decremented { amount } => (
                "INSERT INTO counter_read_model (id, value) VALUES (?, ?) ON CONFLICT(id) DO UPDATE SET value = value + excluded.value;",
                "INSERT INTO counter_read_model (id, value) VALUES ($1, $2) ON CONFLICT(id) DO UPDATE SET value = counter_read_model.value + EXCLUDED.value;",
                -amount,
            ),
            crate::domain::CounterEvent::ResetPerformed { value } => (
                "INSERT INTO counter_read_model (id, value) VALUES (?, ?) ON CONFLICT(id) DO UPDATE SET value = excluded.value;",
                "INSERT INTO counter_read_model (id, value) VALUES ($1, $2) ON CONFLICT(id) DO UPDATE SET value = EXCLUDED.value;",
                value,
            ),
        };
        
        let params_upsert = vec![
            serde_json::Value::String(aggregate_id_str),
            serde_json::Value::Number(param_val.into()),
        ];
        
        execute_query_routed(sql_sqlite, sql_postgres, params_upsert).await
            .map_err(EventStoreError::Backend)?;
        
        Ok(())
    }
}

// -------------------------------------------------------------------------
// QUERY APIS
// -------------------------------------------------------------------------

fn value_as_i64(value: &serde_json::Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|v| i64::try_from(v).ok()))
        .or_else(|| value.as_str().and_then(|v| v.parse::<i64>().ok()))
}

fn value_as_u64(value: &serde_json::Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|v| u64::try_from(v).ok()))
        .or_else(|| value.as_str().and_then(|v| v.parse::<u64>().ok()))
}

#[cfg(feature = "redis")]
#[allow(dead_code)]
fn redis_value_as_i64(value: &ddd_cqrs_es::redis::RedisValue) -> Option<i64> {
    match value {
        ddd_cqrs_es::redis::RedisValue::Int(value) => Some(*value),
        ddd_cqrs_es::redis::RedisValue::Bytes(bytes) => {
            std::str::from_utf8(bytes).ok()?.parse::<i64>().ok()
        }
        ddd_cqrs_es::redis::RedisValue::Status(value) => value.parse::<i64>().ok(),
        ddd_cqrs_es::redis::RedisValue::Array(values) if values.len() == 1 => {
            redis_value_as_i64(&values[0])
        }
        _ => None,
    }
}

#[cfg(feature = "redis")]
#[allow(dead_code)]
fn redis_value_as_string(value: &ddd_cqrs_es::redis::RedisValue) -> Option<String> {
    match value {
        ddd_cqrs_es::redis::RedisValue::Bytes(bytes) => {
            String::from_utf8(bytes.clone()).ok()
        }
        ddd_cqrs_es::redis::RedisValue::Status(value) => Some(value.clone()),
        ddd_cqrs_es::redis::RedisValue::Int(value) => Some(value.to_string()),
        ddd_cqrs_es::redis::RedisValue::Array(values) if values.len() == 1 => {
            redis_value_as_string(&values[0])
        }
        _ => None,
    }
}

#[cfg(feature = "redis")]
#[allow(dead_code)]
fn ensure_redis_projection_result(
    value: &ddd_cqrs_es::redis::RedisValue,
) -> Result<(), String> {
    let ddd_cqrs_es::redis::RedisValue::Array(items) = value else {
        return Err(format!("Redis projection script returned {value:?}"));
    };
    let status = items
        .first()
        .and_then(redis_value_as_string)
        .ok_or_else(|| format!("Redis projection script returned {value:?}"))?;

    match status.as_str() {
        "OK" | "SKIP" => Ok(()),
        "ERR" => {
            let reason = items
                .get(1)
                .and_then(redis_value_as_string)
                .unwrap_or_else(|| "unknown".to_string());
            let checkpoint = items
                .get(2)
                .and_then(redis_value_as_i64)
                .map_or_else(|| "?".to_string(), |value| value.to_string());
            Err(format!(
                "Redis projection script failed: {reason} at checkpoint {checkpoint}"
            ))
        }
        _ => Err(format!("unknown Redis projection status `{status}`")),
    }
}

fn row_count(row: &serde_json::Value) -> i32 {
    row.get("count")
        .or_else(|| row.get("value"))
        .and_then(value_as_i64)
        .and_then(|v| i32::try_from(v).ok())
        .unwrap_or(0)
}

fn event_log_from_row(row: &serde_json::Value) -> crate::app::EventLogDto {
    let sequence = row.get("sequence").and_then(value_as_u64).unwrap_or(0);
    let event_type = row
        .get("event_type")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let revision = row.get("revision").and_then(value_as_u64).unwrap_or(0);
    let payload = row
        .get("payload")
        .map(|v| {
            if v.is_string() {
                v.as_str().unwrap_or("").to_string()
            } else {
                v.to_string()
            }
        })
        .unwrap_or_default();
    let recorded_at_ms = row
        .get("recorded_at_ms")
        .and_then(value_as_i64)
        .unwrap_or(0);
    let recorded_at = format!("+{}ms", recorded_at_ms % 100000);

    crate::app::EventLogDto {
        sequence,
        event_type,
        revision,
        payload,
        recorded_at,
    }
}

#[allow(dead_code)]
fn event_log_from_envelope(
    envelope: &EventEnvelope<crate::domain::CounterEvent, crate::domain::CounterId>,
) -> crate::app::EventLogDto {
    let payload = serde_json::to_string(&envelope.payload).unwrap_or_default();
    let recorded_at_ms = envelope
        .recorded_at
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default();

    crate::app::EventLogDto {
        sequence: envelope.sequence.unwrap_or(0),
        event_type: envelope.event_type.clone(),
        revision: envelope.revision,
        payload,
        recorded_at: format!("+{}ms", recorded_at_ms % 100000),
    }
}

fn event_logs_from_value(value: Option<&serde_json::Value>) -> Result<Vec<crate::app::EventLogDto>, String> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };

    if value.is_null() {
        return Ok(Vec::new());
    }

    let parsed;
    let value = if let Some(s) = value.as_str() {
        let s_trimmed = s.trim();
        tracing::debug!("[event_logs_from_value] input string s_trimmed: {:?}", s_trimmed);
        if s_trimmed.is_empty() {
            return Ok(Vec::new());
        }
        parsed = serde_json::from_str::<serde_json::Value>(s_trimmed)
            .map_err(|e| {
                tracing::error!("[event_logs_from_value] parsing failed! error: {}, string: {:?}", e, s_trimmed);
                format!("Failed to parse latest_events JSON: {}", e)
            })?;
        tracing::debug!("[event_logs_from_value] successfully parsed value: {:?}", parsed);
        &parsed
    } else {
        tracing::debug!("[event_logs_from_value] input is not a string, value: {:?}", value);
        value
    };

    let Some(rows) = value.as_array() else {
        return Ok(Vec::new());
    };

    Ok(rows.iter().map(event_log_from_row).collect())
}

pub async fn get_counter_view_db() -> Result<crate::app::CounterViewDto, String> {
    let backend = get_backend();

    if backend == "sqlite" || backend == "redis" || backend == "mysql" {
        let count = get_count_db().await?;
        let latest_events = get_latest_events_db().await?;
        let last_sequence = latest_events.first().map(|event| event.sequence).unwrap_or(0);
        return Ok(crate::app::CounterViewDto {
            count,
            latest_events,
            last_sequence,
            realtime_enabled: get_realtime_backend() != "off",
        });
    }

    let aggregate_id = crate::domain::CounterId("global".to_string());
    let aggregate_id_str = serde_json::to_string(&aggregate_id).map_err(|e| e.to_string())?;
    let params = vec![serde_json::Value::String(aggregate_id_str)];

    let query_sqlite = r#"
        SELECT
            COALESCE((SELECT value FROM counter_read_model WHERE id = ?), 0) AS count,
            COALESCE((
                SELECT json_group_array(json_object(
                    'sequence', sequence,
                    'event_type', event_type,
                    'revision', revision,
                    'payload', payload,
                    'recorded_at_ms', recorded_at_ms
                ))
                FROM (
                    SELECT sequence, event_type, revision, payload, recorded_at_ms
                    FROM events
                    ORDER BY sequence DESC
                    LIMIT 5
                )
            ), '[]') AS latest_events
    "#;
    let query_postgres = r#"
        SELECT
            COALESCE((SELECT value FROM counter_read_model WHERE id = $1), 0) AS count,
            COALESCE((
                SELECT json_agg(json_build_object(
                    'sequence', sequence,
                    'event_type', event_type,
                    'revision', revision,
                    'payload', payload,
                    'recorded_at_ms', recorded_at_ms
                ) ORDER BY sequence DESC)
                FROM (
                    SELECT sequence, event_type, revision, payload, recorded_at_ms
                    FROM events
                    ORDER BY sequence DESC
                    LIMIT 5
                ) latest
            ), '[]'::json) AS latest_events
    "#;

    let rows = execute_query_routed(query_sqlite, query_postgres, params).await?;
    let Some(row) = rows.first() else {
        return Ok(crate::app::CounterViewDto {
            count: 0,
            latest_events: Vec::new(),
            last_sequence: 0,
            realtime_enabled: get_realtime_backend() != "off",
        });
    };
    tracing::debug!("[get_counter_view_db] row: {}", serde_json::to_string(row).unwrap_or_default());
    let latest_events = event_logs_from_value(row.get("latest_events"))?;
    let last_sequence = latest_events.first().map(|event| event.sequence).unwrap_or(0);

    Ok(crate::app::CounterViewDto {
        count: row_count(row),
        latest_events,
        last_sequence,
        realtime_enabled: get_realtime_backend() != "off",
    })
}

pub async fn get_count_db() -> Result<i32, String> {
    let backend = get_backend();

    if backend == "redis" {
        #[cfg(feature = "redis")]
        {
            #[cfg(any(
                    all(feature = "spin-redis", runtime_spin),
                    all(feature = "wasi-redis", runtime_wasmtime)
                ))]
            {
                use ddd_cqrs_es::RedisCommandExecutor;

                let aggregate_id = crate::domain::CounterId("global".to_string());
                let aggregate_id_str = serde_json::to_string(&aggregate_id).map_err(|e| e.to_string())?;
                let key = redis_read_model_key(&aggregate_id_str);
                let value = redis_client()
                    .execute("GET", vec![key.into_bytes()])
                    .await
                    .map_err(|e| e.to_string())?;
                return Ok(redis_value_as_i64(&value)
                    .and_then(|value| i32::try_from(value).ok())
                    .unwrap_or(0));
            }
            #[cfg(not(any(
                    all(feature = "spin-redis", runtime_spin),
                    all(feature = "wasi-redis", runtime_wasmtime)
                )))]
            {
                return Err("redis count query requires WASI_RUNTIME=spin or WASI_RUNTIME=wasmtime".to_string());
            }
        }
        #[cfg(not(feature = "redis"))]
        {
            return Err("redis feature not enabled".to_string());
        }
    }
    
    if backend == "sqlite" {
        #[cfg(runtime_spin)]
        {
            #[cfg(feature = "sqlite")]
            {
                let query = "SELECT value FROM counter_read_model WHERE id = ?";
                let aggregate_id = crate::domain::CounterId("global".to_string());
                let aggregate_id_str = serde_json::to_string(&aggregate_id).map_err(|e| e.to_string())?;
                let params = vec![serde_json::Value::String(aggregate_id_str)];
                let rows = ddd_cqrs_es::adapters::execute_spin_sqlite(query, params).await.map_err(|e| e.to_string())?;
                return Ok(rows.first().map(row_count).unwrap_or(0));
            }
            #[cfg(not(feature = "sqlite"))]
            {
                return Err("sqlite feature not enabled".to_string());
            }
        }
        #[cfg(runtime_wasmtime)]
        {
            use std::fs;
            use std::path::Path;
            let path = Path::new("/data/counter_read_model.json");
            if !path.exists() {
                return Ok(0);
            }
            let content = fs::read_to_string(path).map_err(|e| e.to_string())?;
            let map: std::collections::HashMap<String, i32> = serde_json::from_str(&content).map_err(|e| e.to_string())?;
            let aggregate_id = crate::domain::CounterId("global".to_string());
            let aggregate_id_str = serde_json::to_string(&aggregate_id).map_err(|e| e.to_string())?;
            return Ok(map.get(&aggregate_id_str).copied().unwrap_or(0));
        }
    }
    
    let query_sqlite = "SELECT value FROM counter_read_model WHERE id = ?";
    let query_postgres = "SELECT value FROM counter_read_model WHERE id = $1";
    
    let aggregate_id = crate::domain::CounterId("global".to_string());
    let aggregate_id_str = serde_json::to_string(&aggregate_id).map_err(|e| e.to_string())?;
    let params = vec![serde_json::Value::String(aggregate_id_str)];
    
    let rows = execute_query_routed(query_sqlite, query_postgres, params).await?;
    
    Ok(rows.first().map(row_count).unwrap_or(0))
}

pub async fn get_latest_events_db() -> Result<Vec<crate::app::EventLogDto>, String> {
    let backend = get_backend();

    if backend == "redis" {
        #[cfg(feature = "redis")]
        {
            let event_store = MultiBackendEventStore::<crate::domain::Counter>::new();
            let mut events = event_store
                .load_global_after(None)
                .await
                .map_err(|e| e.to_string())?;
            events.sort_by_key(|event| event.sequence.unwrap_or(0));
            events.reverse();
            return Ok(events.iter().take(5).map(event_log_from_envelope).collect());
        }
        #[cfg(not(feature = "redis"))]
        {
            return Err("redis feature not enabled".to_string());
        }
    }
    
    if backend == "sqlite" {
        #[cfg(runtime_spin)]
        {
            #[cfg(feature = "sqlite")]
            {
                let query = "SELECT sequence, event_type, revision, payload, recorded_at_ms FROM events ORDER BY sequence DESC LIMIT 5";
                let rows = ddd_cqrs_es::adapters::execute_spin_sqlite(query, Vec::new()).await.map_err(|e| e.to_string())?;
                return Ok(rows.iter().map(event_log_from_row).collect());
            }
            #[cfg(not(feature = "sqlite"))]
            {
                return Err("sqlite feature not enabled".to_string());
            }
        }
        #[cfg(runtime_wasmtime)]
        {
            use std::fs;
            use std::path::Path;
            let path = Path::new("/data/events.json");
            if !path.exists() {
                return Ok(Vec::new());
            }
            let content = fs::read_to_string(path).map_err(|e| e.to_string())?;
            let values: Vec<serde_json::Value> = serde_json::from_str(&content).map_err(|e| e.to_string())?;
            let mut matching_vals: Vec<serde_json::Value> = values.into_iter()
                .filter(|val| {
                    use ddd_cqrs_es::Aggregate;
                    val.get("aggregate_type").and_then(|t| t.as_str()) == Some(crate::domain::Counter::aggregate_type())
                })
                .collect();
            matching_vals.sort_by_key(|val| val.get("sequence").and_then(|s| s.as_u64()).unwrap_or(0));
            matching_vals.reverse();
            let mut events = Vec::new();
            for val in matching_vals.into_iter().take(5) {
                events.push(event_log_from_row(&val));
            }
            return Ok(events);
        }
    }
    
    let query_sqlite = if backend == "mysql" {
        "SELECT sequence, event_type, revision, CAST(payload AS CHAR(10000) CHARACTER SET utf8mb4) AS payload, recorded_at_ms FROM events ORDER BY sequence DESC LIMIT 5"
    } else {
        "SELECT sequence, event_type, revision, payload, recorded_at_ms FROM events ORDER BY sequence DESC LIMIT 5"
    };
    let query_postgres = "SELECT sequence, event_type, revision, payload, recorded_at_ms FROM events ORDER BY sequence DESC LIMIT 5";
    
    let rows = execute_query_routed(query_sqlite, query_postgres, Vec::new()).await?;
    
    Ok(rows.iter().map(event_log_from_row).collect())
}

// -------------------------------------------------------------------------
// ASYNC COORDINATOR FOR PROJECTIONS RUNNER
// -------------------------------------------------------------------------
#[cfg(any(all(feature = "spin-redis", runtime_spin), all(feature = "wasi-redis", runtime_wasmtime)))]
async fn apply_redis_counter_projection_atomically(
    envelope: &EventEnvelope<crate::domain::CounterEvent, crate::domain::CounterId>,
) -> Result<(), String> {
    use ddd_cqrs_es::RedisCommandExecutor;

    let sequence = envelope
        .sequence
        .ok_or_else(|| "Event envelope is missing global sequence".to_string())?;
    let aggregate_id_json =
        serde_json::to_string(&envelope.aggregate_id).map_err(|error| error.to_string())?;
    let read_model_key = redis_read_model_key(&aggregate_id_json);
    let checkpoint_key = redis_checkpoint_key("counter_projection");
    let (operation, amount) = match envelope.payload {
        crate::domain::CounterEvent::Incremented { amount } => ("incr", i64::from(amount)),
        crate::domain::CounterEvent::Decremented { amount } => ("incr", -i64::from(amount)),
        crate::domain::CounterEvent::ResetPerformed { value } => ("set", i64::from(value)),
    };

    let value = redis_client()
        .execute(
            "EVAL",
            vec![
                REDIS_COUNTER_PROJECTION_LUA.as_bytes().to_vec(),
                b"2".to_vec(),
                read_model_key.into_bytes(),
                checkpoint_key.into_bytes(),
                sequence.to_string().into_bytes(),
                operation.as_bytes().to_vec(),
                amount.to_string().into_bytes(),
            ],
        )
        .await
        .map_err(|error| error.to_string())?;

    ensure_redis_projection_result(&value)
}

#[cfg(any(all(feature = "spin-redis", runtime_spin), all(feature = "wasi-redis", runtime_wasmtime)))]
pub struct RedisAtomicCounterProjection;

#[cfg(any(all(feature = "spin-redis", runtime_spin), all(feature = "wasi-redis", runtime_wasmtime)))]
#[async_trait]
impl ddd_cqrs_es::AsyncCheckpointedProjection<crate::domain::CounterEvent, crate::domain::CounterId> for RedisAtomicCounterProjection {
    type Error = String;

    fn name(&self) -> &'static str {
        "counter_projection"
    }

    async fn load_checkpoint(&self) -> Result<Option<u64>, Self::Error> {
        let store = ddd_cqrs_es::RedisCheckpointStore::new(redis_client());
        ddd_cqrs_es::AsyncCheckpointStore::load_checkpoint(&store, self.name())
            .await
            .map_err(|e| e.to_string())
    }

    async fn apply_and_checkpoint(
        &mut self,
        event: &EventEnvelope<crate::domain::CounterEvent, crate::domain::CounterId>,
    ) -> Result<(), Self::Error> {
        apply_redis_counter_projection_atomically(event).await
    }
}

#[cfg(any(all(feature = "spin-redis", runtime_spin), all(feature = "wasi-redis", runtime_wasmtime)))]
async fn run_redis_counter_projection_atomic(
    event_store: &MultiBackendEventStore<crate::domain::Counter>,
    _checkpoint_store: &MultiBackendCheckpointStore,
) -> Result<usize, String> {
    use ddd_cqrs_es::AsyncCheckpointedProjectionRunner;

    let projection = RedisAtomicCounterProjection;
    let mut runner = AsyncCheckpointedProjectionRunner::new(projection);
    runner.run(event_store).await
        .map_err(|error| format!("atomic runner error: {:?}", error))
}

pub async fn run_projections_async(
    event_store: &MultiBackendEventStore<crate::domain::Counter>,
    checkpoint_store: &MultiBackendCheckpointStore,
    projection: &mut MultiBackendCounterProjection,
) -> Result<usize, String> {
    use ddd_cqrs_es::async_api::AsyncEventStore;

    if get_backend() == "redis" {
        #[cfg(any(all(feature = "spin-redis", runtime_spin), all(feature = "wasi-redis", runtime_wasmtime)))]
        {
            let _ = projection;
            return run_redis_counter_projection_atomic(event_store, checkpoint_store).await;
        }
        #[cfg(not(any(all(feature = "spin-redis", runtime_spin), all(feature = "wasi-redis", runtime_wasmtime))))]
        {
            return Err(
                "redis projection requires redis feature and WASI_RUNTIME=spin or WASI_RUNTIME=wasmtime"
                    .to_string(),
            );
        }
    }
    
    let last_sequence = checkpoint_store.load_checkpoint_async("counter_projection").await
        .map_err(|e| e.to_string())?;
        
    let envelopes = event_store.load_global_after(last_sequence).await
        .map_err(|e| e.to_string())?;
        
    let count = envelopes.len();
    let mut last_sequence_processed = None;
    for envelope in envelopes {
        projection.apply_async(&envelope).await
            .map_err(|e| e.to_string())?;
            
        let sequence = envelope.sequence
            .ok_or_else(|| "Event envelope is missing global sequence".to_string())?;
            
        last_sequence_processed = Some(sequence);
    }
    
    if let Some(seq) = last_sequence_processed {
        checkpoint_store.save_checkpoint_async("counter_projection", seq).await
            .map_err(|e| e.to_string())?;
    }
    
    Ok(count)
}

#[cfg(any(
    all(feature = "spin-redis", runtime_spin),
    all(feature = "wasi-redis", runtime_wasmtime)
))]
async fn redis_execute(
    command: &str,
    args: Vec<Vec<u8>>,
) -> Result<ddd_cqrs_es::redis::RedisValue, String> {
    use ddd_cqrs_es::RedisCommandExecutor;

    redis_client()
        .execute(command, args)
        .await
        .map_err(|error| error.to_string())
}

#[cfg(feature = "redis")]
#[allow(dead_code)]
fn redis_value_bytes(value: &ddd_cqrs_es::redis::RedisValue) -> Option<Vec<u8>> {
    match value {
        ddd_cqrs_es::redis::RedisValue::Bytes(bytes) => Some(bytes.clone()),
        ddd_cqrs_es::redis::RedisValue::Status(value) => Some(value.as_bytes().to_vec()),
        ddd_cqrs_es::redis::RedisValue::Int(value) => Some(value.to_string().into_bytes()),
        ddd_cqrs_es::redis::RedisValue::Array(items) if items.len() == 1 => {
            redis_value_bytes(&items[0])
        }
        ddd_cqrs_es::redis::RedisValue::Nil | ddd_cqrs_es::redis::RedisValue::Array(_) => None,
    }
}

#[cfg(feature = "redis")]
#[allow(dead_code)]
fn redis_value_strings(value: &ddd_cqrs_es::redis::RedisValue) -> Vec<String> {
    match value {
        ddd_cqrs_es::redis::RedisValue::Array(items) => items
            .iter()
            .filter_map(redis_value_bytes)
            .filter_map(|bytes| String::from_utf8(bytes).ok())
            .collect(),
        value => redis_value_bytes(value)
            .and_then(|bytes| String::from_utf8(bytes).ok())
            .into_iter()
            .collect(),
    }
}

#[cfg(feature = "redis")]
#[allow(dead_code)]
const REDIS_REALTIME_FANOUT_LUA: &str = r#"
local subscribers = redis.call('SMEMBERS', KEYS[1])
local pushed = 0

for _, queue_key in ipairs(subscribers) do
    local alive_key = queue_key .. ':alive'
    if redis.call('EXISTS', alive_key) == 1 then
        redis.call('LPUSH', queue_key, ARGV[1])
        redis.call('EXPIRE', queue_key, 120)
        pushed = pushed + 1
    else
        redis.call('SREM', KEYS[1], queue_key)
        redis.call('DEL', queue_key)
    end
end

return pushed
"#;

#[cfg(any(
    all(feature = "spin-redis", runtime_spin),
    all(feature = "wasi-redis", runtime_wasmtime)
))]
async fn redis_touch_realtime_subscriber(queue_key: &str, alive_key: &str) -> Result<(), String> {
    redis_execute(
        "SETEX",
        vec![
            alive_key.as_bytes().to_vec(),
            b"60".to_vec(),
            b"1".to_vec(),
        ],
    )
    .await?;
    redis_execute(
        "SADD",
        vec![
            redis_realtime_subscribers_key().as_bytes().to_vec(),
            queue_key.as_bytes().to_vec(),
        ],
    )
    .await?;
    Ok(())
}

#[cfg(any(
    all(feature = "spin-redis", runtime_spin),
    all(feature = "wasi-redis", runtime_wasmtime)
))]
async fn redis_publish_realtime_wake(payload: &[u8]) -> Result<(), String> {
    let subscribers_key = redis_realtime_subscribers_key();
    redis_execute(
        "EVAL",
        vec![
            REDIS_REALTIME_FANOUT_LUA.as_bytes().to_vec(),
            b"1".to_vec(),
            subscribers_key.as_bytes().to_vec(),
            payload.to_vec(),
        ],
    )
    .await?;
    Ok(())
}

pub async fn publish_counter_realtime(_view: &crate::app::CounterViewDto) {
    if get_realtime_backend() != "redis" {
        return;
    }

    #[cfg(feature = "redis")]
    {
        #[cfg(any(
                    all(feature = "spin-redis", runtime_spin),
                    all(feature = "wasi-redis", runtime_wasmtime)
                ))]
        {
            let message = crate::app::CounterRealtimeMessage {
                view: _view.clone(),
                last_sequence: _view.last_sequence,
            };
            let payload = match serde_json::to_vec(&message) {
                Ok(payload) => payload,
                Err(error) => {
                    eprintln!("failed to serialize Redis realtime notification: {error}");
                    return;
                }
            };

            let publisher =
                ddd_cqrs_es::RedisPubSubPublisher::new(redis_client(), get_redis_channel());
            if let Err(error) = publisher.publish(&payload).await {
                eprintln!("failed to publish Redis realtime notification: {error}");
            }
            if let Err(error) = redis_publish_realtime_wake(&payload).await {
                eprintln!("failed to wake Redis realtime SSE subscribers: {error}");
            }
        }
        #[cfg(not(any(
                    all(feature = "spin-redis", runtime_spin),
                    all(feature = "wasi-redis", runtime_wasmtime)
                )))]
        {
            eprintln!("redis realtime requires WASI_RUNTIME=spin or WASI_RUNTIME=wasmtime");
        }
    }
    #[cfg(not(feature = "redis"))]
    {
        eprintln!("redis realtime requested but redis feature is not enabled");
    }
}

pub async fn counter_realtime_message_after(
    last_sequence: u64,
) -> Result<Option<crate::app::CounterRealtimeMessage>, String> {
    let event_store = MultiBackendEventStore::<crate::domain::Counter>::new();
    let newer_events = event_store
        .load_global_after(Some(last_sequence))
        .await
        .map_err(|e| e.to_string())?;

    if newer_events.is_empty() {
        return Ok(None);
    }

    let checkpoint_store = MultiBackendCheckpointStore::new();
    let mut projection = MultiBackendCounterProjection::new();
    run_projections_async(&event_store, &checkpoint_store, &mut projection).await?;

    let view = get_counter_view_db().await?;
    Ok(Some(crate::app::CounterRealtimeMessage {
        last_sequence: view.last_sequence,
        view,
    }))
}

#[cfg(feature = "ssr")]
struct CounterStreamState {
    last_sequence: u64,
    redis_subscriber: Option<CounterRedisSubscriber>,
    checked_initial_catchup: bool,
}

#[cfg(feature = "ssr")]
#[allow(dead_code)]
struct CounterRedisSubscriber {
    queue_key: String,
    alive_key: String,
}

#[cfg(feature = "ssr")]
impl CounterStreamState {
    async fn new(last_sequence: u64) -> Self {
        Self {
            last_sequence,
            redis_subscriber: CounterRedisSubscriber::register().await,
            checked_initial_catchup: false,
        }
    }

    fn has_redis_wake(&self) -> bool {
        self.redis_subscriber.is_some()
    }

    async fn next_frame(&mut self) -> String {
        match self.next_message().await {
            Ok(Some(message)) => counter_sse_frame(&message),
            Ok(None) => counter_sse_keepalive_frame(),
            Err(error) => counter_sse_error_frame(&error),
        }
    }

    async fn next_message(&mut self) -> Result<Option<crate::app::CounterRealtimeMessage>, String> {
        if !self.checked_initial_catchup {
            self.checked_initial_catchup = true;
            if let Some(message) = counter_realtime_message_after(self.last_sequence).await? {
                self.last_sequence = message.last_sequence;
                return Ok(Some(message));
            }
        }

        let Some(subscriber) = &self.redis_subscriber else {
            return Ok(None);
        };

        if subscriber.next_payload().await?.is_none() {
            return Ok(None);
        }

        let message = counter_realtime_message_after(self.last_sequence).await?;
        if let Some(message) = &message {
            self.last_sequence = message.last_sequence;
        }
        Ok(message)
    }
}

#[cfg(feature = "ssr")]
impl CounterRedisSubscriber {
    #[cfg(any(
        all(feature = "spin-redis", runtime_spin),
        all(feature = "wasi-redis", runtime_wasmtime)
    ))]
    async fn register() -> Option<Self> {
        if get_realtime_backend() != "redis" {
            return None;
        }

        let subscriber_id = EventId::new().as_str().to_owned();
        let queue_key = redis_realtime_queue_key(&subscriber_id);
        let alive_key = redis_realtime_alive_key(&queue_key);
        let subscriber = Self {
            queue_key,
            alive_key,
        };

        if let Err(error) =
            redis_touch_realtime_subscriber(&subscriber.queue_key, &subscriber.alive_key).await
        {
            eprintln!("failed to register Redis realtime SSE subscriber: {error}");
            return None;
        }

        Some(subscriber)
    }

    #[cfg(not(any(
        all(feature = "spin-redis", runtime_spin),
        all(feature = "wasi-redis", runtime_wasmtime)
    )))]
    async fn register() -> Option<Self> {
        None
    }

    #[cfg(any(
        all(feature = "spin-redis", runtime_spin),
        all(feature = "wasi-redis", runtime_wasmtime)
    ))]
    async fn next_payload(&self) -> Result<Option<Vec<u8>>, String> {
        redis_touch_realtime_subscriber(&self.queue_key, &self.alive_key).await?;
        let value = redis_execute(
            "BRPOP",
            vec![self.queue_key.as_bytes().to_vec(), b"25".to_vec()],
        )
        .await?;
        let items = redis_value_strings(&value);
        if items.len() < 2 {
            return Ok(None);
        }
        Ok(items.last().map(|payload| payload.as_bytes().to_vec()))
    }

    #[cfg(not(any(
        all(feature = "spin-redis", runtime_spin),
        all(feature = "wasi-redis", runtime_wasmtime)
    )))]
    async fn next_payload(&self) -> Result<Option<Vec<u8>>, String> {
        Ok(None)
    }
}

#[cfg(feature = "ssr")]
fn counter_sse_frame(message: &crate::app::CounterRealtimeMessage) -> String {
    match serde_json::to_string(message) {
        Ok(json) => format!("id: {}\nevent: counter\ndata: {json}\n\n", message.last_sequence),
        Err(error) => counter_sse_error_frame(&error.to_string()),
    }
}

#[cfg(feature = "ssr")]
fn counter_sse_error_frame(error: &str) -> String {
    format!(
        "event: error\ndata: {{\"message\":\"{}\"}}\n\n",
        error.replace('"', "'")
    )
}

#[cfg(feature = "ssr")]
fn counter_sse_keepalive_frame() -> String {
    "retry: 1000\n: keepalive\n\n".to_string()
}

#[cfg(feature = "ssr")]
pub async fn counter_stream_response(
    req: &http::Request<wasip3::http_compat::IncomingRequestBody>,
) -> Result<
    http::Response<http_body_util::combinators::UnsyncBoxBody<bytes::Bytes, std::io::Error>>,
    String,
> {
    use http_body_util::BodyExt;

    let last_sequence = req
        .headers()
        .get("last-event-id")
        .and_then(|val| val.to_str().ok())
        .and_then(|val| val.parse::<u64>().ok())
        .or_else(|| {
            req.uri()
                .query()
                .and_then(|query| {
                    query.split('&').find_map(|part| {
                        let (key, value) = part.split_once('=')?;
                        (key == "last_sequence")
                            .then(|| value.parse::<u64>().ok())
                            .flatten()
                    })
                })
        })
        .unwrap_or(0);

    let state = CounterStreamState::new(last_sequence).await;

    let stream = if get_realtime_backend() == "redis" && state.has_redis_wake() {
        let s = futures::stream::unfold(state, |mut state| async move {
            let frame = state.next_frame().await;
            Some((
                Ok::<_, std::io::Error>(http_body::Frame::data(bytes::Bytes::from(frame))),
                state,
            ))
        });
        Box::pin(s) as std::pin::Pin<
            Box<
                dyn futures::Stream<Item = Result<http_body::Frame<bytes::Bytes>, std::io::Error>>
                    + Send,
            >,
        >
    } else {
        let s = futures::stream::unfold(state, |mut state| async move {
            let mut pings = 0;
            loop {
                match counter_realtime_message_after(state.last_sequence).await {
                    Ok(Some(message)) => {
                        state.last_sequence = message.last_sequence;
                        let frame = counter_sse_frame(&message);
                        return Some((
                            Ok::<_, std::io::Error>(http_body::Frame::data(bytes::Bytes::from(frame))),
                            state,
                        ));
                    }
                    Ok(None) => {
                        // Sleep for 100 milliseconds
                        wasip3::clocks::monotonic_clock::wait_for(100_000_000).await;
                        pings += 1;
                        // Send a keepalive comment every 15 seconds to prevent gateway timeout
                        if pings >= 150 {
                            let frame = counter_sse_keepalive_frame();
                            return Some((
                                Ok::<_, std::io::Error>(http_body::Frame::data(bytes::Bytes::from(frame))),
                                state,
                            ));
                        }
                    }
                    Err(error) => {
                        wasip3::clocks::monotonic_clock::wait_for(1_000_000_000).await;
                        let frame = counter_sse_error_frame(&error);
                        return Some((
                            Ok::<_, std::io::Error>(http_body::Frame::data(bytes::Bytes::from(frame))),
                            state,
                        ));
                    }
                }
            }
        });
        Box::pin(s) as std::pin::Pin<
            Box<
                dyn futures::Stream<Item = Result<http_body::Frame<bytes::Bytes>, std::io::Error>>
                    + Send,
            >,
        >
    };
    let body = http_body_util::StreamBody::new(stream).boxed_unsync();

    http::Response::builder()
        .status(http::StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "text/event-stream")
        .header(http::header::CACHE_CONTROL, "no-cache, no-transform")
        .header("X-Accel-Buffering", "no")
        .body(body)
        .map_err(|e| e.to_string())
}
