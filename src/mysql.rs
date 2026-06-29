//! MySQL event store adapter.

use crate::aggregate::Aggregate;
use crate::error::EventStoreError;
use crate::event::{EventEnvelope, EventId, ExpectedRevision, NewEvent};
use crate::event_store::{EventStore, EventStream};
use crate::idempotency::{IdempotencyKey, IdempotencyState, IdempotencyStore};
use crate::projection::CheckpointStore;
use crate::sql_common::{
    check_expected_revision, deserialize_id, deserialize_metadata, deserialize_payload,
    millis_to_system_time, serialize_id, serialize_metadata, serialize_payload,
    system_time_to_millis, validate_table_name,
};
use crate::upcast::UpcasterRegistry;
use mysql::prelude::*;
use mysql::{Conn, Error as MySqlError, Opts, Row, TxOpts};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

/// MySQL-backed event store.
pub struct MySqlEventStore<A>
where
    A: Aggregate,
{
    connection: Arc<Mutex<Conn>>,
    table_name: String,
    upcasters: UpcasterRegistry,
    _marker: PhantomData<fn() -> A>,
}

impl<A> Clone for MySqlEventStore<A>
where
    A: Aggregate,
{
    fn clone(&self) -> Self {
        Self {
            connection: Arc::clone(&self.connection),
            table_name: self.table_name.clone(),
            upcasters: self.upcasters.clone(),
            _marker: PhantomData,
        }
    }
}

impl<A> std::fmt::Debug for MySqlEventStore<A>
where
    A: Aggregate,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MySqlEventStore")
            .field("table_name", &self.table_name)
            .finish_non_exhaustive()
    }
}

impl<A> MySqlEventStore<A>
where
    A: Aggregate,
{
    /// Connects to MySQL using standard URL params and the default `events` table.
    pub fn connect(url: &str) -> Result<Self, EventStoreError> {
        let opts = Opts::from_url(url).map_err(|e| EventStoreError::Backend(e.to_string()))?;
        let conn = Conn::new(opts).map_err(map_mysql_error)?;
        Self::new(conn)
    }

    /// Connects to MySQL using standard URL params and a custom table name.
    pub fn connect_with_table_name(
        url: &str,
        table_name: impl Into<String>,
    ) -> Result<Self, EventStoreError> {
        let opts = Opts::from_url(url).map_err(|e| EventStoreError::Backend(e.to_string()))?;
        let conn = Conn::new(opts).map_err(map_mysql_error)?;
        Self::with_table_name(conn, table_name)
    }

    /// Creates a MySQL event store using the default `events` table.
    pub fn new(connection: Conn) -> Result<Self, EventStoreError> {
        Self::with_table_name(connection, "events")
    }

    /// Creates a MySQL event store with a custom table name.
    pub fn with_table_name(
        connection: Conn,
        table_name: impl Into<String>,
    ) -> Result<Self, EventStoreError> {
        let table_name = table_name.into();
        validate_table_name(&table_name)?;

        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            table_name,
            upcasters: UpcasterRegistry::new(),
            _marker: PhantomData,
        })
    }

    /// Returns the upcaster registry.
    pub fn upcasters(&self) -> &UpcasterRegistry {
        &self.upcasters
    }

    /// Registers a sequential schema version upcaster for a specific event type.
    pub fn register_upcaster<U>(&self, event_type: impl Into<String>, upcaster: U)
    where
        U: crate::upcast::EventUpcaster + Send + Sync + 'static,
        U::Error: std::fmt::Debug + std::fmt::Display + Send + Sync + 'static,
    {
        self.upcasters.register(event_type, upcaster);
    }

    /// Migrates the MySQL schemas to the latest version.
    pub fn migrate_schema(&self) -> Result<(), EventStoreError> {
        let config = crate::schema::SqlSchemaConfig::new(crate::schema::SqlDialect::MySql)
            .with_events_table(&self.table_name);
        let migrator = crate::schema::SchemaMigrator::new(config);
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| EventStoreError::Poisoned)?;
        migrator.run_mysql(&mut connection)
    }

    /// Initializes the MySQL event table and indexes.
    pub fn initialize_schema(&self) -> Result<(), EventStoreError> {
        self.migrate_schema()
    }
}

impl<A> EventStore<A> for MySqlEventStore<A>
where
    A: Aggregate + 'static,
    A::Event: serde::Serialize + serde::de::DeserializeOwned,
    A::Id: serde::Serialize + serde::de::DeserializeOwned,
{
    type Error = EventStoreError;

    fn load(&self, aggregate_id: &A::Id) -> Result<EventStream<A>, Self::Error> {
        let aggregate_id = serialize_id(aggregate_id)?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| EventStoreError::Poisoned)?;
        let query = format!(
            "SELECT event_id, aggregate_id, aggregate_type, revision, sequence, event_type, \
             event_version, payload, metadata, recorded_at_ms FROM {table} \
             WHERE aggregate_type = ? AND aggregate_id = ? ORDER BY revision ASC",
            table = self.table_name
        );
        let rows: Vec<Row> = connection
            .exec(&query, (A::aggregate_type(), &aggregate_id))
            .map_err(map_mysql_error)?;

        let upcasters = self.upcasters.clone();
        rows.into_iter()
            .map(|row| row_to_envelope::<A>(&upcasters, row))
            .collect()
    }

    fn append(
        &self,
        aggregate_id: &A::Id,
        expected_revision: ExpectedRevision,
        events: Vec<NewEvent<A::Event>>,
    ) -> Result<EventStream<A>, Self::Error> {
        let aggregate_id_key = serialize_id(aggregate_id)?;
        let prepared = events
            .into_iter()
            .map(PreparedMySqlEvent::new)
            .collect::<Result<Vec<_>, _>>()?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| EventStoreError::Poisoned)?;
        let mut transaction = connection
            .start_transaction(TxOpts::default())
            .map_err(map_mysql_error)?;

        let revision_query = format!(
            "SELECT COALESCE(MAX(revision), 0) FROM {table} \
             WHERE aggregate_type = ? AND aggregate_id = ?",
            table = self.table_name
        );
        let actual_revision: i64 = transaction
            .exec_first(&revision_query, (A::aggregate_type(), &aggregate_id_key))
            .map_err(map_mysql_error)?
            .unwrap_or(0);
        let actual_revision = u64::try_from(actual_revision).map_err(|_| {
            EventStoreError::Deserialization("stored revision cannot be negative".to_owned())
        })?;
        check_expected_revision(expected_revision, actual_revision)?;

        if prepared.is_empty() {
            transaction.commit().map_err(map_mysql_error)?;
            return Ok(Vec::new());
        }

        let insert = format!(
            "INSERT INTO {table} \
             (event_id, aggregate_id, aggregate_type, revision, event_type, event_version, \
              payload, metadata, recorded_at_ms) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            table = self.table_name
        );
        let mut committed = Vec::with_capacity(prepared.len());

        for (index, event) in prepared.into_iter().enumerate() {
            let revision = actual_revision + index as u64 + 1;
            let revision_i64 = i64::try_from(revision).map_err(|_| {
                EventStoreError::Serialization("revision exceeds BIGINT".to_owned())
            })?;
            let event_version_i32 = i32::try_from(event.event_version).map_err(|_| {
                EventStoreError::Serialization("event_version exceeds i32".to_owned())
            })?;

            transaction
                .exec_drop(
                    &insert,
                    (
                        event.event_id.as_str(),
                        &aggregate_id_key,
                        A::aggregate_type(),
                        revision_i64,
                        &event.event_type,
                        event_version_i32,
                        &event.payload_json,
                        &event.metadata_json,
                        event.recorded_at_ms,
                    ),
                )
                .map_err(|error| {
                    map_mysql_insert_error(error, expected_revision, actual_revision)
                })?;

            let sequence = transaction.last_insert_id().ok_or_else(|| {
                EventStoreError::Backend("MySQL last_insert_id failed".to_owned())
            })?;

            committed.push(EventEnvelope::new(
                event.event_id,
                aggregate_id.clone(),
                A::aggregate_type(),
                revision,
                Some(sequence),
                event.event_type,
                event.event_version,
                event.payload,
                event.metadata,
                event.recorded_at,
            ));
        }

        transaction.commit().map_err(map_mysql_error)?;
        Ok(committed)
    }

    fn load_global_after(&self, sequence: Option<u64>) -> Result<EventStream<A>, Self::Error> {
        let sequence = sequence.unwrap_or_default();
        let sequence_i64 = i64::try_from(sequence).map_err(|_| {
            EventStoreError::Deserialization("global sequence exceeds BIGINT".to_owned())
        })?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| EventStoreError::Poisoned)?;
        let query = format!(
            "SELECT event_id, aggregate_id, aggregate_type, revision, sequence, event_type, \
             event_version, payload, metadata, recorded_at_ms FROM {table} \
             WHERE aggregate_type = ? AND sequence > ? ORDER BY sequence ASC",
            table = self.table_name
        );
        let rows: Vec<Row> = connection
            .exec(&query, (A::aggregate_type(), sequence_i64))
            .map_err(map_mysql_error)?;

        let upcasters = self.upcasters.clone();
        rows.into_iter()
            .map(|row| row_to_envelope::<A>(&upcasters, row))
            .collect()
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl<A> crate::async_api::AsyncEventStore<A> for MySqlEventStore<A>
where
    A: Aggregate + Send + Sync + 'static,
    A::Event: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
    A::Id: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    type Error = EventStoreError;

    async fn load(&self, aggregate_id: &A::Id) -> Result<EventStream<A>, Self::Error> {
        let this = self.clone();
        let aggregate_id = aggregate_id.clone();
        tokio::task::spawn_blocking(move || EventStore::load(&this, &aggregate_id))
            .await
            .map_err(|e| EventStoreError::Backend(e.to_string()))?
    }

    async fn append(
        &self,
        aggregate_id: &A::Id,
        expected_revision: ExpectedRevision,
        events: Vec<NewEvent<A::Event>>,
    ) -> Result<EventStream<A>, Self::Error> {
        let this = self.clone();
        let aggregate_id = aggregate_id.clone();
        tokio::task::spawn_blocking(move || {
            EventStore::append(&this, &aggregate_id, expected_revision, events)
        })
        .await
        .map_err(|e| EventStoreError::Backend(e.to_string()))?
    }

    async fn load_global_after(
        &self,
        sequence: Option<u64>,
    ) -> Result<EventStream<A>, Self::Error> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || EventStore::load_global_after(&this, sequence))
            .await
            .map_err(|e| EventStoreError::Backend(e.to_string()))?
    }
}

struct PreparedMySqlEvent<E> {
    event_id: EventId,
    event_type: String,
    event_version: u32,
    payload: E,
    payload_json: String,
    metadata: crate::Metadata,
    metadata_json: String,
    recorded_at: SystemTime,
    recorded_at_ms: i64,
}

impl<E> PreparedMySqlEvent<E>
where
    E: serde::Serialize,
{
    fn new(event: NewEvent<E>) -> Result<Self, EventStoreError> {
        let event_id = EventId::new();
        let recorded_at = SystemTime::now();
        let recorded_at_ms = system_time_to_millis(recorded_at)?;
        let payload_json = serialize_payload(&event.payload)?.to_string();
        let metadata_json = serialize_metadata(&event.metadata)?.to_string();

        Ok(Self {
            event_id,
            event_type: event.event_type,
            event_version: event.event_version,
            payload: event.payload,
            payload_json,
            metadata: event.metadata,
            metadata_json,
            recorded_at,
            recorded_at_ms,
        })
    }
}

fn row_to_envelope<A>(
    upcasters: &UpcasterRegistry,
    row: Row,
) -> Result<EventEnvelope<A::Event, A::Id>, EventStoreError>
where
    A: Aggregate,
    A::Event: serde::de::DeserializeOwned,
    A::Id: serde::de::DeserializeOwned,
{
    let event_id: String = row
        .get(0)
        .ok_or_else(|| EventStoreError::Deserialization("missing event_id column".to_owned()))?;
    let aggregate_id: String = row.get(1).ok_or_else(|| {
        EventStoreError::Deserialization("missing aggregate_id column".to_owned())
    })?;
    let aggregate_type: String = row.get(2).ok_or_else(|| {
        EventStoreError::Deserialization("missing aggregate_type column".to_owned())
    })?;
    let revision: i64 = row
        .get(3)
        .ok_or_else(|| EventStoreError::Deserialization("missing revision column".to_owned()))?;
    let sequence: u64 = row
        .get(4)
        .ok_or_else(|| EventStoreError::Deserialization("missing sequence column".to_owned()))?;
    let event_type: String = row
        .get(5)
        .ok_or_else(|| EventStoreError::Deserialization("missing event_type column".to_owned()))?;
    let event_version: i32 = row.get(6).ok_or_else(|| {
        EventStoreError::Deserialization("missing event_version column".to_owned())
    })?;
    let payload_str: String = row
        .get(7)
        .ok_or_else(|| EventStoreError::Deserialization("missing payload column".to_owned()))?;
    let metadata_str: String = row
        .get(8)
        .ok_or_else(|| EventStoreError::Deserialization("missing metadata column".to_owned()))?;
    let recorded_at_ms: i64 = row.get(9).ok_or_else(|| {
        EventStoreError::Deserialization("missing recorded_at_ms column".to_owned())
    })?;

    let revision = u64::try_from(revision).map_err(|_| {
        EventStoreError::Deserialization("stored revision cannot be negative".to_owned())
    })?;
    let event_version = u32::try_from(event_version).map_err(|_| {
        EventStoreError::Deserialization("event_version cannot be negative".to_owned())
    })?;
    let aggregate_id = deserialize_id(&aggregate_id)?;

    let payload_val: serde_json::Value = serde_json::from_str(&payload_str)
        .map_err(|error| EventStoreError::Deserialization(format!("payload JSON: {error}")))?;

    let payload_bytes = serde_json::to_vec(&payload_val).map_err(|error| {
        EventStoreError::Deserialization(format!(
            "payload serialization for upcasting failed: {error}"
        ))
    })?;

    let (event_version, upcasted_bytes) = upcasters
        .upcast(&event_type, event_version, payload_bytes)
        .map_err(|err| EventStoreError::Deserialization(err.to_string()))?;

    let payload = serde_json::from_slice(&upcasted_bytes)
        .map_err(|error| EventStoreError::Deserialization(format!("payload JSON: {error}")))?;

    let payload = deserialize_payload(&event_id, &event_type, payload)?;
    let metadata_val: serde_json::Value = serde_json::from_str(&metadata_str)
        .map_err(|error| EventStoreError::Deserialization(format!("metadata JSON: {error}")))?;
    let metadata = deserialize_metadata(&event_id, metadata_val)?;
    let recorded_at = millis_to_system_time(recorded_at_ms)?;

    Ok(EventEnvelope::new(
        EventId::from_string(event_id),
        aggregate_id,
        aggregate_type,
        revision,
        Some(sequence),
        event_type,
        event_version,
        payload,
        metadata,
        recorded_at,
    ))
}

fn map_mysql_error(error: MySqlError) -> EventStoreError {
    EventStoreError::Backend(error.to_string())
}

fn map_mysql_insert_error(
    error: MySqlError,
    expected_revision: ExpectedRevision,
    actual_revision: u64,
) -> EventStoreError {
    match &error {
        MySqlError::MySqlError(e) if e.code == 1062 => {
            EventStoreError::Concurrency(crate::ConcurrencyError::WrongExpectedRevision {
                expected: expected_revision,
                actual: actual_revision,
            })
        }
        _ => map_mysql_error(error),
    }
}

/// MySQL checkpoint store implementation.
#[derive(Clone, Debug)]
pub struct MySqlCheckpointStore {
    connection: Arc<Mutex<Conn>>,
    table_name: String,
}

impl MySqlCheckpointStore {
    /// Creates a MySQL checkpoint store using the default table name.
    pub fn new(connection: Conn) -> Result<Self, EventStoreError> {
        Self::with_table_name(connection, "projection_checkpoints")
    }

    /// Creates a MySQL checkpoint store with a custom table name.
    pub fn with_table_name(
        connection: Conn,
        table_name: impl Into<String>,
    ) -> Result<Self, EventStoreError> {
        let table_name = table_name.into();
        validate_table_name(&table_name)?;

        let store = Self {
            connection: Arc::new(Mutex::new(connection)),
            table_name,
        };
        store.initialize_schema()?;
        Ok(store)
    }

    /// Initializes the checkpoint schema table.
    pub fn initialize_schema(&self) -> Result<(), EventStoreError> {
        let config = crate::schema::SqlSchemaConfig::new(crate::schema::SqlDialect::MySql)
            .with_checkpoints_table(&self.table_name);
        let migrator = crate::schema::SchemaMigrator::new(config);
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| EventStoreError::Poisoned)?;
        migrator.run_mysql(&mut connection)
    }
}

impl CheckpointStore for MySqlCheckpointStore {
    type Error = EventStoreError;

    fn load_checkpoint(&self, projection_name: &str) -> Result<Option<u64>, Self::Error> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| EventStoreError::Poisoned)?;
        let sql = format!(
            "SELECT sequence FROM {} WHERE projection_name = ?;",
            self.table_name
        );
        let row_opt: Option<Row> = connection
            .exec_first(&sql, (projection_name,))
            .map_err(map_mysql_error)?;

        if let Some(row) = row_opt {
            let sequence: u64 = row.get(0).ok_or_else(|| {
                EventStoreError::Deserialization("missing sequence in checkpoint row".to_owned())
            })?;
            Ok(Some(sequence))
        } else {
            Ok(None)
        }
    }

    fn save_checkpoint(&self, projection_name: &str, sequence: u64) -> Result<(), Self::Error> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| EventStoreError::Poisoned)?;
        let sql = format!(
            "INSERT INTO {} (projection_name, sequence) VALUES (?, ?) \
             ON DUPLICATE KEY UPDATE sequence = VALUES(sequence);",
            self.table_name
        );
        connection
            .exec_drop(&sql, (projection_name, sequence))
            .map_err(map_mysql_error)?;
        Ok(())
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl crate::projection::AsyncCheckpointStore for MySqlCheckpointStore {
    type Error = EventStoreError;

    async fn load_checkpoint(&self, projection_name: &str) -> Result<Option<u64>, Self::Error> {
        let this = self.clone();
        let name = projection_name.to_owned();
        tokio::task::spawn_blocking(move || CheckpointStore::load_checkpoint(&this, &name))
            .await
            .map_err(|e| EventStoreError::Backend(e.to_string()))?
    }

    async fn save_checkpoint(
        &self,
        projection_name: &str,
        sequence: u64,
    ) -> Result<(), Self::Error> {
        let this = self.clone();
        let name = projection_name.to_owned();
        tokio::task::spawn_blocking(move || {
            CheckpointStore::save_checkpoint(&this, &name, sequence)
        })
        .await
        .map_err(|e| EventStoreError::Backend(e.to_string()))?
    }
}

/// MySQL-backed idempotency store.
pub struct MySqlIdempotencyStore<V>
where
    V: Clone,
{
    connection: Arc<Mutex<Conn>>,
    table_name: String,
    _marker: PhantomData<fn() -> V>,
}

impl<V> Clone for MySqlIdempotencyStore<V>
where
    V: Clone,
{
    fn clone(&self) -> Self {
        Self {
            connection: Arc::clone(&self.connection),
            table_name: self.table_name.clone(),
            _marker: PhantomData,
        }
    }
}

impl<V> std::fmt::Debug for MySqlIdempotencyStore<V>
where
    V: Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MySqlIdempotencyStore")
            .field("table_name", &self.table_name)
            .finish_non_exhaustive()
    }
}

impl<V> MySqlIdempotencyStore<V>
where
    V: Clone,
{
    /// Creates a MySQL idempotency store using the default table name.
    pub fn new(connection: Conn) -> Result<Self, EventStoreError> {
        Self::with_table_name(connection, "idempotency_keys")
    }

    /// Creates a MySQL idempotency store with a custom table name.
    pub fn with_table_name(
        connection: Conn,
        table_name: impl Into<String>,
    ) -> Result<Self, EventStoreError> {
        let table_name = table_name.into();
        validate_table_name(&table_name)?;

        let store = Self {
            connection: Arc::new(Mutex::new(connection)),
            table_name,
            _marker: PhantomData,
        };
        store.initialize_schema()?;
        Ok(store)
    }

    /// Initializes the idempotency schema table.
    pub fn initialize_schema(&self) -> Result<(), EventStoreError> {
        let config = crate::schema::SqlSchemaConfig::new(crate::schema::SqlDialect::MySql)
            .with_idempotency_table(&self.table_name);
        let migrator = crate::schema::SchemaMigrator::new(config);
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| EventStoreError::Poisoned)?;
        migrator.run_mysql(&mut connection)
    }
}

impl<V> IdempotencyStore<V> for MySqlIdempotencyStore<V>
where
    V: Clone + serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    type Error = EventStoreError;

    fn load(&self, key: &IdempotencyKey) -> Result<Option<IdempotencyState<V>>, Self::Error> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| EventStoreError::Poisoned)?;
        let sql = format!(
            "SELECT state, value FROM {} WHERE idempotency_key = ?;",
            self.table_name
        );
        let row_opt: Option<Row> = connection
            .exec_first(&sql, (key.as_str(),))
            .map_err(map_mysql_error)?;

        let Some(row) = row_opt else {
            return Ok(None);
        };

        let state: String = row
            .get(0)
            .ok_or_else(|| EventStoreError::Deserialization("missing state column".to_owned()))?;
        let value_str: Option<String> = row.get::<Option<String>, _>(1).flatten();

        match (state.as_str(), value_str) {
            ("pending", _) => Ok(Some(IdempotencyState::Pending)),
            ("complete", Some(value_str)) => {
                let value = serde_json::from_str(&value_str).map_err(|error| {
                    EventStoreError::Deserialization(format!("idempotency value JSON: {error}"))
                })?;
                Ok(Some(IdempotencyState::Complete(value)))
            }
            ("complete", None) => Err(EventStoreError::Deserialization(
                "completed idempotency row is missing value".to_owned(),
            )),
            (state, _) => Err(EventStoreError::Deserialization(format!(
                "unknown idempotency state: {state}"
            ))),
        }
    }

    fn reserve(&self, key: IdempotencyKey) -> Result<bool, Self::Error> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| EventStoreError::Poisoned)?;
        let updated_at_ms = system_time_to_millis(SystemTime::now())?;

        // MySQL INSERT IGNORE behaves like INSERT OR IGNORE / ON CONFLICT DO NOTHING
        let sql = format!(
            "INSERT IGNORE INTO {} (idempotency_key, state, value, updated_at_ms) \
             VALUES (?, 'pending', NULL, ?);",
            self.table_name
        );
        connection
            .exec_drop(&sql, (key.as_str(), updated_at_ms))
            .map_err(map_mysql_error)?;

        let affected = connection.affected_rows();
        Ok(affected == 1)
    }

    fn save(&self, key: IdempotencyKey, value: V) -> Result<(), Self::Error> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| EventStoreError::Poisoned)?;
        let updated_at_ms = system_time_to_millis(SystemTime::now())?;
        let value_json = serde_json::to_string(&value).map_err(|error| {
            EventStoreError::Serialization(format!("idempotency value JSON: {error}"))
        })?;
        let sql = format!(
            "INSERT INTO {} (idempotency_key, state, value, updated_at_ms) \
             VALUES (?, 'complete', ?, ?) \
             ON DUPLICATE KEY UPDATE \
                state = VALUES(state), \
                value = VALUES(value), \
                updated_at_ms = VALUES(updated_at_ms);",
            self.table_name
        );
        connection
            .exec_drop(&sql, (key.as_str(), value_json, updated_at_ms))
            .map_err(map_mysql_error)?;
        Ok(())
    }

    fn remove(&self, key: &IdempotencyKey) -> Result<(), Self::Error> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| EventStoreError::Poisoned)?;
        let sql = format!("DELETE FROM {} WHERE idempotency_key = ?;", self.table_name);
        connection
            .exec_drop(&sql, (key.as_str(),))
            .map_err(map_mysql_error)?;
        Ok(())
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl<V> crate::async_api::AsyncIdempotencyStore<V> for MySqlIdempotencyStore<V>
where
    V: Clone + serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    type Error = EventStoreError;

    async fn load(&self, key: &IdempotencyKey) -> Result<Option<IdempotencyState<V>>, Self::Error> {
        let this = self.clone();
        let key = key.clone();
        tokio::task::spawn_blocking(move || IdempotencyStore::load(&this, &key))
            .await
            .map_err(|e| EventStoreError::Backend(e.to_string()))?
    }

    async fn reserve(&self, key: IdempotencyKey) -> Result<bool, Self::Error> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || IdempotencyStore::reserve(&this, key))
            .await
            .map_err(|e| EventStoreError::Backend(e.to_string()))?
    }

    async fn save(&self, key: IdempotencyKey, value: V) -> Result<(), Self::Error> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || IdempotencyStore::save(&this, key, value))
            .await
            .map_err(|e| EventStoreError::Backend(e.to_string()))?
    }

    async fn remove(&self, key: &IdempotencyKey) -> Result<(), Self::Error> {
        let this = self.clone();
        let key = key.clone();
        tokio::task::spawn_blocking(move || IdempotencyStore::remove(&this, &key))
            .await
            .map_err(|e| EventStoreError::Backend(e.to_string()))?
    }
}
