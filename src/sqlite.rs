//! SQLite event store adapter.

use crate::aggregate::Aggregate;
use crate::error::EventStoreError;
use crate::event::{EventEnvelope, EventId, ExpectedRevision, NewEvent};
use crate::event_store::{
    AtomicIdempotentEventStore, EventStore, EventStream, IdempotentAppendError,
};
use crate::idempotency::{IdempotencyKey, IdempotencyState, IdempotencyStore};
use crate::snapshot::{Snapshot, SnapshotStore};
use crate::sql_common::{
    check_expected_revision, deserialize_id, deserialize_metadata, deserialize_payload,
    millis_to_system_time, serialize_id, serialize_metadata, serialize_payload,
    system_time_to_millis, validate_table_name,
};
use crate::upcast::UpcasterRegistry;
use rusqlite::{params, Connection, ErrorCode, OptionalExtension};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

/// SQLite-backed event store.
///
/// The adapter stores aggregate IDs, payloads, and metadata as JSON text. It
/// uses SQLite transactions and a unique `(aggregate_type, aggregate_id,
/// revision)` constraint for optimistic concurrency.
pub struct SqliteEventStore<A>
where
    A: Aggregate,
{
    connection: Arc<Mutex<Connection>>,
    table_name: String,
    idempotency_table: String,
    upcasters: UpcasterRegistry,
    _marker: PhantomData<fn() -> A>,
}

impl<A> Clone for SqliteEventStore<A>
where
    A: Aggregate,
{
    fn clone(&self) -> Self {
        Self {
            connection: Arc::clone(&self.connection),
            table_name: self.table_name.clone(),
            idempotency_table: self.idempotency_table.clone(),
            upcasters: self.upcasters.clone(),
            _marker: PhantomData,
        }
    }
}

impl<A> std::fmt::Debug for SqliteEventStore<A>
where
    A: Aggregate,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteEventStore")
            .field("table_name", &self.table_name)
            .field("idempotency_table", &self.idempotency_table)
            .finish_non_exhaustive()
    }
}

impl<A> SqliteEventStore<A>
where
    A: Aggregate,
{
    /// Creates a SQLite event store using the default `events` table.
    pub fn new(connection: Connection) -> Result<Self, EventStoreError> {
        Self::with_table_name(connection, "events")
    }

    /// Creates an in-memory SQLite event store and initializes its schema.
    pub fn in_memory() -> Result<Self, EventStoreError> {
        let store = Self::new(Connection::open_in_memory().map_err(map_sqlite_error)?)?;
        store.initialize_schema()?;
        Ok(store)
    }

    /// Creates a SQLite event store with a custom table name.
    pub fn with_table_name(
        connection: Connection,
        table_name: impl Into<String>,
    ) -> Result<Self, EventStoreError> {
        Self::with_table_names(connection, table_name, "idempotency_keys")
    }

    /// Creates a SQLite event store with custom event and idempotency table names.
    pub fn with_table_names(
        connection: Connection,
        table_name: impl Into<String>,
        idempotency_table: impl Into<String>,
    ) -> Result<Self, EventStoreError> {
        let table_name = table_name.into();
        let idempotency_table = idempotency_table.into();
        validate_table_name(&table_name)?;
        validate_table_name(&idempotency_table)?;

        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            table_name,
            idempotency_table,
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

    /// Migrates the SQLite schemas to the latest version.
    pub fn migrate_schema(&self) -> Result<(), EventStoreError> {
        let config = crate::schema::SqlSchemaConfig::new(crate::schema::SqlDialect::Sqlite)
            .with_events_table(&self.table_name)?
            .with_idempotency_table(&self.idempotency_table)?;
        let migrator = crate::schema::SchemaMigrator::new(config);
        let connection = self
            .connection
            .lock()
            .map_err(|_| EventStoreError::Poisoned)?;
        migrator.run_sqlite(&connection)
    }

    /// Initializes the SQLite event table and indexes.
    pub fn initialize_schema(&self) -> Result<(), EventStoreError> {
        self.migrate_schema()
    }

    fn current_revision_locked(
        table_name: &str,
        connection: &Connection,
        aggregate_id: &str,
    ) -> Result<u64, EventStoreError> {
        let query = format!(
            "SELECT COALESCE(MAX(revision), 0) FROM {table} \
             WHERE aggregate_type = ?1 AND aggregate_id = ?2",
            table = table_name
        );
        let revision: i64 = connection
            .query_row(&query, params![A::aggregate_type(), aggregate_id], |row| {
                row.get(0)
            })
            .map_err(map_sqlite_error)?;

        u64::try_from(revision).map_err(|_| {
            EventStoreError::Deserialization("stored revision cannot be negative".to_owned())
        })
    }
}

impl<A> EventStore<A> for SqliteEventStore<A>
where
    A: Aggregate + 'static,
    A::Event: serde::Serialize + serde::de::DeserializeOwned,
    A::Id: serde::Serialize + serde::de::DeserializeOwned,
{
    type Error = EventStoreError;

    fn load(&self, aggregate_id: &A::Id) -> Result<EventStream<A>, Self::Error> {
        let aggregate_id = serialize_id(aggregate_id)?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| EventStoreError::Poisoned)?;
        let query = format!(
            "SELECT event_id, aggregate_id, aggregate_type, revision, sequence, event_type, \
             event_version, payload, metadata, recorded_at_ms FROM {table} \
             WHERE aggregate_type = ?1 AND aggregate_id = ?2 ORDER BY revision ASC",
            table = self.table_name
        );
        let mut statement = connection.prepare(&query).map_err(map_sqlite_error)?;
        let upcasters = self.upcasters.clone();
        let rows = statement
            .query_map(params![A::aggregate_type(), aggregate_id], move |row| {
                row_to_envelope::<A>(&upcasters, row)
            })
            .map_err(map_sqlite_error)?;

        rows.collect::<Result<Vec<_>, _>>()
            .map_err(map_sqlite_error)
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
            .map(PreparedSqliteEvent::new)
            .collect::<Result<Vec<_>, _>>()?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| EventStoreError::Poisoned)?;
        let transaction = connection.transaction().map_err(map_sqlite_error)?;
        let actual_revision =
            Self::current_revision_locked(&self.table_name, &transaction, &aggregate_id_key)?;
        check_expected_revision(expected_revision, actual_revision)?;

        if prepared.is_empty() {
            transaction.commit().map_err(map_sqlite_error)?;
            return Ok(Vec::new());
        }

        let insert = format!(
            "INSERT INTO {table} \
             (event_id, aggregate_id, aggregate_type, revision, event_type, event_version, \
              payload, metadata, recorded_at_ms) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            table = self.table_name
        );
        let mut committed = Vec::with_capacity(prepared.len());

        for (index, event) in prepared.into_iter().enumerate() {
            let revision = actual_revision + index as u64 + 1;
            let revision_i64 = i64::try_from(revision).map_err(|_| {
                EventStoreError::Serialization("revision exceeds SQLite INTEGER".to_owned())
            })?;
            let event_version_i64 = i64::from(event.event_version);

            transaction
                .execute(
                    &insert,
                    params![
                        event.event_id.as_str(),
                        aggregate_id_key,
                        A::aggregate_type(),
                        revision_i64,
                        event.event_type,
                        event_version_i64,
                        event.payload_json,
                        event.metadata_json,
                        event.recorded_at_ms,
                    ],
                )
                .map_err(|error| {
                    map_sqlite_insert_error(error, expected_revision, actual_revision)
                })?;
            let sequence = transaction.last_insert_rowid();
            let sequence = u64::try_from(sequence).map_err(|_| {
                EventStoreError::Deserialization("SQLite sequence cannot be negative".to_owned())
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

        transaction.commit().map_err(map_sqlite_error)?;
        Ok(committed)
    }

    fn load_global_after(&self, sequence: Option<u64>) -> Result<EventStream<A>, Self::Error> {
        let sequence = sequence.unwrap_or_default();
        let sequence = i64::try_from(sequence).map_err(|_| {
            EventStoreError::Deserialization("global sequence exceeds SQLite INTEGER".to_owned())
        })?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| EventStoreError::Poisoned)?;
        let query = format!(
            "SELECT event_id, aggregate_id, aggregate_type, revision, sequence, event_type, \
             event_version, payload, metadata, recorded_at_ms FROM {table} \
             WHERE aggregate_type = ?1 AND sequence > ?2 ORDER BY sequence ASC",
            table = self.table_name
        );
        let mut statement = connection.prepare(&query).map_err(map_sqlite_error)?;
        let upcasters = self.upcasters.clone();
        let rows = statement
            .query_map(params![A::aggregate_type(), sequence], move |row| {
                row_to_envelope::<A>(&upcasters, row)
            })
            .map_err(map_sqlite_error)?;

        rows.collect::<Result<Vec<_>, _>>()
            .map_err(map_sqlite_error)
    }
}

impl<A> AtomicIdempotentEventStore<A> for SqliteEventStore<A>
where
    A: Aggregate + 'static,
    A::Event: serde::Serialize + serde::de::DeserializeOwned,
    A::Id: serde::Serialize + serde::de::DeserializeOwned,
{
    fn append_idempotent(
        &self,
        idempotency_key: IdempotencyKey,
        aggregate_id: &A::Id,
        expected_revision: ExpectedRevision,
        events: Vec<NewEvent<A::Event>>,
    ) -> Result<EventStream<A>, IdempotentAppendError<Self::Error>> {
        #[cfg(feature = "tracing")]
        let _span = tracing::debug_span!(
            "event_store.append_idempotent",
            dialect = "sqlite",
            aggregate_type = A::aggregate_type(),
            expected_revision = ?expected_revision,
            event_count = events.len()
        )
        .entered();

        let aggregate_id_key = serialize_id(aggregate_id).map_err(IdempotentAppendError::Store)?;
        let prepared = events
            .into_iter()
            .map(PreparedSqliteEvent::new)
            .collect::<Result<Vec<_>, _>>()
            .map_err(IdempotentAppendError::Store)?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| IdempotentAppendError::Store(EventStoreError::Poisoned))?;
        let transaction = connection
            .transaction()
            .map_err(|error| IdempotentAppendError::Store(map_sqlite_error(error)))?;

        let load_idempotency = format!(
            "SELECT state, value FROM {} WHERE idempotency_key = ?1;",
            self.idempotency_table
        );
        let row = transaction
            .query_row(
                &load_idempotency,
                params![idempotency_key.as_str()],
                |row| {
                    let state: String = row.get(0)?;
                    let value: Option<String> = row.get(1)?;
                    Ok((state, value))
                },
            )
            .optional()
            .map_err(|error| IdempotentAppendError::Store(map_sqlite_error(error)))?;

        match row {
            Some((state, Some(value))) if state == "complete" => {
                let committed = serde_json::from_str(&value).map_err(|error| {
                    IdempotentAppendError::Store(EventStoreError::Deserialization(format!(
                        "idempotent committed events JSON: {error}"
                    )))
                })?;
                transaction
                    .commit()
                    .map_err(|error| IdempotentAppendError::Store(map_sqlite_error(error)))?;
                return Ok(committed);
            }
            Some((state, None)) if state == "complete" => {
                return Err(IdempotentAppendError::Store(
                    EventStoreError::Deserialization(
                        "completed idempotency row is missing value".to_owned(),
                    ),
                ));
            }
            Some((state, _)) if state == "pending" => {
                return Err(IdempotentAppendError::Pending {
                    key: idempotency_key,
                });
            }
            Some((state, _)) => {
                return Err(IdempotentAppendError::Store(
                    EventStoreError::Deserialization(format!("unknown idempotency state: {state}")),
                ));
            }
            None => {}
        }

        let updated_at_ms =
            system_time_to_millis(SystemTime::now()).map_err(IdempotentAppendError::Store)?;
        let reserve = format!(
            "INSERT INTO {} (idempotency_key, state, value, updated_at_ms)
             VALUES (?1, 'pending', NULL, ?2);",
            self.idempotency_table
        );
        transaction
            .execute(&reserve, params![idempotency_key.as_str(), updated_at_ms])
            .map_err(|error| IdempotentAppendError::Store(map_sqlite_error(error)))?;

        let actual_revision =
            Self::current_revision_locked(&self.table_name, &transaction, &aggregate_id_key)
                .map_err(IdempotentAppendError::Store)?;
        check_expected_revision(expected_revision, actual_revision)
            .map_err(IdempotentAppendError::Store)?;

        let insert = format!(
            "INSERT INTO {table} \
             (event_id, aggregate_id, aggregate_type, revision, event_type, event_version, \
              payload, metadata, recorded_at_ms) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            table = self.table_name
        );
        let mut committed = Vec::with_capacity(prepared.len());

        for (index, event) in prepared.into_iter().enumerate() {
            let revision = actual_revision + index as u64 + 1;
            let revision_i64 = i64::try_from(revision).map_err(|_| {
                IdempotentAppendError::Store(EventStoreError::Serialization(
                    "revision exceeds SQLite INTEGER".to_owned(),
                ))
            })?;
            let event_version_i64 = i64::from(event.event_version);

            transaction
                .execute(
                    &insert,
                    params![
                        event.event_id.as_str(),
                        aggregate_id_key,
                        A::aggregate_type(),
                        revision_i64,
                        event.event_type,
                        event_version_i64,
                        event.payload_json,
                        event.metadata_json,
                        event.recorded_at_ms,
                    ],
                )
                .map_err(|error| {
                    IdempotentAppendError::Store(map_sqlite_insert_error(
                        error,
                        expected_revision,
                        actual_revision,
                    ))
                })?;
            let sequence = transaction.last_insert_rowid();
            let sequence = u64::try_from(sequence).map_err(|_| {
                IdempotentAppendError::Store(EventStoreError::Deserialization(
                    "SQLite sequence cannot be negative".to_owned(),
                ))
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

        let value_json = serde_json::to_string(&committed).map_err(|error| {
            IdempotentAppendError::Store(EventStoreError::Serialization(format!(
                "idempotent committed events JSON: {error}"
            )))
        })?;
        let complete = format!(
            "UPDATE {} SET state = 'complete', value = ?2, updated_at_ms = ?3
             WHERE idempotency_key = ?1;",
            self.idempotency_table
        );
        transaction
            .execute(
                &complete,
                params![idempotency_key.as_str(), value_json, updated_at_ms],
            )
            .map_err(|error| IdempotentAppendError::Store(map_sqlite_error(error)))?;
        transaction
            .commit()
            .map_err(|error| IdempotentAppendError::Store(map_sqlite_error(error)))?;
        Ok(committed)
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl<A> crate::async_api::AsyncEventStore<A> for SqliteEventStore<A>
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
            .map_err(|error| EventStoreError::Backend(error.to_string()))?
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
        .map_err(|error| EventStoreError::Backend(error.to_string()))?
    }

    async fn load_global_after(
        &self,
        sequence: Option<u64>,
    ) -> Result<EventStream<A>, Self::Error> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || EventStore::load_global_after(&this, sequence))
            .await
            .map_err(|error| EventStoreError::Backend(error.to_string()))?
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl<A> crate::async_api::AsyncAtomicIdempotentEventStore<A> for SqliteEventStore<A>
where
    A: Aggregate + Send + Sync + 'static,
    A::Event: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
    A::Id: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    async fn append_idempotent(
        &self,
        idempotency_key: IdempotencyKey,
        aggregate_id: &A::Id,
        expected_revision: ExpectedRevision,
        events: Vec<NewEvent<A::Event>>,
    ) -> Result<EventStream<A>, IdempotentAppendError<Self::Error>> {
        let this = self.clone();
        let aggregate_id = aggregate_id.clone();
        tokio::task::spawn_blocking(move || {
            AtomicIdempotentEventStore::append_idempotent(
                &this,
                idempotency_key,
                &aggregate_id,
                expected_revision,
                events,
            )
        })
        .await
        .map_err(|error| {
            IdempotentAppendError::Store(EventStoreError::Backend(error.to_string()))
        })?
    }
}

struct PreparedSqliteEvent<E> {
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

impl<E> PreparedSqliteEvent<E>
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
            event_type: event.event_type.into_string(),
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
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<EventEnvelope<A::Event, A::Id>>
where
    A: Aggregate,
    A::Event: serde::de::DeserializeOwned,
    A::Id: serde::de::DeserializeOwned,
{
    let event_id: String = row.get(0)?;
    let aggregate_id: String = row.get(1)?;
    let aggregate_type: String = row.get(2)?;
    let revision: i64 = row.get(3)?;
    let sequence: i64 = row.get(4)?;
    let event_type: String = row.get(5)?;
    let event_version: i64 = row.get(6)?;
    let payload: String = row.get(7)?;
    let metadata: String = row.get(8)?;
    let recorded_at_ms: i64 = row.get(9)?;

    let revision = u64::try_from(revision).map_err(|_| {
        from_event_store_error(EventStoreError::Deserialization(
            "stored revision cannot be negative".to_owned(),
        ))
    })?;
    let sequence = u64::try_from(sequence).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            4,
            rusqlite::types::Type::Integer,
            Box::new(EventStoreError::Deserialization(
                "SQLite sequence cannot be negative".to_owned(),
            )),
        )
    })?;
    let event_version = u32::try_from(event_version).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            6,
            rusqlite::types::Type::Integer,
            Box::new(EventStoreError::Deserialization(
                "event_version exceeds u32".to_owned(),
            )),
        )
    })?;
    let aggregate_id = deserialize_id(&aggregate_id).map_err(from_event_store_error)?;

    let (event_version, upcasted_bytes) = upcasters
        .upcast(&event_type, event_version, payload.into_bytes())
        .map_err(|err| from_event_store_error(EventStoreError::Deserialization(err.to_string())))?;

    let payload_value = serde_json::from_slice(&upcasted_bytes).map_err(|error| {
        from_event_store_error(EventStoreError::Deserialization(format!(
            "payload JSON: {error}"
        )))
    })?;
    let payload = deserialize_payload(&event_id, &event_type, payload_value)
        .map_err(from_event_store_error)?;
    let metadata_value = serde_json::from_str(&metadata).map_err(|error| {
        from_event_store_error(EventStoreError::Deserialization(format!(
            "metadata JSON: {error}"
        )))
    })?;
    let metadata =
        deserialize_metadata(&event_id, metadata_value).map_err(from_event_store_error)?;
    let recorded_at = millis_to_system_time(recorded_at_ms).map_err(from_event_store_error)?;

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

fn map_sqlite_insert_error(
    error: rusqlite::Error,
    expected: ExpectedRevision,
    actual: u64,
) -> EventStoreError {
    match &error {
        rusqlite::Error::SqliteFailure(failure, _)
            if failure.code == ErrorCode::ConstraintViolation =>
        {
            EventStoreError::Concurrency(crate::ConcurrencyError::WrongExpectedRevision {
                expected,
                actual,
            })
        }
        _ => map_sqlite_error(error),
    }
}

fn map_sqlite_error(error: rusqlite::Error) -> EventStoreError {
    EventStoreError::backend_with_source(error.to_string(), error)
}

fn from_event_store_error(error: EventStoreError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
}

/// SQLite checkpoint store implementation.
#[derive(Clone, Debug)]
pub struct SqliteCheckpointStore {
    connection: Arc<Mutex<Connection>>,
    table_name: String,
}

impl SqliteCheckpointStore {
    /// Creates a SQLite checkpoint store using the default table name.
    pub fn new(connection: Connection) -> Result<Self, EventStoreError> {
        Self::with_table_name(connection, "projection_checkpoints")
    }

    /// Creates a SQLite checkpoint store with a custom table name.
    pub fn with_table_name(
        connection: Connection,
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
        let config = crate::schema::SqlSchemaConfig::new(crate::schema::SqlDialect::Sqlite)
            .with_checkpoints_table(&self.table_name)?;
        let migrator = crate::schema::SchemaMigrator::new(config);
        let connection = self
            .connection
            .lock()
            .map_err(|_| EventStoreError::Poisoned)?;
        migrator.run_sqlite(&connection)
    }
}

impl crate::projection::CheckpointStore for SqliteCheckpointStore {
    type Error = EventStoreError;

    fn load_checkpoint(&self, projection_name: &str) -> Result<Option<u64>, Self::Error> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| EventStoreError::Poisoned)?;
        let sql = format!(
            "SELECT sequence FROM {} WHERE projection_name = ?1;",
            self.table_name
        );
        let mut stmt = connection.prepare(&sql).map_err(map_sqlite_error)?;
        let mut rows = stmt
            .query(params![projection_name])
            .map_err(map_sqlite_error)?;

        if let Some(row) = rows.next().map_err(map_sqlite_error)? {
            let sequence: i64 = row.get(0).map_err(map_sqlite_error)?;
            let sequence = u64::try_from(sequence).map_err(|_| {
                EventStoreError::Deserialization("SQLite checkpoint cannot be negative".to_owned())
            })?;
            Ok(Some(sequence))
        } else {
            Ok(None)
        }
    }

    fn save_checkpoint(&self, projection_name: &str, sequence: u64) -> Result<(), Self::Error> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| EventStoreError::Poisoned)?;
        let sql = format!(
            "INSERT INTO {} (projection_name, sequence) VALUES (?1, ?2)
             ON CONFLICT(projection_name) DO UPDATE SET sequence = CASE
                WHEN excluded.sequence > {table}.sequence THEN excluded.sequence
                ELSE {table}.sequence
             END;",
            self.table_name,
            table = self.table_name
        );
        let sequence_i64 = i64::try_from(sequence)
            .map_err(|_| EventStoreError::Deserialization("checkpoint exceeds i64".to_owned()))?;
        connection
            .execute(&sql, params![projection_name, sequence_i64])
            .map_err(map_sqlite_error)?;
        Ok(())
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl crate::projection::AsyncCheckpointStore for SqliteCheckpointStore {
    type Error = EventStoreError;

    async fn load_checkpoint(&self, projection_name: &str) -> Result<Option<u64>, Self::Error> {
        let this = self.clone();
        let name = projection_name.to_owned();
        tokio::task::spawn_blocking(move || {
            crate::projection::CheckpointStore::load_checkpoint(&this, &name)
        })
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
            crate::projection::CheckpointStore::save_checkpoint(&this, &name, sequence)
        })
        .await
        .map_err(|e| EventStoreError::Backend(e.to_string()))?
    }
}

/// SQLite-backed idempotency store.
///
/// The store persists pending reservations and completed JSON-serializable
/// values so command retries can be deduplicated across process restarts.
pub struct SqliteIdempotencyStore<V>
where
    V: Clone,
{
    connection: Arc<Mutex<Connection>>,
    table_name: String,
    _marker: PhantomData<fn() -> V>,
}

impl<V> Clone for SqliteIdempotencyStore<V>
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

impl<V> std::fmt::Debug for SqliteIdempotencyStore<V>
where
    V: Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteIdempotencyStore")
            .field("table_name", &self.table_name)
            .finish_non_exhaustive()
    }
}

impl<V> SqliteIdempotencyStore<V>
where
    V: Clone,
{
    /// Creates a SQLite idempotency store using the default table name.
    pub fn new(connection: Connection) -> Result<Self, EventStoreError> {
        Self::with_table_name(connection, "idempotency_keys")
    }

    /// Creates a SQLite idempotency store with a custom table name.
    pub fn with_table_name(
        connection: Connection,
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
        let config = crate::schema::SqlSchemaConfig::new(crate::schema::SqlDialect::Sqlite)
            .with_idempotency_table(&self.table_name)?;
        let migrator = crate::schema::SchemaMigrator::new(config);
        let connection = self
            .connection
            .lock()
            .map_err(|_| EventStoreError::Poisoned)?;
        migrator.run_sqlite(&connection)
    }
}

impl<V> IdempotencyStore<V> for SqliteIdempotencyStore<V>
where
    V: Clone + serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    type Error = EventStoreError;

    fn load(&self, key: &IdempotencyKey) -> Result<Option<IdempotencyState<V>>, Self::Error> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| EventStoreError::Poisoned)?;
        let sql = format!(
            "SELECT state, value FROM {} WHERE idempotency_key = ?1;",
            self.table_name
        );
        let row = connection
            .query_row(&sql, params![key.as_str()], |row| {
                let state: String = row.get(0)?;
                let value: Option<String> = row.get(1)?;
                Ok((state, value))
            })
            .optional()
            .map_err(map_sqlite_error)?;

        match row {
            None => Ok(None),
            Some((state, _)) if state == "pending" => Ok(Some(IdempotencyState::Pending)),
            Some((state, Some(value))) if state == "complete" => {
                let value = serde_json::from_str(&value).map_err(|error| {
                    EventStoreError::Deserialization(format!("idempotency value JSON: {error}"))
                })?;
                Ok(Some(IdempotencyState::Complete(value)))
            }
            Some((state, None)) if state == "complete" => Err(EventStoreError::Deserialization(
                "completed idempotency row is missing value".to_owned(),
            )),
            Some((state, _)) => Err(EventStoreError::Deserialization(format!(
                "unknown idempotency state: {state}"
            ))),
        }
    }

    fn reserve(&self, key: IdempotencyKey) -> Result<bool, Self::Error> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| EventStoreError::Poisoned)?;
        let updated_at_ms = system_time_to_millis(SystemTime::now())?;
        let sql = format!(
            "INSERT OR IGNORE INTO {} (idempotency_key, state, value, updated_at_ms)
             VALUES (?1, 'pending', NULL, ?2);",
            self.table_name
        );
        let changed = connection
            .execute(&sql, params![key.as_str(), updated_at_ms])
            .map_err(map_sqlite_error)?;
        Ok(changed == 1)
    }

    fn save(&self, key: IdempotencyKey, value: V) -> Result<(), Self::Error> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| EventStoreError::Poisoned)?;
        let updated_at_ms = system_time_to_millis(SystemTime::now())?;
        let value_json = serde_json::to_string(&value).map_err(|error| {
            EventStoreError::Serialization(format!("idempotency value JSON: {error}"))
        })?;
        let sql = format!(
            "INSERT INTO {} (idempotency_key, state, value, updated_at_ms)
             VALUES (?1, 'complete', ?2, ?3)
             ON CONFLICT(idempotency_key) DO UPDATE SET
                state = excluded.state,
                value = excluded.value,
                updated_at_ms = excluded.updated_at_ms;",
            self.table_name
        );
        connection
            .execute(&sql, params![key.as_str(), value_json, updated_at_ms])
            .map_err(map_sqlite_error)?;
        Ok(())
    }

    fn remove(&self, key: &IdempotencyKey) -> Result<(), Self::Error> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| EventStoreError::Poisoned)?;
        let sql = format!(
            "DELETE FROM {} WHERE idempotency_key = ?1;",
            self.table_name
        );
        connection
            .execute(&sql, params![key.as_str()])
            .map_err(map_sqlite_error)?;
        Ok(())
    }
}

/// SQLite-backed durable snapshot store.
pub struct SqliteSnapshotStore<A>
where
    A: Aggregate,
{
    connection: Arc<Mutex<Connection>>,
    table_name: String,
    _marker: PhantomData<fn() -> A>,
}

impl<A> Clone for SqliteSnapshotStore<A>
where
    A: Aggregate,
{
    fn clone(&self) -> Self {
        Self {
            connection: Arc::clone(&self.connection),
            table_name: self.table_name.clone(),
            _marker: PhantomData,
        }
    }
}

impl<A> std::fmt::Debug for SqliteSnapshotStore<A>
where
    A: Aggregate,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteSnapshotStore")
            .field("table_name", &self.table_name)
            .finish_non_exhaustive()
    }
}

impl<A> SqliteSnapshotStore<A>
where
    A: Aggregate,
{
    /// Creates a SQLite snapshot store using the default table name.
    pub fn new(connection: Connection) -> Result<Self, EventStoreError> {
        Self::with_table_name(connection, "snapshots")
    }

    /// Creates a SQLite snapshot store with a custom table name.
    pub fn with_table_name(
        connection: Connection,
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

    /// Initializes the snapshot schema table.
    pub fn initialize_schema(&self) -> Result<(), EventStoreError> {
        let config = crate::schema::SqlSchemaConfig::new(crate::schema::SqlDialect::Sqlite)
            .with_snapshots_table(&self.table_name)?;
        let migrator = crate::schema::SchemaMigrator::new(config);
        let connection = self
            .connection
            .lock()
            .map_err(|_| EventStoreError::Poisoned)?;
        migrator.run_sqlite(&connection)
    }
}

impl<A> SnapshotStore<A> for SqliteSnapshotStore<A>
where
    A: Aggregate + serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
    A::Id: serde::Serialize + serde::de::DeserializeOwned,
{
    type Error = EventStoreError;

    fn load_snapshot(&self, aggregate_id: &A::Id) -> Result<Option<Snapshot<A>>, Self::Error> {
        #[cfg(feature = "tracing")]
        let _span = tracing::debug_span!(
            "snapshot.load",
            dialect = "sqlite",
            aggregate_type = A::aggregate_type()
        )
        .entered();

        let aggregate_id = serialize_id(aggregate_id)?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| EventStoreError::Poisoned)?;
        let sql = format!(
            "SELECT revision, state, metadata, recorded_at_ms FROM {} \
             WHERE aggregate_type = ?1 AND aggregate_id = ?2;",
            self.table_name
        );
        let row = connection
            .query_row(&sql, params![A::aggregate_type(), aggregate_id], |row| {
                let revision: i64 = row.get(0)?;
                let state: String = row.get(1)?;
                let metadata: String = row.get(2)?;
                let recorded_at_ms: i64 = row.get(3)?;
                Ok((revision, state, metadata, recorded_at_ms))
            })
            .optional()
            .map_err(map_sqlite_error)?;

        let Some((revision, state, metadata, recorded_at_ms)) = row else {
            return Ok(None);
        };

        let revision = u64::try_from(revision).map_err(|_| {
            EventStoreError::Deserialization(
                "SQLite snapshot revision cannot be negative".to_owned(),
            )
        })?;
        let state = serde_json::from_str(&state).map_err(|error| {
            EventStoreError::Deserialization(format!("snapshot state JSON: {error}"))
        })?;
        let metadata = serde_json::from_str(&metadata).map_err(|error| {
            EventStoreError::Deserialization(format!("snapshot metadata JSON: {error}"))
        })?;
        let recorded_at = millis_to_system_time(recorded_at_ms)?;
        let aggregate_id = deserialize_id(&aggregate_id)?;

        Ok(Some(Snapshot {
            aggregate_id,
            aggregate_type: A::aggregate_type().to_owned(),
            revision,
            state,
            metadata,
            recorded_at,
        }))
    }

    fn save_snapshot(&self, snapshot: Snapshot<A>) -> Result<(), Self::Error> {
        #[cfg(feature = "tracing")]
        let _span = tracing::debug_span!(
            "snapshot.save",
            dialect = "sqlite",
            aggregate_type = A::aggregate_type(),
            revision = snapshot.revision
        )
        .entered();

        let aggregate_id = serialize_id(&snapshot.aggregate_id)?;
        let revision_i64 = i64::try_from(snapshot.revision).map_err(|_| {
            EventStoreError::Serialization("snapshot revision exceeds i64".to_owned())
        })?;
        let state_json = serde_json::to_string(&snapshot.state).map_err(|error| {
            EventStoreError::Serialization(format!("snapshot state JSON: {error}"))
        })?;
        let metadata_json = serde_json::to_string(&snapshot.metadata).map_err(|error| {
            EventStoreError::Serialization(format!("snapshot metadata JSON: {error}"))
        })?;
        let recorded_at_ms = system_time_to_millis(snapshot.recorded_at)?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| EventStoreError::Poisoned)?;
        let sql = format!(
            "INSERT INTO {} (aggregate_type, aggregate_id, revision, state, metadata, recorded_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(aggregate_type, aggregate_id) DO UPDATE SET
                revision = excluded.revision,
                state = excluded.state,
                metadata = excluded.metadata,
                recorded_at_ms = excluded.recorded_at_ms
             WHERE excluded.revision >= {}.revision;",
            self.table_name, self.table_name
        );
        connection
            .execute(
                &sql,
                params![
                    A::aggregate_type(),
                    aggregate_id,
                    revision_i64,
                    state_json,
                    metadata_json,
                    recorded_at_ms,
                ],
            )
            .map_err(map_sqlite_error)?;
        Ok(())
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl<A> crate::async_api::AsyncSnapshotStore<A> for SqliteSnapshotStore<A>
where
    A: Aggregate + serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
    A::Id: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    type Error = EventStoreError;

    async fn load_snapshot(
        &self,
        aggregate_id: &A::Id,
    ) -> Result<Option<Snapshot<A>>, Self::Error> {
        let this = self.clone();
        let aggregate_id = aggregate_id.clone();
        tokio::task::spawn_blocking(move || SnapshotStore::load_snapshot(&this, &aggregate_id))
            .await
            .map_err(|e| EventStoreError::Backend(e.to_string()))?
    }

    async fn save_snapshot(&self, snapshot: Snapshot<A>) -> Result<(), Self::Error> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || SnapshotStore::save_snapshot(&this, snapshot))
            .await
            .map_err(|e| EventStoreError::Backend(e.to_string()))?
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl<V> crate::async_api::AsyncIdempotencyStore<V> for SqliteIdempotencyStore<V>
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
