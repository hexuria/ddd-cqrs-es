//! PostgreSQL event store adapter.

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
use ::postgres::{Client, NoTls};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

/// PostgreSQL-backed event store.
///
/// The adapter stores payloads and metadata as `JSONB`, assigns global sequence
/// numbers through `BIGSERIAL`, and enforces optimistic concurrency with a
/// unique `(aggregate_type, aggregate_id, revision)` constraint.
pub struct PostgresEventStore<A>
where
    A: Aggregate,
{
    client: Arc<Mutex<Client>>,
    table_name: String,
    idempotency_table: String,
    upcasters: UpcasterRegistry,
    _marker: PhantomData<fn() -> A>,
}

impl<A> Clone for PostgresEventStore<A>
where
    A: Aggregate,
{
    fn clone(&self) -> Self {
        Self {
            client: Arc::clone(&self.client),
            table_name: self.table_name.clone(),
            idempotency_table: self.idempotency_table.clone(),
            upcasters: self.upcasters.clone(),
            _marker: PhantomData,
        }
    }
}

impl<A> std::fmt::Debug for PostgresEventStore<A>
where
    A: Aggregate,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresEventStore")
            .field("table_name", &self.table_name)
            .field("idempotency_table", &self.idempotency_table)
            .finish_non_exhaustive()
    }
}

impl<A> PostgresEventStore<A>
where
    A: Aggregate,
{
    /// Connects to PostgreSQL using [`NoTls`] and the default `events` table.
    pub fn connect(params: &str) -> Result<Self, EventStoreError> {
        let client = Client::connect(params, NoTls).map_err(map_postgres_error)?;
        Self::new(client)
    }

    /// Connects to PostgreSQL using [`NoTls`] and a custom table name.
    pub fn connect_with_table_name(
        params: &str,
        table_name: impl Into<String>,
    ) -> Result<Self, EventStoreError> {
        let client = Client::connect(params, NoTls).map_err(map_postgres_error)?;
        Self::with_table_name(client, table_name)
    }

    /// Creates a PostgreSQL event store using the default `events` table.
    pub fn new(client: Client) -> Result<Self, EventStoreError> {
        Self::with_table_name(client, "events")
    }

    /// Creates a PostgreSQL event store with a custom table name.
    pub fn with_table_name(
        client: Client,
        table_name: impl Into<String>,
    ) -> Result<Self, EventStoreError> {
        Self::with_table_names(client, table_name, "idempotency_keys")
    }

    /// Creates a PostgreSQL event store with custom event and idempotency table names.
    pub fn with_table_names(
        client: Client,
        table_name: impl Into<String>,
        idempotency_table: impl Into<String>,
    ) -> Result<Self, EventStoreError> {
        let table_name = table_name.into();
        let idempotency_table = idempotency_table.into();
        validate_table_name(&table_name)?;
        validate_table_name(&idempotency_table)?;

        Ok(Self {
            client: Arc::new(Mutex::new(client)),
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

    /// Migrates the PostgreSQL schemas to the latest version.
    pub fn migrate_schema(&self) -> Result<(), EventStoreError> {
        let config = crate::schema::SqlSchemaConfig::new(crate::schema::SqlDialect::Postgres)
            .with_events_table(&self.table_name)?
            .with_idempotency_table(&self.idempotency_table)?;
        let migrator = crate::schema::SchemaMigrator::new(config);
        let mut client = self.client.lock().map_err(|_| EventStoreError::Poisoned)?;
        migrator.run_postgres(&mut client)
    }

    /// Initializes the PostgreSQL event table and indexes.
    pub fn initialize_schema(&self) -> Result<(), EventStoreError> {
        self.migrate_schema()
    }
}

impl<A> EventStore<A> for PostgresEventStore<A>
where
    A: Aggregate + 'static,
    A::Event: serde::Serialize + serde::de::DeserializeOwned,
    A::Id: serde::Serialize + serde::de::DeserializeOwned,
{
    type Error = EventStoreError;

    fn load(&self, aggregate_id: &A::Id) -> Result<EventStream<A>, Self::Error> {
        let aggregate_id = serialize_id(aggregate_id)?;
        let mut client = self.client.lock().map_err(|_| EventStoreError::Poisoned)?;
        let query = format!(
            "SELECT event_id, aggregate_id, aggregate_type, revision, sequence, event_type, \
             event_version, payload, metadata, recorded_at_ms FROM {table} \
             WHERE aggregate_type = $1 AND aggregate_id = $2 ORDER BY revision ASC",
            table = self.table_name
        );
        let rows = client
            .query(&query, &[&A::aggregate_type(), &aggregate_id])
            .map_err(map_postgres_error)?;

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
            .map(PreparedPostgresEvent::new)
            .collect::<Result<Vec<_>, _>>()?;
        let mut client = self.client.lock().map_err(|_| EventStoreError::Poisoned)?;
        let mut transaction = client.transaction().map_err(map_postgres_error)?;
        let revision_query = format!(
            "SELECT COALESCE(MAX(revision), 0)::BIGINT FROM {table} \
             WHERE aggregate_type = $1 AND aggregate_id = $2",
            table = self.table_name
        );
        let actual_revision: i64 = transaction
            .query_one(&revision_query, &[&A::aggregate_type(), &aggregate_id_key])
            .and_then(|row| row.try_get(0))
            .map_err(map_postgres_error)?;
        let actual_revision = u64::try_from(actual_revision).map_err(|_| {
            EventStoreError::Deserialization("stored revision cannot be negative".to_owned())
        })?;
        check_expected_revision(expected_revision, actual_revision)?;

        if prepared.is_empty() {
            transaction.commit().map_err(map_postgres_error)?;
            return Ok(Vec::new());
        }

        let insert = format!(
            "INSERT INTO {table} \
             (event_id, aggregate_id, aggregate_type, revision, event_type, event_version, \
              payload, metadata, recorded_at_ms) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING sequence",
            table = self.table_name
        );
        let mut committed = Vec::with_capacity(prepared.len());

        for (index, event) in prepared.into_iter().enumerate() {
            let revision = actual_revision + index as u64 + 1;
            let revision_i64 = i64::try_from(revision).map_err(|_| {
                EventStoreError::Serialization("revision exceeds BIGINT".to_owned())
            })?;
            let event_version_i32 = i32::try_from(event.event_version).map_err(|_| {
                EventStoreError::Serialization("event_version exceeds INT".to_owned())
            })?;
            let row = transaction
                .query_one(
                    &insert,
                    &[
                        &event.event_id.as_str(),
                        &aggregate_id_key,
                        &A::aggregate_type(),
                        &revision_i64,
                        &event.event_type,
                        &event_version_i32,
                        &event.payload_json,
                        &event.metadata_json,
                        &event.recorded_at_ms,
                    ],
                )
                .map_err(|error| {
                    map_postgres_insert_error(error, expected_revision, actual_revision)
                })?;
            let sequence: i64 = row.try_get(0).map_err(map_postgres_error)?;
            let sequence = u64::try_from(sequence).map_err(|_| {
                EventStoreError::Deserialization(
                    "PostgreSQL sequence cannot be negative".to_owned(),
                )
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

        transaction.commit().map_err(map_postgres_error)?;
        Ok(committed)
    }

    fn load_global_after(&self, sequence: Option<u64>) -> Result<EventStream<A>, Self::Error> {
        let sequence = sequence.unwrap_or_default();
        let sequence = i64::try_from(sequence).map_err(|_| {
            EventStoreError::Deserialization("global sequence exceeds BIGINT".to_owned())
        })?;
        let mut client = self.client.lock().map_err(|_| EventStoreError::Poisoned)?;
        let query = format!(
            "SELECT event_id, aggregate_id, aggregate_type, revision, sequence, event_type, \
             event_version, payload, metadata, recorded_at_ms FROM {table} \
             WHERE aggregate_type = $1 AND sequence > $2 ORDER BY sequence ASC",
            table = self.table_name
        );
        let rows = client
            .query(&query, &[&A::aggregate_type(), &sequence])
            .map_err(map_postgres_error)?;

        let upcasters = self.upcasters.clone();
        rows.into_iter()
            .map(|row| row_to_envelope::<A>(&upcasters, row))
            .collect()
    }
}

impl<A> AtomicIdempotentEventStore<A> for PostgresEventStore<A>
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
            dialect = "postgres",
            aggregate_type = A::aggregate_type(),
            expected_revision = ?expected_revision,
            event_count = events.len()
        )
        .entered();

        let aggregate_id_key = serialize_id(aggregate_id).map_err(IdempotentAppendError::Store)?;
        let prepared = events
            .into_iter()
            .map(PreparedPostgresEvent::new)
            .collect::<Result<Vec<_>, _>>()
            .map_err(IdempotentAppendError::Store)?;
        let mut client = self
            .client
            .lock()
            .map_err(|_| IdempotentAppendError::Store(EventStoreError::Poisoned))?;
        let mut transaction = client
            .transaction()
            .map_err(|error| IdempotentAppendError::Store(map_postgres_error(error)))?;

        let load_idempotency = format!(
            "SELECT state, value FROM {} WHERE idempotency_key = $1;",
            self.idempotency_table
        );
        let row = transaction
            .query_opt(&load_idempotency, &[&idempotency_key.as_str()])
            .map_err(|error| IdempotentAppendError::Store(map_postgres_error(error)))?;

        if let Some(row) = row {
            let state: String = row
                .try_get(0)
                .map_err(|error| IdempotentAppendError::Store(map_postgres_error(error)))?;
            let value: Option<serde_json::Value> = row
                .try_get(1)
                .map_err(|error| IdempotentAppendError::Store(map_postgres_error(error)))?;
            match (state.as_str(), value) {
                ("complete", Some(value)) => {
                    let committed = serde_json::from_value(value).map_err(|error| {
                        IdempotentAppendError::Store(EventStoreError::Deserialization(format!(
                            "idempotent committed events JSON: {error}"
                        )))
                    })?;
                    transaction
                        .commit()
                        .map_err(|error| IdempotentAppendError::Store(map_postgres_error(error)))?;
                    return Ok(committed);
                }
                ("complete", None) => {
                    return Err(IdempotentAppendError::Store(
                        EventStoreError::Deserialization(
                            "completed idempotency row is missing value".to_owned(),
                        ),
                    ));
                }
                ("pending", _) => {
                    return Err(IdempotentAppendError::Pending {
                        key: idempotency_key,
                    });
                }
                (state, _) => {
                    return Err(IdempotentAppendError::Store(
                        EventStoreError::Deserialization(format!(
                            "unknown idempotency state: {state}"
                        )),
                    ));
                }
            }
        }

        let updated_at_ms =
            system_time_to_millis(SystemTime::now()).map_err(IdempotentAppendError::Store)?;
        let reserve = format!(
            "INSERT INTO {} (idempotency_key, state, value, updated_at_ms)
             VALUES ($1, 'pending', NULL, $2);",
            self.idempotency_table
        );
        transaction
            .execute(&reserve, &[&idempotency_key.as_str(), &updated_at_ms])
            .map_err(|error| IdempotentAppendError::Store(map_postgres_error(error)))?;

        let revision_query = format!(
            "SELECT COALESCE(MAX(revision), 0)::BIGINT FROM {table} \
             WHERE aggregate_type = $1 AND aggregate_id = $2",
            table = self.table_name
        );
        let actual_revision: i64 = transaction
            .query_one(&revision_query, &[&A::aggregate_type(), &aggregate_id_key])
            .and_then(|row| row.try_get(0))
            .map_err(|error| IdempotentAppendError::Store(map_postgres_error(error)))?;
        let actual_revision = u64::try_from(actual_revision).map_err(|_| {
            IdempotentAppendError::Store(EventStoreError::Deserialization(
                "stored revision cannot be negative".to_owned(),
            ))
        })?;
        check_expected_revision(expected_revision, actual_revision)
            .map_err(IdempotentAppendError::Store)?;

        let insert = format!(
            "INSERT INTO {table} \
             (event_id, aggregate_id, aggregate_type, revision, event_type, event_version, \
              payload, metadata, recorded_at_ms) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING sequence",
            table = self.table_name
        );
        let mut committed = Vec::with_capacity(prepared.len());

        for (index, event) in prepared.into_iter().enumerate() {
            let revision = actual_revision + index as u64 + 1;
            let revision_i64 = i64::try_from(revision).map_err(|_| {
                IdempotentAppendError::Store(EventStoreError::Serialization(
                    "revision exceeds BIGINT".to_owned(),
                ))
            })?;
            let event_version_i32 = i32::try_from(event.event_version).map_err(|_| {
                IdempotentAppendError::Store(EventStoreError::Serialization(
                    "event_version exceeds INT".to_owned(),
                ))
            })?;
            let row = transaction
                .query_one(
                    &insert,
                    &[
                        &event.event_id.as_str(),
                        &aggregate_id_key,
                        &A::aggregate_type(),
                        &revision_i64,
                        &event.event_type,
                        &event_version_i32,
                        &event.payload_json,
                        &event.metadata_json,
                        &event.recorded_at_ms,
                    ],
                )
                .map_err(|error| {
                    IdempotentAppendError::Store(map_postgres_insert_error(
                        error,
                        expected_revision,
                        actual_revision,
                    ))
                })?;
            let sequence: i64 = row
                .try_get(0)
                .map_err(|error| IdempotentAppendError::Store(map_postgres_error(error)))?;
            let sequence = u64::try_from(sequence).map_err(|_| {
                IdempotentAppendError::Store(EventStoreError::Deserialization(
                    "PostgreSQL sequence cannot be negative".to_owned(),
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

        let value_json = serde_json::to_value(&committed).map_err(|error| {
            IdempotentAppendError::Store(EventStoreError::Serialization(format!(
                "idempotent committed events JSON: {error}"
            )))
        })?;
        let complete = format!(
            "UPDATE {} SET state = 'complete', value = $2::jsonb, updated_at_ms = $3
             WHERE idempotency_key = $1;",
            self.idempotency_table
        );
        transaction
            .execute(
                &complete,
                &[&idempotency_key.as_str(), &value_json, &updated_at_ms],
            )
            .map_err(|error| IdempotentAppendError::Store(map_postgres_error(error)))?;
        transaction
            .commit()
            .map_err(|error| IdempotentAppendError::Store(map_postgres_error(error)))?;
        Ok(committed)
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl<A> crate::async_api::AsyncEventStore<A> for PostgresEventStore<A>
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
impl<A> crate::async_api::AsyncAtomicIdempotentEventStore<A> for PostgresEventStore<A>
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

struct PreparedPostgresEvent<E> {
    event_id: EventId,
    event_type: String,
    event_version: u32,
    payload: E,
    payload_json: serde_json::Value,
    metadata: crate::Metadata,
    metadata_json: serde_json::Value,
    recorded_at: SystemTime,
    recorded_at_ms: i64,
}

impl<E> PreparedPostgresEvent<E>
where
    E: serde::Serialize,
{
    fn new(event: NewEvent<E>) -> Result<Self, EventStoreError> {
        let event_id = EventId::new();
        let recorded_at = SystemTime::now();
        let recorded_at_ms = system_time_to_millis(recorded_at)?;
        let payload_json = serialize_payload(&event.payload)?;
        let metadata_json = serialize_metadata(&event.metadata)?;

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
    row: ::postgres::Row,
) -> Result<EventEnvelope<A::Event, A::Id>, EventStoreError>
where
    A: Aggregate,
    A::Event: serde::de::DeserializeOwned,
    A::Id: serde::de::DeserializeOwned,
{
    let event_id: String = row.try_get(0).map_err(map_postgres_error)?;
    let aggregate_id: String = row.try_get(1).map_err(map_postgres_error)?;
    let aggregate_type: String = row.try_get(2).map_err(map_postgres_error)?;
    let revision: i64 = row.try_get(3).map_err(map_postgres_error)?;
    let sequence: i64 = row.try_get(4).map_err(map_postgres_error)?;
    let event_type: String = row.try_get(5).map_err(map_postgres_error)?;
    let event_version: i32 = row.try_get(6).map_err(map_postgres_error)?;
    let payload_val: serde_json::Value = row.try_get(7).map_err(map_postgres_error)?;
    let metadata: serde_json::Value = row.try_get(8).map_err(map_postgres_error)?;
    let recorded_at_ms: i64 = row.try_get(9).map_err(map_postgres_error)?;

    let revision = u64::try_from(revision).map_err(|_| {
        EventStoreError::Deserialization("stored revision cannot be negative".to_owned())
    })?;
    let sequence = u64::try_from(sequence).map_err(|_| {
        EventStoreError::Deserialization("PostgreSQL sequence cannot be negative".to_owned())
    })?;
    let event_version = u32::try_from(event_version).map_err(|_| {
        EventStoreError::Deserialization("event_version cannot be negative".to_owned())
    })?;
    let aggregate_id = deserialize_id(&aggregate_id)?;

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
    let metadata = deserialize_metadata(&event_id, metadata)?;
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

fn map_postgres_insert_error(
    error: ::postgres::Error,
    expected: ExpectedRevision,
    actual: u64,
) -> EventStoreError {
    if error
        .code()
        .is_some_and(|code| *code == ::postgres::error::SqlState::UNIQUE_VIOLATION)
    {
        return EventStoreError::Concurrency(crate::ConcurrencyError::WrongExpectedRevision {
            expected,
            actual,
        });
    }

    map_postgres_error(error)
}

fn map_postgres_error(error: ::postgres::Error) -> EventStoreError {
    EventStoreError::backend_with_source(error.to_string(), error)
}

/// Postgres checkpoint store implementation.
#[derive(Clone)]
pub struct PostgresCheckpointStore {
    client: Arc<Mutex<::postgres::Client>>,
    table_name: String,
}

impl std::fmt::Debug for PostgresCheckpointStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresCheckpointStore")
            .field("table_name", &self.table_name)
            .finish_non_exhaustive()
    }
}

impl PostgresCheckpointStore {
    /// Creates a Postgres checkpoint store using the default table name.
    pub fn new(client: ::postgres::Client) -> Result<Self, EventStoreError> {
        Self::with_table_name(client, "projection_checkpoints")
    }

    /// Creates a Postgres checkpoint store with a custom table name.
    pub fn with_table_name(
        client: ::postgres::Client,
        table_name: impl Into<String>,
    ) -> Result<Self, EventStoreError> {
        let table_name = table_name.into();
        validate_table_name(&table_name)?;

        let store = Self {
            client: Arc::new(Mutex::new(client)),
            table_name,
        };
        store.initialize_schema()?;
        Ok(store)
    }

    /// Initializes the checkpoint schema table.
    pub fn initialize_schema(&self) -> Result<(), EventStoreError> {
        let config = crate::schema::SqlSchemaConfig::new(crate::schema::SqlDialect::Postgres)
            .with_checkpoints_table(&self.table_name)?;
        let migrator = crate::schema::SchemaMigrator::new(config);
        let mut client = self.client.lock().map_err(|_| EventStoreError::Poisoned)?;
        migrator.run_postgres(&mut client)
    }
}

impl crate::projection::CheckpointStore for PostgresCheckpointStore {
    type Error = EventStoreError;

    fn load_checkpoint(&self, projection_name: &str) -> Result<Option<u64>, Self::Error> {
        let mut client = self.client.lock().map_err(|_| EventStoreError::Poisoned)?;
        let sql = format!(
            "SELECT sequence FROM {} WHERE projection_name = $1;",
            self.table_name
        );
        let rows = client
            .query(&sql, &[&projection_name])
            .map_err(map_postgres_error)?;

        if let Some(row) = rows.first() {
            let sequence: i64 = row.get(0);
            let sequence = u64::try_from(sequence).map_err(|_| {
                EventStoreError::Deserialization(
                    "Postgres checkpoint cannot be negative".to_owned(),
                )
            })?;
            Ok(Some(sequence))
        } else {
            Ok(None)
        }
    }

    fn save_checkpoint(&self, projection_name: &str, sequence: u64) -> Result<(), Self::Error> {
        let mut client = self.client.lock().map_err(|_| EventStoreError::Poisoned)?;
        let sql = format!(
            "INSERT INTO {} (projection_name, sequence) VALUES ($1, $2)
             ON CONFLICT (projection_name) DO UPDATE SET sequence = GREATEST({table}.sequence, EXCLUDED.sequence);",
            self.table_name,
            table = self.table_name
        );
        let sequence_i64 = i64::try_from(sequence)
            .map_err(|_| EventStoreError::Deserialization("checkpoint exceeds i64".to_owned()))?;
        client
            .execute(&sql, &[&projection_name, &sequence_i64])
            .map_err(map_postgres_error)?;
        Ok(())
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl crate::projection::AsyncCheckpointStore for PostgresCheckpointStore {
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

/// PostgreSQL-backed idempotency store.
///
/// The store persists pending reservations and completed JSON-serializable
/// values so command retries can be deduplicated across process restarts.
pub struct PostgresIdempotencyStore<V>
where
    V: Clone,
{
    client: Arc<Mutex<Client>>,
    table_name: String,
    _marker: PhantomData<fn() -> V>,
}

impl<V> Clone for PostgresIdempotencyStore<V>
where
    V: Clone,
{
    fn clone(&self) -> Self {
        Self {
            client: Arc::clone(&self.client),
            table_name: self.table_name.clone(),
            _marker: PhantomData,
        }
    }
}

impl<V> std::fmt::Debug for PostgresIdempotencyStore<V>
where
    V: Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresIdempotencyStore")
            .field("table_name", &self.table_name)
            .finish_non_exhaustive()
    }
}

impl<V> PostgresIdempotencyStore<V>
where
    V: Clone,
{
    /// Creates a PostgreSQL idempotency store using the default table name.
    pub fn new(client: Client) -> Result<Self, EventStoreError> {
        Self::with_table_name(client, "idempotency_keys")
    }

    /// Creates a PostgreSQL idempotency store with a custom table name.
    pub fn with_table_name(
        client: Client,
        table_name: impl Into<String>,
    ) -> Result<Self, EventStoreError> {
        let table_name = table_name.into();
        validate_table_name(&table_name)?;

        let store = Self {
            client: Arc::new(Mutex::new(client)),
            table_name,
            _marker: PhantomData,
        };
        store.initialize_schema()?;
        Ok(store)
    }

    /// Initializes the idempotency schema table.
    pub fn initialize_schema(&self) -> Result<(), EventStoreError> {
        let config = crate::schema::SqlSchemaConfig::new(crate::schema::SqlDialect::Postgres)
            .with_idempotency_table(&self.table_name)?;
        let migrator = crate::schema::SchemaMigrator::new(config);
        let mut client = self.client.lock().map_err(|_| EventStoreError::Poisoned)?;
        migrator.run_postgres(&mut client)
    }
}

impl<V> IdempotencyStore<V> for PostgresIdempotencyStore<V>
where
    V: Clone + serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    type Error = EventStoreError;

    fn load(&self, key: &IdempotencyKey) -> Result<Option<IdempotencyState<V>>, Self::Error> {
        let mut client = self.client.lock().map_err(|_| EventStoreError::Poisoned)?;
        let sql = format!(
            "SELECT state, value FROM {} WHERE idempotency_key = $1;",
            self.table_name
        );
        let row = client
            .query_opt(&sql, &[&key.as_str()])
            .map_err(map_postgres_error)?;

        let Some(row) = row else {
            return Ok(None);
        };

        let state: String = row.try_get(0).map_err(map_postgres_error)?;
        let value: Option<serde_json::Value> = row.try_get(1).map_err(map_postgres_error)?;

        match (state.as_str(), value) {
            ("pending", _) => Ok(Some(IdempotencyState::Pending)),
            ("complete", Some(value)) => {
                let value = serde_json::from_value(value).map_err(|error| {
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
        let mut client = self.client.lock().map_err(|_| EventStoreError::Poisoned)?;
        let updated_at_ms = system_time_to_millis(SystemTime::now())?;
        let sql = format!(
            "INSERT INTO {} (idempotency_key, state, value, updated_at_ms)
             VALUES ($1, 'pending', NULL, $2)
             ON CONFLICT (idempotency_key) DO NOTHING;",
            self.table_name
        );
        let changed = client
            .execute(&sql, &[&key.as_str(), &updated_at_ms])
            .map_err(map_postgres_error)?;
        Ok(changed == 1)
    }

    fn save(&self, key: IdempotencyKey, value: V) -> Result<(), Self::Error> {
        let mut client = self.client.lock().map_err(|_| EventStoreError::Poisoned)?;
        let updated_at_ms = system_time_to_millis(SystemTime::now())?;
        let value_json = serde_json::to_value(value).map_err(|error| {
            EventStoreError::Serialization(format!("idempotency value JSON: {error}"))
        })?;
        let sql = format!(
            "INSERT INTO {} (idempotency_key, state, value, updated_at_ms)
             VALUES ($1, 'complete', $2::jsonb, $3)
             ON CONFLICT (idempotency_key) DO UPDATE SET
                state = EXCLUDED.state,
                value = EXCLUDED.value,
                updated_at_ms = EXCLUDED.updated_at_ms;",
            self.table_name
        );
        client
            .execute(&sql, &[&key.as_str(), &value_json, &updated_at_ms])
            .map_err(map_postgres_error)?;
        Ok(())
    }

    fn remove(&self, key: &IdempotencyKey) -> Result<(), Self::Error> {
        let mut client = self.client.lock().map_err(|_| EventStoreError::Poisoned)?;
        let sql = format!(
            "DELETE FROM {} WHERE idempotency_key = $1;",
            self.table_name
        );
        client
            .execute(&sql, &[&key.as_str()])
            .map_err(map_postgres_error)?;
        Ok(())
    }
}

/// PostgreSQL-backed durable snapshot store.
pub struct PostgresSnapshotStore<A>
where
    A: Aggregate,
{
    client: Arc<Mutex<Client>>,
    table_name: String,
    _marker: PhantomData<fn() -> A>,
}

impl<A> Clone for PostgresSnapshotStore<A>
where
    A: Aggregate,
{
    fn clone(&self) -> Self {
        Self {
            client: Arc::clone(&self.client),
            table_name: self.table_name.clone(),
            _marker: PhantomData,
        }
    }
}

impl<A> std::fmt::Debug for PostgresSnapshotStore<A>
where
    A: Aggregate,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresSnapshotStore")
            .field("table_name", &self.table_name)
            .finish_non_exhaustive()
    }
}

impl<A> PostgresSnapshotStore<A>
where
    A: Aggregate,
{
    /// Creates a PostgreSQL snapshot store using the default table name.
    pub fn new(client: Client) -> Result<Self, EventStoreError> {
        Self::with_table_name(client, "snapshots")
    }

    /// Creates a PostgreSQL snapshot store with a custom table name.
    pub fn with_table_name(
        client: Client,
        table_name: impl Into<String>,
    ) -> Result<Self, EventStoreError> {
        let table_name = table_name.into();
        validate_table_name(&table_name)?;

        let store = Self {
            client: Arc::new(Mutex::new(client)),
            table_name,
            _marker: PhantomData,
        };
        store.initialize_schema()?;
        Ok(store)
    }

    /// Initializes the snapshot schema table.
    pub fn initialize_schema(&self) -> Result<(), EventStoreError> {
        let config = crate::schema::SqlSchemaConfig::new(crate::schema::SqlDialect::Postgres)
            .with_snapshots_table(&self.table_name)?;
        let migrator = crate::schema::SchemaMigrator::new(config);
        let mut client = self.client.lock().map_err(|_| EventStoreError::Poisoned)?;
        migrator.run_postgres(&mut client)
    }
}

impl<A> SnapshotStore<A> for PostgresSnapshotStore<A>
where
    A: Aggregate + serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
    A::Id: serde::Serialize + serde::de::DeserializeOwned,
{
    type Error = EventStoreError;

    fn load_snapshot(&self, aggregate_id: &A::Id) -> Result<Option<Snapshot<A>>, Self::Error> {
        #[cfg(feature = "tracing")]
        let _span = tracing::debug_span!(
            "snapshot.load",
            dialect = "postgres",
            aggregate_type = A::aggregate_type()
        )
        .entered();

        let aggregate_id = serialize_id(aggregate_id)?;
        let mut client = self.client.lock().map_err(|_| EventStoreError::Poisoned)?;
        let sql = format!(
            "SELECT revision, state, metadata, recorded_at_ms FROM {} \
             WHERE aggregate_type = $1 AND aggregate_id = $2;",
            self.table_name
        );
        let row = client
            .query_opt(&sql, &[&A::aggregate_type(), &aggregate_id])
            .map_err(map_postgres_error)?;
        let Some(row) = row else {
            return Ok(None);
        };

        let revision: i64 = row.try_get(0).map_err(map_postgres_error)?;
        let state: serde_json::Value = row.try_get(1).map_err(map_postgres_error)?;
        let metadata: serde_json::Value = row.try_get(2).map_err(map_postgres_error)?;
        let recorded_at_ms: i64 = row.try_get(3).map_err(map_postgres_error)?;
        let revision = u64::try_from(revision).map_err(|_| {
            EventStoreError::Deserialization(
                "Postgres snapshot revision cannot be negative".to_owned(),
            )
        })?;
        let state = serde_json::from_value(state).map_err(|error| {
            EventStoreError::Deserialization(format!("snapshot state JSON: {error}"))
        })?;
        let metadata = serde_json::from_value(metadata).map_err(|error| {
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
            dialect = "postgres",
            aggregate_type = A::aggregate_type(),
            revision = snapshot.revision
        )
        .entered();

        let aggregate_id = serialize_id(&snapshot.aggregate_id)?;
        let revision_i64 = i64::try_from(snapshot.revision).map_err(|_| {
            EventStoreError::Serialization("snapshot revision exceeds i64".to_owned())
        })?;
        let state_json = serde_json::to_value(&snapshot.state).map_err(|error| {
            EventStoreError::Serialization(format!("snapshot state JSON: {error}"))
        })?;
        let metadata_json = serde_json::to_value(&snapshot.metadata).map_err(|error| {
            EventStoreError::Serialization(format!("snapshot metadata JSON: {error}"))
        })?;
        let recorded_at_ms = system_time_to_millis(snapshot.recorded_at)?;
        let mut client = self.client.lock().map_err(|_| EventStoreError::Poisoned)?;
        let sql = format!(
            "INSERT INTO {} (aggregate_type, aggregate_id, revision, state, metadata, recorded_at_ms)
             VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6)
             ON CONFLICT (aggregate_type, aggregate_id) DO UPDATE SET
                revision = EXCLUDED.revision,
                state = EXCLUDED.state,
                metadata = EXCLUDED.metadata,
                recorded_at_ms = EXCLUDED.recorded_at_ms
             WHERE EXCLUDED.revision >= {}.revision;",
            self.table_name, self.table_name
        );
        client
            .execute(
                &sql,
                &[
                    &A::aggregate_type(),
                    &aggregate_id,
                    &revision_i64,
                    &state_json,
                    &metadata_json,
                    &recorded_at_ms,
                ],
            )
            .map_err(map_postgres_error)?;
        Ok(())
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl<A> crate::async_api::AsyncSnapshotStore<A> for PostgresSnapshotStore<A>
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
impl<V> crate::async_api::AsyncIdempotencyStore<V> for PostgresIdempotencyStore<V>
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
