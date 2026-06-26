use std::marker::PhantomData;
use ddd_cqrs_es::{Aggregate, EventEnvelope, EventId, ExpectedRevision, NewEvent, EventStore};
use ddd_cqrs_es::error::EventStoreError;

#[cfg(feature = "postgres")]
pub use ddd_cqrs_es::{PostgresEventStore, PostgresCheckpointStore};

static SCHEMA_INITIALIZED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);


// =========================================================================
// 1. RUNTIME: SPIN SQLITE (BACKWARDS COMPATIBILITY)
// =========================================================================
#[cfg(runtime_spin)]
use spin_sdk::sqlite::{Connection as SpinConnection, Value as SpinValue};
#[cfg(runtime_spin)]
use futures::executor::block_on as spin_block_on;

#[cfg(runtime_spin)]
pub struct SpinSqliteEventStore<A> {
    db_name: String,
    _phantom: PhantomData<fn() -> A>,
}

#[cfg(runtime_spin)]
impl<A> Clone for SpinSqliteEventStore<A> {
    fn clone(&self) -> Self {
        Self {
            db_name: self.db_name.clone(),
            _phantom: PhantomData,
        }
    }
}

#[cfg(runtime_spin)]
impl<A> SpinSqliteEventStore<A>
where
    A: Aggregate,
{
    pub fn new(db_name: impl Into<String>) -> Self {
        Self {
            db_name: db_name.into(),
            _phantom: PhantomData,
        }
    }

    pub fn initialize_schema(&self) -> Result<(), String> {
        let connection = spin_block_on(SpinConnection::open(&self.db_name)).map_err(|e| e.to_string())?;
        
        let create_events = r#"
            CREATE TABLE IF NOT EXISTS events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                aggregate_id TEXT NOT NULL,
                aggregate_type TEXT NOT NULL,
                revision INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                event_version INTEGER NOT NULL,
                payload TEXT NOT NULL,
                metadata TEXT NOT NULL,
                recorded_at_ms INTEGER NOT NULL,
                UNIQUE (aggregate_type, aggregate_id, revision)
            );
        "#;
        spin_block_on(connection.execute(create_events, [])).map_err(|e| e.to_string())?;

        let create_checkpoints = r#"
            CREATE TABLE IF NOT EXISTS checkpoints (
                projection_name TEXT PRIMARY KEY,
                last_sequence INTEGER NOT NULL
            );
        "#;
        spin_block_on(connection.execute(create_checkpoints, [])).map_err(|e| e.to_string())?;

        let create_read_model = r#"
            CREATE TABLE IF NOT EXISTS counter_read_model (
                id TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            );
        "#;
        spin_block_on(connection.execute(create_read_model, [])).map_err(|e| e.to_string())?;

        Ok(())
    }
}

#[cfg(runtime_spin)]
impl<A> SpinSqliteEventStore<A>
where
    A: Aggregate + 'static,
    A::Event: serde::Serialize + serde::de::DeserializeOwned,
    A::Id: serde::Serialize + serde::de::DeserializeOwned,
{
    pub async fn initialize_schema_async(&self) -> Result<(), String> {
        let connection = SpinConnection::open(&self.db_name).await.map_err(|e| e.to_string())?;
        
        let create_events = r#"
            CREATE TABLE IF NOT EXISTS events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                aggregate_id TEXT NOT NULL,
                aggregate_type TEXT NOT NULL,
                revision INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                event_version INTEGER NOT NULL,
                payload TEXT NOT NULL,
                metadata TEXT NOT NULL,
                recorded_at_ms INTEGER NOT NULL,
                UNIQUE (aggregate_type, aggregate_id, revision)
            );
        "#;
        connection.execute(create_events, []).await.map_err(|e| e.to_string())?;

        let create_checkpoints = r#"
            CREATE TABLE IF NOT EXISTS checkpoints (
                projection_name TEXT PRIMARY KEY,
                last_sequence INTEGER NOT NULL
            );
        "#;
        connection.execute(create_checkpoints, []).await.map_err(|e| e.to_string())?;

        let create_read_model = r#"
            CREATE TABLE IF NOT EXISTS counter_read_model (
                id TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            );
        "#;
        connection.execute(create_read_model, []).await.map_err(|e| e.to_string())?;

        Ok(())
    }

    pub async fn load_async(&self, aggregate_id: &A::Id) -> Result<Vec<EventEnvelope<A::Event, A::Id>>, EventStoreError> {
        let aggregate_id_str = serde_json::to_string(aggregate_id)
            .map_err(|e| EventStoreError::Serialization(e.to_string()))?;
        
        let connection = SpinConnection::open(&self.db_name).await
            .map_err(|e| EventStoreError::Connection(e.to_string()))?;
        
        let query = "SELECT sequence, event_id, aggregate_id, aggregate_type, revision, event_type, event_version, payload, metadata, recorded_at_ms FROM events WHERE aggregate_type = ? AND aggregate_id = ? ORDER BY revision ASC";
        let params = vec![
            SpinValue::Text(A::aggregate_type().to_string()),
            SpinValue::Text(aggregate_id_str),
        ];

        let query_result = connection.execute(query, params).await
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;

        let rows = query_result.collect().await
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;

        let mut envelopes = Vec::new();
        for row in rows {
            let sequence = row.get::<i64>(0)
                .ok_or_else(|| EventStoreError::Deserialization("Missing sequence".to_string()))? as u64;
            let event_id_str = row.get::<&str>(1)
                .ok_or_else(|| EventStoreError::Deserialization("Missing event_id".to_string()))?.to_string();
            let aggregate_id_raw = row.get::<&str>(2)
                .ok_or_else(|| EventStoreError::Deserialization("Missing aggregate_id".to_string()))?.to_string();
            let aggregate_type = row.get::<&str>(3)
                .ok_or_else(|| EventStoreError::Deserialization("Missing aggregate_type".to_string()))?.to_string();
            let revision = row.get::<i64>(4)
                .ok_or_else(|| EventStoreError::Deserialization("Missing revision".to_string()))? as u64;
            let event_type = row.get::<&str>(5)
                .ok_or_else(|| EventStoreError::Deserialization("Missing event_type".to_string()))?.to_string();
            let event_version = row.get::<i64>(6)
                .ok_or_else(|| EventStoreError::Deserialization("Missing event_version".to_string()))? as u32;
            let payload_str = row.get::<&str>(7)
                .ok_or_else(|| EventStoreError::Deserialization("Missing payload".to_string()))?.to_string();
            let metadata_str = row.get::<&str>(8)
                .ok_or_else(|| EventStoreError::Deserialization("Missing metadata".to_string()))?.to_string();
            let recorded_at_ms = row.get::<i64>(9)
                .ok_or_else(|| EventStoreError::Deserialization("Missing recorded_at_ms".to_string()))?;

            let aggregate_id_val: A::Id = serde_json::from_str(&aggregate_id_raw)
                .map_err(|e| EventStoreError::Deserialization(e.to_string()))?;

            let payload: A::Event = serde_json::from_str(&payload_str)
                .map_err(|e| EventStoreError::Deserialization(e.to_string()))?;

            let metadata: ddd_cqrs_es::Metadata = serde_json::from_str(&metadata_str)
                .map_err(|e| EventStoreError::Deserialization(e.to_string()))?;

            let recorded_at = std::time::UNIX_EPOCH + std::time::Duration::from_millis(recorded_at_ms as u64);

            envelopes.push(EventEnvelope::new(
                EventId::from_string(event_id_str),
                aggregate_id_val,
                aggregate_type,
                revision,
                Some(sequence),
                event_type,
                event_version,
                payload,
                metadata,
                recorded_at,
            ));
        }

        Ok(envelopes)
    }

    pub async fn append_async(
        &self,
        aggregate_id: &A::Id,
        expected_revision: ExpectedRevision,
        events: Vec<NewEvent<A::Event>>,
    ) -> Result<Vec<EventEnvelope<A::Event, A::Id>>, EventStoreError> {
        let aggregate_id_str = serde_json::to_string(aggregate_id)
            .map_err(|e| EventStoreError::Serialization(e.to_string()))?;

        let connection = SpinConnection::open(&self.db_name).await
            .map_err(|e| EventStoreError::Connection(e.to_string()))?;

        let current_revision = {
            let query = "SELECT COALESCE(MAX(revision), 0) as max_rev FROM events WHERE aggregate_type = ? AND aggregate_id = ?";
            let params = vec![
                SpinValue::Text(A::aggregate_type().to_string()),
                SpinValue::Text(aggregate_id_str.clone()),
            ];
            let query_result = connection.execute(query, params).await
                .map_err(|e| EventStoreError::Backend(e.to_string()))?;
            let rows = query_result.collect().await
                .map_err(|e| EventStoreError::Backend(e.to_string()))?;
            let mut actual = 0u64;
            if let Some(row) = rows.first() {
                if let Some(rev) = row.get::<i64>(0) {
                    actual = rev as u64;
                }
            }
            actual
        };

        match expected_revision {
            ExpectedRevision::Any => {}
            ExpectedRevision::NoStream if current_revision == 0 => {}
            ExpectedRevision::NoStream => {
                return Err(EventStoreError::Concurrency(
                    ddd_cqrs_es::ConcurrencyError::StreamAlreadyExists,
                ));
            }
            ExpectedRevision::Exact(expected) if expected == current_revision => {}
            ExpectedRevision::Exact(_) => {
                return Err(EventStoreError::Concurrency(
                    ddd_cqrs_es::ConcurrencyError::WrongExpectedRevision {
                        expected: expected_revision,
                        actual: current_revision,
                    },
                ));
            }
        }

        if events.is_empty() {
            return Ok(Vec::new());
        }

        let mut envelopes = Vec::new();
        let insert_query = r#"
            INSERT INTO events (
                event_id, aggregate_id, aggregate_type, revision, event_type, event_version, payload, metadata, recorded_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#;

        let now = std::time::SystemTime::now();
        let recorded_at_ms = now.duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        for (i, event) in events.into_iter().enumerate() {
            let revision = current_revision + i as u64 + 1;
            let event_id = EventId::new();

            let payload_str = serde_json::to_string(&event.payload)
                .map_err(|e| EventStoreError::Serialization(e.to_string()))?;
            let metadata_str = serde_json::to_string(&event.metadata)
                .map_err(|e| EventStoreError::Serialization(e.to_string()))?;

            let params = vec![
                SpinValue::Text(event_id.to_string()),
                SpinValue::Text(aggregate_id_str.clone()),
                SpinValue::Text(A::aggregate_type().to_string()),
                SpinValue::Integer(revision as i64),
                SpinValue::Text(event.event_type.clone()),
                SpinValue::Integer(event.event_version as i64),
                SpinValue::Text(payload_str),
                SpinValue::Text(metadata_str),
                SpinValue::Integer(recorded_at_ms),
            ];

            connection.execute(insert_query, params).await
                .map_err(|e| {
                    let err_str = e.to_string();
                    if err_str.contains("constraint") || err_str.contains("UNIQUE") {
                        EventStoreError::Concurrency(ddd_cqrs_es::ConcurrencyError::WrongExpectedRevision {
                            expected: expected_revision,
                            actual: current_revision,
                        })
                    } else {
                        EventStoreError::Backend(err_str)
                    }
                })?;

            let sequence = connection.last_insert_rowid().await as u64;

            envelopes.push(EventEnvelope::new(
                event_id,
                aggregate_id.clone(),
                A::aggregate_type(),
                revision,
                Some(sequence),
                event.event_type,
                event.event_version,
                event.payload,
                event.metadata,
                now,
            ));
        }

        Ok(envelopes)
    }

    pub async fn load_global_after_async(&self, sequence: Option<u64>) -> Result<Vec<EventEnvelope<A::Event, A::Id>>, EventStoreError> {
        let seq = sequence.unwrap_or(0) as i64;
        
        let connection = SpinConnection::open(&self.db_name).await
            .map_err(|e| EventStoreError::Connection(e.to_string()))?;
        
        let query = "SELECT sequence, event_id, aggregate_id, aggregate_type, revision, event_type, event_version, payload, metadata, recorded_at_ms FROM events WHERE aggregate_type = ? AND sequence > ? ORDER BY sequence ASC";
        let params = vec![
            SpinValue::Text(A::aggregate_type().to_string()),
            SpinValue::Integer(seq),
        ];

        let query_result = connection.execute(query, params).await
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;

        let rows = query_result.collect().await
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;

        let mut envelopes = Vec::new();
        for row in rows {
            let sequence = row.get::<i64>(0)
                .ok_or_else(|| EventStoreError::Deserialization("Missing sequence".to_string()))? as u64;
            let event_id_str = row.get::<&str>(1)
                .ok_or_else(|| EventStoreError::Deserialization("Missing event_id".to_string()))?.to_string();
            let aggregate_id_raw = row.get::<&str>(2)
                .ok_or_else(|| EventStoreError::Deserialization("Missing aggregate_id".to_string()))?.to_string();
            let aggregate_type = row.get::<&str>(3)
                .ok_or_else(|| EventStoreError::Deserialization("Missing aggregate_type".to_string()))?.to_string();
            let revision = row.get::<i64>(4)
                .ok_or_else(|| EventStoreError::Deserialization("Missing revision".to_string()))? as u64;
            let event_type = row.get::<&str>(5)
                .ok_or_else(|| EventStoreError::Deserialization("Missing event_type".to_string()))?.to_string();
            let event_version = row.get::<i64>(6)
                .ok_or_else(|| EventStoreError::Deserialization("Missing event_version".to_string()))? as u32;
            let payload_str = row.get::<&str>(7)
                .ok_or_else(|| EventStoreError::Deserialization("Missing payload".to_string()))?.to_string();
            let metadata_str = row.get::<&str>(8)
                .ok_or_else(|| EventStoreError::Deserialization("Missing metadata".to_string()))?.to_string();
            let recorded_at_ms = row.get::<i64>(9)
                .ok_or_else(|| EventStoreError::Deserialization("Missing recorded_at_ms".to_string()))?;

            let aggregate_id_val: A::Id = serde_json::from_str(&aggregate_id_raw)
                .map_err(|e| EventStoreError::Deserialization(e.to_string()))?;

            let payload: A::Event = serde_json::from_str(&payload_str)
                .map_err(|e| EventStoreError::Deserialization(e.to_string()))?;

            let metadata: ddd_cqrs_es::Metadata = serde_json::from_str(&metadata_str)
                .map_err(|e| EventStoreError::Deserialization(e.to_string()))?;

            let recorded_at = std::time::UNIX_EPOCH + std::time::Duration::from_millis(recorded_at_ms as u64);

            envelopes.push(EventEnvelope::new(
                EventId::from_string(event_id_str),
                aggregate_id_val,
                aggregate_type,
                revision,
                Some(sequence),
                event_type,
                event_version,
                payload,
                metadata,
                recorded_at,
            ));
        }

        Ok(envelopes)
    }
}

#[cfg(runtime_spin)]
impl<A> EventStore<A> for SpinSqliteEventStore<A>
where
    A: Aggregate + 'static,
    A::Event: serde::Serialize + serde::de::DeserializeOwned,
    A::Id: serde::Serialize + serde::de::DeserializeOwned,
{
    type Error = EventStoreError;

    fn load(&self, aggregate_id: &A::Id) -> Result<Vec<EventEnvelope<A::Event, A::Id>>, Self::Error> {
        let aggregate_id_str = serde_json::to_string(aggregate_id)
            .map_err(|e| EventStoreError::Serialization(e.to_string()))?;
        
        let connection = spin_block_on(SpinConnection::open(&self.db_name))
            .map_err(|e| EventStoreError::Connection(e.to_string()))?;
        
        let query = "SELECT sequence, event_id, aggregate_id, aggregate_type, revision, event_type, event_version, payload, metadata, recorded_at_ms FROM events WHERE aggregate_type = ? AND aggregate_id = ? ORDER BY revision ASC";
        let params = vec![
            SpinValue::Text(A::aggregate_type().to_string()),
            SpinValue::Text(aggregate_id_str),
        ];

        let query_result = spin_block_on(connection.execute(query, params))
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;

        let rows = spin_block_on(query_result.collect())
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;

        let mut envelopes = Vec::new();
        for row in rows {
            let sequence = row.get::<i64>(0)
                .ok_or_else(|| EventStoreError::Deserialization("Missing sequence".to_string()))? as u64;
            let event_id_str = row.get::<&str>(1)
                .ok_or_else(|| EventStoreError::Deserialization("Missing event_id".to_string()))?.to_string();
            let aggregate_id_raw = row.get::<&str>(2)
                .ok_or_else(|| EventStoreError::Deserialization("Missing aggregate_id".to_string()))?.to_string();
            let aggregate_type = row.get::<&str>(3)
                .ok_or_else(|| EventStoreError::Deserialization("Missing aggregate_type".to_string()))?.to_string();
            let revision = row.get::<i64>(4)
                .ok_or_else(|| EventStoreError::Deserialization("Missing revision".to_string()))? as u64;
            let event_type = row.get::<&str>(5)
                .ok_or_else(|| EventStoreError::Deserialization("Missing event_type".to_string()))?.to_string();
            let event_version = row.get::<i64>(6)
                .ok_or_else(|| EventStoreError::Deserialization("Missing event_version".to_string()))? as u32;
            let payload_str = row.get::<&str>(7)
                .ok_or_else(|| EventStoreError::Deserialization("Missing payload".to_string()))?.to_string();
            let metadata_str = row.get::<&str>(8)
                .ok_or_else(|| EventStoreError::Deserialization("Missing metadata".to_string()))?.to_string();
            let recorded_at_ms = row.get::<i64>(9)
                .ok_or_else(|| EventStoreError::Deserialization("Missing recorded_at_ms".to_string()))?;

            let aggregate_id_val: A::Id = serde_json::from_str(&aggregate_id_raw)
                .map_err(|e| EventStoreError::Deserialization(e.to_string()))?;

            let payload: A::Event = serde_json::from_str(&payload_str)
                .map_err(|e| EventStoreError::Deserialization(e.to_string()))?;

            let metadata: ddd_cqrs_es::Metadata = serde_json::from_str(&metadata_str)
                .map_err(|e| EventStoreError::Deserialization(e.to_string()))?;

            let recorded_at = std::time::UNIX_EPOCH + std::time::Duration::from_millis(recorded_at_ms as u64);

            envelopes.push(EventEnvelope::new(
                EventId::from_string(event_id_str),
                aggregate_id_val,
                aggregate_type,
                revision,
                Some(sequence),
                event_type,
                event_version,
                payload,
                metadata,
                recorded_at,
            ));
        }

        Ok(envelopes)
    }

    fn append(
        &self,
        aggregate_id: &A::Id,
        expected_revision: ExpectedRevision,
        events: Vec<NewEvent<A::Event>>,
    ) -> Result<Vec<EventEnvelope<A::Event, A::Id>>, Self::Error> {
        let aggregate_id_str = serde_json::to_string(aggregate_id)
            .map_err(|e| EventStoreError::Serialization(e.to_string()))?;

        let connection = spin_block_on(SpinConnection::open(&self.db_name))
            .map_err(|e| EventStoreError::Connection(e.to_string()))?;

        let current_revision = {
            let query = "SELECT COALESCE(MAX(revision), 0) as max_rev FROM events WHERE aggregate_type = ? AND aggregate_id = ?";
            let params = vec![
                SpinValue::Text(A::aggregate_type().to_string()),
                SpinValue::Text(aggregate_id_str.clone()),
            ];
            let query_result = spin_block_on(connection.execute(query, params))
                .map_err(|e| EventStoreError::Backend(e.to_string()))?;
            let rows = spin_block_on(query_result.collect())
                .map_err(|e| EventStoreError::Backend(e.to_string()))?;
            let mut actual = 0u64;
            if let Some(row) = rows.first() {
                if let Some(rev) = row.get::<i64>(0) {
                    actual = rev as u64;
                }
            }
            actual
        };

        match expected_revision {
            ExpectedRevision::Any => {}
            ExpectedRevision::NoStream if current_revision == 0 => {}
            ExpectedRevision::NoStream => {
                return Err(EventStoreError::Concurrency(
                    ddd_cqrs_es::ConcurrencyError::StreamAlreadyExists,
                ));
            }
            ExpectedRevision::Exact(expected) if expected == current_revision => {}
            ExpectedRevision::Exact(_) => {
                return Err(EventStoreError::Concurrency(
                    ddd_cqrs_es::ConcurrencyError::WrongExpectedRevision {
                        expected: expected_revision,
                        actual: current_revision,
                    },
                ));
            }
        }

        if events.is_empty() {
            return Ok(Vec::new());
        }

        let mut envelopes = Vec::new();
        let insert_query = r#"
            INSERT INTO events (
                event_id, aggregate_id, aggregate_type, revision, event_type, event_version, payload, metadata, recorded_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#;

        let now = std::time::SystemTime::now();
        let recorded_at_ms = now.duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        for (i, event) in events.into_iter().enumerate() {
            let revision = current_revision + i as u64 + 1;
            let event_id = EventId::new();

            let payload_str = serde_json::to_string(&event.payload)
                .map_err(|e| EventStoreError::Serialization(e.to_string()))?;
            let metadata_str = serde_json::to_string(&event.metadata)
                .map_err(|e| EventStoreError::Serialization(e.to_string()))?;

            let params = vec![
                SpinValue::Text(event_id.to_string()),
                SpinValue::Text(aggregate_id_str.clone()),
                SpinValue::Text(A::aggregate_type().to_string()),
                SpinValue::Integer(revision as i64),
                SpinValue::Text(event.event_type.clone()),
                SpinValue::Integer(event.event_version as i64),
                SpinValue::Text(payload_str),
                SpinValue::Text(metadata_str),
                SpinValue::Integer(recorded_at_ms),
            ];

            spin_block_on(connection.execute(insert_query, params))
                .map_err(|e| {
                    let err_str = e.to_string();
                    if err_str.contains("constraint") || err_str.contains("UNIQUE") {
                        EventStoreError::Concurrency(ddd_cqrs_es::ConcurrencyError::WrongExpectedRevision {
                            expected: expected_revision,
                            actual: current_revision,
                        })
                    } else {
                        EventStoreError::Backend(err_str)
                    }
                })?;

            let sequence = spin_block_on(connection.last_insert_rowid()) as u64;

            envelopes.push(EventEnvelope::new(
                event_id,
                aggregate_id.clone(),
                A::aggregate_type(),
                revision,
                Some(sequence),
                event.event_type,
                event.event_version,
                event.payload,
                event.metadata,
                now,
            ));
        }

        Ok(envelopes)
    }

    fn load_global_after(&self, sequence: Option<u64>) -> Result<Vec<EventEnvelope<A::Event, A::Id>>, Self::Error> {
        let seq = sequence.unwrap_or(0) as i64;
        
        let connection = spin_block_on(SpinConnection::open(&self.db_name))
            .map_err(|e| EventStoreError::Connection(e.to_string()))?;
        
        let query = "SELECT sequence, event_id, aggregate_id, aggregate_type, revision, event_type, event_version, payload, metadata, recorded_at_ms FROM events WHERE aggregate_type = ? AND sequence > ? ORDER BY sequence ASC";
        let params = vec![
            SpinValue::Text(A::aggregate_type().to_string()),
            SpinValue::Integer(seq),
        ];

        let query_result = spin_block_on(connection.execute(query, params))
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;

        let rows = spin_block_on(query_result.collect())
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;

        let mut envelopes = Vec::new();
        for row in rows {
            let sequence = row.get::<i64>(0)
                .ok_or_else(|| EventStoreError::Deserialization("Missing sequence".to_string()))? as u64;
            let event_id_str = row.get::<&str>(1)
                .ok_or_else(|| EventStoreError::Deserialization("Missing event_id".to_string()))?.to_string();
            let aggregate_id_raw = row.get::<&str>(2)
                .ok_or_else(|| EventStoreError::Deserialization("Missing aggregate_id".to_string()))?.to_string();
            let aggregate_type = row.get::<&str>(3)
                .ok_or_else(|| EventStoreError::Deserialization("Missing aggregate_type".to_string()))?.to_string();
            let revision = row.get::<i64>(4)
                .ok_or_else(|| EventStoreError::Deserialization("Missing revision".to_string()))? as u64;
            let event_type = row.get::<&str>(5)
                .ok_or_else(|| EventStoreError::Deserialization("Missing event_type".to_string()))?.to_string();
            let event_version = row.get::<i64>(6)
                .ok_or_else(|| EventStoreError::Deserialization("Missing event_version".to_string()))? as u32;
            let payload_str = row.get::<&str>(7)
                .ok_or_else(|| EventStoreError::Deserialization("Missing payload".to_string()))?.to_string();
            let metadata_str = row.get::<&str>(8)
                .ok_or_else(|| EventStoreError::Deserialization("Missing metadata".to_string()))?.to_string();
            let recorded_at_ms = row.get::<i64>(9)
                .ok_or_else(|| EventStoreError::Deserialization("Missing recorded_at_ms".to_string()))?;

            let aggregate_id_val: A::Id = serde_json::from_str(&aggregate_id_raw)
                .map_err(|e| EventStoreError::Deserialization(e.to_string()))?;

            let payload: A::Event = serde_json::from_str(&payload_str)
                .map_err(|e| EventStoreError::Deserialization(e.to_string()))?;

            let metadata: ddd_cqrs_es::Metadata = serde_json::from_str(&metadata_str)
                .map_err(|e| EventStoreError::Deserialization(e.to_string()))?;

            let recorded_at = std::time::UNIX_EPOCH + std::time::Duration::from_millis(recorded_at_ms as u64);

            envelopes.push(EventEnvelope::new(
                EventId::from_string(event_id_str),
                aggregate_id_val,
                aggregate_type,
                revision,
                Some(sequence),
                event_type,
                event_version,
                payload,
                metadata,
                recorded_at,
            ));
        }

        Ok(envelopes)
    }
}

#[cfg(runtime_spin)]
pub struct SpinSqliteCheckpointStore {
    db_name: String,
}

#[cfg(runtime_spin)]
impl Clone for SpinSqliteCheckpointStore {
    fn clone(&self) -> Self {
        Self {
            db_name: self.db_name.clone(),
        }
    }
}

#[cfg(runtime_spin)]
impl SpinSqliteCheckpointStore {
    pub fn new(db_name: impl Into<String>) -> Self {
        Self {
            db_name: db_name.into(),
        }
    }

    pub async fn load_checkpoint_async(&self, projection_name: &str) -> Result<Option<u64>, EventStoreError> {
        let connection = SpinConnection::open(&self.db_name).await
            .map_err(|e| EventStoreError::Connection(e.to_string()))?;
        
        let sql = "SELECT last_sequence FROM checkpoints WHERE projection_name = ?;";
        let params = vec![SpinValue::Text(projection_name.to_string())];
        let query_result = connection.execute(sql, params).await
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;

        let rows = query_result.collect().await
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;

        if let Some(row) = rows.first() {
            let last_sequence = row.get::<i64>(0)
                .ok_or_else(|| EventStoreError::Deserialization("Missing last_sequence".to_string()))? as u64;
            Ok(Some(last_sequence))
        } else {
            Ok(None)
        }
    }

    pub async fn save_checkpoint_async(&self, projection_name: &str, sequence: u64) -> Result<(), EventStoreError> {
        let connection = SpinConnection::open(&self.db_name).await
            .map_err(|e| EventStoreError::Connection(e.to_string()))?;
        
        let sql = "INSERT INTO checkpoints (projection_name, last_sequence) VALUES (?, ?) \
                   ON CONFLICT(projection_name) DO UPDATE SET last_sequence = excluded.last_sequence;";
        let params = vec![
            SpinValue::Text(projection_name.to_string()),
            SpinValue::Integer(sequence as i64),
        ];
        let _ = connection.execute(sql, params).await
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;
        Ok(())
    }
}

#[cfg(runtime_spin)]
impl ddd_cqrs_es::CheckpointStore for SpinSqliteCheckpointStore {
    type Error = EventStoreError;

    fn load_checkpoint(&self, projection_name: &str) -> Result<Option<u64>, Self::Error> {
        let connection = spin_block_on(SpinConnection::open(&self.db_name))
            .map_err(|e| EventStoreError::Connection(e.to_string()))?;
        
        let sql = "SELECT last_sequence FROM checkpoints WHERE projection_name = ?;";
        let params = vec![SpinValue::Text(projection_name.to_string())];
        let query_result = spin_block_on(connection.execute(sql, params))
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;

        let rows = spin_block_on(query_result.collect())
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;

        if let Some(row) = rows.first() {
            let last_sequence = row.get::<i64>(0)
                .ok_or_else(|| EventStoreError::Deserialization("Missing last_sequence".to_string()))? as u64;
            Ok(Some(last_sequence))
        } else {
            Ok(None)
        }
    }

    fn save_checkpoint(&self, projection_name: &str, sequence: u64) -> Result<(), Self::Error> {
        let connection = spin_block_on(SpinConnection::open(&self.db_name))
            .map_err(|e| EventStoreError::Connection(e.to_string()))?;
        
        let sql = "INSERT INTO checkpoints (projection_name, last_sequence) VALUES (?, ?) \
                   ON CONFLICT(projection_name) DO UPDATE SET last_sequence = excluded.last_sequence;";
        let params = vec![
            SpinValue::Text(projection_name.to_string()),
            SpinValue::Integer(sequence as i64),
        ];
        let _ = spin_block_on(connection.execute(sql, params))
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;
        Ok(())
    }
}

#[cfg(runtime_spin)]
pub struct CounterProjection {
    db_name: String,
}

#[cfg(runtime_spin)]
impl CounterProjection {
    pub fn new(db_name: impl Into<String>) -> Self {
        Self {
            db_name: db_name.into(),
        }
    }

    pub async fn apply_async(&mut self, envelope: &EventEnvelope<crate::domain::CounterEvent, crate::domain::CounterId>) -> Result<(), EventStoreError> {
        let aggregate_id_str = serde_json::to_string(&envelope.aggregate_id)
            .map_err(|e| EventStoreError::Serialization(e.to_string()))?;
        
        let connection = SpinConnection::open(&self.db_name).await
            .map_err(|e| EventStoreError::Connection(e.to_string()))?;
        
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
            SpinValue::Text(aggregate_id_str),
            SpinValue::Integer(param_val as i64),
        ];

        let _ = connection.execute(sql, params).await
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;
        
        Ok(())
    }
}

#[cfg(runtime_spin)]
impl ddd_cqrs_es::Projection<crate::domain::CounterEvent, crate::domain::CounterId> for CounterProjection {
    type Error = EventStoreError;

    fn name(&self) -> &'static str {
        "counter_projection"
    }

    fn apply(&mut self, envelope: &EventEnvelope<crate::domain::CounterEvent, crate::domain::CounterId>) -> Result<(), Self::Error> {
        let aggregate_id_str = serde_json::to_string(&envelope.aggregate_id)
            .map_err(|e| EventStoreError::Serialization(e.to_string()))?;
        
        let connection = spin_block_on(SpinConnection::open(&self.db_name))
            .map_err(|e| EventStoreError::Connection(e.to_string()))?;
        
        let query = "SELECT value FROM counter_read_model WHERE id = ?";
        let params = vec![SpinValue::Text(aggregate_id_str.clone())];
        let query_result = spin_block_on(connection.execute(query, params))
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;
        
        let rows = spin_block_on(query_result.collect())
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;

        let mut current_value = 0i32;
        if let Some(row) = rows.first() {
            if let Some(val) = row.get::<i64>(0) {
                current_value = val as i32;
            }
        }

        let new_value = match envelope.payload {
            crate::domain::CounterEvent::Incremented { amount } => current_value.saturating_add(amount),
            crate::domain::CounterEvent::Decremented { amount } => current_value.saturating_sub(amount),
            crate::domain::CounterEvent::ResetPerformed { value } => value,
        };

        let upsert_sql = "INSERT INTO counter_read_model (id, value) VALUES (?, ?) \
                          ON CONFLICT(id) DO UPDATE SET value = excluded.value;";
        let upsert_params = vec![
            SpinValue::Text(aggregate_id_str),
            SpinValue::Integer(new_value as i64),
        ];
        let _ = spin_block_on(connection.execute(upsert_sql, upsert_params))
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;

        Ok(())
    }
}


// =========================================================================
// 2. RUNTIME: GENERIC WASMTIME (WASI FILE PERSISTENCE)
// =========================================================================
#[cfg(runtime_wasmtime)]
use std::fs;
#[cfg(runtime_wasmtime)]
use std::path::Path;

#[cfg(runtime_wasmtime)]
pub struct SpinSqliteEventStore<A> {
    db_name: String,
    _phantom: PhantomData<fn() -> A>,
}

#[cfg(runtime_wasmtime)]
impl<A> Clone for SpinSqliteEventStore<A> {
    fn clone(&self) -> Self {
        Self {
            db_name: self.db_name.clone(),
            _phantom: PhantomData,
        }
    }
}

#[cfg(runtime_wasmtime)]
impl<A> SpinSqliteEventStore<A>
where
    A: Aggregate,
{
    pub fn new(db_name: impl Into<String>) -> Self {
        Self {
            db_name: db_name.into(),
            _phantom: PhantomData,
        }
    }

    pub fn initialize_schema(&self) -> Result<(), String> {
        fs::create_dir_all("/data").map_err(|e| e.to_string())?;
        
        let events_path = Path::new("/data/events.json");
        if !events_path.exists() {
            fs::write(events_path, "[]").map_err(|e| e.to_string())?;
        }

        let checkpoints_path = Path::new("/data/checkpoints.json");
        if !checkpoints_path.exists() {
            fs::write(checkpoints_path, "{}").map_err(|e| e.to_string())?;
        }

        let rm_path = Path::new("/data/counter_read_model.json");
        if !rm_path.exists() {
            fs::write(rm_path, "{}").map_err(|e| e.to_string())?;
        }

        Ok(())
    }
}

#[cfg(runtime_wasmtime)]
impl<A> EventStore<A> for SpinSqliteEventStore<A>
where
    A: Aggregate + 'static,
    A::Event: serde::Serialize + serde::de::DeserializeOwned + Clone,
    A::Id: serde::Serialize + serde::de::DeserializeOwned + Clone + PartialEq + std::fmt::Display,
{
    type Error = EventStoreError;

    fn load(&self, aggregate_id: &A::Id) -> Result<Vec<EventEnvelope<A::Event, A::Id>>, Self::Error> {
        let events_path = Path::new("/data/events.json");
        if !events_path.exists() {
            return Ok(Vec::new());
        }

        let content = fs::read_to_string(events_path)
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;
        
        let values: Vec<serde_json::Value> = serde_json::from_str(&content)
            .map_err(|e| EventStoreError::Deserialization(e.to_string()))?;

        let mut envelopes = Vec::new();
        for val in values {
            if let Some(agg_type) = val.get("aggregate_type").and_then(|v| v.as_str()) {
                if agg_type == A::aggregate_type() {
                    if let Some(id_val) = val.get("aggregate_id") {
                        if let Ok(id) = serde_json::from_value::<A::Id>(id_val.clone()) {
                            if &id == aggregate_id {
                                if let Ok(envelope) = serde_json::from_value::<EventEnvelope<A::Event, A::Id>>(val) {
                                    envelopes.push(envelope);
                                }
                            }
                        }
                    }
                }
            }
        }

        envelopes.sort_by_key(|e| e.revision);
        Ok(envelopes)
    }

    fn append(
        &self,
        aggregate_id: &A::Id,
        expected_revision: ExpectedRevision,
        events: Vec<NewEvent<A::Event>>,
    ) -> Result<Vec<EventEnvelope<A::Event, A::Id>>, Self::Error> {
        let events_path = Path::new("/data/events.json");
        let content = if events_path.exists() {
            fs::read_to_string(events_path).map_err(|e| EventStoreError::Backend(e.to_string()))?
        } else {
            "[]".to_string()
        };

        let mut all_values: Vec<serde_json::Value> = serde_json::from_str(&content)
            .map_err(|e| EventStoreError::Deserialization(e.to_string()))?;

        let mut current_revision = 0u64;
        let mut max_sequence = 0u64;

        for val in &all_values {
            if let Some(seq) = val.get("sequence").and_then(|s| s.as_u64()) {
                if seq > max_sequence {
                    max_sequence = seq;
                }
            }

            if let Some(agg_type) = val.get("aggregate_type").and_then(|v| v.as_str()) {
                if agg_type == A::aggregate_type() {
                    if let Some(id_val) = val.get("aggregate_id") {
                        if let Ok(id) = serde_json::from_value::<A::Id>(id_val.clone()) {
                            if &id == aggregate_id {
                                if let Some(rev) = val.get("revision").and_then(|r| r.as_u64()) {
                                    if rev > current_revision {
                                        current_revision = rev;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        match expected_revision {
            ExpectedRevision::Any => {}
            ExpectedRevision::NoStream if current_revision == 0 => {}
            ExpectedRevision::NoStream => {
                return Err(EventStoreError::Concurrency(
                    ddd_cqrs_es::ConcurrencyError::StreamAlreadyExists,
                ));
            }
            ExpectedRevision::Exact(expected) if expected == current_revision => {}
            ExpectedRevision::Exact(_) => {
                return Err(EventStoreError::Concurrency(
                    ddd_cqrs_es::ConcurrencyError::WrongExpectedRevision {
                        expected: expected_revision,
                        actual: current_revision,
                    },
                ));
            }
        }

        if events.is_empty() {
            return Ok(Vec::new());
        }

        let mut envelopes = Vec::new();
        let now = std::time::SystemTime::now();

        for (i, event) in events.into_iter().enumerate() {
            let revision = current_revision + i as u64 + 1;
            let sequence = max_sequence + i as u64 + 1;
            let event_id = EventId::new();

            let envelope = EventEnvelope::new(
                event_id,
                aggregate_id.clone(),
                A::aggregate_type().to_string(),
                revision,
                Some(sequence),
                event.event_type,
                event.event_version,
                event.payload,
                event.metadata,
                now,
            );

            let val = serde_json::to_value(&envelope)
                .map_err(|e| EventStoreError::Serialization(e.to_string()))?;
            
            all_values.push(val);
            envelopes.push(envelope);
        }

        let new_content = serde_json::to_string(&all_values)
            .map_err(|e| EventStoreError::Serialization(e.to_string()))?;
        fs::write(events_path, new_content)
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;

        Ok(envelopes)
    }

    fn load_global_after(&self, sequence: Option<u64>) -> Result<Vec<EventEnvelope<A::Event, A::Id>>, Self::Error> {
        let seq = sequence.unwrap_or(0);
        let events_path = Path::new("/data/events.json");
        if !events_path.exists() {
            return Ok(Vec::new());
        }

        let content = fs::read_to_string(events_path)
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;
        
        let values: Vec<serde_json::Value> = serde_json::from_str(&content)
            .map_err(|e| EventStoreError::Deserialization(e.to_string()))?;

        let mut envelopes = Vec::new();
        for val in values {
            if let Some(agg_type) = val.get("aggregate_type").and_then(|v| v.as_str()) {
                if agg_type == A::aggregate_type() {
                    if let Some(s) = val.get("sequence").and_then(|s| s.as_u64()) {
                        if s > seq {
                            if let Ok(envelope) = serde_json::from_value::<EventEnvelope<A::Event, A::Id>>(val) {
                                envelopes.push(envelope);
                            }
                        }
                    }
                }
            }
        }

        envelopes.sort_by_key(|e| e.sequence.unwrap_or(0));
        Ok(envelopes)
    }
}

#[cfg(runtime_wasmtime)]
pub struct SpinSqliteCheckpointStore {
    db_name: String,
}

#[cfg(runtime_wasmtime)]
impl Clone for SpinSqliteCheckpointStore {
    fn clone(&self) -> Self {
        Self {
            db_name: self.db_name.clone(),
        }
    }
}

#[cfg(runtime_wasmtime)]
impl SpinSqliteCheckpointStore {
    pub fn new(db_name: impl Into<String>) -> Self {
        Self {
            db_name: db_name.into(),
        }
    }

    pub async fn load_checkpoint_async(&self, projection_name: &str) -> Result<Option<u64>, EventStoreError> {
        use ddd_cqrs_es::CheckpointStore;
        self.load_checkpoint(projection_name)
    }

    pub async fn save_checkpoint_async(&self, projection_name: &str, sequence: u64) -> Result<(), EventStoreError> {
        use ddd_cqrs_es::CheckpointStore;
        self.save_checkpoint(projection_name, sequence)
    }
}

#[cfg(runtime_wasmtime)]
impl ddd_cqrs_es::CheckpointStore for SpinSqliteCheckpointStore {
    type Error = EventStoreError;

    fn load_checkpoint(&self, projection_name: &str) -> Result<Option<u64>, Self::Error> {
        let path = Path::new("/data/checkpoints.json");
        if !path.exists() {
            return Ok(None);
        }

        let content = fs::read_to_string(path)
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;
        
        let map: std::collections::HashMap<String, u64> = serde_json::from_str(&content)
            .map_err(|e| EventStoreError::Deserialization(e.to_string()))?;
        
        Ok(map.get(projection_name).copied())
    }

    fn save_checkpoint(&self, projection_name: &str, sequence: u64) -> Result<(), Self::Error> {
        let path = Path::new("/data/checkpoints.json");
        let mut map: std::collections::HashMap<String, u64> = if path.exists() {
            let content = fs::read_to_string(path)
                .map_err(|e| EventStoreError::Backend(e.to_string()))?;
            serde_json::from_str(&content)
                .map_err(|e| EventStoreError::Deserialization(e.to_string()))?
        } else {
            std::collections::HashMap::new()
        };

        map.insert(projection_name.to_string(), sequence);

        let new_content = serde_json::to_string(&map)
            .map_err(|e| EventStoreError::Serialization(e.to_string()))?;
        fs::write(path, new_content)
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;
        
        Ok(())
    }
}

#[cfg(runtime_wasmtime)]
pub struct CounterProjection {
    #[allow(dead_code)]
    db_name: String,
}

#[cfg(runtime_wasmtime)]
impl CounterProjection {
    pub fn new(db_name: impl Into<String>) -> Self {
        Self {
            db_name: db_name.into(),
        }
    }

    pub async fn apply_async(&mut self, envelope: &EventEnvelope<crate::domain::CounterEvent, crate::domain::CounterId>) -> Result<(), EventStoreError> {
        use ddd_cqrs_es::Projection;
        self.apply(envelope)
    }
}

#[cfg(runtime_wasmtime)]
impl ddd_cqrs_es::Projection<crate::domain::CounterEvent, crate::domain::CounterId> for CounterProjection {
    type Error = EventStoreError;

    fn name(&self) -> &'static str {
        "counter_projection"
    }

    fn apply(&mut self, envelope: &EventEnvelope<crate::domain::CounterEvent, crate::domain::CounterId>) -> Result<(), Self::Error> {
        let aggregate_id_str = serde_json::to_string(&envelope.aggregate_id)
            .map_err(|e| EventStoreError::Serialization(e.to_string()))?;
        
        let path = Path::new("/data/counter_read_model.json");
        let mut map: std::collections::HashMap<String, i32> = if path.exists() {
            let content = fs::read_to_string(path)
                .map_err(|e| EventStoreError::Backend(e.to_string()))?;
            serde_json::from_str(&content)
                .map_err(|e| EventStoreError::Deserialization(e.to_string()))?
        } else {
            std::collections::HashMap::new()
        };

        let current_value = map.get(&aggregate_id_str).copied().unwrap_or(0);

        let new_value = match envelope.payload {
            crate::domain::CounterEvent::Incremented { amount } => current_value.saturating_add(amount),
            crate::domain::CounterEvent::Decremented { amount } => current_value.saturating_sub(amount),
            crate::domain::CounterEvent::ResetPerformed { value } => value,
        };

        map.insert(aggregate_id_str, new_value);

        let new_content = serde_json::to_string(&map)
            .map_err(|e| EventStoreError::Serialization(e.to_string()))?;
        fs::write(path, new_content)
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;

        Ok(())
    }
}


// =========================================================================
// 3. MULTI-BACKEND ENGINE IMPLEMENTATION (NEW)
// =========================================================================

fn get_backend() -> String {
    std::env::var("DATABASE_BACKEND").unwrap_or_else(|_| "sqlite".to_string())
}

fn parse_postgres_url_for_http(url: &str) -> (String, Option<String>) {
    if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        let stripped = if url.starts_with("postgres://") {
            &url["postgres://".len()..]
        } else {
            &url["postgresql://".len()..]
        };

        if let Some(at_idx) = stripped.find('@') {
            let auth_part = &stripped[..at_idx];
            let host_part = &stripped[at_idx + 1..];

            let password = if let Some(colon_idx) = auth_part.find(':') {
                Some(auth_part[colon_idx + 1..].to_string())
            } else {
                None
            };

            let host = if let Some(slash_idx) = host_part.find('/') {
                &host_part[..slash_idx]
            } else if let Some(query_idx) = host_part.find('?') {
                &host_part[..query_idx]
            } else {
                host_part
            };

            let http_url = format!("https://{}/sql", host);
            return (http_url, password);
        }
    }
    
    (url.to_string(), None)
}

fn get_postgres_url() -> String {
    let backend = get_backend();
    
    // For supabase over HTTP (wasmtime), we need SUPABASE_URL (https://)
    #[cfg(not(runtime_spin))]
    {
        if backend == "supabase" {
            if let Ok(sup_url) = std::env::var("SUPABASE_URL") {
                if !sup_url.is_empty() {
                    return sup_url;
                }
            }
        }
    }

    // For all other cases, or if SUPABASE_URL is missing, expect a standard PostgreSQL URL
    let url = std::env::var("POSTGRES_URL")
        .or_else(|_| std::env::var("NEON_URL"))
        .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost:5432/postgres".to_string());
        
    if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        if backend == "neon" {
            #[cfg(not(runtime_spin))]
            {
                let (http_url, _) = parse_postgres_url_for_http(&url);
                return http_url;
            }
        }
    }
    url
}

#[allow(dead_code)]
fn get_neon_api_key() -> Option<String> {
    std::env::var("NEON_API_KEY").ok().filter(|s| !s.is_empty())
}

fn get_turso_url() -> String {
    std::env::var("TURSO_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8080".to_string())
}

fn get_turso_auth_token() -> Option<String> {
    std::env::var("TURSO_AUTH_TOKEN").ok().filter(|s| !s.is_empty())
}

// Outbound HTTP helpers using WASIp3 HTTP standard
async fn wasi_http_post(
    url: &str,
    headers: Vec<(String, String)>,
    body_data: Vec<u8>,
) -> Result<Vec<u8>, String> {
    use http_body_util::Full;
    use bytes::Bytes;
    use http::Request;
    use http_body_util::BodyExt;

    let body = Full::new(Bytes::from(body_data));
    let mut req_builder = Request::builder()
        .method("POST")
        .uri(url);

    for (name, value) in headers {
        req_builder = req_builder.header(name, value);
    }

    let req = req_builder.body(body)
        .map_err(|e| format!("Failed to build HTTP request: {:?}", e))?;

    let wasi_req = wasip3::http_compat::http_into_wasi_request(req)
        .map_err(|e| format!("Failed to convert to WASI request: {:?}", e))?;

    let wasi_resp = wasip3::http::client::send(wasi_req).await
        .map_err(|e| format!("WASI HTTP send error: {:?}", e))?;

    let http_resp = wasip3::http_compat::http_from_wasi_response(wasi_resp)
        .map_err(|e| format!("Failed to convert from WASI response: {:?}", e))?;

    let status = http_resp.status();
    let body_bytes = http_resp.into_body().collect().await
        .map_err(|e| format!("Failed to collect response body: {:?}", e))?
        .to_bytes()
        .to_vec();

    if !status.is_success() {
        let body_str = String::from_utf8_lossy(&body_bytes);
        return Err(format!(
            "HTTP request failed with status {}: {}",
            status, body_str
        ));
    }

    Ok(body_bytes)
}

// -------------------------------------------------------------------------
// Neon / Serverless Postgres HTTP helper
// -------------------------------------------------------------------------
#[allow(dead_code)]
fn base64_encode(input: &[u8]) -> String {
    const CHARSET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity((input.len() + 2) / 3 * 4);
    let mut i = 0;
    while i < input.len() {
        let b0 = input[i] as usize;
        let b1 = if i + 1 < input.len() { input[i + 1] as usize } else { 0 };
        let b2 = if i + 2 < input.len() { input[i + 2] as usize } else { 0 };

        let triple = (b0 << 16) | (b1 << 8) | b2;

        result.push(CHARSET[(triple >> 18) & 63] as char);
        result.push(CHARSET[(triple >> 12) & 63] as char);
        result.push(if i + 1 < input.len() { CHARSET[(triple >> 6) & 63] as char } else { '=' });
        result.push(if i + 2 < input.len() { CHARSET[triple & 63] as char } else { '=' });

        i += 3;
    }
    result
}

async fn execute_neon_query(
    url: &str,
    api_key: Option<&str>,
    sql: &str,
    params: Vec<serde_json::Value>,
) -> Result<Vec<serde_json::Value>, String> {
    let req_payload = serde_json::json!({
        "query": sql,
        "params": params,
    });
    let body_data = serde_json::to_vec(&req_payload)
        .map_err(|e| e.to_string())?;

    let mut headers = vec![
        ("Content-Type".to_string(), "application/json".to_string()),
        ("Neon-Raw-Text-Output".to_string(), "true".to_string()),
        ("Neon-Array-Mode".to_string(), "true".to_string()),
    ];
    
    println!("DEBUG execute_neon_query: url={}, api_key={:?}", url, api_key);
    if let Ok(conn_str) = std::env::var("POSTGRES_URL").or_else(|_| std::env::var("NEON_URL")) {
        println!("DEBUG POSTGRES_URL is set: {}", conn_str);
        headers.push(("Neon-Connection-String".to_string(), conn_str));
    } else {
        println!("DEBUG POSTGRES_URL is NOT set!");
    }
    
    if let Some(key) = api_key {
        if !key.is_empty() {
            headers.push(("Authorization".to_string(), format!("Bearer {}", key)));
        }
    }
    
    println!("DEBUG final headers before post: {:?}", headers);
    let resp_bytes = wasi_http_post(url, headers, body_data).await?;
    
    let resp_val: serde_json::Value = serde_json::from_slice(&resp_bytes)
        .map_err(|e| format!("Failed to parse Neon response JSON: {}. Body was: {}", e, String::from_utf8_lossy(&resp_bytes)))?;

    let mut parsed_rows = Vec::new();
    if let Some(arr) = resp_val.as_array() {
        parsed_rows = arr.clone();
    } else if let Some(obj) = resp_val.as_object() {
        if let (Some(fields_val), Some(rows_val)) = (obj.get("fields"), obj.get("rows")) {
            if let (Some(fields_arr), Some(rows_arr)) = (fields_val.as_array(), rows_val.as_array()) {
                let col_names: Vec<String> = fields_arr
                    .iter()
                    .map(|f| f.get("name").and_then(|n| n.as_str()).unwrap_or("").to_string())
                    .collect();
                
                for row_val in rows_arr {
                    if let Some(row_arr) = row_val.as_array() {
                        let mut row_obj = serde_json::Map::new();
                        for (i, col_val) in row_arr.iter().enumerate() {
                            if i < col_names.len() {
                                row_obj.insert(col_names[i].clone(), col_val.clone());
                            }
                        }
                        parsed_rows.push(serde_json::Value::Object(row_obj));
                    }
                }
            }
        }
    }
    
    Ok(parsed_rows)
}

// -------------------------------------------------------------------------
// Supabase PostgREST RPC HTTP helper
// -------------------------------------------------------------------------
#[allow(dead_code)]
fn get_supabase_secret_key() -> Option<String> {
    std::env::var("SUPABASE_SECRET_KEY")
        .or_else(|_| std::env::var("SUPABASE_PUBLISHABLE_KEY"))
        .ok()
        .filter(|s| !s.is_empty())
}

#[allow(dead_code)]
async fn execute_supabase_query(
    url: &str,
    secret_key: Option<&str>,
    sql: &str,
    params: Vec<serde_json::Value>,
) -> Result<Vec<serde_json::Value>, String> {
    let rpc_url = if url.ends_with("/rest/v1/rpc/execute_sql") {
        url.to_string()
    } else {
        format!("{}/rest/v1/rpc/execute_sql", url.trim_end_matches('/'))
    };

    let req_payload = serde_json::json!({
        "query_text": sql,
        "query_params": params,
    });
    let body_data = serde_json::to_vec(&req_payload)
        .map_err(|e| e.to_string())?;

    let mut headers = vec![
        ("Content-Type".to_string(), "application/json".to_string()),
    ];
    if let Some(key) = secret_key {
        headers.push(("apikey".to_string(), key.to_string()));
        headers.push(("Authorization".to_string(), format!("Bearer {}", key)));
    }

    let resp_bytes = wasi_http_post(&rpc_url, headers, body_data).await?;

    let resp_val: serde_json::Value = serde_json::from_slice(&resp_bytes)
        .map_err(|e| format!("Failed to parse Supabase response: {}. Body was: {}", e, String::from_utf8_lossy(&resp_bytes)))?;

    if let Some(err_obj) = resp_val.as_object() {
        if let Some(err_msg) = err_obj.get("error") {
            return Err(format!("Supabase SQL error: {}", err_msg));
        }
    }

    let rows = resp_val.as_array()
        .cloned()
        .unwrap_or_else(|| vec![resp_val]);

    Ok(rows)
}

struct PgConnParams {
    host: String,
    port: u16,
    user: String,
    password: Option<String>,
    database: String,
}

fn parse_pg_url(url: &str) -> Result<PgConnParams, String> {
    let stripped = if url.starts_with("postgres://") {
        &url["postgres://".len()..]
    } else if url.starts_with("postgresql://") {
        &url["postgresql://".len()..]
    } else {
        return Err(format!("Invalid postgres URL prefix: {}", url));
    };

    let (main_part, _) = if let Some(q_idx) = stripped.find('?') {
        (&stripped[..q_idx], &stripped[q_idx+1..])
    } else {
        (stripped, "")
    };

    let (auth_part, host_db_part) = if let Some(at_idx) = main_part.find('@') {
        (Some(&main_part[..at_idx]), &main_part[at_idx+1..])
    } else {
        (None, main_part)
    };

    let mut user = "postgres".to_string();
    let mut password = None;
    if let Some(auth) = auth_part {
        if let Some(colon_idx) = auth.find(':') {
            user = auth[..colon_idx].to_string();
            password = Some(auth[colon_idx+1..].to_string());
        } else {
            user = auth.to_string();
        }
    }

    let (host_port_part, database) = if let Some(slash_idx) = host_db_part.find('/') {
        (&host_db_part[..slash_idx], host_db_part[slash_idx+1..].to_string())
    } else {
        (host_db_part, "postgres".to_string())
    };

    let mut host = host_port_part.to_string();
    let mut port = 5432;
    if let Some(colon_idx) = host_port_part.find(':') {
        host = host_port_part[..colon_idx].to_string();
        if let Ok(p) = host_port_part[colon_idx+1..].parse::<u16>() {
            port = p;
        }
    }

    if host.is_empty() {
        host = "localhost".to_string();
    }
    let database = if database.is_empty() { "postgres".to_string() } else { database };

    Ok(PgConnParams {
        host,
        port,
        user,
        password,
        database,
    })
}

fn format_pg_value(val: &serde_json::Value) -> Result<String, String> {
    match val {
        serde_json::Value::Null => Ok("NULL".to_string()),
        serde_json::Value::Bool(b) => Ok(if *b { "true" } else { "false" }.to_string()),
        serde_json::Value::Number(n) => Ok(n.to_string()),
        serde_json::Value::String(s) => {
            let escaped = s.replace('\'', "''");
            Ok(format!("'{}'", escaped))
        }
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            let s = serde_json::to_string(val).map_err(|e| e.to_string())?;
            let escaped = s.replace('\'', "''");
            Ok(format!("'{}'", escaped))
        }
    }
}

fn interpolate_query(sql: &str, params: &[serde_json::Value]) -> Result<String, String> {
    let mut final_sql = String::new();
    let mut chars = sql.chars().peekable();
    
    while let Some(c) = chars.next() {
        if c == '$' {
            let mut digits = String::new();
            while let Some(&next_c) = chars.peek() {
                if next_c.is_ascii_digit() {
                    digits.push(next_c);
                    chars.next();
                } else {
                    break;
                }
            }
            if digits.is_empty() {
                final_sql.push('$');
            } else {
                let idx = digits.parse::<usize>().map_err(|e| e.to_string())?;
                if idx == 0 || idx > params.len() {
                    return Err(format!("Parameter index ${} out of bounds (params len: {})", idx, params.len()));
                }
                let param_val = &params[idx - 1];
                let formatted = format_pg_value(param_val)?;
                final_sql.push_str(&formatted);
            }
        } else {
            final_sql.push(c);
        }
    }
    
    Ok(final_sql)
}

enum PgStream {
    Plain(std::net::TcpStream),
    Tls(rustls::StreamOwned<rustls::ClientConnection, std::net::TcpStream>),
}

impl std::io::Read for PgStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            PgStream::Plain(s) => s.read(buf),
            PgStream::Tls(s) => s.read(buf),
        }
    }
}

impl std::io::Write for PgStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            PgStream::Plain(s) => s.write(buf),
            PgStream::Tls(s) => s.write(buf),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            PgStream::Plain(s) => s.flush(),
            PgStream::Tls(s) => s.flush(),
        }
    }
}

fn write_startup_message(stream: &mut PgStream, user: &str, database: &str) -> std::io::Result<()> {
    use std::io::Write;
    let mut body = Vec::new();
    body.extend_from_slice(&0x00030000u32.to_be_bytes()); // Protocol v3.0
    
    body.extend_from_slice(b"user\0");
    body.extend_from_slice(user.as_bytes());
    body.extend_from_slice(b"\0");
    
    body.extend_from_slice(b"database\0");
    body.extend_from_slice(database.as_bytes());
    body.extend_from_slice(b"\0");
    
    body.extend_from_slice(b"\0");
    
    let length = (body.len() + 4) as u32;
    stream.write_all(&length.to_be_bytes())?;
    stream.write_all(&body)?;
    stream.flush()?;
    Ok(())
}

fn write_query_message(stream: &mut PgStream, sql: &str) -> std::io::Result<()> {
    use std::io::Write;
    let mut body = Vec::new();
    body.extend_from_slice(sql.as_bytes());
    body.extend_from_slice(b"\0");
    
    let length = (body.len() + 4) as u32;
    stream.write_all(&[b'Q'])?;
    stream.write_all(&length.to_be_bytes())?;
    stream.write_all(&body)?;
    stream.flush()?;
    Ok(())
}

fn write_password_message(stream: &mut PgStream, password: &str) -> std::io::Result<()> {
    use std::io::Write;
    let mut body = Vec::new();
    body.extend_from_slice(password.as_bytes());
    body.extend_from_slice(b"\0");
    
    let length = (body.len() + 4) as u32;
    stream.write_all(&[b'p'])?;
    stream.write_all(&length.to_be_bytes())?;
    stream.write_all(&body)?;
    stream.flush()?;
    Ok(())
}

fn generate_client_nonce() -> String {
    use std::time::SystemTime;
    let seed = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(42) as u64;
    
    let mut rng = seed;
    let chars = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut nonce = String::with_capacity(24);
    for _ in 0..24 {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let idx = (rng % chars.len() as u64) as usize;
        nonce.push(chars[idx] as char);
    }
    nonce
}

fn write_sasl_initial_response(stream: &mut PgStream, mechanism: &str, initial_response: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut body = Vec::new();
    body.extend_from_slice(mechanism.as_bytes());
    body.push(0); // null terminator for mechanism string
    
    let data_len = initial_response.len() as i32;
    body.extend_from_slice(&data_len.to_be_bytes());
    body.extend_from_slice(initial_response);
    
    let length = (body.len() + 4) as u32;
    stream.write_all(&[b'p'])?;
    stream.write_all(&length.to_be_bytes())?;
    stream.write_all(&body)?;
    stream.flush()?;
    Ok(())
}

fn write_sasl_response(stream: &mut PgStream, response: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut body = Vec::new();
    body.extend_from_slice(response);
    
    let length = (body.len() + 4) as u32;
    stream.write_all(&[b'p'])?;
    stream.write_all(&length.to_be_bytes())?;
    stream.write_all(&body)?;
    stream.flush()?;
    Ok(())
}

struct PgMessage {
    msg_type: u8,
    payload: Vec<u8>,
}

fn read_message(stream: &mut PgStream) -> std::io::Result<PgMessage> {
    use std::io::Read;
    let mut type_buf = [0u8; 1];
    stream.read_exact(&mut type_buf)?;
    let msg_type = type_buf[0];
    
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let length = u32::from_be_bytes(len_buf);
    
    if length < 4 {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid message length"));
    }
    
    let payload_len = (length - 4) as usize;
    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        stream.read_exact(&mut payload)?;
    }
    
    Ok(PgMessage { msg_type, payload })
}

fn parse_error_response(payload: &[u8]) -> String {
    let mut parts = Vec::new();
    let mut idx = 0;
    while idx < payload.len() && payload[idx] != 0 {
        let field_type = payload[idx] as char;
        idx += 1;
        let mut end = idx;
        while end < payload.len() && payload[end] != 0 {
            end += 1;
        }
        if let Ok(s) = std::str::from_utf8(&payload[idx..end]) {
            parts.push(format!("{}: {}", field_type, s));
        }
        idx = end + 1;
    }
    parts.join(", ")
}

struct PgColumn {
    name: String,
    type_oid: u32,
}

fn parse_row_description(payload: &[u8]) -> std::io::Result<Vec<PgColumn>> {
    if payload.len() < 2 {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid RowDescription length"));
    }
    let num_fields = u16::from_be_bytes([payload[0], payload[1]]) as usize;
    let mut columns = Vec::with_capacity(num_fields);
    
    let mut idx = 2;
    for _ in 0..num_fields {
        let start = idx;
        while idx < payload.len() && payload[idx] != 0 {
            idx += 1;
        }
        if idx >= payload.len() {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid RowDescription name"));
        }
        let name = std::str::from_utf8(&payload[start..idx])
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?
            .to_string();
        idx += 1; // skip null byte
        
        if idx + 18 > payload.len() {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid RowDescription column metadata"));
        }
        idx += 6;
        let type_oid = u32::from_be_bytes([payload[idx], payload[idx+1], payload[idx+2], payload[idx+3]]);
        idx += 4;
        idx += 8;
        
        columns.push(PgColumn { name, type_oid });
    }
    
    Ok(columns)
}

fn parse_data_row(payload: &[u8], columns: &[PgColumn]) -> std::io::Result<serde_json::Value> {
    if payload.len() < 2 {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid DataRow length"));
    }
    let num_values = u16::from_be_bytes([payload[0], payload[1]]) as usize;
    if num_values != columns.len() {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "DataRow column count mismatch"));
    }
    
    let mut row_obj = serde_json::Map::new();
    let mut idx = 2;
    
    for col in columns {
        if idx + 4 > payload.len() {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid DataRow column size prefix"));
        }
        let val_len = i32::from_be_bytes([payload[idx], payload[idx+1], payload[idx+2], payload[idx+3]]);
        idx += 4;
        
        if val_len < 0 {
            row_obj.insert(col.name.clone(), serde_json::Value::Null);
        } else {
            let val_len = val_len as usize;
            if idx + val_len > payload.len() {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid DataRow column data length"));
            }
            let val_bytes = &payload[idx..idx+val_len];
            idx += val_len;
            
            let text_val = std::str::from_utf8(val_bytes)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                
            let json_val = match col.type_oid {
                16 => {
                    serde_json::Value::Bool(text_val == "t" || text_val == "true" || text_val == "1")
                }
                20 | 21 | 23 => {
                    if let Ok(i) = text_val.parse::<i64>() {
                        serde_json::Value::Number(serde_json::Number::from(i))
                    } else {
                        serde_json::Value::String(text_val.to_string())
                    }
                }
                700 | 701 | 1700 => {
                    if let Ok(f) = text_val.parse::<f64>() {
                        if let Some(num) = serde_json::Number::from_f64(f) {
                            serde_json::Value::Number(num)
                        } else {
                            serde_json::Value::String(text_val.to_string())
                        }
                    } else {
                        serde_json::Value::String(text_val.to_string())
                    }
                }
                114 | 3802 => {
                    if let Ok(jv) = serde_json::from_str::<serde_json::Value>(text_val) {
                        jv
                    } else {
                        serde_json::Value::String(text_val.to_string())
                    }
                }
                _ => {
                    serde_json::Value::String(text_val.to_string())
                }
            };
            row_obj.insert(col.name.clone(), json_val);
        }
    }
    
    Ok(serde_json::Value::Object(row_obj))
}

static PG_CONN: std::sync::Mutex<Option<(String, PgStream)>> = std::sync::Mutex::new(None);

fn connect_and_auth_postgres(
    url: &str,
    pg_params: &PgConnParams,
    addr: &str,
) -> Result<PgStream, String> {
    let mut stream = std::net::TcpStream::connect(addr)
        .map_err(|e| format!("Failed to connect to Postgres at {}: {}", addr, e))?;
        
    use std::io::{Read, Write};
    
    // Send SSLRequest first
    let ssl_request = [0u8, 0, 0, 8, 4, 210, 22, 47];
    stream.write_all(&ssl_request)
        .map_err(|e| format!("Failed to send SSLRequest: {}", e))?;
    stream.flush()
        .map_err(|e| format!("Failed to flush SSLRequest: {}", e))?;

    let mut ssl_response = [0u8; 1];
    stream.read_exact(&mut ssl_response)
        .map_err(|e| format!("Failed to read SSLRequest response: {}", e))?;

    let mut pg_stream = if ssl_response[0] == b'S' {
        let _ = rustls_rustcrypto::provider().install_default();

        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        let config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        let server_name = rustls::pki_types::ServerName::try_from(pg_params.host.as_str())
            .map_err(|e| format!("Invalid server name '{}': {}", pg_params.host, e))?
            .to_owned();

        let conn = rustls::ClientConnection::new(std::sync::Arc::new(config), server_name)
            .map_err(|e| format!("Failed to create rustls ClientConnection: {}", e))?;

        let tls_stream = rustls::StreamOwned::new(conn, stream);
        PgStream::Tls(tls_stream)
    } else if ssl_response[0] == b'N' {
        if url.contains("sslmode=require") {
            return Err("Server rejected SSL request, but sslmode=require was requested".to_string());
        }
        PgStream::Plain(stream)
    } else {
        return Err(format!("Unexpected response to SSLRequest: {:?}", ssl_response[0] as char));
    };
        
    write_startup_message(&mut pg_stream, &pg_params.user, &pg_params.database)
        .map_err(|e| format!("Failed to send startup message: {}", e))?;
        
    loop {
        let msg = read_message(&mut pg_stream)
            .map_err(|e| format!("Failed to read message from Postgres: {}", e))?;
            
        match msg.msg_type {
            b'R' => {
                if msg.payload.len() < 4 {
                    return Err("Invalid AuthenticationRequest payload".to_string());
                }
                let auth_type = u32::from_be_bytes([msg.payload[0], msg.payload[1], msg.payload[2], msg.payload[3]]);
                match auth_type {
                    0 => {} // Auth OK
                    3 => {
                        let pwd = pg_params.password.as_deref().unwrap_or("");
                        write_password_message(&mut pg_stream, pwd)
                            .map_err(|e| format!("Failed to send password message: {}", e))?;
                    }
                    5 => {
                        if msg.payload.len() < 8 {
                            return Err("Invalid AuthenticationMD5Password payload".to_string());
                        }
                        let salt = &msg.payload[4..8];
                        let pwd = pg_params.password.as_deref().unwrap_or("");
                        
                        let hash1 = format!("{:x}", md5::compute(format!("{}{}", pwd, pg_params.user)));
                        let mut hash2_input = Vec::new();
                        hash2_input.extend_from_slice(hash1.as_bytes());
                        hash2_input.extend_from_slice(salt);
                        let hash2 = format!("md5{:x}", md5::compute(&hash2_input));
                        
                        write_password_message(&mut pg_stream, &hash2)
                            .map_err(|e| format!("Failed to send MD5 password message: {}", e))?;
                    }
                    10 => {
                        // SCRAM-SHA-256 Authentication
                        let mut has_scram = false;
                        let mut idx = 4;
                        while idx < msg.payload.len() {
                            let start = idx;
                            while idx < msg.payload.len() && msg.payload[idx] != 0 {
                                idx += 1;
                            }
                            if idx < msg.payload.len() {
                                let mech = std::str::from_utf8(&msg.payload[start..idx]).unwrap_or("");
                                if mech == "SCRAM-SHA-256" {
                                    has_scram = true;
                                    break;
                                }
                            }
                            idx += 1; // skip null byte
                        }
                        
                        if !has_scram {
                            return Err("Server SASL mechanisms do not support SCRAM-SHA-256".to_string());
                        }
                        
                        // 1. Generate client nonce
                        let client_nonce = generate_client_nonce();
                        let client_first_message_bare = format!("n={},r={}", pg_params.user, client_nonce);
                        let client_first_message = format!("n,,{}", client_first_message_bare);
                        
                        // 2. Write SASLInitialResponse
                        write_sasl_initial_response(&mut pg_stream, "SCRAM-SHA-256", client_first_message.as_bytes())
                            .map_err(|e| format!("Failed to write SASLInitialResponse: {}", e))?;
                        
                        // 3. Read SASLContinue message (R / 11)
                        let next_msg = read_message(&mut pg_stream)
                            .map_err(|e| format!("Failed to read SASLContinue message: {}", e))?;
                            
                        if next_msg.msg_type == b'E' {
                            let err_msg = parse_error_response(&next_msg.payload);
                            return Err(format!("Postgres authentication failed: {}", err_msg));
                        }
                        if next_msg.msg_type != b'R' {
                            return Err(format!("Expected AuthenticationRequest ('R') during SASL, got '{}'", next_msg.msg_type as char));
                        }
                        
                        let next_auth_type = u32::from_be_bytes([next_msg.payload[0], next_msg.payload[1], next_msg.payload[2], next_msg.payload[3]]);
                        if next_auth_type != 11 {
                            return Err(format!("Expected SASLContinue (11), got {}", next_auth_type));
                        }
                        
                        let server_first_message_str = std::str::from_utf8(&next_msg.payload[4..])
                            .map_err(|e| format!("Invalid UTF-8 in SASLContinue payload: {}", e))?;
                            
                        let mut server_nonce = "";
                        let mut salt_base64 = "";
                        let mut iterations_str = "";

                        for part in server_first_message_str.split(',') {
                            if let Some(val) = part.strip_prefix("r=") {
                                server_nonce = val;
                            } else if let Some(val) = part.strip_prefix("s=") {
                                salt_base64 = val;
                            } else if let Some(val) = part.strip_prefix("i=") {
                                iterations_str = val;
                            }
                        }
                        
                        if !server_nonce.starts_with(&client_nonce) {
                            return Err("Server nonce does not match client nonce prefix".to_string());
                        }
                        
                        use base64::Engine;
                        let salt = base64::engine::general_purpose::STANDARD.decode(salt_base64)
                            .map_err(|e| format!("Invalid salt base64: {}", e))?;
                        let iterations = iterations_str.parse::<u32>()
                            .map_err(|e| format!("Invalid iterations '{}': {}", iterations_str, e))?;
                            
                        // 4. Compute proof
                        let client_final_message_without_proof = format!("c=biws,r={}", server_nonce);
                        let auth_message = format!("{},{},{}", client_first_message_bare, server_first_message_str, client_final_message_without_proof);
                        
                        let mut salted_password = [0u8; 32];
                        let _ = pbkdf2::pbkdf2::<hmac::Hmac<sha2::Sha256>>(
                            pg_params.password.as_deref().unwrap_or("").as_bytes(),
                            &salt,
                            iterations,
                            &mut salted_password,
                        );
                        
                        use hmac::Mac;
                        let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(&salted_password)
                            .map_err(|e| format!("Failed to create HMAC-SHA256 for Client Key: {}", e))?;
                        mac.update(b"Client Key");
                        let client_key = mac.finalize().into_bytes();
                        
                        use sha2::Digest;
                        let mut hasher = sha2::Sha256::new();
                        hasher.update(&client_key);
                        let stored_key = hasher.finalize();
                        
                        let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(&stored_key)
                            .map_err(|e| format!("Failed to create HMAC-SHA256 for Client Signature: {}", e))?;
                        mac.update(auth_message.as_bytes());
                        let client_signature = mac.finalize().into_bytes();
                        
                        let mut client_proof = [0u8; 32];
                        for i in 0..32 {
                            client_proof[i] = client_key[i] ^ client_signature[i];
                        }
                        
                        let client_final_message = format!("{},p={}", client_final_message_without_proof, base64::engine::general_purpose::STANDARD.encode(client_proof));
                        
                        // 5. Send SASLResponse
                        write_sasl_response(&mut pg_stream, client_final_message.as_bytes())
                            .map_err(|e| format!("Failed to write SASLResponse: {}", e))?;
                            
                        // 6. Read SASLFinal message (R / 12)
                        let final_msg = read_message(&mut pg_stream)
                            .map_err(|e| format!("Failed to read SASLFinal message: {}", e))?;
                            
                        if final_msg.msg_type == b'E' {
                            let err_msg = parse_error_response(&final_msg.payload);
                            return Err(format!("Postgres authentication failed at final stage: {}", err_msg));
                        }
                        if final_msg.msg_type != b'R' {
                            return Err(format!("Expected AuthenticationRequest ('R') during SASL final, got '{}'", final_msg.msg_type as char));
                        }
                        
                        let final_auth_type = u32::from_be_bytes([final_msg.payload[0], final_msg.payload[1], final_msg.payload[2], final_msg.payload[3]]);
                        if final_auth_type != 12 {
                            return Err(format!("Expected SASLFinal (12), got {}", final_auth_type));
                        }
                    }
                    _ => {
                        return Err(format!("Unsupported PostgreSQL authentication type: {}", auth_type));
                    }
                }
            }
            b'E' => {
                let err_msg = parse_error_response(&msg.payload);
                return Err(format!("Postgres error: {}", err_msg));
            }
            b'Z' => {
                // Fully ready!
                break;
            }
            _ => {}
        }
    }
    
    Ok(pg_stream)
}

fn execute_query_on_stream(
    pg_stream: &mut PgStream,
    sql: &str,
    params: Vec<serde_json::Value>,
) -> Result<Vec<serde_json::Value>, String> {
    let interpolated = interpolate_query(sql, &params)?;
    write_query_message(pg_stream, &interpolated)
        .map_err(|e| format!("Failed to send query: {}", e))?;
        
    let mut columns = Vec::new();
    let mut rows = Vec::new();
    
    loop {
        let msg = read_message(pg_stream)
            .map_err(|e| format!("Failed to read message from Postgres: {}", e))?;
            
        match msg.msg_type {
            b'E' => {
                let err_msg = parse_error_response(&msg.payload);
                return Err(format!("Postgres error: {}", err_msg));
            }
            b'T' => {
                columns = parse_row_description(&msg.payload)
                    .map_err(|e| format!("Failed to parse row description: {}", e))?;
            }
            b'D' => {
                let row = parse_data_row(&msg.payload, &columns)
                    .map_err(|e| format!("Failed to parse data row: {}", e))?;
                rows.push(row);
            }
            b'Z' => {
                break;
            }
            _ => {}
        }
    }
    
    Ok(rows)
}

fn execute_raw_tcp_postgres(
    url: &str,
    sql: &str,
    params: Vec<serde_json::Value>,
) -> Result<Vec<serde_json::Value>, String> {
    let pg_params = parse_pg_url(url)?;
    let addr = format!("{}:{}", pg_params.host, pg_params.port);

    let mut guard = PG_CONN.lock().map_err(|_| "Failed to lock PG_CONN mutex".to_string())?;

    // Check if we have a cached connection for this exact URL
    let pg_stream = if let Some((ref cached_url, _)) = *guard {
        if cached_url == url {
            // Take the connection from the guard so we can use it
            let (_, stream) = guard.take().unwrap();
            println!("[POSTGRES] Found cached connection for {}. Testing health...", pg_params.host);
            Some(stream)
        } else {
            // URL mismatch, let's close/drop the old one
            println!("[POSTGRES] Cached URL mismatch. Dropping cached connection.");
            guard.take();
            None
        }
    } else {
        None
    };

    if let Some(mut stream) = pg_stream {
        // Try executing the query on the cached stream
        match execute_query_on_stream(&mut stream, sql, params.clone()) {
            Ok(rows) => {
                // Connection is healthy! Put it back.
                println!("[POSTGRES] Connection is HEALTHY. Reusing connection.");
                *guard = Some((url.to_string(), stream));
                return Ok(rows);
            }
            Err(e) => {
                // Cached connection was stale/dead. Discard it (it is already taken out of guard).
                // We will fall back to establishing a fresh connection below.
                println!("[POSTGRES] Cached connection was STALE or FAILED: {}. Discarding and establishing a fresh connection...", e);
            }
        }
    }

    // Connect and authenticate a fresh stream
    println!("[POSTGRES] Connecting to fresh Postgres instance at {}...", addr);
    let mut fresh_stream = connect_and_auth_postgres(url, &pg_params, &addr)?;
    println!("[POSTGRES] Fresh connection connected and authenticated successfully.");
    
    // Execute query on the fresh stream
    let res = execute_query_on_stream(&mut fresh_stream, sql, params);

    if res.is_ok() {
        // Cache the fresh stream for future queries
        println!("[POSTGRES] Query on fresh connection succeeded. Caching connection.");
        *guard = Some((url.to_string(), fresh_stream));
    } else {
        println!("[POSTGRES] Query on fresh connection FAILED: {:?}", res.as_ref().err());
    }

    res
}

// -------------------------------------------------------------------------
// Unified Postgres routing HTTP helper (Neon & Supabase Router)
// -------------------------------------------------------------------------
#[allow(dead_code)]
async fn execute_postgres_query(
    url: &str,
    api_key: Option<&str>,
    sql: &str,
    params: Vec<serde_json::Value>,
) -> Result<Vec<serde_json::Value>, String> {
    let backend = get_backend();
    
    #[cfg(runtime_wasmtime)]
    {
        if backend == "postgres" && (url.starts_with("postgres://") || url.starts_with("postgresql://")) {
            let res = execute_raw_tcp_postgres(url, sql, params);
            if let Err(ref e) = res {
                eprintln!("[SERVER ERROR] execute_raw_tcp_postgres failed: {}", e);
            }
            return res;
        }
    }

    if backend == "supabase" {
        let secret_key = get_supabase_secret_key();
        execute_supabase_query(url, secret_key.as_deref(), sql, params).await
    } else {
        execute_neon_query(url, api_key, sql, params).await
    }
}

// -------------------------------------------------------------------------
// Turso / LibSQL HTTP pipeline helper
// -------------------------------------------------------------------------
struct LibSqlResult {
    rows: Vec<serde_json::Value>,
    last_insert_rowid: Option<u64>,
}

fn to_hrana_arg(val: serde_json::Value) -> serde_json::Value {
    match val {
        serde_json::Value::Null => serde_json::json!({ "type": "null" }),
        serde_json::Value::Bool(b) => serde_json::json!({ "type": "integer", "value": if b { "1" } else { "0" } }),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                serde_json::json!({ "type": "integer", "value": i.to_string() })
            } else if let Some(f) = n.as_f64() {
                serde_json::json!({ "type": "float", "value": f })
            } else {
                serde_json::json!({ "type": "null" })
            }
        }
        serde_json::Value::String(s) => serde_json::json!({ "type": "text", "value": s }),
        _ => serde_json::json!({ "type": "text", "value": val.to_string() }),
    }
}

fn from_hrana_val(val: &serde_json::Value) -> serde_json::Value {
    if let Some(t) = val.get("type").and_then(|v| v.as_str()) {
        match t {
            "null" => serde_json::Value::Null,
            "text" => val.get("value").cloned().unwrap_or(serde_json::Value::Null),
            "integer" => {
                if let Some(s) = val.get("value").and_then(|v| v.as_str()) {
                    if let Ok(i) = s.parse::<i64>() {
                        serde_json::Value::Number(serde_json::Number::from(i))
                    } else {
                        serde_json::Value::Null
                    }
                } else {
                    serde_json::Value::Null
                }
            }
            "float" => val.get("value").cloned().unwrap_or(serde_json::Value::Null),
            "blob" => val.get("base64").cloned().unwrap_or(serde_json::Value::Null),
            _ => serde_json::Value::Null,
        }
    } else {
        serde_json::Value::Null
    }
}

fn parse_libsql_result(resp: &serde_json::Value) -> Result<LibSqlResult, String> {
    let results = resp.get("results")
        .and_then(|r| r.as_array())
        .ok_or_else(|| "Missing results array in LibSQL response".to_string())?;

    for res in results {
        if let Some(t) = res.get("type").and_then(|v| v.as_str()) {
            if t == "error" {
                let msg = res.get("error").and_then(|e| e.get("message")).and_then(|m| m.as_str()).unwrap_or("Unknown error");
                return Err(format!("LibSQL query error: {}", msg));
            }
        }

        if let Some(response) = res.get("response") {
            if let Some(result) = response.get("result") {
                let cols = result.get("cols")
                    .and_then(|c| c.as_array())
                    .ok_or_else(|| "Missing cols in LibSQL execute result".to_string())?;
                
                let rows_array = result.get("rows")
                    .and_then(|r| r.as_array())
                    .ok_or_else(|| "Missing rows in LibSQL execute result".to_string())?;

                let last_insert_rowid = result.get("last_insert_rowid")
                    .and_then(|v| {
                        if let Some(s) = v.as_str() {
                            s.parse::<u64>().ok()
                        } else {
                            v.as_u64()
                        }
                    });

                let col_names: Vec<String> = cols.iter()
                    .map(|c| c.get("name").and_then(|n| n.as_str()).unwrap_or("").to_string())
                    .collect();

                let mut rows = Vec::new();
                for r in rows_array {
                    let r_arr = r.as_array()
                        .ok_or_else(|| "Row is not an array".to_string())?;
                    
                    let mut obj = serde_json::Map::new();
                    for (i, val) in r_arr.iter().enumerate() {
                        if let Some(col_name) = col_names.get(i) {
                            obj.insert(col_name.clone(), from_hrana_val(val));
                        }
                    }
                    rows.push(serde_json::Value::Object(obj));
                }
                return Ok(LibSqlResult { rows, last_insert_rowid });
            }
        }
    }
    
    Ok(LibSqlResult { rows: Vec::new(), last_insert_rowid: None })
}

async fn execute_libsql_query(
    url: &str,
    auth_token: Option<&str>,
    sql: &str,
    params: Vec<serde_json::Value>,
) -> Result<LibSqlResult, String> {
    let hrana_args: Vec<serde_json::Value> = params.into_iter().map(to_hrana_arg).collect();
    
    let req_payload = serde_json::json!({
        "baton": null,
        "requests": [
            {
                "type": "execute",
                "stmt": {
                    "sql": sql,
                    "args": hrana_args
                }
            },
            {
                "type": "close"
            }
        ]
    });
    
    let body_data = serde_json::to_vec(&req_payload)
        .map_err(|e| e.to_string())?;

    let mut headers = vec![
        ("Content-Type".to_string(), "application/json".to_string()),
    ];
    if let Some(tok) = auth_token {
        headers.push(("Authorization".to_string(), format!("Bearer {}", tok)));
    }
    
    // Resolve libsql:// to https://
    let resolved_url = if url.starts_with("libsql://") {
        format!("https://{}", &url["libsql://".len()..])
    } else {
        url.to_string()
    };
    
    let pipeline_url = if resolved_url.ends_with("/v2/pipeline") {
        resolved_url
    } else if resolved_url.ends_with('/') {
        format!("{}v2/pipeline", resolved_url)
    } else {
        format!("{}/v2/pipeline", resolved_url)
    };

    let resp_bytes = wasi_http_post(&pipeline_url, headers, body_data).await?;
    
    let resp_json: serde_json::Value = serde_json::from_slice(&resp_bytes)
        .map_err(|e| format!("Failed to parse LibSQL response: {}. Body was: {}", e, String::from_utf8_lossy(&resp_bytes)))?;
    
    parse_libsql_result(&resp_json)
}

// -------------------------------------------------------------------------
// Spin pg native helper
// -------------------------------------------------------------------------
#[cfg(runtime_spin)]
fn execute_spin_pg(sql: &str, params: Vec<serde_json::Value>) -> Result<Vec<serde_json::Value>, String> {
    use spin_sdk::pg::{Connection as SpinPgConn, ParameterValue as SpinPgParam, DbValue as SpinPgDbVal};
    
    let db_url = get_postgres_url();
    let conn = spin_block_on(SpinPgConn::open(&db_url)).map_err(|e| format!("Pg connection error: {:?}", e))?;
    
    // Check if query is SELECT or modifying command that returns rows (e.g. contains RETURNING)
    let sql_upper = sql.trim_start().to_ascii_uppercase();
    let is_select = sql_upper.starts_with("SELECT") || sql_upper.contains("RETURNING");
    
    let pg_params: Vec<SpinPgParam> = params.into_iter().map(|v| {
        match v {
            serde_json::Value::Null => SpinPgParam::DbNull,
            serde_json::Value::Bool(b) => SpinPgParam::Boolean(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    SpinPgParam::Int64(i)
                } else if let Some(f) = n.as_f64() {
                    SpinPgParam::Floating64(f)
                } else {
                    SpinPgParam::DbNull
                }
            }
            serde_json::Value::String(s) => SpinPgParam::Str(s),
            other => SpinPgParam::Str(other.to_string()),
        }
    }).collect();
    
    if is_select {
        let mut rowset = spin_block_on(conn.query(sql, pg_params)).map_err(|e| format!("Pg query error: {:?}", e))?;
        
        let mut rows = Vec::new();
        let col_names: Vec<String> = rowset.columns().iter().map(|c| c.name.clone()).collect();
        
        let rows_reader = rowset.rows();
        while let Some(row) = spin_block_on(rows_reader.next()) {
            let mut obj = serde_json::Map::new();
            for (i, val) in row.iter().enumerate() {
                let col_name = col_names.get(i).cloned().unwrap_or_else(|| format!("col_{}", i));
                
                let json_val = match val {
                    SpinPgDbVal::Boolean(b) => serde_json::Value::Bool(*b),
                    SpinPgDbVal::Int8(i) => serde_json::Value::Number((*i as i32).into()),
                    SpinPgDbVal::Int16(i) => serde_json::Value::Number((*i as i32).into()),
                    SpinPgDbVal::Int32(i) => serde_json::Value::Number((*i).into()),
                    SpinPgDbVal::Int64(i) => serde_json::Value::Number((*i).into()),
                    SpinPgDbVal::Floating32(f) => serde_json::Value::Number(serde_json::Number::from_f64(*f as f64).unwrap()),
                    SpinPgDbVal::Floating64(f) => serde_json::Value::Number(serde_json::Number::from_f64(*f).unwrap()),
                    SpinPgDbVal::Str(s) => serde_json::Value::String(s.clone()),
                    SpinPgDbVal::Binary(b) => serde_json::Value::String(String::from_utf8_lossy(b).to_string()),
                    SpinPgDbVal::DbNull => serde_json::Value::Null,
                    _ => serde_json::Value::Null,
                };
                obj.insert(col_name, json_val);
            }
            rows.push(serde_json::Value::Object(obj));
        }
        Ok(rows)
    } else {
        spin_block_on(conn.execute(sql, pg_params)).map_err(|e| format!("Pg execute error: {:?}", e))?;
        Ok(Vec::new())
    }
}

#[cfg(runtime_spin)]
async fn execute_spin_pg_async(sql: &str, params: Vec<serde_json::Value>) -> Result<Vec<serde_json::Value>, String> {
    use spin_sdk::pg::{Connection as SpinPgConn, ParameterValue as SpinPgParam, DbValue as SpinPgDbVal};
    
    let db_url = get_postgres_url();
    let conn = SpinPgConn::open(&db_url).await.map_err(|e| format!("Pg connection error: {:?}", e))?;
    
    // Check if query is SELECT or modifying command that returns rows (e.g. contains RETURNING)
    let sql_upper = sql.trim_start().to_ascii_uppercase();
    let is_select = sql_upper.starts_with("SELECT") || sql_upper.contains("RETURNING");
    
    let pg_params: Vec<SpinPgParam> = params.into_iter().map(|v| {
        match v {
            serde_json::Value::Null => SpinPgParam::DbNull,
            serde_json::Value::Bool(b) => SpinPgParam::Boolean(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    SpinPgParam::Int64(i)
                } else if let Some(f) = n.as_f64() {
                    SpinPgParam::Floating64(f)
                } else {
                    SpinPgParam::DbNull
                }
            }
            serde_json::Value::String(s) => SpinPgParam::Str(s),
            other => SpinPgParam::Str(other.to_string()),
        }
    }).collect();
    
    if is_select {
        let mut rowset = conn.query(sql, pg_params).await.map_err(|e| format!("Pg query error: {:?}", e))?;
        
        let mut rows = Vec::new();
        let col_names: Vec<String> = rowset.columns().iter().map(|c| c.name.clone()).collect();
        
        let rows_reader = rowset.rows();
        while let Some(row) = rows_reader.next().await {
            let mut obj = serde_json::Map::new();
            for (i, val) in row.iter().enumerate() {
                let col_name = col_names.get(i).cloned().unwrap_or_else(|| format!("col_{}", i));
                
                let json_val = match val {
                    SpinPgDbVal::Boolean(b) => serde_json::Value::Bool(*b),
                    SpinPgDbVal::Int8(i) => serde_json::Value::Number((*i as i32).into()),
                    SpinPgDbVal::Int16(i) => serde_json::Value::Number((*i as i32).into()),
                    SpinPgDbVal::Int32(i) => serde_json::Value::Number((*i).into()),
                    SpinPgDbVal::Int64(i) => serde_json::Value::Number((*i).into()),
                    SpinPgDbVal::Floating32(f) => serde_json::Value::Number(serde_json::Number::from_f64(*f as f64).unwrap()),
                    SpinPgDbVal::Floating64(f) => serde_json::Value::Number(serde_json::Number::from_f64(*f).unwrap()),
                    SpinPgDbVal::Str(s) => serde_json::Value::String(s.clone()),
                    SpinPgDbVal::Binary(b) => serde_json::Value::String(String::from_utf8_lossy(b).to_string()),
                    SpinPgDbVal::DbNull => serde_json::Value::Null,
                    _ => serde_json::Value::Null,
                };
                obj.insert(col_name, json_val);
            }
            rows.push(serde_json::Value::Object(obj));
        }
        Ok(rows)
    } else {
        conn.execute(sql, pg_params).await.map_err(|e| format!("Pg execute error: {:?}", e))?;
        Ok(Vec::new())
    }
}


// -------------------------------------------------------------------------
// JSON deserialization helper for database rows
// -------------------------------------------------------------------------
fn row_to_envelope<A>(row: &serde_json::Value) -> Result<EventEnvelope<A::Event, A::Id>, EventStoreError>
where
    A: Aggregate,
    A::Event: serde::de::DeserializeOwned,
    A::Id: serde::de::DeserializeOwned,
{
    let sequence = row.get("sequence")
        .and_then(|v| {
            if let Some(s) = v.as_str() {
                s.parse::<u64>().ok()
            } else {
                v.as_u64()
            }
        })
        .ok_or_else(|| EventStoreError::Deserialization("Missing sequence".to_string()))?;
        
    let event_id_str = row.get("event_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| EventStoreError::Deserialization("Missing event_id".to_string()))?.to_string();
        
    let aggregate_id_raw = row.get("aggregate_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| EventStoreError::Deserialization("Missing aggregate_id".to_string()))?.to_string();
        
    let aggregate_type = row.get("aggregate_type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| EventStoreError::Deserialization("Missing aggregate_type".to_string()))?.to_string();
        
    let revision = row.get("revision")
        .and_then(|v| {
            if let Some(s) = v.as_str() {
                s.parse::<u64>().ok()
            } else {
                v.as_u64()
            }
        })
        .ok_or_else(|| EventStoreError::Deserialization("Missing revision".to_string()))?;
        
    let event_type = row.get("event_type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| EventStoreError::Deserialization("Missing event_type".to_string()))?.to_string();
        
    let event_version = row.get("event_version")
        .and_then(|v| {
            if let Some(s) = v.as_str() {
                s.parse::<u64>().ok()
            } else {
                v.as_u64()
            }
        })
        .ok_or_else(|| EventStoreError::Deserialization("Missing event_version".to_string()))? as u32;
        
    let payload_str = row.get("payload")
        .and_then(|v| v.as_str())
        .ok_or_else(|| EventStoreError::Deserialization("Missing payload".to_string()))?.to_string();
        
    let metadata_str = row.get("metadata")
        .and_then(|v| v.as_str())
        .ok_or_else(|| EventStoreError::Deserialization("Missing metadata".to_string()))?.to_string();
        
    let recorded_at_ms = row.get("recorded_at_ms")
        .and_then(|v| {
            if let Some(s) = v.as_str() {
                s.parse::<i64>().ok()
            } else {
                v.as_i64()
            }
        })
        .ok_or_else(|| EventStoreError::Deserialization("Missing recorded_at_ms".to_string()))?;

    let aggregate_id_val: A::Id = serde_json::from_str(&aggregate_id_raw)
        .map_err(|e| EventStoreError::Deserialization(e.to_string()))?;

    let payload: A::Event = serde_json::from_str(&payload_str)
        .map_err(|e| EventStoreError::Deserialization(e.to_string()))?;

    let metadata: ddd_cqrs_es::Metadata = serde_json::from_str(&metadata_str)
        .map_err(|e| EventStoreError::Deserialization(e.to_string()))?;

    let recorded_at = std::time::UNIX_EPOCH + std::time::Duration::from_millis(recorded_at_ms as u64);

    Ok(EventEnvelope::new(
        EventId::from_string(event_id_str),
        aggregate_id_val,
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


// =========================================================================
// 4. UNIFIED DYNAMIC BACKEND ADAPTERS (NEW)
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

impl<A> MultiBackendEventStore<A> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<A> MultiBackendEventStore<A>
where
    A: Aggregate,
{
    pub fn initialize_schema(&self) -> Result<(), String> {
        if SCHEMA_INITIALIZED.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(());
        }
        
        let backend = get_backend();
        
        #[cfg(runtime_wasmtime)]
        {
            let marker_path = format!("/data/.schema_initialized_{}", backend);
            if std::path::Path::new(&marker_path).exists() {
                SCHEMA_INITIALIZED.store(true, std::sync::atomic::Ordering::Relaxed);
                return Ok(());
            }
        }
        
        // Pre-existence check for Postgres to avoid catalog writing/locking collisions under parallel requests
        let mut tables_exist = false;
        if backend == "postgres" {
            let check_query = "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'events');";
            #[cfg(runtime_spin)]
            {
                if let Ok(rows) = execute_spin_pg(check_query, Vec::new()) {
                    if let Some(first) = rows.first() {
                        if let Some(exists) = first.get("exists").and_then(|v| v.as_bool()) {
                            if exists {
                                tables_exist = true;
                            }
                        }
                    }
                }
            }
            #[cfg(runtime_wasmtime)]
            {
                let url = get_postgres_url();
                let api_key = get_neon_api_key();
                if let Ok(rows) = futures::executor::block_on(execute_postgres_query(&url, api_key.as_deref(), check_query, Vec::new())) {
                    if let Some(first) = rows.first() {
                        if let Some(exists) = first.get("exists").and_then(|v| v.as_bool()) {
                            if exists {
                                tables_exist = true;
                            }
                        }
                    }
                }
            }
        }
        
        if tables_exist {
            SCHEMA_INITIALIZED.store(true, std::sync::atomic::Ordering::Relaxed);
            #[cfg(runtime_wasmtime)]
            {
                let marker_path = format!("/data/.schema_initialized_{}", backend);
                let _ = std::fs::write(&marker_path, b"1");
            }
            return Ok(());
        }
        
        if backend == "sqlite" {
            let store = SpinSqliteEventStore::<A>::new("default");
            let res = store.initialize_schema();
            if res.is_ok() {
                SCHEMA_INITIALIZED.store(true, std::sync::atomic::Ordering::Relaxed);
                #[cfg(runtime_wasmtime)]
                {
                    let marker_path = format!("/data/.schema_initialized_{}", backend);
                    let _ = std::fs::write(&marker_path, b"1");
                }
            }
            return res;
        }
        
        let create_events_sqlite = r#"
            CREATE TABLE IF NOT EXISTS events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                aggregate_id TEXT NOT NULL,
                aggregate_type TEXT NOT NULL,
                revision INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                event_version INTEGER NOT NULL,
                payload TEXT NOT NULL,
                metadata TEXT NOT NULL,
                recorded_at_ms INTEGER NOT NULL,
                UNIQUE (aggregate_type, aggregate_id, revision)
            );
        "#;
        
        let create_checkpoints_sqlite = r#"
            CREATE TABLE IF NOT EXISTS checkpoints (
                projection_name TEXT PRIMARY KEY,
                last_sequence INTEGER NOT NULL
            );
        "#;
        
        let create_read_model_sqlite = r#"
            CREATE TABLE IF NOT EXISTS counter_read_model (
                id TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            );
        "#;
        
        let create_events_postgres = r#"
            CREATE TABLE IF NOT EXISTS events (
                sequence BIGSERIAL PRIMARY KEY,
                event_id TEXT NOT NULL UNIQUE,
                aggregate_id TEXT NOT NULL,
                aggregate_type TEXT NOT NULL,
                revision BIGINT NOT NULL,
                event_type TEXT NOT NULL,
                event_version BIGINT NOT NULL,
                payload TEXT NOT NULL,
                metadata TEXT NOT NULL,
                recorded_at_ms BIGINT NOT NULL,
                UNIQUE (aggregate_type, aggregate_id, revision)
            );
        "#;
        
        let create_checkpoints_postgres = r#"
            CREATE TABLE IF NOT EXISTS checkpoints (
                projection_name TEXT PRIMARY KEY,
                last_sequence BIGINT NOT NULL
            );
        "#;
        
        let create_read_model_postgres = r#"
            CREATE TABLE IF NOT EXISTS counter_read_model (
                id TEXT PRIMARY KEY,
                value BIGINT NOT NULL
            );
        "#;
        
        if backend == "libsql" || backend == "turso" {
            let url = get_turso_url();
            let auth = get_turso_auth_token();
            
            let req_payload = serde_json::json!({
                "baton": null,
                "requests": [
                    { "type": "execute", "stmt": { "sql": create_events_sqlite } },
                    { "type": "execute", "stmt": { "sql": create_checkpoints_sqlite } },
                    { "type": "execute", "stmt": { "sql": create_read_model_sqlite } },
                    { "type": "close" }
                ]
            });
            
            let body_data = serde_json::to_vec(&req_payload)
                .map_err(|e| e.to_string())?;

            let mut headers = vec![
                ("Content-Type".to_string(), "application/json".to_string()),
            ];
            if let Some(tok) = auth {
                headers.push(("Authorization".to_string(), format!("Bearer {}", tok)));
            }
            
            let resolved_url = if url.starts_with("libsql://") {
                format!("https://{}", &url["libsql://".len()..])
            } else {
                url.to_string()
            };
            
            let pipeline_url = if resolved_url.ends_with("/v2/pipeline") {
                resolved_url
            } else if resolved_url.ends_with('/') {
                format!("{}v2/pipeline", resolved_url)
            } else {
                format!("{}/v2/pipeline", resolved_url)
            };

            let resp_bytes = futures::executor::block_on(wasi_http_post(&pipeline_url, headers, body_data))?;
            
            let resp_json: serde_json::Value = serde_json::from_slice(&resp_bytes)
                .map_err(|e| format!("Failed to parse LibSQL response: {}", e))?;
            
            let _ = parse_libsql_result(&resp_json)?;
        } else {
            #[cfg(runtime_spin)]
            {
                if let Err(e) = execute_spin_pg(create_events_postgres, Vec::new()) {
                    let e_lower = e.to_lowercase();
                    if !e_lower.contains("pg_type_typname_nsp_index") && !e_lower.contains("duplicate key") && !e_lower.contains("23505") && !e_lower.contains("already exists") {
                        return Err(e);
                    }
                }
                if let Err(e) = execute_spin_pg(create_checkpoints_postgres, Vec::new()) {
                    let e_lower = e.to_lowercase();
                    if !e_lower.contains("pg_type_typname_nsp_index") && !e_lower.contains("duplicate key") && !e_lower.contains("23505") && !e_lower.contains("already exists") {
                        return Err(e);
                    }
                }
                if let Err(e) = execute_spin_pg(create_read_model_postgres, Vec::new()) {
                    let e_lower = e.to_lowercase();
                    if !e_lower.contains("pg_type_typname_nsp_index") && !e_lower.contains("duplicate key") && !e_lower.contains("23505") && !e_lower.contains("already exists") {
                        return Err(e);
                    }
                }
            }
            #[cfg(runtime_wasmtime)]
            {
                let url = get_postgres_url();
                let api_key = get_neon_api_key();
                if let Err(e) = futures::executor::block_on(execute_postgres_query(&url, api_key.as_deref(), create_events_postgres, Vec::new())) {
                    let e_lower = e.to_lowercase();
                    if !e_lower.contains("pg_type_typname_nsp_index") && !e_lower.contains("duplicate key") && !e_lower.contains("23505") && !e_lower.contains("already exists") {
                        return Err(e);
                    }
                }
                if let Err(e) = futures::executor::block_on(execute_postgres_query(&url, api_key.as_deref(), create_checkpoints_postgres, Vec::new())) {
                    let e_lower = e.to_lowercase();
                    if !e_lower.contains("pg_type_typname_nsp_index") && !e_lower.contains("duplicate key") && !e_lower.contains("23505") && !e_lower.contains("already exists") {
                        return Err(e);
                    }
                }
                if let Err(e) = futures::executor::block_on(execute_postgres_query(&url, api_key.as_deref(), create_read_model_postgres, Vec::new())) {
                    let e_lower = e.to_lowercase();
                    if !e_lower.contains("pg_type_typname_nsp_index") && !e_lower.contains("duplicate key") && !e_lower.contains("23505") && !e_lower.contains("already exists") {
                        return Err(e);
                    }
                }
            }
        }
        
        SCHEMA_INITIALIZED.store(true, std::sync::atomic::Ordering::Relaxed);
        #[cfg(runtime_wasmtime)]
        {
            let marker_path = format!("/data/.schema_initialized_{}", backend);
            let _ = std::fs::write(&marker_path, b"1");
        }
        Ok(())
    }
}

impl<A> MultiBackendEventStore<A>
where
    A: Aggregate + 'static,
    A::Event: serde::Serialize + serde::de::DeserializeOwned + Clone,
    A::Id: serde::Serialize + serde::de::DeserializeOwned + Clone + PartialEq + std::fmt::Display,
{
    pub async fn initialize_schema_async(&self) -> Result<(), String> {
        if SCHEMA_INITIALIZED.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(());
        }
        
        let backend = get_backend();
        
        #[cfg(runtime_wasmtime)]
        {
            let marker_path = format!("/data/.schema_initialized_{}", backend);
            if std::path::Path::new(&marker_path).exists() {
                SCHEMA_INITIALIZED.store(true, std::sync::atomic::Ordering::Relaxed);
                return Ok(());
            }
        }
        
        // Pre-existence check for Postgres to avoid catalog writing/locking collisions under parallel requests
        let mut tables_exist = false;
        if backend == "postgres" {
            let check_query = "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'events');";
            #[cfg(runtime_spin)]
            {
                if let Ok(rows) = execute_spin_pg_async(check_query, Vec::new()).await {
                    if let Some(first) = rows.first() {
                        if let Some(exists) = first.get("exists").and_then(|v| v.as_bool()) {
                            if exists {
                                tables_exist = true;
                            }
                        }
                    }
                }
            }
            #[cfg(runtime_wasmtime)]
            {
                let url = get_postgres_url();
                let api_key = get_neon_api_key();
                if let Ok(rows) = execute_postgres_query(&url, api_key.as_deref(), check_query, Vec::new()).await {
                    if let Some(first) = rows.first() {
                        if let Some(exists) = first.get("exists").and_then(|v| v.as_bool()) {
                            if exists {
                                tables_exist = true;
                            }
                        }
                    }
                }
            }
        }
        
        if tables_exist {
            SCHEMA_INITIALIZED.store(true, std::sync::atomic::Ordering::Relaxed);
            #[cfg(runtime_wasmtime)]
            {
                let marker_path = format!("/data/.schema_initialized_{}", backend);
                let _ = std::fs::write(&marker_path, b"1");
            }
            return Ok(());
        }
        
        if backend == "sqlite" {
            #[cfg(runtime_spin)]
            {
                let store = SpinSqliteEventStore::<A>::new("default");
                let res = store.initialize_schema_async().await;
                if res.is_ok() {
                    SCHEMA_INITIALIZED.store(true, std::sync::atomic::Ordering::Relaxed);
                }
                return res;
            }
            #[cfg(runtime_wasmtime)]
            {
                let res = self.initialize_schema();
                if res.is_ok() {
                    SCHEMA_INITIALIZED.store(true, std::sync::atomic::Ordering::Relaxed);
                    let marker_path = format!("/data/.schema_initialized_{}", backend);
                    let _ = std::fs::write(&marker_path, b"1");
                }
                return res;
            }
        }
        
        let create_events_sqlite = r#"
            CREATE TABLE IF NOT EXISTS events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                aggregate_id TEXT NOT NULL,
                aggregate_type TEXT NOT NULL,
                revision INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                event_version INTEGER NOT NULL,
                payload TEXT NOT NULL,
                metadata TEXT NOT NULL,
                recorded_at_ms INTEGER NOT NULL,
                UNIQUE (aggregate_type, aggregate_id, revision)
            );
        "#;
        
        let create_checkpoints_sqlite = r#"
            CREATE TABLE IF NOT EXISTS checkpoints (
                projection_name TEXT PRIMARY KEY,
                last_sequence INTEGER NOT NULL
            );
        "#;
        
        let create_read_model_sqlite = r#"
            CREATE TABLE IF NOT EXISTS counter_read_model (
                id TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            );
        "#;
        
        let create_events_postgres = r#"
            CREATE TABLE IF NOT EXISTS events (
                sequence BIGSERIAL PRIMARY KEY,
                event_id TEXT NOT NULL UNIQUE,
                aggregate_id TEXT NOT NULL,
                aggregate_type TEXT NOT NULL,
                revision BIGINT NOT NULL,
                event_type TEXT NOT NULL,
                event_version BIGINT NOT NULL,
                payload TEXT NOT NULL,
                metadata TEXT NOT NULL,
                recorded_at_ms BIGINT NOT NULL,
                UNIQUE (aggregate_type, aggregate_id, revision)
            );
        "#;
        
        let create_checkpoints_postgres = r#"
            CREATE TABLE IF NOT EXISTS checkpoints (
                projection_name TEXT PRIMARY KEY,
                last_sequence BIGINT NOT NULL
            );
        "#;
        
        let create_read_model_postgres = r#"
            CREATE TABLE IF NOT EXISTS counter_read_model (
                id TEXT PRIMARY KEY,
                value BIGINT NOT NULL
            );
        "#;
        
        if backend == "libsql" || backend == "turso" {
            let url = get_turso_url();
            let auth = get_turso_auth_token();
            
            let req_payload = serde_json::json!({
                "baton": null,
                "requests": [
                    { "type": "execute", "stmt": { "sql": create_events_sqlite } },
                    { "type": "execute", "stmt": { "sql": create_checkpoints_sqlite } },
                    { "type": "execute", "stmt": { "sql": create_read_model_sqlite } },
                    { "type": "close" }
                ]
            });
            
            let body_data = serde_json::to_vec(&req_payload)
                .map_err(|e| e.to_string())?;

            let mut headers = vec![
                ("Content-Type".to_string(), "application/json".to_string()),
            ];
            if let Some(tok) = auth {
                headers.push(("Authorization".to_string(), format!("Bearer {}", tok)));
            }
            
            let resolved_url = if url.starts_with("libsql://") {
                format!("https://{}", &url["libsql://".len()..])
            } else {
                url.to_string()
            };
            
            let pipeline_url = if resolved_url.ends_with("/v2/pipeline") {
                resolved_url
            } else if resolved_url.ends_with('/') {
                format!("{}v2/pipeline", resolved_url)
            } else {
                format!("{}/v2/pipeline", resolved_url)
            };

            let resp_bytes = wasi_http_post(&pipeline_url, headers, body_data).await?;
            
            let resp_json: serde_json::Value = serde_json::from_slice(&resp_bytes)
                .map_err(|e| format!("Failed to parse LibSQL response: {}", e))?;
            
            let _ = parse_libsql_result(&resp_json)?;
        } else {
            #[cfg(runtime_spin)]
            {
                if let Err(e) = execute_spin_pg_async(create_events_postgres, Vec::new()).await {
                    let e_lower = e.to_lowercase();
                    if !e_lower.contains("pg_type_typname_nsp_index") && !e_lower.contains("duplicate key") && !e_lower.contains("23505") && !e_lower.contains("already exists") {
                        return Err(e);
                    }
                }
                if let Err(e) = execute_spin_pg_async(create_checkpoints_postgres, Vec::new()).await {
                    let e_lower = e.to_lowercase();
                    if !e_lower.contains("pg_type_typname_nsp_index") && !e_lower.contains("duplicate key") && !e_lower.contains("23505") && !e_lower.contains("already exists") {
                        return Err(e);
                    }
                }
                if let Err(e) = execute_spin_pg_async(create_read_model_postgres, Vec::new()).await {
                    let e_lower = e.to_lowercase();
                    if !e_lower.contains("pg_type_typname_nsp_index") && !e_lower.contains("duplicate key") && !e_lower.contains("23505") && !e_lower.contains("already exists") {
                        return Err(e);
                    }
                }
            }
            #[cfg(runtime_wasmtime)]
            {
                let url = get_postgres_url();
                let api_key = get_neon_api_key();
                if let Err(e) = execute_postgres_query(&url, api_key.as_deref(), create_events_postgres, Vec::new()).await {
                    let e_lower = e.to_lowercase();
                    if !e_lower.contains("pg_type_typname_nsp_index") && !e_lower.contains("duplicate key") && !e_lower.contains("23505") && !e_lower.contains("already exists") {
                        return Err(e);
                    }
                }
                if let Err(e) = execute_postgres_query(&url, api_key.as_deref(), create_checkpoints_postgres, Vec::new()).await {
                    let e_lower = e.to_lowercase();
                    if !e_lower.contains("pg_type_typname_nsp_index") && !e_lower.contains("duplicate key") && !e_lower.contains("23505") && !e_lower.contains("already exists") {
                        return Err(e);
                    }
                }
                if let Err(e) = execute_postgres_query(&url, api_key.as_deref(), create_read_model_postgres, Vec::new()).await {
                    let e_lower = e.to_lowercase();
                    if !e_lower.contains("pg_type_typname_nsp_index") && !e_lower.contains("duplicate key") && !e_lower.contains("23505") && !e_lower.contains("already exists") {
                        return Err(e);
                    }
                }
            }
        }
        
        SCHEMA_INITIALIZED.store(true, std::sync::atomic::Ordering::Relaxed);
        #[cfg(runtime_wasmtime)]
        {
            let marker_path = format!("/data/.schema_initialized_{}", backend);
            let _ = std::fs::write(&marker_path, b"1");
        }
        Ok(())
    }

    pub async fn load_async(&self, aggregate_id: &A::Id) -> Result<Vec<EventEnvelope<A::Event, A::Id>>, EventStoreError> {
        let aggregate_id_str = serde_json::to_string(aggregate_id)
            .map_err(|e| EventStoreError::Serialization(e.to_string()))?;
            
        let backend = get_backend();
        
        if backend == "sqlite" {
            #[cfg(runtime_spin)]
            {
                let store = SpinSqliteEventStore::<A>::new("default");
                return store.load_async(aggregate_id).await;
            }
            #[cfg(runtime_wasmtime)]
            {
                return self.load(aggregate_id);
            }
        }
        
        let query_sqlite = "SELECT sequence, event_id, aggregate_id, aggregate_type, revision, event_type, event_version, payload, metadata, recorded_at_ms FROM events WHERE aggregate_type = ? AND aggregate_id = ? ORDER BY revision ASC";
        let query_postgres = "SELECT sequence, event_id, aggregate_id, aggregate_type, revision, event_type, event_version, payload, metadata, recorded_at_ms FROM events WHERE aggregate_type = $1 AND aggregate_id = $2 ORDER BY revision ASC";
        
        let rows = if backend == "libsql" || backend == "turso" {
            let url = get_turso_url();
            let auth = get_turso_auth_token();
            let params = vec![
                serde_json::Value::String(A::aggregate_type().to_string()),
                serde_json::Value::String(aggregate_id_str),
            ];
            execute_libsql_query(&url, auth.as_deref(), query_sqlite, params).await
                .map_err(|e| EventStoreError::Backend(e))?.rows
        } else {
            // Postgres (neon, supabase, local postgres)
            let params = vec![
                serde_json::Value::String(A::aggregate_type().to_string()),
                serde_json::Value::String(aggregate_id_str),
            ];
            #[cfg(runtime_spin)]
            {
                execute_spin_pg_async(query_postgres, params).await.map_err(|e| EventStoreError::Backend(e))?
            }
            #[cfg(runtime_wasmtime)]
            {
                let url = get_postgres_url();
                let api_key = get_neon_api_key();
                execute_postgres_query(&url, api_key.as_deref(), query_postgres, params).await
                    .map_err(|e| EventStoreError::Backend(e))?
            }
        };
        
        let mut envelopes = Vec::new();
        for row in rows {
            envelopes.push(row_to_envelope::<A>(&row)?);
        }
        Ok(envelopes)
    }

    pub async fn append_async(
        &self,
        aggregate_id: &A::Id,
        expected_revision: ExpectedRevision,
        events: Vec<NewEvent<A::Event>>,
    ) -> Result<Vec<EventEnvelope<A::Event, A::Id>>, EventStoreError> {
        let aggregate_id_str = serde_json::to_string(aggregate_id)
            .map_err(|e| EventStoreError::Serialization(e.to_string()))?;
            
        let backend = get_backend();
        
        if backend == "sqlite" {
            #[cfg(runtime_spin)]
            {
                let store = SpinSqliteEventStore::<A>::new("default");
                return store.append_async(aggregate_id, expected_revision, events).await;
            }
            #[cfg(runtime_wasmtime)]
            {
                return self.append(aggregate_id, expected_revision, events);
            }
        }
        
        let query_sqlite_rev = "SELECT COALESCE(MAX(revision), 0) as max_rev FROM events WHERE aggregate_type = ? AND aggregate_id = ?";
        let query_postgres_rev = "SELECT COALESCE(MAX(revision), 0) as max_rev FROM events WHERE aggregate_type = $1 AND aggregate_id = $2";
        
        let current_revision = {
            let params = vec![
                serde_json::Value::String(A::aggregate_type().to_string()),
                serde_json::Value::String(aggregate_id_str.clone()),
            ];
            let rows = if backend == "libsql" || backend == "turso" {
                let url = get_turso_url();
                let auth = get_turso_auth_token();
                execute_libsql_query(&url, auth.as_deref(), query_sqlite_rev, params).await
                    .map_err(|e| EventStoreError::Backend(e))?.rows
            } else {
                #[cfg(runtime_spin)]
                {
                    execute_spin_pg_async(query_postgres_rev, params).await.map_err(|e| EventStoreError::Backend(e))?
                }
                #[cfg(runtime_wasmtime)]
                {
                    let url = get_postgres_url();
                    let api_key = get_neon_api_key();
                    execute_postgres_query(&url, api_key.as_deref(), query_postgres_rev, params).await
                        .map_err(|e| EventStoreError::Backend(e))?
                }
            };
            
            let mut actual = 0u64;
            if let Some(row) = rows.first() {
                if let Some(rev) = row.get("max_rev") {
                    if let Some(r) = rev.as_u64() {
                        actual = r;
                    } else if let Some(s) = rev.as_str() {
                        actual = s.parse::<u64>().unwrap_or(0);
                    } else if let Some(i) = rev.as_i64() {
                        actual = i as u64;
                    }
                }
            }
            actual
        };
        
        match expected_revision {
            ExpectedRevision::Any => {}
            ExpectedRevision::NoStream if current_revision == 0 => {}
            ExpectedRevision::NoStream => {
                return Err(EventStoreError::Concurrency(
                    ddd_cqrs_es::ConcurrencyError::StreamAlreadyExists,
                ));
            }
            ExpectedRevision::Exact(expected) if expected == current_revision => {}
            ExpectedRevision::Exact(_) => {
                return Err(EventStoreError::Concurrency(
                    ddd_cqrs_es::ConcurrencyError::WrongExpectedRevision {
                        expected: expected_revision,
                        actual: current_revision,
                    },
                ));
            }
        }
        
        if events.is_empty() {
            return Ok(Vec::new());
        }
        
        let mut envelopes = Vec::new();
        let now = std::time::SystemTime::now();
        let recorded_at_ms = now.duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
            
        let insert_sqlite = r#"
            INSERT INTO events (
                event_id, aggregate_id, aggregate_type, revision, event_type, event_version, payload, metadata, recorded_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#;
        
        let insert_postgres = r#"
            INSERT INTO events (
                event_id, aggregate_id, aggregate_type, revision, event_type, event_version, payload, metadata, recorded_at_ms
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        "#;
        
        let select_seq_postgres = r#"
            SELECT sequence FROM events
            WHERE aggregate_type = $1 AND aggregate_id = $2 AND revision = $3
        "#;
        
        for (i, event) in events.into_iter().enumerate() {
            let revision = current_revision + i as u64 + 1;
            let event_id = EventId::new();
            
            let payload_str = serde_json::to_string(&event.payload)
                .map_err(|e| EventStoreError::Serialization(e.to_string()))?;
            let metadata_str = serde_json::to_string(&event.metadata)
                .map_err(|e| EventStoreError::Serialization(e.to_string()))?;
                
            let params = vec![
                serde_json::Value::String(event_id.to_string()),
                serde_json::Value::String(aggregate_id_str.clone()),
                serde_json::Value::String(A::aggregate_type().to_string()),
                serde_json::Value::Number(revision.into()),
                serde_json::Value::String(event.event_type.clone()),
                serde_json::Value::Number((event.event_version as u64).into()),
                serde_json::Value::String(payload_str),
                serde_json::Value::String(metadata_str),
                serde_json::Value::Number(recorded_at_ms.into()),
            ];
            
            let sequence = if backend == "libsql" || backend == "turso" {
                let url = get_turso_url();
                let auth = get_turso_auth_token();
                let res = execute_libsql_query(&url, auth.as_deref(), insert_sqlite, params).await
                    .map_err(|e| {
                        if e.contains("UNIQUE") || e.contains("constraint") {
                            EventStoreError::Concurrency(ddd_cqrs_es::ConcurrencyError::WrongExpectedRevision {
                                expected: expected_revision,
                                actual: current_revision,
                            })
                        } else {
                            EventStoreError::Backend(e)
                        }
                    })?;
                res.last_insert_rowid.ok_or_else(|| EventStoreError::Backend("Missing last_insert_rowid".to_string()))?
            } else {
                // Postgres
                // 1. Execute the INSERT statement without expecting any return rows
                {
                    #[cfg(runtime_spin)]
                    {
                        execute_spin_pg_async(insert_postgres, params).await.map_err(|e| {
                            if e.contains("UNIQUE") || e.contains("constraint") {
                                EventStoreError::Concurrency(ddd_cqrs_es::ConcurrencyError::WrongExpectedRevision {
                                    expected: expected_revision,
                                    actual: current_revision,
                                })
                            } else {
                                EventStoreError::Backend(e)
                            }
                        })?;
                    }
                    #[cfg(runtime_wasmtime)]
                    {
                        let url = get_postgres_url();
                        let api_key = get_neon_api_key();
                        execute_postgres_query(&url, api_key.as_deref(), insert_postgres, params).await
                            .map_err(|e| {
                                if e.contains("UNIQUE") || e.contains("constraint") {
                                    EventStoreError::Concurrency(ddd_cqrs_es::ConcurrencyError::WrongExpectedRevision {
                                        expected: expected_revision,
                                        actual: current_revision,
                                    })
                                } else {
                                    EventStoreError::Backend(e)
                                }
                            })?;
                    }
                }
                
                // 2. Query the inserted sequence row using unique columns to bypass the RETURNING constraint
                let seq_params = vec![
                    serde_json::Value::String(A::aggregate_type().to_string()),
                    serde_json::Value::String(aggregate_id_str.clone()),
                    serde_json::Value::Number(revision.into()),
                ];
                
                let seq_rows = {
                    #[cfg(runtime_spin)]
                    {
                        execute_spin_pg_async(select_seq_postgres, seq_params).await
                            .map_err(|e| EventStoreError::Backend(format!("Failed to retrieve sequence: {}", e)))?
                    }
                    #[cfg(runtime_wasmtime)]
                    {
                        let url = get_postgres_url();
                        let api_key = get_neon_api_key();
                        execute_postgres_query(&url, api_key.as_deref(), select_seq_postgres, seq_params).await
                            .map_err(|e| EventStoreError::Backend(format!("Failed to retrieve sequence: {}", e)))?
                    }
                };
                
                let first_row = seq_rows.first()
                    .ok_or_else(|| EventStoreError::Backend("Pg sequence query returned empty rowset".to_string()))?;
                let seq_val = first_row.get("sequence")
                    .ok_or_else(|| EventStoreError::Backend("Pg sequence row missing sequence field".to_string()))?;
                
                if let Some(s) = seq_val.as_u64() {
                    s
                } else if let Some(s) = seq_val.as_str() {
                    s.parse::<u64>().unwrap_or(0)
                } else if let Some(i) = seq_val.as_i64() {
                    i as u64
                } else {
                    return Err(EventStoreError::Backend("Failed to parse returned sequence".to_string()));
                }
            };
            
            envelopes.push(EventEnvelope::new(
                event_id,
                aggregate_id.clone(),
                A::aggregate_type(),
                revision,
                Some(sequence),
                event.event_type,
                event.event_version,
                event.payload,
                event.metadata,
                now,
            ));
        }
        
        Ok(envelopes)
    }

    pub async fn load_global_after_async(&self, sequence: Option<u64>) -> Result<Vec<EventEnvelope<A::Event, A::Id>>, EventStoreError> {
        let backend = get_backend();
        
        if backend == "sqlite" {
            #[cfg(runtime_spin)]
            {
                let store = SpinSqliteEventStore::<A>::new("default");
                return store.load_global_after_async(sequence).await;
            }
            #[cfg(runtime_wasmtime)]
            {
                return self.load_global_after(sequence);
            }
        }
        
        let seq = sequence.unwrap_or(0);
        let query_sqlite = "SELECT sequence, event_id, aggregate_id, aggregate_type, revision, event_type, event_version, payload, metadata, recorded_at_ms FROM events WHERE aggregate_type = ? AND sequence > ? ORDER BY sequence ASC";
        let query_postgres = "SELECT sequence, event_id, aggregate_id, aggregate_type, revision, event_type, event_version, payload, metadata, recorded_at_ms FROM events WHERE aggregate_type = $1 AND sequence > $2 ORDER BY sequence ASC";
        
        let rows = if backend == "libsql" || backend == "turso" {
            let url = get_turso_url();
            let auth = get_turso_auth_token();
            let params = vec![
                serde_json::Value::String(A::aggregate_type().to_string()),
                serde_json::Value::Number(seq.into()),
            ];
            execute_libsql_query(&url, auth.as_deref(), query_sqlite, params).await
                .map_err(|e| EventStoreError::Backend(e))?.rows
        } else {
            // Postgres (neon, supabase, local postgres)
            let params = vec![
                serde_json::Value::String(A::aggregate_type().to_string()),
                serde_json::Value::Number(seq.into()),
            ];
            #[cfg(runtime_spin)]
            {
                execute_spin_pg_async(query_postgres, params).await.map_err(|e| EventStoreError::Backend(e))?
            }
            #[cfg(runtime_wasmtime)]
            {
                let url = get_postgres_url();
                let api_key = get_neon_api_key();
                execute_postgres_query(&url, api_key.as_deref(), query_postgres, params).await
                    .map_err(|e| EventStoreError::Backend(e))?
            }
        };
        
        let mut envelopes = Vec::new();
        for row in rows {
            envelopes.push(row_to_envelope::<A>(&row)?);
        }
        Ok(envelopes)
    }
}

impl<A> EventStore<A> for MultiBackendEventStore<A>
where
    A: Aggregate + 'static,
    A::Event: serde::Serialize + serde::de::DeserializeOwned + Clone,
    A::Id: serde::Serialize + serde::de::DeserializeOwned + Clone + PartialEq + std::fmt::Display,
{
    type Error = EventStoreError;

    fn load(&self, aggregate_id: &A::Id) -> Result<Vec<EventEnvelope<A::Event, A::Id>>, Self::Error> {
        let aggregate_id_str = serde_json::to_string(aggregate_id)
            .map_err(|e| EventStoreError::Serialization(e.to_string()))?;
            
        let backend = get_backend();
        
        if backend == "sqlite" {
            let store = SpinSqliteEventStore::<A>::new("default");
            return store.load(aggregate_id);
        }
        
        let query_sqlite = "SELECT sequence, event_id, aggregate_id, aggregate_type, revision, event_type, event_version, payload, metadata, recorded_at_ms FROM events WHERE aggregate_type = ? AND aggregate_id = ? ORDER BY revision ASC";
        let query_postgres = "SELECT sequence, event_id, aggregate_id, aggregate_type, revision, event_type, event_version, payload, metadata, recorded_at_ms FROM events WHERE aggregate_type = $1 AND aggregate_id = $2 ORDER BY revision ASC";
        
        let rows = if backend == "libsql" || backend == "turso" {
            let url = get_turso_url();
            let auth = get_turso_auth_token();
            let params = vec![
                serde_json::Value::String(A::aggregate_type().to_string()),
                serde_json::Value::String(aggregate_id_str),
            ];
            futures::executor::block_on(execute_libsql_query(&url, auth.as_deref(), query_sqlite, params))
                .map_err(|e| EventStoreError::Backend(e))?.rows
        } else {
            // Postgres (neon, supabase, local postgres)
            let params = vec![
                serde_json::Value::String(A::aggregate_type().to_string()),
                serde_json::Value::String(aggregate_id_str),
            ];
            #[cfg(runtime_spin)]
            {
                execute_spin_pg(query_postgres, params).map_err(|e| EventStoreError::Backend(e))?
            }
            #[cfg(runtime_wasmtime)]
            {
                let url = get_postgres_url();
                let api_key = get_neon_api_key();
                futures::executor::block_on(execute_postgres_query(&url, api_key.as_deref(), query_postgres, params))
                    .map_err(|e| EventStoreError::Backend(e))?
            }
        };
        
        let mut envelopes = Vec::new();
        for row in rows {
            envelopes.push(row_to_envelope::<A>(&row)?);
        }
        Ok(envelopes)
    }

    fn append(
        &self,
        aggregate_id: &A::Id,
        expected_revision: ExpectedRevision,
        events: Vec<NewEvent<A::Event>>,
    ) -> Result<Vec<EventEnvelope<A::Event, A::Id>>, Self::Error> {
        let aggregate_id_str = serde_json::to_string(aggregate_id)
            .map_err(|e| EventStoreError::Serialization(e.to_string()))?;
            
        let backend = get_backend();
        
        if backend == "sqlite" {
            let store = SpinSqliteEventStore::<A>::new("default");
            return store.append(aggregate_id, expected_revision, events);
        }
        
        let query_sqlite_rev = "SELECT COALESCE(MAX(revision), 0) as max_rev FROM events WHERE aggregate_type = ? AND aggregate_id = ?";
        let query_postgres_rev = "SELECT COALESCE(MAX(revision), 0) as max_rev FROM events WHERE aggregate_type = $1 AND aggregate_id = $2";
        
        let current_revision = {
            let params = vec![
                serde_json::Value::String(A::aggregate_type().to_string()),
                serde_json::Value::String(aggregate_id_str.clone()),
            ];
            let rows = if backend == "libsql" || backend == "turso" {
                let url = get_turso_url();
                let auth = get_turso_auth_token();
                futures::executor::block_on(execute_libsql_query(&url, auth.as_deref(), query_sqlite_rev, params))
                    .map_err(|e| EventStoreError::Backend(e))?.rows
            } else {
                #[cfg(runtime_spin)]
                {
                    execute_spin_pg(query_postgres_rev, params).map_err(|e| EventStoreError::Backend(e))?
                }
                #[cfg(runtime_wasmtime)]
                {
                    let url = get_postgres_url();
                    let api_key = get_neon_api_key();
                    futures::executor::block_on(execute_postgres_query(&url, api_key.as_deref(), query_postgres_rev, params))
                        .map_err(|e| EventStoreError::Backend(e))?
                }
            };
            
            let mut actual = 0u64;
            if let Some(row) = rows.first() {
                if let Some(rev) = row.get("max_rev") {
                    if let Some(r) = rev.as_u64() {
                        actual = r;
                    } else if let Some(s) = rev.as_str() {
                        actual = s.parse::<u64>().unwrap_or(0);
                    } else if let Some(i) = rev.as_i64() {
                        actual = i as u64;
                    }
                }
            }
            actual
        };
        
        match expected_revision {
            ExpectedRevision::Any => {}
            ExpectedRevision::NoStream if current_revision == 0 => {}
            ExpectedRevision::NoStream => {
                return Err(EventStoreError::Concurrency(
                    ddd_cqrs_es::ConcurrencyError::StreamAlreadyExists,
                ));
            }
            ExpectedRevision::Exact(expected) if expected == current_revision => {}
            ExpectedRevision::Exact(_) => {
                return Err(EventStoreError::Concurrency(
                    ddd_cqrs_es::ConcurrencyError::WrongExpectedRevision {
                        expected: expected_revision,
                        actual: current_revision,
                    },
                ));
            }
        }
        
        if events.is_empty() {
            return Ok(Vec::new());
        }
        
        let mut envelopes = Vec::new();
        let now = std::time::SystemTime::now();
        let recorded_at_ms = now.duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
            
        let insert_sqlite = r#"
            INSERT INTO events (
                event_id, aggregate_id, aggregate_type, revision, event_type, event_version, payload, metadata, recorded_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#;
        
        let insert_postgres = r#"
            INSERT INTO events (
                event_id, aggregate_id, aggregate_type, revision, event_type, event_version, payload, metadata, recorded_at_ms
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        "#;
        
        let select_seq_postgres = r#"
            SELECT sequence FROM events
            WHERE aggregate_type = $1 AND aggregate_id = $2 AND revision = $3
        "#;
        
        for (i, event) in events.into_iter().enumerate() {
            let revision = current_revision + i as u64 + 1;
            let event_id = EventId::new();
            
            let payload_str = serde_json::to_string(&event.payload)
                .map_err(|e| EventStoreError::Serialization(e.to_string()))?;
            let metadata_str = serde_json::to_string(&event.metadata)
                .map_err(|e| EventStoreError::Serialization(e.to_string()))?;
                
            let params = vec![
                serde_json::Value::String(event_id.to_string()),
                serde_json::Value::String(aggregate_id_str.clone()),
                serde_json::Value::String(A::aggregate_type().to_string()),
                serde_json::Value::Number(revision.into()),
                serde_json::Value::String(event.event_type.clone()),
                serde_json::Value::Number((event.event_version as u64).into()),
                serde_json::Value::String(payload_str),
                serde_json::Value::String(metadata_str),
                serde_json::Value::Number(recorded_at_ms.into()),
            ];
            
            let sequence = if backend == "libsql" || backend == "turso" {
                let url = get_turso_url();
                let auth = get_turso_auth_token();
                let res = futures::executor::block_on(execute_libsql_query(&url, auth.as_deref(), insert_sqlite, params))
                    .map_err(|e| {
                        if e.contains("UNIQUE") || e.contains("constraint") {
                            EventStoreError::Concurrency(ddd_cqrs_es::ConcurrencyError::WrongExpectedRevision {
                                expected: expected_revision,
                                actual: current_revision,
                            })
                        } else {
                            EventStoreError::Backend(e)
                        }
                    })?;
                res.last_insert_rowid.ok_or_else(|| EventStoreError::Backend("Missing last_insert_rowid".to_string()))?
            } else {
                // Postgres
                // 1. Execute the INSERT statement without expecting any return rows
                {
                    #[cfg(runtime_spin)]
                    {
                        execute_spin_pg(insert_postgres, params).map_err(|e| {
                            if e.contains("UNIQUE") || e.contains("constraint") {
                                EventStoreError::Concurrency(ddd_cqrs_es::ConcurrencyError::WrongExpectedRevision {
                                    expected: expected_revision,
                                    actual: current_revision,
                                })
                            } else {
                                EventStoreError::Backend(e)
                            }
                        })?;
                    }
                    #[cfg(runtime_wasmtime)]
                    {
                        let url = get_postgres_url();
                        let api_key = get_neon_api_key();
                        futures::executor::block_on(execute_postgres_query(&url, api_key.as_deref(), insert_postgres, params))
                            .map_err(|e| {
                                if e.contains("UNIQUE") || e.contains("constraint") {
                                    EventStoreError::Concurrency(ddd_cqrs_es::ConcurrencyError::WrongExpectedRevision {
                                        expected: expected_revision,
                                        actual: current_revision,
                                    })
                                } else {
                                    EventStoreError::Backend(e)
                                }
                            })?;
                    }
                }
                
                // 2. Query the inserted sequence row using unique columns to bypass the RETURNING constraint
                let seq_params = vec![
                    serde_json::Value::String(A::aggregate_type().to_string()),
                    serde_json::Value::String(aggregate_id_str.clone()),
                    serde_json::Value::Number(revision.into()),
                ];
                
                let seq_rows = {
                    #[cfg(runtime_spin)]
                    {
                        execute_spin_pg(select_seq_postgres, seq_params)
                            .map_err(|e| EventStoreError::Backend(format!("Failed to retrieve sequence: {}", e)))?
                    }
                    #[cfg(runtime_wasmtime)]
                    {
                        let url = get_postgres_url();
                        let api_key = get_neon_api_key();
                        futures::executor::block_on(execute_postgres_query(&url, api_key.as_deref(), select_seq_postgres, seq_params))
                            .map_err(|e| EventStoreError::Backend(format!("Failed to retrieve sequence: {}", e)))?
                    }
                };
                
                let first_row = seq_rows.first()
                    .ok_or_else(|| EventStoreError::Backend("Pg sequence query returned empty rowset".to_string()))?;
                let seq_val = first_row.get("sequence")
                    .ok_or_else(|| EventStoreError::Backend("Pg sequence row missing sequence field".to_string()))?;
                
                if let Some(s) = seq_val.as_u64() {
                    s
                } else if let Some(s) = seq_val.as_str() {
                    s.parse::<u64>().unwrap_or(0)
                } else if let Some(i) = seq_val.as_i64() {
                    i as u64
                } else {
                    return Err(EventStoreError::Backend("Failed to parse returned sequence".to_string()));
                }
            };
            
            envelopes.push(EventEnvelope::new(
                event_id,
                aggregate_id.clone(),
                A::aggregate_type(),
                revision,
                Some(sequence),
                event.event_type,
                event.event_version,
                event.payload,
                event.metadata,
                now,
            ));
        }
        
        Ok(envelopes)
    }

    fn load_global_after(&self, sequence: Option<u64>) -> Result<Vec<EventEnvelope<A::Event, A::Id>>, Self::Error> {
        let backend = get_backend();
        
        if backend == "sqlite" {
            let store = SpinSqliteEventStore::<A>::new("default");
            return store.load_global_after(sequence);
        }
        
        let seq = sequence.unwrap_or(0);
        let query_sqlite = "SELECT sequence, event_id, aggregate_id, aggregate_type, revision, event_type, event_version, payload, metadata, recorded_at_ms FROM events WHERE aggregate_type = ? AND sequence > ? ORDER BY sequence ASC";
        let query_postgres = "SELECT sequence, event_id, aggregate_id, aggregate_type, revision, event_type, event_version, payload, metadata, recorded_at_ms FROM events WHERE aggregate_type = $1 AND sequence > $2 ORDER BY sequence ASC";
        
        let rows = if backend == "libsql" || backend == "turso" {
            let url = get_turso_url();
            let auth = get_turso_auth_token();
            let params = vec![
                serde_json::Value::String(A::aggregate_type().to_string()),
                serde_json::Value::Number(seq.into()),
            ];
            futures::executor::block_on(execute_libsql_query(&url, auth.as_deref(), query_sqlite, params))
                .map_err(|e| EventStoreError::Backend(e))?.rows
        } else {
            // Postgres (neon, supabase, local postgres)
            let params = vec![
                serde_json::Value::String(A::aggregate_type().to_string()),
                serde_json::Value::Number(seq.into()),
            ];
            #[cfg(runtime_spin)]
            {
                execute_spin_pg(query_postgres, params).map_err(|e| EventStoreError::Backend(e))?
            }
            #[cfg(runtime_wasmtime)]
            {
                let url = get_postgres_url();
                let api_key = get_neon_api_key();
                futures::executor::block_on(execute_postgres_query(&url, api_key.as_deref(), query_postgres, params))
                    .map_err(|e| EventStoreError::Backend(e))?
            }
        };
        
        let mut envelopes = Vec::new();
        for row in rows {
            envelopes.push(row_to_envelope::<A>(&row)?);
        }
        Ok(envelopes)
    }
}

pub struct MultiBackendCheckpointStore;

impl Clone for MultiBackendCheckpointStore {
    fn clone(&self) -> Self {
        Self
    }
}

impl MultiBackendCheckpointStore {
    pub fn new() -> Self {
        Self
    }
}

impl ddd_cqrs_es::CheckpointStore for MultiBackendCheckpointStore {
    type Error = EventStoreError;

    fn load_checkpoint(&self, projection_name: &str) -> Result<Option<u64>, Self::Error> {
        let backend = get_backend();
        
        if backend == "sqlite" {
            let store = SpinSqliteCheckpointStore::new("default");
            return store.load_checkpoint(projection_name);
        }
        
        let query_sqlite = "SELECT last_sequence FROM checkpoints WHERE projection_name = ?";
        let query_postgres = "SELECT last_sequence FROM checkpoints WHERE projection_name = $1";
        
        let rows = if backend == "libsql" || backend == "turso" {
            let url = get_turso_url();
            let auth = get_turso_auth_token();
            let params = vec![serde_json::Value::String(projection_name.to_string())];
            futures::executor::block_on(execute_libsql_query(&url, auth.as_deref(), query_sqlite, params))
                .map_err(|e| EventStoreError::Backend(e))?.rows
        } else {
            // Postgres
            let params = vec![serde_json::Value::String(projection_name.to_string())];
            #[cfg(runtime_spin)]
            {
                execute_spin_pg(query_postgres, params).map_err(|e| EventStoreError::Backend(e))?
            }
            #[cfg(runtime_wasmtime)]
            {
                let url = get_postgres_url();
                let api_key = get_neon_api_key();
                futures::executor::block_on(execute_postgres_query(&url, api_key.as_deref(), query_postgres, params))
                    .map_err(|e| EventStoreError::Backend(e))?
            }
        };
        
        if let Some(row) = rows.first() {
            if let Some(last) = row.get("last_sequence") {
                if let Some(u) = last.as_u64() {
                    return Ok(Some(u));
                } else if let Some(s) = last.as_str() {
                    return Ok(s.parse::<u64>().ok());
                } else if let Some(i) = last.as_i64() {
                    return Ok(Some(i as u64));
                }
            }
        }
        
        Ok(None)
    }

    fn save_checkpoint(&self, projection_name: &str, sequence: u64) -> Result<(), Self::Error> {
        let backend = get_backend();
        
        if backend == "sqlite" {
            let store = SpinSqliteCheckpointStore::new("default");
            return store.save_checkpoint(projection_name, sequence);
        }
        
        let sql_sqlite = "INSERT INTO checkpoints (projection_name, last_sequence) VALUES (?, ?) \
                          ON CONFLICT(projection_name) DO UPDATE SET last_sequence = excluded.last_sequence;";
        let sql_postgres = "INSERT INTO checkpoints (projection_name, last_sequence) VALUES ($1, $2) \
                            ON CONFLICT(projection_name) DO UPDATE SET last_sequence = EXCLUDED.last_sequence;";
        
        let params = vec![
            serde_json::Value::String(projection_name.to_string()),
            serde_json::Value::Number(sequence.into()),
        ];
        
        if backend == "libsql" || backend == "turso" {
            let url = get_turso_url();
            let auth = get_turso_auth_token();
            let _ = futures::executor::block_on(execute_libsql_query(&url, auth.as_deref(), sql_sqlite, params))
                .map_err(|e| EventStoreError::Backend(e))?;
        } else {
            // Postgres
            #[cfg(runtime_spin)]
            {
                let _ = execute_spin_pg(sql_postgres, params).map_err(|e| EventStoreError::Backend(e))?;
            }
            #[cfg(runtime_wasmtime)]
            {
                let url = get_postgres_url();
                let api_key = get_neon_api_key();
                let _ = futures::executor::block_on(execute_postgres_query(&url, api_key.as_deref(), sql_postgres, params))
                    .map_err(|e| EventStoreError::Backend(e))?;
            }
        }
        
        Ok(())
    }
}

impl MultiBackendCheckpointStore {
    pub async fn load_checkpoint_async(&self, projection_name: &str) -> Result<Option<u64>, EventStoreError> {
        let backend = get_backend();
        
        if backend == "sqlite" {
            #[cfg(runtime_spin)]
            {
                let store = SpinSqliteCheckpointStore::new("default");
                return store.load_checkpoint_async(projection_name).await;
            }
            #[cfg(runtime_wasmtime)]
            {
                use ddd_cqrs_es::CheckpointStore;
                return self.load_checkpoint(projection_name);
            }
        }
        
        let query_sqlite = "SELECT last_sequence FROM checkpoints WHERE projection_name = ?";
        let query_postgres = "SELECT last_sequence FROM checkpoints WHERE projection_name = $1";
        
        let rows = if backend == "libsql" || backend == "turso" {
            let url = get_turso_url();
            let auth = get_turso_auth_token();
            let params = vec![serde_json::Value::String(projection_name.to_string())];
            execute_libsql_query(&url, auth.as_deref(), query_sqlite, params).await
                .map_err(|e| EventStoreError::Backend(e))?.rows
        } else {
            // Postgres
            let params = vec![serde_json::Value::String(projection_name.to_string())];
            #[cfg(runtime_spin)]
            {
                execute_spin_pg_async(query_postgres, params).await.map_err(|e| EventStoreError::Backend(e))?
            }
            #[cfg(runtime_wasmtime)]
            {
                let url = get_postgres_url();
                let api_key = get_neon_api_key();
                execute_postgres_query(&url, api_key.as_deref(), query_postgres, params).await
                    .map_err(|e| EventStoreError::Backend(e))?
            }
        };
        
        if let Some(row) = rows.first() {
            if let Some(last) = row.get("last_sequence") {
                if let Some(u) = last.as_u64() {
                    return Ok(Some(u));
                } else if let Some(s) = last.as_str() {
                    return Ok(s.parse::<u64>().ok());
                } else if let Some(i) = last.as_i64() {
                    return Ok(Some(i as u64));
                }
            }
        }
        
        Ok(None)
    }

    pub async fn save_checkpoint_async(&self, projection_name: &str, sequence: u64) -> Result<(), EventStoreError> {
        let backend = get_backend();
        
        if backend == "sqlite" {
            #[cfg(runtime_spin)]
            {
                let store = SpinSqliteCheckpointStore::new("default");
                return store.save_checkpoint_async(projection_name, sequence).await;
            }
            #[cfg(runtime_wasmtime)]
            {
                use ddd_cqrs_es::CheckpointStore;
                return self.save_checkpoint(projection_name, sequence);
            }
        }
        
        let sql_sqlite = "INSERT INTO checkpoints (projection_name, last_sequence) VALUES (?, ?) \
                          ON CONFLICT(projection_name) DO UPDATE SET last_sequence = excluded.last_sequence;";
        let sql_postgres = "INSERT INTO checkpoints (projection_name, last_sequence) VALUES ($1, $2) \
                            ON CONFLICT(projection_name) DO UPDATE SET last_sequence = EXCLUDED.last_sequence;";
        
        let params = vec![
            serde_json::Value::String(projection_name.to_string()),
            serde_json::Value::Number(sequence.into()),
        ];
        
        if backend == "libsql" || backend == "turso" {
            let url = get_turso_url();
            let auth = get_turso_auth_token();
            let _ = execute_libsql_query(&url, auth.as_deref(), sql_sqlite, params).await
                .map_err(|e| EventStoreError::Backend(e))?;
        } else {
            // Postgres
            #[cfg(runtime_spin)]
            {
                let _ = execute_spin_pg_async(sql_postgres, params).await.map_err(|e| EventStoreError::Backend(e))?;
            }
            #[cfg(runtime_wasmtime)]
            {
                let url = get_postgres_url();
                let api_key = get_neon_api_key();
                let _ = execute_postgres_query(&url, api_key.as_deref(), sql_postgres, params).await
                    .map_err(|e| EventStoreError::Backend(e))?;
            }
        }
        
        Ok(())
    }
}

pub struct MultiBackendCounterProjection;

impl MultiBackendCounterProjection {
    pub fn new() -> Self {
        Self
    }
}

impl ddd_cqrs_es::Projection<crate::domain::CounterEvent, crate::domain::CounterId> for MultiBackendCounterProjection {
    type Error = EventStoreError;

    fn name(&self) -> &'static str {
        "counter_projection"
    }

    fn apply(&mut self, envelope: &EventEnvelope<crate::domain::CounterEvent, crate::domain::CounterId>) -> Result<(), Self::Error> {
        let aggregate_id_str = serde_json::to_string(&envelope.aggregate_id)
            .map_err(|e| EventStoreError::Serialization(e.to_string()))?;
            
        let backend = get_backend();
        
        if backend == "sqlite" {
            let mut store = CounterProjection::new("default");
            return store.apply(envelope);
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
        
        if backend == "libsql" || backend == "turso" {
            let url = get_turso_url();
            let auth = get_turso_auth_token();
            let _ = futures::executor::block_on(execute_libsql_query(&url, auth.as_deref(), sql_sqlite, params_upsert))
                .map_err(|e| EventStoreError::Backend(e))?;
        } else {
            // Postgres
            #[cfg(runtime_spin)]
            {
                let _ = execute_spin_pg(sql_postgres, params_upsert).map_err(|e| EventStoreError::Backend(e))?;
            }
            #[cfg(runtime_wasmtime)]
            {
                let url = get_postgres_url();
                let api_key = get_neon_api_key();
                let _ = futures::executor::block_on(execute_postgres_query(&url, api_key.as_deref(), sql_postgres, params_upsert))
                    .map_err(|e| EventStoreError::Backend(e))?;
            }
        }
        
        Ok(())
    }
}

impl MultiBackendCounterProjection {
    pub async fn apply_async(&mut self, envelope: &EventEnvelope<crate::domain::CounterEvent, crate::domain::CounterId>) -> Result<(), EventStoreError> {
        let aggregate_id_str = serde_json::to_string(&envelope.aggregate_id)
            .map_err(|e| EventStoreError::Serialization(e.to_string()))?;
            
        let backend = get_backend();
        
        if backend == "sqlite" {
            #[cfg(runtime_spin)]
            {
                let mut store = CounterProjection::new("default");
                return store.apply_async(envelope).await;
            }
            #[cfg(runtime_wasmtime)]
            {
                use ddd_cqrs_es::Projection;
                return self.apply(envelope);
            }
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
        
        if backend == "libsql" || backend == "turso" {
            let url = get_turso_url();
            let auth = get_turso_auth_token();
            let _ = execute_libsql_query(&url, auth.as_deref(), sql_sqlite, params_upsert).await
                .map_err(|e| EventStoreError::Backend(e))?;
        } else {
            // Postgres
            #[cfg(runtime_spin)]
            {
                let _ = execute_spin_pg_async(sql_postgres, params_upsert).await.map_err(|e| EventStoreError::Backend(e))?;
            }
            #[cfg(runtime_wasmtime)]
            {
                let url = get_postgres_url();
                let api_key = get_neon_api_key();
                let _ = execute_postgres_query(&url, api_key.as_deref(), sql_postgres, params_upsert).await
                    .map_err(|e| EventStoreError::Backend(e))?;
            }
        }
        
        Ok(())
    }
}

// =========================================================================
// 5. UNIFIED HIGH-LEVEL QUERY APIS FOR LEPTOS SERVER FUNCTIONS
// =========================================================================

pub async fn get_count_db() -> Result<i32, String> {
    let backend = get_backend();
    
    if backend == "sqlite" {
        #[cfg(runtime_spin)]
        {
            use spin_sdk::sqlite::{Connection, Value as SpinValue};
            let connection = Connection::open("default").await
                .map_err(|e| e.to_string())?;
            let query = "SELECT value FROM counter_read_model WHERE id = ?";
            let aggregate_id = crate::domain::CounterId("global".to_string());
            let aggregate_id_str = serde_json::to_string(&aggregate_id)
                .map_err(|e| e.to_string())?;
            
            let params = vec![SpinValue::Text(aggregate_id_str)];
            let rowset = connection.execute(query, params).await
                .map_err(|e| e.to_string())?;

            let rows = rowset.collect().await
                .map_err(|e| e.to_string())?;

            if let Some(row) = rows.first() {
                if let Some(val) = row.get::<i64>(0) {
                    return Ok(val as i32);
                }
            }
            return Ok(0);
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
            let map: std::collections::HashMap<String, i32> = serde_json::from_str(&content)
                .map_err(|e| e.to_string())?;
            let aggregate_id = crate::domain::CounterId("global".to_string());
            let aggregate_id_str = serde_json::to_string(&aggregate_id)
                .map_err(|e| e.to_string())?;
            return Ok(map.get(&aggregate_id_str).copied().unwrap_or(0));
        }
    }
    
    let query_sqlite = "SELECT value FROM counter_read_model WHERE id = ?";
    let query_postgres = "SELECT value FROM counter_read_model WHERE id = $1";
    
    let aggregate_id = crate::domain::CounterId("global".to_string());
    let aggregate_id_str = serde_json::to_string(&aggregate_id)
        .map_err(|e| e.to_string())?;
    let params = vec![serde_json::Value::String(aggregate_id_str)];
    
    let rows = if backend == "libsql" || backend == "turso" {
        let url = get_turso_url();
        let auth = get_turso_auth_token();
        execute_libsql_query(&url, auth.as_deref(), query_sqlite, params).await
            .map_err(|e| e)?
            .rows
    } else {
        // Postgres
        #[cfg(runtime_spin)]
        {
            execute_spin_pg_async(query_postgres, params).await?
        }
        #[cfg(runtime_wasmtime)]
        {
            let url = get_postgres_url();
            let api_key = get_neon_api_key();
            execute_postgres_query(&url, api_key.as_deref(), query_postgres, params).await?
        }
    };
    
    if let Some(row) = rows.first() {
        if let Some(val) = row.get("value") {
            if let Some(i) = val.as_i64() {
                return Ok(i as i32);
            } else if let Some(s) = val.as_str() {
                if let Ok(i) = s.parse::<i32>() {
                    return Ok(i);
                }
            }
        }
    }
    
    Ok(0)
}

pub async fn get_latest_events_db() -> Result<Vec<crate::app::EventLogDto>, String> {
    let backend = get_backend();
    
    if backend == "sqlite" {
        #[cfg(runtime_spin)]
        {
            use spin_sdk::sqlite::{Connection, Value as SpinValue};
            let connection = Connection::open("default").await
                .map_err(|e| e.to_string())?;
            let query = "SELECT sequence, event_type, revision, payload, recorded_at_ms FROM events ORDER BY sequence DESC LIMIT 5";
            let rowset = connection.execute(query, Vec::<SpinValue>::new()).await
                .map_err(|e| e.to_string())?;

            let rows = rowset.collect().await
                .map_err(|e| e.to_string())?;

            let mut events = Vec::new();
            for row in rows {
                let sequence = row.get::<i64>(0).unwrap_or(0) as u64;
                let event_type = row.get::<&str>(1).unwrap_or("").to_string();
                let revision = row.get::<i64>(2).unwrap_or(0) as u64;
                let payload = row.get::<&str>(3).unwrap_or("").to_string();
                let recorded_at_ms = row.get::<i64>(4).unwrap_or(0);

                let recorded_at = format!("+{}ms", recorded_at_ms % 100000);

                events.push(crate::app::EventLogDto {
                    sequence,
                    event_type,
                    revision,
                    payload,
                    recorded_at,
                });
            }
            return Ok(events);
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

            let mut events = Vec::new();
            let mut matching_vals: Vec<serde_json::Value> = values.into_iter()
                .filter(|val| {
                    use ddd_cqrs_es::Aggregate;
                    val.get("aggregate_type").and_then(|t| t.as_str()) == Some(crate::domain::Counter::aggregate_type())
                })
                .collect();
            
            matching_vals.sort_by_key(|val| val.get("sequence").and_then(|s| s.as_u64()).unwrap_or(0));
            matching_vals.reverse();

            for val in matching_vals.into_iter().take(5) {
                let sequence = val.get("sequence").and_then(|s| s.as_u64()).unwrap_or(0);
                let event_type = val.get("event_type").and_then(|t| t.as_str()).unwrap_or("").to_string();
                let revision = val.get("revision").and_then(|r| r.as_u64()).unwrap_or(0);
                let payload = val.get("payload").map(|p| p.to_string()).unwrap_or_default();
                let recorded_at_ms = val.get("recorded_at_ms").and_then(|r| r.as_i64()).unwrap_or(0);

                let recorded_at = format!("+{}ms", recorded_at_ms % 100000);

                events.push(crate::app::EventLogDto {
                    sequence,
                    event_type,
                    revision,
                    payload,
                    recorded_at,
                });
            }
            return Ok(events);
        }
    }
    
    let query = "SELECT sequence, event_type, revision, payload, recorded_at_ms FROM events ORDER BY sequence DESC LIMIT 5";
    
    let rows = if backend == "libsql" || backend == "turso" {
        let url = get_turso_url();
        let auth = get_turso_auth_token();
        execute_libsql_query(&url, auth.as_deref(), query, Vec::new()).await
            .map_err(|e| e)?
            .rows
    } else {
        // Postgres
        #[cfg(runtime_spin)]
        {
            execute_spin_pg_async(query, Vec::new()).await?
        }
        #[cfg(runtime_wasmtime)]
        {
            let url = get_postgres_url();
            let api_key = get_neon_api_key();
            execute_postgres_query(&url, api_key.as_deref(), query, Vec::new()).await?
        }
    };
    
    let mut events = Vec::new();
    for row in rows {
        let sequence = if let Some(seq) = row.get("sequence") {
            if let Some(u) = seq.as_u64() {
                u
            } else if let Some(s) = seq.as_str() {
                s.parse::<u64>().unwrap_or(0)
            } else if let Some(i) = seq.as_i64() {
                i as u64
            } else {
                0
            }
        } else {
            0
        };
        
        let event_type = row.get("event_type")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
            
        let revision = if let Some(rev) = row.get("revision") {
            if let Some(u) = rev.as_u64() {
                u
            } else if let Some(s) = rev.as_str() {
                s.parse::<u64>().unwrap_or(0)
            } else if let Some(i) = rev.as_i64() {
                i as u64
            } else {
                0
            }
        } else {
            0
        };
        
        let payload = row.get("payload")
            .map(|v| {
                if v.is_string() {
                    v.as_str().unwrap_or("").to_string()
                } else {
                    v.to_string()
                }
            })
            .unwrap_or_default();
            
        let recorded_at_ms = if let Some(rec) = row.get("recorded_at_ms") {
            if let Some(i) = rec.as_i64() {
                i
            } else if let Some(s) = rec.as_str() {
                s.parse::<i64>().unwrap_or(0)
            } else if let Some(u) = rec.as_u64() {
                u as i64
            } else {
                0
            }
        } else {
            0
        };
        
        let recorded_at = format!("+{}ms", recorded_at_ms % 100000);
        
        events.push(crate::app::EventLogDto {
            sequence,
            event_type,
            revision,
            payload,
            recorded_at,
        });
    }
    
    Ok(events)
}

// -------------------------------------------------------------------------
// ASYNC COORDINATOR FOR PROJECTIONS RUNNER
// -------------------------------------------------------------------------
pub async fn run_projections_async(
    event_store: &MultiBackendEventStore<crate::domain::Counter>,
    checkpoint_store: &MultiBackendCheckpointStore,
    projection: &mut MultiBackendCounterProjection,
) -> Result<usize, String> {
    use ddd_cqrs_es::Projection;
    
    let last_sequence = checkpoint_store.load_checkpoint_async(projection.name()).await
        .map_err(|e| e.to_string())?;
        
    let envelopes = event_store.load_global_after_async(last_sequence).await
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
        checkpoint_store.save_checkpoint_async(projection.name(), seq).await
            .map_err(|e| e.to_string())?;
    }
    
    Ok(count)
}
