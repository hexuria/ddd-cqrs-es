use crate::error::EventStoreError;
use crate::event::{EventEnvelope, ExpectedRevision, NewEvent};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Event persistence abstraction.
///
/// Implement this trait for Postgres, DynamoDB, Kafka, EventStoreDB, or any
/// storage engine you use in production.
pub trait EventStore<E, M = ()>: Clone + Send + Sync + 'static
where
    E: Clone,
    M: Clone,
{
    fn load(&self, aggregate_id: &str) -> Result<Vec<EventEnvelope<E, M>>, EventStoreError>;

    fn append(
        &self,
        aggregate_id: &str,
        expected: ExpectedRevision,
        events: Vec<NewEvent<E, M>>,
    ) -> Result<Vec<EventEnvelope<E, M>>, EventStoreError>;
}

/// Thread-safe in-memory event store.
///
/// Useful for tests, examples, and local development. It is not a durable
/// production store.
#[derive(Debug)]
pub struct InMemoryEventStore<E, M = ()>
where
    E: Clone,
    M: Clone,
{
    streams: Arc<Mutex<HashMap<String, Vec<EventEnvelope<E, M>>>>>,
}

impl<E, M> Clone for InMemoryEventStore<E, M>
where
    E: Clone,
    M: Clone,
{
    fn clone(&self) -> Self {
        Self {
            streams: Arc::clone(&self.streams),
        }
    }
}

impl<E, M> Default for InMemoryEventStore<E, M>
where
    E: Clone,
    M: Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<E, M> InMemoryEventStore<E, M>
where
    E: Clone,
    M: Clone,
{
    pub fn new() -> Self {
        Self {
            streams: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn stream_count(&self) -> Result<usize, EventStoreError> {
        Ok(self
            .streams
            .lock()
            .map_err(|_| EventStoreError::Poisoned)?
            .len())
    }
}

impl<E, M> EventStore<E, M> for InMemoryEventStore<E, M>
where
    E: Clone + Send + 'static,
    M: Clone + Send + 'static,
{
    fn load(&self, aggregate_id: &str) -> Result<Vec<EventEnvelope<E, M>>, EventStoreError> {
        let streams = self.streams.lock().map_err(|_| EventStoreError::Poisoned)?;
        Ok(streams.get(aggregate_id).cloned().unwrap_or_default())
    }

    fn append(
        &self,
        aggregate_id: &str,
        expected: ExpectedRevision,
        events: Vec<NewEvent<E, M>>,
    ) -> Result<Vec<EventEnvelope<E, M>>, EventStoreError> {
        let mut streams = self.streams.lock().map_err(|_| EventStoreError::Poisoned)?;
        let stream = streams.entry(aggregate_id.to_owned()).or_default();
        let actual_revision = stream.len() as u64;

        let expected_matches = match expected {
            ExpectedRevision::Any => true,
            ExpectedRevision::NoStream => actual_revision == 0,
            ExpectedRevision::Exact(revision) => actual_revision == revision,
        };

        if !expected_matches {
            return Err(EventStoreError::Conflict {
                expected,
                actual: actual_revision,
            });
        }

        let mut persisted = Vec::with_capacity(events.len());
        for new_event in events {
            let revision = stream.len() as u64 + 1;
            let envelope = EventEnvelope::new(
                aggregate_id.to_owned(),
                revision,
                new_event.event,
                new_event.metadata,
            );
            stream.push(envelope.clone());
            persisted.push(envelope);
        }

        Ok(persisted)
    }
}
