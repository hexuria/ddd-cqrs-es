use crate::aggregate::Aggregate;
use crate::error::{ConcurrencyError, EventStoreError};
use crate::event::{EventEnvelope, EventId, ExpectedRevision, NewEvent};
use crate::event_store::{EventStore, EventStream};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

struct MemoryState<A>
where
    A: Aggregate,
{
    streams: HashMap<A::Id, EventStream<A>>,
    global: EventStream<A>,
    next_sequence: u64,
}

impl<A> Default for MemoryState<A>
where
    A: Aggregate,
{
    fn default() -> Self {
        Self {
            streams: HashMap::new(),
            global: Vec::new(),
            next_sequence: 1,
        }
    }
}

/// Thread-safe in-memory event store.
///
/// This store is intended for tests, examples, and local development. It is
/// not durable, but it enforces the same stream revision checks production
/// adapters should enforce.
pub struct InMemoryEventStore<A>
where
    A: Aggregate,
{
    state: Arc<RwLock<MemoryState<A>>>,
}

impl<A> Clone for InMemoryEventStore<A>
where
    A: Aggregate,
{
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl<A> std::fmt::Debug for InMemoryEventStore<A>
where
    A: Aggregate,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryEventStore").finish_non_exhaustive()
    }
}

impl<A> Default for InMemoryEventStore<A>
where
    A: Aggregate,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<A> InMemoryEventStore<A>
where
    A: Aggregate,
{
    /// Creates an empty in-memory event store.
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(MemoryState::default())),
        }
    }

    /// Returns the number of aggregate streams currently stored.
    pub fn stream_count(&self) -> Result<usize, EventStoreError> {
        let state = self.state.read().map_err(|_| EventStoreError::Poisoned)?;
        Ok(state.streams.len())
    }

    /// Removes all streams and resets the global sequence.
    pub fn clear(&self) -> Result<(), EventStoreError> {
        let mut state = self.state.write().map_err(|_| EventStoreError::Poisoned)?;
        state.streams.clear();
        state.global.clear();
        state.next_sequence = 1;
        Ok(())
    }
}

impl<A> EventStore<A> for InMemoryEventStore<A>
where
    A: Aggregate + 'static,
{
    type Error = EventStoreError;

    fn load(&self, aggregate_id: &A::Id) -> Result<EventStream<A>, Self::Error> {
        let state = self.state.read().map_err(|_| EventStoreError::Poisoned)?;
        Ok(state.streams.get(aggregate_id).cloned().unwrap_or_default())
    }

    fn append(
        &self,
        aggregate_id: &A::Id,
        expected_revision: ExpectedRevision,
        events: Vec<NewEvent<A::Event>>,
    ) -> Result<EventStream<A>, Self::Error> {
        let mut state = self.state.write().map_err(|_| EventStoreError::Poisoned)?;
        let actual_revision = state
            .streams
            .get(aggregate_id)
            .map(|stream| stream.len() as u64)
            .unwrap_or_default();

        match expected_revision {
            ExpectedRevision::Any => {}
            ExpectedRevision::NoStream if actual_revision == 0 => {}
            ExpectedRevision::NoStream => {
                return Err(EventStoreError::Concurrency(
                    ConcurrencyError::StreamAlreadyExists,
                ));
            }
            ExpectedRevision::Exact(expected) if expected == actual_revision => {}
            ExpectedRevision::Exact(_) => {
                return Err(EventStoreError::Concurrency(
                    ConcurrencyError::WrongExpectedRevision {
                        expected: expected_revision,
                        actual: actual_revision,
                    },
                ));
            }
        }

        if events.is_empty() {
            return Ok(Vec::new());
        }

        let mut stream_events = Vec::with_capacity(events.len());
        for new_event in events {
            let sequence = state.next_sequence;
            state.next_sequence += 1;

            let revision = actual_revision + stream_events.len() as u64 + 1;
            let envelope = EventEnvelope::new(
                EventId::new(),
                aggregate_id.clone(),
                A::aggregate_type(),
                revision,
                Some(sequence),
                new_event.event_type,
                new_event.event_version,
                new_event.payload,
                new_event.metadata,
                SystemTime::now(),
            );

            state.global.push(envelope.clone());
            stream_events.push(envelope);
        }

        state
            .streams
            .entry(aggregate_id.clone())
            .or_default()
            .extend(stream_events.clone());

        Ok(stream_events)
    }

    fn load_global_after(&self, sequence: Option<u64>) -> Result<EventStream<A>, Self::Error> {
        let state = self.state.read().map_err(|_| EventStoreError::Poisoned)?;
        Ok(state
            .global
            .iter()
            .filter(|event| match (sequence, event.sequence) {
                (Some(checkpoint), Some(current)) => current > checkpoint,
                (Some(_), None) => false,
                (None, _) => true,
            })
            .cloned()
            .collect())
    }
}
