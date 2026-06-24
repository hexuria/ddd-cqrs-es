//! Async extension traits and repository.

use crate::aggregate::{Aggregate, LoadedAggregate};
use crate::error::{EventStoreFailure, RepositoryError};
use crate::event::{ExpectedRevision, NewEvent};
use crate::event_store::EventStream;
use crate::metadata::Metadata;
use async_trait::async_trait;
use std::marker::PhantomData;

/// Async event persistence abstraction for one aggregate type.
#[async_trait]
pub trait AsyncEventStore<A>: Clone + Send + Sync + 'static
where
    A: Aggregate + Send + Sync,
{
    /// Store-specific error type.
    type Error: Send;

    /// Loads all events for one aggregate stream.
    async fn load(&self, aggregate_id: &A::Id) -> Result<EventStream<A>, Self::Error>;

    /// Loads events for one aggregate stream after the given revision.
    async fn load_after_revision(
        &self,
        aggregate_id: &A::Id,
        revision: u64,
    ) -> Result<EventStream<A>, Self::Error> {
        let events = self.load(aggregate_id).await?;
        Ok(events
            .into_iter()
            .filter(|event| event.revision > revision)
            .collect())
    }

    /// Appends events to one aggregate stream.
    async fn append(
        &self,
        aggregate_id: &A::Id,
        expected_revision: ExpectedRevision,
        events: Vec<NewEvent<A::Event>>,
    ) -> Result<EventStream<A>, Self::Error>;

    /// Loads globally ordered events after a global sequence number.
    async fn load_global_after(&self, sequence: Option<u64>)
        -> Result<EventStream<A>, Self::Error>;
}

/// Result type returned by async repository operations.
pub type AsyncRepositoryResult<A, S, T> =
    Result<T, RepositoryError<<A as Aggregate>::Error, <S as AsyncEventStore<A>>::Error>>;

/// Coordinates aggregate loading, command execution, and async event appending.
#[derive(Clone, Debug)]
pub struct AsyncRepository<A, S>
where
    A: Aggregate + Send + Sync,
    S: AsyncEventStore<A>,
{
    store: S,
    _marker: PhantomData<A>,
}

impl<A, S> AsyncRepository<A, S>
where
    A: Aggregate + Send + Sync,
    S: AsyncEventStore<A>,
    S::Error: EventStoreFailure,
{
    /// Creates an async repository backed by an async event store.
    pub fn new(store: S) -> Self {
        Self {
            store,
            _marker: PhantomData,
        }
    }

    /// Returns the backing event store.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Loads and replays one aggregate stream.
    pub async fn load(
        &self,
        aggregate_id: &A::Id,
    ) -> AsyncRepositoryResult<A, S, LoadedAggregate<A>> {
        let events = self
            .store
            .load(aggregate_id)
            .await
            .map_err(EventStoreFailure::into_repository_error)?;
        Ok(A::replay(&events))
    }

    /// Executes a command and returns committed event envelopes.
    pub async fn execute(
        &self,
        aggregate_id: &A::Id,
        command: A::Command,
        metadata: Metadata,
    ) -> AsyncRepositoryResult<A, S, EventStream<A>> {
        let loaded = self.load(aggregate_id).await?;
        let events = loaded
            .state
            .handle(command)
            .map_err(RepositoryError::Domain)?;
        let events = events
            .into_iter()
            .map(|event| NewEvent::new(event, metadata.clone()))
            .collect();

        self.store
            .append(
                aggregate_id,
                ExpectedRevision::Exact(loaded.revision),
                events,
            )
            .await
            .map_err(EventStoreFailure::into_repository_error)
    }
}

/// Dispatches commands asynchronously without requiring a specific framework.
#[async_trait]
pub trait AsyncCommandBus<C>: Send + Sync {
    /// Command result returned by the bus.
    type Output: Send;
    /// Error returned when dispatch fails.
    type Error: Send;

    /// Dispatches a command to its handler.
    async fn dispatch(&self, command: C) -> Result<Self::Output, Self::Error>;
}

/// Handles a command asynchronously in application code.
#[async_trait]
pub trait AsyncCommandHandler<C>: Send + Sync {
    /// Handler result.
    type Output: Send;
    /// Handler error.
    type Error: Send;

    /// Handles a command.
    async fn handle(&self, command: C) -> Result<Self::Output, Self::Error>;
}

/// Handles a query asynchronously on the read side of a CQRS application.
#[async_trait]
pub trait AsyncQueryHandler<Q>: Send + Sync {
    /// Query result.
    type Output: Send;
    /// Query error.
    type Error: Send;

    /// Executes a query.
    async fn handle(&self, query: Q) -> Result<Self::Output, Self::Error>;
}
