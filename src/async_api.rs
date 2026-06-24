//! Async extension traits and repository.

use crate::aggregate::{Aggregate, LoadedAggregate};
use crate::error::{EventStoreFailure, RepositoryError};
use crate::event::{ExpectedRevision, NewEvent};
use crate::event_store::EventStream;
use crate::metadata::Metadata;
use async_trait::async_trait;
use crate::idempotency::{IdempotencyKey, IdempotentRepositoryError};
use crate::snapshot::{Snapshot, SnapshotRepositoryError};
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
///
/// # Example
///
/// ```rust,no_run
/// use ddd_cqrs_es::{AsyncRepository, Metadata};
/// # use ddd_cqrs_es::{Aggregate, async_api::AsyncEventStore, event_store::EventStream, ExpectedRevision, NewEvent, INITIAL_REVISION};
/// # use async_trait::async_trait;
/// #
/// # #[derive(Clone)]
/// # struct DummyEvent;
/// # impl ddd_cqrs_es::DomainEvent for DummyEvent {
/// #     fn event_type(&self) -> &'static str { "dummy" }
/// # }
/// # #[derive(Debug, Clone, PartialEq)]
/// # struct DummyError;
/// # impl std::fmt::Display for DummyError {
/// #     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "dummy") }
/// # }
/// # impl std::error::Error for DummyError {}
/// # #[derive(Clone, Debug, PartialEq)]
/// # struct MyAggregate;
/// # impl Aggregate for MyAggregate {
/// #     type Id = String;
/// #     type Command = ();
/// #     type Event = DummyEvent;
/// #     type Error = DummyError;
/// #     fn aggregate_type() -> &'static str { "dummy" }
/// #     fn id(&self) -> Option<&Self::Id> { None }
/// #     fn revision(&self) -> u64 { 0 }
/// #     fn new() -> Self { MyAggregate }
/// #     fn apply(&mut self, _event: &Self::Event) {}
/// #     fn handle(&self, _command: Self::Command) -> Result<Vec<Self::Event>, Self::Error> { Ok(vec![]) }
/// # }
/// #
/// # #[derive(Clone)]
/// # struct MockStore;
/// # #[async_trait]
/// # impl AsyncEventStore<MyAggregate> for MockStore {
/// #     type Error = ddd_cqrs_es::error::EventStoreError;
/// #     async fn load(&self, _id: &String) -> Result<EventStream<MyAggregate>, Self::Error> { Ok(vec![]) }
/// #     async fn append(&self, _id: &String, _exp: ExpectedRevision, _evts: Vec<NewEvent<DummyEvent>>) -> Result<EventStream<MyAggregate>, Self::Error> { Ok(vec![]) }
/// #     async fn load_global_after(&self, _seq: Option<u64>) -> Result<EventStream<MyAggregate>, Self::Error> { Ok(vec![]) }
/// # }
///
/// # async fn doc_example() -> Result<(), Box<dyn std::error::Error>> {
/// let store = MockStore;
/// let repo = AsyncRepository::new(store);
///
/// let counter_id = "counter-1".to_owned();
/// let loaded = repo.load(&counter_id).await?;
/// assert_eq!(loaded.revision, 0);
/// # Ok(())
/// # }
/// ```
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

    /// Persists new events for a previously loaded aggregate.
    pub async fn save(
        &self,
        aggregate_id: &A::Id,
        loaded: &LoadedAggregate<A>,
        events: Vec<A::Event>,
        metadata: Metadata,
    ) -> AsyncRepositoryResult<A, S, EventStream<A>> {
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

    /// Persists explicitly named new events for a previously loaded aggregate.
    pub async fn save_new_events(
        &self,
        aggregate_id: &A::Id,
        loaded: &LoadedAggregate<A>,
        events: Vec<NewEvent<A::Event>>,
    ) -> AsyncRepositoryResult<A, S, EventStream<A>> {
        self.store
            .append(
                aggregate_id,
                ExpectedRevision::Exact(loaded.revision),
                events,
            )
            .await
            .map_err(EventStoreFailure::into_repository_error)
    }

    /// Executes a command and returns both committed events and updated state.
    pub async fn execute_returning_state(
        &self,
        aggregate_id: &A::Id,
        command: A::Command,
        metadata: Metadata,
    ) -> AsyncRepositoryResult<A, S, (LoadedAggregate<A>, EventStream<A>)> {
        let committed = self.execute(aggregate_id, command, metadata).await?;
        let loaded = self.load(aggregate_id).await?;
        Ok((loaded, committed))
    }

    /// Loads an aggregate using the latest snapshot, then replays events after
    /// the snapshot revision.
    pub async fn load_with_snapshot<SS>(
        &self,
        aggregate_id: &A::Id,
        snapshots: &SS,
    ) -> Result<
        LoadedAggregate<A>,
        SnapshotRepositoryError<A::Error, S::Error, SS::Error>,
    >
    where
        SS: AsyncSnapshotStore<A>,
    {
        let snapshot = snapshots
            .load_snapshot(aggregate_id)
            .await
            .map_err(SnapshotRepositoryError::Snapshot)?;

        let Some(snapshot) = snapshot else {
            let events = self
                .store
                .load(aggregate_id)
                .await
                .map_err(SnapshotRepositoryError::from_store_error)?;
            return Ok(A::replay(&events));
        };

        let events = self
            .store
            .load_after_revision(aggregate_id, snapshot.revision)
            .await
            .map_err(SnapshotRepositoryError::from_store_error)?;
        let mut state = snapshot.state;
        let mut revision = snapshot.revision;

        for envelope in events {
            state.apply(&envelope.payload);
            revision = envelope.revision;
        }

        Ok(LoadedAggregate::new(state, revision))
    }

    /// Executes a command using snapshot-aware loading before appending events.
    pub async fn execute_with_snapshot<SS>(
        &self,
        aggregate_id: &A::Id,
        command: A::Command,
        metadata: Metadata,
        snapshots: &SS,
    ) -> Result<
        EventStream<A>,
        SnapshotRepositoryError<A::Error, S::Error, SS::Error>,
    >
    where
        SS: AsyncSnapshotStore<A>,
    {
        let loaded = self.load_with_snapshot(aggregate_id, snapshots).await?;
        let events = loaded
            .state
            .handle(command)
            .map_err(SnapshotRepositoryError::Domain)?;
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
            .map_err(SnapshotRepositoryError::from_store_error)
    }

    /// Executes a command once for an idempotency key and returns the previous
    /// committed events when the same key is retried.
    pub async fn execute_idempotent<I>(
        &self,
        aggregate_id: &A::Id,
        command: A::Command,
        metadata: Metadata,
        idempotency_key: IdempotencyKey,
        idempotency_store: &I,
    ) -> Result<
        EventStream<A>,
        IdempotentRepositoryError<A::Error, S::Error, I::Error>,
    >
    where
        I: AsyncIdempotencyStore<EventStream<A>>,
    {
        if let Some(committed) = idempotency_store
            .load(&idempotency_key)
            .await
            .map_err(IdempotentRepositoryError::Idempotency)?
        {
            return Ok(committed);
        }

        let loaded = self.load(aggregate_id).await.map_err(|error| match error {
            RepositoryError::Domain(error) => IdempotentRepositoryError::Domain(error),
            RepositoryError::Concurrency(error) => IdempotentRepositoryError::Concurrency(error),
            RepositoryError::Store(error) => IdempotentRepositoryError::Store(error),
        })?;
        let events = loaded
            .state
            .handle(command)
            .map_err(IdempotentRepositoryError::Domain)?;
        let events = events
            .into_iter()
            .map(|event| NewEvent::new(event, metadata.clone()))
            .collect();
        let committed = self
            .store
            .append(
                aggregate_id,
                ExpectedRevision::Exact(loaded.revision),
                events,
            )
            .await
            .map_err(IdempotentRepositoryError::from_store_error)?;

        idempotency_store
            .save(idempotency_key, committed.clone())
            .await
            .map_err(IdempotentRepositoryError::Idempotency)?;

        Ok(committed)
    }
}


/// Async snapshot persistence abstraction.
#[async_trait]
pub trait AsyncSnapshotStore<A>: Clone + Send + Sync + 'static
where
    A: Aggregate + Send + Sync,
{
    /// Store-specific error type.
    type Error: Send;

    /// Loads the latest snapshot for an aggregate stream.
    async fn load_snapshot(&self, aggregate_id: &A::Id) -> Result<Option<Snapshot<A>>, Self::Error>;

    /// Saves a snapshot.
    async fn save_snapshot(&self, snapshot: Snapshot<A>) -> Result<(), Self::Error>;
}

/// Async idempotency persistence abstraction.
#[async_trait]
pub trait AsyncIdempotencyStore<V>: Clone + Send + Sync + 'static
where
    V: Clone + Send + Sync + 'static,
{
    /// Store-specific error type.
    type Error: Send;

    /// Loads a previous result for an idempotency key.
    async fn load(&self, key: &IdempotencyKey) -> Result<Option<V>, Self::Error>;

    /// Saves a result for an idempotency key.
    async fn save(&self, key: IdempotencyKey, value: V) -> Result<(), Self::Error>;
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
