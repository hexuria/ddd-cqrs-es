use crate::aggregate::{Aggregate, LoadedAggregate};
use crate::error::{EventStoreError, EventStoreFailure, RepositoryError};
use crate::event::{EventEnvelope, ExpectedRevision, NewEvent};
use crate::event_store::{
    AtomicIdempotentEventStore, EventStore, EventStream, IdempotentAppendError,
};
use crate::idempotency::{
    IdempotencyKey, IdempotencyState, IdempotencyStore, IdempotencyWaitConfig,
    IdempotentRepositoryError,
};
use crate::metadata::Metadata;
use crate::snapshot::{SnapshotRepositoryError, SnapshotStore};
use std::marker::PhantomData;

/// Result type returned by repository operations.
pub type RepositoryResult<A, S, T> =
    Result<T, RepositoryError<<A as Aggregate>::Error, <S as EventStore<A>>::Error>>;

/// Committed events returned by repository command execution.
pub type CommittedEvents<A> = EventStream<A>;

/// Updated aggregate state plus committed events.
pub type ExecutionOutcome<A> = (LoadedAggregate<A>, CommittedEvents<A>);

/// Result type returned by snapshot-aware repository operations.
pub type SnapshotRepositoryResult<A, S, SS, T> = Result<
    T,
    SnapshotRepositoryError<
        <A as Aggregate>::Error,
        <S as EventStore<A>>::Error,
        <SS as SnapshotStore<A>>::Error,
    >,
>;

/// Result type returned by idempotent repository operations.
pub type IdempotentRepositoryResult<A, S, I, T> = Result<
    T,
    IdempotentRepositoryError<
        <A as Aggregate>::Error,
        <S as EventStore<A>>::Error,
        <I as IdempotencyStore<CommittedEvents<A>>>::Error,
    >,
>;

/// Result type returned by atomic idempotent repository operations.
pub type AtomicIdempotentRepositoryResult<A, S, T> = Result<
    T,
    IdempotentRepositoryError<
        <A as Aggregate>::Error,
        <S as EventStore<A>>::Error,
        <S as EventStore<A>>::Error,
    >,
>;

/// Coordinates aggregate loading, command execution, and event appending.
///
/// # Example
///
/// ```rust
/// use ddd_cqrs_es::{Repository, InMemoryEventStore, Metadata};
/// # use ddd_cqrs_es::{Aggregate, DomainEvent};
/// #
/// # #[derive(Clone)]
/// # enum CounterEvent { Created }
/// # impl DomainEvent for CounterEvent {
/// #     fn event_type(&self) -> &'static str { "counter_created" }
/// # }
/// # struct CounterAggregate { revision: u64 }
/// # impl Aggregate for CounterAggregate {
/// #     type Id = String;
/// #     type Command = ();
/// #     type Event = CounterEvent;
/// #     type Error = ();
/// #     fn aggregate_type() -> &'static str { "counter" }
/// #     fn revision(&self) -> u64 { self.revision }
/// #     fn new() -> Self { CounterAggregate { revision: 0 } }
/// #     fn apply(&mut self, _event: &Self::Event) { self.revision += 1; }
/// #     fn handle(&self, _command: Self::Command) -> Result<Vec<Self::Event>, Self::Error> { Ok(vec![CounterEvent::Created]) }
/// # }
///
/// let store = InMemoryEventStore::<CounterAggregate>::new();
/// let repo = Repository::new(store);
///
/// let aggregate_id = "counter-1".to_string();
/// repo.execute(&aggregate_id, (), Metadata::default()).unwrap();
///
/// let loaded = repo.load(&aggregate_id).unwrap();
/// assert_eq!(loaded.revision, 1);
/// ```
#[derive(Clone, Debug)]
pub struct Repository<A, S>
where
    A: Aggregate,
    S: EventStore<A>,
    S::Error: EventStoreFailure,
{
    store: S,
    _marker: PhantomData<A>,
}

impl<A, S> Repository<A, S>
where
    A: Aggregate,
    S: EventStore<A>,
    S::Error: EventStoreFailure,
{
    /// Creates a repository backed by an event store.
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
    pub fn load(&self, aggregate_id: &A::Id) -> RepositoryResult<A, S, LoadedAggregate<A>> {
        #[cfg(feature = "tracing")]
        let _span =
            tracing::debug_span!("aggregate.load", aggregate_type = A::aggregate_type()).entered();

        let events = self
            .store
            .load(aggregate_id)
            .map_err(EventStoreFailure::into_repository_error)?;
        Ok(A::replay(&events))
    }

    /// Persists new events for a previously loaded aggregate.
    pub fn save(
        &self,
        aggregate_id: &A::Id,
        loaded: &LoadedAggregate<A>,
        events: Vec<A::Event>,
        metadata: Metadata,
    ) -> RepositoryResult<A, S, CommittedEvents<A>> {
        #[cfg(feature = "tracing")]
        let _span = tracing::debug_span!(
            "event_store.append",
            aggregate_type = A::aggregate_type(),
            expected_revision = loaded.revision,
            event_count = events.len()
        )
        .entered();

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
            .map_err(EventStoreFailure::into_repository_error)
    }

    /// Persists explicitly named new events for a previously loaded aggregate.
    pub fn save_new_events(
        &self,
        aggregate_id: &A::Id,
        loaded: &LoadedAggregate<A>,
        events: Vec<NewEvent<A::Event>>,
    ) -> RepositoryResult<A, S, CommittedEvents<A>> {
        self.store
            .append(
                aggregate_id,
                ExpectedRevision::Exact(loaded.revision),
                events,
            )
            .map_err(EventStoreFailure::into_repository_error)
    }

    /// Executes a command and returns committed event envelopes.
    pub fn execute(
        &self,
        aggregate_id: &A::Id,
        command: A::Command,
        metadata: Metadata,
    ) -> RepositoryResult<A, S, CommittedEvents<A>> {
        #[cfg(feature = "tracing")]
        let _span =
            tracing::debug_span!("repository.execute", aggregate_type = A::aggregate_type())
                .entered();

        let loaded = self.load(aggregate_id)?;
        let events = loaded
            .state
            .handle(command)
            .map_err(RepositoryError::Domain)?;
        #[cfg(feature = "tracing")]
        let event_count = events.len();

        let committed = self.save(aggregate_id, &loaded, events, metadata)?;

        #[cfg(feature = "tracing")]
        tracing::debug!(
            aggregate_type = A::aggregate_type(),
            expected_revision = loaded.revision,
            event_count,
            "committed aggregate events"
        );

        Ok(committed)
    }

    /// Executes a command and returns both committed events and updated state.
    pub fn execute_returning_state(
        &self,
        aggregate_id: &A::Id,
        command: A::Command,
        metadata: Metadata,
    ) -> RepositoryResult<A, S, ExecutionOutcome<A>> {
        #[cfg(feature = "tracing")]
        let _span = tracing::debug_span!(
            "repository.execute_returning_state",
            aggregate_type = A::aggregate_type()
        )
        .entered();

        let loaded = self.load(aggregate_id)?;
        let events = loaded
            .state
            .handle(command)
            .map_err(RepositoryError::Domain)?;
        #[cfg(feature = "tracing")]
        let event_count = events.len();

        let committed = self.save(aggregate_id, &loaded, events, metadata)?;
        let updated = apply_committed_events(loaded, &committed);

        #[cfg(feature = "tracing")]
        tracing::debug!(
            aggregate_type = A::aggregate_type(),
            expected_revision = updated.revision.saturating_sub(event_count as u64),
            event_count,
            "committed aggregate events and rebuilt state"
        );

        Ok((updated, committed))
    }

    /// Loads an aggregate using the latest snapshot, then replays events after
    /// the snapshot revision.
    pub fn load_with_snapshot<SS>(
        &self,
        aggregate_id: &A::Id,
        snapshots: &SS,
    ) -> SnapshotRepositoryResult<A, S, SS, LoadedAggregate<A>>
    where
        SS: SnapshotStore<A>,
    {
        #[cfg(feature = "tracing")]
        let _span = tracing::debug_span!(
            "snapshot.load_with_replay",
            aggregate_type = A::aggregate_type()
        )
        .entered();

        let snapshot = snapshots
            .load_snapshot(aggregate_id)
            .map_err(SnapshotRepositoryError::Snapshot)?;

        let Some(snapshot) = snapshot else {
            let events = self
                .store
                .load(aggregate_id)
                .map_err(SnapshotRepositoryError::from_store_error)?;
            return Ok(A::replay(&events));
        };

        let events = self
            .store
            .load_after_revision(aggregate_id, snapshot.revision)
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
    pub fn execute_with_snapshot<SS>(
        &self,
        aggregate_id: &A::Id,
        command: A::Command,
        metadata: Metadata,
        snapshots: &SS,
    ) -> SnapshotRepositoryResult<A, S, SS, CommittedEvents<A>>
    where
        SS: SnapshotStore<A>,
    {
        let loaded = self.load_with_snapshot(aggregate_id, snapshots)?;
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
            .map_err(SnapshotRepositoryError::from_store_error)
    }

    /// Executes a command once for an idempotency key and returns the previous
    /// committed events when the same key is retried.
    ///
    /// # Reliability
    ///
    /// This generic implementation coordinates the event store and idempotency
    /// store through separate calls. It prevents concurrent duplicate execution,
    /// but it is not a crash-atomic transaction across both stores. If a process
    /// stops after appending events and before saving the completed idempotency
    /// result, the key can remain pending until application recovery removes or
    /// completes it. Production systems that require exactly-once crash recovery
    /// should use a transaction-aware adapter, outbox/recovery worker, or a
    /// pending-key timeout policy.
    pub fn execute_idempotent<I>(
        &self,
        aggregate_id: &A::Id,
        command: A::Command,
        metadata: Metadata,
        idempotency_key: IdempotencyKey,
        idempotency_store: &I,
    ) -> IdempotentRepositoryResult<A, S, I, CommittedEvents<A>>
    where
        I: IdempotencyStore<CommittedEvents<A>>,
    {
        self.execute_idempotent_with_wait_config(
            aggregate_id,
            command,
            metadata,
            idempotency_key,
            idempotency_store,
            IdempotencyWaitConfig::default(),
        )
    }

    /// Executes a command once for an idempotency key using explicit pending-key wait limits.
    pub fn execute_idempotent_with_wait_config<I>(
        &self,
        aggregate_id: &A::Id,
        command: A::Command,
        metadata: Metadata,
        idempotency_key: IdempotencyKey,
        idempotency_store: &I,
        wait_config: IdempotencyWaitConfig,
    ) -> IdempotentRepositoryResult<A, S, I, CommittedEvents<A>>
    where
        I: IdempotencyStore<CommittedEvents<A>>,
    {
        let started = std::time::Instant::now();

        loop {
            match idempotency_store
                .load(&idempotency_key)
                .map_err(IdempotentRepositoryError::Idempotency)?
            {
                Some(IdempotencyState::Complete(committed)) => {
                    return Ok(committed);
                }
                Some(IdempotencyState::Pending) => {
                    let Some(delay) = wait_config.next_delay(started.elapsed()) else {
                        return Err(IdempotentRepositoryError::IdempotencyPendingTimeout {
                            key: idempotency_key,
                            waited: started.elapsed(),
                        });
                    };
                    std::thread::sleep(delay);
                    continue;
                }
                None => {
                    if idempotency_store
                        .reserve(idempotency_key.clone())
                        .map_err(IdempotentRepositoryError::Idempotency)?
                    {
                        break;
                    }
                    let Some(delay) = wait_config.next_delay(started.elapsed()) else {
                        return Err(IdempotentRepositoryError::IdempotencyPendingTimeout {
                            key: idempotency_key,
                            waited: started.elapsed(),
                        });
                    };
                    std::thread::sleep(delay);
                }
            }
        }

        let committed =
            match (|| -> Result<CommittedEvents<A>, RepositoryError<A::Error, S::Error>> {
                let loaded = self.load(aggregate_id)?;
                let events = loaded
                    .state
                    .handle(command)
                    .map_err(RepositoryError::Domain)?;
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
                    .map_err(EventStoreFailure::into_repository_error)?;
                Ok(committed)
            })() {
                Ok(committed) => committed,
                Err(err) => {
                    let _ = idempotency_store.remove(&idempotency_key);
                    return Err(match err {
                        RepositoryError::Domain(error) => IdempotentRepositoryError::Domain(error),
                        RepositoryError::Concurrency(error) => {
                            IdempotentRepositoryError::Concurrency(error)
                        }
                        RepositoryError::Store(error) => IdempotentRepositoryError::Store(error),
                    });
                }
            };

        idempotency_store
            .save(idempotency_key, committed.clone())
            .map_err(IdempotentRepositoryError::Idempotency)?;

        Ok(committed)
    }

    /// Executes a command once using an event-store-native transaction-aware
    /// idempotency record.
    ///
    /// Unlike [`Self::execute_idempotent`], this path requires the event store
    /// to reserve the idempotency key, append events, and save the completed
    /// result in one backing-store transaction. Use this for SQL-backed
    /// production workflows that require crash-atomic retry recovery.
    pub fn execute_idempotent_atomic(
        &self,
        aggregate_id: &A::Id,
        command: A::Command,
        metadata: Metadata,
        idempotency_key: IdempotencyKey,
    ) -> AtomicIdempotentRepositoryResult<A, S, CommittedEvents<A>>
    where
        S: AtomicIdempotentEventStore<A>,
        A::Event: Clone,
    {
        self.execute_idempotent_atomic_with_wait_config(
            aggregate_id,
            command,
            metadata,
            idempotency_key,
            IdempotencyWaitConfig::default(),
        )
    }

    /// Executes a command once using a transaction-aware idempotency record and
    /// explicit pending-key wait limits.
    pub fn execute_idempotent_atomic_with_wait_config(
        &self,
        aggregate_id: &A::Id,
        command: A::Command,
        metadata: Metadata,
        idempotency_key: IdempotencyKey,
        wait_config: IdempotencyWaitConfig,
    ) -> AtomicIdempotentRepositoryResult<A, S, CommittedEvents<A>>
    where
        S: AtomicIdempotentEventStore<A>,
        A::Event: Clone,
    {
        #[cfg(feature = "tracing")]
        let _span = tracing::debug_span!(
            "repository.execute_idempotent_atomic",
            aggregate_type = A::aggregate_type()
        )
        .entered();

        let loaded = self.load(aggregate_id).map_err(|error| match error {
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
            .collect::<Vec<_>>();
        let expected_revision = ExpectedRevision::Exact(loaded.revision);
        let started = std::time::Instant::now();

        loop {
            match self.store.append_idempotent(
                idempotency_key.clone(),
                aggregate_id,
                expected_revision,
                events.clone(),
            ) {
                Ok(committed) => return Ok(committed),
                Err(IdempotentAppendError::Pending { .. }) => {
                    let Some(delay) = wait_config.next_delay(started.elapsed()) else {
                        return Err(IdempotentRepositoryError::IdempotencyPendingTimeout {
                            key: idempotency_key,
                            waited: started.elapsed(),
                        });
                    };
                    std::thread::sleep(delay);
                }
                Err(IdempotentAppendError::Store(error)) => {
                    return Err(IdempotentRepositoryError::from_store_error(error));
                }
            }
        }
    }
}

fn apply_committed_events<A>(
    mut loaded: LoadedAggregate<A>,
    committed: &[EventEnvelope<A::Event, A::Id>],
) -> LoadedAggregate<A>
where
    A: Aggregate,
{
    for envelope in committed {
        loaded.state.apply(&envelope.payload);
        loaded.revision = envelope.revision;
    }

    loaded
}

impl<A, S> Repository<A, S>
where
    A: Aggregate,
    S: EventStore<A, Error = EventStoreError>,
{
    /// Executes a command and maps standard event store concurrency errors to
    /// [`RepositoryError::Concurrency`].
    pub fn execute_standard(
        &self,
        aggregate_id: &A::Id,
        command: A::Command,
        metadata: Metadata,
    ) -> Result<CommittedEvents<A>, RepositoryError<A::Error, EventStoreError>> {
        self.execute(aggregate_id, command, metadata)
    }
}
