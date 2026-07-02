use crate::aggregate::Aggregate;
use crate::error::{ConcurrencyError, EventStoreFailure, RepositoryError};
use crate::event::{ExpectedRevision, NewEvent};
use crate::event_store::EventStore;
use crate::idempotency::{IdempotencyKey, IdempotencyState, IdempotencyStore};
use crate::metadata::Metadata;
use crate::projection::CheckpointStore;
use crate::snapshot::{Snapshot, SnapshotStore};
use std::fmt::Debug;

/// Fluent aggregate test fixture.
///
/// The fixture exercises aggregate decision logic without requiring a
/// repository or event store.
///
/// # Example
///
/// ```rust
/// use ddd_cqrs_es::AggregateFixture;
/// # use ddd_cqrs_es::Aggregate;
/// # #[derive(Clone, Debug, PartialEq)]
/// # enum CounterEvent { Incremented(u32) }
/// # impl ddd_cqrs_es::DomainEvent for CounterEvent {
/// #     fn event_type(&self) -> &'static str { "incremented" }
/// # }
/// # #[derive(Clone, Debug, Default, PartialEq)]
/// # struct Counter { value: u32, revision: u64 }
/// # impl Aggregate for Counter {
/// #     type Id = String;
/// #     type Command = u32;
/// #     type Event = CounterEvent;
/// #     type Error = &'static str;
/// #     fn aggregate_type() -> &'static str { "counter" }
/// #     fn revision(&self) -> u64 { self.revision }
/// #     fn new() -> Self { Self::default() }
/// #     fn apply(&mut self, event: &Self::Event) {
/// #         match event { CounterEvent::Incremented(by) => self.value += by }
/// #         self.revision += 1;
/// #     }
/// #     fn handle(&self, command: Self::Command) -> Result<Vec<Self::Event>, Self::Error> {
/// #         if command == 0 { return Err("must be > 0"); }
/// #         Ok(vec![CounterEvent::Incremented(command)])
/// #     }
/// # }
///
/// let fixture = AggregateFixture::<Counter>::new();
///
/// fixture
///     .given(vec![CounterEvent::Incremented(5)])
///     .when(3)
///     .then_expect_events(vec![CounterEvent::Incremented(3)])
///     .then_expect_state(|state| {
///         assert_eq!(state.value, 8);
///     });
/// ```
#[derive(Clone, Debug)]
pub struct AggregateFixture<A>
where
    A: Aggregate,
{
    given: Vec<A::Event>,
}

/// Options for the reusable event-store contract test.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EventStoreContractOptions {
    expected_first_global_sequence: Option<u64>,
}

impl EventStoreContractOptions {
    /// Expects the first appended event to have the provided global sequence.
    pub fn with_expected_first_global_sequence(sequence: u64) -> Self {
        Self {
            expected_first_global_sequence: Some(sequence),
        }
    }

    /// Skips exact global sequence-number assertions.
    pub fn without_exact_global_sequence_assertions() -> Self {
        Self {
            expected_first_global_sequence: None,
        }
    }
}

impl Default for EventStoreContractOptions {
    fn default() -> Self {
        Self::with_expected_first_global_sequence(1)
    }
}

/// Runs the common event-store contract against a store implementation.
///
/// Adapter crates can call this from their own integration tests to verify
/// stream loading, optimistic concurrency, metadata preservation, revision
/// assignment, and global sequencing.
pub fn assert_event_store_contract<A, S>(
    store: S,
    aggregate_id: A::Id,
    first_event: A::Event,
    second_event: A::Event,
    options: EventStoreContractOptions,
) where
    A: Aggregate,
    A::Event: PartialEq + Debug,
    S: EventStore<A>,
    S::Error: EventStoreFailure + Debug,
{
    assert!(store.load(&aggregate_id).unwrap().is_empty());

    let first_metadata = Metadata::new().with_correlation_id("contract-1");
    let first = store
        .append(
            &aggregate_id,
            ExpectedRevision::NoStream,
            vec![NewEvent::new(first_event.clone(), first_metadata.clone())],
        )
        .unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].revision, 1);
    if let Some(expected) = options.expected_first_global_sequence {
        assert_eq!(first[0].sequence, Some(expected));
    }
    assert_eq!(first[0].metadata, first_metadata);

    let duplicate = store.append(
        &aggregate_id,
        ExpectedRevision::NoStream,
        vec![NewEvent::new(second_event.clone(), Metadata::default())],
    );
    let Err(duplicate) = duplicate else {
        panic!("expected NoStream append to fail after stream creation");
    };
    assert!(matches!(
        duplicate.into_repository_error::<()>(),
        RepositoryError::Concurrency(ConcurrencyError::StreamAlreadyExists)
    ));

    let second = store
        .append(
            &aggregate_id,
            ExpectedRevision::Exact(1),
            vec![NewEvent::new(second_event.clone(), Metadata::default())],
        )
        .unwrap();
    assert_eq!(second[0].revision, 2);
    if let Some(expected) = options.expected_first_global_sequence {
        assert_eq!(second[0].sequence, Some(expected + 1));
    }

    let stream = store.load(&aggregate_id).unwrap();
    assert_eq!(stream.len(), 2);
    assert_eq!(stream[0].payload, first_event);
    assert_eq!(stream[1].payload, second_event);

    if let Some(first_sequence) = first[0].sequence {
        let global = store.load_global_after(Some(first_sequence)).unwrap();
        assert_eq!(global.len(), 1);
        assert_eq!(global[0].revision, 2);
    }
}

/// Runs a focused global replay contract against a store implementation.
pub fn assert_event_store_global_replay_contract<A, S>(
    store: S,
    first_id: A::Id,
    second_id: A::Id,
    first_event: A::Event,
    second_event: A::Event,
) where
    A: Aggregate,
    A::Event: PartialEq + Debug,
    S: EventStore<A>,
    S::Error: Debug,
{
    store
        .append(
            &first_id,
            ExpectedRevision::NoStream,
            vec![NewEvent::new(first_event.clone(), Metadata::default())],
        )
        .unwrap();
    let first_global = store.load_global_after(None).unwrap();
    let first_sequence = first_global[0].sequence;

    store
        .append(
            &second_id,
            ExpectedRevision::NoStream,
            vec![NewEvent::new(second_event.clone(), Metadata::default())],
        )
        .unwrap();

    let all_global = store.load_global_after(None).unwrap();
    assert_eq!(all_global.len(), 2);
    assert_eq!(all_global[0].payload, first_event);
    assert_eq!(all_global[1].payload, second_event);

    if let Some(sequence) = first_sequence {
        let after_first = store.load_global_after(Some(sequence)).unwrap();
        assert_eq!(after_first.len(), 1);
        assert_eq!(after_first[0].payload, second_event);
    }
}

/// Runs a focused checkpoint-store contract.
pub fn assert_checkpoint_store_contract<C>(store: C, projection_name: &str)
where
    C: CheckpointStore,
    C::Error: Debug,
{
    assert_eq!(store.load_checkpoint(projection_name).unwrap(), None);
    store.save_checkpoint(projection_name, 42).unwrap();
    assert_eq!(store.load_checkpoint(projection_name).unwrap(), Some(42));
    store.save_checkpoint(projection_name, 100).unwrap();
    assert_eq!(store.load_checkpoint(projection_name).unwrap(), Some(100));
    store.save_checkpoint(projection_name, 90).unwrap();
    assert_eq!(store.load_checkpoint(projection_name).unwrap(), Some(100));
}

/// Runs a focused idempotency-store contract.
pub fn assert_idempotency_store_contract<S, V>(store: S, key: IdempotencyKey, value: V)
where
    S: IdempotencyStore<V>,
    S::Error: Debug,
    V: Clone + PartialEq + Debug,
{
    assert_eq!(store.load(&key).unwrap(), None);
    assert!(store.reserve(key.clone()).unwrap());
    assert_eq!(store.load(&key).unwrap(), Some(IdempotencyState::Pending));
    assert!(!store.reserve(key.clone()).unwrap());
    store.save(key.clone(), value.clone()).unwrap();
    assert_eq!(
        store.load(&key).unwrap(),
        Some(IdempotencyState::Complete(value))
    );
    assert!(!store.reserve(key.clone()).unwrap());
    store.remove(&key).unwrap();
    assert_eq!(store.load(&key).unwrap(), None);
}

/// Runs a focused snapshot-store contract.
pub fn assert_snapshot_store_contract<A, S>(store: S, aggregate_id: A::Id, older: A, newer: A)
where
    A: Aggregate + Clone + PartialEq + Debug,
    A::Id: Debug,
    S: SnapshotStore<A>,
    S::Error: Debug,
{
    assert_eq!(store.load_snapshot(&aggregate_id).unwrap(), None);

    store
        .save_snapshot(Snapshot::new(
            aggregate_id.clone(),
            1,
            older.clone(),
            Metadata::default(),
        ))
        .unwrap();
    assert_eq!(
        store
            .load_snapshot(&aggregate_id)
            .unwrap()
            .map(|snapshot| snapshot.state),
        Some(older)
    );

    store
        .save_snapshot(Snapshot::new(
            aggregate_id.clone(),
            2,
            newer.clone(),
            Metadata::default(),
        ))
        .unwrap();
    let loaded = store.load_snapshot(&aggregate_id).unwrap().unwrap();
    assert_eq!(loaded.revision, 2);
    assert_eq!(loaded.state, newer);
}

impl<A> AggregateFixture<A>
where
    A: Aggregate,
{
    /// Creates an empty fixture.
    pub fn new() -> Self {
        Self { given: Vec::new() }
    }

    /// Starts from an empty event history.
    pub fn given_no_events(mut self) -> Self {
        self.given.clear();
        self
    }

    /// Starts from a given event history.
    pub fn given(mut self, events: Vec<A::Event>) -> Self {
        self.given = events;
        self
    }

    /// Handles a command against replayed state.
    pub fn when(self, command: A::Command) -> AggregateFixtureResult<A> {
        let loaded = A::replay_raw_events_from_zero(&self.given);
        let result = loaded.state.handle(command);

        AggregateFixtureResult {
            state: loaded.state,
            revision: loaded.revision,
            result,
        }
    }
}

impl<A> Default for AggregateFixture<A>
where
    A: Aggregate,
{
    fn default() -> Self {
        Self::new()
    }
}

/// Result of executing a command in an aggregate fixture.
#[derive(Clone, Debug)]
pub struct AggregateFixtureResult<A>
where
    A: Aggregate,
{
    state: A,
    revision: u64,
    result: Result<Vec<A::Event>, A::Error>,
}

impl<A> AggregateFixtureResult<A>
where
    A: Aggregate,
{
    /// Asserts that command handling produced exactly the expected events.
    pub fn then_expect_events(self, expected: Vec<A::Event>) -> Self
    where
        A::Event: PartialEq + Debug,
        A::Error: Debug,
    {
        assert_eq!(self.result.as_ref().unwrap(), &expected);
        self
    }

    /// Asserts that command handling produced no events.
    pub fn then_expect_no_events(self) -> Self
    where
        A::Error: Debug,
    {
        assert!(self.result.as_ref().unwrap().is_empty());
        self
    }

    /// Asserts that command handling returned the expected domain error.
    pub fn then_expect_error(self, expected: A::Error) -> Self
    where
        A::Error: PartialEq + Debug,
    {
        match &self.result {
            Ok(_) => panic!("expected aggregate error, got events"),
            Err(error) => assert_eq!(error, &expected),
        }
        self
    }

    /// Asserts against aggregate state after successful command events apply.
    pub fn then_expect_state(self, assertion: impl FnOnce(&A)) -> Self
    where
        A: Clone,
        A::Error: Debug,
    {
        let events = self.result.as_ref().unwrap();
        let mut state = self.state.clone();
        for event in events {
            state.apply(event);
        }

        assertion(&state);
        self
    }

    /// Asserts the replayed revision before the command.
    pub fn then_expect_revision(self, expected: u64) -> Self {
        assert_eq!(self.revision, expected);
        self
    }
}
