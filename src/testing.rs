use crate::aggregate::Aggregate;
use crate::error::{ConcurrencyError, EventStoreError};
use crate::event::{ExpectedRevision, NewEvent};
use crate::event_store::EventStore;
use crate::metadata::Metadata;
use std::fmt::Debug;

/// Fluent aggregate test fixture.
///
/// The fixture exercises aggregate decision logic without requiring a
/// repository or event store.
#[derive(Clone, Debug)]
pub struct AggregateFixture<A>
where
    A: Aggregate,
{
    given: Vec<A::Event>,
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
) where
    A: Aggregate,
    A::Event: PartialEq + Debug,
    S: EventStore<A, Error = EventStoreError>,
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
    assert_eq!(first[0].sequence, Some(1));
    assert_eq!(first[0].metadata, first_metadata);

    let duplicate = store.append(
        &aggregate_id,
        ExpectedRevision::NoStream,
        vec![NewEvent::new(second_event.clone(), Metadata::default())],
    );
    let Err(duplicate) = duplicate else {
        panic!("expected NoStream append to fail after stream creation");
    };
    assert_eq!(
        duplicate,
        EventStoreError::Concurrency(ConcurrencyError::StreamAlreadyExists)
    );

    let second = store
        .append(
            &aggregate_id,
            ExpectedRevision::Exact(1),
            vec![NewEvent::new(second_event.clone(), Metadata::default())],
        )
        .unwrap();
    assert_eq!(second[0].revision, 2);
    assert_eq!(second[0].sequence, Some(2));

    let stream = store.load(&aggregate_id).unwrap();
    assert_eq!(stream.len(), 2);
    assert_eq!(stream[0].payload, first_event);
    assert_eq!(stream[1].payload, second_event);

    let global = store.load_global_after(Some(1)).unwrap();
    assert_eq!(global.len(), 1);
    assert_eq!(global[0].revision, 2);
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
        let loaded = A::replay_events(&self.given);
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
