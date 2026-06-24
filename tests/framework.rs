use ddd_cqrs_es::{
    assert_event_store_contract, Aggregate, AggregateFixture, ConcurrencyError, DomainEvent,
    EventStore, EventStoreError, ExpectedRevision, IdempotencyKey, InMemoryEventStore,
    InMemoryIdempotencyStore, InMemoryProjectionRunner, InMemorySnapshotStore, Metadata, NewEvent,
    Projection, Repository, RepositoryError, Snapshot, SnapshotStore,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug, PartialEq, Eq)]
enum CounterEvent {
    Created,
    Incremented { by: u64 },
}

impl DomainEvent for CounterEvent {
    fn event_type(&self) -> &'static str {
        match self {
            CounterEvent::Created => "counter_created",
            CounterEvent::Incremented { .. } => "counter_incremented",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CounterCommand {
    Create,
    Increment { by: u64 },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Counter {
    id: Option<String>,
    value: u64,
    revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CounterError {
    AlreadyCreated,
    NotCreated,
    InvalidIncrement,
}

impl Aggregate for Counter {
    type Id = String;
    type Command = CounterCommand;
    type Event = CounterEvent;
    type Error = CounterError;

    fn aggregate_type() -> &'static str {
        "counter"
    }

    fn id(&self) -> Option<&Self::Id> {
        self.id.as_ref()
    }

    fn revision(&self) -> u64 {
        self.revision
    }

    fn apply(&mut self, event: &Self::Event) {
        match event {
            CounterEvent::Created => {
                self.id = Some("fixture-counter".to_owned());
            }
            CounterEvent::Incremented { by } => {
                self.value += by;
            }
        }

        self.revision += 1;
    }

    fn handle(&self, command: Self::Command) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            CounterCommand::Create => {
                if self.id.is_some() {
                    return Err(CounterError::AlreadyCreated);
                }
                Ok(vec![CounterEvent::Created])
            }
            CounterCommand::Increment { by } => {
                if self.id.is_none() {
                    return Err(CounterError::NotCreated);
                }
                if by == 0 {
                    return Err(CounterError::InvalidIncrement);
                }
                Ok(vec![CounterEvent::Incremented { by }])
            }
        }
    }

    fn new() -> Self {
        Self::default()
    }
}

#[test]
fn repository_executes_commands_and_replays_state() {
    let store = InMemoryEventStore::<Counter>::new();
    let repo = Repository::new(store);
    let counter_id = "counter-1".to_owned();

    repo.execute(&counter_id, CounterCommand::Create, Metadata::default())
        .unwrap();
    repo.execute(
        &counter_id,
        CounterCommand::Increment { by: 2 },
        Metadata::default(),
    )
    .unwrap();
    repo.execute(
        &counter_id,
        CounterCommand::Increment { by: 3 },
        Metadata::default(),
    )
    .unwrap();

    let loaded = repo.load(&counter_id).unwrap();
    assert_eq!(loaded.state.value, 5);
    assert_eq!(loaded.revision, 3);
    assert_eq!(loaded.state.revision(), 3);
}

#[test]
fn event_store_rejects_wrong_expected_revision() {
    let store = InMemoryEventStore::<Counter>::new();
    let counter_id = "counter-1".to_owned();

    store
        .append(
            &counter_id,
            ExpectedRevision::NoStream,
            vec![NewEvent::new(CounterEvent::Created, Metadata::default())],
        )
        .unwrap();

    let result = store.append(
        &counter_id,
        ExpectedRevision::NoStream,
        vec![NewEvent::new(
            CounterEvent::Incremented { by: 1 },
            Metadata::default(),
        )],
    );

    assert!(matches!(
        result,
        Err(EventStoreError::Concurrency(
            ConcurrencyError::StreamAlreadyExists
        ))
    ));
}

#[test]
fn in_memory_store_passes_reusable_contract() {
    assert_event_store_contract::<Counter, _>(
        InMemoryEventStore::<Counter>::new(),
        "contract-counter".to_owned(),
        CounterEvent::Created,
        CounterEvent::Incremented { by: 1 },
    );
}

#[test]
fn domain_errors_are_not_persisted() {
    let store = InMemoryEventStore::<Counter>::new();
    let repo = Repository::new(store.clone());
    let counter_id = "counter-1".to_owned();

    let result = repo.execute(
        &counter_id,
        CounterCommand::Increment { by: 1 },
        Metadata::default(),
    );

    assert!(matches!(result, Err(RepositoryError::Domain(_))));
    let events = store.load(&counter_id).unwrap();
    assert!(events.is_empty());
}

#[test]
fn metadata_and_global_sequence_are_preserved() {
    let store = InMemoryEventStore::<Counter>::new();
    let repo = Repository::new(store.clone());
    let counter_id = "counter-1".to_owned();
    let metadata = Metadata::new()
        .with_actor_id("user-1")
        .with_correlation_id("corr-1")
        .with_header("source", "test");

    let committed = repo
        .execute(&counter_id, CounterCommand::Create, metadata.clone())
        .unwrap();

    assert_eq!(committed[0].sequence, Some(1));
    assert_eq!(committed[0].revision, 1);
    assert_eq!(committed[0].event_type, "counter_created");
    assert_eq!(committed[0].metadata, metadata);
    assert_eq!(committed[0].aggregate_type, "counter");

    let global = store.load_global_after(None).unwrap();
    assert_eq!(global.len(), 1);
    assert_eq!(global[0].sequence, Some(1));
}

#[cfg(feature = "uuid")]
#[test]
fn event_ids_use_uuid_when_feature_is_enabled() {
    let store = InMemoryEventStore::<Counter>::new();
    let repo = Repository::new(store);
    let counter_id = "counter-1".to_owned();

    let committed = repo
        .execute(&counter_id, CounterCommand::Create, Metadata::default())
        .unwrap();

    assert!(uuid::Uuid::parse_str(committed[0].event_id.as_str()).is_ok());
}

#[cfg(feature = "json")]
#[test]
fn event_envelopes_round_trip_through_json() {
    let store = InMemoryEventStore::<Counter>::new();
    let repo = Repository::new(store);
    let counter_id = "counter-1".to_owned();

    let committed = repo
        .execute(&counter_id, CounterCommand::Create, Metadata::default())
        .unwrap();
    let json = committed[0].to_json().unwrap();
    let restored = ddd_cqrs_es::EventEnvelope::<CounterEvent, String>::from_json(&json).unwrap();

    assert_eq!(restored, committed[0]);
}

#[test]
fn repository_surfaces_concurrency_on_main_api() {
    let store = InMemoryEventStore::<Counter>::new();
    let repo = Repository::new(store.clone());
    let counter_id = "counter-1".to_owned();
    let stale = repo.load(&counter_id).unwrap();

    store
        .append(
            &counter_id,
            ExpectedRevision::Any,
            vec![NewEvent::new(CounterEvent::Created, Metadata::default())],
        )
        .unwrap();

    let error = repo
        .save(
            &counter_id,
            &stale,
            vec![CounterEvent::Incremented { by: 1 }],
            Metadata::default(),
        )
        .unwrap_err();

    assert!(matches!(
        error,
        RepositoryError::Concurrency(ConcurrencyError::WrongExpectedRevision {
            expected: ExpectedRevision::Exact(0),
            actual: 1,
        })
    ));
}

#[test]
fn exact_revision_conflicts_are_first_class() {
    let store = InMemoryEventStore::<Counter>::new();
    let counter_id = "counter-1".to_owned();

    store
        .append(
            &counter_id,
            ExpectedRevision::Any,
            vec![NewEvent::new(CounterEvent::Created, Metadata::default())],
        )
        .unwrap();

    let error = store
        .append(
            &counter_id,
            ExpectedRevision::Exact(0),
            vec![NewEvent::new(
                CounterEvent::Incremented { by: 1 },
                Metadata::default(),
            )],
        )
        .unwrap_err();

    assert!(matches!(
        error,
        EventStoreError::Concurrency(ConcurrencyError::WrongExpectedRevision {
            expected: ExpectedRevision::Exact(0),
            actual: 1,
        })
    ));
}

#[test]
fn concurrent_appends_to_same_stream_preserve_one_winner_per_revision() {
    let store = Arc::new(InMemoryEventStore::<Counter>::new());
    let counter_id = "counter-1".to_owned();

    let handles = (0..8)
        .map(|_| {
            let store = Arc::clone(&store);
            let counter_id = counter_id.clone();
            thread::spawn(move || {
                store.append(
                    &counter_id,
                    ExpectedRevision::NoStream,
                    vec![NewEvent::new(CounterEvent::Created, Metadata::default())],
                )
            })
        })
        .collect::<Vec<_>>();

    let successes = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .filter(Result::is_ok)
        .count();

    assert_eq!(successes, 1);
    assert_eq!(store.load(&counter_id).unwrap().len(), 1);
}

#[test]
fn aggregate_fixture_asserts_events_errors_state_and_revision() {
    AggregateFixture::<Counter>::new()
        .given_no_events()
        .when(CounterCommand::Create)
        .then_expect_events(vec![CounterEvent::Created])
        .then_expect_revision(0);

    AggregateFixture::<Counter>::new()
        .given(vec![CounterEvent::Created])
        .when(CounterCommand::Increment { by: 2 })
        .then_expect_events(vec![CounterEvent::Incremented { by: 2 }])
        .then_expect_state(|counter| {
            assert_eq!(counter.value, 2);
            assert_eq!(counter.revision(), 2);
        });

    AggregateFixture::<Counter>::new()
        .given(vec![CounterEvent::Created])
        .when(CounterCommand::Increment { by: 0 })
        .then_expect_error(CounterError::InvalidIncrement);
}

#[derive(Default)]
struct CounterProjection {
    values: HashMap<String, u64>,
}

impl Projection<CounterEvent, String> for CounterProjection {
    type Error = ();

    fn name(&self) -> &'static str {
        "counter_projection"
    }

    fn apply(
        &mut self,
        event: &ddd_cqrs_es::EventEnvelope<CounterEvent, String>,
    ) -> Result<(), Self::Error> {
        let value = self.values.entry(event.aggregate_id.clone()).or_default();
        match event.payload {
            CounterEvent::Created => {}
            CounterEvent::Incremented { by } => *value += by,
        }
        Ok(())
    }
}

#[test]
fn projection_runner_resumes_from_checkpoint() {
    let store = InMemoryEventStore::<Counter>::new();
    let repo = Repository::new(store.clone());
    let counter_id = "counter-1".to_owned();

    repo.execute(&counter_id, CounterCommand::Create, Metadata::default())
        .unwrap();

    let mut runner = InMemoryProjectionRunner::new(CounterProjection::default());
    assert_eq!(runner.run::<Counter, _>(&store).unwrap(), 1);
    assert_eq!(runner.checkpoint(), Some(1));

    repo.execute(
        &counter_id,
        CounterCommand::Increment { by: 4 },
        Metadata::default(),
    )
    .unwrap();

    assert_eq!(runner.run::<Counter, _>(&store).unwrap(), 1);
    assert_eq!(runner.checkpoint(), Some(2));
    assert_eq!(runner.projection().values[&counter_id], 4);
}

#[test]
fn repository_loads_from_snapshot_and_replays_later_events() {
    let store = InMemoryEventStore::<Counter>::new();
    let snapshots = InMemorySnapshotStore::<Counter>::new();
    let repo = Repository::new(store.clone());
    let counter_id = "counter-1".to_owned();

    repo.execute(&counter_id, CounterCommand::Create, Metadata::default())
        .unwrap();
    repo.execute(
        &counter_id,
        CounterCommand::Increment { by: 2 },
        Metadata::default(),
    )
    .unwrap();

    let loaded = repo.load(&counter_id).unwrap();
    snapshots
        .save_snapshot(Snapshot::new(
            counter_id.clone(),
            loaded.revision,
            loaded.state,
            Metadata::default(),
        ))
        .unwrap();

    repo.execute(
        &counter_id,
        CounterCommand::Increment { by: 3 },
        Metadata::default(),
    )
    .unwrap();

    let loaded = repo.load_with_snapshot(&counter_id, &snapshots).unwrap();
    assert_eq!(loaded.state.value, 5);
    assert_eq!(loaded.revision, 3);
}

#[test]
fn repository_returns_previous_result_for_idempotent_retry() {
    let store = InMemoryEventStore::<Counter>::new();
    let idempotency = InMemoryIdempotencyStore::new();
    let repo = Repository::new(store.clone());
    let counter_id = "counter-1".to_owned();
    let key = IdempotencyKey::new("request-1");

    let first = repo
        .execute_idempotent(
            &counter_id,
            CounterCommand::Create,
            Metadata::default(),
            key.clone(),
            &idempotency,
        )
        .unwrap();
    let retry = repo
        .execute_idempotent(
            &counter_id,
            CounterCommand::Increment { by: 9 },
            Metadata::default(),
            key,
            &idempotency,
        )
        .unwrap();

    assert_eq!(first, retry);
    assert_eq!(store.load(&counter_id).unwrap().len(), 1);
}
