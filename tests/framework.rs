use ddd_cqrs_es::{
    assert_event_store_contract, Aggregate, AggregateFixture, ConcurrencyError, DomainEvent,
    EventStore, EventStoreError, ExpectedRevision, IdempotencyKey, IdempotencyState,
    IdempotencyStore, InMemoryEventStore, InMemoryIdempotencyStore, InMemoryProjectionRunner,
    InMemorySnapshotStore, Metadata, NewEvent, Projection, Repository, RepositoryError, Snapshot,
    SnapshotStore,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

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

#[cfg(any(feature = "sqlite", feature = "postgres"))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct StoredIdempotencyResult {
    value: u64,
    label: String,
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
fn assert_sql_idempotency_store_contract<S>(store: S)
where
    S: IdempotencyStore<StoredIdempotencyResult, Error = EventStoreError>
        + Clone
        + Send
        + Sync
        + 'static,
{
    let missing_key = IdempotencyKey::new("sql-idempotency-missing");
    assert_eq!(store.load(&missing_key).unwrap(), None);

    let key = IdempotencyKey::new("sql-idempotency-complete");
    assert!(store.reserve(key.clone()).unwrap());
    assert_eq!(store.load(&key).unwrap(), Some(IdempotencyState::Pending));
    assert!(!store.reserve(key.clone()).unwrap());

    let value = StoredIdempotencyResult {
        value: 42,
        label: "json-round-trip".to_owned(),
    };
    store.save(key.clone(), value.clone()).unwrap();
    assert_eq!(
        store.load(&key).unwrap(),
        Some(IdempotencyState::Complete(value.clone()))
    );
    assert!(!store.reserve(key.clone()).unwrap());

    let failed_key = IdempotencyKey::new("sql-idempotency-failed");
    assert!(store.reserve(failed_key.clone()).unwrap());
    store.remove(&failed_key).unwrap();
    assert_eq!(store.load(&failed_key).unwrap(), None);
    assert!(store.reserve(failed_key.clone()).unwrap());

    let concurrent_key = IdempotencyKey::new("sql-idempotency-concurrent");
    let store = Arc::new(store);
    let handles = (0..10)
        .map(|_| {
            let store = Arc::clone(&store);
            let key = concurrent_key.clone();
            thread::spawn(move || store.reserve(key).unwrap())
        })
        .collect::<Vec<_>>();
    let winners = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .filter(|reserved| *reserved)
        .count();

    assert_eq!(winners, 1);
    assert_eq!(
        store.load(&concurrent_key).unwrap(),
        Some(IdempotencyState::Pending)
    );
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

#[cfg(feature = "sqlite")]
#[test]
fn sqlite_store_passes_reusable_contract() {
    assert_event_store_contract::<Counter, _>(
        ddd_cqrs_es::SqliteEventStore::<Counter>::in_memory().unwrap(),
        "sqlite-contract-counter".to_owned(),
        CounterEvent::Created,
        CounterEvent::Incremented { by: 1 },
    );
}

#[cfg(feature = "sqlite")]
#[test]
fn sqlite_idempotency_store_passes_contract() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    let store = ddd_cqrs_es::SqliteIdempotencyStore::new(connection).unwrap();
    assert_sql_idempotency_store_contract(store);
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_store_passes_reusable_contract_when_url_is_provided() {
    let Ok(database_url) = std::env::var("DDD_CQRS_ES_POSTGRES_URL") else {
        eprintln!("skipping live Postgres contract test: DDD_CQRS_ES_POSTGRES_URL is not set");
        return;
    };
    let table_name = format!(
        "events_live_contract_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );

    let store = ddd_cqrs_es::PostgresEventStore::<Counter>::connect_with_table_name(
        &database_url,
        table_name,
    )
    .unwrap();
    store.initialize_schema().unwrap();

    assert_event_store_contract::<Counter, _>(
        store,
        "postgres-contract-counter".to_owned(),
        CounterEvent::Created,
        CounterEvent::Incremented { by: 1 },
    );
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_idempotency_store_passes_contract_when_url_is_provided() {
    let Ok(database_url) = std::env::var("DDD_CQRS_ES_POSTGRES_URL") else {
        eprintln!("skipping live Postgres idempotency test: DDD_CQRS_ES_POSTGRES_URL is not set");
        return;
    };
    let table_name = format!(
        "idempotency_live_contract_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );

    let client = postgres::Client::connect(&database_url, postgres::NoTls).unwrap();
    let store =
        ddd_cqrs_es::PostgresIdempotencyStore::with_table_name(client, table_name.clone()).unwrap();

    assert_sql_idempotency_store_contract(store.clone());
    drop(store);

    let mut cleanup = postgres::Client::connect(&database_url, postgres::NoTls).unwrap();
    cleanup
        .batch_execute(&format!("DROP TABLE IF EXISTS {table_name};"))
        .unwrap();
}

#[cfg(feature = "json")]
#[test]
fn postgres_interpolation_escapes_strings_and_rejects_bad_parameter_indexes() {
    let sql = ddd_cqrs_es::adapters::interpolate_query(
        "SELECT $1, $2, $3",
        &[
            serde_json::json!("O'Reilly"),
            serde_json::json!({ "text": "it's quoted" }),
            serde_json::Value::Null,
        ],
    )
    .unwrap();

    assert_eq!(
        sql,
        "SELECT 'O''Reilly', '{\"text\":\"it''s quoted\"}', NULL"
    );
    assert!(
        ddd_cqrs_es::adapters::interpolate_query("SELECT $0", &[serde_json::json!(1)])
            .unwrap_err()
            .contains("out of bounds")
    );
    assert!(
        ddd_cqrs_es::adapters::interpolate_query("SELECT $2", &[serde_json::json!(1)])
            .unwrap_err()
            .contains("out of bounds")
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

#[test]
fn repository_idempotent_concurrency() {
    let store = InMemoryEventStore::<Counter>::new();
    let idempotency = InMemoryIdempotencyStore::new();
    let repo = Repository::new(store.clone());
    let counter_id = "concurrent-counter".to_owned();
    let key = IdempotencyKey::new("concurrent-req");

    let repo_arc = Arc::new(repo);
    let idempotency_arc = Arc::new(idempotency);
    let counter_id_arc = Arc::new(counter_id.clone());
    let key_arc = Arc::new(key);

    let mut handles = vec![];
    for _ in 0..10 {
        let repo = Arc::clone(&repo_arc);
        let idempotency = Arc::clone(&idempotency_arc);
        let counter_id = Arc::clone(&counter_id_arc);
        let key = Arc::clone(&key_arc);

        handles.push(thread::spawn(move || {
            repo.execute_idempotent(
                &counter_id,
                CounterCommand::Create,
                Metadata::default(),
                (*key).clone(),
                &*idempotency,
            )
        }));
    }

    let mut results = vec![];
    for handle in handles {
        results.push(handle.join().unwrap().unwrap());
    }

    let first_result = &results[0];
    for r in &results {
        assert_eq!(r, first_result);
    }

    assert_eq!(store.load(&counter_id).unwrap().len(), 1);
}

#[cfg(feature = "async")]
mod async_tests {
    use super::*;
    use ddd_cqrs_es::{
        async_api::AsyncEventStore, AsyncRepository, AsyncSnapshotStore, InMemoryEventStore,
        InMemoryIdempotencyStore, InMemorySnapshotStore, Snapshot,
    };

    #[tokio::test]
    async fn test_async_repository_flow() {
        let store = InMemoryEventStore::<Counter>::new();
        let repo = AsyncRepository::new(store);
        let counter_id = "async-counter-1".to_owned();

        repo.execute(&counter_id, CounterCommand::Create, Metadata::default())
            .await
            .unwrap();

        repo.execute(
            &counter_id,
            CounterCommand::Increment { by: 5 },
            Metadata::default(),
        )
        .await
        .unwrap();

        let loaded = repo.load(&counter_id).await.unwrap();
        assert_eq!(loaded.state.value, 5);
        assert_eq!(loaded.revision, 2);
    }

    #[tokio::test]
    async fn test_async_repository_with_snapshots() {
        let store = InMemoryEventStore::<Counter>::new();
        let snapshots = InMemorySnapshotStore::<Counter>::new();
        let repo = AsyncRepository::new(store);
        let counter_id = "async-counter-snapshot".to_owned();

        repo.execute(&counter_id, CounterCommand::Create, Metadata::default())
            .await
            .unwrap();

        let loaded = repo.load(&counter_id).await.unwrap();

        let snapshot = Snapshot::new(
            counter_id.clone(),
            loaded.revision,
            loaded.state.clone(),
            Metadata::default(),
        );
        AsyncSnapshotStore::save_snapshot(&snapshots, snapshot)
            .await
            .unwrap();

        repo.execute(
            &counter_id,
            CounterCommand::Increment { by: 10 },
            Metadata::default(),
        )
        .await
        .unwrap();

        let loaded_snap = repo
            .load_with_snapshot(&counter_id, &snapshots)
            .await
            .unwrap();
        assert_eq!(loaded_snap.state.value, 10);
        assert_eq!(loaded_snap.revision, 2);
    }

    #[tokio::test]
    async fn test_async_repository_idempotent() {
        let store = InMemoryEventStore::<Counter>::new();
        let idempotency = InMemoryIdempotencyStore::new();
        let repo = AsyncRepository::new(store.clone());
        let counter_id = "async-counter-idempotent".to_owned();
        let key = IdempotencyKey::new("async-req-1");

        let first = repo
            .execute_idempotent(
                &counter_id,
                CounterCommand::Create,
                Metadata::default(),
                key.clone(),
                &idempotency,
            )
            .await
            .unwrap();

        let retry = repo
            .execute_idempotent(
                &counter_id,
                CounterCommand::Increment { by: 9 },
                Metadata::default(),
                key,
                &idempotency,
            )
            .await
            .unwrap();

        assert_eq!(first, retry);
        let events = AsyncEventStore::load(&store, &counter_id).await.unwrap();
        assert_eq!(events.len(), 1);
    }

    #[tokio::test]
    async fn test_async_repository_idempotent_concurrency() {
        let store = InMemoryEventStore::<Counter>::new();
        let idempotency = InMemoryIdempotencyStore::new();
        let repo = Arc::new(AsyncRepository::new(store.clone()));
        let idempotency_arc = Arc::new(idempotency);
        let counter_id = Arc::new("async-concurrent-counter".to_owned());
        let key = Arc::new(IdempotencyKey::new("async-concurrent-req"));

        let mut tasks = vec![];
        for _ in 0..10 {
            let repo = Arc::clone(&repo);
            let idempotency = Arc::clone(&idempotency_arc);
            let counter_id = Arc::clone(&counter_id);
            let key = Arc::clone(&key);

            tasks.push(tokio::spawn(async move {
                repo.execute_idempotent(
                    &counter_id,
                    CounterCommand::Create,
                    Metadata::default(),
                    (*key).clone(),
                    &*idempotency,
                )
                .await
            }));
        }

        let mut results = vec![];
        for task in tasks {
            results.push(task.await.unwrap().unwrap());
        }

        let first_result = &results[0];
        for r in &results {
            assert_eq!(r, first_result);
        }

        let events = AsyncEventStore::load(&store, &*counter_id).await.unwrap();
        assert_eq!(events.len(), 1);
    }

    #[tokio::test]
    #[cfg(feature = "sqlite")]
    async fn test_async_persisted_projection_runner() {
        use ddd_cqrs_es::projection::{AsyncCheckpointStore, AsyncPersistedProjectionRunner};
        use ddd_cqrs_es::SqliteCheckpointStore;
        use rusqlite::Connection;

        let conn = Connection::open_in_memory().unwrap();
        let checkpoint_store = SqliteCheckpointStore::new(conn).unwrap();

        let store = InMemoryEventStore::<Counter>::new();
        let repo = AsyncRepository::new(store.clone());
        let counter_id = "counter-1".to_owned();

        repo.execute(&counter_id, CounterCommand::Create, Metadata::default())
            .await
            .unwrap();

        let projection = CounterProjection::default();
        let mut runner = AsyncPersistedProjectionRunner::new(projection, checkpoint_store.clone());

        let applied = runner.run::<Counter, _>(&store).await.unwrap();
        assert_eq!(applied, 1);

        let cp = checkpoint_store
            .load_checkpoint("counter_projection")
            .await
            .unwrap();
        assert_eq!(cp, Some(1));
    }
}

#[cfg(feature = "sqlite")]
#[test]
fn test_sqlite_chained_upcaster() {
    use ddd_cqrs_es::EventUpcaster;

    struct Upcaster1To2;
    impl EventUpcaster for Upcaster1To2 {
        type Error = std::convert::Infallible;
        fn source_version(&self) -> u32 {
            1
        }
        fn target_version(&self) -> u32 {
            2
        }
        fn upcast(&self, raw_payload: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
            let s = String::from_utf8(raw_payload).unwrap();
            let upgraded = s.replace("OldCreated", "V2Created");
            Ok(upgraded.into_bytes())
        }
    }

    struct Upcaster2To3;
    impl EventUpcaster for Upcaster2To3 {
        type Error = std::convert::Infallible;
        fn source_version(&self) -> u32 {
            2
        }
        fn target_version(&self) -> u32 {
            3
        }
        fn upcast(&self, raw_payload: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
            let s = String::from_utf8(raw_payload).unwrap();
            let upgraded = s.replace("V2Created", "Created");
            Ok(upgraded.into_bytes())
        }
    }

    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE events (
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
        "#,
    )
    .unwrap();

    conn.execute(
        "INSERT INTO events (event_id, aggregate_id, aggregate_type, revision, event_type, event_version, payload, metadata, recorded_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        rusqlite::params![
            "event-123",
            "\"counter-123\"",
            "counter",
            1,
            "counter_created",
            1,
            "\"OldCreated\"",
            serde_json::to_string(&Metadata::default()).unwrap(),
            1700000000000i64,
        ]
    ).unwrap();

    let store = ddd_cqrs_es::SqliteEventStore::<Counter>::new(conn).unwrap();
    store.register_upcaster("counter_created", Upcaster1To2);
    store.register_upcaster("counter_created", Upcaster2To3);

    let events = ddd_cqrs_es::EventStore::load(&store, &"counter-123".to_owned()).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].payload, CounterEvent::Created);
    assert_eq!(events[0].event_version, 3);
}

#[cfg(feature = "sqlite")]
#[test]
fn test_sqlite_checkpoint_store() {
    use ddd_cqrs_es::projection::CheckpointStore;
    use ddd_cqrs_es::SqliteCheckpointStore;
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    let store = SqliteCheckpointStore::new(conn).unwrap();

    assert_eq!(store.load_checkpoint("proj1").unwrap(), None);
    store.save_checkpoint("proj1", 42).unwrap();
    assert_eq!(store.load_checkpoint("proj1").unwrap(), Some(42));
    store.save_checkpoint("proj1", 100).unwrap();
    assert_eq!(store.load_checkpoint("proj1").unwrap(), Some(100));
}

#[cfg(feature = "sqlite")]
#[test]
fn test_sync_persisted_projection_runner() {
    use ddd_cqrs_es::projection::PersistedProjectionRunner;
    use ddd_cqrs_es::SqliteCheckpointStore;

    let conn = rusqlite::Connection::open_in_memory().unwrap();
    let checkpoint_store = SqliteCheckpointStore::new(conn).unwrap();

    let store = InMemoryEventStore::<Counter>::new();
    let repo = Repository::new(store.clone());
    let counter_id = "counter-1".to_owned();

    repo.execute(&counter_id, CounterCommand::Create, Metadata::default())
        .unwrap();

    let projection = CounterProjection::default();
    let mut runner = PersistedProjectionRunner::new(projection, checkpoint_store.clone());

    let applied = runner.run::<Counter, _>(&store).unwrap();
    assert_eq!(applied, 1);

    use ddd_cqrs_es::projection::CheckpointStore;
    let cp = checkpoint_store
        .load_checkpoint("counter_projection")
        .unwrap();
    assert_eq!(cp, Some(1));
}

#[cfg(feature = "postgres")]
#[test]
fn test_postgres_chained_upcaster() {
    let Ok(database_url) = std::env::var("DDD_CQRS_ES_POSTGRES_URL") else {
        eprintln!("skipping live Postgres upcaster test: DDD_CQRS_ES_POSTGRES_URL is not set");
        return;
    };
    use ddd_cqrs_es::EventUpcaster;

    struct Upcaster1To2;
    impl EventUpcaster for Upcaster1To2 {
        type Error = std::convert::Infallible;
        fn source_version(&self) -> u32 {
            1
        }
        fn target_version(&self) -> u32 {
            2
        }
        fn upcast(&self, raw_payload: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
            let s = String::from_utf8(raw_payload).unwrap();
            let upgraded = s.replace("OldCreated", "V2Created");
            Ok(upgraded.into_bytes())
        }
    }

    struct Upcaster2To3;
    impl EventUpcaster for Upcaster2To3 {
        type Error = std::convert::Infallible;
        fn source_version(&self) -> u32 {
            2
        }
        fn target_version(&self) -> u32 {
            3
        }
        fn upcast(&self, raw_payload: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
            let s = String::from_utf8(raw_payload).unwrap();
            let upgraded = s.replace("V2Created", "Created");
            Ok(upgraded.into_bytes())
        }
    }

    let mut client = postgres::Client::connect(&database_url, postgres::NoTls).unwrap();
    let table_name = format!("events_upcast_{}", std::process::id());
    let _ = client.execute(&format!("DROP TABLE IF EXISTS {};", table_name), &[]);

    let store = ddd_cqrs_es::PostgresEventStore::<Counter>::connect_with_table_name(
        &database_url,
        table_name.clone(),
    )
    .unwrap();
    store.initialize_schema().unwrap();

    client.execute(
        &format!(
            "INSERT INTO {} (event_id, aggregate_id, aggregate_type, revision, event_type, event_version, payload, metadata, recorded_at_ms)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            table_name
        ),
        &[
            &"my-test-event-id".to_owned(),
            &"\"counter-123\"".to_owned(),
            &"counter".to_owned(),
            &1i64,
            &"counter_created".to_owned(),
            &1i32,
            &serde_json::to_value("OldCreated").unwrap(),
            &serde_json::to_value(Metadata::default()).unwrap(),
            &1700000000000i64,
        ]
    ).unwrap();

    store.register_upcaster("counter_created", Upcaster1To2);
    store.register_upcaster("counter_created", Upcaster2To3);

    let events = ddd_cqrs_es::EventStore::load(&store, &"counter-123".to_owned()).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].payload, CounterEvent::Created);
    assert_eq!(events[0].event_version, 3);

    let _ = client.execute(&format!("DROP TABLE IF EXISTS {};", table_name), &[]);
}

#[cfg(feature = "postgres")]
#[test]
fn test_postgres_checkpoint_store() {
    let Ok(database_url) = std::env::var("DDD_CQRS_ES_POSTGRES_URL") else {
        eprintln!("skipping live Postgres checkpoint test: DDD_CQRS_ES_POSTGRES_URL is not set");
        return;
    };
    use ddd_cqrs_es::projection::CheckpointStore;
    use ddd_cqrs_es::PostgresCheckpointStore;

    let mut client = postgres::Client::connect(&database_url, postgres::NoTls).unwrap();
    let table_name = format!("checkpoints_{}", std::process::id());
    let _ = client.execute(&format!("DROP TABLE IF EXISTS {};", table_name), &[]);

    let store = PostgresCheckpointStore::with_table_name(client, table_name.clone()).unwrap();

    assert_eq!(store.load_checkpoint("proj1").unwrap(), None);
    store.save_checkpoint("proj1", 42).unwrap();
    assert_eq!(store.load_checkpoint("proj1").unwrap(), Some(42));
    store.save_checkpoint("proj1", 100).unwrap();
    assert_eq!(store.load_checkpoint("proj1").unwrap(), Some(100));

    let mut client = postgres::Client::connect(&database_url, postgres::NoTls).unwrap();
    let _ = client.execute(&format!("DROP TABLE IF EXISTS {};", table_name), &[]);
}
