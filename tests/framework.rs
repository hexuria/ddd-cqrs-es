use ddd_cqrs_es::{
    assert_event_store_contract, Aggregate, AggregateFixture, ConcurrencyError, DomainEvent,
    EventStore, EventStoreContractOptions, EventStoreError, EventStream, EventType,
    ExpectedRevision, IdempotencyKey, IdempotencyState, IdempotencyStore, IdempotencyWaitConfig,
    InMemoryEventStore, InMemoryIdempotencyStore, InMemoryProjectionRunner, InMemorySnapshotStore,
    Metadata, NewEvent, Projection, Repository, RepositoryError, Snapshot, SnapshotStore,
};
use std::collections::HashMap;
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

#[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct StoredIdempotencyResult {
    value: u64,
    label: String,
}

#[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
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

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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

#[derive(Clone, Debug)]
struct LoadCountingStore {
    inner: InMemoryEventStore<Counter>,
    load_count: Arc<AtomicUsize>,
}

impl LoadCountingStore {
    fn new(inner: InMemoryEventStore<Counter>) -> Self {
        Self {
            inner,
            load_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn load_count(&self) -> usize {
        self.load_count.load(Ordering::SeqCst)
    }
}

impl EventStore<Counter> for LoadCountingStore {
    type Error = EventStoreError;

    fn load(&self, aggregate_id: &String) -> Result<EventStream<Counter>, Self::Error> {
        self.load_count.fetch_add(1, Ordering::SeqCst);
        self.inner.load(aggregate_id)
    }

    fn append(
        &self,
        aggregate_id: &String,
        expected_revision: ExpectedRevision,
        events: Vec<NewEvent<CounterEvent>>,
    ) -> Result<EventStream<Counter>, Self::Error> {
        self.inner.append(aggregate_id, expected_revision, events)
    }

    fn load_global_after(
        &self,
        sequence: Option<u64>,
    ) -> Result<EventStream<Counter>, Self::Error> {
        self.inner.load_global_after(sequence)
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl ddd_cqrs_es::async_api::AsyncEventStore<Counter> for LoadCountingStore {
    type Error = EventStoreError;

    async fn load(&self, aggregate_id: &String) -> Result<EventStream<Counter>, Self::Error> {
        EventStore::load(self, aggregate_id)
    }

    async fn append(
        &self,
        aggregate_id: &String,
        expected_revision: ExpectedRevision,
        events: Vec<NewEvent<CounterEvent>>,
    ) -> Result<EventStream<Counter>, Self::Error> {
        EventStore::append(self, aggregate_id, expected_revision, events)
    }

    async fn load_global_after(
        &self,
        sequence: Option<u64>,
    ) -> Result<EventStream<Counter>, Self::Error> {
        EventStore::load_global_after(self, sequence)
    }
}

#[derive(Clone, Debug)]
struct OffsetSequenceStore {
    inner: InMemoryEventStore<Counter>,
    offset: u64,
}

impl OffsetSequenceStore {
    fn new(offset: u64) -> Self {
        Self {
            inner: InMemoryEventStore::new(),
            offset,
        }
    }

    fn map_sequences(&self, mut events: EventStream<Counter>) -> EventStream<Counter> {
        for event in &mut events {
            event.sequence = event.sequence.map(|sequence| sequence + self.offset);
        }
        events
    }
}

impl EventStore<Counter> for OffsetSequenceStore {
    type Error = EventStoreError;

    fn load(&self, aggregate_id: &String) -> Result<EventStream<Counter>, Self::Error> {
        self.inner
            .load(aggregate_id)
            .map(|events| self.map_sequences(events))
    }

    fn append(
        &self,
        aggregate_id: &String,
        expected_revision: ExpectedRevision,
        events: Vec<NewEvent<CounterEvent>>,
    ) -> Result<EventStream<Counter>, Self::Error> {
        self.inner
            .append(aggregate_id, expected_revision, events)
            .map(|events| self.map_sequences(events))
    }

    fn load_global_after(
        &self,
        sequence: Option<u64>,
    ) -> Result<EventStream<Counter>, Self::Error> {
        let inner_sequence = sequence.map(|sequence| sequence.saturating_sub(self.offset));
        self.inner
            .load_global_after(inner_sequence)
            .map(|events| self.map_sequences(events))
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
fn repository_execute_returning_state_loads_stream_once() {
    let store = LoadCountingStore::new(InMemoryEventStore::<Counter>::new());
    let observed_store = store.clone();
    let repo = Repository::new(store);
    let counter_id = "counter-load-once".to_owned();

    let (loaded, committed) = repo
        .execute_returning_state(&counter_id, CounterCommand::Create, Metadata::default())
        .unwrap();

    assert_eq!(observed_store.load_count(), 1);
    assert_eq!(committed.len(), 1);
    assert_eq!(loaded.revision, 1);
    assert!(loaded.state.id.is_some());
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
        EventStoreContractOptions::default(),
    );
}

#[test]
fn event_store_contract_accepts_custom_first_sequence() {
    assert_event_store_contract::<Counter, _>(
        OffsetSequenceStore::new(100),
        "offset-contract-counter".to_owned(),
        CounterEvent::Created,
        CounterEvent::Incremented { by: 1 },
        EventStoreContractOptions::with_expected_first_global_sequence(101),
    );
}

#[test]
fn event_type_is_a_string_newtype() {
    let event_type = EventType::from("counter_created");

    assert_eq!(event_type.as_str(), "counter_created");
    assert_eq!(event_type.to_string(), "counter_created");
    assert_eq!(event_type.clone().into_string(), "counter_created");
}

#[cfg(feature = "json")]
#[test]
fn event_type_round_trips_through_serde() {
    let event_type = EventType::from("counter_created");
    let json = serde_json::to_string(&event_type).unwrap();
    let restored: EventType = serde_json::from_str(&json).unwrap();

    assert_eq!(restored, event_type);
}

#[cfg(feature = "sqlite")]
#[test]
fn sqlite_store_passes_reusable_contract() {
    assert_event_store_contract::<Counter, _>(
        ddd_cqrs_es::SqliteEventStore::<Counter>::in_memory().unwrap(),
        "sqlite-contract-counter".to_owned(),
        CounterEvent::Created,
        CounterEvent::Incremented { by: 1 },
        EventStoreContractOptions::default(),
    );
}

#[cfg(feature = "sqlite")]
#[test]
fn sqlite_idempotency_store_passes_contract() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    let store = ddd_cqrs_es::SqliteIdempotencyStore::new(connection).unwrap();
    assert_sql_idempotency_store_contract(store);
}

#[cfg(feature = "sqlite")]
#[test]
fn sqlite_atomic_idempotent_retry_returns_original_committed_events() {
    let store = ddd_cqrs_es::SqliteEventStore::<Counter>::in_memory().unwrap();
    let repo = Repository::new(store.clone());
    let counter_id = "sqlite-atomic-counter".to_owned();
    let key = IdempotencyKey::new("sqlite-atomic-request");

    let first = repo
        .execute_idempotent_atomic(
            &counter_id,
            CounterCommand::Create,
            Metadata::default(),
            key.clone(),
        )
        .unwrap();
    let retry = repo
        .execute_idempotent_atomic(
            &counter_id,
            CounterCommand::Increment { by: 9 },
            Metadata::default(),
            key,
        )
        .unwrap();

    assert_eq!(first, retry);
    assert_eq!(first[0].payload, CounterEvent::Created);
    assert_eq!(store.load(&counter_id).unwrap().len(), 1);
}

#[cfg(feature = "sqlite")]
#[test]
fn sqlite_atomic_idempotent_pending_key_times_out() {
    let database_name = format!(
        "file:sqlite_atomic_pending_{}_{}?mode=memory&cache=shared",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let event_connection = rusqlite::Connection::open(&database_name).unwrap();
    let idempotency_connection = rusqlite::Connection::open(&database_name).unwrap();
    let store = ddd_cqrs_es::SqliteEventStore::<Counter>::new(event_connection).unwrap();
    store.initialize_schema().unwrap();
    let idempotency =
        ddd_cqrs_es::SqliteIdempotencyStore::<EventStream<Counter>>::new(idempotency_connection)
            .unwrap();
    let key = IdempotencyKey::new("sqlite-atomic-pending-request");
    idempotency.reserve(key.clone()).unwrap();

    let repo = Repository::new(store);
    let error = repo
        .execute_idempotent_atomic_with_wait_config(
            &"sqlite-atomic-pending-counter".to_owned(),
            CounterCommand::Create,
            Metadata::default(),
            key.clone(),
            IdempotencyWaitConfig::new(Duration::from_millis(5), Duration::from_millis(1)),
        )
        .unwrap_err();

    assert!(matches!(
        error,
        ddd_cqrs_es::IdempotentRepositoryError::IdempotencyPendingTimeout {
            key: timeout_key,
            ..
        } if timeout_key == key
    ));
}

#[cfg(feature = "sqlite")]
#[test]
fn sqlite_snapshot_store_persists_latest_snapshot() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    let store = ddd_cqrs_es::SqliteSnapshotStore::<Counter>::new(connection).unwrap();
    let counter_id = "sqlite-snapshot-counter".to_owned();
    let older = Counter {
        id: Some(counter_id.clone()),
        value: 1,
        revision: 1,
    };
    let newer = Counter {
        id: Some(counter_id.clone()),
        value: 7,
        revision: 2,
    };

    ddd_cqrs_es::assert_snapshot_store_contract::<Counter, _>(
        store.clone(),
        counter_id.clone(),
        older.clone(),
        newer.clone(),
    );
    store
        .save_snapshot(Snapshot::new(
            counter_id.clone(),
            1,
            older,
            Metadata::default(),
        ))
        .unwrap();

    let loaded = store.load_snapshot(&counter_id).unwrap().unwrap();
    assert_eq!(loaded.revision, 2);
    assert_eq!(loaded.state, newer);
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
        EventStoreContractOptions::default(),
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

#[test]
fn projection_runner_error_formats_and_exposes_source() {
    let error: ddd_cqrs_es::ProjectionRunnerError<std::io::Error, std::io::Error, std::io::Error> =
        ddd_cqrs_es::ProjectionRunnerError::Store(std::io::Error::other("store failed"));

    assert_eq!(error.to_string(), "store failed");
    assert!(error.source().is_some());
}

#[test]
fn event_store_error_preserves_sources_without_changing_display() {
    let error = EventStoreError::backend_with_source(
        "database unavailable",
        std::io::Error::other("socket refused"),
    );

    assert_eq!(
        error.to_string(),
        "event store backend error: database unavailable"
    );
    assert!(error.source().is_some());

    #[cfg(feature = "json")]
    {
        let source = serde_json::from_str::<CounterEvent>("not json").unwrap_err();
        let error = EventStoreError::deserialization_with_source(
            format!("event payload: {source}"),
            source,
        );

        assert!(error.to_string().starts_with("deserialization error:"));
        assert!(error.source().is_some());
    }
}

#[test]
fn process_manager_runner_dispatches_emitted_commands() {
    #[derive(Clone, Debug, PartialEq, Eq)]
    enum Event {
        Created,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum Command {
        SendEmail,
    }

    #[derive(Clone, Debug)]
    struct WelcomeProcess;

    impl ddd_cqrs_es::ProcessManager<Event, Command> for WelcomeProcess {
        type Error = std::convert::Infallible;

        fn name(&self) -> &'static str {
            "welcome"
        }

        fn handle(&mut self, event: &Event) -> Result<Vec<Command>, Self::Error> {
            match event {
                Event::Created => Ok(vec![Command::SendEmail]),
            }
        }
    }

    #[derive(Clone, Debug)]
    struct RecordingBus;

    impl ddd_cqrs_es::CommandBus<Command> for RecordingBus {
        type Output = &'static str;
        type Error = std::convert::Infallible;

        fn dispatch(&self, command: Command) -> Result<Self::Output, Self::Error> {
            match command {
                Command::SendEmail => Ok("sent"),
            }
        }
    }

    let mut runner = ddd_cqrs_es::ProcessManagerRunner::new(WelcomeProcess, RecordingBus);
    let outputs = runner.run(&Event::Created).unwrap();

    assert_eq!(outputs, vec!["sent"]);
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

#[cfg(feature = "sqlite")]
#[test]
fn transactional_projection_rolls_back_read_model_and_checkpoint_together() {
    use rusqlite::OptionalExtension;

    struct SqliteTransactionalCounterProjection {
        connection: rusqlite::Connection,
        fail_on_sequence: Option<u64>,
    }

    impl SqliteTransactionalCounterProjection {
        fn new(fail_on_sequence: Option<u64>) -> Self {
            let connection = rusqlite::Connection::open_in_memory().unwrap();
            connection
                .execute_batch(
                    r#"
                    CREATE TABLE counter_values (
                        id TEXT PRIMARY KEY,
                        value INTEGER NOT NULL
                    );
                    CREATE TABLE tx_checkpoints (
                        projection_name TEXT PRIMARY KEY,
                        sequence INTEGER NOT NULL
                    );
                    "#,
                )
                .unwrap();
            Self {
                connection,
                fail_on_sequence,
            }
        }

        fn counter_value(&self, id: &str) -> u64 {
            self.connection
                .query_row(
                    "SELECT value FROM counter_values WHERE id = ?1",
                    [id],
                    |row| row.get::<_, i64>(0),
                )
                .optional()
                .unwrap()
                .unwrap_or(0) as u64
        }
    }

    impl ddd_cqrs_es::TransactionalCheckpointedProjection<CounterEvent, String>
        for SqliteTransactionalCounterProjection
    {
        type Error = String;

        fn name(&self) -> &'static str {
            "sqlite_tx_counter_projection"
        }

        fn load_checkpoint(&self) -> Result<Option<u64>, Self::Error> {
            self.connection
                .query_row(
                    "SELECT sequence FROM tx_checkpoints WHERE projection_name = ?1",
                    [self.name()],
                    |row| row.get::<_, i64>(0),
                )
                .optional()
                .map(|value| value.map(|sequence| sequence as u64))
                .map_err(|error| error.to_string())
        }

        fn apply_and_checkpoint_transactionally(
            &mut self,
            event: &ddd_cqrs_es::EventEnvelope<CounterEvent, String>,
        ) -> Result<(), Self::Error> {
            let projection_name = self.name();
            let transaction = self
                .connection
                .transaction()
                .map_err(|error| error.to_string())?;

            match event.payload {
                CounterEvent::Created => {
                    transaction
                        .execute(
                            "INSERT INTO counter_values (id, value)
                             VALUES (?1, 0)
                             ON CONFLICT(id) DO NOTHING",
                            [event.aggregate_id.as_str()],
                        )
                        .map_err(|error| error.to_string())?;
                }
                CounterEvent::Incremented { by } => {
                    transaction
                        .execute(
                            "INSERT INTO counter_values (id, value)
                             VALUES (?1, ?2)
                             ON CONFLICT(id) DO UPDATE SET value = value + excluded.value",
                            rusqlite::params![event.aggregate_id.as_str(), by as i64],
                        )
                        .map_err(|error| error.to_string())?;
                }
            }

            if event.sequence == self.fail_on_sequence {
                return Err("projection failed".to_owned());
            }

            if let Some(sequence) = event.sequence {
                transaction
                    .execute(
                        "INSERT INTO tx_checkpoints (projection_name, sequence)
                         VALUES (?1, ?2)
                         ON CONFLICT(projection_name) DO UPDATE
                         SET sequence = excluded.sequence
                         WHERE excluded.sequence > tx_checkpoints.sequence",
                        rusqlite::params![projection_name, sequence as i64],
                    )
                    .map_err(|error| error.to_string())?;
            }

            transaction.commit().map_err(|error| error.to_string())
        }
    }

    let store = InMemoryEventStore::<Counter>::new();
    let repo = Repository::new(store.clone());
    let counter_id = "sqlite-transactional-projection".to_owned();
    repo.execute(&counter_id, CounterCommand::Create, Metadata::default())
        .unwrap();
    repo.execute(
        &counter_id,
        CounterCommand::Increment { by: 4 },
        Metadata::default(),
    )
    .unwrap();

    let projection = SqliteTransactionalCounterProjection::new(Some(2));
    let mut runner = ddd_cqrs_es::TransactionalCheckpointedProjectionRunner::new(projection);
    assert!(runner.run::<Counter, _>(&store).is_err());
    assert_eq!(runner.projection().counter_value(&counter_id), 0);

    runner.projection_mut().fail_on_sequence = None;
    assert_eq!(runner.run::<Counter, _>(&store).unwrap(), 1);
    assert_eq!(runner.projection().counter_value(&counter_id), 4);
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
fn repository_idempotent_pending_key_times_out() {
    let store = InMemoryEventStore::<Counter>::new();
    let idempotency = InMemoryIdempotencyStore::new();
    let repo = Repository::new(store);
    let counter_id = "pending-counter".to_owned();
    let key = IdempotencyKey::new("pending-request");
    idempotency.reserve(key.clone()).unwrap();

    let error = repo
        .execute_idempotent_with_wait_config(
            &counter_id,
            CounterCommand::Create,
            Metadata::default(),
            key.clone(),
            &idempotency,
            IdempotencyWaitConfig::new(Duration::from_millis(5), Duration::from_millis(1)),
        )
        .unwrap_err();

    assert!(matches!(
        error,
        ddd_cqrs_es::IdempotentRepositoryError::IdempotencyPendingTimeout {
            key: timeout_key,
            ..
        } if timeout_key == key
    ));
}

#[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
#[test]
fn sql_schema_config_rejects_invalid_table_names_eagerly() {
    let result = ddd_cqrs_es::SqlSchemaConfig::new(ddd_cqrs_es::SqlDialect::Sqlite)
        .with_events_table("not-valid-table-name");

    assert!(result.is_err());
    let result = ddd_cqrs_es::SqlSchemaConfig::new(ddd_cqrs_es::SqlDialect::Sqlite)
        .with_snapshots_table("not-valid-table-name");

    assert!(result.is_err());
}

#[cfg(feature = "sqlite")]
#[test]
fn sqlite_schema_creates_replay_index_without_duplicate_stream_index() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    let config = ddd_cqrs_es::SqlSchemaConfig::new(ddd_cqrs_es::SqlDialect::Sqlite)
        .with_events_table("custom_events")
        .unwrap();
    let migrator = ddd_cqrs_es::SchemaMigrator::new(config);

    migrator.run_sqlite(&conn).unwrap();

    let replay_index_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'custom_events_global_replay_idx'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let duplicate_stream_index_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'custom_events_stream_idx'",
            [],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(replay_index_count, 1);
    assert_eq!(duplicate_stream_index_count, 0);
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
    async fn async_repository_execute_returning_state_loads_stream_once() {
        let store = LoadCountingStore::new(InMemoryEventStore::<Counter>::new());
        let observed_store = store.clone();
        let repo = AsyncRepository::new(store);
        let counter_id = "async-counter-load-once".to_owned();

        let (loaded, committed) = repo
            .execute_returning_state(&counter_id, CounterCommand::Create, Metadata::default())
            .await
            .unwrap();

        assert_eq!(observed_store.load_count(), 1);
        assert_eq!(committed.len(), 1);
        assert_eq!(loaded.revision, 1);
        assert!(loaded.state.id.is_some());
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

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn sqlite_async_atomic_idempotent_retry_returns_original_committed_events() {
        let store = ddd_cqrs_es::SqliteEventStore::<Counter>::in_memory().unwrap();
        let repo = AsyncRepository::new(store.clone());
        let counter_id = "sqlite-async-atomic-counter".to_owned();
        let key = IdempotencyKey::new("sqlite-async-atomic-request");

        let first = repo
            .execute_idempotent_atomic(
                &counter_id,
                CounterCommand::Create,
                Metadata::default(),
                key.clone(),
            )
            .await
            .unwrap();
        let retry = repo
            .execute_idempotent_atomic(
                &counter_id,
                CounterCommand::Increment { by: 9 },
                Metadata::default(),
                key,
            )
            .await
            .unwrap();

        assert_eq!(first, retry);
        let events = AsyncEventStore::load(&store, &counter_id).await.unwrap();
        assert_eq!(events.len(), 1);
    }

    #[tokio::test]
    async fn async_repository_idempotent_pending_key_times_out() {
        let store = InMemoryEventStore::<Counter>::new();
        let idempotency = InMemoryIdempotencyStore::new();
        let repo = AsyncRepository::new(store);
        let counter_id = "async-pending-counter".to_owned();
        let key = IdempotencyKey::new("async-pending-request");
        idempotency.reserve(key.clone()).unwrap();

        let error = repo
            .execute_idempotent_with_wait_config(
                &counter_id,
                CounterCommand::Create,
                Metadata::default(),
                key.clone(),
                &idempotency,
                IdempotencyWaitConfig::new(Duration::from_millis(5), Duration::from_millis(1)),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            ddd_cqrs_es::IdempotentRepositoryError::IdempotencyPendingTimeout {
                key: timeout_key,
                ..
            } if timeout_key == key
        ));
    }

    #[tokio::test]
    async fn async_process_manager_runner_dispatches_emitted_commands() {
        #[derive(Clone, Debug, PartialEq, Eq)]
        enum Event {
            Created,
        }

        #[derive(Clone, Debug, PartialEq, Eq)]
        enum Command {
            SendEmail,
        }

        #[derive(Clone, Debug)]
        struct WelcomeProcess;

        impl ddd_cqrs_es::ProcessManager<Event, Command> for WelcomeProcess {
            type Error = std::convert::Infallible;

            fn name(&self) -> &'static str {
                "welcome"
            }

            fn handle(&mut self, event: &Event) -> Result<Vec<Command>, Self::Error> {
                match event {
                    Event::Created => Ok(vec![Command::SendEmail]),
                }
            }
        }

        #[derive(Clone, Debug)]
        struct RecordingBus;

        #[async_trait::async_trait]
        impl ddd_cqrs_es::AsyncCommandBus<Command> for RecordingBus {
            type Output = &'static str;
            type Error = std::convert::Infallible;

            async fn dispatch(&self, command: Command) -> Result<Self::Output, Self::Error> {
                match command {
                    Command::SendEmail => Ok("sent"),
                }
            }
        }

        let mut runner = ddd_cqrs_es::AsyncProcessManagerRunner::new(WelcomeProcess, RecordingBus);
        let outputs = runner.run(&Event::Created).await.unwrap();

        assert_eq!(outputs, vec!["sent"]);
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
    store.save_checkpoint("proj1", 90).unwrap();
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
    store.save_checkpoint("proj1", 90).unwrap();
    assert_eq!(store.load_checkpoint("proj1").unwrap(), Some(100));

    let mut client = postgres::Client::connect(&database_url, postgres::NoTls).unwrap();
    let _ = client.execute(&format!("DROP TABLE IF EXISTS {};", table_name), &[]);
}

#[cfg(feature = "sqlite")]
#[test]
fn test_sqlite_sequential_custom_table_initialization() {
    let connection = rusqlite::Connection::open("file::memory:?cache=shared").unwrap();

    // 1. Create event store with "custom_events_a" table name.
    let event_store = ddd_cqrs_es::sqlite::SqliteEventStore::<Counter>::with_table_name(
        connection,
        "custom_events_a".to_owned(),
    )
    .unwrap();
    event_store.initialize_schema().unwrap();

    // 2. Open another connection to the shared in-memory DB and create a checkpoint store with "custom_checkpoints_a".
    let connection2 = rusqlite::Connection::open("file::memory:?cache=shared").unwrap();
    let checkpoint_store = ddd_cqrs_es::sqlite::SqliteCheckpointStore::with_table_name(
        connection2,
        "custom_checkpoints_a",
    )
    .unwrap();

    // 3. Let's write to both of them to verify they work together and both tables exist!
    let id = "counter-123".to_owned();
    let events = vec![ddd_cqrs_es::NewEvent::new(
        CounterEvent::Created,
        Metadata::default(),
    )];
    event_store
        .append(&id, ddd_cqrs_es::ExpectedRevision::Any, events)
        .unwrap();

    use ddd_cqrs_es::projection::CheckpointStore;
    checkpoint_store
        .save_checkpoint("projection-a", 99)
        .unwrap();

    assert_eq!(event_store.load(&id).unwrap().len(), 1);
    assert_eq!(
        checkpoint_store.load_checkpoint("projection-a").unwrap(),
        Some(99)
    );
}

#[cfg(feature = "json-file")]
#[test]
fn test_json_file_concurrency_and_atomicity() {
    let dir = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let events_path = dir.join(format!("test_events_{}.json", nanos));
    let checkpoints_path = dir.join(format!("test_checkpoints_{}.json", nanos));

    let event_store = ddd_cqrs_es::JsonFileEventStore::<Counter>::new(events_path.clone());
    let checkpoint_store = ddd_cqrs_es::JsonFileCheckpointStore::new(checkpoints_path.clone());

    // Run parallel threads appending events concurrently
    let store_arc = std::sync::Arc::new(event_store);
    let mut handles = Vec::new();

    for i in 0..10 {
        let store = std::sync::Arc::clone(&store_arc);
        let agg_id = format!("thread-{}", i);
        let handle = thread::spawn(move || {
            let events = vec![ddd_cqrs_es::NewEvent::new(
                CounterEvent::Created,
                Metadata::default(),
            )];
            store
                .append(&agg_id, ddd_cqrs_es::ExpectedRevision::Any, events)
                .unwrap();
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().unwrap();
    }

    // Verify all 10 aggregates got created and saved without corruption!
    for i in 0..10 {
        let agg_id = format!("thread-{}", i);
        let stream = store_arc.load(&agg_id).unwrap();
        assert_eq!(stream.len(), 1);
    }

    // Verify checkpoints concurrent writes
    let cp_store = std::sync::Arc::new(checkpoint_store);
    let mut cp_handles = Vec::new();
    for i in 0..10 {
        let store = std::sync::Arc::clone(&cp_store);
        let proj_name = format!("proj-{}", i);
        let handle = thread::spawn(move || {
            use ddd_cqrs_es::projection::CheckpointStore;
            store.save_checkpoint(&proj_name, i as u64).unwrap();
        });
        cp_handles.push(handle);
    }

    for h in cp_handles {
        h.join().unwrap();
    }

    for i in 0..10 {
        let proj_name = format!("proj-{}", i);
        use ddd_cqrs_es::projection::CheckpointStore;
        assert_eq!(
            cp_store.load_checkpoint(&proj_name).unwrap(),
            Some(i as u64)
        );
    }

    // Cleanup
    let _ = std::fs::remove_file(events_path);
    let _ = std::fs::remove_file(checkpoints_path);
}

#[cfg(feature = "mysql")]
static MYSQL_TEST_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(feature = "mysql")]
struct MySqlTestDb {
    test_url: String,
}

#[cfg(feature = "mysql")]
impl MySqlTestDb {
    fn new() -> Result<Option<Self>, String> {
        let test_url = match std::env::var("DDD_CQRS_ES_MYSQL_URL") {
            Ok(value) if value.trim().is_empty() => return Ok(None),
            Ok(value) => value.trim().to_owned(),
            Err(std::env::VarError::NotPresent) => return Ok(None),
            Err(error) => {
                return Err(format!(
                    "DDD_CQRS_ES_MYSQL_URL contains invalid unicode: {error}"
                ));
            }
        };

        mysql::Conn::new(test_url.as_str()).map_err(|error| {
            format!("failed to connect to MySQL URL from DDD_CQRS_ES_MYSQL_URL: {error}")
        })?;

        Ok(Some(Self { test_url }))
    }
}

#[cfg(feature = "mysql")]
fn unique_mysql_table(prefix: &str) -> String {
    format!(
        "{}_{}_{}",
        prefix,
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

#[cfg(feature = "mysql")]
struct MySqlTableCleanup {
    test_url: String,
    tables: Vec<String>,
}

#[cfg(feature = "mysql")]
impl MySqlTableCleanup {
    fn new(test_url: &str, tables: Vec<String>) -> Self {
        Self {
            test_url: test_url.to_owned(),
            tables,
        }
    }
}

#[cfg(feature = "mysql")]
impl Drop for MySqlTableCleanup {
    fn drop(&mut self) {
        if let Ok(mut conn) = mysql::Conn::new(self.test_url.as_str()) {
            use mysql::prelude::Queryable;
            for table in &self.tables {
                let _ = conn.query_drop(format!("DROP TABLE IF EXISTS `{table}`;"));
            }
        }
    }
}

#[cfg(feature = "mysql")]
fn mysql_test_db_or_skip(test_name: &str) -> Option<MySqlTestDb> {
    match MySqlTestDb::new() {
        Ok(Some(db)) => Some(db),
        Ok(None) => {
            eprintln!("skipping live MySQL {test_name}: DDD_CQRS_ES_MYSQL_URL is not set");
            None
        }
        Err(error) => panic!("failed to prepare live MySQL {test_name}: {error}"),
    }
}

#[cfg(feature = "mysql")]
#[test]
fn test_mysql_store_passes_reusable_contract_when_url_is_provided() {
    let _guard = MYSQL_TEST_MUTEX
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(db) = mysql_test_db_or_skip("contract test") else {
        return;
    };

    let table_name = unique_mysql_table("events_live_contract");
    let _cleanup = MySqlTableCleanup::new(&db.test_url, vec![table_name.clone()]);

    let store = ddd_cqrs_es::MySqlEventStore::<Counter>::connect_with_table_name(
        &db.test_url,
        table_name.clone(),
    )
    .unwrap();
    store.initialize_schema().unwrap();

    assert_event_store_contract::<Counter, _>(
        store,
        "mysql-contract-counter".to_owned(),
        CounterEvent::Created,
        CounterEvent::Incremented { by: 1 },
        EventStoreContractOptions::default(),
    );
}

#[cfg(feature = "mysql")]
#[test]
fn test_mysql_checkpoint_store() {
    let _guard = MYSQL_TEST_MUTEX
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(db) = mysql_test_db_or_skip("checkpoint test") else {
        return;
    };
    use ddd_cqrs_es::projection::CheckpointStore;
    use ddd_cqrs_es::MySqlCheckpointStore;

    let conn = mysql::Conn::new(db.test_url.as_str()).unwrap();
    let table_name = unique_mysql_table("checkpoints");
    let _cleanup = MySqlTableCleanup::new(&db.test_url, vec![table_name.clone()]);

    let store = MySqlCheckpointStore::with_table_name(conn, table_name.clone()).unwrap();

    assert_eq!(store.load_checkpoint("proj1").unwrap(), None);
    store.save_checkpoint("proj1", 42).unwrap();
    assert_eq!(store.load_checkpoint("proj1").unwrap(), Some(42));
    store.save_checkpoint("proj1", 100).unwrap();
    assert_eq!(store.load_checkpoint("proj1").unwrap(), Some(100));
    store.save_checkpoint("proj1", 90).unwrap();
    assert_eq!(store.load_checkpoint("proj1").unwrap(), Some(100));
}

#[cfg(feature = "mysql")]
#[test]
fn test_mysql_idempotency_store() {
    let _guard = MYSQL_TEST_MUTEX
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(db) = mysql_test_db_or_skip("idempotency test") else {
        return;
    };
    use ddd_cqrs_es::MySqlIdempotencyStore;

    let conn = mysql::Conn::new(db.test_url.as_str()).unwrap();
    let table_name = unique_mysql_table("idempotency");
    let _cleanup = MySqlTableCleanup::new(&db.test_url, vec![table_name.clone()]);

    let store = MySqlIdempotencyStore::with_table_name(conn, table_name.clone()).unwrap();

    assert_sql_idempotency_store_contract(store);
}
