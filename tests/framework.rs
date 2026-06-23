use ddd_cqrs_es::{
    Aggregate, CommandHandler, EventStore, EventStoreError, ExpectedRevision, InMemoryEventStore,
    NewEvent, Repository,
};

#[derive(Clone, Debug, PartialEq, Eq)]
enum CounterEvent {
    Created,
    Incremented { by: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CounterCommand {
    Create,
    Increment { by: u64 },
}

#[derive(Default, Clone, Debug, PartialEq, Eq)]
struct Counter {
    exists: bool,
    value: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CounterError {
    AlreadyCreated,
    NotCreated,
    InvalidIncrement,
}

impl Aggregate for Counter {
    type Event = CounterEvent;

    fn apply(&mut self, event: &Self::Event) {
        match event {
            CounterEvent::Created => self.exists = true,
            CounterEvent::Incremented { by } => self.value += by,
        }
    }
}

impl CommandHandler<CounterCommand> for Counter {
    type Error = CounterError;

    fn handle(&self, command: CounterCommand) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            CounterCommand::Create => {
                if self.exists {
                    return Err(CounterError::AlreadyCreated);
                }
                Ok(vec![CounterEvent::Created])
            }
            CounterCommand::Increment { by } => {
                if !self.exists {
                    return Err(CounterError::NotCreated);
                }
                if by == 0 {
                    return Err(CounterError::InvalidIncrement);
                }
                Ok(vec![CounterEvent::Incremented { by }])
            }
        }
    }
}

#[test]
fn repository_executes_commands_and_replays_state() {
    let store = InMemoryEventStore::<CounterEvent>::new();
    let repo = Repository::new(store);

    repo.execute::<Counter, _>("counter-1", CounterCommand::Create)
        .unwrap();
    repo.execute::<Counter, _>("counter-1", CounterCommand::Increment { by: 2 })
        .unwrap();
    repo.execute::<Counter, _>("counter-1", CounterCommand::Increment { by: 3 })
        .unwrap();

    let loaded = repo.load::<Counter>("counter-1").unwrap();
    assert_eq!(loaded.state.value, 5);
    assert_eq!(loaded.version, 3);
}

#[test]
fn event_store_rejects_wrong_expected_revision() {
    let store = InMemoryEventStore::<CounterEvent>::new();

    store
        .append(
            "counter-1",
            ExpectedRevision::NoStream,
            vec![NewEvent::without_metadata(CounterEvent::Created)],
        )
        .unwrap();

    let result = store.append(
        "counter-1",
        ExpectedRevision::NoStream,
        vec![NewEvent::without_metadata(CounterEvent::Incremented { by: 1 })],
    );

    assert!(matches!(
        result,
        Err(EventStoreError::Conflict {
            expected: ExpectedRevision::NoStream,
            actual: 1
        })
    ));
}

#[test]
fn domain_errors_are_not_persisted() {
    let store = InMemoryEventStore::<CounterEvent>::new();
    let repo = Repository::new(store.clone());

    let result = repo.execute::<Counter, _>("counter-1", CounterCommand::Increment { by: 1 });
    assert!(result.is_err());

    let events = store.load("counter-1").unwrap();
    assert!(events.is_empty());
}
