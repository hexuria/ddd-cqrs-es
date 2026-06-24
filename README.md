# ddd_cqrs_es

A lightweight Rust framework for Domain-Driven Design, CQRS, and Event Sourcing.

The crate gives you explicit, infrastructure-light building blocks:

- `Aggregate`: typed domain consistency boundary
- `DomainEvent`: stable event type/version metadata
- `EventEnvelope`: persisted event metadata, revision, and global sequence
- `Metadata`: audit, tracing, causality, and tenancy context
- `EventStore`: pluggable persistence abstraction
- `InMemoryEventStore`: thread-safe test/local store with optimistic concurrency
- `Repository`: aggregate loading, command execution, and append coordination
- `Projection` and `InMemoryProjectionRunner`: read-model replay with checkpoints
- `ProcessManager`: event-to-command saga abstraction
- `SnapshotStore`: optional snapshot persistence abstraction
- `AggregateFixture`: concise aggregate unit tests

The core does not require a web framework, database, serializer, message broker,
or async runtime.

## Install

Use it as a path dependency while this crate is local:

```toml
[dependencies]
ddd_cqrs_es = { path = "../ddd_cqrs_es" }
```

## Core Flow

1. Define commands and past-tense domain events.
2. Implement `Aggregate` for your consistency boundary.
3. Use `Repository` with an `EventStore` to execute commands.
4. Build query models with projections from committed envelopes.

## Example

```rust
use ddd_cqrs_es::{Aggregate, DomainEvent, InMemoryEventStore, Metadata, Repository};

#[derive(Clone)]
enum CounterEvent {
    Created,
    Incremented(u64),
}

impl DomainEvent for CounterEvent {
    fn event_type(&self) -> &'static str {
        match self {
            CounterEvent::Created => "counter_created",
            CounterEvent::Incremented(_) => "counter_incremented",
        }
    }
}

enum CounterCommand {
    Create,
    Increment(u64),
}

#[derive(Default)]
struct Counter {
    exists: bool,
    value: u64,
    revision: u64,
}

#[derive(Debug)]
enum CounterError {
    AlreadyCreated,
    NotCreated,
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
        None
    }

    fn revision(&self) -> u64 {
        self.revision
    }

    fn apply(&mut self, event: &Self::Event) {
        match event {
            CounterEvent::Created => self.exists = true,
            CounterEvent::Incremented(by) => self.value += by,
        }
        self.revision += 1;
    }

    fn handle(&self, command: Self::Command) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            CounterCommand::Create if self.exists => Err(CounterError::AlreadyCreated),
            CounterCommand::Create => Ok(vec![CounterEvent::Created]),
            CounterCommand::Increment(_) if !self.exists => Err(CounterError::NotCreated),
            CounterCommand::Increment(by) => Ok(vec![CounterEvent::Incremented(by)]),
        }
    }

    fn new() -> Self {
        Self::default()
    }
}

let store = InMemoryEventStore::<Counter>::new();
let repo = Repository::new(store);
let counter_id = "counter-1".to_owned();

repo.execute(&counter_id, CounterCommand::Create, Metadata::default())?;
repo.execute(&counter_id, CounterCommand::Increment(5), Metadata::default())?;

let loaded = repo.load(&counter_id)?;
assert_eq!(loaded.state.value, 5);
# Ok::<(), ddd_cqrs_es::RepositoryError<CounterError>>(())
```

Run the full bank account example:

```bash
cargo run --example bank_account
```

Run verification:

```bash
cargo test
cargo test --doc
```

## Design Notes

- Commands are imperative; events are facts named in the past tense.
- Aggregates do not mutate during command handling. They return events.
- Repository append uses `ExpectedRevision::Exact(revision)` to enforce optimistic concurrency.
- Event envelopes preserve metadata, event type/version, stream revision, and global sequence.
- Projections should be idempotent because read models are eventually consistent.
- Snapshots are optional and never replace the event log.

See `docs/` for architecture, getting started, testing, persistence, and projection guides.
