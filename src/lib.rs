//! # ddd_cqrs_es
//!
//! A lightweight framework for building Domain-Driven Design, CQRS, and Event
//! Sourcing applications in Rust.
//!
//! The crate provides explicit building blocks for aggregate command handling,
//! optimistic concurrency, event replay, projections, process managers,
//! snapshots, and pluggable persistence backends. The included in-memory store
//! is intended for tests, examples, and local development.
//!
//! # Example
//!
//! ```
//! use ddd_cqrs_es::{Aggregate, DomainEvent, InMemoryEventStore, Metadata, Repository};
//!
//! #[derive(Clone)]
//! enum CounterEvent {
//!     Created,
//!     Incremented(u64),
//! }
//!
//! impl DomainEvent for CounterEvent {
//!     fn event_type(&self) -> &'static str {
//!         match self {
//!             CounterEvent::Created => "counter_created",
//!             CounterEvent::Incremented(_) => "counter_incremented",
//!         }
//!     }
//! }
//!
//! enum CounterCommand {
//!     Create,
//!     Increment(u64),
//! }
//!
//! #[derive(Default)]
//! struct Counter {
//!     exists: bool,
//!     value: u64,
//!     revision: u64,
//! }
//!
//! #[derive(Debug)]
//! enum CounterError {
//!     AlreadyCreated,
//!     NotCreated,
//! }
//!
//! impl Aggregate for Counter {
//!     type Id = String;
//!     type Command = CounterCommand;
//!     type Event = CounterEvent;
//!     type Error = CounterError;
//!
//!     fn aggregate_type() -> &'static str { "counter" }
//!     fn id(&self) -> Option<&Self::Id> { None }
//!     fn revision(&self) -> u64 { self.revision }
//!     fn new() -> Self { Self::default() }
//!
//!     fn apply(&mut self, event: &Self::Event) {
//!         match event {
//!             CounterEvent::Created => self.exists = true,
//!             CounterEvent::Incremented(by) => self.value += by,
//!         }
//!         self.revision += 1;
//!     }
//!
//!     fn handle(&self, command: Self::Command) -> Result<Vec<Self::Event>, Self::Error> {
//!         match command {
//!             CounterCommand::Create if self.exists => Err(CounterError::AlreadyCreated),
//!             CounterCommand::Create => Ok(vec![CounterEvent::Created]),
//!             CounterCommand::Increment(_) if !self.exists => Err(CounterError::NotCreated),
//!             CounterCommand::Increment(by) => Ok(vec![CounterEvent::Incremented(by)]),
//!         }
//!     }
//! }
//!
//! let store = InMemoryEventStore::<Counter>::new();
//! let repo = Repository::new(store);
//! let counter_id = "counter-1".to_owned();
//!
//! repo.execute(&counter_id, CounterCommand::Create, Metadata::default())?;
//! repo.execute(&counter_id, CounterCommand::Increment(5), Metadata::default())?;
//! let loaded = repo.load(&counter_id)?;
//!
//! assert_eq!(loaded.state.value, 5);
//! # Ok::<(), ddd_cqrs_es::RepositoryError<CounterError>>(())
//! ```

#[cfg(feature = "json")]
pub mod adapters;
pub mod aggregate;
#[cfg(feature = "async")]
pub mod async_api;
pub mod command;
pub mod error;
pub mod event;
pub mod event_store;
pub mod idempotency;
pub mod memory;
pub mod metadata;
#[cfg(feature = "postgres")]
pub mod postgres;
pub mod process_manager;
pub mod projection;
pub mod repository;
pub mod snapshot;
#[cfg(any(feature = "postgres", feature = "sqlite"))]
mod sql_common;
#[cfg(feature = "sqlite")]
pub mod sqlite;
pub mod store;
pub mod testing;
pub mod upcast;

pub use aggregate::{Aggregate, LoadedAggregate};
#[cfg(feature = "async")]
pub use async_api::{
    AsyncCommandBus, AsyncCommandHandler, AsyncEventStore, AsyncIdempotencyStore,
    AsyncQueryHandler, AsyncRepository, AsyncRepositoryResult, AsyncSnapshotStore,
};

pub use command::{CommandBus, CommandHandler, QueryHandler};
pub use error::{ConcurrencyError, EventStoreError, EventStoreFailure, RepositoryError};
pub use event::{
    DomainEvent, EventEnvelope, EventId, EventType, ExpectedRevision, NewEvent, Revision,
    INITIAL_REVISION,
};
pub use event_store::{EventStore, EventStream, StandardEventStore};
pub use idempotency::{
    IdempotencyKey, IdempotencyState, IdempotencyStore, IdempotentRepositoryError,
    InMemoryIdempotencyError, InMemoryIdempotencyStore,
};
pub use memory::InMemoryEventStore;
pub use metadata::Metadata;
#[cfg(feature = "postgres")]
pub use postgres::{PostgresCheckpointStore, PostgresEventStore, PostgresIdempotencyStore};
pub use process_manager::ProcessManager;
#[cfg(feature = "async")]
pub use projection::{AsyncCheckpointStore, AsyncPersistedProjectionRunner};
pub use projection::{
    CheckpointStore, InMemoryProjectionRunner, PersistedProjectionRunner, Projection,
    ProjectionRunnerError,
};
pub use repository::{
    CommittedEvents, ExecutionOutcome, IdempotentRepositoryResult, Repository, RepositoryResult,
    SnapshotRepositoryResult,
};
pub use snapshot::{
    InMemorySnapshotError, InMemorySnapshotStore, Snapshot, SnapshotRepositoryError, SnapshotStore,
};
#[cfg(feature = "sqlite")]
pub use sqlite::{SqliteCheckpointStore, SqliteEventStore, SqliteIdempotencyStore};
pub use testing::{assert_event_store_contract, AggregateFixture};
pub use upcast::{ErasedUpcaster, EventUpcaster, UpcasterRegistry};
