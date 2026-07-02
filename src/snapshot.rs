use crate::aggregate::Aggregate;
use crate::error::{ConcurrencyError, EventStoreFailure, RepositoryError};
use crate::metadata::Metadata;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

/// Persisted aggregate snapshot used to speed up replay of long streams.
///
/// Snapshots are optional and must never replace the event log.
///
/// # Example
///
/// ```rust
/// use ddd_cqrs_es::{Snapshot, Metadata};
/// # use ddd_cqrs_es::Aggregate;
/// # #[derive(Clone)]
/// # struct DummyEvent;
/// # impl ddd_cqrs_es::DomainEvent for DummyEvent {
/// #     fn event_type(&self) -> &'static str { "dummy" }
/// # }
/// # #[derive(Clone, Debug, PartialEq)]
/// # struct MyAggregate;
/// # impl Aggregate for MyAggregate {
/// #     type Id = String;
/// #     type Command = ();
/// #     type Event = DummyEvent;
/// #     type Error = ();
/// #     fn aggregate_type() -> &'static str { "dummy" }
/// #     fn revision(&self) -> u64 { 0 }
/// #     fn new() -> Self { MyAggregate }
/// #     fn apply(&mut self, _event: &Self::Event) {}
/// #     fn handle(&self, _command: Self::Command) -> Result<Vec<Self::Event>, Self::Error> { Ok(vec![]) }
/// # }
///
/// let snapshot = Snapshot::new("stream-1".to_string(), 10, MyAggregate, Metadata::default());
/// assert_eq!(snapshot.revision, 10);
/// assert_eq!(snapshot.aggregate_id, "stream-1");
/// ```
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot<A>
where
    A: Aggregate,
{
    /// Aggregate stream identifier.
    pub aggregate_id: A::Id,
    /// Stable aggregate type name.
    pub aggregate_type: String,
    /// Stream revision represented by the snapshot.
    pub revision: u64,
    /// Aggregate state at the snapshot revision.
    pub state: A,
    /// Snapshot metadata.
    pub metadata: Metadata,
    /// Time the snapshot was recorded.
    pub recorded_at: SystemTime,
}

impl<A> Snapshot<A>
where
    A: Aggregate,
{
    /// Creates a snapshot.
    pub fn new(aggregate_id: A::Id, revision: u64, state: A, metadata: Metadata) -> Self {
        Self {
            aggregate_id,
            aggregate_type: A::aggregate_type().to_owned(),
            revision,
            state,
            metadata,
            recorded_at: SystemTime::now(),
        }
    }
}

/// Snapshot persistence abstraction.
pub trait SnapshotStore<A>
where
    A: Aggregate,
{
    /// Store-specific error type.
    type Error;

    /// Loads the latest snapshot for an aggregate stream.
    fn load_snapshot(&self, aggregate_id: &A::Id) -> Result<Option<Snapshot<A>>, Self::Error>;

    /// Saves a snapshot.
    fn save_snapshot(&self, snapshot: Snapshot<A>) -> Result<(), Self::Error>;
}

/// Error returned by [`InMemorySnapshotStore`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InMemorySnapshotError {
    /// Shared state was poisoned by a panic while holding a lock.
    Poisoned,
}

impl Display for InMemorySnapshotError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            InMemorySnapshotError::Poisoned => f.write_str("snapshot store lock was poisoned"),
        }
    }
}

impl Error for InMemorySnapshotError {}

/// Thread-safe in-memory snapshot store.
///
/// # Example
///
/// ```rust
/// use ddd_cqrs_es::{InMemorySnapshotStore, SnapshotStore, Snapshot, Metadata};
/// # use ddd_cqrs_es::Aggregate;
/// # #[derive(Clone)]
/// # struct DummyEvent;
/// # impl ddd_cqrs_es::DomainEvent for DummyEvent {
/// #     fn event_type(&self) -> &'static str { "dummy" }
/// # }
/// # #[derive(Clone, Debug, PartialEq)]
/// # struct MyAggregate;
/// # impl Aggregate for MyAggregate {
/// #     type Id = String;
/// #     type Command = ();
/// #     type Event = DummyEvent;
/// #     type Error = ();
/// #     fn aggregate_type() -> &'static str { "dummy" }
/// #     fn revision(&self) -> u64 { 0 }
/// #     fn new() -> Self { MyAggregate }
/// #     fn apply(&mut self, _event: &Self::Event) {}
/// #     fn handle(&self, _command: Self::Command) -> Result<Vec<Self::Event>, Self::Error> { Ok(vec![]) }
/// # }
///
/// let store = InMemorySnapshotStore::<MyAggregate>::new();
/// let snapshot = Snapshot::new("stream-1".to_string(), 10, MyAggregate, Metadata::default());
///
/// store.save_snapshot(snapshot.clone()).unwrap();
/// let loaded = store.load_snapshot(&"stream-1".to_string()).unwrap().unwrap();
/// assert_eq!(loaded.revision, 10);
/// ```
#[derive(Clone, Debug)]
pub struct InMemorySnapshotStore<A>
where
    A: Aggregate + Clone,
{
    snapshots: Arc<RwLock<HashMap<A::Id, Snapshot<A>>>>,
}

impl<A> Default for InMemorySnapshotStore<A>
where
    A: Aggregate + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<A> InMemorySnapshotStore<A>
where
    A: Aggregate + Clone,
{
    /// Creates an empty in-memory snapshot store.
    pub fn new() -> Self {
        Self {
            snapshots: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Removes all snapshots.
    pub fn clear(&self) -> Result<(), InMemorySnapshotError> {
        self.snapshots
            .write()
            .map_err(|_| InMemorySnapshotError::Poisoned)?
            .clear();
        Ok(())
    }
}

impl<A> SnapshotStore<A> for InMemorySnapshotStore<A>
where
    A: Aggregate + Clone + Send + Sync + 'static,
{
    type Error = InMemorySnapshotError;

    fn load_snapshot(&self, aggregate_id: &A::Id) -> Result<Option<Snapshot<A>>, Self::Error> {
        let snapshots = self
            .snapshots
            .read()
            .map_err(|_| InMemorySnapshotError::Poisoned)?;
        Ok(snapshots.get(aggregate_id).cloned())
    }

    fn save_snapshot(&self, snapshot: Snapshot<A>) -> Result<(), Self::Error> {
        let mut snapshots = self
            .snapshots
            .write()
            .map_err(|_| InMemorySnapshotError::Poisoned)?;
        snapshots.insert(snapshot.aggregate_id.clone(), snapshot);
        Ok(())
    }
}

/// Error returned by snapshot-aware repository operations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotRepositoryError<DomainError, StoreError, SnapshotError> {
    /// Aggregate command handling rejected the command.
    Domain(DomainError),
    /// Event store rejected the append due to optimistic concurrency.
    Concurrency(ConcurrencyError),
    /// Event store or infrastructure operation failed.
    Store(StoreError),
    /// Snapshot store operation failed.
    Snapshot(SnapshotError),
}

impl<DomainError, StoreError, SnapshotError>
    SnapshotRepositoryError<DomainError, StoreError, SnapshotError>
where
    StoreError: EventStoreFailure,
{
    /// Converts an event store error into the snapshot-aware repository error.
    pub fn from_store_error(error: StoreError) -> Self {
        match error.into_repository_error() {
            RepositoryError::Domain(error) => SnapshotRepositoryError::Domain(error),
            RepositoryError::Concurrency(error) => SnapshotRepositoryError::Concurrency(error),
            RepositoryError::Store(error) => SnapshotRepositoryError::Store(error),
        }
    }
}

impl<DomainError, StoreError, SnapshotError> Display
    for SnapshotRepositoryError<DomainError, StoreError, SnapshotError>
where
    DomainError: Display,
    StoreError: Display,
    SnapshotError: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SnapshotRepositoryError::Domain(error) => Display::fmt(error, f),
            SnapshotRepositoryError::Concurrency(error) => Display::fmt(error, f),
            SnapshotRepositoryError::Store(error) => Display::fmt(error, f),
            SnapshotRepositoryError::Snapshot(error) => Display::fmt(error, f),
        }
    }
}

impl<DomainError, StoreError, SnapshotError> Error
    for SnapshotRepositoryError<DomainError, StoreError, SnapshotError>
where
    DomainError: Error + 'static,
    StoreError: Error + 'static,
    SnapshotError: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            SnapshotRepositoryError::Domain(error) => Some(error),
            SnapshotRepositoryError::Concurrency(error) => Some(error),
            SnapshotRepositoryError::Store(error) => Some(error),
            SnapshotRepositoryError::Snapshot(error) => Some(error),
        }
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl<A> crate::async_api::AsyncSnapshotStore<A> for InMemorySnapshotStore<A>
where
    A: Aggregate + Clone + Send + Sync + 'static,
{
    type Error = InMemorySnapshotError;

    async fn load_snapshot(
        &self,
        aggregate_id: &A::Id,
    ) -> Result<Option<Snapshot<A>>, Self::Error> {
        SnapshotStore::load_snapshot(self, aggregate_id)
    }

    async fn save_snapshot(&self, snapshot: Snapshot<A>) -> Result<(), Self::Error> {
        SnapshotStore::save_snapshot(self, snapshot)
    }
}
