use crate::aggregate::Aggregate;
use crate::error::EventStoreError;
use crate::event::{EventEnvelope, ExpectedRevision, NewEvent};
use crate::idempotency::IdempotencyKey;
use std::error::Error;
use std::fmt::{Display, Formatter};

/// Committed events for one aggregate type.
pub type EventStream<A> = Vec<EventEnvelope<<A as Aggregate>::Event, <A as Aggregate>::Id>>;

/// Error returned by transaction-aware idempotent append operations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IdempotentAppendError<StoreError> {
    /// Another executor has reserved the key and has not completed yet.
    Pending {
        /// Key that is still pending.
        key: IdempotencyKey,
    },
    /// The backing event store failed.
    Store(StoreError),
}

impl<StoreError> Display for IdempotentAppendError<StoreError>
where
    StoreError: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            IdempotentAppendError::Pending { key } => {
                write!(f, "idempotency key `{key}` is pending")
            }
            IdempotentAppendError::Store(error) => Display::fmt(error, f),
        }
    }
}

impl<StoreError> Error for IdempotentAppendError<StoreError>
where
    StoreError: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            IdempotentAppendError::Pending { .. } => None,
            IdempotentAppendError::Store(error) => Some(error),
        }
    }
}

/// Event persistence abstraction for one aggregate type.
///
/// Durable adapters such as PostgreSQL, SQLite, Kafka, or object storage should
/// implement this trait while preserving stream order and optimistic
/// concurrency semantics.
///
/// # Example
///
/// ```rust
/// use ddd_cqrs_es::{EventStore, InMemoryEventStore, NewEvent, ExpectedRevision, Metadata};
/// # use ddd_cqrs_es::{Aggregate, DomainEvent};
/// #
/// # #[derive(Clone)]
/// # enum MyEvent { Created }
/// # impl DomainEvent for MyEvent {
/// #     fn event_type(&self) -> &'static str { "my_event" }
/// # }
/// # struct MyAggregate;
/// # impl Aggregate for MyAggregate {
/// #     type Id = String;
/// #     type Command = ();
/// #     type Event = MyEvent;
/// #     type Error = ();
/// #     fn aggregate_type() -> &'static str { "my_aggregate" }
/// #     fn revision(&self) -> u64 { 0 }
/// #     fn new() -> Self { MyAggregate }
/// #     fn apply(&mut self, _event: &Self::Event) {}
/// #     fn handle(&self, _command: Self::Command) -> Result<Vec<Self::Event>, Self::Error> { Ok(vec![]) }
/// # }
///
/// let store = InMemoryEventStore::<MyAggregate>::new();
/// let event = NewEvent::new(MyEvent::Created, Metadata::default());
///
/// store.append(&"stream-1".to_string(), ExpectedRevision::NoStream, vec![event]).unwrap();
/// let events = store.load(&"stream-1".to_string()).unwrap();
/// assert_eq!(events.len(), 1);
/// assert_eq!(events[0].revision, 1);
/// ```
pub trait EventStore<A>: Clone + Send + Sync + 'static
where
    A: Aggregate,
{
    /// Store-specific error type.
    type Error;

    /// Loads all events for one aggregate stream.
    fn load(&self, aggregate_id: &A::Id) -> Result<EventStream<A>, Self::Error>;

    /// Loads events for one aggregate stream after the given revision.
    fn load_after_revision(
        &self,
        aggregate_id: &A::Id,
        revision: u64,
    ) -> Result<EventStream<A>, Self::Error> {
        let events = self.load(aggregate_id)?;
        Ok(events
            .into_iter()
            .filter(|event| event.revision > revision)
            .collect())
    }

    /// Appends events to one aggregate stream.
    fn append(
        &self,
        aggregate_id: &A::Id,
        expected_revision: ExpectedRevision,
        events: Vec<NewEvent<A::Event>>,
    ) -> Result<EventStream<A>, Self::Error>;

    /// Loads globally ordered events after a global sequence number.
    fn load_global_after(&self, sequence: Option<u64>) -> Result<EventStream<A>, Self::Error>;
}

/// Event store extension for crash-atomic idempotent appends.
///
/// Implementations must reserve the idempotency key, append events, and persist
/// the completed committed event stream in one backing-store transaction. A
/// retry with a completed key returns the originally committed events without
/// appending again. A pending key returns [`IdempotentAppendError::Pending`] so
/// repositories can apply a bounded wait policy.
pub trait AtomicIdempotentEventStore<A>: EventStore<A>
where
    A: Aggregate,
{
    /// Appends events once for the idempotency key, atomically with the
    /// idempotency completion record.
    fn append_idempotent(
        &self,
        idempotency_key: IdempotencyKey,
        aggregate_id: &A::Id,
        expected_revision: ExpectedRevision,
        events: Vec<NewEvent<A::Event>>,
    ) -> Result<EventStream<A>, IdempotentAppendError<Self::Error>>;
}

/// Convenience alias for stores that use the framework's standard error type.
pub trait StandardEventStore<A>: EventStore<A, Error = EventStoreError>
where
    A: Aggregate,
{
}

impl<A, S> StandardEventStore<A> for S
where
    A: Aggregate,
    S: EventStore<A, Error = EventStoreError>,
{
}
