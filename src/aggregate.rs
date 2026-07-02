use crate::event::{DomainEvent, EventEnvelope, Revision, INITIAL_REVISION};
use std::hash::Hash;

/// Event-sourced domain consistency boundary.
///
/// Aggregates validate commands and return new events. They should not persist
/// themselves, publish messages, call infrastructure, or mutate themselves
/// during command handling.
///
/// # Example
///
/// ```rust
/// use ddd_cqrs_es::{Aggregate, DomainEvent, LoadedAggregate};
///
/// #[derive(Clone)]
/// enum CounterEvent {
///     Incremented(u32),
/// }
///
/// impl DomainEvent for CounterEvent {
///     fn event_type(&self) -> &'static str { "counter_incremented" }
/// }
///
/// struct Counter {
///     value: u32,
///     revision: u64,
/// }
///
/// impl Aggregate for Counter {
///     type Id = String;
///     type Command = u32;
///     type Event = CounterEvent;
///     type Error = &'static str;
///
///     fn aggregate_type() -> &'static str { "counter" }
///     fn revision(&self) -> u64 { self.revision }
///     fn new() -> Self { Self { value: 0, revision: 0 } }
///
///     fn apply(&mut self, event: &Self::Event) {
///         match event {
///             CounterEvent::Incremented(by) => self.value += by,
///         }
///         self.revision += 1;
///     }
///
///     fn handle(&self, command: Self::Command) -> Result<Vec<Self::Event>, Self::Error> {
///         if command == 0 {
///             return Err("increment must be greater than zero");
///         }
///         Ok(vec![CounterEvent::Incremented(command)])
///     }
/// }
///
/// let counter = Counter::new();
/// let events = counter.handle(5).unwrap();
/// assert_eq!(events.len(), 1);
/// ```
pub trait Aggregate: Sized {
    /// Aggregate identifier type.
    type Id: Clone + Eq + Hash + Send + Sync + 'static;
    /// Command type handled by this aggregate.
    type Command;
    /// Domain event type applied by this aggregate.
    type Event: DomainEvent;
    /// Domain error type returned when a command is rejected.
    type Error;

    /// Stable aggregate type name used in event envelopes.
    fn aggregate_type() -> &'static str;

    /// Returns the aggregate's own revision if it tracks one.
    fn revision(&self) -> Revision;

    /// Mutates aggregate state from a previously decided domain event.
    fn apply(&mut self, event: &Self::Event);

    /// Validates a command against current state and returns events to persist.
    fn handle(&self, command: Self::Command) -> Result<Vec<Self::Event>, Self::Error>;

    /// Creates a new empty aggregate state.
    fn new() -> Self;

    /// Rebuilds an aggregate from persisted event envelopes.
    fn replay(events: &[EventEnvelope<Self::Event, Self::Id>]) -> LoadedAggregate<Self> {
        let mut state = Self::new();
        let mut revision = INITIAL_REVISION;

        for envelope in events {
            state.apply(&envelope.payload);
            revision = envelope.revision;
        }

        LoadedAggregate { state, revision }
    }

    /// Rebuilds an aggregate from raw events starting at revision zero.
    ///
    /// This helper is intended for aggregate unit tests. Use [`Aggregate::replay`]
    /// for persisted event envelopes because it preserves stored revisions.
    fn replay_raw_events_from_zero(events: &[Self::Event]) -> LoadedAggregate<Self> {
        let mut state = Self::new();

        for event in events {
            state.apply(event);
        }

        LoadedAggregate {
            state,
            revision: events.len() as u64,
        }
    }
}

/// Aggregate state plus the persisted stream revision that produced it.
///
/// # Example
///
/// ```rust
/// use ddd_cqrs_es::LoadedAggregate;
///
/// let loaded = LoadedAggregate::new("my state".to_string(), 5);
/// assert_eq!(loaded.revision, 5);
/// assert_eq!(loaded.into_inner(), "my state");
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadedAggregate<A> {
    /// Replayed aggregate state.
    pub state: A,
    /// Persisted stream revision after replay.
    pub revision: Revision,
}

impl<A> LoadedAggregate<A> {
    /// Creates loaded aggregate state from a state value and revision.
    pub fn new(state: A, revision: Revision) -> Self {
        Self { state, revision }
    }

    /// Returns the aggregate state, discarding the tracked revision.
    pub fn into_inner(self) -> A {
        self.state
    }
}
