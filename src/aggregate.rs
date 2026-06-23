use crate::event::{EventEnvelope, Revision, INITIAL_REVISION};
use std::hash::Hash;

/// Event-sourced domain consistency boundary.
///
/// Aggregates validate commands and return new events. They should not persist
/// themselves, publish messages, call infrastructure, or mutate themselves
/// during command handling.
pub trait Aggregate: Sized {
    /// Aggregate identifier type.
    type Id: Clone + Eq + Hash + Send + Sync + 'static;
    /// Command type handled by this aggregate.
    type Command;
    /// Domain event type applied by this aggregate.
    type Event: Clone + Send + Sync + 'static;
    /// Domain error type returned when a command is rejected.
    type Error;

    /// Stable aggregate type name used in event envelopes.
    fn aggregate_type() -> &'static str;

    /// Returns the aggregate ID if this state has been created.
    fn id(&self) -> Option<&Self::Id>;

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
}

/// Aggregate state plus the persisted stream revision that produced it.
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
