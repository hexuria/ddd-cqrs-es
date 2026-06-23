use crate::aggregate::{Aggregate, LoadedAggregate};
use crate::command::CommandHandler;
use crate::error::{EventStoreError, ExecuteError};
use crate::event::{EventEnvelope, ExpectedRevision, NewEvent};
use crate::store::EventStore;
use std::marker::PhantomData;

/// Loads event-sourced aggregates and appends new events with optimistic
/// concurrency control.
#[derive(Clone, Debug)]
pub struct Repository<S, E, M = ()> {
    store: S,
    _marker: PhantomData<(E, M)>,
}

impl<S, E, M> Repository<S, E, M>
where
    S: EventStore<E, M>,
    E: Clone + Send + 'static,
    M: Clone + Default + Send + 'static,
{
    pub fn new(store: S) -> Self {
        Self {
            store,
            _marker: PhantomData,
        }
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn load<A>(&self, aggregate_id: &str) -> Result<LoadedAggregate<A>, EventStoreError>
    where
        A: Aggregate<Event = E>,
    {
        let events = self.store.load(aggregate_id)?;
        Ok(A::replay(&events))
    }

    pub fn save<A>(
        &self,
        aggregate_id: &str,
        loaded: &LoadedAggregate<A>,
        events: Vec<E>,
    ) -> Result<Vec<EventEnvelope<E, M>>, EventStoreError>
    where
        A: Aggregate<Event = E>,
    {
        let events = events
            .into_iter()
            .map(|event| NewEvent::new(event, M::default()))
            .collect();

        self.store
            .append(aggregate_id, ExpectedRevision::Exact(loaded.version), events)
    }

    pub fn save_with_metadata<A>(
        &self,
        aggregate_id: &str,
        loaded: &LoadedAggregate<A>,
        events: Vec<NewEvent<E, M>>,
    ) -> Result<Vec<EventEnvelope<E, M>>, EventStoreError>
    where
        A: Aggregate<Event = E>,
    {
        self.store
            .append(aggregate_id, ExpectedRevision::Exact(loaded.version), events)
    }

    pub fn execute<A, C>(
        &self,
        aggregate_id: &str,
        command: C,
    ) -> Result<Vec<EventEnvelope<E, M>>, ExecuteError<A::Error>>
    where
        A: Aggregate<Event = E> + CommandHandler<C>,
    {
        let loaded = self.load::<A>(aggregate_id).map_err(ExecuteError::Store)?;
        let events = loaded.state.handle(command).map_err(ExecuteError::Domain)?;
        self.save::<A>(aggregate_id, &loaded, events)
            .map_err(ExecuteError::Store)
    }
}
