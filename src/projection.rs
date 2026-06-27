use crate::aggregate::Aggregate;
use crate::event::EventEnvelope;
use crate::event_store::EventStore;

#[cfg(feature = "async")]
use async_trait::async_trait;

/// A read-model updater.
///
/// Projections consume committed event envelopes and update query-optimized
/// state. Implementations should be idempotent because projection runners may
/// retry after failures.
///
/// # Example
///
/// ```rust
/// use ddd_cqrs_es::{Projection, EventEnvelope, EventId, Metadata};
/// use std::time::SystemTime;
///
/// #[derive(Clone)]
/// enum UserEvent {
///     Created(String),
/// }
///
/// struct UserCounter {
///     count: usize,
/// }
///
/// impl Projection<UserEvent, String> for UserCounter {
///     type Error = std::convert::Infallible;
///
///     fn name(&self) -> &'static str { "user_counter" }
///
///     fn apply(&mut self, event: &EventEnvelope<UserEvent, String>) -> Result<(), Self::Error> {
///         match event.event() {
///             UserEvent::Created(_) => self.count += 1,
///         }
///         Ok(())
///     }
/// }
///
/// let mut counter = UserCounter { count: 0 };
/// let envelope = EventEnvelope::new(
///     EventId::new(),
///     "user-1".to_string(),
///     "user",
///     1,
///     None,
///     "UserCreated",
///     1,
///     UserEvent::Created("Alice".to_owned()),
///     Metadata::default(),
///     SystemTime::now(),
/// );
/// counter.apply(&envelope).unwrap();
/// assert_eq!(counter.count, 1);
/// ```
pub trait Projection<E, Id> {
    /// Projection error.
    type Error;

    /// Stable projection name used for checkpoint storage.
    fn name(&self) -> &'static str;

    /// Applies one committed event to the projection.
    fn apply(&mut self, event: &EventEnvelope<E, Id>) -> Result<(), Self::Error>;
}

/// In-memory projection runner with a sequence checkpoint.
///
/// # Example
///
/// ```rust
/// use ddd_cqrs_es::{InMemoryProjectionRunner, InMemoryEventStore, Projection, EventEnvelope, EventId, Metadata};
/// use std::time::SystemTime;
/// # use ddd_cqrs_es::Aggregate;
/// # #[derive(Clone)]
/// # enum UserEvent { Created }
/// # impl ddd_cqrs_es::DomainEvent for UserEvent {
/// #     fn event_type(&self) -> &'static str { "user_created" }
/// # }
/// # #[derive(Clone, Debug, PartialEq)]
/// # struct UserAggregate;
/// # impl Aggregate for UserAggregate {
/// #     type Id = String;
/// #     type Command = ();
/// #     type Event = UserEvent;
/// #     type Error = ();
/// #     fn aggregate_type() -> &'static str { "user" }
/// #     fn id(&self) -> Option<&Self::Id> { None }
/// #     fn revision(&self) -> u64 { 0 }
/// #     fn new() -> Self { UserAggregate }
/// #     fn apply(&mut self, _event: &Self::Event) {}
/// #     fn handle(&self, _command: Self::Command) -> Result<Vec<Self::Event>, Self::Error> { Ok(vec![]) }
/// # }
///
/// struct UserCounter {
///     count: usize,
/// }
///
/// impl Projection<UserEvent, String> for UserCounter {
///     type Error = std::convert::Infallible;
///     fn name(&self) -> &'static str { "user_counter" }
///     fn apply(&mut self, event: &EventEnvelope<UserEvent, String>) -> Result<(), Self::Error> {
///         self.count += 1;
///         Ok(())
///     }
/// }
///
/// let store = InMemoryEventStore::<UserAggregate>::new();
/// let mut runner = InMemoryProjectionRunner::new(UserCounter { count: 0 });
/// runner.run(&store).unwrap();
/// assert_eq!(runner.projection().count, 0);
/// ```
#[derive(Clone, Debug)]
pub struct InMemoryProjectionRunner<P> {
    projection: P,
    checkpoint: Option<u64>,
}

impl<P> InMemoryProjectionRunner<P> {
    /// Creates a runner for a projection.
    pub fn new(projection: P) -> Self {
        Self {
            projection,
            checkpoint: None,
        }
    }

    /// Returns the last successfully applied global sequence.
    pub fn checkpoint(&self) -> Option<u64> {
        self.checkpoint
    }

    /// Returns the wrapped projection.
    pub fn projection(&self) -> &P {
        &self.projection
    }

    /// Returns the wrapped projection mutably.
    pub fn projection_mut(&mut self) -> &mut P {
        &mut self.projection
    }

    /// Consumes the runner and returns the projection.
    pub fn into_projection(self) -> P {
        self.projection
    }
}

impl<P> InMemoryProjectionRunner<P> {
    /// Loads global events after the current checkpoint and applies them.
    pub fn run<A, S>(
        &mut self,
        store: &S,
    ) -> Result<usize, ProjectionRunnerError<P::Error, S::Error>>
    where
        A: Aggregate,
        S: EventStore<A>,
        P: Projection<A::Event, A::Id>,
    {
        let events = store
            .load_global_after(self.checkpoint)
            .map_err(ProjectionRunnerError::Store)?;
        let mut applied = 0;

        for event in events {
            self.projection
                .apply(&event)
                .map_err(ProjectionRunnerError::Projection)?;
            self.checkpoint = event.sequence;
            applied += 1;
        }

        Ok(applied)
    }
}

/// Error returned by a projection runner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProjectionRunnerError<
    ProjectionError,
    StoreError,
    CheckpointError = std::convert::Infallible,
> {
    /// Projection logic failed.
    Projection(ProjectionError),
    /// Event store read failed.
    Store(StoreError),
    /// Checkpoint storage failed.
    Checkpoint(CheckpointError),
}

/// A persistent store for tracking projection sequence checkpoints.
pub trait CheckpointStore {
    /// Error type.
    type Error;

    /// Loads the last successfully processed event global sequence for a given projection name.
    fn load_checkpoint(&self, projection_name: &str) -> Result<Option<u64>, Self::Error>;

    /// Saves the last successfully processed event global sequence for a given projection name.
    fn save_checkpoint(&self, projection_name: &str, sequence: u64) -> Result<(), Self::Error>;
}

/// An async persistent store for tracking projection sequence checkpoints.
#[cfg(feature = "async")]
#[async_trait]
pub trait AsyncCheckpointStore {
    /// Error type.
    type Error;

    /// Loads the last successfully processed event global sequence for a given projection name.
    async fn load_checkpoint(&self, projection_name: &str) -> Result<Option<u64>, Self::Error>;

    /// Saves the last successfully processed event global sequence for a given projection name.
    async fn save_checkpoint(
        &self,
        projection_name: &str,
        sequence: u64,
    ) -> Result<(), Self::Error>;
}

/// A projection runner that uses a persistent `CheckpointStore` to coordinate progress.
#[derive(Debug)]
pub struct PersistedProjectionRunner<P, C> {
    projection: P,
    checkpoint_store: C,
}

impl<P, C> PersistedProjectionRunner<P, C> {
    /// Creates a new persisted runner for a projection and checkpoint store.
    pub fn new(projection: P, checkpoint_store: C) -> Self {
        Self {
            projection,
            checkpoint_store,
        }
    }

    /// Returns the wrapped projection.
    pub fn projection(&self) -> &P {
        &self.projection
    }

    /// Returns the wrapped projection mutably.
    pub fn projection_mut(&mut self) -> &mut P {
        &mut self.projection
    }

    /// Consumes the runner and returns the projection.
    pub fn into_projection(self) -> P {
        self.projection
    }
}

impl<P, C> PersistedProjectionRunner<P, C>
where
    C: CheckpointStore,
{
    /// Loads global events after the current persistent checkpoint, applies them,
    /// then saves each event sequence as the new checkpoint after successful
    /// projection application.
    ///
    /// Projection side effects and checkpoint writes are not one transaction;
    /// projection implementations must be idempotent for retry safety.
    #[allow(clippy::type_complexity)]
    pub fn run<A, S>(
        &mut self,
        store: &S,
    ) -> Result<usize, ProjectionRunnerError<P::Error, S::Error, C::Error>>
    where
        A: Aggregate,
        S: EventStore<A>,
        P: Projection<A::Event, A::Id>,
    {
        let name = self.projection.name();
        let checkpoint = self
            .checkpoint_store
            .load_checkpoint(name)
            .map_err(ProjectionRunnerError::Checkpoint)?;

        let events = store
            .load_global_after(checkpoint)
            .map_err(ProjectionRunnerError::Store)?;
        let mut applied = 0;

        for event in events {
            self.projection
                .apply(&event)
                .map_err(ProjectionRunnerError::Projection)?;
            if let Some(seq) = event.sequence {
                self.checkpoint_store
                    .save_checkpoint(name, seq)
                    .map_err(ProjectionRunnerError::Checkpoint)?;
            }
            applied += 1;
        }

        Ok(applied)
    }
}

/// An async projection runner that uses a persistent `AsyncCheckpointStore` to coordinate progress.
#[cfg(feature = "async")]
#[derive(Debug)]
pub struct AsyncPersistedProjectionRunner<P, C> {
    projection: P,
    checkpoint_store: C,
}

#[cfg(feature = "async")]
impl<P, C> AsyncPersistedProjectionRunner<P, C> {
    /// Creates a new async persisted runner for a projection and checkpoint store.
    pub fn new(projection: P, checkpoint_store: C) -> Self {
        Self {
            projection,
            checkpoint_store,
        }
    }

    /// Returns the wrapped projection.
    pub fn projection(&self) -> &P {
        &self.projection
    }

    /// Returns the wrapped projection mutably.
    pub fn projection_mut(&mut self) -> &mut P {
        &mut self.projection
    }

    /// Consumes the runner and returns the projection.
    pub fn into_projection(self) -> P {
        self.projection
    }
}

#[cfg(feature = "async")]
impl<P, C> AsyncPersistedProjectionRunner<P, C>
where
    C: AsyncCheckpointStore,
{
    /// Loads global events after the current persistent checkpoint, applies them,
    /// then saves each event sequence as the new checkpoint after successful
    /// projection application.
    ///
    /// Projection side effects and checkpoint writes are not one transaction;
    /// projection implementations must be idempotent for retry safety.
    #[allow(clippy::type_complexity)]
    pub async fn run<A, S>(
        &mut self,
        store: &S,
    ) -> Result<usize, ProjectionRunnerError<P::Error, S::Error, C::Error>>
    where
        A: Aggregate + Send + Sync,
        S: crate::async_api::AsyncEventStore<A>,
        P: Projection<A::Event, A::Id>,
    {
        let name = self.projection.name();
        let checkpoint = self
            .checkpoint_store
            .load_checkpoint(name)
            .await
            .map_err(ProjectionRunnerError::Checkpoint)?;

        let events = store
            .load_global_after(checkpoint)
            .await
            .map_err(ProjectionRunnerError::Store)?;
        let mut applied = 0;

        for event in events {
            self.projection
                .apply(&event)
                .map_err(ProjectionRunnerError::Projection)?;
            if let Some(seq) = event.sequence {
                self.checkpoint_store
                    .save_checkpoint(name, seq)
                    .await
                    .map_err(ProjectionRunnerError::Checkpoint)?;
            }
            applied += 1;
        }

        Ok(applied)
    }
}
