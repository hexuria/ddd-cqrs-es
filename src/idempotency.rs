use crate::error::{ConcurrencyError, EventStoreFailure, RepositoryError};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, RwLock};

/// Stable idempotency key used to deduplicate command retries.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    /// Creates a new idempotency key.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the key as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for IdempotencyKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Stores previously committed command results by idempotency key.
pub trait IdempotencyStore<V>: Clone + Send + Sync + 'static
where
    V: Clone,
{
    /// Store-specific error type.
    type Error;

    /// Loads a previous result for an idempotency key.
    fn load(&self, key: &IdempotencyKey) -> Result<Option<V>, Self::Error>;

    /// Saves a result for an idempotency key.
    fn save(&self, key: IdempotencyKey, value: V) -> Result<(), Self::Error>;
}

/// Error returned by [`InMemoryIdempotencyStore`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InMemoryIdempotencyError {
    /// Shared state was poisoned by a panic while holding a lock.
    Poisoned,
}

impl Display for InMemoryIdempotencyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            InMemoryIdempotencyError::Poisoned => {
                f.write_str("idempotency store lock was poisoned")
            }
        }
    }
}

impl Error for InMemoryIdempotencyError {}

/// Thread-safe in-memory idempotency store.
#[derive(Clone, Debug)]
pub struct InMemoryIdempotencyStore<V>
where
    V: Clone,
{
    entries: Arc<RwLock<HashMap<IdempotencyKey, V>>>,
}

impl<V> Default for InMemoryIdempotencyStore<V>
where
    V: Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<V> InMemoryIdempotencyStore<V>
where
    V: Clone,
{
    /// Creates an empty in-memory idempotency store.
    pub fn new() -> Self {
        Self {
            entries: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Removes all stored entries.
    pub fn clear(&self) -> Result<(), InMemoryIdempotencyError> {
        self.entries
            .write()
            .map_err(|_| InMemoryIdempotencyError::Poisoned)?
            .clear();
        Ok(())
    }
}

impl<V> IdempotencyStore<V> for InMemoryIdempotencyStore<V>
where
    V: Clone + Send + Sync + 'static,
{
    type Error = InMemoryIdempotencyError;

    fn load(&self, key: &IdempotencyKey) -> Result<Option<V>, Self::Error> {
        let entries = self
            .entries
            .read()
            .map_err(|_| InMemoryIdempotencyError::Poisoned)?;
        Ok(entries.get(key).cloned())
    }

    fn save(&self, key: IdempotencyKey, value: V) -> Result<(), Self::Error> {
        let mut entries = self
            .entries
            .write()
            .map_err(|_| InMemoryIdempotencyError::Poisoned)?;
        entries.entry(key).or_insert(value);
        Ok(())
    }
}

/// Error returned by idempotent repository execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IdempotentRepositoryError<DomainError, StoreError, IdempotencyError> {
    /// Aggregate command handling rejected the command.
    Domain(DomainError),
    /// Event store rejected the append due to optimistic concurrency.
    Concurrency(ConcurrencyError),
    /// Event store or infrastructure operation failed.
    Store(StoreError),
    /// Idempotency store operation failed.
    Idempotency(IdempotencyError),
}

impl<DomainError, StoreError, IdempotencyError>
    IdempotentRepositoryError<DomainError, StoreError, IdempotencyError>
where
    StoreError: EventStoreFailure,
{
    /// Converts an event store error into an idempotent repository error.
    pub fn from_store_error(error: StoreError) -> Self {
        match error.into_repository_error() {
            RepositoryError::Domain(error) => IdempotentRepositoryError::Domain(error),
            RepositoryError::Concurrency(error) => IdempotentRepositoryError::Concurrency(error),
            RepositoryError::Store(error) => IdempotentRepositoryError::Store(error),
        }
    }
}

impl<DomainError, StoreError, IdempotencyError> Display
    for IdempotentRepositoryError<DomainError, StoreError, IdempotencyError>
where
    DomainError: Display,
    StoreError: Display,
    IdempotencyError: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            IdempotentRepositoryError::Domain(error) => Display::fmt(error, f),
            IdempotentRepositoryError::Concurrency(error) => Display::fmt(error, f),
            IdempotentRepositoryError::Store(error) => Display::fmt(error, f),
            IdempotentRepositoryError::Idempotency(error) => Display::fmt(error, f),
        }
    }
}

impl<DomainError, StoreError, IdempotencyError> Error
    for IdempotentRepositoryError<DomainError, StoreError, IdempotencyError>
where
    DomainError: Error + 'static,
    StoreError: Error + 'static,
    IdempotencyError: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            IdempotentRepositoryError::Domain(error) => Some(error),
            IdempotentRepositoryError::Concurrency(error) => Some(error),
            IdempotentRepositoryError::Store(error) => Some(error),
            IdempotentRepositoryError::Idempotency(error) => Some(error),
        }
    }
}
