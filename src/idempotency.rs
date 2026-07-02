use crate::error::{ConcurrencyError, EventStoreFailure, RepositoryError};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// Default time to wait for an idempotency key that is already pending.
pub const DEFAULT_IDEMPOTENCY_PENDING_TIMEOUT: Duration = Duration::from_secs(30);

/// Default polling interval while waiting for a pending idempotency key to complete.
pub const DEFAULT_IDEMPOTENCY_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Wait policy used when an idempotency key is already pending.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IdempotencyWaitConfig {
    /// Maximum time to wait for a pending key before returning a timeout error.
    pub pending_timeout: Duration,
    /// Polling interval while waiting for another caller to complete the key.
    pub poll_interval: Duration,
}

impl IdempotencyWaitConfig {
    /// Creates an idempotency wait policy.
    pub fn new(pending_timeout: Duration, poll_interval: Duration) -> Self {
        Self {
            pending_timeout,
            poll_interval,
        }
    }

    /// Returns the next delay, capped by the remaining timeout.
    pub(crate) fn next_delay(&self, elapsed: Duration) -> Option<Duration> {
        let remaining = self.pending_timeout.checked_sub(elapsed)?;
        if remaining.is_zero() {
            return None;
        }

        let poll_interval = if self.poll_interval.is_zero() {
            Duration::from_millis(1)
        } else {
            self.poll_interval
        };

        Some(remaining.min(poll_interval))
    }
}

impl Default for IdempotencyWaitConfig {
    fn default() -> Self {
        Self {
            pending_timeout: DEFAULT_IDEMPOTENCY_PENDING_TIMEOUT,
            poll_interval: DEFAULT_IDEMPOTENCY_POLL_INTERVAL,
        }
    }
}

/// Stable idempotency key used to deduplicate command retries.
///
/// # Example
///
/// ```rust
/// use ddd_cqrs_es::IdempotencyKey;
///
/// let key = IdempotencyKey::new("command-123");
/// assert_eq!(key.as_str(), "command-123");
/// assert_eq!(key.to_string(), "command-123");
/// ```
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

/// State of a processed or in-progress command.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IdempotencyState<V> {
    /// Command is currently being processed.
    Pending,
    /// Command has completed, containing the original result.
    Complete(V),
}

/// Stores previously committed command results by idempotency key.
pub trait IdempotencyStore<V>: Clone + Send + Sync + 'static
where
    V: Clone,
{
    /// Store-specific error type.
    type Error;

    /// Loads a previous result or execution status for an idempotency key.
    fn load(&self, key: &IdempotencyKey) -> Result<Option<IdempotencyState<V>>, Self::Error>;

    /// Reserves an idempotency key, marking it as pending/in-progress.
    /// Returns `true` if the key was successfully reserved, or `false` if it was already reserved/completed.
    fn reserve(&self, key: IdempotencyKey) -> Result<bool, Self::Error>;

    /// Saves a completed result for an idempotency key.
    fn save(&self, key: IdempotencyKey, value: V) -> Result<(), Self::Error>;

    /// Removes a reservation/entry (e.g. if execution failed).
    fn remove(&self, key: &IdempotencyKey) -> Result<(), Self::Error>;
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
///
/// # Example
///
/// ```rust
/// use ddd_cqrs_es::{IdempotencyKey, IdempotencyStore, InMemoryIdempotencyStore, IdempotencyState};
///
/// let store = InMemoryIdempotencyStore::<String>::new();
/// let key = IdempotencyKey::new("msg-1");
///
/// store.reserve(key.clone()).unwrap();
/// store.save(key.clone(), "processed".to_string()).unwrap();
/// let value = store.load(&key).unwrap();
/// assert_eq!(value, Some(IdempotencyState::Complete("processed".to_string())));
/// ```
#[derive(Clone, Debug)]
pub struct InMemoryIdempotencyStore<V>
where
    V: Clone,
{
    entries: Arc<RwLock<HashMap<IdempotencyKey, IdempotencyState<V>>>>,
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

    fn load(&self, key: &IdempotencyKey) -> Result<Option<IdempotencyState<V>>, Self::Error> {
        let entries = self
            .entries
            .read()
            .map_err(|_| InMemoryIdempotencyError::Poisoned)?;
        Ok(entries.get(key).cloned())
    }

    fn reserve(&self, key: IdempotencyKey) -> Result<bool, Self::Error> {
        let mut entries = self
            .entries
            .write()
            .map_err(|_| InMemoryIdempotencyError::Poisoned)?;
        match entries.entry(key) {
            std::collections::hash_map::Entry::Occupied(_) => Ok(false),
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(IdempotencyState::Pending);
                Ok(true)
            }
        }
    }

    fn save(&self, key: IdempotencyKey, value: V) -> Result<(), Self::Error> {
        let mut entries = self
            .entries
            .write()
            .map_err(|_| InMemoryIdempotencyError::Poisoned)?;
        entries.insert(key, IdempotencyState::Complete(value));
        Ok(())
    }

    fn remove(&self, key: &IdempotencyKey) -> Result<(), Self::Error> {
        let mut entries = self
            .entries
            .write()
            .map_err(|_| InMemoryIdempotencyError::Poisoned)?;
        entries.remove(key);
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
    /// The idempotency key remained pending until the configured wait timeout elapsed.
    IdempotencyPendingTimeout {
        /// Key that was still pending.
        key: IdempotencyKey,
        /// Time spent waiting for the pending key.
        waited: Duration,
    },
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
            IdempotentRepositoryError::IdempotencyPendingTimeout { key, waited } => {
                write!(
                    f,
                    "idempotency key `{key}` remained pending after {} ms",
                    waited.as_millis()
                )
            }
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
            IdempotentRepositoryError::IdempotencyPendingTimeout { .. } => None,
        }
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl<V> crate::async_api::AsyncIdempotencyStore<V> for InMemoryIdempotencyStore<V>
where
    V: Clone + Send + Sync + 'static,
{
    type Error = InMemoryIdempotencyError;

    async fn load(&self, key: &IdempotencyKey) -> Result<Option<IdempotencyState<V>>, Self::Error> {
        IdempotencyStore::load(self, key)
    }

    async fn reserve(&self, key: IdempotencyKey) -> Result<bool, Self::Error> {
        IdempotencyStore::reserve(self, key)
    }

    async fn save(&self, key: IdempotencyKey, value: V) -> Result<(), Self::Error> {
        IdempotencyStore::save(self, key, value)
    }

    async fn remove(&self, key: &IdempotencyKey) -> Result<(), Self::Error> {
        IdempotencyStore::remove(self, key)
    }
}
