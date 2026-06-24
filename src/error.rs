use crate::event::{ExpectedRevision, Revision};
use std::error::Error;
use std::fmt::{Display, Formatter};

/// Optimistic concurrency failure.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConcurrencyError {
    /// The append expected an empty stream, but events already exist.
    StreamAlreadyExists,
    /// The append expected one revision but found another.
    WrongExpectedRevision {
        /// Expected revision constraint.
        expected: ExpectedRevision,
        /// Actual stream revision at append time.
        actual: Revision,
    },
}

impl Display for ConcurrencyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConcurrencyError::StreamAlreadyExists => f.write_str("event stream already exists"),
            ConcurrencyError::WrongExpectedRevision { expected, actual } => {
                write!(
                    f,
                    "wrong expected revision: expected {:?}, actual revision {}",
                    expected, actual
                )
            }
        }
    }
}

impl Error for ConcurrencyError {}

/// Errors produced by event store implementations.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventStoreError {
    /// Optimistic concurrency check failed.
    Concurrency(ConcurrencyError),
    /// Event serialization failed.
    Serialization(String),
    /// Event deserialization failed.
    Deserialization(String),
    /// Backend connection or availability failure.
    Connection(String),
    /// Shared state was poisoned by a panic while holding a lock.
    Poisoned,
    /// Adapter-specific failure.
    Backend(String),
    /// Unknown adapter failure.
    Unknown(String),
}

/// Classifies store errors for repository-level error mapping.
///
/// Custom event stores can implement this trait to surface concurrency failures
/// as [`RepositoryError::Concurrency`] while preserving all other errors as
/// [`RepositoryError::Store`].
pub trait EventStoreFailure: Sized {
    /// Converts a store error into a repository error.
    fn into_repository_error<DomainError>(self) -> RepositoryError<DomainError, Self> {
        RepositoryError::Store(self)
    }
}

impl EventStoreFailure for EventStoreError {
    fn into_repository_error<DomainError>(self) -> RepositoryError<DomainError, Self> {
        match self {
            EventStoreError::Concurrency(error) => RepositoryError::Concurrency(error),
            error => RepositoryError::Store(error),
        }
    }
}

impl Display for EventStoreError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            EventStoreError::Concurrency(error) => Display::fmt(error, f),
            EventStoreError::Serialization(message) => write!(f, "serialization error: {message}"),
            EventStoreError::Deserialization(message) => {
                write!(f, "deserialization error: {message}")
            }
            EventStoreError::Connection(message) => write!(f, "connection error: {message}"),
            EventStoreError::Poisoned => f.write_str("event store lock was poisoned"),
            EventStoreError::Backend(message) => write!(f, "event store backend error: {message}"),
            EventStoreError::Unknown(message) => write!(f, "unknown event store error: {message}"),
        }
    }
}

impl Error for EventStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            EventStoreError::Concurrency(error) => Some(error),
            _ => None,
        }
    }
}

/// Error returned by repository operations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepositoryError<DomainError, StoreError = EventStoreError> {
    /// Aggregate command handling rejected the command.
    Domain(DomainError),
    /// Event store rejected the append due to optimistic concurrency.
    Concurrency(ConcurrencyError),
    /// Event store or infrastructure operation failed.
    Store(StoreError),
}

impl<DomainError, StoreError> Display for RepositoryError<DomainError, StoreError>
where
    DomainError: Display,
    StoreError: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RepositoryError::Domain(error) => Display::fmt(error, f),
            RepositoryError::Concurrency(error) => Display::fmt(error, f),
            RepositoryError::Store(error) => Display::fmt(error, f),
        }
    }
}

impl<DomainError, StoreError> Error for RepositoryError<DomainError, StoreError>
where
    DomainError: Error + 'static,
    StoreError: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            RepositoryError::Domain(error) => Some(error),
            RepositoryError::Concurrency(error) => Some(error),
            RepositoryError::Store(error) => Some(error),
        }
    }
}

impl<DomainError> From<EventStoreError> for RepositoryError<DomainError, EventStoreError> {
    fn from(value: EventStoreError) -> Self {
        match value {
            EventStoreError::Concurrency(error) => RepositoryError::Concurrency(error),
            error => RepositoryError::Store(error),
        }
    }
}
