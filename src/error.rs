use crate::event::{ExpectedRevision, Revision};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// Optimistic concurrency failure.
///
/// # Example
///
/// ```rust
/// use ddd_cqrs_es::{ConcurrencyError, ExpectedRevision};
///
/// let error = ConcurrencyError::WrongExpectedRevision {
///     expected: ExpectedRevision::Exact(4),
///     actual: 3,
/// };
/// assert_eq!(
///     error.to_string(),
///     "wrong expected revision: expected Exact(4), actual revision 3"
/// );
/// ```
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
///
/// # Example
///
/// ```rust
/// use ddd_cqrs_es::{EventStoreError, ConcurrencyError};
/// use std::error::Error;
///
/// let error = EventStoreError::Concurrency(ConcurrencyError::StreamAlreadyExists);
/// assert!(error.source().is_some());
/// ```
/// Stored source error used when a backend-specific error type cannot be part
/// of the public enum without leaking adapter implementation details.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventStoreErrorSource {
    message: String,
}

impl EventStoreErrorSource {
    /// Creates a stored source error from an adapter error message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl Display for EventStoreErrorSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for EventStoreErrorSource {}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug)]
pub enum EventStoreError {
    /// Optimistic concurrency check failed.
    Concurrency(ConcurrencyError),
    /// Event serialization failed.
    Serialization(String),
    /// Event serialization failed with a preserved source error.
    SerializationWithSource {
        /// Public error message.
        message: String,
        /// Source error for error-chain aware callers.
        #[cfg_attr(feature = "serde", serde(skip))]
        source: Option<Arc<EventStoreErrorSource>>,
    },
    /// Event deserialization failed.
    Deserialization(String),
    /// Event deserialization failed with a preserved source error.
    DeserializationWithSource {
        /// Public error message.
        message: String,
        /// Source error for error-chain aware callers.
        #[cfg_attr(feature = "serde", serde(skip))]
        source: Option<Arc<EventStoreErrorSource>>,
    },
    /// Backend connection or availability failure.
    Connection(String),
    /// Backend connection or availability failure with a preserved source error.
    ConnectionWithSource {
        /// Public error message.
        message: String,
        /// Source error for error-chain aware callers.
        #[cfg_attr(feature = "serde", serde(skip))]
        source: Option<Arc<EventStoreErrorSource>>,
    },
    /// Shared state was poisoned by a panic while holding a lock.
    Poisoned,
    /// Adapter-specific failure.
    Backend(String),
    /// Adapter-specific failure with a preserved source error.
    BackendWithSource {
        /// Public error message.
        message: String,
        /// Source error for error-chain aware callers.
        #[cfg_attr(feature = "serde", serde(skip))]
        source: Option<Arc<EventStoreErrorSource>>,
    },
    /// Unknown adapter failure.
    Unknown(String),
    /// Unknown adapter failure with a preserved source error.
    UnknownWithSource {
        /// Public error message.
        message: String,
        /// Source error for error-chain aware callers.
        #[cfg_attr(feature = "serde", serde(skip))]
        source: Option<Arc<EventStoreErrorSource>>,
    },
}

impl EventStoreError {
    /// Creates a serialization error that preserves source context.
    pub fn serialization_with_source(message: impl Into<String>, source: impl Display) -> Self {
        Self::SerializationWithSource {
            message: message.into(),
            source: Some(Arc::new(EventStoreErrorSource::new(source.to_string()))),
        }
    }

    /// Creates a deserialization error that preserves source context.
    pub fn deserialization_with_source(message: impl Into<String>, source: impl Display) -> Self {
        Self::DeserializationWithSource {
            message: message.into(),
            source: Some(Arc::new(EventStoreErrorSource::new(source.to_string()))),
        }
    }

    /// Creates a connection error that preserves source context.
    pub fn connection_with_source(message: impl Into<String>, source: impl Display) -> Self {
        Self::ConnectionWithSource {
            message: message.into(),
            source: Some(Arc::new(EventStoreErrorSource::new(source.to_string()))),
        }
    }

    /// Creates a backend error that preserves source context.
    pub fn backend_with_source(message: impl Into<String>, source: impl Display) -> Self {
        Self::BackendWithSource {
            message: message.into(),
            source: Some(Arc::new(EventStoreErrorSource::new(source.to_string()))),
        }
    }

    /// Creates an unknown error that preserves source context.
    pub fn unknown_with_source(message: impl Into<String>, source: impl Display) -> Self {
        Self::UnknownWithSource {
            message: message.into(),
            source: Some(Arc::new(EventStoreErrorSource::new(source.to_string()))),
        }
    }
}

impl PartialEq for EventStoreError {
    fn eq(&self, other: &Self) -> bool {
        use EventStoreError::*;

        match (self, other) {
            (Concurrency(left), Concurrency(right)) => left == right,
            (Serialization(left), Serialization(right)) => left == right,
            (
                SerializationWithSource { message: left, .. },
                SerializationWithSource { message: right, .. },
            ) => left == right,
            (Deserialization(left), Deserialization(right)) => left == right,
            (
                DeserializationWithSource { message: left, .. },
                DeserializationWithSource { message: right, .. },
            ) => left == right,
            (Connection(left), Connection(right)) => left == right,
            (
                ConnectionWithSource { message: left, .. },
                ConnectionWithSource { message: right, .. },
            ) => left == right,
            (Poisoned, Poisoned) => true,
            (Backend(left), Backend(right)) => left == right,
            (BackendWithSource { message: left, .. }, BackendWithSource { message: right, .. }) => {
                left == right
            }
            (Unknown(left), Unknown(right)) => left == right,
            (UnknownWithSource { message: left, .. }, UnknownWithSource { message: right, .. }) => {
                left == right
            }
            _ => false,
        }
    }
}

impl Eq for EventStoreError {}

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
            EventStoreError::SerializationWithSource { message, .. } => {
                write!(f, "serialization error: {message}")
            }
            EventStoreError::Deserialization(message) => {
                write!(f, "deserialization error: {message}")
            }
            EventStoreError::DeserializationWithSource { message, .. } => {
                write!(f, "deserialization error: {message}")
            }
            EventStoreError::Connection(message) => write!(f, "connection error: {message}"),
            EventStoreError::ConnectionWithSource { message, .. } => {
                write!(f, "connection error: {message}")
            }
            EventStoreError::Poisoned => f.write_str("event store lock was poisoned"),
            EventStoreError::Backend(message) => write!(f, "event store backend error: {message}"),
            EventStoreError::BackendWithSource { message, .. } => {
                write!(f, "event store backend error: {message}")
            }
            EventStoreError::Unknown(message) => write!(f, "unknown event store error: {message}"),
            EventStoreError::UnknownWithSource { message, .. } => {
                write!(f, "unknown event store error: {message}")
            }
        }
    }
}

impl Error for EventStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            EventStoreError::Concurrency(error) => Some(error),
            EventStoreError::SerializationWithSource { source, .. }
            | EventStoreError::DeserializationWithSource { source, .. }
            | EventStoreError::ConnectionWithSource { source, .. }
            | EventStoreError::BackendWithSource { source, .. }
            | EventStoreError::UnknownWithSource { source, .. } => source
                .as_deref()
                .map(|source| source as &(dyn Error + 'static)),
            _ => None,
        }
    }
}

/// Error returned by repository operations.
///
/// # Example
///
/// ```rust
/// use ddd_cqrs_es::{RepositoryError, EventStoreError};
///
/// let store_err = EventStoreError::Connection("db offline".to_string());
/// let error: RepositoryError<&'static str, EventStoreError> = RepositoryError::Store(store_err);
/// assert_eq!(error.to_string(), "connection error: db offline");
/// ```
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
