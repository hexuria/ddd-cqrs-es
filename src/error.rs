use crate::event::{ExpectedRevision, Revision};
use std::error::Error;
use std::fmt::{Display, Formatter};

/// Errors produced by an [`EventStore`](crate::EventStore).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventStoreError {
    /// Optimistic concurrency check failed.
    Conflict {
        expected: ExpectedRevision,
        actual: Revision,
    },
    /// Shared state was poisoned, usually because a previous thread panicked
    /// while holding a lock.
    Poisoned,
    /// Adapter-specific failure.
    Backend(String),
}

impl Display for EventStoreError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            EventStoreError::Conflict { expected, actual } => {
                write!(
                    f,
                    "event stream conflict: expected {:?}, actual revision {}",
                    expected, actual
                )
            }
            EventStoreError::Poisoned => write!(f, "event store lock was poisoned"),
            EventStoreError::Backend(message) => write!(f, "event store backend error: {message}"),
        }
    }
}

impl Error for EventStoreError {}

/// Error returned by [`Repository::execute`](crate::Repository::execute).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecuteError<DomainError> {
    Store(EventStoreError),
    Domain(DomainError),
}

impl<DomainError> From<EventStoreError> for ExecuteError<DomainError> {
    fn from(value: EventStoreError) -> Self {
        ExecuteError::Store(value)
    }
}

impl<DomainError: Display> Display for ExecuteError<DomainError> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecuteError::Store(error) => Display::fmt(error, f),
            ExecuteError::Domain(error) => Display::fmt(error, f),
        }
    }
}

impl<DomainError> Error for ExecuteError<DomainError>
where
    DomainError: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ExecuteError::Store(error) => Some(error),
            ExecuteError::Domain(error) => Some(error),
        }
    }
}
