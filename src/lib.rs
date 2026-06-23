//! # ddd_cqrs_es
//!
//! A small, dependency-free framework for building Domain-Driven Design,
//! CQRS, and Event Sourcing applications in Rust.
//!
//! The crate gives you the core abstractions:
//!
//! - [`Aggregate`] for event-sourced domain state
//! - [`CommandHandler`] for deciding which events a command produces
//! - [`EventStore`] for loading and appending events
//! - [`Repository`] for loading aggregates and saving new events
//! - [`Projection`] for read-model updates
//!
//! The included [`InMemoryEventStore`] is intended for tests, examples, and
//! local development. Production systems should implement [`EventStore`] using
//! their database/event-log of choice.

pub mod aggregate;
pub mod command;
pub mod error;
pub mod event;
pub mod projection;
pub mod repository;
pub mod store;

pub use aggregate::{Aggregate, LoadedAggregate};
pub use command::CommandHandler;
pub use error::{EventStoreError, ExecuteError};
pub use event::{EventEnvelope, ExpectedRevision, NewEvent, Revision, INITIAL_REVISION};
pub use projection::Projection;
pub use repository::Repository;
pub use store::{EventStore, InMemoryEventStore};
