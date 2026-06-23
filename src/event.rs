use crate::metadata::Metadata;
use std::any::type_name;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// The current stream revision of an aggregate.
///
/// Revision `0` means the stream is empty. The first persisted event has
/// revision `1`.
pub type Revision = u64;

/// The revision of an aggregate stream with no persisted events.
pub const INITIAL_REVISION: Revision = 0;

/// A unique identifier assigned to a persisted event.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventId(String);

impl EventId {
    /// Creates a process-local unique event identifier.
    pub fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(1);

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        let next = COUNTER.fetch_add(1, Ordering::Relaxed);

        Self(format!("evt-{nanos:x}-{next:x}"))
    }

    /// Creates an event identifier from an existing stable value.
    pub fn from_string(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the identifier as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for EventId {
    fn default() -> Self {
        Self::new()
    }
}

impl Display for EventId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Concurrency expectation used when appending events.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpectedRevision {
    /// Append without checking the current stream revision.
    Any,
    /// Append only if the stream does not exist yet.
    NoStream,
    /// Append only if the stream is currently at this exact revision.
    Exact(Revision),
}

/// A domain event before persistence assigns revision and sequence values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewEvent<E> {
    /// Event payload supplied by aggregate command handling.
    pub payload: E,
    /// Stable event type name used by storage adapters and projections.
    pub event_type: String,
    /// Schema version for manual migrations and future upcasting support.
    pub event_version: u32,
    /// Audit, tracing, tenancy, and causality metadata.
    pub metadata: Metadata,
}

impl<E> NewEvent<E> {
    /// Creates a new event using the Rust payload type name as event type.
    ///
    /// Production applications should prefer [`Self::with_type`] with an
    /// explicit stable name.
    pub fn new(payload: E, metadata: Metadata) -> Self {
        Self {
            payload,
            event_type: type_name::<E>().to_owned(),
            event_version: 1,
            metadata,
        }
    }

    /// Creates a new event with an explicit stable event type.
    pub fn with_type(payload: E, event_type: impl Into<String>, metadata: Metadata) -> Self {
        Self {
            payload,
            event_type: event_type.into(),
            event_version: 1,
            metadata,
        }
    }

    /// Sets the schema version for this event.
    pub fn with_version(mut self, event_version: u32) -> Self {
        self.event_version = event_version;
        self
    }
}

/// A persisted event with stream identity, revision, metadata, and timestamps.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventEnvelope<E, Id> {
    /// Unique event identifier.
    pub event_id: EventId,
    /// Aggregate stream identifier.
    pub aggregate_id: Id,
    /// Stable aggregate type name.
    pub aggregate_type: String,
    /// Per-aggregate stream revision assigned on append.
    pub revision: Revision,
    /// Global append order assigned by stores that support sequencing.
    pub sequence: Option<u64>,
    /// Stable event type name.
    pub event_type: String,
    /// Event schema version.
    pub event_version: u32,
    /// Domain event payload.
    pub payload: E,
    /// Audit, tracing, tenancy, and causality metadata.
    pub metadata: Metadata,
    /// Time the event was recorded by the event store.
    pub recorded_at: SystemTime,
}

impl<E, Id> EventEnvelope<E, Id> {
    /// Creates a persisted event envelope.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        event_id: EventId,
        aggregate_id: Id,
        aggregate_type: impl Into<String>,
        revision: Revision,
        sequence: Option<u64>,
        event_type: impl Into<String>,
        event_version: u32,
        payload: E,
        metadata: Metadata,
        recorded_at: SystemTime,
    ) -> Self {
        Self {
            event_id,
            aggregate_id,
            aggregate_type: aggregate_type.into(),
            revision,
            sequence,
            event_type: event_type.into(),
            event_version,
            payload,
            metadata,
            recorded_at,
        }
    }

    /// Returns a reference to the domain event payload.
    pub fn event(&self) -> &E {
        &self.payload
    }

    /// Maps the event payload while preserving the envelope metadata.
    pub fn map_payload<T>(self, f: impl FnOnce(E) -> T) -> EventEnvelope<T, Id> {
        EventEnvelope {
            event_id: self.event_id,
            aggregate_id: self.aggregate_id,
            aggregate_type: self.aggregate_type,
            revision: self.revision,
            sequence: self.sequence,
            event_type: self.event_type,
            event_version: self.event_version,
            payload: f(self.payload),
            metadata: self.metadata,
            recorded_at: self.recorded_at,
        }
    }
}
