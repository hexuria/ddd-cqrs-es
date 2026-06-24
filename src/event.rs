use crate::metadata::Metadata;
use std::fmt::{Display, Formatter};
#[cfg(not(feature = "uuid"))]
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;
#[cfg(not(feature = "uuid"))]
use std::time::UNIX_EPOCH;

/// The current stream revision of an aggregate.
///
/// Revision `0` means the stream is empty. The first persisted event has
/// revision `1`.
pub type Revision = u64;

/// The revision of an aggregate stream with no persisted events.
pub const INITIAL_REVISION: Revision = 0;

/// Stable event type name stored with an event envelope.
pub type EventType = String;

/// A unique identifier assigned to a persisted event.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventId(String);

impl EventId {
    /// Creates a unique event identifier.
    ///
    /// With the `uuid` feature enabled, this uses UUID v4. Without that
    /// feature it falls back to a process-local identifier suitable for tests.
    pub fn new() -> Self {
        #[cfg(feature = "uuid")]
        {
            Self(uuid::Uuid::new_v4().to_string())
        }

        #[cfg(not(feature = "uuid"))]
        {
            static COUNTER: AtomicU64 = AtomicU64::new(1);

            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or_default();
            let next = COUNTER.fetch_add(1, Ordering::Relaxed);

            Self(format!("evt-{nanos:x}-{next:x}"))
        }
    }

    /// Creates an event identifier from an existing stable value.
    pub fn from_string(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Creates an event identifier from a UUID.
    #[cfg(feature = "uuid")]
    pub fn from_uuid(value: uuid::Uuid) -> Self {
        Self(value.to_string())
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

/// Domain event metadata supplied by the event payload.
///
/// Implement this for event enums so stored envelopes use stable event names
/// and schema versions instead of Rust type paths.
pub trait DomainEvent: Clone + Send + Sync + 'static {
    /// Stable event type name, such as `account_opened`.
    fn event_type(&self) -> &'static str;

    /// Event schema version. Increment when the payload schema changes.
    fn event_version(&self) -> u32 {
        1
    }
}

/// Concurrency expectation used when appending events.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewEvent<E> {
    /// Event payload supplied by aggregate command handling.
    pub payload: E,
    /// Stable event type name used by storage adapters and projections.
    pub event_type: EventType,
    /// Schema version for manual migrations and future upcasting support.
    pub event_version: u32,
    /// Audit, tracing, tenancy, and causality metadata.
    pub metadata: Metadata,
}

impl<E> NewEvent<E> {
    /// Creates a new event using stable metadata from [`DomainEvent`].
    pub fn new(payload: E, metadata: Metadata) -> Self
    where
        E: DomainEvent,
    {
        Self::from_domain_event(payload, metadata)
    }

    /// Creates a new event using stable metadata from [`DomainEvent`].
    pub fn from_domain_event(payload: E, metadata: Metadata) -> Self
    where
        E: DomainEvent,
    {
        let event_type = payload.event_type().to_owned();
        let event_version = payload.event_version();

        Self {
            payload,
            event_type,
            event_version,
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
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
    pub event_type: EventType,
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

    /// Returns the recording time as a chrono UTC timestamp.
    #[cfg(feature = "chrono")]
    pub fn recorded_at_utc(&self) -> chrono::DateTime<chrono::Utc> {
        self.recorded_at.into()
    }

    /// Serializes this envelope as JSON.
    #[cfg(feature = "json")]
    pub fn to_json(&self) -> Result<String, crate::error::EventStoreError>
    where
        E: serde::Serialize,
        Id: serde::Serialize,
    {
        serde_json::to_string(self)
            .map_err(|error| crate::error::EventStoreError::Serialization(error.to_string()))
    }

    /// Deserializes an envelope from JSON.
    #[cfg(feature = "json")]
    pub fn from_json(json: &str) -> Result<Self, crate::error::EventStoreError>
    where
        E: serde::de::DeserializeOwned,
        Id: serde::de::DeserializeOwned,
    {
        serde_json::from_str(json)
            .map_err(|error| crate::error::EventStoreError::Deserialization(error.to_string()))
    }
}
