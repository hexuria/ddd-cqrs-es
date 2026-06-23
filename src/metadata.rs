use std::collections::BTreeMap;

/// Metadata carried with commands and persisted event envelopes.
///
/// The fields support tracing, auditability, multi-tenancy, and event
/// causality without requiring a specific observability crate.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Metadata {
    /// Correlates all work belonging to the same business request.
    pub correlation_id: Option<String>,
    /// Identifies the command or event that caused this event.
    pub causation_id: Option<String>,
    /// Identifies the user, service, or process that initiated the change.
    pub actor_id: Option<String>,
    /// Identifies the tenant when applications use multi-tenancy.
    pub tenant_id: Option<String>,
    /// Identifies the external request that initiated the change.
    pub request_id: Option<String>,
    /// Additional adapter or application-specific metadata.
    pub headers: BTreeMap<String, String>,
}

impl Metadata {
    /// Creates empty metadata.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the correlation ID.
    pub fn with_correlation_id(mut self, value: impl Into<String>) -> Self {
        self.correlation_id = Some(value.into());
        self
    }

    /// Sets the causation ID.
    pub fn with_causation_id(mut self, value: impl Into<String>) -> Self {
        self.causation_id = Some(value.into());
        self
    }

    /// Sets the actor ID.
    pub fn with_actor_id(mut self, value: impl Into<String>) -> Self {
        self.actor_id = Some(value.into());
        self
    }

    /// Sets the tenant ID.
    pub fn with_tenant_id(mut self, value: impl Into<String>) -> Self {
        self.tenant_id = Some(value.into());
        self
    }

    /// Sets the request ID.
    pub fn with_request_id(mut self, value: impl Into<String>) -> Self {
        self.request_id = Some(value.into());
        self
    }

    /// Inserts an arbitrary metadata header.
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }
}
