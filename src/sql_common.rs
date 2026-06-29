use crate::error::EventStoreError;
use crate::{ConcurrencyError, ExpectedRevision};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[allow(dead_code)]
pub(crate) fn validate_table_name(table_name: &str) -> Result<(), EventStoreError> {
    let mut chars = table_name.chars();
    let Some(first) = chars.next() else {
        return Err(EventStoreError::Backend(
            "SQL event table name cannot be empty".to_owned(),
        ));
    };

    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(EventStoreError::Backend(format!(
            "invalid SQL event table name `{table_name}`"
        )));
    }

    if chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric()) {
        Ok(())
    } else {
        Err(EventStoreError::Backend(format!(
            "invalid SQL event table name `{table_name}`"
        )))
    }
}

#[allow(dead_code)]
pub(crate) fn check_expected_revision(
    expected: ExpectedRevision,
    actual: u64,
) -> Result<(), EventStoreError> {
    match expected {
        ExpectedRevision::Any => Ok(()),
        ExpectedRevision::NoStream if actual == 0 => Ok(()),
        ExpectedRevision::NoStream => Err(EventStoreError::Concurrency(
            ConcurrencyError::StreamAlreadyExists,
        )),
        ExpectedRevision::Exact(expected) if expected == actual => Ok(()),
        ExpectedRevision::Exact(_) => Err(EventStoreError::Concurrency(
            ConcurrencyError::WrongExpectedRevision { expected, actual },
        )),
    }
}

#[allow(dead_code)]
pub(crate) fn system_time_to_millis(recorded_at: SystemTime) -> Result<i64, EventStoreError> {
    let duration = recorded_at.duration_since(UNIX_EPOCH).map_err(|error| {
        EventStoreError::Serialization(format!("recorded_at is before UNIX_EPOCH: {error}"))
    })?;

    i64::try_from(duration.as_millis()).map_err(|_| {
        EventStoreError::Serialization("recorded_at timestamp exceeds i64 millis".to_owned())
    })
}

#[allow(dead_code)]
pub(crate) fn millis_to_system_time(millis: i64) -> Result<SystemTime, EventStoreError> {
    let millis = u64::try_from(millis).map_err(|_| {
        EventStoreError::Deserialization("recorded_at_ms cannot be negative".to_owned())
    })?;

    Ok(UNIX_EPOCH + Duration::from_millis(millis))
}

#[cfg(feature = "json")]
#[allow(dead_code)]
pub(crate) fn serialize_id<Id>(id: &Id) -> Result<String, EventStoreError>
where
    Id: serde::Serialize,
{
    serde_json::to_string(id)
        .map_err(|error| EventStoreError::Serialization(format!("aggregate_id: {error}")))
}

#[cfg(feature = "json")]
#[allow(dead_code)]
pub(crate) fn deserialize_id<Id>(value: &str) -> Result<Id, EventStoreError>
where
    Id: serde::de::DeserializeOwned,
{
    serde_json::from_str(value)
        .map_err(|error| EventStoreError::Deserialization(format!("aggregate_id: {error}")))
}

#[cfg(feature = "json")]
#[allow(dead_code)]
pub(crate) fn serialize_payload<E>(event: &E) -> Result<serde_json::Value, EventStoreError>
where
    E: serde::Serialize,
{
    serde_json::to_value(event)
        .map_err(|error| EventStoreError::Serialization(format!("event payload: {error}")))
}

#[cfg(feature = "json")]
#[allow(dead_code)]
pub(crate) fn deserialize_payload<E>(
    event_id: &str,
    event_type: &str,
    value: serde_json::Value,
) -> Result<E, EventStoreError>
where
    E: serde::de::DeserializeOwned,
{
    serde_json::from_value(value).map_err(|error| {
        EventStoreError::Deserialization(format!(
            "event_id `{event_id}` event_type `{event_type}` payload: {error}"
        ))
    })
}

#[cfg(feature = "json")]
#[allow(dead_code)]
pub(crate) fn serialize_metadata(
    metadata: &crate::Metadata,
) -> Result<serde_json::Value, EventStoreError> {
    serde_json::to_value(metadata)
        .map_err(|error| EventStoreError::Serialization(format!("metadata: {error}")))
}

#[cfg(feature = "json")]
#[allow(dead_code)]
pub(crate) fn deserialize_metadata(
    event_id: &str,
    value: serde_json::Value,
) -> Result<crate::Metadata, EventStoreError> {
    serde_json::from_value(value).map_err(|error| {
        EventStoreError::Deserialization(format!("event_id `{event_id}` metadata: {error}"))
    })
}
