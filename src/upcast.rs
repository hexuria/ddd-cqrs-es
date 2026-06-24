/// Converts serialized event payloads from one schema version to another.
///
/// Upcasters operate on raw bytes so storage adapters can use JSON, MessagePack,
/// protobuf, or another encoding without coupling the core crate to that format.
pub trait EventUpcaster {
    /// Upcaster error.
    type Error;

    /// Source schema version.
    fn source_version(&self) -> u32;

    /// Target schema version.
    fn target_version(&self) -> u32;

    /// Converts one raw event payload into the next schema version.
    fn upcast(&self, raw_payload: Vec<u8>) -> Result<Vec<u8>, Self::Error>;
}
