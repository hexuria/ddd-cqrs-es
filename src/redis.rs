//! Experimental Redis event store, checkpoint store, and pub/sub helpers.
//!
//! Redis support is async-only in this crate. The event store uses Redis as a
//! persistence backend with optimistic concurrency enforced by one Lua `EVAL`
//! append script. Pub/sub publishing is intentionally separate from event
//! durability; Redis messages are notifications and must not be treated as the
//! source of truth.

use crate::aggregate::Aggregate;
use crate::async_api::AsyncEventStore;
use crate::error::EventStoreError;
use crate::event::{EventEnvelope, EventId, ExpectedRevision, NewEvent};
use crate::event_store::EventStream;
use crate::projection::AsyncCheckpointStore;
use crate::sql_common::{
    check_expected_revision, deserialize_id, deserialize_metadata, deserialize_payload,
    millis_to_system_time, serialize_id, serialize_metadata, serialize_payload,
    system_time_to_millis,
};
use crate::upcast::UpcasterRegistry;
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
#[cfg(feature = "wasi-redis")]
use std::io::{BufRead, BufReader, ErrorKind, Read, Write};
use std::marker::PhantomData;
#[cfg(feature = "wasi-redis")]
use std::net::TcpStream;
#[cfg(feature = "wasi-redis")]
use std::time::Duration;
use std::time::SystemTime;

const DEFAULT_PREFIX: &str = "ddd_cqrs_es";
const DEFAULT_CHECKPOINT_PREFIX: &str = "ddd_cqrs_es";

const APPEND_LUA: &str = r#"
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
local expected_kind = ARGV[1]
local expected_revision = tonumber(ARGV[2])
local count = tonumber(ARGV[3])
local event_key_prefix = ARGV[4]

if expected_kind == 'no_stream' and current ~= 0 then
    return {'ERR', 'stream_exists', current}
end

if expected_kind == 'exact' and current ~= expected_revision then
    return {'ERR', 'wrong_revision', current}
end

if count == 0 then
    return {'OK', 0, 0, current}
end

local last_sequence = redis.call('INCRBY', KEYS[2], count)
local first_sequence = last_sequence - count + 1

for i = 0, count - 1 do
    local base = 5 + (i * 8)
    local revision = current + i + 1
    local sequence = first_sequence + i
    local event_key = event_key_prefix .. tostring(sequence)

    redis.call(
        'HSET',
        event_key,
        'event_id', ARGV[base],
        'aggregate_id', ARGV[base + 1],
        'aggregate_type', ARGV[base + 2],
        'revision', tostring(revision),
        'sequence', tostring(sequence),
        'event_type', ARGV[base + 3],
        'event_version', ARGV[base + 4],
        'payload', ARGV[base + 5],
        'metadata', ARGV[base + 6],
        'recorded_at_ms', ARGV[base + 7]
    )
    redis.call('ZADD', KEYS[3], revision, tostring(sequence))
    redis.call('ZADD', KEYS[4], sequence, tostring(sequence))
end

redis.call('SET', KEYS[1], current + count)
return {'OK', first_sequence, last_sequence, current + count}
"#;

/// Redis protocol value returned by [`RedisCommandExecutor`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RedisValue {
    /// Redis null/nil value.
    Nil,
    /// Simple string status value.
    Status(String),
    /// Integer value.
    Int(i64),
    /// Bulk byte value.
    Bytes(Vec<u8>),
    /// RESP array value.
    Array(Vec<RedisValue>),
}

/// Minimal async Redis command abstraction used by the experimental Redis
/// event store.
#[async_trait]
pub trait RedisCommandExecutor: Clone + Send + Sync + 'static {
    /// Executor-specific error type.
    type Error: Display + Send + Sync + 'static;

    /// Executes one Redis command with already encoded binary arguments.
    async fn execute(&self, command: &str, args: Vec<Vec<u8>>) -> Result<RedisValue, Self::Error>;

    /// Publishes a notification payload to a Redis channel.
    async fn publish(&self, channel: &str, payload: &[u8]) -> Result<(), Self::Error> {
        let _ = self
            .execute(
                "PUBLISH",
                vec![channel.as_bytes().to_vec(), payload.to_vec()],
            )
            .await?;
        Ok(())
    }
}

/// Experimental Redis-backed async event store.
///
/// This adapter is intentionally not a sync [`crate::EventStore`]
/// implementation. Redis host APIs used by Spin and the WASI example are
/// async, so the stable surface for this backend is [`AsyncEventStore`].
pub struct RedisEventStore<A, C>
where
    A: Aggregate + Send + Sync,
    C: RedisCommandExecutor,
{
    client: C,
    prefix: String,
    upcasters: UpcasterRegistry,
    _marker: PhantomData<fn() -> A>,
}

impl<A, C> Clone for RedisEventStore<A, C>
where
    A: Aggregate + Send + Sync,
    C: RedisCommandExecutor,
{
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            prefix: self.prefix.clone(),
            upcasters: self.upcasters.clone(),
            _marker: PhantomData,
        }
    }
}

impl<A, C> std::fmt::Debug for RedisEventStore<A, C>
where
    A: Aggregate + Send + Sync,
    C: RedisCommandExecutor,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisEventStore")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl<A, C> RedisEventStore<A, C>
where
    A: Aggregate + Send + Sync,
    C: RedisCommandExecutor,
{
    /// Creates a Redis event store with the default `ddd_cqrs_es` key prefix.
    pub fn new(client: C) -> Self {
        Self {
            client,
            prefix: DEFAULT_PREFIX.to_owned(),
            upcasters: UpcasterRegistry::new(),
            _marker: PhantomData,
        }
    }

    /// Creates a Redis event store with a custom key prefix.
    pub fn with_prefix(client: C, prefix: impl Into<String>) -> Result<Self, EventStoreError> {
        let prefix = prefix.into();
        validate_redis_prefix(&prefix)?;

        Ok(Self {
            client,
            prefix,
            upcasters: UpcasterRegistry::new(),
            _marker: PhantomData,
        })
    }

    /// Returns the Redis command executor.
    pub fn client(&self) -> &C {
        &self.client
    }

    /// Returns the key prefix used by this store.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Returns the upcaster registry.
    pub fn upcasters(&self) -> &UpcasterRegistry {
        &self.upcasters
    }

    /// Registers a sequential schema version upcaster for a specific event type.
    pub fn register_upcaster<U>(&self, event_type: impl Into<String>, upcaster: U)
    where
        U: crate::upcast::EventUpcaster + Send + Sync + 'static,
        U::Error: std::fmt::Debug + Display + Send + Sync + 'static,
    {
        self.upcasters.register(event_type, upcaster);
    }

    fn event_key_prefix(&self) -> String {
        format!("{}:event:", self.prefix)
    }

    fn sequence_key(&self) -> String {
        format!("{}:seq", self.prefix)
    }

    fn global_key(&self) -> String {
        format!("{}:global", self.prefix)
    }

    fn event_key(&self, sequence: u64) -> String {
        format!("{}{}", self.event_key_prefix(), sequence)
    }

    fn stream_keys(&self, aggregate_id: &A::Id) -> Result<RedisStreamKeys, EventStoreError>
    where
        A::Id: serde::Serialize,
    {
        let aggregate_id_json = serialize_id(aggregate_id)?;
        let aggregate_type_key = hex_encode(A::aggregate_type().as_bytes());
        let aggregate_id_key = hex_encode(aggregate_id_json.as_bytes());

        Ok(RedisStreamKeys {
            aggregate_id_json,
            revision_key: format!(
                "{}:revision:{}:{}",
                self.prefix, aggregate_type_key, aggregate_id_key
            ),
            stream_key: format!(
                "{}:stream:{}:{}",
                self.prefix, aggregate_type_key, aggregate_id_key
            ),
        })
    }

    async fn current_revision(&self, revision_key: &str) -> Result<u64, EventStoreError> {
        let value = self
            .client
            .execute("GET", vec![revision_key.as_bytes().to_vec()])
            .await
            .map_err(map_executor_error)?;
        redis_optional_u64(&value, "stream revision")
    }

    async fn load_sequence(
        &self,
        sequence: u64,
    ) -> Result<EventEnvelope<A::Event, A::Id>, EventStoreError>
    where
        A::Event: serde::de::DeserializeOwned,
        A::Id: serde::de::DeserializeOwned,
    {
        let hash = self.load_sequence_hash(sequence).await?;
        hash_to_envelope::<A>(&self.upcasters, hash)
    }

    async fn load_sequence_hash(
        &self,
        sequence: u64,
    ) -> Result<BTreeMap<String, Vec<u8>>, EventStoreError> {
        let value = self
            .client
            .execute("HGETALL", vec![self.event_key(sequence).into_bytes()])
            .await
            .map_err(map_executor_error)?;
        let hash = redis_hash(&value)?;
        if hash.is_empty() {
            return Err(EventStoreError::Deserialization(format!(
                "Redis event sequence {sequence} is indexed but missing"
            )));
        }
        Ok(hash)
    }
}

#[async_trait]
impl<A, C> AsyncEventStore<A> for RedisEventStore<A, C>
where
    A: Aggregate + Send + Sync + 'static,
    A::Event: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    A::Id: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + Clone,
    C: RedisCommandExecutor,
{
    type Error = EventStoreError;

    async fn load(&self, aggregate_id: &A::Id) -> Result<EventStream<A>, Self::Error> {
        let keys = self.stream_keys(aggregate_id)?;
        let value = self
            .client
            .execute(
                "ZRANGE",
                vec![keys.stream_key.into_bytes(), b"0".to_vec(), b"-1".to_vec()],
            )
            .await
            .map_err(map_executor_error)?;

        let sequences = redis_sequence_list(&value)?;
        let mut events = Vec::with_capacity(sequences.len());
        for sequence in sequences {
            let hash = self.load_sequence_hash(sequence).await?;
            if hash_field_string(&hash, "aggregate_type")? == A::aggregate_type() {
                events.push(hash_to_envelope::<A>(&self.upcasters, hash)?);
            }
        }

        Ok(events)
    }

    async fn append(
        &self,
        aggregate_id: &A::Id,
        expected_revision: ExpectedRevision,
        events: Vec<NewEvent<A::Event>>,
    ) -> Result<EventStream<A>, Self::Error> {
        let keys = self.stream_keys(aggregate_id)?;
        let prepared = events
            .into_iter()
            .map(PreparedRedisEvent::new)
            .collect::<Result<Vec<_>, _>>()?;

        if prepared.is_empty() {
            let actual = self.current_revision(&keys.revision_key).await?;
            check_expected_revision(expected_revision, actual)?;
            return Ok(Vec::new());
        }

        let sequence_key = self.sequence_key();
        let global_key = self.global_key();
        let event_key_prefix = self.event_key_prefix();
        let args = build_append_eval_args(AppendEvalArgs {
            script: APPEND_LUA,
            aggregate_type: A::aggregate_type(),
            keys: &keys,
            sequence_key: &sequence_key,
            global_key: &global_key,
            event_key_prefix: &event_key_prefix,
            expected_revision,
            events: &prepared,
        });
        let value = self
            .client
            .execute("EVAL", args)
            .await
            .map_err(map_executor_error)?;
        let AppendEvalResult {
            first_sequence,
            next_revision,
            ..
        } = parse_append_eval_result(&value, expected_revision)?;
        let base_revision = next_revision
            .checked_sub(prepared.len() as u64)
            .ok_or_else(|| {
                EventStoreError::Deserialization(
                    "Redis append script returned revision smaller than event count".to_owned(),
                )
            })?;

        let mut committed = Vec::with_capacity(prepared.len());
        for (index, event) in prepared.into_iter().enumerate() {
            let revision = base_revision + index as u64 + 1;
            let sequence = first_sequence + index as u64;
            committed.push(EventEnvelope::new(
                event.event_id,
                aggregate_id.clone(),
                A::aggregate_type(),
                revision,
                Some(sequence),
                event.event_type,
                event.event_version,
                event.payload,
                event.metadata,
                event.recorded_at,
            ));
        }

        Ok(committed)
    }

    async fn load_global_after(
        &self,
        sequence: Option<u64>,
    ) -> Result<EventStream<A>, Self::Error> {
        let min_sequence = sequence.unwrap_or_default();
        let value = self
            .client
            .execute(
                "ZRANGEBYSCORE",
                vec![
                    self.global_key().into_bytes(),
                    format!("({min_sequence}").into_bytes(),
                    b"+inf".to_vec(),
                ],
            )
            .await
            .map_err(map_executor_error)?;

        let sequences = redis_sequence_list(&value)?;
        let mut events = Vec::with_capacity(sequences.len());
        for sequence in sequences {
            events.push(self.load_sequence(sequence).await?);
        }

        Ok(events)
    }
}

/// Experimental Redis-backed async checkpoint store.
#[derive(Clone, Debug)]
pub struct RedisCheckpointStore<C>
where
    C: RedisCommandExecutor,
{
    client: C,
    prefix: String,
}

impl<C> RedisCheckpointStore<C>
where
    C: RedisCommandExecutor,
{
    /// Creates a Redis checkpoint store with the default `ddd_cqrs_es` prefix.
    pub fn new(client: C) -> Self {
        Self {
            client,
            prefix: DEFAULT_CHECKPOINT_PREFIX.to_owned(),
        }
    }

    /// Creates a Redis checkpoint store with a custom key prefix.
    pub fn with_prefix(client: C, prefix: impl Into<String>) -> Result<Self, EventStoreError> {
        let prefix = prefix.into();
        validate_redis_prefix(&prefix)?;

        Ok(Self { client, prefix })
    }

    fn checkpoint_key(&self, projection_name: &str) -> String {
        format!(
            "{}:checkpoint:{}",
            self.prefix,
            hex_encode(projection_name.as_bytes())
        )
    }
}

#[async_trait]
impl<C> AsyncCheckpointStore for RedisCheckpointStore<C>
where
    C: RedisCommandExecutor,
{
    type Error = EventStoreError;

    async fn load_checkpoint(&self, projection_name: &str) -> Result<Option<u64>, Self::Error> {
        let value = self
            .client
            .execute(
                "GET",
                vec![self.checkpoint_key(projection_name).into_bytes()],
            )
            .await
            .map_err(map_executor_error)?;
        let sequence = redis_optional_u64(&value, "projection checkpoint")?;
        Ok((sequence != 0).then_some(sequence))
    }

    async fn save_checkpoint(
        &self,
        projection_name: &str,
        sequence: u64,
    ) -> Result<(), Self::Error> {
        let _ = self
            .client
            .execute(
                "SET",
                vec![
                    self.checkpoint_key(projection_name).into_bytes(),
                    sequence.to_string().into_bytes(),
                ],
            )
            .await
            .map_err(map_executor_error)?;
        Ok(())
    }
}

/// Redis notification publisher for read-model/realtime invalidation.
///
/// Publishing is best-effort notification only. Callers should commit events
/// and update projections first, then publish a message to wake clients.
#[derive(Clone, Debug)]
pub struct RedisPubSubPublisher<C>
where
    C: RedisCommandExecutor,
{
    client: C,
    channel: String,
}

impl<C> RedisPubSubPublisher<C>
where
    C: RedisCommandExecutor,
{
    /// Creates a publisher for one Redis channel.
    pub fn new(client: C, channel: impl Into<String>) -> Self {
        Self {
            client,
            channel: channel.into(),
        }
    }

    /// Returns the configured Redis channel.
    pub fn channel(&self) -> &str {
        &self.channel
    }

    /// Publishes a raw notification payload.
    pub async fn publish(&self, payload: &[u8]) -> Result<(), EventStoreError> {
        self.client
            .publish(&self.channel, payload)
            .await
            .map_err(map_executor_error)
    }

    /// Publishes a JSON-serialized notification payload.
    pub async fn publish_json<T>(&self, value: &T) -> Result<(), EventStoreError>
    where
        T: serde::Serialize + Sync,
    {
        let payload = serde_json::to_vec(value)
            .map_err(|error| EventStoreError::Serialization(error.to_string()))?;
        self.publish(&payload).await
    }
}

/// Spin SDK Redis command executor.
#[cfg(feature = "spin-redis")]
#[derive(Clone, Debug)]
pub struct SpinRedisClient {
    url: String,
}

#[cfg(feature = "spin-redis")]
impl SpinRedisClient {
    /// Creates a Spin Redis client for a Redis URL.
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }

    /// Returns the Redis URL.
    pub fn url(&self) -> &str {
        &self.url
    }
}

/// Error returned by [`SpinRedisClient`].
#[cfg(feature = "spin-redis")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpinRedisError(String);

#[cfg(feature = "spin-redis")]
impl Display for SpinRedisError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(feature = "spin-redis")]
impl std::error::Error for SpinRedisError {}

#[cfg(feature = "spin-redis")]
impl From<spin_sdk::redis::Error> for SpinRedisError {
    fn from(value: spin_sdk::redis::Error) -> Self {
        Self(format!("{value:?}"))
    }
}

#[cfg(feature = "spin-redis")]
#[async_trait]
impl RedisCommandExecutor for SpinRedisClient {
    type Error = SpinRedisError;

    async fn execute(&self, command: &str, args: Vec<Vec<u8>>) -> Result<RedisValue, Self::Error> {
        let connection = spin_sdk::redis::Connection::open(&self.url).await?;
        let args = args
            .into_iter()
            .map(spin_sdk::redis::RedisParameter::Binary)
            .collect::<Vec<_>>();
        let values = connection.execute(command, args).await?;
        Ok(RedisValue::Array(
            values.into_iter().map(spin_result_to_value).collect(),
        ))
    }

    async fn publish(&self, channel: &str, payload: &[u8]) -> Result<(), Self::Error> {
        let connection = spin_sdk::redis::Connection::open(&self.url).await?;
        connection.publish(channel, payload).await?;
        Ok(())
    }
}

/// Minimal raw RESP Redis command executor for generic WASI/Wasmtime.
///
/// This client supports plain `redis://` TCP URLs. It is deliberately small and
/// does not implement TLS, Sentinel, Cluster, or RESP3-specific behavior.
#[cfg(feature = "wasi-redis")]
#[derive(Clone, Debug)]
pub struct WasiRedisClient {
    url: String,
    read_timeout: Option<Duration>,
    nonblocking_subscription_reads: bool,
}

#[cfg(feature = "wasi-redis")]
impl WasiRedisClient {
    /// Creates a raw RESP Redis client for a `redis://` URL.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            read_timeout: Some(Duration::from_secs(5)),
            nonblocking_subscription_reads: false,
        }
    }

    /// Sets the read timeout used by newly opened TCP connections.
    pub fn with_read_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.read_timeout = timeout;
        self
    }

    /// Configures subscriptions opened by this client to use nonblocking socket
    /// reads after the initial `SUBSCRIBE` acknowledgement is received.
    pub fn with_nonblocking_subscription_reads(mut self, enabled: bool) -> Self {
        self.nonblocking_subscription_reads = enabled;
        self
    }

    /// Returns the Redis URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Subscribes to one Redis channel using a blocking raw RESP connection.
    pub fn subscribe(&self, channel: &str) -> Result<WasiRedisSubscription, RedisClientError> {
        let mut reader = self.open_reader()?;
        write_command(
            reader.get_mut(),
            "SUBSCRIBE",
            &[channel.as_bytes().to_vec()],
        )?;
        let _ = read_resp_value(&mut reader)?;
        if self.nonblocking_subscription_reads {
            reader.get_mut().set_nonblocking(true)?;
        }

        Ok(WasiRedisSubscription {
            channel: channel.to_owned(),
            reader,
        })
    }

    fn open_reader(&self) -> Result<BufReader<TcpStream>, RedisClientError> {
        let address = RedisAddress::parse(&self.url)?;
        let stream = TcpStream::connect((address.host.as_str(), address.port))?;
        stream.set_read_timeout(self.read_timeout)?;
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;
        let mut reader = BufReader::new(stream);

        if let Some(password) = &address.password {
            let mut args = Vec::new();
            if let Some(username) = &address.username {
                args.push(username.as_bytes().to_vec());
            }
            args.push(password.as_bytes().to_vec());
            write_command(reader.get_mut(), "AUTH", &args)?;
            expect_ok(read_resp_value(&mut reader)?, "AUTH")?;
        }

        if let Some(db) = address.db {
            write_command(reader.get_mut(), "SELECT", &[db.to_string().into_bytes()])?;
            expect_ok(read_resp_value(&mut reader)?, "SELECT")?;
        }

        Ok(reader)
    }
}

#[cfg(feature = "wasi-redis")]
#[async_trait]
impl RedisCommandExecutor for WasiRedisClient {
    type Error = RedisClientError;

    async fn execute(&self, command: &str, args: Vec<Vec<u8>>) -> Result<RedisValue, Self::Error> {
        let mut reader = self.open_reader()?;
        write_command(reader.get_mut(), command, &args)?;
        read_resp_value(&mut reader)
    }
}

/// Blocking Redis subscription reader returned by [`WasiRedisClient::subscribe`].
#[cfg(feature = "wasi-redis")]
#[derive(Debug)]
pub struct WasiRedisSubscription {
    channel: String,
    reader: BufReader<TcpStream>,
}

#[cfg(feature = "wasi-redis")]
impl WasiRedisSubscription {
    /// Returns the subscribed channel.
    pub fn channel(&self) -> &str {
        &self.channel
    }

    /// Reads the next published message payload.
    pub fn next_message(&mut self) -> Result<Vec<u8>, RedisClientError> {
        loop {
            let value = read_resp_value(&mut self.reader)?;
            let RedisValue::Array(items) = value else {
                continue;
            };
            if items.len() < 3 {
                continue;
            }
            let kind = redis_value_string(&items[0], "subscription message kind")
                .map_err(|error| RedisClientError::Protocol(error.to_string()))?;
            if kind == "message" {
                return redis_value_bytes(&items[2], "subscription payload")
                    .map_err(|error| RedisClientError::Protocol(error.to_string()));
            }
        }
    }

    /// Reads the next message, returning `Ok(None)` when the configured socket
    /// timeout expires before a message arrives.
    pub fn try_next_message(&mut self) -> Result<Option<Vec<u8>>, RedisClientError> {
        match self.next_message() {
            Ok(message) => Ok(Some(message)),
            Err(RedisClientError::Timeout) => Ok(None),
            Err(error) => Err(error),
        }
    }
}

/// Error returned by the raw RESP Redis client.
#[cfg(feature = "wasi-redis")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RedisClientError {
    /// Redis URL could not be parsed.
    InvalidUrl(String),
    /// TCP or stream I/O failed.
    Io(String),
    /// A blocking socket read timed out before Redis produced a response.
    Timeout,
    /// Redis returned an error response.
    Redis(String),
    /// RESP protocol data was malformed or unexpected.
    Protocol(String),
}

#[cfg(feature = "wasi-redis")]
impl Display for RedisClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisClientError::InvalidUrl(message) => write!(f, "invalid Redis URL: {message}"),
            RedisClientError::Io(message) => write!(f, "Redis I/O error: {message}"),
            RedisClientError::Timeout => f.write_str("Redis read timed out"),
            RedisClientError::Redis(message) => write!(f, "Redis error: {message}"),
            RedisClientError::Protocol(message) => write!(f, "Redis protocol error: {message}"),
        }
    }
}

#[cfg(feature = "wasi-redis")]
impl std::error::Error for RedisClientError {}

#[cfg(feature = "wasi-redis")]
impl From<std::io::Error> for RedisClientError {
    fn from(value: std::io::Error) -> Self {
        match value.kind() {
            ErrorKind::TimedOut | ErrorKind::WouldBlock => RedisClientError::Timeout,
            _ => RedisClientError::Io(value.to_string()),
        }
    }
}

#[derive(Clone, Debug)]
struct RedisStreamKeys {
    aggregate_id_json: String,
    revision_key: String,
    stream_key: String,
}

#[derive(Clone, Debug)]
struct PreparedRedisEvent<E> {
    event_id: EventId,
    event_type: String,
    event_version: u32,
    payload: E,
    payload_json: Vec<u8>,
    metadata: crate::Metadata,
    metadata_json: Vec<u8>,
    recorded_at: SystemTime,
    recorded_at_ms: i64,
}

impl<E> PreparedRedisEvent<E>
where
    E: serde::Serialize,
{
    fn new(event: NewEvent<E>) -> Result<Self, EventStoreError> {
        let event_id = EventId::new();
        let recorded_at = SystemTime::now();
        let recorded_at_ms = system_time_to_millis(recorded_at)?;
        let payload_json = serde_json::to_vec(&serialize_payload(&event.payload)?)
            .map_err(|error| EventStoreError::Serialization(format!("event payload: {error}")))?;
        let metadata_json = serde_json::to_vec(&serialize_metadata(&event.metadata)?)
            .map_err(|error| EventStoreError::Serialization(format!("metadata: {error}")))?;

        Ok(Self {
            event_id,
            event_type: event.event_type,
            event_version: event.event_version,
            payload: event.payload,
            payload_json,
            metadata: event.metadata,
            metadata_json,
            recorded_at,
            recorded_at_ms,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AppendEvalResult {
    first_sequence: u64,
    last_sequence: u64,
    next_revision: u64,
}

struct AppendEvalArgs<'a, E> {
    script: &'a str,
    aggregate_type: &'a str,
    keys: &'a RedisStreamKeys,
    sequence_key: &'a str,
    global_key: &'a str,
    event_key_prefix: &'a str,
    expected_revision: ExpectedRevision,
    events: &'a [PreparedRedisEvent<E>],
}

fn build_append_eval_args<E>(input: AppendEvalArgs<'_, E>) -> Vec<Vec<u8>> {
    let (expected_kind, expected_value) = expected_revision_arg(input.expected_revision);
    let mut args = vec![
        input.script.as_bytes().to_vec(),
        b"4".to_vec(),
        input.keys.revision_key.as_bytes().to_vec(),
        input.sequence_key.as_bytes().to_vec(),
        input.keys.stream_key.as_bytes().to_vec(),
        input.global_key.as_bytes().to_vec(),
        expected_kind.as_bytes().to_vec(),
        expected_value.to_string().into_bytes(),
        input.events.len().to_string().into_bytes(),
        input.event_key_prefix.as_bytes().to_vec(),
    ];

    for event in input.events {
        args.push(event.event_id.as_str().as_bytes().to_vec());
        args.push(input.keys.aggregate_id_json.as_bytes().to_vec());
        args.push(input.aggregate_type.as_bytes().to_vec());
        args.push(event.event_type.as_bytes().to_vec());
        args.push(event.event_version.to_string().into_bytes());
        args.push(event.payload_json.clone());
        args.push(event.metadata_json.clone());
        args.push(event.recorded_at_ms.to_string().into_bytes());
    }

    args
}

fn expected_revision_arg(expected_revision: ExpectedRevision) -> (&'static str, u64) {
    match expected_revision {
        ExpectedRevision::Any => ("any", 0),
        ExpectedRevision::NoStream => ("no_stream", 0),
        ExpectedRevision::Exact(revision) => ("exact", revision),
    }
}

fn parse_append_eval_result(
    value: &RedisValue,
    expected: ExpectedRevision,
) -> Result<AppendEvalResult, EventStoreError> {
    let items = redis_array_items(value)?;
    if items.len() < 3 {
        return Err(EventStoreError::Deserialization(
            "Redis append script returned too few fields".to_owned(),
        ));
    }

    let status = redis_value_string(&items[0], "append script status")?;
    match status.as_str() {
        "OK" => {
            if items.len() < 4 {
                return Err(EventStoreError::Deserialization(
                    "Redis append script returned too few success fields".to_owned(),
                ));
            }
            Ok(AppendEvalResult {
                first_sequence: redis_value_u64(&items[1], "append first sequence")?,
                last_sequence: redis_value_u64(&items[2], "append last sequence")?,
                next_revision: redis_value_u64(&items[3], "append next revision")?,
            })
        }
        "ERR" => {
            let reason = redis_value_string(&items[1], "append error reason")?;
            let actual = redis_value_u64(&items[2], "append actual revision")?;
            match reason.as_str() {
                "stream_exists" => Err(EventStoreError::Concurrency(
                    crate::ConcurrencyError::StreamAlreadyExists,
                )),
                "wrong_revision" => Err(EventStoreError::Concurrency(
                    crate::ConcurrencyError::WrongExpectedRevision { expected, actual },
                )),
                _ => Err(EventStoreError::Backend(format!(
                    "Redis append script failed: {reason}"
                ))),
            }
        }
        _ => Err(EventStoreError::Deserialization(format!(
            "unknown Redis append status `{status}`"
        ))),
    }
}

fn hash_to_envelope<A>(
    upcasters: &UpcasterRegistry,
    hash: BTreeMap<String, Vec<u8>>,
) -> Result<EventEnvelope<A::Event, A::Id>, EventStoreError>
where
    A: Aggregate,
    A::Event: serde::de::DeserializeOwned,
    A::Id: serde::de::DeserializeOwned,
{
    let event_id = hash_field_string(&hash, "event_id")?;
    let aggregate_id_json = hash_field_string(&hash, "aggregate_id")?;
    let aggregate_type = hash_field_string(&hash, "aggregate_type")?;
    let revision = hash_field_u64(&hash, "revision")?;
    let sequence = hash_field_u64(&hash, "sequence")?;
    let event_type = hash_field_string(&hash, "event_type")?;
    let event_version = hash_field_u32(&hash, "event_version")?;
    let payload_bytes = hash_field_bytes(&hash, "payload")?;
    let metadata_bytes = hash_field_bytes(&hash, "metadata")?;
    let recorded_at_ms = hash_field_i64(&hash, "recorded_at_ms")?;

    let aggregate_id = deserialize_id(&aggregate_id_json)?;
    let (event_version, upcasted_bytes) = upcasters
        .upcast(&event_type, event_version, payload_bytes)
        .map_err(|error| EventStoreError::Deserialization(error.to_string()))?;
    let payload_value = serde_json::from_slice(&upcasted_bytes)
        .map_err(|error| EventStoreError::Deserialization(format!("payload JSON: {error}")))?;
    let payload = deserialize_payload(&event_id, &event_type, payload_value)?;
    let metadata_value = serde_json::from_slice(&metadata_bytes)
        .map_err(|error| EventStoreError::Deserialization(format!("metadata JSON: {error}")))?;
    let metadata = deserialize_metadata(&event_id, metadata_value)?;
    let recorded_at = millis_to_system_time(recorded_at_ms)?;

    Ok(EventEnvelope::new(
        EventId::from_string(event_id),
        aggregate_id,
        aggregate_type,
        revision,
        Some(sequence),
        event_type,
        event_version,
        payload,
        metadata,
        recorded_at,
    ))
}

fn validate_redis_prefix(prefix: &str) -> Result<(), EventStoreError> {
    if prefix.is_empty() {
        return Err(EventStoreError::Backend(
            "Redis key prefix cannot be empty".to_owned(),
        ));
    }

    if prefix
        .chars()
        .all(|ch| ch == ':' || ch == '_' || ch == '-' || ch.is_ascii_alphanumeric())
    {
        Ok(())
    } else {
        Err(EventStoreError::Backend(format!(
            "invalid Redis key prefix `{prefix}`"
        )))
    }
}

fn map_executor_error<E>(error: E) -> EventStoreError
where
    E: Display,
{
    EventStoreError::Backend(error.to_string())
}

fn redis_array_items(value: &RedisValue) -> Result<&[RedisValue], EventStoreError> {
    match value {
        RedisValue::Array(items) => Ok(items),
        RedisValue::Nil => Ok(&[]),
        _ => Err(EventStoreError::Deserialization(format!(
            "expected Redis array, got {value:?}"
        ))),
    }
}

fn redis_scalar(value: &RedisValue) -> &RedisValue {
    match value {
        RedisValue::Array(items) if items.len() == 1 => &items[0],
        _ => value,
    }
}

fn redis_optional_u64(value: &RedisValue, label: &str) -> Result<u64, EventStoreError> {
    match redis_scalar(value) {
        RedisValue::Nil => Ok(0),
        value => redis_value_u64(value, label),
    }
}

fn redis_sequence_list(value: &RedisValue) -> Result<Vec<u64>, EventStoreError> {
    redis_array_items(value)?
        .iter()
        .map(|value| redis_value_u64(value, "Redis sequence"))
        .collect()
}

fn redis_hash(value: &RedisValue) -> Result<BTreeMap<String, Vec<u8>>, EventStoreError> {
    let items = redis_array_items(value)?;
    if items.len() % 2 != 0 {
        return Err(EventStoreError::Deserialization(
            "Redis hash reply has odd field count".to_owned(),
        ));
    }

    let mut hash = BTreeMap::new();
    for pair in items.chunks_exact(2) {
        let field = redis_value_string(&pair[0], "Redis hash field")?;
        let value = redis_value_bytes(&pair[1], "Redis hash value")?;
        hash.insert(field, value);
    }

    Ok(hash)
}

fn redis_value_string(value: &RedisValue, label: &str) -> Result<String, EventStoreError> {
    let bytes = redis_value_bytes(value, label)?;
    String::from_utf8(bytes).map_err(|error| {
        EventStoreError::Deserialization(format!("{label} is not valid UTF-8: {error}"))
    })
}

fn redis_value_bytes(value: &RedisValue, label: &str) -> Result<Vec<u8>, EventStoreError> {
    match value {
        RedisValue::Bytes(bytes) => Ok(bytes.clone()),
        RedisValue::Status(value) => Ok(value.as_bytes().to_vec()),
        RedisValue::Int(value) => Ok(value.to_string().into_bytes()),
        _ => Err(EventStoreError::Deserialization(format!(
            "{label}: expected Redis scalar, got {value:?}"
        ))),
    }
}

fn redis_value_u64(value: &RedisValue, label: &str) -> Result<u64, EventStoreError> {
    match value {
        RedisValue::Int(value) => u64::try_from(*value)
            .map_err(|_| EventStoreError::Deserialization(format!("{label} cannot be negative"))),
        RedisValue::Bytes(bytes) => {
            let text = std::str::from_utf8(bytes).map_err(|error| {
                EventStoreError::Deserialization(format!("{label} is not valid UTF-8: {error}"))
            })?;
            text.parse::<u64>().map_err(|error| {
                EventStoreError::Deserialization(format!("{label} is not a u64: {error}"))
            })
        }
        RedisValue::Status(text) => text.parse::<u64>().map_err(|error| {
            EventStoreError::Deserialization(format!("{label} is not a u64: {error}"))
        }),
        _ => Err(EventStoreError::Deserialization(format!(
            "{label}: expected Redis integer scalar, got {value:?}"
        ))),
    }
}

fn hash_field_bytes(
    hash: &BTreeMap<String, Vec<u8>>,
    field: &str,
) -> Result<Vec<u8>, EventStoreError> {
    hash.get(field).cloned().ok_or_else(|| {
        EventStoreError::Deserialization(format!("Redis event hash missing `{field}`"))
    })
}

fn hash_field_string(
    hash: &BTreeMap<String, Vec<u8>>,
    field: &str,
) -> Result<String, EventStoreError> {
    let value = hash_field_bytes(hash, field)?;
    String::from_utf8(value).map_err(|error| {
        EventStoreError::Deserialization(format!("Redis event hash `{field}` UTF-8: {error}"))
    })
}

fn hash_field_u64(hash: &BTreeMap<String, Vec<u8>>, field: &str) -> Result<u64, EventStoreError> {
    let value = hash_field_string(hash, field)?;
    value.parse::<u64>().map_err(|error| {
        EventStoreError::Deserialization(format!("Redis event hash `{field}` u64: {error}"))
    })
}

fn hash_field_i64(hash: &BTreeMap<String, Vec<u8>>, field: &str) -> Result<i64, EventStoreError> {
    let value = hash_field_string(hash, field)?;
    value.parse::<i64>().map_err(|error| {
        EventStoreError::Deserialization(format!("Redis event hash `{field}` i64: {error}"))
    })
}

fn hash_field_u32(hash: &BTreeMap<String, Vec<u8>>, field: &str) -> Result<u32, EventStoreError> {
    let value = hash_field_u64(hash, field)?;
    u32::try_from(value).map_err(|_| {
        EventStoreError::Deserialization(format!("Redis event hash `{field}` exceeds u32"))
    })
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(feature = "spin-redis")]
fn spin_result_to_value(value: spin_sdk::redis::RedisResult) -> RedisValue {
    match value {
        spin_sdk::redis::RedisResult::Nil => RedisValue::Nil,
        spin_sdk::redis::RedisResult::Status(value) => RedisValue::Status(value),
        spin_sdk::redis::RedisResult::Int64(value) => RedisValue::Int(value),
        spin_sdk::redis::RedisResult::Binary(value) => RedisValue::Bytes(value),
    }
}

#[cfg(feature = "wasi-redis")]
#[derive(Clone, Debug, PartialEq, Eq)]
struct RedisAddress {
    host: String,
    port: u16,
    username: Option<String>,
    password: Option<String>,
    db: Option<u32>,
}

#[cfg(feature = "wasi-redis")]
impl RedisAddress {
    fn parse(url: &str) -> Result<Self, RedisClientError> {
        let Some(rest) = url.strip_prefix("redis://") else {
            return Err(RedisClientError::InvalidUrl(
                "only redis:// URLs are supported".to_owned(),
            ));
        };
        let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
        let (auth, host_port) = authority
            .rsplit_once('@')
            .map_or((None, authority), |(auth, host_port)| {
                (Some(auth), host_port)
            });

        if host_port.is_empty() {
            return Err(RedisClientError::InvalidUrl("host is required".to_owned()));
        }

        let (host, port) = match host_port.rsplit_once(':') {
            Some((host, port)) => {
                let port = port.parse::<u16>().map_err(|error| {
                    RedisClientError::InvalidUrl(format!("invalid port `{port}`: {error}"))
                })?;
                (host.to_owned(), port)
            }
            None => (host_port.to_owned(), 6379),
        };

        let (username, password) = auth
            .map(|auth| {
                let (username, password) = auth.split_once(':').map_or(("", auth), |parts| parts);
                (
                    (!username.is_empty()).then(|| username.to_owned()),
                    (!password.is_empty()).then(|| password.to_owned()),
                )
            })
            .unwrap_or((None, None));
        let db = if path.is_empty() {
            None
        } else {
            Some(path.parse::<u32>().map_err(|error| {
                RedisClientError::InvalidUrl(format!("invalid database `{path}`: {error}"))
            })?)
        };

        Ok(Self {
            host,
            port,
            username,
            password,
            db,
        })
    }
}

#[cfg(feature = "wasi-redis")]
fn write_command(
    stream: &mut TcpStream,
    command: &str,
    args: &[Vec<u8>],
) -> Result<(), RedisClientError> {
    let encoded = encode_resp_command(command, args);
    stream.write_all(&encoded)?;
    stream.flush()?;
    Ok(())
}

#[cfg(feature = "wasi-redis")]
fn encode_resp_command(command: &str, args: &[Vec<u8>]) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(format!("*{}\r\n", args.len() + 1).as_bytes());
    push_bulk(&mut output, command.as_bytes());
    for arg in args {
        push_bulk(&mut output, arg);
    }
    output
}

#[cfg(feature = "wasi-redis")]
fn push_bulk(output: &mut Vec<u8>, bytes: &[u8]) {
    output.extend_from_slice(format!("${}\r\n", bytes.len()).as_bytes());
    output.extend_from_slice(bytes);
    output.extend_from_slice(b"\r\n");
}

#[cfg(feature = "wasi-redis")]
fn read_resp_value(reader: &mut impl BufRead) -> Result<RedisValue, RedisClientError> {
    let mut prefix = [0_u8; 1];
    reader.read_exact(&mut prefix)?;
    match prefix[0] {
        b'+' => Ok(RedisValue::Status(read_resp_line(reader)?)),
        b'-' => Err(RedisClientError::Redis(read_resp_line(reader)?)),
        b':' => {
            let line = read_resp_line(reader)?;
            let value = line.parse::<i64>().map_err(|error| {
                RedisClientError::Protocol(format!("invalid integer `{line}`: {error}"))
            })?;
            Ok(RedisValue::Int(value))
        }
        b'$' => {
            let line = read_resp_line(reader)?;
            let len = line.parse::<i64>().map_err(|error| {
                RedisClientError::Protocol(format!("invalid bulk length `{line}`: {error}"))
            })?;
            if len < 0 {
                return Ok(RedisValue::Nil);
            }
            let len = usize::try_from(len)
                .map_err(|_| RedisClientError::Protocol("bulk length exceeds usize".to_owned()))?;
            let mut bytes = vec![0_u8; len];
            reader.read_exact(&mut bytes)?;
            read_expected_crlf(reader)?;
            Ok(RedisValue::Bytes(bytes))
        }
        b'*' => {
            let line = read_resp_line(reader)?;
            let len = line.parse::<i64>().map_err(|error| {
                RedisClientError::Protocol(format!("invalid array length `{line}`: {error}"))
            })?;
            if len < 0 {
                return Ok(RedisValue::Nil);
            }
            let len = usize::try_from(len)
                .map_err(|_| RedisClientError::Protocol("array length exceeds usize".to_owned()))?;
            let mut values = Vec::with_capacity(len);
            for _ in 0..len {
                values.push(read_resp_value(reader)?);
            }
            Ok(RedisValue::Array(values))
        }
        other => Err(RedisClientError::Protocol(format!(
            "unknown RESP prefix byte `{other}`"
        ))),
    }
}

#[cfg(feature = "wasi-redis")]
fn read_resp_line(reader: &mut impl BufRead) -> Result<String, RedisClientError> {
    let mut line = Vec::new();
    reader.read_until(b'\n', &mut line)?;
    if !line.ends_with(b"\r\n") {
        return Err(RedisClientError::Protocol(
            "RESP line did not end with CRLF".to_owned(),
        ));
    }
    line.truncate(line.len() - 2);
    String::from_utf8(line)
        .map_err(|error| RedisClientError::Protocol(format!("RESP line UTF-8: {error}")))
}

#[cfg(feature = "wasi-redis")]
fn read_expected_crlf(reader: &mut impl Read) -> Result<(), RedisClientError> {
    let mut crlf = [0_u8; 2];
    reader.read_exact(&mut crlf)?;
    if crlf == *b"\r\n" {
        Ok(())
    } else {
        Err(RedisClientError::Protocol(
            "bulk string did not end with CRLF".to_owned(),
        ))
    }
}

#[cfg(feature = "wasi-redis")]
fn expect_ok(value: RedisValue, command: &str) -> Result<(), RedisClientError> {
    match value {
        RedisValue::Status(status) if status.eq_ignore_ascii_case("OK") => Ok(()),
        other => Err(RedisClientError::Protocol(format!(
            "{command} returned unexpected value {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Aggregate, DomainEvent, Metadata};
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    enum TestEvent {
        Created { value: i32 },
        Updated { value: i32 },
    }

    impl DomainEvent for TestEvent {
        fn event_type(&self) -> &'static str {
            match self {
                TestEvent::Created { .. } => "created",
                TestEvent::Updated { .. } => "updated",
            }
        }
    }

    #[derive(Clone, Debug, Default, PartialEq, Eq)]
    struct TestAggregate {
        value: i32,
        revision: u64,
    }

    impl Aggregate for TestAggregate {
        type Id = String;
        type Command = ();
        type Event = TestEvent;
        type Error = String;

        fn aggregate_type() -> &'static str {
            "test_aggregate"
        }

        fn id(&self) -> Option<&Self::Id> {
            None
        }

        fn revision(&self) -> u64 {
            self.revision
        }

        fn new() -> Self {
            Self::default()
        }

        fn apply(&mut self, event: &Self::Event) {
            self.value = match event {
                TestEvent::Created { value } | TestEvent::Updated { value } => *value,
            };
            self.revision += 1;
        }

        fn handle(&self, _command: Self::Command) -> Result<Vec<Self::Event>, Self::Error> {
            Ok(Vec::new())
        }
    }

    #[derive(Clone, Default)]
    struct RecordingRedisClient {
        calls: RecordedCalls,
    }

    type RecordedCalls = Arc<Mutex<Vec<(String, Vec<Vec<u8>>)>>>;

    #[async_trait]
    impl RedisCommandExecutor for RecordingRedisClient {
        type Error = String;

        async fn execute(
            &self,
            command: &str,
            args: Vec<Vec<u8>>,
        ) -> Result<RedisValue, Self::Error> {
            self.calls
                .lock()
                .map_err(|_| "poisoned".to_owned())?
                .push((command.to_owned(), args));
            Ok(RedisValue::Array(vec![
                RedisValue::Status("OK".to_owned()),
                RedisValue::Int(1),
                RedisValue::Int(1),
                RedisValue::Int(1),
            ]))
        }
    }

    #[test]
    fn redis_prefix_validation_accepts_key_safe_names() {
        assert!(validate_redis_prefix("ddd:tenant-1_events").is_ok());
    }

    #[test]
    fn redis_prefix_validation_rejects_whitespace() {
        assert!(validate_redis_prefix("ddd tenant").is_err());
    }

    #[test]
    fn key_names_hex_encode_aggregate_identity() {
        let client = RecordingRedisClient::default();
        let store = RedisEventStore::<TestAggregate, _>::with_prefix(client, "ddd:test").unwrap();

        let keys = store.stream_keys(&"counter:1".to_owned()).unwrap();

        assert!(keys.revision_key.starts_with("ddd:test:revision:"));
        assert!(!keys.revision_key.contains("\"counter:1\""));
        assert!(keys.stream_key.starts_with("ddd:test:stream:"));
    }

    #[test]
    fn append_lua_arguments_include_atomic_eval_shape() {
        let keys = RedisStreamKeys {
            aggregate_id_json: "\"stream-1\"".to_owned(),
            revision_key: "ddd:revision".to_owned(),
            stream_key: "ddd:stream".to_owned(),
        };
        let event = PreparedRedisEvent::new(NewEvent::new(
            TestEvent::Created { value: 7 },
            Metadata::new().with_correlation_id("corr-1"),
        ))
        .unwrap();

        let args = build_append_eval_args(AppendEvalArgs {
            script: "return 1",
            aggregate_type: TestAggregate::aggregate_type(),
            keys: &keys,
            sequence_key: "ddd:seq",
            global_key: "ddd:global",
            event_key_prefix: "ddd:event:",
            expected_revision: ExpectedRevision::NoStream,
            events: &[event],
        });

        assert_eq!(args[0], b"return 1");
        assert_eq!(args[1], b"4");
        assert_eq!(args[2], b"ddd:revision");
        assert_eq!(args[6], b"no_stream");
        assert_eq!(args[8], b"1");
        assert_eq!(args[12], b"test_aggregate");
    }

    #[cfg(feature = "wasi-redis")]
    #[test]
    fn resp_encoder_writes_binary_safe_bulk_arguments() {
        let encoded = encode_resp_command("SET", &[b"k\r\n1".to_vec(), b"v".to_vec()]);

        assert_eq!(
            encoded,
            b"*3\r\n$3\r\nSET\r\n$4\r\nk\r\n1\r\n$1\r\nv\r\n".to_vec()
        );
    }

    #[cfg(feature = "wasi-redis")]
    #[test]
    fn resp_decoder_reads_arrays_and_bulk_values() {
        let input = b"*2\r\n$3\r\nfoo\r\n:42\r\n";
        let mut reader = BufReader::new(&input[..]);

        let value = read_resp_value(&mut reader).unwrap();

        assert_eq!(
            value,
            RedisValue::Array(vec![
                RedisValue::Bytes(b"foo".to_vec()),
                RedisValue::Int(42)
            ])
        );
    }

    #[cfg(feature = "wasi-redis")]
    #[test]
    fn redis_url_parser_supports_password_and_database() {
        let parsed = RedisAddress::parse("redis://:secret@localhost:6380/2").unwrap();

        assert_eq!(
            parsed,
            RedisAddress {
                host: "localhost".to_owned(),
                port: 6380,
                username: None,
                password: Some("secret".to_owned()),
                db: Some(2),
            }
        );
    }

    #[cfg(feature = "wasi-redis")]
    #[test]
    fn redis_url_parser_supports_acl_username_and_password() {
        let parsed = RedisAddress::parse("redis://app:secret@localhost/0").unwrap();

        assert_eq!(
            parsed,
            RedisAddress {
                host: "localhost".to_owned(),
                port: 6379,
                username: Some("app".to_owned()),
                password: Some("secret".to_owned()),
                db: Some(0),
            }
        );
    }

    #[cfg(feature = "wasi-redis")]
    fn live_client() -> Option<WasiRedisClient> {
        std::env::var("DDD_CQRS_ES_REDIS_URL")
            .ok()
            .or_else(|| std::env::var("REDIS_URL").ok())
            .map(WasiRedisClient::new)
    }

    #[cfg(feature = "wasi-redis")]
    async fn cleanup_prefix(client: &WasiRedisClient, prefix: &str) {
        let Ok(keys) = client
            .execute("KEYS", vec![format!("{prefix}:*").into_bytes()])
            .await
        else {
            return;
        };
        let Ok(keys) = redis_array_items(&keys) else {
            return;
        };
        let keys = keys
            .iter()
            .filter_map(|value| redis_value_bytes(value, "cleanup key").ok())
            .collect::<Vec<_>>();
        if !keys.is_empty() {
            let _ = client.execute("DEL", keys).await;
        }
    }

    #[cfg(feature = "wasi-redis")]
    fn unique_prefix(test_name: &str) -> String {
        format!(
            "ddd:test:{}:{}",
            test_name,
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or_default()
        )
    }

    #[cfg(feature = "wasi-redis")]
    #[tokio::test]
    async fn live_redis_append_and_load_round_trips_events() {
        let Some(client) = live_client() else {
            eprintln!("skipping live Redis test: DDD_CQRS_ES_REDIS_URL or REDIS_URL is not set");
            return;
        };
        let prefix = unique_prefix("round_trip");
        cleanup_prefix(&client, &prefix).await;
        let store =
            RedisEventStore::<TestAggregate, _>::with_prefix(client.clone(), prefix.clone())
                .unwrap();

        let committed = store
            .append(
                &"stream-1".to_owned(),
                ExpectedRevision::NoStream,
                vec![NewEvent::new(
                    TestEvent::Created { value: 11 },
                    Metadata::default(),
                )],
            )
            .await
            .unwrap();
        let loaded = store.load(&"stream-1".to_owned()).await.unwrap();

        assert_eq!(committed[0].payload, loaded[0].payload);
        cleanup_prefix(&client, &prefix).await;
    }

    #[cfg(feature = "wasi-redis")]
    #[tokio::test]
    async fn live_redis_expected_revision_conflicts() {
        let Some(client) = live_client() else {
            eprintln!("skipping live Redis test: DDD_CQRS_ES_REDIS_URL or REDIS_URL is not set");
            return;
        };
        let prefix = unique_prefix("expected_revision");
        cleanup_prefix(&client, &prefix).await;
        let store =
            RedisEventStore::<TestAggregate, _>::with_prefix(client.clone(), prefix.clone())
                .unwrap();

        store
            .append(
                &"stream-1".to_owned(),
                ExpectedRevision::NoStream,
                vec![NewEvent::new(
                    TestEvent::Created { value: 1 },
                    Metadata::default(),
                )],
            )
            .await
            .unwrap();
        let duplicate = store
            .append(
                &"stream-1".to_owned(),
                ExpectedRevision::NoStream,
                vec![NewEvent::new(
                    TestEvent::Updated { value: 2 },
                    Metadata::default(),
                )],
            )
            .await
            .unwrap_err();

        assert_eq!(
            duplicate,
            EventStoreError::Concurrency(crate::ConcurrencyError::StreamAlreadyExists)
        );
        cleanup_prefix(&client, &prefix).await;
    }

    #[cfg(feature = "wasi-redis")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_redis_concurrent_same_stream_append_has_one_revision_winner() {
        let Some(client) = live_client() else {
            eprintln!("skipping live Redis test: DDD_CQRS_ES_REDIS_URL or REDIS_URL is not set");
            return;
        };
        let prefix = unique_prefix("concurrent");
        cleanup_prefix(&client, &prefix).await;
        let store =
            RedisEventStore::<TestAggregate, _>::with_prefix(client.clone(), prefix.clone())
                .unwrap();
        let first = store.clone();
        let second = store.clone();
        let left_id = "stream-1".to_owned();
        let right_id = left_id.clone();

        let (left, right) = tokio::join!(
            async {
                first
                    .append(
                        &left_id,
                        ExpectedRevision::NoStream,
                        vec![NewEvent::new(
                            TestEvent::Created { value: 1 },
                            Metadata::default(),
                        )],
                    )
                    .await
            },
            async {
                second
                    .append(
                        &right_id,
                        ExpectedRevision::NoStream,
                        vec![NewEvent::new(
                            TestEvent::Created { value: 2 },
                            Metadata::default(),
                        )],
                    )
                    .await
            },
        );
        let winners = usize::from(left.is_ok()) + usize::from(right.is_ok());

        assert_eq!(winners, 1);
        cleanup_prefix(&client, &prefix).await;
    }

    #[cfg(feature = "wasi-redis")]
    #[tokio::test]
    async fn live_redis_global_ordering_and_checkpoint_update() {
        let Some(client) = live_client() else {
            eprintln!("skipping live Redis test: DDD_CQRS_ES_REDIS_URL or REDIS_URL is not set");
            return;
        };
        let prefix = unique_prefix("global_checkpoint");
        cleanup_prefix(&client, &prefix).await;
        let store =
            RedisEventStore::<TestAggregate, _>::with_prefix(client.clone(), prefix.clone())
                .unwrap();
        let checkpoint = RedisCheckpointStore::with_prefix(client.clone(), prefix.clone()).unwrap();

        store
            .append(
                &"stream-1".to_owned(),
                ExpectedRevision::NoStream,
                vec![NewEvent::new(
                    TestEvent::Created { value: 1 },
                    Metadata::default(),
                )],
            )
            .await
            .unwrap();
        store
            .append(
                &"stream-2".to_owned(),
                ExpectedRevision::NoStream,
                vec![NewEvent::new(
                    TestEvent::Created { value: 2 },
                    Metadata::default(),
                )],
            )
            .await
            .unwrap();
        checkpoint.save_checkpoint("projection", 1).await.unwrap();

        let global = store.load_global_after(Some(1)).await.unwrap();
        let loaded_checkpoint = checkpoint.load_checkpoint("projection").await.unwrap();

        assert_eq!((global[0].sequence, loaded_checkpoint), (Some(2), Some(1)));
        cleanup_prefix(&client, &prefix).await;
    }

    #[cfg(feature = "wasi-redis")]
    #[tokio::test]
    async fn live_redis_publish_and_subscribe_round_trip() {
        let Some(client) = live_client() else {
            eprintln!(
                "skipping live Redis pub/sub test: DDD_CQRS_ES_REDIS_URL or REDIS_URL is not set"
            );
            return;
        };
        let channel = unique_prefix("pubsub");
        let mut subscription = client.subscribe(&channel).unwrap();
        let handle = std::thread::spawn(move || subscription.next_message());
        std::thread::sleep(Duration::from_millis(50));

        client.publish(&channel, b"{\"ok\":true}").await.unwrap();
        let message = handle.join().unwrap().unwrap();

        assert_eq!(message, b"{\"ok\":true}");
    }
}
