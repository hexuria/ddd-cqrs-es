use anyhow::{Result, anyhow};
use serde::Deserialize;
use spin_sdk::redis::Connection;
use spin_sdk::redis_subscriber;
use std::env;

const DEFAULT_REDIS_URL: &str = "redis://127.0.0.1:6379";
const LAST_SEQUENCE_KEY: &str = "counter:redis_trigger:last_sequence";
const LAST_COUNT_KEY: &str = "counter:redis_trigger:last_count";
const RECEIVED_COUNT_KEY: &str = "counter:redis_trigger:received_count";

#[derive(Debug, Deserialize)]
struct CounterRealtimeMessage {
    view: CounterView,
    last_sequence: u64,
}

#[derive(Debug, Deserialize)]
struct CounterView {
    count: i32,
    last_sequence: u64,
}

/// Spin Redis trigger entrypoint.
///
/// This component only records subscriber health. Durable events, projections,
/// checkpoints, and browser SSE delivery remain owned by the HTTP component.
#[redis_subscriber]
async fn on_message(message: Vec<u8>) -> Result<()> {
    let Ok(message) = serde_json::from_slice::<CounterRealtimeMessage>(&message) else {
        eprintln!("counter Redis trigger received malformed realtime payload");
        return Ok(());
    };
    if message.view.last_sequence != message.last_sequence {
        eprintln!(
            "counter Redis trigger received mismatched sequences: view={} message={}",
            message.view.last_sequence, message.last_sequence
        );
        return Ok(());
    }

    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| DEFAULT_REDIS_URL.to_string());
    let connection = Connection::open(&redis_url)
        .await
        .map_err(|error| anyhow!("failed to open Redis connection: {error:?}"))?;

    connection
        .set(LAST_SEQUENCE_KEY, message.last_sequence.to_string())
        .await
        .map_err(|error| anyhow!("failed to write Redis trigger last sequence: {error:?}"))?;
    connection
        .set(LAST_COUNT_KEY, message.view.count.to_string())
        .await
        .map_err(|error| anyhow!("failed to write Redis trigger last count: {error:?}"))?;
    connection
        .incr(RECEIVED_COUNT_KEY)
        .await
        .map_err(|error| anyhow!("failed to increment Redis trigger count: {error:?}"))?;

    println!(
        "counter Redis trigger observed sequence {} count {}",
        message.last_sequence, message.view.count
    );
    Ok(())
}
