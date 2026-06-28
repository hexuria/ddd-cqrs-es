---
title: 5.5. Event Upcasters
description: Handle event schema evolution over time without mutating historical records.
---

In traditional CRUD applications, evolving database schemas requires running SQL migrations that alter tables, columns, or rows. 

In an Event Sourced system, we have a major constraint: **our event store is an immutable, append-only ledger**. We cannot run `ALTER TABLE` or `UPDATE` statements to modify historical event payloads because doing so would destroy our tamper-proof audit log.

However, business requirements evolve. Fields are added, renamed, or deprecated. To resolve this without mutating historical data, we use **Event Upcasting**.

---

## What Is Upcasting?

An **Upcaster** is a lightweight, raw-level transformer that intercepts old event payloads during the loading/replay phase and upgrades them on-the-fly to the latest schema version before they are deserialized into your domain types:

```
[ Immutable Store (v1 JSON) ] ──► [ EventUpcaster ] ──► [ Latest Domain Event (v2) ]
```

Because upcasters operate during the loading loop, the database remains completely untouched, preserving our historical audit guarantees, while your application code only ever has to reason about the latest, current event structure.

---

## Implementing the `EventUpcaster` Trait

Our framework provides a built-in `EventUpcaster` trait that works directly on raw vector bytes. This ensures that upcasting is decoupled from whatever serialization engine (JSON, MessagePack, Protobuf) your databases use.

```rust
use ddd_cqrs_es::EventUpcaster;

/// A simple upcaster that converts old bank account opened payloads (v1)
/// to the newer version (v2) which includes an additional "currency" field.
struct AccountOpenedUpcaster;

impl EventUpcaster for AccountOpenedUpcaster {
    type Error = &'static str;

    /// The old schema version of the event stored in the database.
    fn source_version(&self) -> u32 {
        1
    }

    /// The new target schema version to upgrade to.
    fn target_version(&self) -> u32 {
        2
    }

    /// Performs the schema migration on the raw event payload.
    fn upcast(&self, raw_payload: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
        // Parse raw payload as JSON (assuming serde_json is used)
        let mut json: serde_json::Value = serde_json::from_slice(&raw_payload)
            .map_err(|_| "Failed to deserialize v1 payload")?;
            
        // Inject new fields with sensible default values
        if let Some(obj) = json.as_object_mut() {
            obj.insert("currency".to_owned(), serde_json::Value::String("USD".to_owned()));
        }

        // Serialize back to raw bytes
        let upgraded_payload = serde_json::to_vec(&json)
            .map_err(|_| "Failed to serialize v2 payload")?;
            
        Ok(upgraded_payload)
    }
}
```

---

## Registering Upcasters

Once you have defined your `EventUpcaster`, you must register it with your `EventStore` adapter (such as `SqliteEventStore` or `PostgresEventStore`). The store's deserialization loop will automatically query the registered upcasters and apply them sequentially if it loads an event matching the registered event type name and source version.

```rust
// Create your event store
let store = SqliteEventStore::<BankAccount>::new(connection)?;

// Register the upcaster for the "account_opened" event type
store.register_upcaster("account_opened", AccountOpenedUpcaster);
```

If multiple upcasters are registered for the same event type (e.g., `v1 -> v2` and `v2 -> v3`), the engine automatically constructs an upcaster chain and applies them sequentially when replaying events.

---

## Upcasting Design Guidelines

To manage schema changes cleanly as your product grows:

1. **Keep Upcasters Small:** Each upcaster should only be responsible for a single step transformation (e.g., v1 to v2).
2. **Chain Upcasters:** If an event has evolved from v1 to v3, the framework will automatically chain upcasters (v1 -> v2, then v2 -> v3) sequentially, so you don't need to write a complex v1 -> v3 upcaster.
3. **Run Unit Tests:** Always write unit tests for your upcasters. Assert that passing raw bytes representing an old JSON schema correctly returns the expected, modified JSON schema.
