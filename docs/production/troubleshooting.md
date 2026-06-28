---
title: 5.7. Troubleshooting & Driver Pitfalls
description: Common integration issues, database driver quirks, and serialization pitfalls when deploying to WebAssembly runtimes.
---

When building Event-Sourced microservices on sandboxed WebAssembly runtimes (like Wasmtime or Fermyon Spin), you are running database operations across strict host boundaries. This can sometimes lead to unexpected serialization or type-mapping behaviors from low-level database drivers.

This document lists known driver-specific quirks, common errors, and how to resolve them.

---

## 🐘 PostgreSQL Driver Pitfalls

### Issue: "Failed to parse JSON: expected value at line 1 column 1"

#### Symptom
When querying aggregated events or JSONB payloads from a PostgreSQL database under a WebAssembly runtime (such as Fermyon Spin), you encounter a parsing/deserialization error like:
```text
Constraint Validation Error
error running server function: Failed to parse latest_events JSON: expected value at line 1 column 1
```

#### Root Cause
This error occurs when using PostgreSQL-native aggregation functions (e.g., `json_agg` or `json_build_object`) inside custom raw SQL queries. 

Because the low-level Fermyon Spin PostgreSQL WASI driver doesn't have native, high-level rust-postgres type-mappings for aggregated JSON results, it returns them under the low-level `DbValue::Unsupported(Vec<u8>)` (or `SpinPgDbVal::Unsupported`) variant rather than standard strings or structured types. 

If this unsupported byte payload is unmatched or printed raw via its debug representation, it generates a non-JSON debug string like `"DbValue::Unsupported([91, 123, ...])"`. Feeding this raw string into `serde_json::from_str` throws the `expected value at line 1 column 1` error (because the string begins with `"D"` instead of valid JSON brackets/braces `[` or `{`).

#### Solution
Our database adapter ([adapters.rs](file:///Users/uriah/Code/ddd/src/adapters.rs)) has been updated to explicitly pattern-match `SpinPgDbVal::Unsupported(b)` and `SpinPgDbVal::Jsonb(j)` variants.

Instead of falling back to debug string formatting, the adapter attempts to parse the raw byte slices directly using `serde_json::from_slice`:

```rust
SpinPgDbVal::Unsupported(b) => {
    if let Ok(jv) = serde_json::from_slice::<serde_json::Value>(b) {
        jv
    } else {
        // Fallback to lossy UTF-8 if it is not valid JSON
        serde_json::Value::String(String::from_utf8_lossy(b).into_owned())
    }
}
```

If you write **custom repository methods** or **custom raw database query handlers** in your application:
* Avoid treating JSON-aggregated query columns as simple text strings (`DbValue::Str`).
* Always check if your driver returns them as `DbValue::Unsupported` or `DbValue::Jsonb` byte arrays.
* Safely parse using `serde_json::from_slice` to prevent decoding crashes.

---

## 🗄️ SQLite Driver Pitfalls

### Issue: Dynamic Type Conversions
In SQLite, columns are dynamically typed (affinity-based). If a query column contains `json_object` or `json_group_array`, the driver may return the result as a raw `Value::Blob(Vec<u8>)` or `Value::Text(String)`.

* **Pitfall**: Attempting to extract a text string using `.as_str()` directly on a value returned as a `Blob` will fail.
* **Best Practice**: Use our core adapter's helper methods, which safely handle both text strings and binary bytes dynamically:
  ```rust
  let text = match value {
      Value::Text(s) => s.clone(),
      Value::Blob(b) => String::from_utf8_lossy(b).into_owned(),
      _ => return Err("Unexpected sqlite type".to_string()),
  };
  ```

---

## 🔄 Concurrency Collision vs. Database Locking

### Symptom: `RepositoryError::Concurrency` vs Gateway Timeout
* **Concurrency Collision**: Occurs when two request threads try to commit events with the same `revision` sequence under Optimistic Concurrency Control (OCC). This is a **healthy, expected system transition**. The transaction rolls back cleanly, and the client should load the latest event stream and retry.
* **Database Deadlock / Timeout**: Occurs when your database connection pool is exhausted or a connection block is held open indefinitely by long-running transactions.
  * **Tip**: In Spin or Wasmtime serverless environments, avoid holding long-running synchronous locks or blocks. Projections should write and checkpoint asynchronously to prevent blocking the write transaction paths.
