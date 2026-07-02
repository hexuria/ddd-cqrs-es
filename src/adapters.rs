//! # ddd_cqrs_es adapters
//!
//! Stable built-in persistence adapters are exposed as `SqliteEventStore`,
//! `PostgresEventStore`, `SqliteCheckpointStore`, and
//! `PostgresCheckpointStore`, plus the SQL idempotency stores, when the
//! corresponding SQL feature is enabled.
//!
//! This module contains shared schema snippets and experimental WASI/Spin
//! query helpers for runtime-specific transports such as Neon, Supabase,
//! LibSQL, raw PostgreSQL TCP, and Spin host calls. These helpers are not
//! general-purpose SQL parameterization APIs and are not full event-store or
//! checkpoint-store backends until they implement the reusable library traits.

pub const EVENTS_TABLE_SCHEMA_POSTGRES: &str = r#"
CREATE TABLE IF NOT EXISTS events (
    sequence BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    aggregate_id TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    revision BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    event_version INT NOT NULL,
    payload JSONB NOT NULL,
    metadata JSONB NOT NULL,
    recorded_at_ms BIGINT NOT NULL,
    UNIQUE (aggregate_type, aggregate_id, revision)
);
"#;

pub const CHECKPOINTS_TABLE_SCHEMA_POSTGRES: &str = r#"
CREATE TABLE IF NOT EXISTS checkpoints (
    projection_name VARCHAR(255) PRIMARY KEY,
    last_sequence BIGINT NOT NULL
);
"#;

pub const EVENTS_TABLE_SCHEMA_SQLITE: &str = r#"
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT NOT NULL UNIQUE,
    aggregate_id TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    revision INTEGER NOT NULL,
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    event_version INTEGER NOT NULL,
    payload TEXT NOT NULL,
    metadata TEXT NOT NULL,
    recorded_at_ms INTEGER NOT NULL,
    UNIQUE (aggregate_id, aggregate_type, revision)
);
"#;

pub const CHECKPOINTS_TABLE_SCHEMA_SQLITE: &str = r#"
CREATE TABLE IF NOT EXISTS checkpoints (
    projection_name TEXT PRIMARY KEY,
    last_sequence INTEGER NOT NULL
);
"#;

#[cfg(feature = "wasi-neon")]
fn env_non_empty(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|s| !s.is_empty())
}

/// Helper to decode any database JSON row to standard EventEnvelope
pub fn row_to_envelope<E, Id>(
    row: &serde_json::Value,
) -> Result<crate::event::EventEnvelope<E, Id>, String>
where
    E: serde::de::DeserializeOwned,
    Id: serde::de::DeserializeOwned,
{
    let obj = row
        .as_object()
        .ok_or_else(|| "Row is not a JSON object".to_string())?;

    let event_id_str = obj
        .get("event_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Missing or invalid event_id".to_string())?;

    let aggregate_id_str = obj
        .get("aggregate_id")
        .and_then(|v| {
            if let Some(s) = v.as_str() {
                Some(s.to_string())
            } else {
                serde_json::to_string(v).ok()
            }
        })
        .ok_or_else(|| "Missing or invalid aggregate_id".to_string())?;

    let aggregate_type = obj
        .get("aggregate_type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Missing or invalid aggregate_type".to_string())?
        .to_string();

    let revision_val = obj
        .get("revision")
        .ok_or_else(|| "Missing revision".to_string())?;
    let revision = if let Some(s) = revision_val.as_str() {
        s.parse::<u64>().map_err(|e| e.to_string())?
    } else {
        revision_val
            .as_u64()
            .ok_or_else(|| "Invalid revision type".to_string())?
    };

    let sequence_val = obj.get("sequence");
    let sequence = match sequence_val {
        Some(v) if !v.is_null() => {
            if let Some(s) = v.as_str() {
                s.parse::<u64>().ok()
            } else {
                v.as_u64()
            }
        }
        _ => None,
    };

    let event_type = obj
        .get("event_type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Missing or invalid event_type".to_string())?
        .to_string();

    let event_version_val = obj
        .get("event_version")
        .ok_or_else(|| "Missing event_version".to_string())?;
    let event_version = if let Some(s) = event_version_val.as_str() {
        s.parse::<u32>().map_err(|e| e.to_string())?
    } else {
        event_version_val
            .as_u64()
            .ok_or_else(|| "Invalid event_version type".to_string())? as u32
    };

    let payload_val = obj
        .get("payload")
        .ok_or_else(|| "Missing payload".to_string())?;
    let payload: E = match payload_val {
        serde_json::Value::String(s) => {
            serde_json::from_str(s).map_err(|e| format!("payload string deserialize: {}", e))?
        }
        other => serde_json::from_value(other.clone())
            .map_err(|e| format!("payload value deserialize: {}", e))?,
    };

    let metadata_val = obj
        .get("metadata")
        .ok_or_else(|| "Missing metadata".to_string())?;
    let metadata: crate::metadata::Metadata = match metadata_val {
        serde_json::Value::String(s) => {
            serde_json::from_str(s).map_err(|e| format!("metadata string deserialize: {}", e))?
        }
        other => serde_json::from_value(other.clone())
            .map_err(|e| format!("metadata value deserialize: {}", e))?,
    };

    let recorded_at_ms_val = obj
        .get("recorded_at_ms")
        .ok_or_else(|| "Missing recorded_at_ms".to_string())?;
    let recorded_at_ms = if let Some(s) = recorded_at_ms_val.as_str() {
        s.parse::<i64>().map_err(|e| e.to_string())?
    } else {
        recorded_at_ms_val
            .as_i64()
            .ok_or_else(|| "Invalid recorded_at_ms type".to_string())?
    };

    let duration = std::time::Duration::from_millis(recorded_at_ms as u64);
    let recorded_at = std::time::UNIX_EPOCH + duration;

    let aggregate_id: Id = serde_json::from_str(&aggregate_id_str)
        .or_else(|_| serde_json::from_value(serde_json::Value::String(aggregate_id_str.clone())))
        .map_err(|e| format!("aggregate_id deserialization failure: {}", e))?;

    Ok(crate::event::EventEnvelope::new(
        crate::event::EventId::from_string(event_id_str.to_string()),
        aggregate_id,
        aggregate_type,
        revision,
        sequence,
        event_type,
        event_version,
        payload,
        metadata,
        recorded_at,
    ))
}

// -------------------------------------------------------------------------
// WASI HTTP Outbound post client helper
// -------------------------------------------------------------------------
#[cfg(feature = "wasi-http")]
pub async fn wasi_http_post(
    url: &str,
    headers: Vec<(String, String)>,
    body_data: Vec<u8>,
) -> Result<Vec<u8>, String> {
    use http_body_util::BodyExt;
    let body = http_body_util::Full::new(bytes::Bytes::from(body_data));
    let mut req_builder = http::Request::builder().method("POST").uri(url);

    for (name, value) in headers {
        req_builder = req_builder.header(name, value);
    }

    let req = req_builder
        .body(body)
        .map_err(|e| format!("Failed to build HTTP request: {:?}", e))?;

    let wasi_req = wasip3::http_compat::http_into_wasi_request(req)
        .map_err(|e| format!("Failed to convert to WASI request: {:?}", e))?;

    let wasi_resp = wasip3::http::client::send(wasi_req)
        .await
        .map_err(|e| format!("WASI HTTP send error: {:?}", e))?;

    let http_resp = wasip3::http_compat::http_from_wasi_response(wasi_resp)
        .map_err(|e| format!("Failed to convert from WASI response: {:?}", e))?;

    let status = http_resp.status();
    let body_bytes = http_resp
        .into_body()
        .collect()
        .await
        .map_err(|e| format!("Failed to collect response body: {:?}", e))?
        .to_bytes()
        .to_vec();

    if !status.is_success() {
        let body_str = String::from_utf8_lossy(&body_bytes);
        return Err(format!(
            "HTTP request failed with status {}: {}",
            status, body_str
        ));
    }

    Ok(body_bytes)
}

// -------------------------------------------------------------------------
// Postgres formatting & local query interpolation helpers
// -------------------------------------------------------------------------
pub fn format_pg_value(val: &serde_json::Value) -> Result<String, String> {
    match val {
        serde_json::Value::Null => Ok("NULL".to_string()),
        serde_json::Value::Bool(b) => Ok(if *b { "true" } else { "false" }.to_string()),
        serde_json::Value::Number(n) => Ok(n.to_string()),
        serde_json::Value::String(s) => {
            let escaped = s.replace('\'', "''");
            Ok(format!("'{}'", escaped))
        }
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            let s = serde_json::to_string(val).map_err(|e| e.to_string())?;
            let escaped = s.replace('\'', "''");
            Ok(format!("'{}'", escaped))
        }
    }
}

pub fn interpolate_query(sql: &str, params: &[serde_json::Value]) -> Result<String, String> {
    let mut final_sql = String::new();
    let mut chars = sql.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '$' {
            let mut digits = String::new();
            while let Some(&next_c) = chars.peek() {
                if next_c.is_ascii_digit() {
                    digits.push(next_c);
                    chars.next();
                } else {
                    break;
                }
            }
            if digits.is_empty() {
                final_sql.push('$');
            } else {
                let idx = digits.parse::<usize>().map_err(|e| e.to_string())?;
                if idx == 0 || idx > params.len() {
                    return Err(format!(
                        "Parameter index ${} out of bounds (params len: {})",
                        idx,
                        params.len()
                    ));
                }
                let param_val = &params[idx - 1];
                let formatted = format_pg_value(param_val)?;
                final_sql.push_str(&formatted);
            }
        } else {
            final_sql.push(c);
        }
    }

    Ok(final_sql)
}

pub fn base64_encode(input: &[u8]) -> String {
    const CHARSET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity(input.len().div_ceil(3) * 4);
    let mut i = 0;
    while i < input.len() {
        let b0 = input[i] as usize;
        let b1 = if i + 1 < input.len() {
            input[i + 1] as usize
        } else {
            0
        };
        let b2 = if i + 2 < input.len() {
            input[i + 2] as usize
        } else {
            0
        };

        let triple = (b0 << 16) | (b1 << 8) | b2;

        result.push(CHARSET[(triple >> 18) & 63] as char);
        result.push(CHARSET[(triple >> 12) & 63] as char);
        result.push(if i + 1 < input.len() {
            CHARSET[(triple >> 6) & 63] as char
        } else {
            '='
        });
        result.push(if i + 2 < input.len() {
            CHARSET[triple & 63] as char
        } else {
            '='
        });

        i += 3;
    }
    result
}

// -------------------------------------------------------------------------
// Neon / Serverless Postgres HTTP adapter
// -------------------------------------------------------------------------
#[cfg(feature = "wasi-neon")]
pub async fn execute_neon_query(
    url: &str,
    sql: &str,
    params: Vec<serde_json::Value>,
) -> Result<Vec<serde_json::Value>, String> {
    let req_payload = serde_json::json!({
        "query": sql,
        "params": params,
    });
    let body_data = serde_json::to_vec(&req_payload).map_err(|e| e.to_string())?;

    let mut headers = vec![
        ("Content-Type".to_string(), "application/json".to_string()),
        ("Neon-Raw-Text-Output".to_string(), "true".to_string()),
        ("Neon-Array-Mode".to_string(), "true".to_string()),
    ];

    let connection_string = env_non_empty("DATABASE_URL")
        .or_else(|| env_non_empty("NEON_DB_URL"))
        .unwrap_or_else(|| url.to_string());
    if !connection_string.is_empty()
        && (connection_string.starts_with("postgres://")
            || connection_string.starts_with("postgresql://"))
    {
        headers.push(("Neon-Connection-String".to_string(), connection_string));
    }

    let http_url = if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        let stripped = url
            .strip_prefix("postgres://")
            .or_else(|| url.strip_prefix("postgresql://"))
            .unwrap_or(url);
        let host_part = if let Some(at_idx) = stripped.find('@') {
            &stripped[at_idx + 1..]
        } else {
            stripped
        };
        let host = if let Some(slash_idx) = host_part.find('/') {
            &host_part[..slash_idx]
        } else if let Some(query_idx) = host_part.find('?') {
            &host_part[..query_idx]
        } else {
            host_part
        };
        let host_name = if let Some(colon_idx) = host.find(':') {
            &host[..colon_idx]
        } else {
            host
        };
        format!("https://{}/sql", host_name)
    } else {
        url.to_string()
    };

    let resp_bytes = wasi_http_post(&http_url, headers, body_data).await?;

    let resp_val: serde_json::Value = serde_json::from_slice(&resp_bytes)
        .map_err(|e| format!("Failed to parse Neon response JSON: {}", e))?;

    let mut parsed_rows = Vec::new();
    if let Some(arr) = resp_val.as_array() {
        parsed_rows = arr.clone();
    } else if let Some(obj) = resp_val.as_object() {
        if let (Some(fields_val), Some(rows_val)) = (obj.get("fields"), obj.get("rows")) {
            if let (Some(fields_arr), Some(rows_arr)) = (fields_val.as_array(), rows_val.as_array())
            {
                let col_names: Vec<String> = fields_arr
                    .iter()
                    .map(|f| {
                        f.get("name")
                            .and_then(|n| n.as_str())
                            .unwrap_or("")
                            .to_string()
                    })
                    .collect();

                for row_val in rows_arr {
                    if let Some(row_arr) = row_val.as_array() {
                        let mut row_obj = serde_json::Map::new();
                        for (i, col_val) in row_arr.iter().enumerate() {
                            if i < col_names.len() {
                                row_obj.insert(col_names[i].clone(), col_val.clone());
                            }
                        }
                        parsed_rows.push(serde_json::Value::Object(row_obj));
                    }
                }
            }
        }
    }

    Ok(parsed_rows)
}

// -------------------------------------------------------------------------
// Supabase PostgREST RPC HTTP adapter
// -------------------------------------------------------------------------
#[cfg(feature = "wasi-supabase-rpc")]
pub async fn execute_supabase_query(
    url: &str,
    secret_key: Option<&str>,
    sql: &str,
    params: Vec<serde_json::Value>,
) -> Result<Vec<serde_json::Value>, String> {
    let rpc_url = if url.ends_with("/rest/v1/rpc/execute_sql") {
        url.to_string()
    } else {
        format!("{}/rest/v1/rpc/execute_sql", url.trim_end_matches('/'))
    };

    let interpolated_sql = interpolate_query(sql, &params)?;

    let req_payload = serde_json::json!({
        "query_text": interpolated_sql,
        "query_params": Vec::<serde_json::Value>::new(),
    });
    let body_data = serde_json::to_vec(&req_payload).map_err(|e| e.to_string())?;

    let mut headers = vec![("Content-Type".to_string(), "application/json".to_string())];
    if let Some(key) = secret_key {
        headers.push(("apikey".to_string(), key.to_string()));
        headers.push(("Authorization".to_string(), format!("Bearer {}", key)));
    }

    let resp_bytes = wasi_http_post(&rpc_url, headers, body_data).await?;
    let resp_val: serde_json::Value = serde_json::from_slice(&resp_bytes)
        .map_err(|e| format!("Failed to parse Supabase response: {}", e))?;

    if let Some(err_obj) = resp_val.as_object() {
        if let Some(err_msg) = err_obj.get("error") {
            return Err(format!("Supabase SQL error: {}", err_msg));
        }

        if let Some(message) = err_obj.get("message").and_then(|v| v.as_str()) {
            let code = err_obj
                .get("code")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let details = err_obj
                .get("details")
                .and_then(|v| v.as_str())
                .filter(|v| !v.is_empty())
                .map(|v| format!(" details: {}", v))
                .unwrap_or_default();
            let hint = err_obj
                .get("hint")
                .and_then(|v| v.as_str())
                .filter(|v| !v.is_empty())
                .map(|v| format!(" hint: {}", v))
                .unwrap_or_default();

            return Err(format!(
                "Supabase SQL error [{}]: {}{}{}",
                code, message, details, hint
            ));
        }
    }

    let rows = resp_val
        .as_array()
        .cloned()
        .unwrap_or_else(|| vec![resp_val]);

    Ok(rows)
}

// -------------------------------------------------------------------------
// Turso / LibSQL Hrana /v2/pipeline HTTP adapter
// -------------------------------------------------------------------------
#[cfg(feature = "wasi-libsql")]
pub struct LibSqlResult {
    pub rows: Vec<serde_json::Value>,
    pub last_insert_rowid: Option<u64>,
}

#[cfg(feature = "wasi-libsql")]
fn to_hrana_arg(val: serde_json::Value) -> serde_json::Value {
    match val {
        serde_json::Value::Null => serde_json::json!({ "type": "null" }),
        serde_json::Value::Bool(b) => {
            serde_json::json!({ "type": "integer", "value": if b { "1" } else { "0" } })
        }
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                serde_json::json!({ "type": "integer", "value": i.to_string() })
            } else if let Some(f) = n.as_f64() {
                serde_json::json!({ "type": "float", "value": f })
            } else {
                serde_json::json!({ "type": "null" })
            }
        }
        serde_json::Value::String(s) => serde_json::json!({ "type": "text", "value": s }),
        _ => serde_json::json!({ "type": "text", "value": val.to_string() }),
    }
}

#[cfg(feature = "wasi-libsql")]
fn from_hrana_val(val: &serde_json::Value) -> serde_json::Value {
    if let Some(t) = val.get("type").and_then(|v| v.as_str()) {
        match t {
            "null" => serde_json::Value::Null,
            "text" => val.get("value").cloned().unwrap_or(serde_json::Value::Null),
            "integer" => {
                if let Some(s) = val.get("value").and_then(|v| v.as_str()) {
                    if let Ok(i) = s.parse::<i64>() {
                        serde_json::Value::Number(serde_json::Number::from(i))
                    } else {
                        serde_json::Value::Null
                    }
                } else {
                    serde_json::Value::Null
                }
            }
            "float" => val.get("value").cloned().unwrap_or(serde_json::Value::Null),
            "blob" => val
                .get("base64")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
            _ => serde_json::Value::Null,
        }
    } else {
        serde_json::Value::Null
    }
}

#[cfg(feature = "wasi-libsql")]
fn parse_libsql_result(resp: &serde_json::Value) -> Result<LibSqlResult, String> {
    let results = resp
        .get("results")
        .and_then(|r| r.as_array())
        .ok_or_else(|| "Missing results array in LibSQL response".to_string())?;

    for res in results {
        if let Some(t) = res.get("type").and_then(|v| v.as_str()) {
            if t == "error" {
                let msg = res
                    .get("error")
                    .and_then(|e| e.get("message"))
                    .and_then(|m| m.as_str())
                    .unwrap_or("Unknown error");
                return Err(format!("LibSQL query error: {}", msg));
            }
        }

        if let Some(response) = res.get("response") {
            if let Some(result) = response.get("result") {
                let cols = result
                    .get("cols")
                    .and_then(|c| c.as_array())
                    .ok_or_else(|| "Missing cols in LibSQL execute result".to_string())?;

                let rows_array = result
                    .get("rows")
                    .and_then(|r| r.as_array())
                    .ok_or_else(|| "Missing rows in LibSQL execute result".to_string())?;

                let last_insert_rowid = result.get("last_insert_rowid").and_then(|v| {
                    if let Some(s) = v.as_str() {
                        s.parse::<u64>().ok()
                    } else {
                        v.as_u64()
                    }
                });

                let col_names: Vec<String> = cols
                    .iter()
                    .map(|c| {
                        c.get("name")
                            .and_then(|n| n.as_str())
                            .unwrap_or("")
                            .to_string()
                    })
                    .collect();

                let mut rows = Vec::new();
                for r in rows_array {
                    let r_arr = r
                        .as_array()
                        .ok_or_else(|| "Row is not an array".to_string())?;

                    let mut obj = serde_json::Map::new();
                    for (i, val) in r_arr.iter().enumerate() {
                        if let Some(col_name) = col_names.get(i) {
                            obj.insert(col_name.clone(), from_hrana_val(val));
                        }
                    }
                    rows.push(serde_json::Value::Object(obj));
                }
                return Ok(LibSqlResult {
                    rows,
                    last_insert_rowid,
                });
            }
        }
    }

    Ok(LibSqlResult {
        rows: Vec::new(),
        last_insert_rowid: None,
    })
}

#[cfg(feature = "wasi-libsql")]
pub async fn execute_libsql_query(
    url: &str,
    auth_token: Option<&str>,
    sql: &str,
    params: Vec<serde_json::Value>,
) -> Result<LibSqlResult, String> {
    let hrana_args: Vec<serde_json::Value> = params.into_iter().map(to_hrana_arg).collect();

    let req_payload = serde_json::json!({
        "baton": null,
        "requests": [
            {
                "type": "execute",
                "stmt": {
                    "sql": sql,
                    "args": hrana_args
                }
            },
            {
                "type": "close"
            }
        ]
    });

    let body_data = serde_json::to_vec(&req_payload).map_err(|e| e.to_string())?;

    let mut headers = vec![("Content-Type".to_string(), "application/json".to_string())];
    if let Some(tok) = auth_token {
        headers.push(("Authorization".to_string(), format!("Bearer {}", tok)));
    }

    let resolved_url = if let Some(rest) = url.strip_prefix("libsql://") {
        format!("https://{}", rest)
    } else {
        url.to_string()
    };

    let pipeline_url = if resolved_url.ends_with("/v2/pipeline") {
        resolved_url
    } else if resolved_url.ends_with('/') {
        format!("{}v2/pipeline", resolved_url)
    } else {
        format!("{}/v2/pipeline", resolved_url)
    };

    let resp_bytes = wasi_http_post(&pipeline_url, headers, body_data).await?;
    let resp_json: serde_json::Value = serde_json::from_slice(&resp_bytes)
        .map_err(|e| format!("Failed to parse LibSQL response: {}", e))?;

    parse_libsql_result(&resp_json)
}

// -------------------------------------------------------------------------
// Wasmtime Raw TCP PostgreSQL driver (requires no library other than TLS/crypto)
// -------------------------------------------------------------------------
#[cfg(feature = "wasi-postgres-tcp")]
pub struct PgConnParams {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub database: String,
}

#[cfg(feature = "wasi-postgres-tcp")]
pub fn parse_pg_url(url: &str) -> Result<PgConnParams, String> {
    let stripped = if let Some(rest) = url.strip_prefix("postgres://") {
        rest
    } else if let Some(rest) = url.strip_prefix("postgresql://") {
        rest
    } else {
        return Err(format!("Invalid postgres URL prefix: {}", url));
    };

    let (main_part, _) = if let Some(q_idx) = stripped.find('?') {
        (&stripped[..q_idx], &stripped[q_idx + 1..])
    } else {
        (stripped, "")
    };

    let (auth_part, host_db_part) = if let Some(at_idx) = main_part.find('@') {
        (Some(&main_part[..at_idx]), &main_part[at_idx + 1..])
    } else {
        (None, main_part)
    };

    let mut user = "postgres".to_string();
    let mut password = None;
    if let Some(auth) = auth_part {
        if let Some(colon_idx) = auth.find(':') {
            user = auth[..colon_idx].to_string();
            password = Some(auth[colon_idx + 1..].to_string());
        } else {
            user = auth.to_string();
        }
    }

    let (host_port_part, database) = if let Some(slash_idx) = host_db_part.find('/') {
        (
            &host_db_part[..slash_idx],
            host_db_part[slash_idx + 1..].to_string(),
        )
    } else {
        (host_db_part, "postgres".to_string())
    };

    let mut host = host_port_part.to_string();
    let mut port = 5432;
    if let Some(colon_idx) = host_port_part.find(':') {
        host = host_port_part[..colon_idx].to_string();
        if let Ok(p) = host_port_part[colon_idx + 1..].parse::<u16>() {
            port = p;
        }
    }

    if host.is_empty() {
        host = "localhost".to_string();
    }
    let database = if database.is_empty() {
        "postgres".to_string()
    } else {
        database
    };

    Ok(PgConnParams {
        host,
        port,
        user,
        password,
        database,
    })
}

#[cfg(feature = "wasi-postgres-tcp")]
pub enum PgStream {
    Plain(std::net::TcpStream),
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, std::net::TcpStream>>),
}

#[cfg(feature = "wasi-postgres-tcp")]
impl std::io::Read for PgStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            PgStream::Plain(s) => s.read(buf),
            PgStream::Tls(s) => s.read(buf),
        }
    }
}

#[cfg(feature = "wasi-postgres-tcp")]
impl std::io::Write for PgStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            PgStream::Plain(s) => s.write(buf),
            PgStream::Tls(s) => s.write(buf),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            PgStream::Plain(s) => s.flush(),
            PgStream::Tls(s) => s.flush(),
        }
    }
}

#[cfg(feature = "wasi-postgres-tcp")]
fn write_startup_message(stream: &mut PgStream, user: &str, database: &str) -> std::io::Result<()> {
    use std::io::Write;
    let mut body = Vec::new();
    body.extend_from_slice(&0x00030000u32.to_be_bytes()); // Protocol v3.0

    body.extend_from_slice(b"user\0");
    body.extend_from_slice(user.as_bytes());
    body.extend_from_slice(b"\0");

    body.extend_from_slice(b"database\0");
    body.extend_from_slice(database.as_bytes());
    body.extend_from_slice(b"\0");

    body.extend_from_slice(b"\0");

    let length = (body.len() + 4) as u32;
    stream.write_all(&length.to_be_bytes())?;
    stream.write_all(&body)?;
    stream.flush()?;
    Ok(())
}

#[cfg(feature = "wasi-postgres-tcp")]
fn write_query_message(stream: &mut PgStream, sql: &str) -> std::io::Result<()> {
    use std::io::Write;
    let mut body = Vec::new();
    body.extend_from_slice(sql.as_bytes());
    body.extend_from_slice(b"\0");

    let length = (body.len() + 4) as u32;
    stream.write_all(b"Q")?;
    stream.write_all(&length.to_be_bytes())?;
    stream.write_all(&body)?;
    stream.flush()?;
    Ok(())
}

#[cfg(feature = "wasi-postgres-tcp")]
fn write_password_message(stream: &mut PgStream, password: &str) -> std::io::Result<()> {
    use std::io::Write;
    let mut body = Vec::new();
    body.extend_from_slice(password.as_bytes());
    body.extend_from_slice(b"\0");

    let length = (body.len() + 4) as u32;
    stream.write_all(b"p")?;
    stream.write_all(&length.to_be_bytes())?;
    stream.write_all(&body)?;
    stream.flush()?;
    Ok(())
}

#[cfg(feature = "wasi-postgres-tcp")]
fn generate_client_nonce() -> String {
    use std::time::SystemTime;
    let seed = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(42) as u64;

    let mut rng = seed;
    let chars = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut nonce = String::with_capacity(24);
    for _ in 0..24 {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let idx = (rng % chars.len() as u64) as usize;
        nonce.push(chars[idx] as char);
    }
    nonce
}

#[cfg(feature = "wasi-postgres-tcp")]
fn write_sasl_initial_response(
    stream: &mut PgStream,
    mechanism: &str,
    initial_response: &[u8],
) -> std::io::Result<()> {
    use std::io::Write;
    let mut body = Vec::new();
    body.extend_from_slice(mechanism.as_bytes());
    body.push(0);

    let data_len = initial_response.len() as i32;
    body.extend_from_slice(&data_len.to_be_bytes());
    body.extend_from_slice(initial_response);

    let length = (body.len() + 4) as u32;
    stream.write_all(b"p")?;
    stream.write_all(&length.to_be_bytes())?;
    stream.write_all(&body)?;
    stream.flush()?;
    Ok(())
}

#[cfg(feature = "wasi-postgres-tcp")]
fn write_sasl_response(stream: &mut PgStream, response: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut body = Vec::new();
    body.extend_from_slice(response);

    let length = (body.len() + 4) as u32;
    stream.write_all(b"p")?;
    stream.write_all(&length.to_be_bytes())?;
    stream.write_all(&body)?;
    stream.flush()?;
    Ok(())
}

#[cfg(feature = "wasi-postgres-tcp")]
struct PgMessage {
    msg_type: u8,
    payload: Vec<u8>,
}

#[cfg(feature = "wasi-postgres-tcp")]
fn read_message(stream: &mut PgStream) -> std::io::Result<PgMessage> {
    use std::io::Read;
    let mut type_buf = [0u8; 1];
    stream.read_exact(&mut type_buf)?;
    let msg_type = type_buf[0];

    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let length = u32::from_be_bytes(len_buf);

    if length < 4 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid message length",
        ));
    }

    let payload_len = (length - 4) as usize;
    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        stream.read_exact(&mut payload)?;
    }

    Ok(PgMessage { msg_type, payload })
}

#[cfg(feature = "wasi-postgres-tcp")]
fn parse_error_response(payload: &[u8]) -> String {
    let mut parts = Vec::new();
    let mut idx = 0;
    while idx < payload.len() && payload[idx] != 0 {
        let field_type = payload[idx] as char;
        idx += 1;
        let mut end = idx;
        while end < payload.len() && payload[end] != 0 {
            end += 1;
        }
        if let Ok(s) = std::str::from_utf8(&payload[idx..end]) {
            parts.push(format!("{}: {}", field_type, s));
        }
        idx = end + 1;
    }
    parts.join(", ")
}

#[cfg(feature = "wasi-postgres-tcp")]
struct PgColumn {
    name: String,
    type_oid: u32,
}

#[cfg(feature = "wasi-postgres-tcp")]
fn parse_row_description(payload: &[u8]) -> std::io::Result<Vec<PgColumn>> {
    if payload.len() < 2 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid RowDescription length",
        ));
    }
    let num_fields = u16::from_be_bytes([payload[0], payload[1]]) as usize;
    let mut columns = Vec::with_capacity(num_fields);

    let mut idx = 2;
    for _ in 0..num_fields {
        let start = idx;
        while idx < payload.len() && payload[idx] != 0 {
            idx += 1;
        }
        if idx >= payload.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid RowDescription name",
            ));
        }
        let name = std::str::from_utf8(&payload[start..idx])
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?
            .to_string();
        idx += 1;

        if idx + 18 > payload.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid RowDescription column metadata",
            ));
        }
        idx += 6;
        let type_oid = u32::from_be_bytes([
            payload[idx],
            payload[idx + 1],
            payload[idx + 2],
            payload[idx + 3],
        ]);
        idx += 4;
        idx += 8;

        columns.push(PgColumn { name, type_oid });
    }

    Ok(columns)
}

#[cfg(feature = "wasi-postgres-tcp")]
fn parse_data_row(payload: &[u8], columns: &[PgColumn]) -> std::io::Result<serde_json::Value> {
    if payload.len() < 2 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid DataRow length",
        ));
    }
    let num_values = u16::from_be_bytes([payload[0], payload[1]]) as usize;
    if num_values != columns.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "DataRow column count mismatch",
        ));
    }

    let mut row_obj = serde_json::Map::new();
    let mut idx = 2;

    for col in columns {
        if idx + 4 > payload.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid DataRow column size prefix",
            ));
        }
        let val_len = i32::from_be_bytes([
            payload[idx],
            payload[idx + 1],
            payload[idx + 2],
            payload[idx + 3],
        ]);
        idx += 4;

        if val_len < 0 {
            row_obj.insert(col.name.clone(), serde_json::Value::Null);
        } else {
            let val_len = val_len as usize;
            if idx + val_len > payload.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid DataRow column data length",
                ));
            }
            let val_bytes = &payload[idx..idx + val_len];
            idx += val_len;

            let text_val = std::str::from_utf8(val_bytes)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

            let json_val = match col.type_oid {
                16 => serde_json::Value::Bool(
                    text_val == "t" || text_val == "true" || text_val == "1",
                ),
                20 | 21 | 23 => {
                    if let Ok(i) = text_val.parse::<i64>() {
                        serde_json::Value::Number(serde_json::Number::from(i))
                    } else {
                        serde_json::Value::String(text_val.to_string())
                    }
                }
                700 | 701 | 1700 => {
                    if let Ok(f) = text_val.parse::<f64>() {
                        if let Some(num) = serde_json::Number::from_f64(f) {
                            serde_json::Value::Number(num)
                        } else {
                            serde_json::Value::String(text_val.to_string())
                        }
                    } else {
                        serde_json::Value::String(text_val.to_string())
                    }
                }
                114 | 3802 => {
                    if let Ok(jv) = serde_json::from_str::<serde_json::Value>(text_val) {
                        jv
                    } else {
                        serde_json::Value::String(text_val.to_string())
                    }
                }
                _ => serde_json::Value::String(text_val.to_string()),
            };
            row_obj.insert(col.name.clone(), json_val);
        }
    }

    Ok(serde_json::Value::Object(row_obj))
}

#[cfg(feature = "wasi-postgres-tcp")]
static PG_CONN: std::sync::Mutex<Option<(String, PgStream)>> = std::sync::Mutex::new(None);

#[cfg(feature = "wasi-postgres-tcp")]
pub fn connect_and_auth_postgres(
    url: &str,
    pg_params: &PgConnParams,
    addr: &str,
) -> Result<PgStream, String> {
    let mut stream = std::net::TcpStream::connect(addr)
        .map_err(|e| format!("Failed to connect to Postgres: {}", e))?;

    use std::io::{Read, Write};

    let ssl_request = [0u8, 0, 0, 8, 4, 210, 22, 47];
    stream
        .write_all(&ssl_request)
        .map_err(|e| format!("Failed to send SSLRequest: {}", e))?;
    stream
        .flush()
        .map_err(|e| format!("Failed to flush SSLRequest: {}", e))?;

    let mut ssl_response = [0u8; 1];
    stream
        .read_exact(&mut ssl_response)
        .map_err(|e| format!("Failed to read SSLRequest response: {}", e))?;

    let mut pg_stream = if ssl_response[0] == b'S' {
        let _ = rustls_rustcrypto::provider().install_default();

        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        let config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        let server_name = rustls::pki_types::ServerName::try_from(pg_params.host.as_str())
            .map_err(|e| format!("Invalid server name '{}': {}", pg_params.host, e))?
            .to_owned();

        let conn = rustls::ClientConnection::new(std::sync::Arc::new(config), server_name)
            .map_err(|e| format!("Failed to create rustls ClientConnection: {}", e))?;

        let tls_stream = rustls::StreamOwned::new(conn, stream);
        PgStream::Tls(Box::new(tls_stream))
    } else if ssl_response[0] == b'N' {
        if url.contains("sslmode=require") {
            return Err(
                "Server rejected SSL request, but sslmode=require was requested".to_string(),
            );
        }
        PgStream::Plain(stream)
    } else {
        return Err(format!(
            "Unexpected response to SSLRequest: {:?}",
            ssl_response[0] as char
        ));
    };

    write_startup_message(&mut pg_stream, &pg_params.user, &pg_params.database)
        .map_err(|e| format!("Failed to send startup message: {}", e))?;

    loop {
        let msg = read_message(&mut pg_stream)
            .map_err(|e| format!("Failed to read message from Postgres: {}", e))?;

        match msg.msg_type {
            b'R' => {
                if msg.payload.len() < 4 {
                    return Err("Invalid AuthenticationRequest payload".to_string());
                }
                let auth_type = u32::from_be_bytes([
                    msg.payload[0],
                    msg.payload[1],
                    msg.payload[2],
                    msg.payload[3],
                ]);
                match auth_type {
                    0 => {} // Auth OK
                    3 => {
                        let pwd = pg_params.password.as_deref().unwrap_or("");
                        write_password_message(&mut pg_stream, pwd)
                            .map_err(|e| format!("Failed to send password message: {}", e))?;
                    }
                    5 => {
                        if msg.payload.len() < 8 {
                            return Err("Invalid AuthenticationMD5Password payload".to_string());
                        }
                        let salt = &msg.payload[4..8];
                        let pwd = pg_params.password.as_deref().unwrap_or("");

                        let hash1 =
                            format!("{:x}", md5::compute(format!("{}{}", pwd, pg_params.user)));
                        let mut hash2_input = Vec::new();
                        hash2_input.extend_from_slice(hash1.as_bytes());
                        hash2_input.extend_from_slice(salt);
                        let hash2 = format!("md5{:x}", md5::compute(&hash2_input));

                        write_password_message(&mut pg_stream, &hash2)
                            .map_err(|e| format!("Failed to send MD5 password message: {}", e))?;
                    }
                    10 => {
                        let mut has_scram = false;
                        let mut idx = 4;
                        while idx < msg.payload.len() {
                            let start = idx;
                            while idx < msg.payload.len() && msg.payload[idx] != 0 {
                                idx += 1;
                            }
                            if idx < msg.payload.len() {
                                let mech =
                                    std::str::from_utf8(&msg.payload[start..idx]).unwrap_or("");
                                if mech == "SCRAM-SHA-256" {
                                    has_scram = true;
                                    break;
                                }
                            }
                            idx += 1;
                        }

                        if !has_scram {
                            return Err(
                                "Server SASL mechanisms do not support SCRAM-SHA-256".to_string()
                            );
                        }

                        let client_nonce = generate_client_nonce();
                        let client_first_message_bare =
                            format!("n={},r={}", pg_params.user, client_nonce);
                        let client_first_message = format!("n,,{}", client_first_message_bare);

                        write_sasl_initial_response(
                            &mut pg_stream,
                            "SCRAM-SHA-256",
                            client_first_message.as_bytes(),
                        )
                        .map_err(|e| format!("Failed to write SASLInitialResponse: {}", e))?;

                        let next_msg = read_message(&mut pg_stream)
                            .map_err(|e| format!("Failed to read SASLContinue message: {}", e))?;

                        if next_msg.msg_type == b'E' {
                            let err_msg = parse_error_response(&next_msg.payload);
                            return Err(format!("Postgres authentication failed: {}", err_msg));
                        }
                        if next_msg.msg_type != b'R' {
                            return Err(format!(
                                "Expected AuthenticationRequest ('R') during SASL, got '{}'",
                                next_msg.msg_type as char
                            ));
                        }

                        let next_auth_type = u32::from_be_bytes([
                            next_msg.payload[0],
                            next_msg.payload[1],
                            next_msg.payload[2],
                            next_msg.payload[3],
                        ]);
                        if next_auth_type != 11 {
                            return Err(format!(
                                "Expected SASLContinue (11), got {}",
                                next_auth_type
                            ));
                        }

                        let server_first_message_str = std::str::from_utf8(&next_msg.payload[4..])
                            .map_err(|e| format!("Invalid UTF-8 in SASLContinue payload: {}", e))?;

                        let mut server_nonce = "";
                        let mut salt_base64 = "";
                        let mut iterations_str = "";

                        for part in server_first_message_str.split(',') {
                            if let Some(val) = part.strip_prefix("r=") {
                                server_nonce = val;
                            } else if let Some(val) = part.strip_prefix("s=") {
                                salt_base64 = val;
                            } else if let Some(val) = part.strip_prefix("i=") {
                                iterations_str = val;
                            }
                        }

                        if !server_nonce.starts_with(&client_nonce) {
                            return Err(
                                "Server nonce does not match client nonce prefix".to_string()
                            );
                        }

                        use base64::Engine;
                        let salt = base64::engine::general_purpose::STANDARD
                            .decode(salt_base64)
                            .map_err(|e| format!("Invalid salt base64: {}", e))?;
                        let iterations = iterations_str.parse::<u32>().map_err(|e| {
                            format!("Invalid iterations '{}': {}", iterations_str, e)
                        })?;

                        let client_final_message_without_proof =
                            format!("c=biws,r={}", server_nonce);
                        let auth_message = format!(
                            "{},{},{}",
                            client_first_message_bare,
                            server_first_message_str,
                            client_final_message_without_proof
                        );

                        let mut salted_password = [0u8; 32];
                        let _ = pbkdf2::pbkdf2::<hmac::Hmac<sha2::Sha256>>(
                            pg_params.password.as_deref().unwrap_or("").as_bytes(),
                            &salt,
                            iterations,
                            &mut salted_password,
                        );

                        use hmac::Mac;
                        let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(&salted_password)
                            .map_err(|e| {
                                format!("Failed to create HMAC-SHA256 for Client Key: {}", e)
                            })?;
                        mac.update(b"Client Key");
                        let client_key = mac.finalize().into_bytes();

                        use sha2::Digest;
                        let mut hasher = sha2::Sha256::new();
                        hasher.update(client_key);
                        let stored_key = hasher.finalize();

                        let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(&stored_key)
                            .map_err(|e| {
                                format!("Failed to create HMAC-SHA256 for Client Signature: {}", e)
                            })?;
                        mac.update(auth_message.as_bytes());
                        let client_signature = mac.finalize().into_bytes();

                        let mut client_proof = [0u8; 32];
                        for i in 0..32 {
                            client_proof[i] = client_key[i] ^ client_signature[i];
                        }

                        let client_final_message = format!(
                            "{},p={}",
                            client_final_message_without_proof,
                            base64::engine::general_purpose::STANDARD.encode(client_proof)
                        );

                        write_sasl_response(&mut pg_stream, client_final_message.as_bytes())
                            .map_err(|e| format!("Failed to write SASLResponse: {}", e))?;

                        let final_msg = read_message(&mut pg_stream)
                            .map_err(|e| format!("Failed to read SASLFinal message: {}", e))?;

                        if final_msg.msg_type == b'E' {
                            let err_msg = parse_error_response(&final_msg.payload);
                            return Err(format!(
                                "Postgres authentication failed at final stage: {}",
                                err_msg
                            ));
                        }
                        if final_msg.msg_type != b'R' {
                            return Err(format!(
                                "Expected AuthenticationRequest ('R') during SASL final, got '{}'",
                                final_msg.msg_type as char
                            ));
                        }

                        let final_auth_type = u32::from_be_bytes([
                            final_msg.payload[0],
                            final_msg.payload[1],
                            final_msg.payload[2],
                            final_msg.payload[3],
                        ]);
                        if final_auth_type != 12 {
                            return Err(format!(
                                "Expected SASLFinal (12), got {}",
                                final_auth_type
                            ));
                        }
                    }
                    _ => {
                        return Err(format!(
                            "Unsupported PostgreSQL authentication type: {}",
                            auth_type
                        ));
                    }
                }
            }
            b'E' => {
                let err_msg = parse_error_response(&msg.payload);
                return Err(format!("Postgres error: {}", err_msg));
            }
            b'Z' => {
                break;
            }
            _ => {}
        }
    }

    Ok(pg_stream)
}

#[cfg(feature = "wasi-postgres-tcp")]
pub fn execute_query_on_stream(
    pg_stream: &mut PgStream,
    sql: &str,
    params: Vec<serde_json::Value>,
) -> Result<Vec<serde_json::Value>, String> {
    let interpolated = interpolate_query(sql, &params)?;
    write_query_message(pg_stream, &interpolated)
        .map_err(|e| format!("Failed to send query: {}", e))?;

    let mut columns = Vec::new();
    let mut rows = Vec::new();

    loop {
        let msg = read_message(pg_stream)
            .map_err(|e| format!("Failed to read message from Postgres: {}", e))?;

        match msg.msg_type {
            b'E' => {
                let err_msg = parse_error_response(&msg.payload);
                return Err(format!("Postgres error: {}", err_msg));
            }
            b'T' => {
                columns = parse_row_description(&msg.payload)
                    .map_err(|e| format!("Failed to parse row description: {}", e))?;
            }
            b'D' => {
                let row = parse_data_row(&msg.payload, &columns)
                    .map_err(|e| format!("Failed to parse data row: {}", e))?;
                rows.push(row);
            }
            b'Z' => {
                break;
            }
            _ => {}
        }
    }

    Ok(rows)
}

#[cfg(feature = "wasi-postgres-tcp")]
pub fn execute_raw_tcp_postgres(
    url: &str,
    sql: &str,
    params: Vec<serde_json::Value>,
) -> Result<Vec<serde_json::Value>, String> {
    let pg_params = parse_pg_url(url)?;
    let addr = format!("{}:{}", pg_params.host, pg_params.port);

    let mut guard = PG_CONN
        .lock()
        .map_err(|_| "Failed to lock PG_CONN mutex".to_string())?;

    let pg_stream = if let Some((ref cached_url, _)) = *guard {
        if cached_url == url {
            let (_, stream) = guard.take().unwrap();
            Some(stream)
        } else {
            guard.take();
            None
        }
    } else {
        None
    };

    if let Some(mut stream) = pg_stream {
        match execute_query_on_stream(&mut stream, sql, params.clone()) {
            Ok(rows) => {
                *guard = Some((url.to_string(), stream));
                return Ok(rows);
            }
            Err(_) => {
                // connection was stale, drop it and establish a fresh one below
            }
        }
    }

    let mut fresh_stream = connect_and_auth_postgres(url, &pg_params, &addr)?;
    let res = execute_query_on_stream(&mut fresh_stream, sql, params);

    if res.is_ok() {
        *guard = Some((url.to_string(), fresh_stream));
    }

    res
}

// -------------------------------------------------------------------------
// Raw TCP MySQL adapter for generic WASI runtimes like Wasmtime
// -------------------------------------------------------------------------
#[cfg(feature = "wasi-mysql")]
const MYSQL_CLIENT_LONG_PASSWORD: u32 = 0x0000_0001;
#[cfg(feature = "wasi-mysql")]
const MYSQL_CLIENT_LONG_FLAG: u32 = 0x0000_0004;
#[cfg(feature = "wasi-mysql")]
const MYSQL_CLIENT_CONNECT_WITH_DB: u32 = 0x0000_0008;
#[cfg(feature = "wasi-mysql")]
const MYSQL_CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
#[cfg(feature = "wasi-mysql")]
const MYSQL_CLIENT_TRANSACTIONS: u32 = 0x0000_2000;
#[cfg(feature = "wasi-mysql")]
const MYSQL_CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
#[cfg(feature = "wasi-mysql")]
const MYSQL_CLIENT_MULTI_RESULTS: u32 = 0x0002_0000;
#[cfg(feature = "wasi-mysql")]
const MYSQL_CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;

#[cfg(feature = "wasi-mysql")]
#[derive(Clone, Debug)]
struct MySqlConnParams {
    host: String,
    port: u16,
    user: String,
    password: Option<String>,
    database: String,
}

#[cfg(feature = "wasi-mysql")]
#[derive(Debug)]
struct MySqlHandshake {
    auth_plugin_name: String,
    auth_plugin_data: Vec<u8>,
}

#[cfg(feature = "wasi-mysql")]
#[derive(Clone, Debug)]
struct MySqlColumn {
    name: String,
    column_type: u8,
}

#[cfg(feature = "wasi-mysql")]
pub fn execute_raw_tcp_mysql(
    url: &str,
    sql: &str,
    params: Vec<serde_json::Value>,
) -> Result<Vec<serde_json::Value>, String> {
    let mysql_params = parse_mysql_url(url)?;
    let addr = format!("{}:{}", mysql_params.host, mysql_params.port);
    let mut stream = std::net::TcpStream::connect(&addr)
        .map_err(|error| format!("Failed to connect to MySQL at {addr}: {error}"))?;
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .map_err(|error| format!("Failed to set MySQL read timeout: {error}"))?;
    stream
        .set_write_timeout(Some(std::time::Duration::from_secs(10)))
        .map_err(|error| format!("Failed to set MySQL write timeout: {error}"))?;

    mysql_authenticate(&mut stream, &mysql_params)?;

    let interpolated = interpolate_mysql_query(sql, &params)?;
    let sql_upper = interpolated.trim_start().to_ascii_uppercase();
    let returns_rows = sql_upper.starts_with("SELECT") || sql_upper.contains("RETURNING");

    if returns_rows {
        let query_sql = interpolated.replace(" RETURNING sequence", "");
        if query_sql != interpolated {
            mysql_execute_query(&mut stream, &query_sql)?;
            return mysql_execute_query(&mut stream, "SELECT LAST_INSERT_ID() AS sequence");
        }
    }

    mysql_execute_query(&mut stream, &interpolated)
}

#[cfg(feature = "wasi-mysql")]
fn parse_mysql_url(url: &str) -> Result<MySqlConnParams, String> {
    let stripped = url
        .strip_prefix("mysql://")
        .ok_or_else(|| format!("Invalid MySQL URL prefix: {url}"))?;
    let without_fragment = stripped.split_once('#').map_or(stripped, |(main, _)| main);
    let without_query = without_fragment
        .split_once('?')
        .map_or(without_fragment, |(main, _)| main);
    let (authority, database) = without_query
        .split_once('/')
        .ok_or_else(|| "MySQL URL must include a database name".to_string())?;
    if database.is_empty() {
        return Err("MySQL URL must include a database name".to_string());
    }

    let (auth, host_port) = authority
        .rsplit_once('@')
        .map_or((None, authority), |(auth, host_port)| {
            (Some(auth), host_port)
        });

    let (user, password) = auth
        .map(|auth| {
            let (user, password) = auth.split_once(':').map_or((auth, ""), |parts| parts);
            (
                percent_decode(user),
                (!password.is_empty()).then(|| percent_decode(password)),
            )
        })
        .unwrap_or_else(|| ("root".to_string(), None));

    let (host, port) = if host_port.starts_with('[') {
        let end = host_port
            .find(']')
            .ok_or_else(|| "invalid bracketed IPv6 host in MySQL URL".to_string())?;
        let host = host_port[1..end].to_string();
        let rest = &host_port[end + 1..];
        let port = rest
            .strip_prefix(':')
            .map(|port| {
                port.parse::<u16>()
                    .map_err(|error| format!("invalid MySQL port `{port}`: {error}"))
            })
            .transpose()?
            .unwrap_or(3306);
        (host, port)
    } else {
        match host_port.rsplit_once(':') {
            Some((host, port)) => {
                let port = port
                    .parse::<u16>()
                    .map_err(|error| format!("invalid MySQL port `{port}`: {error}"))?;
                (host.to_string(), port)
            }
            None => (host_port.to_string(), 3306),
        }
    };

    if host.is_empty() {
        return Err("MySQL URL host is required".to_string());
    }

    Ok(MySqlConnParams {
        host,
        port,
        user,
        password,
        database: percent_decode(database),
    })
}

#[cfg(feature = "wasi-mysql")]
fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut idx = 0;

    while idx < bytes.len() {
        if bytes[idx] == b'%' && idx + 2 < bytes.len() {
            if let (Some(high), Some(low)) = (hex_value(bytes[idx + 1]), hex_value(bytes[idx + 2]))
            {
                output.push((high << 4) | low);
                idx += 3;
                continue;
            }
        }
        output.push(bytes[idx]);
        idx += 1;
    }

    String::from_utf8_lossy(&output).into_owned()
}

#[cfg(feature = "wasi-mysql")]
fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(feature = "wasi-mysql")]
fn mysql_authenticate(
    stream: &mut std::net::TcpStream,
    params: &MySqlConnParams,
) -> Result<(), String> {
    let (handshake_packet, _) = mysql_read_packet(stream)?;
    let handshake = mysql_parse_handshake(&handshake_packet)?;
    let mut active_auth_seed = handshake.auth_plugin_data.clone();
    let mut sequence_id = 1;
    let auth_response = mysql_auth_response(
        &handshake.auth_plugin_name,
        params.password.as_deref().unwrap_or(""),
        &active_auth_seed,
    )?;
    let response = mysql_handshake_response(params, &handshake.auth_plugin_name, &auth_response)?;
    mysql_write_packet(stream, &mut sequence_id, &response)?;

    loop {
        let (packet, packet_sequence_id) = mysql_read_packet(stream)?;
        if packet.is_empty() {
            return Err("empty MySQL authentication packet".to_string());
        }

        match packet[0] {
            0x00 => return Ok(()),
            0xff => return Err(mysql_error_packet(&packet)),
            0xfe if packet.len() > 1 => {
                let (plugin, seed_start) = mysql_read_null_string(&packet, 1)?;
                let seed = packet[seed_start..]
                    .iter()
                    .copied()
                    .filter(|byte| *byte != 0)
                    .collect::<Vec<_>>();
                active_auth_seed = seed;
                let auth_response = mysql_auth_response(
                    &plugin,
                    params.password.as_deref().unwrap_or(""),
                    &active_auth_seed,
                )?;
                let mut response_sequence_id = packet_sequence_id.wrapping_add(1);
                mysql_write_packet(stream, &mut response_sequence_id, &auth_response)?;
            }
            0x01 => {
                let status = packet.get(1).copied().unwrap_or_default();
                match status {
                    0x03 => continue,
                    0x04 => {
                        mysql_continue_caching_sha2_full_auth(
                            stream,
                            params.password.as_deref().unwrap_or(""),
                            &active_auth_seed,
                            packet_sequence_id,
                        )?;
                        continue;
                    }
                    _ => return Err(format!("unsupported MySQL auth-more status: {status}")),
                }
            }
            _ => return Err(format!("unexpected MySQL auth packet: 0x{:02x}", packet[0])),
        }
    }
}

#[cfg(feature = "wasi-mysql")]
fn mysql_parse_handshake(payload: &[u8]) -> Result<MySqlHandshake, String> {
    if payload.len() < 34 {
        return Err("invalid MySQL handshake packet".to_string());
    }

    let mut idx = 0;
    let _protocol = payload[idx];
    idx += 1;
    let (_, next_idx) = mysql_read_null_string(payload, idx)?;
    idx = next_idx;
    idx += 4;

    if idx + 8 > payload.len() {
        return Err("invalid MySQL handshake auth data part 1".to_string());
    }
    let mut auth_plugin_data = payload[idx..idx + 8].to_vec();
    idx += 8;
    idx += 1;

    if idx + 2 > payload.len() {
        return Err("invalid MySQL handshake capability flags".to_string());
    }
    let capability_lower = u16::from_le_bytes([payload[idx], payload[idx + 1]]) as u32;
    idx += 2;

    if idx >= payload.len() {
        return Err("invalid MySQL handshake payload".to_string());
    }
    idx += 1;
    idx += 2;

    if idx + 2 > payload.len() {
        return Err("invalid MySQL handshake upper capability flags".to_string());
    }
    let capability_upper = u16::from_le_bytes([payload[idx], payload[idx + 1]]) as u32;
    idx += 2;
    let capabilities = capability_lower | (capability_upper << 16);

    let auth_plugin_data_len = payload.get(idx).copied().unwrap_or(21) as usize;
    idx += 1;
    idx += 10;

    let part2_len = auth_plugin_data_len.saturating_sub(8).max(13);
    let end = payload.len().min(idx + part2_len);
    if idx < end {
        auth_plugin_data.extend(payload[idx..end].iter().copied().filter(|byte| *byte != 0));
        idx = end;
    }

    let auth_plugin_name = if capabilities & MYSQL_CLIENT_PLUGIN_AUTH != 0 && idx < payload.len() {
        mysql_read_null_string(payload, idx)
            .map(|(plugin, _)| plugin)
            .unwrap_or_else(|_| "mysql_native_password".to_string())
    } else {
        "mysql_native_password".to_string()
    };

    Ok(MySqlHandshake {
        auth_plugin_name,
        auth_plugin_data,
    })
}

#[cfg(feature = "wasi-mysql")]
fn mysql_handshake_response(
    params: &MySqlConnParams,
    auth_plugin_name: &str,
    auth_response: &[u8],
) -> Result<Vec<u8>, String> {
    let capabilities = MYSQL_CLIENT_LONG_PASSWORD
        | MYSQL_CLIENT_LONG_FLAG
        | MYSQL_CLIENT_CONNECT_WITH_DB
        | MYSQL_CLIENT_PROTOCOL_41
        | MYSQL_CLIENT_TRANSACTIONS
        | MYSQL_CLIENT_SECURE_CONNECTION
        | MYSQL_CLIENT_MULTI_RESULTS
        | MYSQL_CLIENT_PLUGIN_AUTH;

    if auth_response.len() > u8::MAX as usize {
        return Err("MySQL auth response is too large".to_string());
    }

    let mut payload = Vec::new();
    payload.extend_from_slice(&capabilities.to_le_bytes());
    payload.extend_from_slice(&(16_u32 * 1024 * 1024).to_le_bytes());
    payload.push(45);
    payload.extend_from_slice(&[0_u8; 23]);
    payload.extend_from_slice(params.user.as_bytes());
    payload.push(0);
    payload.push(auth_response.len() as u8);
    payload.extend_from_slice(auth_response);
    payload.extend_from_slice(params.database.as_bytes());
    payload.push(0);
    payload.extend_from_slice(auth_plugin_name.as_bytes());
    payload.push(0);

    Ok(payload)
}

#[cfg(feature = "wasi-mysql")]
fn mysql_auth_response(plugin: &str, password: &str, seed: &[u8]) -> Result<Vec<u8>, String> {
    if password.is_empty() {
        return Ok(Vec::new());
    }

    match plugin {
        "mysql_native_password" => Ok(mysql_native_password_token(password, seed).to_vec()),
        "caching_sha2_password" => Ok(mysql_caching_sha2_token(password, seed).to_vec()),
        "mysql_clear_password" => {
            let mut response = password.as_bytes().to_vec();
            response.push(0);
            Ok(response)
        }
        plugin => Err(format!("unsupported MySQL auth plugin `{plugin}`")),
    }
}

#[cfg(feature = "wasi-mysql")]
fn mysql_continue_caching_sha2_full_auth(
    stream: &mut std::net::TcpStream,
    password: &str,
    seed: &[u8],
    packet_sequence_id: u8,
) -> Result<(), String> {
    let mut request_sequence_id = packet_sequence_id.wrapping_add(1);
    mysql_write_packet(stream, &mut request_sequence_id, &[0x02])?;

    let (key_packet, key_packet_sequence_id) = mysql_read_packet(stream)?;
    let public_key = mysql_public_key_from_packet(&key_packet)?;
    let encrypted_password = mysql_encrypt_caching_sha2_password(password, seed, public_key)?;

    let mut auth_sequence_id = key_packet_sequence_id.wrapping_add(1);
    mysql_write_packet(stream, &mut auth_sequence_id, &encrypted_password)?;
    Ok(())
}

#[cfg(feature = "wasi-mysql")]
fn mysql_public_key_from_packet(packet: &[u8]) -> Result<&[u8], String> {
    if packet.is_empty() {
        return Err("empty MySQL caching_sha2_password public-key packet".to_string());
    }
    if packet[0] == 0xff {
        return Err(mysql_error_packet(packet));
    }

    let public_key = if packet[0] == 0x01 {
        &packet[1..]
    } else {
        packet
    };

    if public_key.is_empty() {
        return Err("MySQL caching_sha2_password public key is empty".to_string());
    }

    Ok(public_key)
}

#[cfg(feature = "wasi-mysql")]
fn mysql_encrypt_caching_sha2_password(
    password: &str,
    seed: &[u8],
    public_key_pem: &[u8],
) -> Result<Vec<u8>, String> {
    use rsa::{pkcs1::DecodeRsaPublicKey, pkcs8::DecodePublicKey, Oaep, RsaPublicKey};

    if seed.is_empty() {
        return Err("MySQL caching_sha2_password auth seed is empty".to_string());
    }

    let public_key_pem = std::str::from_utf8(public_key_pem)
        .map_err(|error| format!("invalid MySQL RSA public key UTF-8: {error}"))?;
    let public_key = RsaPublicKey::from_public_key_pem(public_key_pem)
        .or_else(|_| RsaPublicKey::from_pkcs1_pem(public_key_pem))
        .map_err(|error| format!("failed to parse MySQL RSA public key: {error}"))?;

    let mut scrambled_password = password.as_bytes().to_vec();
    scrambled_password.push(0);
    for (index, byte) in scrambled_password.iter_mut().enumerate() {
        *byte ^= seed[index % seed.len()];
    }

    let mut rng = MySqlAuthRng;
    public_key
        .encrypt(&mut rng, Oaep::new::<sha1::Sha1>(), &scrambled_password)
        .map_err(|error| format!("failed to encrypt MySQL caching_sha2_password response: {error}"))
}

#[cfg(feature = "wasi-mysql")]
struct MySqlAuthRng;

#[cfg(feature = "wasi-mysql")]
impl rsa::rand_core::RngCore for MySqlAuthRng {
    fn next_u32(&mut self) -> u32 {
        let mut bytes = [0_u8; 4];
        self.fill_bytes(&mut bytes);
        u32::from_le_bytes(bytes)
    }

    fn next_u64(&mut self) -> u64 {
        let mut bytes = [0_u8; 8];
        self.fill_bytes(&mut bytes);
        u64::from_le_bytes(bytes)
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        self.try_fill_bytes(dest)
            .unwrap_or_else(|error| panic!("failed to read random bytes for MySQL auth: {error}"));
    }

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rsa::rand_core::Error> {
        getrandom::fill(dest).map_err(|error| {
            rsa::rand_core::Error::new(std::io::Error::other(format!(
                "getrandom failed for MySQL auth: {error}",
            )))
        })
    }
}

#[cfg(feature = "wasi-mysql")]
impl rsa::rand_core::CryptoRng for MySqlAuthRng {}

#[cfg(feature = "wasi-mysql")]
fn mysql_native_password_token(password: &str, seed: &[u8]) -> [u8; 20] {
    let stage1 = mysql_sha1(password.as_bytes());
    let stage2 = mysql_sha1(&stage1);
    let mut challenge = Vec::with_capacity(seed.len() + stage2.len());
    challenge.extend_from_slice(seed);
    challenge.extend_from_slice(&stage2);
    let stage3 = mysql_sha1(&challenge);
    let mut token = [0_u8; 20];
    for index in 0..20 {
        token[index] = stage1[index] ^ stage3[index];
    }
    token
}

#[cfg(feature = "wasi-mysql")]
fn mysql_caching_sha2_token(password: &str, seed: &[u8]) -> [u8; 32] {
    let stage1 = mysql_sha256(password.as_bytes());
    let stage2 = mysql_sha256(&stage1);
    let stage3 = mysql_sha256(&stage2);
    let mut challenge = Vec::with_capacity(stage3.len() + seed.len());
    challenge.extend_from_slice(&stage3);
    challenge.extend_from_slice(seed);
    let stage4 = mysql_sha256(&challenge);
    let mut token = [0_u8; 32];
    for index in 0..32 {
        token[index] = stage1[index] ^ stage4[index];
    }
    token
}

#[cfg(feature = "wasi-mysql")]
fn mysql_sha1(bytes: &[u8]) -> [u8; 20] {
    use sha1::{Digest, Sha1};
    let mut hasher = Sha1::new();
    hasher.update(bytes);
    hasher.finalize().into()
}

#[cfg(feature = "wasi-mysql")]
fn mysql_sha256(bytes: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher.finalize().into()
}

#[cfg(feature = "wasi-mysql")]
fn mysql_execute_query(
    stream: &mut std::net::TcpStream,
    sql: &str,
) -> Result<Vec<serde_json::Value>, String> {
    let mut sequence_id = 0;
    let mut payload = Vec::with_capacity(sql.len() + 1);
    payload.push(0x03);
    payload.extend_from_slice(sql.as_bytes());
    mysql_write_packet(stream, &mut sequence_id, &payload)?;

    let (first_packet, _) = mysql_read_packet(stream)?;
    if first_packet.is_empty() {
        return Err("empty MySQL query response".to_string());
    }
    if first_packet[0] == 0xff {
        return Err(mysql_error_packet(&first_packet));
    }
    if mysql_is_ok_or_eof_packet(&first_packet) {
        return Ok(Vec::new());
    }

    let mut idx = 0;
    let column_count = mysql_read_lenenc_int(&first_packet, &mut idx)?
        .ok_or_else(|| "MySQL column count cannot be NULL".to_string())?
        as usize;
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let (packet, _) = mysql_read_packet(stream)?;
        if packet.first() == Some(&0xff) {
            return Err(mysql_error_packet(&packet));
        }
        columns.push(mysql_parse_column_definition(&packet)?);
    }

    let (terminator, _) = mysql_read_packet(stream)?;
    if terminator.first() == Some(&0xff) {
        return Err(mysql_error_packet(&terminator));
    }

    let mut rows = Vec::new();
    loop {
        let (packet, _) = mysql_read_packet(stream)?;
        if packet.first() == Some(&0xff) {
            return Err(mysql_error_packet(&packet));
        }
        if mysql_is_ok_or_eof_packet(&packet) {
            break;
        }
        rows.push(mysql_parse_text_row(&packet, &columns)?);
    }

    Ok(rows)
}

#[cfg(feature = "wasi-mysql")]
fn mysql_parse_column_definition(payload: &[u8]) -> Result<MySqlColumn, String> {
    let mut idx = 0;
    for _ in 0..4 {
        let _ = mysql_read_lenenc_bytes(payload, &mut idx)?;
    }
    let name = mysql_read_lenenc_bytes(payload, &mut idx)?
        .ok_or_else(|| "MySQL column name cannot be NULL".to_string())
        .and_then(|bytes| {
            std::str::from_utf8(bytes)
                .map(|value| value.to_string())
                .map_err(|error| format!("invalid MySQL column name UTF-8: {error}"))
        })?;
    let _ = mysql_read_lenenc_bytes(payload, &mut idx)?;

    if idx + 13 > payload.len() {
        return Err("invalid MySQL column definition packet".to_string());
    }
    idx += 1;
    idx += 2;
    idx += 4;
    let column_type = payload[idx];

    Ok(MySqlColumn { name, column_type })
}

#[cfg(feature = "wasi-mysql")]
fn mysql_parse_text_row(
    payload: &[u8],
    columns: &[MySqlColumn],
) -> Result<serde_json::Value, String> {
    let mut idx = 0;
    let mut row = serde_json::Map::new();

    for column in columns {
        let value = mysql_read_lenenc_bytes(payload, &mut idx)?;
        let json_value = match value {
            None => serde_json::Value::Null,
            Some(bytes) => mysql_text_value_to_json(column, bytes),
        };
        row.insert(column.name.clone(), json_value);
    }

    Ok(serde_json::Value::Object(row))
}

#[cfg(feature = "wasi-mysql")]
fn mysql_text_value_to_json(column: &MySqlColumn, bytes: &[u8]) -> serde_json::Value {
    let text = String::from_utf8_lossy(bytes);
    match column.column_type {
        0x01 | 0x02 | 0x03 | 0x08 | 0x09 | 0x0d => text
            .parse::<i64>()
            .map(|value| serde_json::Value::Number(value.into()))
            .unwrap_or_else(|_| serde_json::Value::String(text.into_owned())),
        0x04 | 0x05 | 0xf6 => text
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(serde_json::Value::Number)
            .unwrap_or_else(|| serde_json::Value::String(text.into_owned())),
        0xf5 => serde_json::from_str::<serde_json::Value>(&text)
            .unwrap_or_else(|_| serde_json::Value::String(text.into_owned())),
        _ => serde_json::Value::String(text.into_owned()),
    }
}

#[cfg(feature = "wasi-mysql")]
fn mysql_read_packet(stream: &mut std::net::TcpStream) -> Result<(Vec<u8>, u8), String> {
    use std::io::Read;

    let mut payload = Vec::new();

    loop {
        let mut header = [0_u8; 4];
        stream
            .read_exact(&mut header)
            .map_err(|error| format!("failed to read MySQL packet header: {error}"))?;
        let len = (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
        let sequence_id = header[3];
        let mut chunk = vec![0_u8; len];
        stream
            .read_exact(&mut chunk)
            .map_err(|error| format!("failed to read MySQL packet payload: {error}"))?;
        payload.extend_from_slice(&chunk);
        if len < 0x00ff_ffff {
            return Ok((payload, sequence_id));
        }
    }
}

#[cfg(feature = "wasi-mysql")]
fn mysql_write_packet(
    stream: &mut std::net::TcpStream,
    sequence_id: &mut u8,
    payload: &[u8],
) -> Result<(), String> {
    use std::io::Write;

    if payload.len() > 0x00ff_ffff {
        return Err("MySQL packet payload exceeds 16MiB".to_string());
    }

    let len = payload.len();
    let header = [
        (len & 0xff) as u8,
        ((len >> 8) & 0xff) as u8,
        ((len >> 16) & 0xff) as u8,
        *sequence_id,
    ];
    stream
        .write_all(&header)
        .map_err(|error| format!("failed to write MySQL packet header: {error}"))?;
    stream
        .write_all(payload)
        .map_err(|error| format!("failed to write MySQL packet payload: {error}"))?;
    stream
        .flush()
        .map_err(|error| format!("failed to flush MySQL packet: {error}"))?;
    *sequence_id = sequence_id.wrapping_add(1);
    Ok(())
}

#[cfg(feature = "wasi-mysql")]
fn mysql_read_null_string(payload: &[u8], start: usize) -> Result<(String, usize), String> {
    if start > payload.len() {
        return Err("invalid MySQL null string offset".to_string());
    }
    let mut end = start;
    while end < payload.len() && payload[end] != 0 {
        end += 1;
    }
    let value = std::str::from_utf8(&payload[start..end])
        .map_err(|error| format!("invalid MySQL UTF-8 string: {error}"))?
        .to_string();
    Ok((value, (end + 1).min(payload.len())))
}

#[cfg(feature = "wasi-mysql")]
fn mysql_read_lenenc_int(payload: &[u8], idx: &mut usize) -> Result<Option<u64>, String> {
    if *idx >= payload.len() {
        return Err("unexpected end of MySQL length-encoded integer".to_string());
    }
    let first = payload[*idx];
    *idx += 1;
    match first {
        0xfb => Ok(None),
        0xfc => {
            if *idx + 2 > payload.len() {
                return Err("invalid MySQL 2-byte length-encoded integer".to_string());
            }
            let value = u16::from_le_bytes([payload[*idx], payload[*idx + 1]]) as u64;
            *idx += 2;
            Ok(Some(value))
        }
        0xfd => {
            if *idx + 3 > payload.len() {
                return Err("invalid MySQL 3-byte length-encoded integer".to_string());
            }
            let value = (payload[*idx] as u64)
                | ((payload[*idx + 1] as u64) << 8)
                | ((payload[*idx + 2] as u64) << 16);
            *idx += 3;
            Ok(Some(value))
        }
        0xfe => {
            if *idx + 8 > payload.len() {
                return Err("invalid MySQL 8-byte length-encoded integer".to_string());
            }
            let value = u64::from_le_bytes([
                payload[*idx],
                payload[*idx + 1],
                payload[*idx + 2],
                payload[*idx + 3],
                payload[*idx + 4],
                payload[*idx + 5],
                payload[*idx + 6],
                payload[*idx + 7],
            ]);
            *idx += 8;
            Ok(Some(value))
        }
        value => Ok(Some(value as u64)),
    }
}

#[cfg(feature = "wasi-mysql")]
fn mysql_read_lenenc_bytes<'a>(
    payload: &'a [u8],
    idx: &mut usize,
) -> Result<Option<&'a [u8]>, String> {
    let Some(len) = mysql_read_lenenc_int(payload, idx)? else {
        return Ok(None);
    };
    let len = len as usize;
    if *idx + len > payload.len() {
        return Err("invalid MySQL length-encoded string length".to_string());
    }
    let bytes = &payload[*idx..*idx + len];
    *idx += len;
    Ok(Some(bytes))
}

#[cfg(feature = "wasi-mysql")]
fn mysql_is_ok_or_eof_packet(packet: &[u8]) -> bool {
    packet.first() == Some(&0x00) || (packet.first() == Some(&0xfe) && packet.len() < 9)
}

#[cfg(feature = "wasi-mysql")]
fn mysql_error_packet(packet: &[u8]) -> String {
    if packet.len() < 3 || packet[0] != 0xff {
        return format!("MySQL error packet: {packet:?}");
    }
    let code = u16::from_le_bytes([packet[1], packet[2]]);
    let message_start = if packet.get(3) == Some(&b'#') && packet.len() >= 9 {
        9
    } else {
        3
    };
    let message = String::from_utf8_lossy(packet.get(message_start..).unwrap_or_default());
    format!("MySQL error {code}: {message}")
}

#[cfg(feature = "wasi-mysql")]
fn interpolate_mysql_query(sql: &str, params: &[serde_json::Value]) -> Result<String, String> {
    let mut output = String::with_capacity(sql.len() + params.len() * 8);
    let mut params_iter = params.iter();

    for ch in sql.chars() {
        if ch == '?' {
            let value = params_iter
                .next()
                .ok_or_else(|| "not enough MySQL query parameters".to_string())?;
            output.push_str(&format_mysql_value(value)?);
        } else {
            output.push(ch);
        }
    }

    if params_iter.next().is_some() {
        return Err("too many MySQL query parameters".to_string());
    }

    Ok(output)
}

#[cfg(feature = "wasi-mysql")]
fn format_mysql_value(value: &serde_json::Value) -> Result<String, String> {
    match value {
        serde_json::Value::Null => Ok("NULL".to_string()),
        serde_json::Value::Bool(value) => Ok(if *value { "TRUE" } else { "FALSE" }.to_string()),
        serde_json::Value::Number(value) => Ok(value.to_string()),
        serde_json::Value::String(value) => Ok(format!("'{}'", escape_mysql_string(value))),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            let value = serde_json::to_string(value).map_err(|error| error.to_string())?;
            Ok(format!("'{}'", escape_mysql_string(&value)))
        }
    }
}

#[cfg(feature = "wasi-mysql")]
fn escape_mysql_string(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\0', "\\0")
        .replace('\'', "''")
}

// -------------------------------------------------------------------------
// Spin SQLite adapter
// -------------------------------------------------------------------------
#[cfg(feature = "spin-sqlite")]
pub async fn execute_spin_sqlite(
    sql: &str,
    params: Vec<serde_json::Value>,
) -> Result<Vec<serde_json::Value>, String> {
    let conn = spin_sdk::sqlite::Connection::open_default()
        .await
        .map_err(|e| format!("SQLite open connection error: {:?}", e))?;

    let spin_params: Vec<spin_sdk::sqlite::Value> = params
        .into_iter()
        .map(|v| match v {
            serde_json::Value::Null => spin_sdk::sqlite::Value::Null,
            serde_json::Value::Bool(b) => spin_sdk::sqlite::Value::Integer(if b { 1 } else { 0 }),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    spin_sdk::sqlite::Value::Integer(i)
                } else if let Some(f) = n.as_f64() {
                    spin_sdk::sqlite::Value::Real(f)
                } else {
                    spin_sdk::sqlite::Value::Null
                }
            }
            serde_json::Value::String(s) => spin_sdk::sqlite::Value::Text(s),
            other => spin_sdk::sqlite::Value::Text(other.to_string()),
        })
        .collect();

    let rowset = conn
        .execute(sql, spin_params)
        .await
        .map_err(|e| format!("SQLite query error: {:?}", e))?;

    let columns = rowset.columns().to_vec();
    let rows_list = rowset
        .collect()
        .await
        .map_err(|e| format!("SQLite rows collection error: {:?}", e))?;

    let mut rows = Vec::new();
    for r in rows_list {
        let mut row_obj = serde_json::Map::new();
        for (i, col_name) in columns.iter().enumerate() {
            let val = match &r.values[i] {
                spin_sdk::sqlite::Value::Null => serde_json::Value::Null,
                spin_sdk::sqlite::Value::Integer(i) => {
                    serde_json::Value::Number(serde_json::Number::from(*i))
                }
                spin_sdk::sqlite::Value::Real(f) => {
                    if let Some(num) = serde_json::Number::from_f64(*f) {
                        serde_json::Value::Number(num)
                    } else {
                        serde_json::Value::Null
                    }
                }
                spin_sdk::sqlite::Value::Text(s) => serde_json::Value::String(s.clone()),
                spin_sdk::sqlite::Value::Blob(b) => {
                    serde_json::Value::String(String::from_utf8_lossy(b).into_owned())
                }
            };
            row_obj.insert(col_name.clone(), val);
        }
        rows.push(serde_json::Value::Object(row_obj));
    }

    Ok(rows)
}

// -------------------------------------------------------------------------
// Spin Postgres adapter
// -------------------------------------------------------------------------
#[cfg(feature = "spin-postgres")]
pub async fn execute_spin_pg(
    db_url: &str,
    sql: &str,
    params: Vec<serde_json::Value>,
) -> Result<Vec<serde_json::Value>, String> {
    use spin_sdk::pg::{
        Connection as SpinPgConn, DbValue as SpinPgDbVal, ParameterValue as SpinPgParam,
    };

    // Check if query is SELECT or modifying command that returns rows (e.g. contains RETURNING)
    let sql_upper = sql.trim_start().to_ascii_uppercase();
    let is_select = sql_upper.starts_with("SELECT") || sql_upper.contains("RETURNING");

    let pg_params: Vec<SpinPgParam> = params
        .into_iter()
        .map(|v| match v {
            serde_json::Value::Null => SpinPgParam::DbNull,
            serde_json::Value::Bool(b) => SpinPgParam::Boolean(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    SpinPgParam::Int64(i)
                } else if let Some(f) = n.as_f64() {
                    SpinPgParam::Floating64(f)
                } else {
                    SpinPgParam::DbNull
                }
            }
            serde_json::Value::String(s) => SpinPgParam::Str(s),
            other => SpinPgParam::Str(other.to_string()),
        })
        .collect();

    let conn = SpinPgConn::open(db_url)
        .await
        .map_err(|e| format!("Pg connection error: {:?}", e))?;

    if is_select {
        let mut rowset = conn
            .query(sql, pg_params)
            .await
            .map_err(|e| format!("Pg query error: {:?}", e))?;
        let col_names: Vec<String> = rowset.columns().iter().map(|c| c.name.clone()).collect();

        let mut rows = Vec::new();
        let rows_reader = rowset.rows();
        while let Some(row) = rows_reader.next().await {
            let mut row_obj = serde_json::Map::new();
            for (i, val) in row.iter().enumerate() {
                let col_name = col_names
                    .get(i)
                    .cloned()
                    .unwrap_or_else(|| format!("col_{}", i));
                let json_val = match val {
                    SpinPgDbVal::DbNull => serde_json::Value::Null,
                    SpinPgDbVal::Boolean(b) => serde_json::Value::Bool(*b),
                    SpinPgDbVal::Int8(i) => serde_json::Value::Number((*i as i32).into()),
                    SpinPgDbVal::Int16(i) => serde_json::Value::Number((*i as i32).into()),
                    SpinPgDbVal::Int32(i) => serde_json::Value::Number((*i).into()),
                    SpinPgDbVal::Int64(i) => serde_json::Value::Number((*i).into()),
                    SpinPgDbVal::Floating32(f) => {
                        if let Some(num) = serde_json::Number::from_f64(*f as f64) {
                            serde_json::Value::Number(num)
                        } else {
                            serde_json::Value::Null
                        }
                    }
                    SpinPgDbVal::Floating64(f) => {
                        if let Some(num) = serde_json::Number::from_f64(*f) {
                            serde_json::Value::Number(num)
                        } else {
                            serde_json::Value::Null
                        }
                    }
                    SpinPgDbVal::Str(s) => {
                        if let Ok(jv) = serde_json::from_str::<serde_json::Value>(s) {
                            jv
                        } else {
                            serde_json::Value::String(s.clone())
                        }
                    }
                    SpinPgDbVal::Binary(b) => {
                        serde_json::Value::String(String::from_utf8_lossy(b).into_owned())
                    }
                    SpinPgDbVal::Jsonb(j) => {
                        if let Ok(jv) = serde_json::from_slice::<serde_json::Value>(j) {
                            jv
                        } else {
                            serde_json::Value::String(String::from_utf8_lossy(j).into_owned())
                        }
                    }
                    SpinPgDbVal::Unsupported(b) => {
                        if let Ok(jv) = serde_json::from_slice::<serde_json::Value>(b) {
                            jv
                        } else {
                            serde_json::Value::String(String::from_utf8_lossy(b).into_owned())
                        }
                    }
                    other => serde_json::Value::String(format!("{:?}", other)),
                };
                row_obj.insert(col_name, json_val);
            }
            rows.push(serde_json::Value::Object(row_obj));
        }
        Ok(rows)
    } else {
        conn.execute(sql, pg_params)
            .await
            .map_err(|e| format!("Pg execute error: {:?}", e))?;
        Ok(Vec::new())
    }
}

// -------------------------------------------------------------------------
// Spin MySQL adapter
// -------------------------------------------------------------------------
#[cfg(feature = "spin-mysql")]
pub async fn execute_spin_mysql(
    db_url: &str,
    sql: &str,
    params: Vec<serde_json::Value>,
) -> Result<Vec<serde_json::Value>, String> {
    use spin_sdk::mysql::{Connection as SpinMysqlConn, ParameterValue as SpinMysqlParam};

    let mysql_params: Vec<SpinMysqlParam> = params
        .into_iter()
        .map(|value| match value {
            serde_json::Value::Null => SpinMysqlParam::DbNull,
            serde_json::Value::Bool(value) => SpinMysqlParam::Int8(if value { 1 } else { 0 }),
            serde_json::Value::Number(value) => {
                if let Some(value) = value.as_i64() {
                    SpinMysqlParam::Int64(value)
                } else if let Some(value) = value.as_u64() {
                    SpinMysqlParam::Uint64(value)
                } else if let Some(value) = value.as_f64() {
                    SpinMysqlParam::Floating64(value)
                } else {
                    SpinMysqlParam::DbNull
                }
            }
            serde_json::Value::String(value) => SpinMysqlParam::Str(value),
            value => SpinMysqlParam::Str(value.to_string()),
        })
        .collect();

    let sql_upper = sql.trim_start().to_ascii_uppercase();
    let returns_rows = sql_upper.starts_with("SELECT") || sql_upper.contains("RETURNING");
    let conn = SpinMysqlConn::open(db_url)
        .map_err(|error| format!("MySQL connection error: {error:?}"))?;

    if returns_rows {
        let query_sql = sql.replace(" RETURNING sequence", "");
        if query_sql != sql {
            conn.execute(&query_sql, &mysql_params)
                .map_err(|error| format!("MySQL execute error: {error:?}"))?;
            return spin_mysql_query_rows(&conn, "SELECT LAST_INSERT_ID() AS sequence", &[]);
        }

        let query_sql = spin_mysql_select_sql(sql);
        spin_mysql_query_rows(&conn, &query_sql, &mysql_params)
    } else {
        conn.execute(sql, &mysql_params)
            .map_err(|error| format!("MySQL execute error: {error:?}"))?;
        Ok(Vec::new())
    }
}

#[cfg(feature = "spin-mysql")]
fn spin_mysql_select_sql(sql: &str) -> String {
    // Spin's MySQL SDK cannot currently convert MySQL JSON columns directly;
    // cast event JSON columns to strings before RowSet decoding.
    if sql.contains(" AS payload") {
        return sql.to_string();
    }

    sql.replace(
        "payload, metadata",
        "CAST(payload AS CHAR(10000) CHARACTER SET utf8mb4) AS payload, CAST(metadata AS CHAR(10000) CHARACTER SET utf8mb4) AS metadata",
    )
    .replace(
        "payload, recorded_at_ms",
        "CAST(payload AS CHAR(10000) CHARACTER SET utf8mb4) AS payload, recorded_at_ms",
    )
}

#[cfg(feature = "spin-mysql")]
fn spin_mysql_query_rows(
    conn: &spin_sdk::mysql::Connection,
    sql: &str,
    params: &[spin_sdk::mysql::ParameterValue],
) -> Result<Vec<serde_json::Value>, String> {
    let rowset = conn
        .query(sql, params)
        .map_err(|error| format!("MySQL query error: {error:?}"))?;
    let col_names: Vec<String> = rowset.columns.iter().map(|col| col.name.clone()).collect();
    let mut rows = Vec::with_capacity(rowset.rows.len());

    for row in &rowset.rows {
        let mut row_obj = serde_json::Map::new();
        for (index, value) in row.iter().enumerate() {
            let col_name = col_names
                .get(index)
                .cloned()
                .unwrap_or_else(|| format!("col_{index}"));
            row_obj.insert(col_name, spin_mysql_value_to_json(value));
        }
        rows.push(serde_json::Value::Object(row_obj));
    }

    Ok(rows)
}

#[cfg(feature = "spin-mysql")]
fn spin_mysql_value_to_json(value: &spin_sdk::mysql::DbValue) -> serde_json::Value {
    use spin_sdk::mysql::DbValue as SpinMysqlDbVal;

    match value {
        SpinMysqlDbVal::DbNull => serde_json::Value::Null,
        SpinMysqlDbVal::Int8(value) => serde_json::Value::Number((*value as i32).into()),
        SpinMysqlDbVal::Int16(value) => serde_json::Value::Number((*value as i32).into()),
        SpinMysqlDbVal::Int32(value) => serde_json::Value::Number((*value).into()),
        SpinMysqlDbVal::Int64(value) => serde_json::Value::Number((*value).into()),
        SpinMysqlDbVal::Uint8(value) => serde_json::Value::Number((*value as u32).into()),
        SpinMysqlDbVal::Uint16(value) => serde_json::Value::Number((*value as u32).into()),
        SpinMysqlDbVal::Uint32(value) => serde_json::Value::Number((*value).into()),
        SpinMysqlDbVal::Uint64(value) => serde_json::Value::Number((*value).into()),
        SpinMysqlDbVal::Floating32(value) => serde_json::Number::from_f64(*value as f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        SpinMysqlDbVal::Floating64(value) => serde_json::Number::from_f64(*value)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        SpinMysqlDbVal::Str(value) => serde_json::from_str::<serde_json::Value>(value)
            .unwrap_or_else(|_| serde_json::Value::String(value.clone())),
        SpinMysqlDbVal::Binary(value) => {
            serde_json::Value::String(String::from_utf8_lossy(value).into_owned())
        }
        other => serde_json::Value::String(format!("{other:?}")),
    }
}

// =========================================================================
// FLAT-FILE FALLBACK STORAGE IMPLEMENTATIONS
// =========================================================================

#[cfg(feature = "json-file")]
static FILE_LOCKS: std::sync::OnceLock<
    std::sync::Mutex<
        std::collections::HashMap<std::path::PathBuf, std::sync::Arc<std::sync::Mutex<()>>>,
    >,
> = std::sync::OnceLock::new();

#[cfg(feature = "json-file")]
fn get_file_lock(
    path: &std::path::Path,
) -> Result<std::sync::Arc<std::sync::Mutex<()>>, crate::error::EventStoreError> {
    let map_lock =
        FILE_LOCKS.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
    let mut map = map_lock
        .lock()
        .map_err(|_| crate::error::EventStoreError::Poisoned)?;
    let canonical = if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
        if let Ok(canon_parent) = parent.canonicalize() {
            if let Some(filename) = path.file_name() {
                canon_parent.join(filename)
            } else {
                canon_parent
            }
        } else {
            path.to_path_buf()
        }
    } else {
        path.to_path_buf()
    };
    Ok(map
        .entry(canonical)
        .or_insert_with(|| std::sync::Arc::new(std::sync::Mutex::new(())))
        .clone())
}

#[cfg(feature = "json-file")]
fn write_atomic(path: &std::path::Path, content: &str) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let tmp_name = format!(
        "{}.tmp.{}",
        path.file_name().unwrap_or_default().to_string_lossy(),
        nanos
    );
    let tmp_path = path.with_file_name(tmp_name);
    std::fs::write(&tmp_path, content)?;
    if let Err(e) = std::fs::rename(&tmp_path, path) {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(e);
    }
    Ok(())
}

#[cfg(feature = "json-file")]
/// A JSON file-backed event store.
///
/// > [!WARNING]
/// > This adapter is intended for **single-process development and testing purposes only**.
/// > It is not designed or certified for production use-cases where high concurrency,
/// > multi-process access, or strict reliability guarantees are required.
pub struct JsonFileEventStore<A> {
    events_path: std::path::PathBuf,
    _marker: std::marker::PhantomData<fn() -> A>,
}

#[cfg(feature = "json-file")]
impl<A> Clone for JsonFileEventStore<A> {
    fn clone(&self) -> Self {
        Self {
            events_path: self.events_path.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}

#[cfg(feature = "json-file")]
impl<A> std::fmt::Debug for JsonFileEventStore<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonFileEventStore")
            .field("events_path", &self.events_path)
            .finish()
    }
}

#[cfg(feature = "json-file")]
impl<A> JsonFileEventStore<A> {
    /// Creates a new JSON file-backed event store.
    pub fn new(events_path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            events_path: events_path.into(),
            _marker: std::marker::PhantomData,
        }
    }
}

#[cfg(feature = "json-file")]
impl<A> crate::event_store::EventStore<A> for JsonFileEventStore<A>
where
    A: crate::aggregate::Aggregate + 'static,
    A::Event: serde::Serialize + serde::de::DeserializeOwned + Clone,
    A::Id: serde::Serialize + serde::de::DeserializeOwned + Clone + PartialEq,
{
    type Error = crate::error::EventStoreError;

    fn load(
        &self,
        aggregate_id: &A::Id,
    ) -> Result<crate::event_store::EventStream<A>, Self::Error> {
        let lock = get_file_lock(&self.events_path)?;
        let _guard = lock
            .lock()
            .map_err(|_| crate::error::EventStoreError::Poisoned)?;

        if !self.events_path.exists() {
            return Ok(Vec::new());
        }

        let content = std::fs::read_to_string(&self.events_path)
            .map_err(|e| crate::error::EventStoreError::Backend(e.to_string()))?;

        let values: Vec<serde_json::Value> = serde_json::from_str(&content)
            .map_err(|e| crate::error::EventStoreError::Deserialization(e.to_string()))?;

        let mut envelopes = Vec::new();
        for val in values {
            if let Some(agg_type_val) = val.get("aggregate_type") {
                if let Some(agg_type_str) = agg_type_val.as_str() {
                    if agg_type_str == A::aggregate_type() {
                        let id_val = val.get("aggregate_id").ok_or_else(|| {
                            crate::error::EventStoreError::Deserialization(
                                "missing aggregate_id".to_string(),
                            )
                        })?;
                        let id = serde_json::from_value::<A::Id>(id_val.clone()).map_err(|e| {
                            crate::error::EventStoreError::Deserialization(format!(
                                "failed to deserialize aggregate_id: {e}"
                            ))
                        })?;
                        if &id == aggregate_id {
                            let envelope = serde_json::from_value::<
                                crate::event::EventEnvelope<A::Event, A::Id>,
                            >(val)
                            .map_err(|e| {
                                crate::error::EventStoreError::Deserialization(format!(
                                    "failed to deserialize event envelope: {e}"
                                ))
                            })?;
                            envelopes.push(envelope);
                        }
                    }
                }
            }
        }

        envelopes.sort_by_key(|e| e.revision);
        Ok(envelopes)
    }

    fn append(
        &self,
        aggregate_id: &A::Id,
        expected_revision: crate::event::ExpectedRevision,
        events: Vec<crate::event::NewEvent<A::Event>>,
    ) -> Result<crate::event_store::EventStream<A>, Self::Error> {
        let lock = get_file_lock(&self.events_path)?;
        let _guard = lock
            .lock()
            .map_err(|_| crate::error::EventStoreError::Poisoned)?;

        if let Some(parent) = self.events_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }

        let content = if self.events_path.exists() {
            std::fs::read_to_string(&self.events_path)
                .map_err(|e| crate::error::EventStoreError::Backend(e.to_string()))?
        } else {
            "[]".to_string()
        };

        let mut all_values: Vec<serde_json::Value> = serde_json::from_str(&content)
            .map_err(|e| crate::error::EventStoreError::Deserialization(e.to_string()))?;

        let mut current_revision = 0u64;
        let mut max_sequence = 0u64;

        for val in &all_values {
            if let Some(seq) = val.get("sequence").and_then(|s| s.as_u64()) {
                if seq > max_sequence {
                    max_sequence = seq;
                }
            }

            if let Some(agg_type_val) = val.get("aggregate_type") {
                if let Some(agg_type_str) = agg_type_val.as_str() {
                    if agg_type_str == A::aggregate_type() {
                        let id_val = val.get("aggregate_id").ok_or_else(|| {
                            crate::error::EventStoreError::Deserialization(
                                "missing aggregate_id".to_string(),
                            )
                        })?;
                        let id = serde_json::from_value::<A::Id>(id_val.clone()).map_err(|e| {
                            crate::error::EventStoreError::Deserialization(format!(
                                "failed to deserialize aggregate_id: {e}"
                            ))
                        })?;
                        if &id == aggregate_id {
                            let rev = val
                                .get("revision")
                                .ok_or_else(|| {
                                    crate::error::EventStoreError::Deserialization(
                                        "missing revision".to_string(),
                                    )
                                })?
                                .as_u64()
                                .ok_or_else(|| {
                                    crate::error::EventStoreError::Deserialization(
                                        "revision is not a valid u64".to_string(),
                                    )
                                })?;
                            if rev > current_revision {
                                current_revision = rev;
                            }
                        }
                    }
                }
            }
        }

        match expected_revision {
            crate::event::ExpectedRevision::Any => {}
            crate::event::ExpectedRevision::NoStream if current_revision == 0 => {}
            crate::event::ExpectedRevision::NoStream => {
                return Err(crate::error::EventStoreError::Concurrency(
                    crate::error::ConcurrencyError::StreamAlreadyExists,
                ));
            }
            crate::event::ExpectedRevision::Exact(expected) if expected == current_revision => {}
            crate::event::ExpectedRevision::Exact(_) => {
                return Err(crate::error::EventStoreError::Concurrency(
                    crate::error::ConcurrencyError::WrongExpectedRevision {
                        expected: expected_revision,
                        actual: current_revision,
                    },
                ));
            }
        }

        if events.is_empty() {
            return Ok(Vec::new());
        }

        let mut envelopes = Vec::new();
        let now = std::time::SystemTime::now();

        for (i, event) in events.into_iter().enumerate() {
            let revision = current_revision + i as u64 + 1;
            let sequence = max_sequence + i as u64 + 1;
            let event_id = crate::event::EventId::new();

            let envelope = crate::event::EventEnvelope::new(
                event_id,
                aggregate_id.clone(),
                A::aggregate_type().to_string(),
                revision,
                Some(sequence),
                event.event_type,
                event.event_version,
                event.payload,
                event.metadata,
                now,
            );

            let val = serde_json::to_value(&envelope)
                .map_err(|e| crate::error::EventStoreError::Serialization(e.to_string()))?;

            all_values.push(val);
            envelopes.push(envelope);
        }

        let new_content = serde_json::to_string(&all_values)
            .map_err(|e| crate::error::EventStoreError::Serialization(e.to_string()))?;
        write_atomic(&self.events_path, &new_content)
            .map_err(|e| crate::error::EventStoreError::Backend(e.to_string()))?;

        Ok(envelopes)
    }

    fn load_global_after(
        &self,
        sequence: Option<u64>,
    ) -> Result<crate::event_store::EventStream<A>, Self::Error> {
        let lock = get_file_lock(&self.events_path)?;
        let _guard = lock
            .lock()
            .map_err(|_| crate::error::EventStoreError::Poisoned)?;

        if !self.events_path.exists() {
            return Ok(Vec::new());
        }

        let content = std::fs::read_to_string(&self.events_path)
            .map_err(|e| crate::error::EventStoreError::Backend(e.to_string()))?;

        let values: Vec<serde_json::Value> = serde_json::from_str(&content)
            .map_err(|e| crate::error::EventStoreError::Deserialization(e.to_string()))?;

        let mut envelopes = Vec::new();
        let seq_num = sequence.unwrap_or(0);
        for val in values {
            if let Some(agg_type_val) = val.get("aggregate_type") {
                if let Some(agg_type_str) = agg_type_val.as_str() {
                    if agg_type_str == A::aggregate_type() {
                        let envelope = serde_json::from_value::<
                            crate::event::EventEnvelope<A::Event, A::Id>,
                        >(val)
                        .map_err(|e| {
                            crate::error::EventStoreError::Deserialization(format!(
                                "failed to deserialize event envelope: {e}"
                            ))
                        })?;
                        if envelope.sequence.unwrap_or(0) > seq_num {
                            envelopes.push(envelope);
                        }
                    }
                }
            }
        }

        envelopes.sort_by_key(|e| e.sequence);
        Ok(envelopes)
    }
}

#[cfg(all(feature = "json-file", feature = "async"))]
#[async_trait::async_trait]
impl<A> crate::async_api::AsyncEventStore<A> for JsonFileEventStore<A>
where
    A: crate::aggregate::Aggregate + Send + Sync + 'static,
    A::Event: serde::Serialize + serde::de::DeserializeOwned + Clone + Send + Sync,
    A::Id: serde::Serialize + serde::de::DeserializeOwned + Clone + PartialEq + Send + Sync,
{
    type Error = crate::error::EventStoreError;

    async fn load(
        &self,
        aggregate_id: &A::Id,
    ) -> Result<crate::event_store::EventStream<A>, Self::Error> {
        let this = self.clone();
        let agg_id = aggregate_id.clone();
        tokio::task::spawn_blocking(move || crate::event_store::EventStore::load(&this, &agg_id))
            .await
            .map_err(|e| crate::error::EventStoreError::Backend(e.to_string()))?
    }

    async fn append(
        &self,
        aggregate_id: &A::Id,
        expected_revision: crate::event::ExpectedRevision,
        events: Vec<crate::event::NewEvent<A::Event>>,
    ) -> Result<crate::event_store::EventStream<A>, Self::Error> {
        let this = self.clone();
        let agg_id = aggregate_id.clone();
        tokio::task::spawn_blocking(move || {
            crate::event_store::EventStore::append(&this, &agg_id, expected_revision, events)
        })
        .await
        .map_err(|e| crate::error::EventStoreError::Backend(e.to_string()))?
    }

    async fn load_global_after(
        &self,
        sequence: Option<u64>,
    ) -> Result<crate::event_store::EventStream<A>, Self::Error> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            crate::event_store::EventStore::load_global_after(&this, sequence)
        })
        .await
        .map_err(|e| crate::error::EventStoreError::Backend(e.to_string()))?
    }
}

#[cfg(feature = "json-file")]
#[derive(Clone, Debug)]
/// A JSON file-backed checkpoint store.
///
/// > [!WARNING]
/// > This adapter is intended for **single-process development and testing purposes only**.
/// > It is not designed or certified for production use-cases where high concurrency,
/// > multi-process access, or strict reliability guarantees are required.
pub struct JsonFileCheckpointStore {
    checkpoints_path: std::path::PathBuf,
}

#[cfg(feature = "json-file")]
impl JsonFileCheckpointStore {
    /// Creates a new JSON file-backed checkpoint store.
    pub fn new(checkpoints_path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            checkpoints_path: checkpoints_path.into(),
        }
    }
}

#[cfg(feature = "json-file")]
impl crate::projection::CheckpointStore for JsonFileCheckpointStore {
    type Error = crate::error::EventStoreError;

    fn load_checkpoint(&self, projection_name: &str) -> Result<Option<u64>, Self::Error> {
        let lock = get_file_lock(&self.checkpoints_path)?;
        let _guard = lock
            .lock()
            .map_err(|_| crate::error::EventStoreError::Poisoned)?;

        if !self.checkpoints_path.exists() {
            return Ok(None);
        }

        let content = std::fs::read_to_string(&self.checkpoints_path)
            .map_err(|e| crate::error::EventStoreError::Backend(e.to_string()))?;

        let map: std::collections::HashMap<String, u64> = serde_json::from_str(&content)
            .map_err(|e| crate::error::EventStoreError::Deserialization(e.to_string()))?;

        Ok(map.get(projection_name).copied())
    }

    fn save_checkpoint(&self, projection_name: &str, sequence: u64) -> Result<(), Self::Error> {
        let lock = get_file_lock(&self.checkpoints_path)?;
        let _guard = lock
            .lock()
            .map_err(|_| crate::error::EventStoreError::Poisoned)?;

        if let Some(parent) = self.checkpoints_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }

        let content = if self.checkpoints_path.exists() {
            std::fs::read_to_string(&self.checkpoints_path)
                .map_err(|e| crate::error::EventStoreError::Backend(e.to_string()))?
        } else {
            "{}".to_string()
        };

        let mut map: std::collections::HashMap<String, u64> = serde_json::from_str(&content)
            .map_err(|e| crate::error::EventStoreError::Deserialization(e.to_string()))?;

        map.insert(projection_name.to_string(), sequence);

        let new_content = serde_json::to_string(&map)
            .map_err(|e| crate::error::EventStoreError::Serialization(e.to_string()))?;
        write_atomic(&self.checkpoints_path, &new_content)
            .map_err(|e| crate::error::EventStoreError::Backend(e.to_string()))?;

        Ok(())
    }
}

#[cfg(all(feature = "json-file", feature = "async"))]
#[async_trait::async_trait]
impl crate::projection::AsyncCheckpointStore for JsonFileCheckpointStore {
    type Error = crate::error::EventStoreError;

    async fn load_checkpoint(&self, projection_name: &str) -> Result<Option<u64>, Self::Error> {
        let this = self.clone();
        let name = projection_name.to_owned();
        tokio::task::spawn_blocking(move || {
            crate::projection::CheckpointStore::load_checkpoint(&this, &name)
        })
        .await
        .map_err(|e| crate::error::EventStoreError::Backend(e.to_string()))?
    }

    async fn save_checkpoint(
        &self,
        projection_name: &str,
        sequence: u64,
    ) -> Result<(), Self::Error> {
        let this = self.clone();
        let name = projection_name.to_owned();
        tokio::task::spawn_blocking(move || {
            crate::projection::CheckpointStore::save_checkpoint(&this, &name, sequence)
        })
        .await
        .map_err(|e| crate::error::EventStoreError::Backend(e.to_string()))?
    }
}
