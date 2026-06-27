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
