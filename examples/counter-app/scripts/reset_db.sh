#!/bin/bash
set -e

# reset_db.sh - Multi-backend database schema reset tool

BACKEND="${DATABASE_BACKEND:-sqlite}"
echo "Resetting database backend: $BACKEND"

case "$BACKEND" in
  sqlite)
    echo "Cleaning local SQLite / flat-file data..."
    rm -f .spin/sqlite_db.db
    rm -rf data/*.json
    rm -f data/.schema_initialized_*
    echo "Local SQLite database and markers reset."
    ;;

  postgres|neon)
    if [ -z "$DATABASE_URL" ]; then
      echo "Error: DATABASE_URL environment variable is not set." >&2
      exit 1
    fi
    echo "Resetting PostgreSQL database..."
    psql "$DATABASE_URL" <<EOF
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS checkpoints;
DROP TABLE IF EXISTS counter_read_model;

CREATE TABLE events (
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

CREATE TABLE checkpoints (
    projection_name VARCHAR(255) PRIMARY KEY,
    last_sequence BIGINT NOT NULL
);

CREATE TABLE counter_read_model (
    id VARCHAR(255) PRIMARY KEY,
    value BIGINT NOT NULL
);
EOF
    echo "PostgreSQL schema successfully dropped and re-created."
    ;;

  libsql|turso)
    if [ -z "$DATABASE_URL" ]; then
      echo "Error: DATABASE_URL environment variable is not set." >&2
      exit 1
    fi
    
    # Resolve libsql:// to https://
    RESOLVED_URL="$DATABASE_URL"
    if [[ "$RESOLVED_URL" =~ ^libsql:// ]]; then
      RESOLVED_URL="https://${RESOLVED_URL#libsql://}"
    fi
    
    # Trim trailing slashes and ensure /v2/pipeline
    RESOLVED_URL="${RESOLVED_URL%/}"
    PIPELINE_URL="${RESOLVED_URL}/v2/pipeline"
    
    echo "Resetting LibSQL database via Hrana API at pipeline endpoint..."
    
    AUTH_HEADER=""
    if [ -n "$DATABASE_AUTH_TOKEN" ]; then
      AUTH_HEADER="Authorization: Bearer $DATABASE_AUTH_TOKEN"
    fi
    
    PAYLOAD=$(cat <<EOF
{
  "baton": null,
  "requests": [
    {
      "type": "execute",
      "stmt": {
        "sql": "DROP TABLE IF EXISTS events;"
      }
    },
    {
      "type": "execute",
      "stmt": {
        "sql": "DROP TABLE IF EXISTS checkpoints;"
      }
    },
    {
      "type": "execute",
      "stmt": {
        "sql": "DROP TABLE IF EXISTS counter_read_model;"
      }
    },
    {
      "type": "execute",
      "stmt": {
        "sql": "CREATE TABLE events (event_id TEXT NOT NULL UNIQUE, aggregate_id TEXT NOT NULL, aggregate_type TEXT NOT NULL, revision INTEGER NOT NULL, sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_type TEXT NOT NULL, event_version INTEGER NOT NULL, payload TEXT NOT NULL, metadata TEXT NOT NULL, recorded_at_ms INTEGER NOT NULL, UNIQUE (aggregate_id, aggregate_type, revision));"
      }
    },
    {
      "type": "execute",
      "stmt": {
        "sql": "CREATE TABLE checkpoints (projection_name TEXT PRIMARY KEY, last_sequence INTEGER NOT NULL);"
      }
    },
    {
      "type": "execute",
      "stmt": {
        "sql": "CREATE TABLE counter_read_model (id TEXT PRIMARY KEY, value INTEGER NOT NULL);"
      }
    },
    {
      "type": "close"
    }
  ]
}
EOF
)
    
    if [ -n "$AUTH_HEADER" ]; then
      curl -s -f -X POST -H "Content-Type: application/json" -H "$AUTH_HEADER" -d "$PAYLOAD" "$PIPELINE_URL" > /dev/null
    else
      curl -s -f -X POST -H "Content-Type: application/json" -d "$PAYLOAD" "$PIPELINE_URL" > /dev/null
    fi
    
    echo "LibSQL schema successfully dropped and re-created."
    ;;

  redis)
    REDIS_RESET_URL="${DATABASE_URL:-${REDIS_URL:-redis://127.0.0.1:6379}}"
    if ! command -v redis-cli >/dev/null 2>&1; then
      echo "Error: redis-cli is required to reset Redis keys." >&2
      exit 1
    fi

    echo "Resetting Redis keys at $REDIS_RESET_URL..."
    for pattern in "ddd_cqrs_es:*" "counter:read_model:*" "counter:redis_trigger:*" "counter:realtime:*"; do
      KEYS="$(redis-cli -u "$REDIS_RESET_URL" --scan --pattern "$pattern")"
      if [ -n "$KEYS" ]; then
        printf '%s\n' "$KEYS" | xargs redis-cli -u "$REDIS_RESET_URL" del >/dev/null
      fi
    done
    echo "Redis event-store, checkpoint, read-model, and realtime keys reset."
    ;;

  *)
    echo "Unsupported DATABASE_BACKEND: $BACKEND" >&2
    exit 1
    ;;
esac
