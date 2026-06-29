#!/bin/bash
set -e

# reset_db.sh - Multi-backend database schema reset tool

BACKEND="${DATABASE_BACKEND:-sqlite}"
echo "Resetting database backend: $BACKEND"

url_decode() {
  local input="${1//+/ }"
  printf '%b' "${input//%/\\x}"
}

query_param() {
  local query="$1"
  local key="$2"
  local pair
  local old_ifs="$IFS"
  IFS='&'
  for pair in $query; do
    case "$pair" in
      "$key="*) url_decode "${pair#*=}"; IFS="$old_ifs"; return 0 ;;
    esac
  done
  IFS="$old_ifs"
  return 1
}

write_mysql_option() {
  local key="$1"
  local value="$2"
  case "$value" in
    *$'\n'*|*$'\r'*)
      echo "Error: MySQL URL field for $key contains a newline." >&2
      exit 1
      ;;
  esac
  printf '%s=%s\n' "$key" "$value"
}

case "$BACKEND" in
  sqlite)
    echo "Cleaning local SQLite / flat-file data..."
    rm -f .spin/sqlite_db.db
    rm -rf data/*.json
    rm -f data/.schema_initialized_*
    echo "Local SQLite database and markers reset."
    ;;

  mysql)
    if [ -z "$DATABASE_URL" ]; then
      echo "Error: DATABASE_URL environment variable is not set." >&2
      exit 1
    fi
    echo "Resetting MySQL database..."

    URL_NO_SCHEME="${DATABASE_URL#mysql://}"
    if [ "$URL_NO_SCHEME" = "$DATABASE_URL" ]; then
      echo "Error: MySQL DATABASE_URL must start with mysql://." >&2
      exit 1
    fi

    URL_NO_FRAGMENT="${URL_NO_SCHEME%%#*}"
    QUERY_STRING=""
    case "$URL_NO_FRAGMENT" in
      *\?*) QUERY_STRING="${URL_NO_FRAGMENT#*\?}" ;;
    esac
    URL_NO_QUERY="${URL_NO_FRAGMENT%%\?*}"
    if [ "$URL_NO_QUERY" = "$URL_NO_FRAGMENT" ]; then
      URL_NO_QUERY="$URL_NO_FRAGMENT"
    fi

    case "$URL_NO_QUERY" in
      */*) ;;
      *)
        echo "Error: MySQL DATABASE_URL must include a database name." >&2
        exit 1
        ;;
    esac

    USER_PASS_HOST_PORT="${URL_NO_QUERY%%/*}"
    DB_NAME="$(url_decode "${URL_NO_QUERY#*/}")"
    USER_PASS="${USER_PASS_HOST_PORT%@*}"
    HOST_PORT="${USER_PASS_HOST_PORT##*@}"

    if [ "$USER_PASS" = "$USER_PASS_HOST_PORT" ]; then
      echo "Error: MySQL DATABASE_URL must include user credentials." >&2
      exit 1
    fi

    if [[ "$USER_PASS" == *:* ]]; then
      DB_USER="$(url_decode "${USER_PASS%%:*}")"
      DB_PASS="$(url_decode "${USER_PASS#*:}")"
    else
      DB_USER="$(url_decode "$USER_PASS")"
      DB_PASS=""
    fi

    if [[ "$HOST_PORT" == \[*\]* ]]; then
      DB_HOST="${HOST_PORT%%]*}"
      DB_HOST="${DB_HOST#[}"
      HOST_PORT_REMAINDER="${HOST_PORT#*]}"
      if [[ "$HOST_PORT_REMAINDER" == :* ]]; then
        DB_PORT="${HOST_PORT_REMAINDER#:}"
      else
        DB_PORT=3306
      fi
    else
      DB_HOST="${HOST_PORT%%:*}"
      DB_PORT="${HOST_PORT#*:}"
      if [ "$DB_PORT" = "$HOST_PORT" ]; then
        DB_PORT=3306
      fi
    fi

    if [ -z "$DB_USER" ] || [ -z "$DB_HOST" ] || [ -z "$DB_NAME" ]; then
      echo "Error: MySQL DATABASE_URL must include user, host, and database name." >&2
      exit 1
    fi
    if [ -z "$DB_PORT" ]; then
      DB_PORT=3306
    fi

    MYSQL_SSL_MODE="$(query_param "$QUERY_STRING" "ssl-mode" || true)"
    if [ -z "$MYSQL_SSL_MODE" ]; then
      MYSQL_SSL_MODE="$(query_param "$QUERY_STRING" "ssl_mode" || true)"
    fi

    MYSQL_DEFAULTS_FILE="$(mktemp)"
    trap 'rm -f "$MYSQL_DEFAULTS_FILE"' EXIT
    chmod 600 "$MYSQL_DEFAULTS_FILE"
    {
      echo "[client]"
      write_mysql_option "user" "$DB_USER"
      if [ -n "$DB_PASS" ]; then
        write_mysql_option "password" "$DB_PASS"
      fi
      write_mysql_option "host" "$DB_HOST"
      write_mysql_option "port" "$DB_PORT"
      if [ -n "$MYSQL_SSL_MODE" ]; then
        write_mysql_option "ssl-mode" "$MYSQL_SSL_MODE"
      fi
    } > "$MYSQL_DEFAULTS_FILE"

    mysql --defaults-extra-file="$MYSQL_DEFAULTS_FILE" "$DB_NAME" <<EOF
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS checkpoints;
DROP TABLE IF EXISTS counter_read_model;
DROP TABLE IF EXISTS schema_migrations;
DROP TABLE IF EXISTS idempotency_keys;

CREATE TABLE events (
    sequence BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id VARCHAR(255) NOT NULL UNIQUE,
    aggregate_id VARCHAR(255) NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    revision BIGINT NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    event_version INT NOT NULL,
    payload LONGTEXT NOT NULL,
    metadata LONGTEXT NOT NULL,
    recorded_at_ms BIGINT NOT NULL,
    UNIQUE KEY (aggregate_type, aggregate_id, revision),
    INDEX (aggregate_type, aggregate_id, revision)
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
    echo "MySQL schema successfully dropped and re-created."
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
