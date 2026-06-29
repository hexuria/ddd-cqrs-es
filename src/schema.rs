//! # Versioned Schema Migration System
//!
//! This module provides a lightweight, framework-owned database schema migrator
//! for events, projection checkpoints, and idempotency keys, supporting SQLite,
//! Postgres, and MySQL. It avoids external heavy migration libraries.

use crate::error::EventStoreError;
#[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
use std::collections::HashSet;
#[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
use std::time::{SystemTime, UNIX_EPOCH};

/// Supported SQL Database Dialects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialect {
    Sqlite,
    Postgres,
    MySql,
}

/// Schema configuration with customizable table names.
#[derive(Debug, Clone)]
pub struct SqlSchemaConfig {
    pub dialect: SqlDialect,
    pub events_table: String,
    pub checkpoints_table: String,
    pub idempotency_table: String,
    pub migrations_table: String,
}

impl SqlSchemaConfig {
    /// Creates a schema configuration with default table names for the given dialect.
    pub fn new(dialect: SqlDialect) -> Self {
        Self {
            dialect,
            events_table: "events".to_string(),
            checkpoints_table: "projection_checkpoints".to_string(),
            idempotency_table: "idempotency_keys".to_string(),
            migrations_table: "schema_migrations".to_string(),
        }
    }

    /// Sets a custom events table name.
    pub fn with_events_table(mut self, name: impl Into<String>) -> Self {
        self.events_table = name.into();
        self
    }

    /// Sets a custom checkpoints table name.
    pub fn with_checkpoints_table(mut self, name: impl Into<String>) -> Self {
        self.checkpoints_table = name.into();
        self
    }

    /// Sets a custom idempotency table name.
    pub fn with_idempotency_table(mut self, name: impl Into<String>) -> Self {
        self.idempotency_table = name.into();
        self
    }

    /// Sets a custom migrations table name.
    pub fn with_migrations_table(mut self, name: impl Into<String>) -> Self {
        self.migrations_table = name.into();
        self
    }

    /// Interpolates a SQL string replacing the placeholders with configured table names.
    pub fn interpolate(&self, sql: &str) -> String {
        sql.replace("{events_table}", &self.events_table)
            .replace("{checkpoints_table}", &self.checkpoints_table)
            .replace("{idempotency_table}", &self.idempotency_table)
            .replace("{migrations_table}", &self.migrations_table)
    }
}

/// A representation of a versioned schema migration.
#[derive(Debug, Clone)]
pub struct SchemaMigration {
    pub version: i32,
    pub description: &'static str,
    pub up_sql: &'static str,
}

/// Canonical framework-owned migrations.
pub fn get_migrations(dialect: SqlDialect) -> Vec<SchemaMigration> {
    match dialect {
        SqlDialect::Sqlite => vec![
            SchemaMigration {
                version: 1,
                description: "create_events_table",
                up_sql: r#"
                    CREATE TABLE IF NOT EXISTS {events_table} (
                        sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                        event_id TEXT NOT NULL UNIQUE,
                        aggregate_id TEXT NOT NULL,
                        aggregate_type TEXT NOT NULL,
                        revision INTEGER NOT NULL,
                        event_type TEXT NOT NULL,
                        event_version INTEGER NOT NULL,
                        payload TEXT NOT NULL,
                        metadata TEXT NOT NULL,
                        recorded_at_ms INTEGER NOT NULL,
                        UNIQUE (aggregate_type, aggregate_id, revision)
                    );
                    CREATE INDEX IF NOT EXISTS {events_table}_stream_idx
                        ON {events_table} (aggregate_type, aggregate_id, revision);
                "#,
            },
            SchemaMigration {
                version: 2,
                description: "create_checkpoints_table",
                up_sql: r#"
                    CREATE TABLE IF NOT EXISTS {checkpoints_table} (
                        projection_name TEXT PRIMARY KEY,
                        sequence INTEGER NOT NULL
                    );
                "#,
            },
            SchemaMigration {
                version: 3,
                description: "create_idempotency_table",
                up_sql: r#"
                    CREATE TABLE IF NOT EXISTS {idempotency_table} (
                        idempotency_key TEXT PRIMARY KEY,
                        state TEXT NOT NULL CHECK (state IN ('pending', 'complete')),
                        value TEXT,
                        updated_at_ms INTEGER NOT NULL
                    );
                "#,
            },
        ],
        SqlDialect::Postgres => vec![
            SchemaMigration {
                version: 1,
                description: "create_events_table",
                up_sql: r#"
                    CREATE TABLE IF NOT EXISTS {events_table} (
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
                    CREATE INDEX IF NOT EXISTS {events_table}_stream_idx
                        ON {events_table} (aggregate_type, aggregate_id, revision);
                "#,
            },
            SchemaMigration {
                version: 2,
                description: "create_checkpoints_table",
                up_sql: r#"
                    CREATE TABLE IF NOT EXISTS {checkpoints_table} (
                        projection_name VARCHAR(255) PRIMARY KEY,
                        sequence BIGINT NOT NULL
                    );
                "#,
            },
            SchemaMigration {
                version: 3,
                description: "create_idempotency_table",
                up_sql: r#"
                    CREATE TABLE IF NOT EXISTS {idempotency_table} (
                        idempotency_key VARCHAR(255) PRIMARY KEY,
                        state VARCHAR(20) NOT NULL CHECK (state IN ('pending', 'complete')),
                        value JSONB,
                        updated_at_ms BIGINT NOT NULL
                    );
                "#,
            },
        ],
        SqlDialect::MySql => vec![
            SchemaMigration {
                version: 1,
                description: "create_events_table",
                up_sql: r#"
                    CREATE TABLE IF NOT EXISTS {events_table} (
                        sequence BIGINT AUTO_INCREMENT PRIMARY KEY,
                        event_id VARCHAR(255) NOT NULL UNIQUE,
                        aggregate_id VARCHAR(255) NOT NULL,
                        aggregate_type VARCHAR(255) NOT NULL,
                        revision BIGINT NOT NULL,
                        event_type VARCHAR(255) NOT NULL,
                        event_version INT NOT NULL,
                        payload JSON NOT NULL,
                        metadata JSON NOT NULL,
                        recorded_at_ms BIGINT NOT NULL,
                        UNIQUE KEY (aggregate_type, aggregate_id, revision),
                        INDEX (aggregate_type, aggregate_id, revision)
                    );
                "#,
            },
            SchemaMigration {
                version: 2,
                description: "create_checkpoints_table",
                up_sql: r#"
                    CREATE TABLE IF NOT EXISTS {checkpoints_table} (
                        projection_name VARCHAR(255) PRIMARY KEY,
                        sequence BIGINT NOT NULL
                    );
                "#,
            },
            SchemaMigration {
                version: 3,
                description: "create_idempotency_table",
                up_sql: r#"
                    CREATE TABLE IF NOT EXISTS {idempotency_table} (
                        idempotency_key VARCHAR(255) PRIMARY KEY,
                        state VARCHAR(20) NOT NULL CHECK (state IN ('pending', 'complete')),
                        value JSON,
                        updated_at_ms BIGINT NOT NULL
                    );
                "#,
            },
        ],
    }
}

#[allow(dead_code)]
fn get_target_table_name(version: i32, config: &SqlSchemaConfig) -> &str {
    match version {
        1 => &config.events_table,
        2 => &config.checkpoints_table,
        3 => &config.idempotency_table,
        _ => "",
    }
}

/// The Versioned Schema Migrator.
///
/// # Atomicity & Transaction Limits
/// While `SchemaMigrator` runs are idempotent, schema migration is not fully transaction-wrapped
/// across all database dialects. Specifically, in **MySQL**, DDL commands (such as `CREATE TABLE` and
/// `DROP TABLE`) trigger **implicit commits**. This means any DDL operation executed during a migration
/// run commits immediately, and cannot be rolled back mid-transaction if a subsequent migration step fails.
///
/// Users should ensure they have proper database backups and verify migration files before applying
/// them to a live MySQL environment.
#[derive(Debug, Clone)]
pub struct SchemaMigrator {
    #[allow(dead_code)]
    config: SqlSchemaConfig,
}

impl SchemaMigrator {
    /// Creates a migrator for a given configuration.
    pub fn new(config: SqlSchemaConfig) -> Self {
        Self { config }
    }

    #[allow(dead_code)]
    fn validate_config(&self) -> Result<(), EventStoreError> {
        crate::sql_common::validate_table_name(&self.config.events_table)?;
        crate::sql_common::validate_table_name(&self.config.checkpoints_table)?;
        crate::sql_common::validate_table_name(&self.config.idempotency_table)?;
        crate::sql_common::validate_table_name(&self.config.migrations_table)?;
        Ok(())
    }

    /// Runs SQLite migrations.
    #[cfg(feature = "sqlite")]
    pub fn run_sqlite(&self, conn: &rusqlite::Connection) -> Result<(), EventStoreError> {
        self.validate_config()?;

        // 1. Ensure migrations table exists (with composite key)
        let create_mig_table = self.config.interpolate(
            "CREATE TABLE IF NOT EXISTS {migrations_table} (
                version INTEGER NOT NULL,
                table_name TEXT NOT NULL,
                description TEXT NOT NULL,
                applied_at_ms INTEGER NOT NULL,
                PRIMARY KEY (version, table_name)
            );",
        );
        conn.execute(&create_mig_table, [])
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;

        // Check if the 'table_name' column exists in SQLite migrations table
        let pragma_query = self
            .config
            .interpolate("PRAGMA table_info({migrations_table});");
        let mut stmt = conn
            .prepare(&pragma_query)
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;
        let columns: HashSet<String> = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(|e| EventStoreError::Backend(e.to_string()))?
            .collect::<Result<HashSet<String>, _>>()
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;

        let has_col = columns.contains("table_name");
        if !has_col && !columns.is_empty() {
            // Drop and recreate table with the new schema
            let drop_table = self.config.interpolate("DROP TABLE {migrations_table};");
            conn.execute(&drop_table, [])
                .map_err(|e| EventStoreError::Backend(e.to_string()))?;
            conn.execute(&create_mig_table, [])
                .map_err(|e| EventStoreError::Backend(e.to_string()))?;
        }

        // 2. Fetch applied migrations
        let query_applied = self
            .config
            .interpolate("SELECT version, table_name FROM {migrations_table};");
        let mut stmt = conn
            .prepare(&query_applied)
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;
        let applied_pairs: HashSet<(i32, String)> = stmt
            .query_map([], |row| {
                let v: i32 = row.get(0)?;
                let t: String = row.get(1)?;
                Ok((v, t))
            })
            .map_err(|e| EventStoreError::Backend(e.to_string()))?
            .collect::<Result<HashSet<(i32, String)>, _>>()
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;

        // 3. Execute unapplied migrations
        let migrations = get_migrations(SqlDialect::Sqlite);
        for m in migrations {
            let target_table = get_target_table_name(m.version, &self.config);
            if !applied_pairs.contains(&(m.version, target_table.to_string())) {
                // Execute migration SQL (exec batch to support multiple statements like CREATE INDEX)
                let sql = self.config.interpolate(m.up_sql);
                conn.execute_batch(&sql)
                    .map_err(|e| EventStoreError::Backend(e.to_string()))?;

                // Version 2 compatibility copy
                if m.version == 2 {
                    let old_checkpoints_exist: bool = conn.query_row(
                        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='checkpoints')",
                        [],
                        |row| row.get(0),
                    ).unwrap_or(false);

                    if old_checkpoints_exist {
                        let copy_sql = self.config.interpolate(
                            "INSERT OR IGNORE INTO {checkpoints_table} (projection_name, sequence) \
                             SELECT projection_name, last_sequence FROM checkpoints;"
                        );
                        conn.execute(&copy_sql, [])
                            .map_err(|e| EventStoreError::Backend(e.to_string()))?;
                    }
                }

                // Record applied migration
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;
                let insert_mig = self.config.interpolate(
                    "INSERT INTO {migrations_table} (version, table_name, description, applied_at_ms) VALUES (?1, ?2, ?3, ?4);"
                );
                conn.execute(
                    &insert_mig,
                    rusqlite::params![m.version, target_table, m.description, now_ms],
                )
                .map_err(|e| EventStoreError::Backend(e.to_string()))?;
            }
        }

        Ok(())
    }

    /// Runs PostgreSQL migrations.
    #[cfg(feature = "postgres")]
    pub fn run_postgres(&self, client: &mut postgres::Client) -> Result<(), EventStoreError> {
        self.validate_config()?;

        // 1. Ensure migrations table exists
        let create_mig_table = self.config.interpolate(
            "CREATE TABLE IF NOT EXISTS {migrations_table} (
                version INT NOT NULL,
                table_name VARCHAR(255) NOT NULL,
                description TEXT NOT NULL,
                applied_at_ms BIGINT NOT NULL,
                PRIMARY KEY (version, table_name)
            );",
        );
        client
            .batch_execute(&create_mig_table)
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;

        // Check if table_name column exists
        let check_col = self.config.interpolate(
            "SELECT EXISTS (
                SELECT 1
                FROM pg_attribute a
                JOIN pg_class c ON a.attrelid = c.oid
                WHERE c.relname = '{migrations_table}' AND a.attname = 'table_name'
            );",
        );
        let has_col: bool = client
            .query_one(&check_col, &[])
            .map(|row| row.get(0))
            .unwrap_or(false);

        if !has_col {
            // Drop and recreate
            let drop_table = self.config.interpolate("DROP TABLE {migrations_table};");
            client
                .batch_execute(&drop_table)
                .map_err(|e| EventStoreError::Backend(e.to_string()))?;
            client
                .batch_execute(&create_mig_table)
                .map_err(|e| EventStoreError::Backend(e.to_string()))?;
        }

        // 2. Fetch applied migrations
        let query_applied = self
            .config
            .interpolate("SELECT version, table_name FROM {migrations_table};");
        let rows = client
            .query(&query_applied, &[])
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;
        let applied_pairs: HashSet<(i32, String)> = rows
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();

        // 3. Execute unapplied migrations
        let migrations = get_migrations(SqlDialect::Postgres);
        for m in migrations {
            let target_table = get_target_table_name(m.version, &self.config);
            if !applied_pairs.contains(&(m.version, target_table.to_string())) {
                let sql = self.config.interpolate(m.up_sql);
                client
                    .batch_execute(&sql)
                    .map_err(|e| EventStoreError::Backend(e.to_string()))?;

                // Version 2 compatibility copy
                if m.version == 2 {
                    let old_checkpoints_exist: bool = client.query_one(
                        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'checkpoints')",
                        &[],
                    ).map(|row| row.get(0)).unwrap_or(false);

                    if old_checkpoints_exist {
                        let copy_sql = self.config.interpolate(
                            "INSERT INTO {checkpoints_table} (projection_name, sequence) \
                             SELECT projection_name, last_sequence FROM checkpoints ON CONFLICT DO NOTHING;"
                        );
                        client
                            .execute(&copy_sql, &[])
                            .map_err(|e| EventStoreError::Backend(e.to_string()))?;
                    }
                }

                // Record applied migration
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;
                let insert_mig = self.config.interpolate(
                    "INSERT INTO {migrations_table} (version, table_name, description, applied_at_ms) VALUES ($1, $2, $3, $4);"
                );
                client
                    .execute(
                        &insert_mig,
                        &[&m.version, &target_table, &m.description, &now_ms],
                    )
                    .map_err(|e| EventStoreError::Backend(e.to_string()))?;
            }
        }

        Ok(())
    }

    /// Runs MySQL migrations.
    #[cfg(feature = "mysql")]
    pub fn run_mysql(&self, conn: &mut mysql::Conn) -> Result<(), EventStoreError> {
        use mysql::prelude::Queryable;
        self.validate_config()?;

        // 1. Ensure migrations table exists
        let create_mig_table = self.config.interpolate(
            "CREATE TABLE IF NOT EXISTS {migrations_table} (
                version INT NOT NULL,
                table_name VARCHAR(255) NOT NULL,
                description VARCHAR(255) NOT NULL,
                applied_at_ms BIGINT NOT NULL,
                PRIMARY KEY (version, table_name)
            );",
        );
        conn.query_drop(&create_mig_table)
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;

        // Check if table_name column exists
        let check_col = self.config.interpolate(
            "SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = '{migrations_table}'
                  AND column_name = 'table_name'
                  AND table_schema = DATABASE()
            );",
        );
        let has_col: bool = conn
            .query_first(&check_col)
            .map(|row_opt| {
                row_opt
                    .and_then(|r: mysql::Row| r.get::<bool, _>(0))
                    .unwrap_or(false)
            })
            .unwrap_or(false);

        if !has_col {
            // Drop and recreate
            let drop_table = self.config.interpolate("DROP TABLE {migrations_table};");
            conn.query_drop(&drop_table)
                .map_err(|e| EventStoreError::Backend(e.to_string()))?;
            conn.query_drop(&create_mig_table)
                .map_err(|e| EventStoreError::Backend(e.to_string()))?;
        }

        // 2. Fetch applied migrations
        let query_applied = self
            .config
            .interpolate("SELECT version, table_name FROM {migrations_table};");
        let rows: Vec<(i32, String)> = conn
            .query(&query_applied)
            .map_err(|e| EventStoreError::Backend(e.to_string()))?;
        let applied_pairs: HashSet<(i32, String)> = rows.into_iter().collect();

        // 3. Execute unapplied migrations
        let migrations = get_migrations(SqlDialect::MySql);
        for m in migrations {
            let target_table = get_target_table_name(m.version, &self.config);
            if !applied_pairs.contains(&(m.version, target_table.to_string())) {
                // Execute migration SQL (using standard query_drop)
                let sql = self.config.interpolate(m.up_sql);
                conn.query_drop(&sql)
                    .map_err(|e| EventStoreError::Backend(e.to_string()))?;

                // Version 2 compatibility copy
                if m.version == 2 {
                    let old_checkpoints_exist: bool = conn.query_first(
                        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'checkpoints' AND table_schema = DATABASE())"
                    ).map(|row_opt| row_opt.and_then(|r: mysql::Row| r.get::<bool, _>(0)).unwrap_or(false)).unwrap_or(false);

                    if old_checkpoints_exist {
                        let copy_sql = self.config.interpolate(
                            "INSERT IGNORE INTO {checkpoints_table} (projection_name, sequence) \
                             SELECT projection_name, last_sequence FROM checkpoints;",
                        );
                        conn.query_drop(&copy_sql)
                            .map_err(|e| EventStoreError::Backend(e.to_string()))?;
                    }
                }

                // Record applied migration
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;
                let insert_mig = self.config.interpolate(
                    "INSERT INTO {migrations_table} (version, table_name, description, applied_at_ms) VALUES (?, ?, ?, ?);"
                );
                conn.exec_drop(
                    &insert_mig,
                    (m.version, target_table, m.description, now_ms),
                )
                .map_err(|e| EventStoreError::Backend(e.to_string()))?;
            }
        }

        Ok(())
    }
}

/// A concurrent-safe async schema initializer to guarantee schemas are initialized exactly once.
#[cfg(feature = "async")]
pub struct AsyncSchemaInitializer {
    initialized: std::sync::atomic::AtomicBool,
    lock: std::sync::OnceLock<tokio::sync::Mutex<()>>,
}

#[cfg(feature = "async")]
impl AsyncSchemaInitializer {
    /// Creates a new schema initializer.
    pub const fn new() -> Self {
        Self {
            initialized: std::sync::atomic::AtomicBool::new(false),
            lock: std::sync::OnceLock::new(),
        }
    }

    /// Runs the provided asynchronous initialization function exactly once, safely handling concurrency.
    pub async fn run<F, Fut, E>(&self, init_fn: F) -> Result<(), E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<(), E>>,
    {
        if self.initialized.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(());
        }

        let lock = self.lock.get_or_init(|| tokio::sync::Mutex::new(()));
        let _guard = lock.lock().await;

        if self.initialized.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(());
        }

        init_fn().await?;

        self.initialized
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Resets the initialization state (primarily for testing purposes).
    pub fn reset(&self) {
        self.initialized
            .store(false, std::sync::atomic::Ordering::Release);
    }
}

#[cfg(feature = "async")]
impl Default for AsyncSchemaInitializer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "async")]
impl std::fmt::Debug for AsyncSchemaInitializer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncSchemaInitializer")
            .field("initialized", &self.initialized)
            .finish()
    }
}
