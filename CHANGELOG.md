# Changelog

## 0.2.0

- Removed the legacy `store` module shim; use top-level exports or the `event_store` and `memory` modules directly.
- Removed `Aggregate::id()` from the aggregate trait and renamed raw test replay to `replay_raw_events_from_zero`.
- Changed `EventType` from a `String` alias to a serde-transparent newtype.
- Made `SqlSchemaConfig` table names private and added eager validation through fallible builders.
- Added bounded idempotency waits through `IdempotencyWaitConfig` and timeout errors.
- Added process-manager runners for sync and async command dispatch.
- Added `ProjectionRunnerError` `Display` and `Error` implementations.
- Added configurable event-store contract-test sequence expectations.
- Optimized `execute_returning_state` to avoid a second stream load.
- Added bounded global replay and projection batch APIs for production catch-up loops.
- Added schema migration v6 to remove legacy duplicate stream indexes while preserving unique stream constraints.
- Added query-plan coverage for SQLite and live-gated PostgreSQL/MySQL adapter checks.
