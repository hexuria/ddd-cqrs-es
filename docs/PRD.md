# Product Requirements

This repository implements the first production skeleton of the Rust DDD CQRS
Event Sourcing framework described in the source PRD.

Implemented in this version:

- Typed aggregate API with ID, command, event, and domain error types.
- Event envelopes with event ID, aggregate type, stream revision, global sequence, event type, version, metadata, and recorded time.
- Metadata for correlation, causation, actor, tenant, request, and custom headers.
- Expected revision and first-class concurrency errors.
- Repository orchestration for load, replay, command execution, append, and updated state return.
- Thread-safe in-memory event store with stream ordering, optimistic concurrency, global ordering, global reads, and clear support.
- Projection trait and in-memory checkpointed projection runner.
- Process manager abstraction for event-to-command policies.
- Snapshot model and snapshot store trait.
- Aggregate fixture for unit testing domain behavior without storage.
- Stable event type/version metadata through `DomainEvent`.
- Feature-gated async traits and async repository.
- Feature-gated serde/JSON envelope round trips.
- UUID-backed event IDs when the `uuid` feature is enabled.
- Event upcaster trait.
- Idempotency key and in-memory idempotency store.
- Snapshot-aware repository loading and execution.
- Reusable event store contract helper for adapter tests.
- Bank account example and integration tests.

Deferred adapters and optional capabilities:

- PostgreSQL and SQLite durable stores.
- Tracing middleware.
