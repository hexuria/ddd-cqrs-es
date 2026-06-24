---
title: 1.5. Event Sourcing
description: Represent application state as an immutable log of historical facts.
---

In a traditional database-driven application (such as CRUD), state is mutated destructively. When a customer withdraws money, you overwrite their balance in a database table. You lose the context of *how* and *when* that state was reached.

In an **Event Sourced** system, we never mutate state directly. Instead, we represent every change to our application as an immutable, sequential log of historical facts called an **Event Stream**.

---

## State Reconstitution (Replay)

To determine the current state of an entity, the system does not read a single mutated row. Instead:
1. It loads the entire stream of past committed events belonging to that specific Aggregate ID from an append-only store.
2. It initializes an empty instance of the aggregate in memory.
3. It replays each event in-order through the aggregate's deterministic `apply` method.

Because the historical events are immutable, replaying them will always reconstitute the exact, correct state of the aggregate root, ensuring high reliability and auditability.

---

## The Command Execution Lifecycle

The following sequence diagram illustrates the complete, end-to-end lifecycle of a client dispatching a command to execute a transaction, persisting the resulting event facts, and asynchronously updating the query models:

```mermaid
sequenceDiagram
    actor Client
    participant CB as CommandBus / Handler
    participant Repo as Repository
    participant ES as EventStore
    participant Agg as Aggregate Root
    participant PR as Projection Runner

    Client->>CB: 1. Dispatch Command (e.g., WithdrawMoney)
    CB->>Repo: 2. Request execution on Aggregate ID
    Repo->>ES: 3. Load committed event stream for ID
    ES-->>Repo: 4. Return Event Envelope Stream (Facts)
    Repo->>Agg: 5. Factory empty state via Aggregate::new()
    
    Note over Repo, Agg: State Reconstitution (Replay Loop)
    loop For each committed Event Envelope
        Repo->>Agg: 6. Apply event via Aggregate::apply(event)
        Note over Agg: Mutates state in-memory deterministically
    end

    Repo->>Agg: 7. Execute business validation via Aggregate::handle(command)
    Note over Agg: Validates invariants against replayed state
    
    alt Business Rule Violated (Validation Fails)
        Agg-->>Repo: 8. Return Domain Error (e.g., InsufficientFunds)
        Repo-->>CB: 9. Propagate RepositoryError::Domain(Error)
        CB-->>Client: 10. Reject request (Transaction Aborted)
    else Validation Succeeds
        Agg-->>Repo: 8. Return Vector of New Domain Events (Facts)
        Repo->>ES: 9. Append envelopes to stream (ExpectedRevision::Exact)
        Note over ES: Enforces Optimistic Concurrency Checks
        
        alt Concurrency Revision Conflict! (Stream edited by someone else)
            ES-->>Repo: 10. Concurrency Collision Error
            Repo-->>CB: 11. Raise RepositoryError::Concurrency
            CB-->>Client: 12. Abort transaction (Client should retry)
        else Storage Succeeds
            ES-->>Repo: 10. Success (Events persisted & seq assigned)
            Repo-->>CB: 11. Return committed envelope info
            CB-->>Client: 12. Send Success Response (Transaction Committed)
        end
    end

    Note over ES, PR: Asynchronous Event-Driven Projection Loop
    loop Polling or Event Stream Listening
        PR->>ES: 13. Poll for new events after last checkpoint sequence
        ES-->>PR: 14. Return committed event envelopes
        PR->>PR: 15. Apply events to read models (Idempotent DB updates)
        PR->>PR: 16. Advance and store sequence checkpoint offset
    end
```

---

## Why Event Sourcing?

By building your core around event streams, you receive several powerful architectural benefits:

* **Auditability:** You have an indisputable, complete history of everything that has ever occurred in your domain. Excellent for compliance, business intelligence, and security audits.
* **Deterministic Bug Reproduction:** If a production error occurs, you can fetch that aggregate's event stream, load it into a local unit test, and replay it. You will replicate the exact in-memory state of the aggregate at that moment, letting you debug and resolve issues rapidly.
* **Temporal Querying:** You can reconstituted state to any point in time. If you want to know what a customer's account looked like exactly on January 1st, 2026, you simply replay only the events committed prior to that date.
