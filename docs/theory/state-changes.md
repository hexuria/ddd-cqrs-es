---
title: 1.3. Making Changes to State
description: Learn how commands are validated and events are applied to transition state.
---

In a traditional CRUD system, changing the state of an application is simple and destructive: you load a database row, modify its columns in memory, and save it back using an `UPDATE` statement. This approach makes it impossible to understand *why* a change occurred or *what* the previous values were.

In an Event Sourced system, we make changes to the application state through a decoupled, two-step process: **Command Handling** and **Event Application**.

---

## The Two-Step State Transition

```
[ Incoming Command ]
        │
        ▼
1. Validation (handle) ──► Fails ──► Return Error (Abort)
        │
        ▼ Succeeds
   Emits Domain Events
        │
        ▼
2. Mutation (apply) ──► Mutates in-memory state deterministically
```

### Step 1: Command Validation (`handle`)
A **Command** represents a user's intent or instructions (e.g., `WithdrawMoney`). 
* The aggregate root is responsible for validating this incoming command against its current in-memory state.
* The handler is a **pure function**. It must NOT mutate the aggregate's state directly, and it must NOT perform any side effects (such as making database queries or network requests).
* If validation succeeds, it returns a vector of **Domain Events** (e.g., `MoneyWithdrawn`). If validation fails, it returns a domain-specific validation error (e.g., `InsufficientFunds`).

### Step 2: Event Application (`apply`)
A **Domain Event** represents an immutable historical fact (e.g., `MoneyWithdrawn`).
* Once the events are returned by the validation step, they are applied to the aggregate's state via the `apply` method.
* The `apply` method is responsible for mutating the internal fields of the aggregate (e.g., subtracting the amount from the `balance` field).
* The `apply` method must be **strictly deterministic**. Given the same event, it must always transition the state to the exact same values, without any external dependencies or side effects.

---

## Why Is This Separation Crucial?

This separation of validation and mutation is the foundational engine of Event Sourcing:

1. **Reconstituting State (Replay):** When an aggregate is loaded, the framework fetches all past committed events belonging to that instance and runs them through `apply` sequentially inside an in-memory loop. Because `apply` is deterministic, the in-memory state is rebuilt perfectly to its current version. No command validation occurs during replay—validation is only performed when handling *new* incoming commands.
2. **Crash Recovery & Auditability:** Because every state transition is recorded as an immutable event, you have a complete, tamper-proof audit trail of the aggregate's entire lifecycle. You can analyze every state change, find bugs easily, and replay events to debug any historical state.
3. **Optimistic Concurrency:** Separating validation and mutation allows the database to check for concurrency conflicts. If two clients try to modify the same aggregate simultaneously, they will load the same state, but the database will reject the second append due to version mismatch, preventing corrupted state.
