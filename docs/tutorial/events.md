---
title: 2.2. Add Domain Events
description: Define domain events to represent past-tense historical facts.
---

Once a command is successfully validated, the aggregate root must emit one or more **Domain Events**. 

An event represents a historical fact—something that has *already happened* within our system. Because they represent the past, domain events are always named using the **past tense** (e.g., `AccountOpened`, `MoneyDeposited`).

---

## Defining Domain Events

In our framework, domain events are represented as a simple Rust enum, and we implement the `DomainEvent` trait to supply metadata about the event schema.

```rust
use ddd_cqrs_es::DomainEvent;

// =========================================================================
// Define Domain Events (The Facts)
// =========================================================================
// Events must represent historical facts that have already occurred. They
// must be stable, immutable, and serializable.
#[derive(Clone, Debug, PartialEq)]
pub enum BankAccountEvent {
    AccountOpened {
        account_id: String,
        owner: String,
    },
    MoneyDeposited {
        amount: u64,
    },
    MoneyWithdrawn {
        amount: u64,
    },
}

impl DomainEvent for BankAccountEvent {
    // Unique identifier for the event schema, useful for adapters or databases
    fn event_type(&self) -> &'static str {
        match self {
            BankAccountEvent::AccountOpened { .. } => "bank_account_opened",
            BankAccountEvent::MoneyDeposited { .. } => "money_deposited",
            BankAccountEvent::MoneyWithdrawn { .. } => "money_withdrawn",
        }
    }
}
```

---

## Event Design Guidelines

When designing events for your business domain, adhere to these guidelines:

1. **Past Tense:** Events represent facts that are set in stone. Always name them in the past tense (`AccountOpened`, `MoneyDeposited`).
2. **Immutable:** Events represent history and should never be modified once committed. In Rust, derive `Clone`, `Debug`, and `PartialEq` on your events, and keep them free of pointers, mutable references, or stateful handles.
3. **Self-Contained:** Events must carry all the parameters that represent the delta of the change. A `MoneyDeposited` event must carry the `amount` that was deposited so that replays and read model projections can calculate the resulting balance.
