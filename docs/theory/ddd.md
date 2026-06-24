---
title: 1.1. Domain-Driven Design
description: Center your software around a highly refined model of your business domain.
---

**Domain-Driven Design (DDD)** is an approach to software development that focuses on modeling real-world business domains with high fidelity. Rather than building database-first schemas, DDD dictates that the software design should directly reflect the business models, rules, and workflows.

By keeping your core business logic separate from infrastructure concerns like database queries or serialization formats, you build a system that is easy to understand, test, and adapt over time.

---

## Core DDD Concepts

To build an event-sourced system, you must understand three foundational pillars of Domain-Driven Design:

### 1. Ubiquitous Language
The **Ubiquitous Language** is a shared, structured language used consistently by both software developers and non-technical business stakeholders (domain experts). 
* The names of your Rust structs, methods, commands, and events must map exactly to real business concepts.
* For example, instead of naming a function `update_balance_with_negative_delta`, use `withdraw_money` or `charge_fee`. 
* This reduces translation errors and makes the code immediately understandable to anyone familiar with the business.

### 2. Entities & Value Objects
Within your domain model, data is separated into two categories:
* **Entities:** Objects that have a unique, stable identity over time, even if their internal attributes change. For example, a `BankAccount` is an entity identified by an account number. Even if the balance changes or the owner's name is updated, it remains the same bank account.
* **Value Objects:** Immutable objects that are defined solely by their attributes and have no identity of their own. For example, an `Address` or a `MoneyAmount` is a value object. If two `MoneyAmount` objects both contain `$100`, they are completely interchangeable.

### 3. The Aggregate Root & Transactional Consistency
An **Aggregate** is a cluster of associated entities and value objects that are treated as a single transaction unit for data changes.
* The **Aggregate Root** is the sole entry point into this cluster. External objects are only allowed to hold references to the aggregate root, never to any entities inside the aggregate.
* The root serves as a **strict transactional consistency boundary**. Any modifications to any state inside the aggregate must go through the root. This guarantees that all business rules (domain invariants) are verified and enforced at all times.
* In our framework, this consistency boundary is represented by implementing the `Aggregate` trait.

---

## How It Maps to Our Code

In our framework (`ddd_cqrs_es`), your aggregate root is represented by a plain Rust struct that implements the `Aggregate` trait.

```rust
use ddd_cqrs_es::Aggregate;

pub struct BankAccount {
    id: Option<String>,
    balance: u64,
    revision: u64,
}

impl Aggregate for BankAccount {
    type Id = String;
    type Command = BankAccountCommand;
    type Event = BankAccountEvent;
    type Error = BankAccountError;

    fn aggregate_type() -> &'static str {
        "bank_account"
    }

    fn id(&self) -> Option<&Self::Id> {
        self.id.as_ref()
    }

    fn revision(&self) -> u64 {
        self.revision
    }

    fn new() -> Self {
        Self {
            id: None,
            balance: 0,
            revision: 0,
        }
    }

    // State mutations and command validations are implemented here...
    fn apply(&mut self, event: &Self::Event) { /* ... */ }
    fn handle(&self, command: Self::Command) -> Result<Vec<Self::Event>, Self::Error> { /* ... */ }
}
```

By encapsulating state mutation within the aggregate root, we guarantee that no invalid transitions can occur, maintaining transactional integrity at the application edge.
