---
title: 2.3. Add an Error and Service
description: Define domain-specific validation errors and services to model business rules.
---

When a user issues a command, it is not guaranteed to succeed. For example:
* A customer cannot withdraw `$150` if their account only has `$100`.
* A customer cannot deposit a negative amount or `$0`.
* A customer cannot deposit money into an account that has not been opened yet.

To protect these business rules (also called **domain invariants**), we define explicit, domain-specific **Domain Errors**.

---

## Defining Domain Errors

Rather than using generic error strings or general database codes, we define our domain errors as a simple Rust enum. This makes errors highly typed, explicit, and easy to handle or translate in our application edge (such as web routers or GraphQL layers).

```rust
// =========================================================================
// Define Domain Errors
// =========================================================================
// Specific errors represent exactly why a business rule validation failed.
#[derive(Debug, PartialEq, Eq)]
pub enum BankAccountError {
    /// Attempted to open an account that is already active.
    AccountAlreadyOpen,
    /// Attempted to operate on an account that has not been initialized yet.
    AccountNotYetOpen,
    /// Attempted to withdraw more money than the current available balance.
    InsufficientFunds {
        available: u64,
        requested: u64,
    },
    /// Attempted to deposit zero or an invalid negative balance amount.
    InvalidDepositAmount,
}
```

---

## What About Domain Services?

In some applications, validating a command requires interacting with an external service or performing a query that spans multiple aggregates.
* In Domain-Driven Design, when an operation doesn't naturally belong to a single Aggregate Root, we encapsulate that logic inside a **Domain Service**.
* In our framework, keeping command handling **pure and synchronous** is a top priority to maintain local reasoning and easy testability.
* If a validation requires external data, we recommend querying that data in your application layer first, and then passing the results directly into the command payload or as a parameter to the command execution, keeping the aggregate's `handle` method completely free of network or database connections.
