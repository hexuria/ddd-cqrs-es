---
title: 4.2. A Simple Query
description: Build a queryable view by projecting committed events in real-time.
---

Now that we have configured our write path using the in-memory event store, we want to construct a read-side query view. This allows us to display active balances or summaries on our user interface.

To do this, we implement a **Read Model** and a **Projection**.

---

## 1. Defining the Read Model State

First, define the structure of the data you want to query. This is a query-optimized state. For our bank account dashboard, we want a simple map associating account IDs with their current total balance:

```rust
use std::collections::HashMap;

// =========================================================================
// Define the Read Model State
// =========================================================================
// A simple, query-optimized in-memory database to store total active balances.
#[derive(Default, Debug)]
pub struct AccountDashboard {
    balances: HashMap<String, u64>,
}

impl AccountDashboard {
    /// Retrieve the balance of a specific account from our pre-computed map.
    pub fn get_balance(&self, account_id: &str) -> Option<u64> {
        self.balances.get(account_id).copied()
    }
}
```

---

## 2. Implementing the `Projection` Trait

Next, implement the `Projection` trait for your read model. This specifies how our database view is updated whenever a new event envelope is committed to the store:

```rust
use ddd_cqrs_es::{EventEnvelope, Projection};

impl Projection<BankAccountEvent, String> for AccountDashboard {
    type Error = std::convert::Infallible;

    /// A unique name identifier for the projection, used to isolate its checkpoint.
    fn name(&self) -> &'static str {
        "account_dashboard_view"
    }

    /// Applies events sequentially to update the read model state.
    /// This method must be idempotent (safe to run multiple times for the same event).
    fn apply(&mut self, envelope: &EventEnvelope<BankAccountEvent, String>) -> Result<(), Self::Error> {
        let account_id = envelope.aggregate_id.clone();
        
        match &envelope.payload {
            BankAccountEvent::AccountOpened { .. } => {
                // Initialize balance idempotently
                self.balances.entry(account_id).or_insert(0);
            }
            BankAccountEvent::MoneyDeposited { amount } => {
                let balance = self.balances.entry(account_id).or_insert(0);
                *balance += amount;
            }
            BankAccountEvent::MoneyWithdrawn { amount } => {
                let balance = self.balances.entry(account_id).or_insert(0);
                // Prevent arithmetic underflows defensively
                *balance = balance.saturating_sub(*amount);
            }
        }
        
        Ok(())
    }
}
```

By maintaining this projection, we denormalize event records into a simple, high-speed key-value map, allowing client requests to resolve balances instantly.
