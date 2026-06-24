---
title: 2.4. Add an Aggregate
description: Implement the Aggregate trait to bind commands, events, errors, and state.
---

With our commands, events, and domain errors defined, we can now bind them together by implementing the `Aggregate` trait. 

The aggregate root maintains our application's internal state. This state is rebuilt by replaying events, and is used to validate incoming commands.

---

## 1. Defining the Aggregate Root Struct

Create your aggregate root struct. It should contain fields representing the internal state (such as `balance`, `owner`, or `id`) along with a `revision` counter that tracks the version of the event stream:

```rust
// =========================================================================
// Define State (The Aggregate Root)
// =========================================================================
// The aggregate root maintains internal state. It is reconstituted by
// replaying events, and is used to validate incoming commands.
#[derive(Default)]
pub struct BankAccount {
    id: Option<String>,
    owner: Option<String>,
    balance: u64,
    revision: u64,
}

impl BankAccount {
    /// Expose helpers for read models or assertions
    pub fn balance(&self) -> u64 {
        self.balance
    }
}
```

---

## 2. Implementing the `Aggregate` Trait

Now, implement the `Aggregate` trait for the `BankAccount` struct. This binds all components together:

```rust
use ddd_cqrs_es::Aggregate;

impl Aggregate for BankAccount {
    type Id = String;
    type Command = BankAccountCommand;
    type Event = BankAccountEvent;
    type Error = BankAccountError;

    /// A unique name for this type of aggregate, used to namespace streams in databases.
    fn aggregate_type() -> &'static str {
        "bank_account"
    }

    /// Exposes the unique ID of this instance.
    fn id(&self) -> Option<&Self::Id> {
        self.id.as_ref()
    }

    /// Current version number of the aggregate (tracks replayed events count).
    fn revision(&self) -> u64 {
        self.revision
    }

    /// Factory method to initialize an empty aggregate prior to state replay.
    fn new() -> Self {
        Self::default()
    }

    /// Replays past historical events to rebuild state in-memory.
    /// This method MUST be completely deterministic and free of side effects.
    fn apply(&mut self, event: &Self::Event) {
        match event {
            BankAccountEvent::AccountOpened { account_id, owner } => {
                self.id = Some(account_id.clone());
                self.owner = Some(owner.clone());
                self.balance = 0;
            }
            BankAccountEvent::MoneyDeposited { amount } => {
                self.balance += amount;
            }
            BankAccountEvent::MoneyWithdrawn { amount } => {
                self.balance -= amount;
            }
        }
        // Increment the revision counter to track stream version
        self.revision += 1;
    }

    /// Handles incoming commands against the current replayed state.
    /// Validates business invariants and returns new events or an error.
    /// It must NOT mutate state directly (state is only mutated in apply()).
    fn handle(&self, command: Self::Command) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            BankAccountCommand::OpenAccount { account_id, owner } => {
                // Invariant: Cannot open an account that is already active
                if self.id.is_some() {
                    return Err(BankAccountError::AccountAlreadyOpen);
                }
                Ok(vec![BankAccountEvent::AccountOpened { account_id, owner }])
            }
            BankAccountCommand::DepositMoney { amount } => {
                // Invariant: Cannot deposit into a non-existent account
                if self.id.is_none() {
                    return Err(BankAccountError::AccountNotYetOpen);
                }
                // Invariant: Deposit must be positive
                if amount == 0 {
                    return Err(BankAccountError::InvalidDepositAmount);
                }
                Ok(vec![BankAccountEvent::MoneyDeposited { amount }])
            }
            BankAccountCommand::WithdrawMoney { amount } => {
                // Invariant: Cannot withdraw from a non-existent account
                if self.id.is_none() {
                    return Err(BankAccountError::AccountNotYetOpen);
                }
                // Invariant: Cannot withdraw more than the available balance
                if self.balance < amount {
                    return Err(BankAccountError::InsufficientFunds {
                        available: self.balance,
                        requested: amount,
                    });
                }
                Ok(vec![BankAccountEvent::MoneyWithdrawn { amount }])
            }
        }
    }
}
```

---

## Important Rules of Aggregates

When implementing your own aggregates, always follow these rules:

1. **Deterministic `apply`:** The `apply` method is run every time your aggregate state is reconstituted from history. It must be completely deterministic, have no side effects, and only mutate fields. Never perform validations or log actions inside `apply`.
2. **Stateless `handle`:** The `handle` method only reads from the reconstituted state to validate business rules and emit events. It must never mutate any fields of the aggregate directly. State mutation is deferred entirely to `apply`.
