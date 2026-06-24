---
title: 2.1. Add Commands
description: Define commands to represent user intents and transactions.
---

In this tutorial, we will build a complete, production-grade **Bank Account** aggregate from scratch using Event Sourcing. 

A Bank Account aggregate needs to support:
1. Opening a new account with an owner's name.
2. Depositing money to increase the account's balance.
3. Withdrawing money to decrease the account's balance (ensuring the account does not underflow).

---

## Defining Commands

A **Command** represents a user's intent or instruction to change state. Because commands represent an *action that should happen*, they are always named using the **present tense** (e.g., `OpenAccount`, `DepositMoney`).

In Rust, we represent commands as a simple enum. This enum encapsulates all possible actions that can be executed on our bank account, along with any parameters required for those actions.

Create your command enum like so:

```rust
// =========================================================================
// Define Commands (The Intent)
// =========================================================================
// Commands represent user intents or instructions. They can be rejected
// if they violate domain business rules.
pub enum BankAccountCommand {
    /// Open a brand-new bank account for an owner.
    OpenAccount {
        account_id: String,
        owner: String,
    },
    /// Deposit a positive sum of money into the account.
    DepositMoney {
        amount: u64,
    },
    /// Withdraw a positive sum of money from the account.
    WithdrawMoney {
        amount: u64,
    },
}
```

---

## Command Design Guidelines

When designing commands in your own applications, keep these best practices in mind:

1. **Be Specific:** Make commands highly specific to the business operation. Prefer `DepositMoney` over `UpdateBalance { delta: i64 }`.
2. **Include Relevant Payload:** Provide only the data required to validate and execute the operation. 
3. **No Aggregate ID in Payload (Optional):** In our framework, the aggregate ID is loaded and specified at the routing level (e.g., when calling `repo.execute(&id, command, ...)`). While some commands (like `OpenAccount`) might include the initial ID in their payload for initialization, subsequent commands (like `DepositMoney`) only need the operational parameters like `amount`.
