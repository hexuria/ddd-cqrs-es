---
title: 4.3. Putting It All Together
description: Assemble your write repository and read projection into a working test application.
---

Now, let's assemble all the components we have built in this tutorial—commands, events, the aggregate root, our in-memory store, and our dashboard view—into a single, working, executable Rust entry point.

---

## The Assembled Code

Here is the complete setup to initialize a test application, dispatch commands to the write path, and update and query the read model:

```rust
use ddd_cqrs_es::{
    Aggregate, DomainEvent, EventEnvelope, InMemoryEventStore, 
    InMemoryProjectionRunner, Metadata, Projection, Repository
};
use std::collections::HashMap;

// 1. Declare Domain Types...
// (Include BankAccountCommand, BankAccountEvent, BankAccountError, and BankAccount)

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // =========================================================================
    // 1. Initialize the Write Path (Command Engine)
    // =========================================================================
    // Create an in-memory event database
    let store = InMemoryEventStore::<BankAccount>::new();
    
    // Bind it to the orchestrating Repository
    let repo = Repository::new(store.clone());
    let account_id = "account-1".to_owned();

    // =========================================================================
    // 2. Dispatch Commands
    // =========================================================================
    println!("1. Dispatching OpenAccount command...");
    repo.execute(
        &account_id,
        BankAccountCommand::OpenAccount {
            account_id: account_id.clone(),
            owner: "Uriah".to_owned(),
        },
        Metadata::default(),
    )?;

    println!("2. Dispatching DepositMoney command...");
    repo.execute(
        &account_id,
        BankAccountCommand::DepositMoney { amount: 250 },
        Metadata::default(),
    )?;

    // =========================================================================
    // 3. Load & Reconstitute Write State (Verification)
    // =========================================================================
    println!("3. Loading reconstituted state from history...");
    let loaded = repo.load(&account_id)?;
    assert_eq!(loaded.state.balance(), 250);
    assert_eq!(loaded.revision, 2);
    
    println!("✓ Account balance successfully reconstituted: ${}", loaded.state.balance());

    // =========================================================================
    // 4. Run Read Projections (The Read Path)
    // =========================================================================
    println!("4. Initializing dashboard projection view...");
    let dashboard = AccountDashboard::default();
    
    // Create a sequential runner wrapping the dashboard view
    let mut runner = InMemoryProjectionRunner::new(dashboard);

    // Poll the event store and apply all new events committed since last checkpoint
    runner.run::<BankAccount, _>(&store)?;

    // Extract the updated read model
    let updated_dashboard = runner.into_inner();

    // Query and assert balances on our read view!
    let current_balance = updated_dashboard.get_balance(&account_id).unwrap_or(0);
    assert_eq!(current_balance, 250);

    println!("✓ Read Model updated! Dashboard reports account balance is: ${}", current_balance);
    Ok(())
}
```

---

## Running the Verification

To run this in your terminal, add it to your `src/main.rs` file or a local integration test binary and run:

```bash
cargo run
```

You should see the following console output confirming a clean, transactional command flow and asynchronous read model materialization:

```text
1. Dispatching OpenAccount command...
2. Dispatching DepositMoney command...
3. Loading reconstituted state from history...
✓ Account balance successfully reconstituted: $250
4. Initializing dashboard projection view...
✓ Read Model updated! Dashboard reports account balance is: $250
```

---

## Summary of the Tutorial

Congratulations! You have successfully built:
* An **aggregate root consistency boundary** representing a bank account.
* A **write-side pipeline** using an in-memory event ledger that reconstitutes state and enforces invariants.
* A **read-side projection** that updates customized materialized views from committed event streams.

Now, let's explore how we migrate this test setup into a **production application** using persistent SQL engines, checkpoint tracking, and metadata headers.
