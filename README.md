# ddd_cqrs_es

A lightweight, dependency-free Rust starter framework for Domain-Driven Design, CQRS, and Event Sourcing.

It gives you the core building blocks without forcing a database, serializer, async runtime, or web framework:

- `Aggregate`: event-sourced domain state
- `CommandHandler`: command decision logic
- `EventStore`: persistence abstraction
- `InMemoryEventStore`: test/local event store
- `Repository`: load, execute, and save aggregate events with optimistic concurrency
- `Projection`: read-model updater abstraction

## Install

Because this is a local crate starter, use it through a path dependency:

```toml
[dependencies]
ddd_cqrs_es = { path = "../ddd_cqrs_es" }
```

## Core flow

1. Define domain events.
2. Define commands.
3. Implement `Aggregate` for your domain object.
4. Implement `CommandHandler<Command>` for decision logic.
5. Use `Repository` to load state, handle commands, and append events.
6. Build projections from `EventEnvelope`s for query/read models.

## Example

```rust
use ddd_cqrs_es::{Aggregate, CommandHandler, InMemoryEventStore, Repository};

#[derive(Clone)]
enum AccountEvent {
    Opened { account_id: String, owner: String },
    Deposited { amount: i64 },
}

enum AccountCommand {
    Open { account_id: String, owner: String },
    Deposit { amount: i64 },
}

#[derive(Default)]
struct Account {
    id: Option<String>,
    balance: i64,
}

#[derive(Debug)]
enum AccountError {
    AlreadyOpened,
    NotOpened,
    InvalidAmount,
}

impl Aggregate for Account {
    type Event = AccountEvent;

    fn apply(&mut self, event: &Self::Event) {
        match event {
            AccountEvent::Opened { account_id, .. } => self.id = Some(account_id.clone()),
            AccountEvent::Deposited { amount } => self.balance += amount,
        }
    }
}

impl CommandHandler<AccountCommand> for Account {
    type Error = AccountError;

    fn handle(&self, command: AccountCommand) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            AccountCommand::Open { account_id, owner } => {
                if self.id.is_some() {
                    return Err(AccountError::AlreadyOpened);
                }
                Ok(vec![AccountEvent::Opened { account_id, owner }])
            }
            AccountCommand::Deposit { amount } => {
                if self.id.is_none() {
                    return Err(AccountError::NotOpened);
                }
                if amount <= 0 {
                    return Err(AccountError::InvalidAmount);
                }
                Ok(vec![AccountEvent::Deposited { amount }])
            }
        }
    }
}

fn main() {
    let store = InMemoryEventStore::<AccountEvent>::new();
    let repo = Repository::new(store);

    repo.execute::<Account, _>(
        "account-1",
        AccountCommand::Open {
            account_id: "account-1".to_string(),
            owner: "Uriah".to_string(),
        },
    ).unwrap();

    repo.execute::<Account, _>(
        "account-1",
        AccountCommand::Deposit { amount: 100 },
    ).unwrap();

    let account = repo.load::<Account>("account-1").unwrap();
    assert_eq!(account.state.balance, 100);
    assert_eq!(account.version, 2);
}
```

Run the full example:

```bash
cargo run --example bank_account
```

Run tests:

```bash
cargo test
```

## Design notes

- Revision `0` means the stream is empty.
- The first persisted event has revision `1`.
- `Repository` saves with `ExpectedRevision::Exact(version)` to enforce optimistic concurrency.
- Metadata is generic and defaults to `()`.
- The crate is synchronous and dependency-free by design. Add async traits, serialization, and a durable store adapter when integrating with your production stack.

## Recommended next production extensions

- `serde` integration for event and metadata serialization.
- Event type/version names for upcasting.
- Snapshot repository for long streams.
- Outbox/subscription runner for projections.
- Postgres, DynamoDB, Kafka, or EventStoreDB implementation of `EventStore`.
- Tracing/correlation metadata.
- Idempotency keys for command handlers.
