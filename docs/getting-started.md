# Getting Started

Implement an aggregate by defining commands, events, state, and domain errors.

```rust
use ddd_cqrs_es::{Aggregate, DomainEvent, InMemoryEventStore, Metadata, Repository};

#[derive(Clone)]
enum AccountEvent {
    Opened { account_id: String },
}

impl DomainEvent for AccountEvent {
    fn event_type(&self) -> &'static str {
        "account_opened"
    }
}

enum AccountCommand {
    Open { account_id: String },
}

#[derive(Default)]
struct Account {
    id: Option<String>,
    revision: u64,
}

#[derive(Debug)]
enum AccountError {
    AlreadyOpen,
}

impl Aggregate for Account {
    type Id = String;
    type Command = AccountCommand;
    type Event = AccountEvent;
    type Error = AccountError;

    fn aggregate_type() -> &'static str { "account" }
    fn id(&self) -> Option<&Self::Id> { self.id.as_ref() }
    fn revision(&self) -> u64 { self.revision }
    fn new() -> Self { Self::default() }

    fn apply(&mut self, event: &Self::Event) {
        match event {
            AccountEvent::Opened { account_id } => self.id = Some(account_id.clone()),
        }
        self.revision += 1;
    }

    fn handle(&self, command: Self::Command) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            AccountCommand::Open { .. } if self.id.is_some() => Err(AccountError::AlreadyOpen),
            AccountCommand::Open { account_id } => Ok(vec![AccountEvent::Opened { account_id }]),
        }
    }
}

let store = InMemoryEventStore::<Account>::new();
let repo = Repository::new(store);
let account_id = "account-1".to_owned();

repo.execute(
    &account_id,
    AccountCommand::Open { account_id: account_id.clone() },
    Metadata::default(),
)?;

let loaded = repo.load(&account_id)?;
assert_eq!(loaded.revision, 1);
# Ok::<(), ddd_cqrs_es::RepositoryError<AccountError>>(())
```

Run the complete example:

```bash
cargo run --example bank_account
```
