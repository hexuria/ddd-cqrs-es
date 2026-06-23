use ddd_cqrs_es::{Aggregate, CommandHandler, InMemoryEventStore, Repository};

#[derive(Clone, Debug, PartialEq, Eq)]
enum AccountEvent {
    Opened { account_id: String, owner: String },
    Deposited { amount: i64 },
    Withdrawn { amount: i64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum AccountCommand {
    Open { account_id: String, owner: String },
    Deposit { amount: i64 },
    Withdraw { amount: i64 },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Account {
    id: Option<String>,
    owner: Option<String>,
    balance: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum AccountError {
    AlreadyOpened,
    NotOpened,
    InvalidAmount,
    InsufficientFunds,
}

impl Aggregate for Account {
    type Event = AccountEvent;

    fn apply(&mut self, event: &Self::Event) {
        match event {
            AccountEvent::Opened { account_id, owner } => {
                self.id = Some(account_id.clone());
                self.owner = Some(owner.clone());
            }
            AccountEvent::Deposited { amount } => {
                self.balance += amount;
            }
            AccountEvent::Withdrawn { amount } => {
                self.balance -= amount;
            }
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
            AccountCommand::Withdraw { amount } => {
                if self.id.is_none() {
                    return Err(AccountError::NotOpened);
                }
                if amount <= 0 {
                    return Err(AccountError::InvalidAmount);
                }
                if self.balance < amount {
                    return Err(AccountError::InsufficientFunds);
                }

                Ok(vec![AccountEvent::Withdrawn { amount }])
            }
        }
    }
}

fn main() {
    let store = InMemoryEventStore::<AccountEvent>::new();
    let repo = Repository::new(store);
    let account_id = "account-1";

    repo.execute::<Account, _>(
        account_id,
        AccountCommand::Open {
            account_id: account_id.to_owned(),
            owner: "Uriah".to_owned(),
        },
    )
    .unwrap();

    repo.execute::<Account, _>(account_id, AccountCommand::Deposit { amount: 100 })
        .unwrap();

    repo.execute::<Account, _>(account_id, AccountCommand::Withdraw { amount: 35 })
        .unwrap();

    let loaded = repo.load::<Account>(account_id).unwrap();
    assert_eq!(loaded.state.balance, 65);
    assert_eq!(loaded.version, 3);

    println!("{:#?}", loaded);
}
