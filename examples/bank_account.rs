use ddd_cqrs_es::{Aggregate, DomainEvent, InMemoryEventStore, Metadata, Repository};

#[derive(Clone, Debug, PartialEq, Eq)]
enum AccountEvent {
    AccountOpened {
        account_id: String,
        owner_name: String,
    },
    MoneyDeposited {
        amount: i64,
    },
    MoneyWithdrawn {
        amount: i64,
    },
    AccountClosed,
}

impl DomainEvent for AccountEvent {
    fn event_type(&self) -> &'static str {
        match self {
            AccountEvent::AccountOpened { .. } => "account_opened",
            AccountEvent::MoneyDeposited { .. } => "money_deposited",
            AccountEvent::MoneyWithdrawn { .. } => "money_withdrawn",
            AccountEvent::AccountClosed => "account_closed",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum AccountCommand {
    OpenAccount {
        account_id: String,
        owner_name: String,
    },
    DepositMoney {
        amount: i64,
    },
    WithdrawMoney {
        amount: i64,
    },
    CloseAccount,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Account {
    id: Option<String>,
    owner_name: Option<String>,
    balance: i64,
    is_open: bool,
    revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum AccountError {
    AlreadyOpen,
    NotOpen,
    InvalidAmount,
    InsufficientFunds,
    AlreadyClosed,
}

impl Aggregate for Account {
    type Id = String;
    type Command = AccountCommand;
    type Event = AccountEvent;
    type Error = AccountError;

    fn aggregate_type() -> &'static str {
        "bank_account"
    }

    fn revision(&self) -> u64 {
        self.revision
    }

    fn apply(&mut self, event: &Self::Event) {
        match event {
            AccountEvent::AccountOpened {
                account_id,
                owner_name,
            } => {
                self.id = Some(account_id.clone());
                self.owner_name = Some(owner_name.clone());
                self.is_open = true;
            }
            AccountEvent::MoneyDeposited { amount } => {
                self.balance += amount;
            }
            AccountEvent::MoneyWithdrawn { amount } => {
                self.balance -= amount;
            }
            AccountEvent::AccountClosed => {
                self.is_open = false;
            }
        }

        self.revision += 1;
    }

    fn handle(&self, command: Self::Command) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            AccountCommand::OpenAccount {
                account_id,
                owner_name,
            } => {
                if self.is_open {
                    return Err(AccountError::AlreadyOpen);
                }

                Ok(vec![AccountEvent::AccountOpened {
                    account_id,
                    owner_name,
                }])
            }
            AccountCommand::DepositMoney { amount } => {
                if !self.is_open {
                    return Err(AccountError::NotOpen);
                }
                if amount <= 0 {
                    return Err(AccountError::InvalidAmount);
                }

                Ok(vec![AccountEvent::MoneyDeposited { amount }])
            }
            AccountCommand::WithdrawMoney { amount } => {
                if !self.is_open {
                    return Err(AccountError::NotOpen);
                }
                if amount <= 0 {
                    return Err(AccountError::InvalidAmount);
                }
                if self.balance < amount {
                    return Err(AccountError::InsufficientFunds);
                }

                Ok(vec![AccountEvent::MoneyWithdrawn { amount }])
            }
            AccountCommand::CloseAccount => {
                if self.id.is_some() && !self.is_open {
                    return Err(AccountError::AlreadyClosed);
                }
                if self.id.is_none() {
                    return Err(AccountError::NotOpen);
                }

                Ok(vec![AccountEvent::AccountClosed])
            }
        }
    }

    fn new() -> Self {
        Self::default()
    }
}

fn main() {
    let store = InMemoryEventStore::<Account>::new();
    let repo = Repository::new(store);
    let account_id = "account-1".to_owned();
    let metadata = Metadata::new().with_actor_id("example-user");

    repo.execute(
        &account_id,
        AccountCommand::OpenAccount {
            account_id: account_id.clone(),
            owner_name: "Uriah".to_owned(),
        },
        metadata.clone(),
    )
    .unwrap();

    repo.execute(
        &account_id,
        AccountCommand::DepositMoney { amount: 100 },
        metadata.clone(),
    )
    .unwrap();

    repo.execute(
        &account_id,
        AccountCommand::WithdrawMoney { amount: 35 },
        metadata.clone(),
    )
    .unwrap();

    repo.execute(&account_id, AccountCommand::CloseAccount, metadata)
        .unwrap();

    let loaded = repo.load(&account_id).unwrap();
    assert_eq!(loaded.state.balance, 65);
    assert_eq!(loaded.revision, 4);
    assert!(!loaded.state.is_open);

    println!("{:#?}", loaded);
}
