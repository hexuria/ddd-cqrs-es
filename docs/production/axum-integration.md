---
title: 5.5. Integration with Web Frameworks (Axum)
description: Build a production-grade, asynchronous REST API on top of your event-sourced domain using Axum.
---

To expose your event-sourced domain logic to the outside world, you can integrate it with any modern Rust asynchronous web framework.

In this guide, we will demonstrate how to build a production-grade, high-performance REST API using **Axum**. We will set up routes to open accounts, deposit money, withdraw money, and query account balances.

---

## 1. Designing the Shared Web State

In Axum, shared state is managed using an `AppState` struct wrapped in an `Arc`. Since our framework's `Repository` is thread-safe and designed to handle concurrent operations, you can easily share it across your web handlers:

```rust
use ddd_cqrs_es::{PostgresEventStore, Repository};
use std::sync::Arc;

pub struct AppState {
    /// Share our thread-safe aggregate repository
    pub repo: Repository<BankAccount, PostgresEventStore<BankAccount>>,
}
```

---

## 2. Defining Request Payloads

We define simple, serializable JSON payload structs representing the input arguments for each of our HTTP operations:

```rust
use serde::Deserialize;

#[derive(Deserialize)]
pub struct OpenAccountRequest {
    pub owner: String,
}

#[derive(Deserialize)]
pub struct DepositRequest {
    pub amount: u64,
}

#[derive(Deserialize)]
pub struct WithdrawRequest {
    pub amount: u64,
}
```

---

## 3. Creating Web Handlers

Our web handlers receive the shared `AppState`, parse incoming JSON payloads, extract URL path parameters, and invoke the repository's `execute` or `load` methods.

### Handling Commands (The Write Path)

```rust
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use ddd_cqrs_es::{Metadata, RepositoryError};

/// POST /accounts
/// Generates a new unique ID and executes the OpenAccount command.
pub async fn open_account(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<OpenAccountRequest>,
) -> Result<impl IntoResponse, AppError> {
    // Generate a unique ID (e.g., UUID)
    let account_id = uuid::Uuid::new_v4().to_string();

    state.repo.execute(
        &account_id,
        BankAccountCommand::OpenAccount {
            account_id: account_id.clone(),
            owner: payload.owner,
        },
        Metadata::new().with_actor_id("web-api"),
    )?;

    // Return the created ID with 201 Created status
    Ok((StatusCode::CREATED, Json(serde_json::json!({ "id": account_id }))))
}

/// POST /accounts/:id/deposits
pub async fn deposit_money(
    State(state): State<Arc<AppState>>,
    Path(account_id): Path<String>,
    Json(payload): Json<DepositRequest>,
) -> Result<impl IntoResponse, AppError> {
    state.repo.execute(
        &account_id,
        BankAccountCommand::DepositMoney { amount: payload.amount },
        Metadata::new().with_actor_id("web-api"),
    )?;

    Ok(StatusCode::OK)
}
```

---

## 4. Translating Errors to HTTP Status Codes

To prevent leakage of internal system details and provide high-fidelity API responses, we map our typed domain errors (`BankAccountError`) and repository errors (`RepositoryError`) to Axum HTTP responses:

```rust
use axum::response::Response;

pub enum AppError {
    /// Business rule validation failed (e.g., InsufficientFunds)
    Domain(BankAccountError),
    /// Optimistic Concurrency collision (someone else edited the stream first)
    Concurrency,
    /// Connection issues or internal database failures
    Internal(String),
}

// Convert our application errors into Axum's response type
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, error_code, message) = match self {
            AppError::Domain(BankAccountError::InsufficientFunds { available, requested }) => (
                StatusCode::BAD_REQUEST,
                "insufficient_funds",
                format!("Requested ${requested} but only have ${available} available."),
            ),
            AppError::Domain(BankAccountError::AccountAlreadyOpen) => (
                StatusCode::CONFLICT,
                "account_already_open",
                "This bank account has already been opened.".to_owned(),
            ),
            AppError::Domain(BankAccountError::AccountNotYetOpen) => (
                StatusCode::BAD_REQUEST,
                "account_not_open",
                "The requested account is not initialized yet.".to_owned(),
            ),
            AppError::Domain(BankAccountError::InvalidDepositAmount) => (
                StatusCode::BAD_REQUEST,
                "invalid_deposit_amount",
                "Deposit amounts must be positive numbers.".to_owned(),
            ),
            AppError::Concurrency => (
                StatusCode::CONFLICT,
                "concurrency_collision",
                "The stream was modified by another request. Please retry.".to_owned(),
            ),
            AppError::Internal(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_server_error",
                err,
            ),
        };

        let body = serde_json::json!({
            "error": error_code,
            "message": message,
        });

        (status, Json(body)).into_response()
    }
}

// Implement standard From traits for clean error propagation via '?' operator
impl From<RepositoryError<BankAccountError>> for AppError {
    fn from(err: RepositoryError<BankAccountError>) -> Self {
        match err {
            RepositoryError::Domain(e) => AppError::Domain(e),
            RepositoryError::Concurrency(_) => AppError::Concurrency,
            RepositoryError::Database(e) => AppError::Internal(e.to_string()),
            _ => AppError::Internal("Unknown repository error occurred.".to_owned()),
        }
    }
}
```

---

## 5. Assembling the Router

Finally, we configure the Axum router, inject our state, and run the server using `tokio`:

```rust
use axum::{routing::post, Router};
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Setup the PostgreSQL Event Store
    let dsn = "host=localhost port=5432 user=postgres dbname=app_events sslmode=disable";
    let store = PostgresEventStore::<BankAccount>::connect(dsn)?;
    store.initialize_schema()?;

    // 2. Initialize the thread-safe Repository
    let repo = Repository::new(store);
    let shared_state = Arc::new(AppState { repo });

    // 3. Configure the routes
    let app = Router::new()
        .route("/accounts", post(open_account))
        .route("/accounts/:id/deposits", post(deposit_money))
        // Share our application state with all route handlers
        .with_state(shared_state);

    // 4. Start the server
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    println!("Server running on http://{}", addr);
    
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
```

---

## 6. Real-World Architectural Advantages

By building your API around this architecture, you gain three massive production advantages:

1. **Lightweight Requests:** Endpoints do not hold heavy, database-level locking transactions. Command validation is executed inside in-memory loops, and writes are completed as simple, ultra-fast SQL appends.
2. **Horizontal Scaling:** Since servers are completely stateless and rely on Optimistic Concurrency Control, you can scale your application servers horizontally without worrying about complex distributed locking mechanisms.
3. **Idempotent Retry Safety:** If a client receives a `409 Conflict` (concurrency collision), the client or API gateway can simply and safely retry the request instantly without risk of corrupting database state.
