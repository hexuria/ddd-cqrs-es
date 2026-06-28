---
title: 5.4. Including Metadata
description: Attach correlation, causation, actor, and tenancy tracking headers to your event ledger.
---

In enterprise software systems, storing only raw domain events (like `MoneyDeposited { amount: 250 }`) is insufficient. To maintain security, auditability, and traceability across microservice boundaries, we need to know:
* **Who** initiated this transaction? (Actor ID)
* **Which** specific HTTP request caused this event? (Request ID)
* **How** can we trace this transaction through our asynchronous background projections? (Correlation and Causation IDs)
* **What** tenant does this customer belong to in our SaaS environment? (Tenant ID)

To support this without cluttering our domain-specific events, our framework wraps all committed events inside an envelope that carries extensible, structural **Metadata**.

---

## The `Metadata` Structure

Our framework provides a built-in `Metadata` struct. It is fully serialized into your SQLite or PostgreSQL tables alongside your event payload.

```rust
pub struct Metadata {
    /// Correlates all work belonging to the same business request.
    pub correlation_id: Option<String>,
    /// Identifies the command or event that caused this event.
    pub causation_id: Option<String>,
    /// Identifies the user, service, or process that initiated the change.
    pub actor_id: Option<String>,
    /// Identifies the tenant when applications use multi-tenancy.
    pub tenant_id: Option<String>,
    /// Identifies the external request that initiated the change.
    pub request_id: Option<String>,
    /// Additional adapter or application-specific metadata.
    pub headers: BTreeMap<String, String>,
}
```

---

## Attaching Metadata to Command Executions

Our `Metadata` struct implements a clean, fluent builder pattern. You can attach correlation headers inside your web router or application handler and pass them directly to the repository's `execute` method:

```rust
use ddd_cqrs_es::{Metadata, Repository};

fn handle_web_request(
    repo: &Repository<BankAccount, PostgresEventStore<BankAccount>>,
    account_id: &str,
    deposit_amount: u64,
    current_user_id: &str,
    correlation_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    
    // 1. Construct audit tracking metadata
    let metadata = Metadata::new()
        .with_actor_id(current_user_id)
        .with_correlation_id(correlation_id)
        .with_tenant_id("tenant-north-america")
        .with_header("client-ip", "192.168.1.45"); // Custom ad-hoc header

    // 2. Dispatch command with metadata
    repo.execute(
        account_id,
        BankAccountCommand::DepositMoney { amount: deposit_amount },
        metadata,
    )?;

    Ok(())
}
```

---

## The Power of Causality Tracking

By utilizing correlation and causation IDs, you can construct extremely powerful diagnostics:
* **Audit Trails:** Show customer support exactly which administrator authorized a refund.
* **Causality Trees:** Trace an asynchronous chain of event reactions (e.g., Command A -> Event B -> Process Manager C -> Command D -> Event E) back to the single root request that triggered them.
* **Performance Tracing:** Correlate telemetry metrics and log statements across write databases, event stores, and background read projections.
