# Payment Ledger Lab: Account Transfers and Reconciliation

[简体中文](README.md)

## Current Status and Local Run

The repository currently contains a runnable Spring Boot application and local PostgreSQL/Kafka configuration. Transfers, the ledger, message consumption, and reconciliation are planned below but have not been implemented.

Requires Java 17, Maven, and a running Docker daemon. From this directory, run:

```sh
docker compose up -d --wait
SPRING_PROFILES_ACTIVE=local mvn spring-boot:run
```

Check `http://localhost:8080/actuator/health` in another terminal. Run `docker compose down` to stop the dependencies; the PostgreSQL volume is retained. The password in the `local` profile is only for local development. Other environments must provide `DB_URL`, `DB_USER`, `DB_PASSWORD`, and `KAFKA_BOOTSTRAP_SERVERS`. Flyway is configured; the first business-table migration will be added with the account feature.

## Proposal

A simulated payment backend for learning and job applications. It is intended to demonstrate three properties: concurrent transfers, including FX, cannot corrupt balances; retried requests and messages cannot duplicate business effects; and every movement of funds can be traced through the ledger and checked by end-of-day reconciliation.

The project does not connect to a real bank or payment network and does not handle real customer data or money.

## Goals and Scope

**Core flow:** Create zero-balance accounts → prepare balances with development-only seed data → obtain an FX quote for a cross-currency transfer → submit a transfer → inspect balances and ledger entries → create an asynchronous notification through Kafka → run end-of-day reconciliation and inspect discrepancies.

The project simulates **transfers and FX conversion between accounts in one system**. The first release supports HKD, USD, and JPY, using preconfigured demonstration rates rather than live market data. Interbank settlement, FPS integration, external FX execution, spreads and fees, loans, KYC/AML decisions, and real email/SMS delivery are out of scope. Reconciliation initially checks this system's balance snapshots, ledger, and transfer records; it does not claim to reconcile with an external clearing institution.

### Required Business Rules

1. Each account holds one currency. Amounts are stored as integers in that currency's smallest unit: HKD 1.00 is `100`, while JPY 1 is `1`. Transfers require a positive amount and two distinct accounts.
2. Same-currency transfers need no quote. A cross-currency transfer requires a time-limited, single-use FX quote tied to the source account, destination account, and source amount. The quote fixes a positive rate and destination amount. The rate means "target-currency major units per one source-currency major unit." An expired or used quote cannot fund a new transfer; an identical idempotent retry still returns its original result.
3. Calculate the destination amount with `BigDecimal`, rounding once to the target currency's smallest unit using `HALF_EVEN`. Reject a quote whose rounded destination amount is zero. A successful transfer records its quote, rate, source amount, and destination amount; later rate changes cannot alter the booked amounts.
4. A successful transfer commits the debit, credit, immutable ledger entries, transfer status, quote usage state, and Outbox event in **one database transaction**. Same-currency transfers have two entries. Cross-currency transfers use internal FX position accounts and have four entries; entries must sum to zero **within each currency**, never across currencies.
5. Customer accounts cannot have negative balances. Internal FX position accounts may be negative to represent simulated positions. Each account balance snapshot must equal the sum of that account's ledger entries in its own currency.
6. Transfer requests include an `Idempotency-Key`. The same caller retrying the same request with the same key receives the same result; reusing the key for a different request is rejected.
7. A failed transfer leaves no partial debit or ledger entries and does not consume its quote. A failed notification does not reverse a completed transfer.
8. Ledger entries are append-only. Important operations retain timestamps, related transaction IDs, and audit information.

For example, converting HKD 100.00 at `1 HKD = 0.13 USD` credits USD 13.00: the customer HKD account records `-10000`, the internal HKD position account `+10000`, the internal USD position account `-1300`, and the customer USD account `+1300`. HKD and USD each balance independently; `10000` and `1300` must not be added together.

## Proposed Stack

| Purpose | Choice | Learning focus |
| --- | --- | --- |
| Backend | Java 17, Spring Boot, Maven | Project setup, layering, validation, error handling |
| Data | PostgreSQL, Flyway | Transactions, row locks, unique constraints, migrations |
| Messaging | Kafka, Spring for Apache Kafka | Outbox, at-least-once delivery, duplicate consumption, retries, dead letters |
| Local runtime | Docker Compose | Start local dependencies consistently |
| Tests | JUnit 5, Spring Boot Test, Testcontainers | Concurrency and failure tests against real PostgreSQL/Kafka |

The first release remains one Spring Boot application, organized around transfers, ledger, messaging, and reconciliation. Kafka carries events after funds have been booked; **booking funds does not depend on successful Kafka consumption**. Redis, Kubernetes, and splitting into microservices are deferred because they are unnecessary to validate the core behavior.

### Why PostgreSQL?

MySQL can also implement this project using transactions, row locks, and unique constraints; correctness does not come from the database brand. PostgreSQL was chosen because `CHECK` constraints, partial indexes (such as an index over unpublished Outbox events), and `INSERT ... ON CONFLICT ... RETURNING` express this project's data rules and idempotent writes directly. If a target team uses MySQL, the business design can remain, but the SQL and concurrency behavior under its transaction isolation settings must be tested again.

## System Design

```text
Client --HTTP--> Spring Boot API --transaction--> PostgreSQL
                      |                             |-- accounts / fx_quotes / transfers / ledger_entries
                      |                             `-- outbox_events
                      `-- Outbox publisher --> Kafka --> Notification consumer --> notifications

End-of-day reconciliation --> compare accounts, transfers, ledger_entries --> discrepancy report
```

### Core Data

- `accounts`: Account, currency, balance snapshot, status, and customer/system type. New accounts start at zero.
- `fx_quotes`: Source and destination accounts, source and destination amounts, rate, expiry, and usage state; generated from preconfigured demonstration rates.
- `transfers`: Both accounts, currencies and amounts, quote reference, status, caller, idempotency key, request hash, and timestamps.
- `ledger_entries`: Accounting transaction (opening or transfer), account, currency, and signed amount; two for a same-currency transfer, four for a cross-currency transfer.
- `outbox_events`: Events committed with transfers and their publishing state.
- `processed_events` and `notifications`: Consumer deduplication and simulated notification results.
- `reconciliation_runs`: Run date, result, and discrepancies.

A development-only seed script creates balanced opening entries between system test accounts and customer accounts in each currency; it never changes balances without ledger entries. It must not be used in production.

### Concurrency and Idempotency

For a cross-currency transfer, lock and validate the quote first, then lock all involved customer and system account rows in a consistent account-ID order. A same-currency transfer only locks its two accounts in account-ID order. Check available source funds before updating balances and ledger entries. This prevents concurrent overspending and quote reuse while reducing deadlock risk. A database unique constraint protects the caller/idempotency-key pair; the application compares the request hash to distinguish an identical retry from a conflict. Integration tests must prove concurrency behavior.

### Message Reliability

The Outbox publisher sends events after the transaction commits and marks them published only after Kafka acknowledges them. A crash between those steps may produce a duplicate, so the notification consumer deduplicates by event ID: the **business effect is idempotent**, without assuming end-to-end exactly-once delivery. Transient failures are retried; events exceeding the retry limit go to a dead-letter topic for investigation. Notifications are stored as simulated records; no real messages are sent.

### End-of-Day Reconciliation

At the end of the day, inspect current accounting state: each successful transfer must have two same-currency or four cross-currency entries, balanced **per currency**; the source and destination amounts of an FX transfer must match its booked quote; failed transfers have no booked entries; each account balance snapshot must match the sum of its own-currency ledger entries. Retain the business date and result of each run, including missing entries, per-currency imbalances, quote/amount mismatches, and balance discrepancies. Historical balance reconstruction and reconciliation against an independent external statement are later extensions.

## Initial API Draft

| Method and path | Purpose |
| --- | --- |
| `POST /accounts` | Create a zero-balance account |
| `GET /accounts/{id}` | Read account and balance |
| `GET /accounts/{id}/ledger` | Page through ledger entries |
| `POST /fx/quotes` | Create a time-limited quote for a cross-currency transfer; return rate, destination amount, and quote ID |
| `POST /transfers` | Submit a same-currency or cross-currency transfer; include `Idempotency-Key` and, for FX, a quote ID |
| `GET /transfers/{id}` | Read transfer result |
| `POST /reconciliations` | Let an administrator reconcile current accounting state |
| `GET /reconciliations/{id}` | Read a reconciliation report |

The API needs input validation, clear error codes, basic authentication, and account-level access control. Administrator operations are separate from customer operations. Logs must omit credentials and complete sensitive data; secrets must not be committed to the repository.

## 12-Week Delivery Plan

| Weeks | Deliverable |
| --- | --- |
| 1-2 | Spring Boot project, Docker Compose, database migrations, accounts, and seed data |
| 3-4 | Same-currency transfer API, double-entry ledger, transactions, and basic integration tests |
| 5-6 | FX quotes, cross-currency entries, idempotency, row locking, concurrency and rollback tests |
| 7-8 | Kafka, Outbox publisher, event consumer, and deduplication |
| 9-10 | Retries, dead-letter handling, simulated notifications, and end-of-day reconciliation |
| 11-12 | Access control, audit/logging, complete tests, run instructions, and project demo |

Set aside about six hours per week for development, two for technical English, and one or two for Java/SQL interview preparation. English practice should cover a project walkthrough, concurrency decisions, and a messaging failure. Job applications can run alongside development.

## Acceptance Criteria

- A fresh environment can start the service and dependencies using the documented commands and pass the tests. Add actual API examples when the endpoints are implemented.
- Concurrent requests against one source account cannot produce a negative balance; the ledger matches balances; concurrent retries with one idempotency key create one transfer.
- Same-currency and cross-currency transfers across HKD, USD, and JPY book in each currency's smallest unit. Tests cover expired and reused quotes, rounding boundaries, and balancing within each currency.
- A completed transfer remains correct while Kafka is unavailable or an event is redelivered; after recovery, the notification has only one business effect.
- Reconciliation tests detect deliberately missing entries, per-currency imbalances, quote/amount mismatches, and balance discrepancies.
- The repository explains its architecture, key tradeoffs, test commands, and one failure demonstration for technical interviews.

## References

- [Apache Fineract](https://github.com/apache/fineract): Banking domain modeling and tests; do not copy its large architecture.
- [Apache Kafka documentation](https://kafka.apache.org/documentation/): Delivery, consumption, partitions, and recovery.
- [HKMA Faster Payment System](https://www.hkma.gov.hk/eng/smart-consumers/faster-payment-system/): Hong Kong payment context; this project does not connect to FPS.
- [HKMA Technology Risk Management guideline TM-G-1](https://brdr.hkma.gov.hk/eng/doc-ldg/spm/current/TM-G-1): Financial-system controls, auditability, and recoverability.
