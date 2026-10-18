<div align="center">

# CommerceFlow

### An event-driven order workflow in TypeScript, built to show how independent services stay consistent when the network, the broker and the database fail

**Node.js · TypeScript · RabbitMQ · PostgreSQL · Express · Zod · Docker · npm workspaces**

[![CI](https://github.com/kris7011/commerce-flow/actions/workflows/ci.yml/badge.svg)](https://github.com/kris7011/commerce-flow/actions/workflows/ci.yml) · [Release notes](docs/releases/v1.0.0.md) · [Changelog](CHANGELOG.md)

[Quick start](#getting-started) ·
[Overview](#overview) ·
[Architecture](#architecture) ·
[Reliability](#reliability-and-delivery-guarantees) ·
[Getting started](#getting-started) ·
[Testing](#testing) ·
[Decisions](#architectural-decisions-and-trade-offs) ·
[Limitations](#known-limitations)

</div>

---

## Overview

CommerceFlow models a small commerce workflow — order, payment, inventory, delivery and customer notification — as five independently running services that collaborate only through events on RabbitMQ.

The business logic is intentionally simple. The engineering focus is on the parts that are hard in real distributed systems:

* **Atomic state changes and events.** Inventory Service writes its stock change and the resulting event in one PostgreSQL transaction (transactional outbox).
* **Reliable publication.** Events are published as mandatory messages with RabbitMQ publisher confirms. A publication counts as successful only if the broker confirmed it *and* routed it to a queue; only then is an outbox event marked as published.
* **Duplicate tolerance.** Delivery is at-least-once. Every consumer is idempotent, and stock can never be reserved twice for the same order.
* **Failure handling.** Transient failures are retried with bounded backoff, poison messages are dead-lettered, and services recover from broker and database outages without restarting.
* **Tests that prove failure behaviour.** Integration and end-to-end tests run against real PostgreSQL and RabbitMQ and cover crashes, outages, duplicates and concurrency.

> CommerceFlow is a portfolio and learning project. It demonstrates production techniques but is **not** production-ready, and it does **not** claim exactly-once delivery. See [Known limitations](#known-limitations).

---

## Architecture

```mermaid
flowchart LR
    client([HTTP client])

    subgraph services[Services]
        order[Order Service<br/>:3001]
        payment[Payment Service<br/>:3002]
        inventory[Inventory Service<br/>:3003]
        delivery[Delivery Service<br/>:3004]
        notification[Notification Service<br/>:3005]
    end

    exchange{{"commerce.events<br/>topic exchange"}}
    db[(PostgreSQL<br/>stock · reservations · outbox)]

    client -- "POST /orders" --> order
    order -- order.created --> exchange
    exchange -- order.created --> payment
    payment -- payment.authorized --> exchange
    exchange -- payment.authorized --> inventory
    inventory <--> db
    inventory -- "inventory.reserved<br/>inventory.reservation.failed<br/>(via outbox relay)" --> exchange
    exchange -- inventory.reserved --> delivery
    delivery -- delivery.booked --> exchange
    exchange -- "delivery.booked<br/>inventory.reservation.failed" --> notification
```

| Service | Responsibility | State | Consumes | Publishes |
|---|---|---|---|---|
| **Order** | Validates `POST /orders`, creates the order id and correlation id | none | – | `OrderCreated` |
| **Payment** | Authorizes the payment (simulated) | none | `OrderCreated` | `PaymentAuthorized` |
| **Inventory** | Reserves stock exactly once per order | **PostgreSQL** | `PaymentAuthorized` | `InventoryReserved` / `InventoryReservationFailed` (outbox) |
| **Delivery** | Books a delivery (simulated) | none | `InventoryReserved` | `DeliveryBooked` |
| **Notification** | Creates one customer notification per outcome | in-memory | `DeliveryBooked`, `InventoryReservationFailed` | – |

Inventory is the only service that changes durable business state as a reaction to an event, so it is the one that needs a transactional outbox. The other consumers are stateless transformers. See [why they don't need an outbox](#why-only-inventory-has-an-outbox).

### Repository layout

```text
services/
  order-service/          HTTP API → OrderCreated
  payment-service/        OrderCreated → PaymentAuthorized
  inventory-service/      PaymentAuthorized → stock + outbox → relay
    src/inventoryService.ts          business rules (no infrastructure)
    src/inventoryStore.ts            persistence ports (unit of work)
    src/postgresInventoryStore.ts    PostgreSQL adapter
    src/inMemoryInventoryStore.ts    in-memory adapter for unit tests
    src/migrations.ts                versioned schema migrations
    src/outbox/                      outbox store, relay and publisher
  delivery-service/       InventoryReserved → DeliveryBooked
  notification-service/   customer notifications
shared/
  contracts/   event types, zod schemas, routing keys, deterministic ids
  messaging/   RabbitMQ client (confirms, retries, DLQ) and supervisor
  logging/     structured JSON logger
  runtime/     validated configuration and graceful service host
  testing/     isolated test databases with safety guards
tests/e2e/     end-to-end tests against isolated vhosts and databases
tools/cli/     developer tools: order-flow demo, status view, log trace
docs/          architecture and reliability deep dives
```

Each service separates three layers:

* **Business logic** (`*Service.ts`) is plain TypeScript with no Express, RabbitMQ or PostgreSQL imports.
* **Adapters** sit behind small ports: message handlers, HTTP apps, stores and publishers.
* **The composition root** (`index.ts`) is the only place that wires concrete infrastructure together.

---

## Event workflow

### Successful order

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant O as Order
    participant X as RabbitMQ
    participant P as Payment
    participant I as Inventory
    participant DB as PostgreSQL
    participant R as Outbox relay
    participant D as Delivery
    participant N as Notification

    C->>O: POST /orders
    O->>X: OrderCreated (await confirm)
    O-->>C: 201 Created
    X->>P: OrderCreated
    P->>X: PaymentAuthorized (await confirm)
    P-->>X: ack
    X->>I: PaymentAuthorized
    rect rgb(235, 245, 255)
    note over I,DB: one transaction
    I->>DB: lock order · lock stock · decrement
    I->>DB: insert reservation + outbox event
    I->>DB: COMMIT
    end
    I-->>X: ack
    R->>DB: claim due events (SKIP LOCKED, lease)
    R->>X: InventoryReserved (await confirm)
    R->>DB: mark published
    X->>D: InventoryReserved
    D->>X: DeliveryBooked (await confirm)
    D-->>X: ack
    X->>N: DeliveryBooked
    N-->>X: ack
```

If any product is unavailable, the transaction stores an `InventoryReservationFailed` event instead, without touching stock. Notification Service then turns that event into a failure notification.

### Events and routing

All events share one envelope, defined in `shared/contracts`:

```ts
interface DomainEvent<TEventType, TData> {
    eventId: string;        // unique id, also the AMQP message-id
    eventType: TEventType;  // also the AMQP "type" property
    occurredAt: string;     // ISO-8601
    correlationId: string;  // follows the whole workflow
    data: TData;
}
```

| Event | Routing key | Producer | Consumer queue |
|---|---|---|---|
| `OrderCreated` | `order.created` | Order | `payment-service.order-created` |
| `PaymentAuthorized` | `payment.authorized` | Payment | `inventory-service.payment-authorized` |
| `InventoryReserved` | `inventory.reserved` | Inventory (outbox) | `delivery-service.inventory-reserved` |
| `InventoryReservationFailed` | `inventory.reservation.failed` | Inventory (outbox) | `notification-service.customer-events` |
| `DeliveryBooked` | `delivery.booked` | Delivery | `notification-service.customer-events` |

The routing keys live in one map (`ROUTING_KEYS`), and the queues bound to them in another (`QUEUES` / `COMMERCE_TOPOLOGY`). Every service declares this **whole topology** idempotently each time it connects, before it publishes anything. An event therefore always has a durable queue to wait in, whichever service starts first; a contract test asserts that every routing key is bound to at least one queue. Every queue is durable and has a dead-letter queue named `<queue>.dead-letter`, bound to the `commerce.events.dead-letter` exchange. Messages are persistent and carry `messageId`, `correlationId` and `type`.

Consumers validate every message against a **zod schema** before handling it. The schemas follow the tolerant-reader principle: they ignore unknown fields, so producers can add optional fields without a coordinated deployment. A breaking change requires a new event type.

Order Service accepts an `x-correlation-id` header (up to 128 safe characters, otherwise a new id is generated) and echoes it in the response. Every log line about an order carries this correlation id.

---

## Reliability and delivery guarantees

The full failure analysis lives in [docs/reliability.md](docs/reliability.md). In summary:

### Transactional outbox

The Inventory Service handler runs a single PostgreSQL transaction:

1. `pg_advisory_xact_lock(order)` serialises every delivery for the same order, and the transaction reads any recorded outcome.
2. The stock rows are locked `FOR UPDATE` in a fixed order, which prevents deadlocks.
3. The transaction either decrements stock or decides that the reservation fails.
4. It inserts the outcome into `inventory_reservations` (primary key `order_id`).
5. It inserts the event into `inventory_outbox`.

Either all of this commits, or nothing does. A crash or outage after the commit cannot lose the event, because the event is in the same database as the reservation.

The **outbox relay** is a background task in the same process. Any number of relays can run concurrently.

```mermaid
stateDiagram-v2
    [*] --> Pending: committed with the business change
    Pending --> Claimed: claim (FOR UPDATE SKIP LOCKED,<br/>available_at = now + lease, attempts + 1)
    Claimed --> Published: broker confirm → mark published
    Claimed --> Pending: publish failed → available_at = now + backoff
    Claimed --> Pending: relay crashed → lease expires
    Published --> [*]
```

* **Claiming** is a single short statement. No transaction is held open while the relay talks to RabbitMQ.
* **Publishing** waits for broker confirms. A row is marked published only after the confirm arrives.
* **Retries** use capped exponential backoff with jitter: 1 s, 2 s, 4 s … up to 60 s.
* **Fencing:** `attempts` is incremented on each claim and must match when a relay reschedules its claim. A relay whose lease expired therefore cannot overwrite a newer claim.
* **Shutdown:** the relay finishes the batch in progress before it stops.

### Delivery semantics: at-least-once

| Mechanism | Guarantees |
|---|---|
| Publisher confirms | Publication counts as successful only once the broker has accepted the message |
| Mandatory publishing + `basic.return` correlation | A message the broker accepted but could not route to any queue counts as **failed**, even though RabbitMQ still sends a positive confirm for it |
| Persistent messages, durable queues | Accepted messages survive a broker restart |
| Manual ack after the handler completes | A crash during handling causes redelivery, never loss |
| Outbox | A committed reservation always gets its event published |

The cost of these guarantees is **duplicates**: a crash between "publish" and "ack" or "mark published" causes the message to be sent again. CommerceFlow absorbs duplicates at every consumer:

| Consumer | Duplicate protection |
|---|---|
| Inventory | **Durable.** The business key `order_id` is serialised by an advisory lock and backed by the primary key. It also catches logically duplicate events that carry a *different* event id. |
| Payment, Delivery | The emitted event is a pure function of the source event: ids are derived deterministically (UUIDv5 layout) and timestamps and dates come from the source event, so reprocessing — even on a later date — re-emits a *byte-identical* event. One event id never stands for two different outcomes. |
| Notification | Deduplicates on the business key (order id, notification type). |
| All | A bounded in-memory cache of recent event ids skips obvious redeliveries cheaply. It is an optimisation only. |

### Retries, poison messages and recovery

* **Handler failures** (for example a short database outage) are retried in-process with exponential backoff: 5 attempts in total, starting at 0.5 s and doubling (capped at 8 s). After that, the message is dead-lettered.
* **Malformed messages** (invalid JSON or a contract violation), and handlers that throw `PermanentMessageError`, are dead-lettered immediately without retries.
* **Prefetch** (default 10) limits how many unacknowledged deliveries each consumer holds.
* **Broker outage:** services stay alive and report `/ready` as `503`. The supervisor reconnects and re-declares the topology and subscriptions. Order Service answers `503` with `Retry-After`. The outbox keeps events pending and retries them.
* **Database outage:** the pool fails fast (5 s connect timeout, 15 s statement timeout). Handler retries and the relay's backoff bridge short outages, and the process survives terminated connections.
* **Shutdown** (SIGTERM or SIGINT): the service stops accepting HTTP requests, stops consuming, drains in-flight handlers, lets the relay finish its batch, and only then closes its connections. If this exceeds the timeout, the process exits forcibly.

---

## Getting started

### Prerequisites

* Node.js **22 or 24** (LTS)
* Docker with Docker Compose

### Quick start: the whole system and a scripted demo

```bash
git clone https://github.com/kris7011/commerce-flow.git
cd commerce-flow
npm ci
npm run stack:up   # RabbitMQ, PostgreSQL and all five services as containers
npm run demo       # orders, a duplicate event and an invalid request, with checks
npm run status     # readiness, stock, outbox, queue and dead-letter depths
npm run demo:recovery  # PostgreSQL outage → dead letter → replay → order completes
```

The demo prints the events of each order as they cross the exchange, and checks the results: stock, notifications, outbox and dead-letter queues. It exits non-zero if anything is wrong, and CI runs it against the containers on every push. [docs/demo.md](docs/demo.md) explains each step, and `npm run trace` follows one order through the logs of all services.

### Install and start the infrastructure (services from source)

```bash
git clone https://github.com/kris7011/commerce-flow.git
cd commerce-flow
npm ci
docker compose up -d --wait rabbitmq postgres
```

The ports are published on `127.0.0.1` only. The containers use **development-only** credentials, which you can override (see [`.env.example`](.env.example)).

### Run the services

Each service runs in its own terminal. Inventory Service applies its database migrations on startup.

```bash
npm run dev:notification
npm run dev:delivery
npm run dev:inventory
npm run dev:payment
npm run dev:order
```

### Try the workflow

```bash
# Initial stock
curl http://localhost:3003/stock

# A successful order
curl -X POST http://localhost:3001/orders \
  -H "content-type: application/json" \
  -H "x-correlation-id: demo-001" \
  -d '{"customerId":"customer-1","items":[{"productId":"washing-machine-01","quantity":1,"unitPrice":4999}]}'

# An order that cannot be fulfilled
curl -X POST http://localhost:3001/orders \
  -H "content-type: application/json" \
  -d '{"customerId":"customer-2","items":[{"productId":"dryer-01","quantity":99,"unitPrice":2999}]}'

curl http://localhost:3005/notifications
curl http://localhost:3003/diagnostics/outbox
```

The demo catalogue contains `washing-machine-01` (10), `dishwasher-01` (5) and `dryer-01` (3). Seeding never overwrites existing quantities.

### HTTP endpoints

| Service | Endpoint | Purpose |
|---|---|---|
| all | `GET /health` | Liveness: the process is running |
| all | `GET /ready` | Readiness: RabbitMQ (and, for Inventory, PostgreSQL) is usable; returns `503` otherwise |
| Order | `POST /orders` | `201` created · `400` invalid input · `503` broker unavailable (retry) |
| Inventory | `GET /stock` | Current stock |
| Inventory | `GET /diagnostics/outbox` | Pending count, oldest pending event, max attempts and relay status (no payloads) |
| Notification | `GET /notifications` | Notifications created so far |

`POST /orders` is the only entry point for outside data, so it is at least as strict as the event contracts: identifiers are limited to the contracts' 200 characters, quantities to the PostgreSQL `INTEGER` range, an order to 100 items, and the total must be a finite number. A request that passes validation therefore always produces an `OrderCreated` event that Payment Service accepts; `services/order-service/tests/orderContract.test.ts` checks this at the boundaries.

### Configuration

Each service validates its environment at startup and fails with a message that names the variable. Error messages never echo credentials. With `NODE_ENV=production`, `DATABASE_URL` and `RABBITMQ_URL` must be set explicitly, so the development defaults are never used.

| Variable | Default | Used by |
|---|---|---|
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672` | all services |
| `DATABASE_URL` | `postgresql://commerce_flow:commerce_flow@localhost:5432/commerce_flow` | Inventory |
| `ORDER_SERVICE_PORT` … `NOTIFICATION_SERVICE_PORT` | `3001` … `3005` | each service |
| `RABBITMQ_PUBLISH_TIMEOUT_MS` | `10000` | Inventory |
| `OUTBOX_BATCH_SIZE` / `OUTBOX_POLL_INTERVAL_MS` | `50` / `1000` | Inventory |
| `OUTBOX_LEASE_MS` | `30000` (must be ≥ 2 × publish timeout) | Inventory |
| `TEST_DATABASE_ADMIN_URL` | `postgresql://…@localhost:5432/postgres` | tests |
| `RABBITMQ_MANAGEMENT_URL` | `http://guest:guest@localhost:15672` | E2E tests |

### Logs

Every service writes one JSON object per line, with the service name, a message and context such as `eventId`, `orderId`, `correlationId`, `attempt` and `queueName`. Failures include a serialised error. To follow one order across all services, pipe the logs into `npm run --silent trace -- <correlation-or-order-id>`. It selects that workflow's lines from all services and sorts them by time (see [docs/demo.md](docs/demo.md#npm-run-trace-one-order-across-all-logs)).

---

## Testing

| Command | Scope | Needs Docker |
|---|---|---|
| `npm run lint` | ESLint, focused on bugs (floating promises, misused promises, `any`, exhaustiveness) | no |
| `npm run typecheck` | Strict TypeScript, all workspaces and the E2E tests | no |
| `npm test` | Unit and component tests (fakes and in-memory adapters) | no |
| `npm run test:integration` | Real PostgreSQL and RabbitMQ | yes |
| `npm run test:e2e` | All five services as processes | yes |

**Isolation.** Each integration test file creates its own `commerce_flow_test_*` database and drops it afterwards. Each E2E file additionally creates its own RabbitMQ virtual host. Tests therefore run in parallel and repeatedly, never depend on each other, and never touch the development database. Before any destructive operation, the helpers check the database name on the live connection and refuse to run against a database without the test prefix.

**What the tests prove** (beyond the success path):

| Scenario | Where |
|---|---|
| Stock, reservation and outbox event commit atomically; a failure rolls everything back | `inventory-service/tests/postgresInventoryStore.integration.test.ts` |
| 12 concurrent orders for 3 items never oversell | same |
| 10 concurrent redeliveries, or 5 duplicates with new event ids, reserve once | same |
| A terminated database connection mid-transaction commits nothing; the service recovers | same |
| Publisher confirms, broker nack, confirm timeout, channel closed before confirm | `shared/messaging/tests` |
| `basic.return` fails the right publication among concurrent ones (even with identical message ids) | `shared/messaging/tests/confirmedPublisher.test.ts` |
| Fresh virtual host without bindings: publish is rejected as unroutable; succeeds once the queue exists; 40 concurrent mixed publications; real broker nack; publisher before any consumer | `shared/messaging/tests/integration/routing.integration.test.ts` |
| Unroutable outbox event stays pending and is published once its queue exists, without a second reservation | `inventory-service/tests/outbox.integration.test.ts` |
| Order Service answers 503 (never 201) when `OrderCreated` cannot be routed | `order-service/tests/orderApp.integration.test.ts` |
| Redelivery across a date change re-emits an identical event | `payment-service` and `delivery-service` unit tests |
| Poison message → DLQ, retry exhaustion → DLQ, graceful drain without redelivery | `shared/messaging/tests/integration` |
| Broker unreachable → events stay pending → a restarted relay publishes them | `inventory-service/tests/outbox.integration.test.ts` |
| Crash between publish and "mark published" → re-published with the same event id | same |
| 4 competing relays publish 200 events exactly once; stale claims are fenced | same |
| The relay survives PostgreSQL terminating its connections and finishes its batch on shutdown | same |
| Concurrent migrations apply once; the legacy schema is adopted; a failed migration rolls back | `inventory-service/tests/database.integration.test.ts` |
| Full workflow, success and failure; orders accepted while every downstream service is still offline | `tests/e2e/commerceFlow.e2e.test.ts` |
| Duplicate deliveries, poison message, outbox recovery after an Inventory `SIGKILL` | `tests/e2e/inventoryReliability.e2e.test.ts` |
| All services survive a broker outage and recover without restarting | `tests/e2e/rabbitMqRecovery.e2e.test.ts` |

Tests wait on observable conditions, such as readiness endpoints, structured log entries and database state, rather than fixed sleeps.

---

## Continuous integration

GitHub Actions ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs on every push to `main` and on pull requests, on Node.js **22 and 24**:

`npm ci` → `npm audit --audit-level=moderate` → lint → typecheck → build → unit tests → `docker compose up --wait` → integration tests → E2E and recovery tests → container logs on failure → `docker compose down --volumes` (always).

A second job, **Full-stack demo (containers)**, builds the service image, starts the complete system with `docker compose --profile app up --wait`, runs `npm run demo` and `npm run status` against it, and traces the demo order through the container logs.

---

## Architectural decisions and trade-offs

**Transactional outbox instead of publishing from the handler.** Committing and then publishing leaves a window where a crash loses the event. Publishing and then committing can announce a reservation that never happened. Writing the event in the same transaction removes both windows. The cost is extra latency between commit and publication, which a wake-up call after each commit plus a 1 s poll keeps small.

**Polling with `SKIP LOCKED` instead of `LISTEN/NOTIFY` or CDC.** Polling is simple, survives restarts and lets several relays share work without coordination. Log-based change data capture (e.g. Debezium) scales further but adds infrastructure that is not justified here.

**Leases instead of long-lived row locks.** Holding `FOR UPDATE` across a network call would tie database connections to broker latency. A lease (`available_at`) bounds how long a crashed relay can block an event, and the `attempts` fencing token keeps late updates harmless.

**Provisioned topology plus mandatory publishing.** Publisher confirms only say that the broker took responsibility, not that a queue received the message: RabbitMQ confirms an unroutable message and drops it. Declaring the complete topology from every service removes the start-order dependency; mandatory publishing with correlated `basic.return` handling turns any remaining gap (a deleted queue, a missing binding) into a failed publication that the outbox retries, instead of silent loss. Either measure alone is insufficient: provisioning cannot detect drift, and mandatory publishing alone would reject every event published before its consumer first started.

**Business-key idempotency instead of an inbox table.** The order id identifies the business operation. Keying the reservation on it catches redeliveries *and* re-emitted events with new ids, which an inbox keyed on message ids would miss. A separate inbox would add a table without adding protection.

<a id="why-only-inventory-has-an-outbox"></a>
**Why only Inventory has an outbox.** Payment and Delivery hold no state of their own. For them, publish-then-ack is already at-least-once: the unacknowledged input message is the durable retry record, and deterministic ids make the retry safe. Order Service has no database. It publishes synchronously and answers `503` when it cannot, so the client is the retry mechanism.

**At-least-once, not exactly-once.** Exactly-once delivery across a broker and a database is not achievable without distributed transactions. CommerceFlow guarantees *effectively-once business effects* for stock (idempotent consumer + atomic outbox) and says so explicitly.

**In-process retries before dead-lettering.** Dead-lettering on the first error turned short outages into stuck orders. In-process retries are simple and bounded; a message that is still failing after them is dead-lettered for inspection. Broker-side delayed retries (TTL queues) would survive restarts better but complicate the topology.

**Pragmatic Clean Architecture.** Ports appear where there is more than one implementation (PostgreSQL and in-memory stores, broker and fake publishers) or where they protect business rules from infrastructure. There is no DI container and no generic repository framework: each composition root is plain code.

**Shared kernel packages.** Contracts, messaging, logging, runtime and testing are small workspaces, so every service validates and routes events the same way. The coupling this creates is acceptable in a single repository; independently versioned contracts would be the next step if the services were split across repositories.

---

## Known limitations

* **No end-to-end exactly-once.** Duplicates are expected and handled; downstream systems must stay idempotent.
* **Order Service has no persistence or idempotency key.** If the broker accepts `OrderCreated` but the confirm is lost (timeout), the client gets `503` and a retry creates a second order. The fix is order persistence plus an `Idempotency-Key` header and an outbox.
* **Payment and delivery are simulated.** Payment always succeeds and nothing is compensated: a failed reservation does not refund the payment (no saga).
* **Notifications are in memory** and are lost on restart.
* **Handler retries live in memory.** A crash during a retry delay leads to redelivery (safe, but the retry counter restarts). Dead-lettered messages are replayed by an operator with `npm run dlq -- replay <queue>`, not automatically.
* **Ordering.** Events are published roughly in outbox order, but concurrent relays and retries can reorder them. This is harmless here because each order produces one inventory event.
* **Unroutable events from Payment and Delivery.** If a binding were missing despite the provisioned topology, their publish fails and the input message is retried and then dead-lettered (not lost, but it needs a replay with `npm run dlq`). Inventory's outbox keeps retrying instead.
* **Outbox retention.** Published rows are kept indefinitely. Production would prune them.
* **Observability** is limited to structured logs, readiness, the outbox diagnostics endpoint and the developer tools (`demo`, `status`, `trace`). There are no metrics backend or distributed traces (OpenTelemetry would be the next step).
* **The containers are for local use and CI.** The Compose `app` profile runs one image per service on a single host. There is no orchestrator manifest, image registry or TLS configuration.
* **Windows test flakiness.** On Windows with Docker Desktop, a service process occasionally terminates with a native fast-fail (`0xC0000409`) while the RabbitMQ container restarts during the recovery E2E test (about 2 in 15 local runs). The process dies in native code, so no JavaScript handler can intercept it. It does not reproduce on Linux CI or with 1,000 simulated handshake failures in isolation. JavaScript-level fatal errors are logged before a controlled exit; a production deployment would rely on a process supervisor's restart policy for native crashes.

---

## Further reading

* [docs/architecture.md](docs/architecture.md): boundaries, ports and adapters, the messaging layer and the service host
* [docs/reliability.md](docs/reliability.md): failure-mode analysis and how each failure is handled and tested
* [docs/demo.md](docs/demo.md): the containerised stack, the scripted demo, `status` and `trace`

---

## Author

Created by **Kris Riisgaard Larsen**.

CommerceFlow is part of a software development portfolio focused on backend engineering, integrations, distributed systems and maintainable application architecture.
