# Changelog

Notable changes to CommerceFlow are documented here. This repository is a **portfolio and architecture demonstration**, not a published npm package or a production commerce platform.

## [1.1.0] - 2026-10-30

Durable, idempotent order submission and a visual order timeline.

### Added

- **Order persistence:** Order Service stores orders in PostgreSQL (`orders`, `order_timeline`, `order_outbox`) with its own versioned migrations.
- **Idempotent `POST /orders`:** a required `Idempotency-Key` header. A retry with the same key and request returns the original order (`200`, `Idempotent-Replayed: true`); the same key with a different request is rejected (`422`). A unique index makes this hold under concurrency.
- **Order outbox:** `OrderCreated` is committed with the order and published by the shared outbox relay, so orders are accepted while RabbitMQ is down.
- **Order timeline:** Order Service consumes the workflow events on `order-service.order-events`, records each event once (counting redeliveries) and moves the order status forward only. `GET /orders`, `GET /orders/{id}` and a timeline page at `/` show the actual steps, outcome, duplicates and which steps are simulated.
- **Shared persistence package:** `@commerce-flow/persistence` (pool, migration runner, transactional outbox, transaction helper), now used by Order and Inventory.

### Changed

- `POST /orders` without an `Idempotency-Key` is rejected with `400` (breaking change for API clients).
- `POST /orders` no longer answers `503` when RabbitMQ is unavailable; it answers `503` only when PostgreSQL is unavailable.
- Order Service readiness now also requires PostgreSQL.

## [1.0.0] - 2026-10-15

First portfolio release of the event-driven commerce workflow.

### Highlights

- **Five independently running services:** Order, Payment, Inventory, Delivery and Notification communicate through typed RabbitMQ events.
- **Transactional inventory:** PostgreSQL commits stock changes, one durable reservation outcome per order, and the matching outbox event in a single transaction.
- **Reliable event publishing:** RabbitMQ publisher confirms, mandatory delivery and correlated `basic.return` handling prevent a confirmed-but-unroutable publication from being treated as success.
- **At-least-once processing:** Business-key idempotency prevents duplicate inventory reservations; Payment and Delivery derive identical output events when the same input is redelivered.
- **Outage recovery:** Outbox leases, bounded retries, dead-letter queues, broker reconnection, and graceful service shutdown.
- **Safer schema and tests:** Versioned inventory migrations; isolated PostgreSQL test databases and RabbitMQ virtual hosts.
- **Automated checks:** Dependency audit, lint, TypeScript checks, build, unit tests, integration tests and end-to-end/recovery tests on Node.js 22 and 24 in GitHub Actions.
- **Developer documentation:** Architecture diagrams, documented event routing, configuration guidance and failure-mode analysis.

### Important scope limitations

CommerceFlow intentionally simulates payments and delivery. Order creation has no durable idempotency key, notifications are in memory, failed/dead-lettered messages require operational handling, and published outbox records have no automatic retention policy. The project does **not** claim production readiness or exactly-once delivery.

See [README](README.md), [architecture](docs/architecture.md), [reliability](docs/reliability.md) and the [v1.0.0 release notes](docs/releases/v1.0.0.md).
