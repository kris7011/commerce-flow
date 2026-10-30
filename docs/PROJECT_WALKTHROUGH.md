# CommerceFlow: project walkthrough

A developer-level tour of what is implemented, how it fits together and how
to explain it. Only existing behaviour is described; limitations and future
work are labelled.

## 1. System overview

Five Node.js/TypeScript services model an order workflow — Order, Payment,
Inventory, Delivery, Notification — and talk only through events on a
RabbitMQ topic exchange. The business logic is deliberately small; the
engineering focus is consistency under failure: transactional outboxes,
publisher confirms with mandatory routing, idempotent consumers, retries,
dead-lettering and recovery.

| Status | Capability |
|---|---|
| Implemented, tested | Event workflow; PostgreSQL stock and reservations with an outbox; durable, idempotent order submission (`Idempotency-Key`) with an outbox; order timeline (API and page); dead-letter inspection and replay; containerised stack and scripted demos |
| Implemented, checked manually | The order timeline page in a browser (Edge: light, dark, 390 px) |
| Simulated by design | Payment (always authorizes) and delivery (no carrier) |
| Not implemented | Authentication, compensation (saga) for failed reservations, metrics/tracing backend, key expiry, production deployment |

## 2. Architecture and structure

```text
services/order-service        POST /orders → orders + OrderCreated (outbox); timeline consumer; page at /
services/payment-service      OrderCreated → PaymentAuthorized (simulated)
services/inventory-service    PaymentAuthorized → stock + reservation + outbox
services/delivery-service     InventoryReserved → DeliveryBooked (simulated)
services/notification-service DeliveryBooked / InventoryReservationFailed → notifications (in memory)
shared/contracts              event types, zod schemas, routing keys, queue topology
shared/messaging              RabbitMqClient (confirms, mandatory, retries, DLQ), supervisor
shared/persistence            pool, migration runner, withTransaction, transactional outbox
shared/runtime, logging, testing
tests/e2e                     all services as processes against isolated vhosts/databases
tools/cli                     demo, recovery demo, status, dlq, trace
```

Each service keeps business rules in a plain class, infrastructure behind
small ports, and wiring in `index.ts`. See [architecture.md](architecture.md).

## 3. Technologies

Node.js 22/24, TypeScript (strict), Express 4, zod 4, amqplib, pg,
PostgreSQL 17, RabbitMQ 3.13, Docker Compose, node:test with tsx, ESLint
with typescript-eslint, GitHub Actions.

## 4. Authentication and authorization

None; this is a local demonstration. Consequences that are documented:
idempotency keys are global rather than per client, and the timeline API
and page show every order. Defensive measures that exist: strict input
validation at the only public entry point (`orderRequestValidator.ts`,
aligned with the event contracts), key format validation, ports bound to
`127.0.0.1` in Compose, a strict Content-Security-Policy on the page, and
no payloads in logs or diagnostics.

## 5. Persistence

One PostgreSQL database; each service that stores data owns its tables and
migration history:

* Inventory: `inventory_stock`, `inventory_reservations`, `inventory_outbox`.
* Order: `orders` (unique `idempotency_key`), `order_timeline` (unique
  `event_id`, `deliveries` counter), `order_outbox`.

Migrations run on service start, inside one transaction under an advisory
lock (`migrateDatabase` in `shared/persistence`). Tests create a separate
`commerce_flow_test_*` database per file and refuse destructive operations
on any other name.

## 6. Core workflows

**Order submission** (`app.ts` → `PostgresOrderStore.submitOrder`): validate
the key and the body → build ids, event and request fingerprint → one
transaction inserts the order (`ON CONFLICT (idempotency_key) DO NOTHING`),
the outbox event and an `Accepted` timeline row → `201`. A conflict with the
same fingerprint records `Replayed` and returns `200` with the original
body; a different fingerprint returns `422`.

**Publication** (`OutboxRelay`): claim due rows with `FOR UPDATE SKIP
LOCKED` and a lease, publish with confirms and `mandatory`, mark published
only after the confirm; failures back off with jitter; `attempts` fences
stale relays.

**Workflow**: Payment derives `PaymentAuthorized` deterministically;
Inventory reserves once per order (advisory lock + primary key) and writes
its outcome to its outbox; Delivery books; Notification notifies.

**Timeline** (`orderWorkflowEventHandler.ts`, `recordWorkflowEvent`,
`buildOrderTimeline`): Order Service consumes the four downstream events on
its own queue, records each event id once (counting redeliveries), moves the
status forward only, and orders the view causally by stage. The page polls
`GET /orders/{id}`. Details: [order-timeline.md](order-timeline.md).

## 7. Testing

| Command | What | Count (last local run, 2026-10-30) |
|---|---|---|
| `npm test` | unit and component tests, all workspaces | 199 |
| `npm run test:integration` | real PostgreSQL and RabbitMQ | 49 |
| `npm run test:e2e` | all services as processes (workflow, reliability, recovery) | 6 |
| `npm run demo`, `npm run demo:recovery` | the containerised stack, scripted checks | 17 and 21 checks |

The order feature's key tests: 20 concurrent submissions with one key give
one order and one outbox event; 8 concurrent HTTP retries give one `201`;
10 concurrent redeliveries give one timeline entry with `deliveries = 10`;
an order accepted while RabbitMQ is down completes after recovery. CI runs
all of it on Node.js 22 and 24, plus the container demos.

## 8. Local development and deployment

```bash
npm ci
npm run stack:up        # RabbitMQ, PostgreSQL and five service containers
npm run demo            # then open http://localhost:3001/
```

Or `docker compose up -d --wait rabbitmq postgres` and `npm run dev:*` per
service. Everything runs locally without paid services; there is no
production deployment.

## 9. Backup and restore

Development data lives in the Compose volume `commerce-flow-postgres-data`.
Back up with `docker compose exec -T postgres pg_dump -U commerce_flow -Fc
commerce_flow > backup.dump` and restore with `pg_restore --clean` into a
stopped stack. After restoring an older dump, outbox events published after
the dump are published again; consumers are idempotent, so this produces
duplicates, not duplicate effects. This procedure is documented, not
automated or tested.

## 10. Design decisions

* **Outbox for Order as well as Inventory** (1.1): makes acceptance
  independent of the broker and removes the lost-confirm duplicate. Cost:
  the API now depends on PostgreSQL, and publication is asynchronous.
* **Idempotency key enforced by a unique index**, not a cache or a lock:
  holds across instances and restarts; the fingerprint distinguishes a
  retry from misuse. Alternative: optional keys — rejected, because then
  retries without a key could still duplicate.
* **Timeline as a read model in Order Service**: no synchronous calls
  between services; only published events appear. Alternative: query each
  service — adds coupling and availability dependencies.
* **Causal ordering by stage**: producer timestamps are copied from source
  events for deterministic re-emission, so they cannot order the timeline.
* **Shared persistence package**: one outbox implementation, parameterised
  by table name, instead of a copy per service.

## 11. Known limitations

See the README's *Known limitations*: no exactly-once delivery, simulated
payment and delivery without compensation, in-memory notifications and
handler retries, global non-expiring idempotency keys, eventually consistent
timeline, unbounded outbox retention, logs-only observability, local
containers only.

## 12. Interview preparation: durable, idempotent orders and the timeline

**Non-technical.** "If a customer's app times out and presses *order* again,
the shop must not charge or ship twice. Each order now carries a unique
ticket number; if the same ticket comes back, the shop shows the order it
already has. And you can open a page that shows every step of an order as
it actually happened, including which steps are only simulated."

**Developer.** "`POST /orders` requires an `Idempotency-Key`. In one
PostgreSQL transaction I insert the order with `ON CONFLICT DO NOTHING` on
the key, plus its `OrderCreated` event in an outbox table. A conflict with
the same request fingerprint returns the stored order with 200; a different
request returns 422. A relay publishes the outbox with publisher confirms.
Order Service also consumes the downstream events on its own queue and
records them per event id for the timeline API and page."

**Senior / architectural.** "The 1.0 design published synchronously, so a
lost confirm plus a client retry could create two orders — the classic dual
write. I moved the decision into the database: the unique index makes
idempotency hold under concurrency and across instances, and the outbox
makes the event atomic with the order. The timeline is a read model fed by
the existing events, so I added no synchronous coupling, and I made its
ordering causal because producer timestamps are intentionally copied for
deterministic re-emission. The trade-offs are an extra dependency on
PostgreSQL for the API, eventual consistency in the timeline, and keys that
would need client scoping and expiry in production."

**Questions and example answers**

1. *Why not just check whether an order with the same content exists?*
   Two legitimate orders can have the same content. The key expresses the
   client's intent ("this is the same attempt"); the fingerprint only guards
   against reusing a key for something else.
2. *What happens when two identical requests arrive at the same time?*
   Both try to insert; the unique index makes the second wait for the first
   transaction. If it commits, the second gets no row back, reads the
   committed order and returns 200. The integration test does this with 20
   concurrent submissions.
3. *Why 422 for a reused key and not 409?* The request is well-formed but
   cannot be processed with that key; the IETF draft on the header suggests
   422 for this case. 409 is also defensible; what matters is that nothing
   is stored.
4. *Why does the timeline not use the events' `occurredAt` for ordering?*
   Payment and Delivery copy it from their source event so a redelivery is
   byte-identical; the demo showed "payment" sorting before "order accepted".
   The timeline orders by workflow stage and shows the observation time.
5. *What would you add for production?* Scope keys to the authenticated
   client and expire them, authorise the timeline per customer, prune
   published outbox rows, add metrics and tracing, and a compensating step
   for failed reservations.

**Hands-on exercise.** With the stack running, submit an order on the page,
press *Retry same request* and confirm `200` with `Idempotent-Replayed`.
Then change the quantity and submit with the same key (expect `422`). Next,
stop RabbitMQ (`docker compose stop rabbitmq`), submit a new order (expect
`201` and *OrderCreated waiting in the outbox* with an error), start RabbitMQ
again and watch the entry turn into *confirmed by RabbitMQ* and the order
complete. Explain which table rows exist at each moment.

## 13. Ownership note

Version 1.1 (durable orders, idempotency and the timeline) was implemented
with AI assistance (Claude Code) on the owner's specification, on top of the
owner's existing services, messaging layer, outbox and tests. Verification:
the test counts above, the e2e suites against real infrastructure, both
container demos, and browser checks of the page; CI on the pull request and
on `main`.
