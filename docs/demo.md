# Running the demo

This guide shows the whole order workflow running on your machine, using real HTTP requests and real RabbitMQ and PostgreSQL, without reading the logs of five services by hand.

## Start the system

You need Docker with Docker Compose and Node.js 22 or 24 (the demo script runs on the host).

```bash
npm ci
npm run stack:up        # docker compose --profile app up -d --build --wait
```

This builds one image for the five services and starts it five times next to RabbitMQ and PostgreSQL. `--wait` returns once every service answers `GET /ready`. All ports are published on `127.0.0.1` only:

| Port | What |
|---|---|
| 3001 to 3005 | Order, Payment, Inventory, Delivery, Notification |
| 5672 / 15672 | RabbitMQ (AMQP / management UI, `guest` / `guest`) |
| 5432 | PostgreSQL |

The image runs the compiled JavaScript under a non-root user, without `tsx` or dev dependencies. The shared packages export `dist/` under the `production` condition (`node --conditions=production`), while development and tests keep running directly from the TypeScript sources.

Instead of containers, you can also run the services from source in five terminals (`npm run dev:*`, see the README). The tools below work the same way with both setups.

## `npm run demo`: the scripted walk-through

```bash
npm run demo
```

The demo connects a temporary, exclusive queue to the `commerce.events` exchange (bound to `#`), so it sees every event that any service publishes. It then runs five steps and checks each result against what the services expose over HTTP:

| Step | What it does | What it checks |
|---|---|---|
| 1. Readiness | Polls `GET /ready` on all five services (up to 60 s) | Every service can use its dependencies |
| 2. Successful order | `POST /orders` for one unit of the product with the most stock | `201`. The timeline shows `OrderCreated → PaymentAuthorized → InventoryReserved → DeliveryBooked`, a `DeliveryBooked` notification exists, stock drops by exactly 1 |
| 3. Unfulfillable order | Orders one more unit than is in stock | `InventoryReservationFailed` and no `DeliveryBooked`, a failure notification exists, stock is unchanged |
| 4. Duplicate event | Re-publishes step 2's `PaymentAuthorized` under a **new event id** | Still exactly one `InventoryReserved` for that order, and stock is unchanged. The event-id cache cannot recognise this duplicate, so only Inventory's durable one-reservation-per-order rule protects the stock |
| 5. Invalid request | `POST /orders` with an empty item list | `400`, and no event is published for its correlation id |
| 6. Final state | Reads the outbox diagnostics and queue depths | No pending outbox events, and the dead-letter queues hold as many messages as before the demo |

For each order, the demo prints the events it observed, with the time since the HTTP request and the producing service. If any check fails, the demo exits with status `1`. CI runs it this way as a full-stack smoke test (job *Full-stack demo (containers)* in [`.github/workflows/ci.yml`](../.github/workflows/ci.yml)).

The orders use correlation ids of the form `demo-<run>-success`, `demo-<run>-out-of-stock` and `demo-<run>-invalid`. Set `DEMO_RUN_ID` to choose `<run>` yourself.

Stock is persistent. Every run uses one unit, and the demo always picks the product with the most stock. Reset the data with `docker compose --profile app down --volumes`.

> While the demo's queue is bound to `#`, every routing key has a queue, so the services' mandatory publishing cannot detect a missing binding during that time. The tap is therefore a developer tool and not part of the services.

### Example output

This excerpt comes from CI run [39109750417](https://github.com/kris7011/commerce-flow/actions/runs/39109750417) (job *Full-stack demo (containers)*). It shows steps 2 and 4: the second `PaymentAuthorized`, at +146 ms, is the injected duplicate, and no second `InventoryReserved` follows it.

```text
== 2. A successful order =============================================
  POST /orders  x-correlation-id: demo-ci-39109750417-1-success  ->  HTTP 201  orderId e4dc940d-43da-4002-930e-02828e215074
  PASS  Order Service answers 201 Created
  Events on commerce.events (outcome: Delivered):
    +   12 ms  order-service        OrderCreated                order.created
    +   19 ms  payment-service      PaymentAuthorized           payment.authorized
    +   69 ms  inventory-service    InventoryReserved           inventory.reserved
    +   75 ms  delivery-service     DeliveryBooked              delivery.booked
  PASS  the order passes all five services and ends with DeliveryBooked
  Notification: "Delivery has been booked with DefaultCarrier. Estimated delivery date: 2026-10-21."
  PASS  Notification Service created a DeliveryBooked notification
  Stock of washing-machine-01: 10 -> 9
  PASS  stock of washing-machine-01 decreased by exactly 1

== 4. A duplicate PaymentAuthorized with a new event id ==============
  Published PaymentAuthorized a4127038-1b54-46c3-a7d6-2c7a8fe4d2cb for order e4dc940d-43da-4002-930e-02828e215074 again
  Events on commerce.events (outcome: Delivered):
    +   12 ms  order-service        OrderCreated                order.created
    +   19 ms  payment-service      PaymentAuthorized           payment.authorized
    +   69 ms  inventory-service    InventoryReserved           inventory.reserved
    +   75 ms  delivery-service     DeliveryBooked              delivery.booked
    +  146 ms  payment-service      PaymentAuthorized           payment.authorized
  PASS  Inventory ignores the duplicate: still exactly one InventoryReserved
  PASS  stock is unchanged by the duplicate

  [steps 3, 5 and 6 omitted]

== Result ============================================================
  All 15 checks passed.
```

The same run's `trace` step, for the successful order, shows the services' log lines in time order. The last two lines are Inventory receiving and ignoring the duplicate:

```text
08:01:07.510  order-service        info  Published event  routingKey=order.created eventId=b425b30b-a8fc-4cc7-b3f0-6bfa6d7847cb eventType=OrderCreated
08:01:07.510  order-service        info  Created order  orderId=e4dc940d-43da-4002-930e-02828e215074 eventId=b425b30b-a8fc-4cc7-b3f0-6bfa6d7847cb customerId=demo-customer-1 totalAmount=4999 itemCount=1
08:01:07.511  payment-service      info  Received OrderCreated  eventId=b425b30b-a8fc-4cc7-b3f0-6bfa6d7847cb orderId=e4dc940d-43da-4002-930e-02828e215074 totalAmount=4999 itemCount=1
08:01:07.515  payment-service      info  Published event  routingKey=payment.authorized eventId=71b82360-2700-5e82-ba1d-e0575083fd34 eventType=PaymentAuthorized
08:01:07.515  payment-service      info  Authorized payment  eventId=71b82360-2700-5e82-ba1d-e0575083fd34 orderId=e4dc940d-43da-4002-930e-02828e215074 paymentId=6176b0e8-6986-5e9d-8092-6acc822f3756 amount=4999
08:01:07.517  inventory-service    info  Received PaymentAuthorized  eventId=71b82360-2700-5e82-ba1d-e0575083fd34 orderId=e4dc940d-43da-4002-930e-02828e215074 paymentId=6176b0e8-6986-5e9d-8092-6acc822f3756 amount=4999 itemCount=1
08:01:07.522  inventory-service    info  Reserved inventory  eventId=3a111cd0-4602-482f-ba96-7827e0a14c5a orderId=e4dc940d-43da-4002-930e-02828e215074 reservationId=419b4bfb-6caf-47b2-9a12-c054553b1843 itemCount=1
08:01:07.565  inventory-service    info  Published event  routingKey=inventory.reserved eventId=3a111cd0-4602-482f-ba96-7827e0a14c5a eventType=InventoryReserved
08:01:07.568  delivery-service     info  Received InventoryReserved  eventId=3a111cd0-4602-482f-ba96-7827e0a14c5a orderId=e4dc940d-43da-4002-930e-02828e215074 reservationId=419b4bfb-6caf-47b2-9a12-c054553b1843 itemCount=1
08:01:07.573  delivery-service     info  Published event  routingKey=delivery.booked eventId=5c686714-ce21-5329-b9a5-17d822feca16 eventType=DeliveryBooked
08:01:07.573  delivery-service     info  Booked delivery  eventId=5c686714-ce21-5329-b9a5-17d822feca16 orderId=e4dc940d-43da-4002-930e-02828e215074 deliveryId=54fe2328-c52c-546e-8c89-113ace041517 carrier=DefaultCarrier estimatedDeliveryDate=2026-10-21
08:01:07.576  notification-service info  Created customer notification  eventId=5c686714-ce21-5329-b9a5-17d822feca16 notificationId=25c18521-90fd-4e61-a07b-2d071ce99d64 notificationType=DeliveryBooked orderId=e4dc940d-43da-4002-930e-02828e215074 deliveryId=54fe2328-c52c-546e-8c89-113ace041517 carrier=DefaultCarrier estimatedDeliveryDate=2026-10-21
08:01:07.641  inventory-service    info  Received PaymentAuthorized  eventId=a4127038-1b54-46c3-a7d6-2c7a8fe4d2cb orderId=e4dc940d-43da-4002-930e-02828e215074 paymentId=6176b0e8-6986-5e9d-8092-6acc822f3756 amount=4999 itemCount=1
08:01:07.642  inventory-service    warn  Ignored duplicate PaymentAuthorized for an already processed order  eventId=a4127038-1b54-46c3-a7d6-2c7a8fe4d2cb orderId=e4dc940d-43da-4002-930e-02828e215074 originalSourceEventId=71b82360-2700-5e82-ba1d-e0575083fd34 originalStatus=Reserved originalResultEventId=3a111cd0-4602-482f-ba96-7827e0a14c5a
```

## `npm run demo:recovery`: outage, dead letter and replay

```bash
npm run demo:recovery
```

This scenario needs the containerised stack, because it stops and starts the `postgres` container with `docker compose`. It shows what happens when a dependency is down for longer than the in-process retries last, and how the system is brought back:

| Step | What happens | What it checks |
|---|---|---|
| 1. Readiness | As in `npm run demo` | All services are ready, and `inventory-service.payment-authorized.dead-letter` is empty |
| 2. Outage | `docker compose stop postgres`, then an order | Inventory reports `database: NotReady`. Order still answers `201`, because it needs no database. Inventory retries the `PaymentAuthorized` message (5 attempts with backoff) and then dead-letters it. No `InventoryReserved` is published |
| 3. Recovery | `docker compose start postgres` | Inventory reports `database: Ready` again without a restart |
| 4. Replay | `replayDeadLetters` for Inventory's queue | Exactly one message is replayed. The order completes with `DeliveryBooked`, the customer is notified, stock drops by exactly 1 and the dead-letter queue is empty |

If the script fails half-way, it still starts PostgreSQL again. CI runs this scenario after `npm run demo`.

## `npm run dlq`: inspect and replay dead letters

```bash
npm run dlq                                   # every dead-letter queue, with samples
npm run dlq -- list inventory-service.payment-authorized
npm run dlq -- replay inventory-service.payment-authorized [--limit 10]
```

Listing fetches messages unacknowledged and closes the channel, so the broker puts them back in their original order. For each message it shows the event type, event id, order id, correlation id, and RabbitMQ's `x-death` record (reason, queue, time and count).

Replaying is deliberately conservative:

* Only consumer queues of the topology (or their `.dead-letter` names) are accepted.
* Each message is published through the default exchange **directly to the queue it came from**. Other queues bound to the same routing key therefore never receive a second copy.
* Publishing is mandatory and confirmed. The dead letter is acknowledged only after the broker has confirmed the publication. A crash in between leaves a copy in both queues, which the idempotent consumers absorb (at-least-once).
* At most the number of messages present at the start is replayed. A message that fails again returns to the dead-letter queue and does not cause an endless loop.
* `x-death` is dropped, `x-commerce-flow-replayed-from` is set and `x-commerce-flow-replay-count` is incremented, so a message that is replayed repeatedly is easy to spot.

`tools/cli/tests/deadLetters.integration.test.ts` checks these properties against a real broker, in its own virtual host with the production topology.

## `npm run status`: the system on one screen

```bash
npm run status
```

This prints readiness and dependency status per service, stock, the outbox backlog and relay status, and the messages waiting in every consumer queue and dead-letter queue. Queue counts are read live over AMQP; the management API's statistics lag by several seconds. It also lists the latest notifications. The command exits with `1` if a service is not ready or a dead-letter queue is not empty.

## `npm run trace`: one order across all logs

Every service logs one JSON object per line, with the `correlationId`, `orderId` and `eventId` of whatever it is working on. `trace` selects the lines of a single workflow and sorts them by time:

```bash
docker compose --profile app logs --no-log-prefix | npm run --silent trace -- demo-<run>-success
```

The id can be a correlation id or an order id. Lines that carry only an id belonging to the same workflow are included too, such as the outbox relay's log entry when it publishes an event. To read saved log files instead of stdin, pass them after the id.

## Configuration

| Variable | Default |
|---|---|
| `ORDER_SERVICE_URL` … `NOTIFICATION_SERVICE_URL` | `http://127.0.0.1:3001` … `:3005` |
| `RABBITMQ_URL` | `amqp://guest:guest@127.0.0.1:5672` |
| `DEMO_RUN_ID` | the current UTC time, e.g. `20261018101500` (also used by `demo:recovery`, whose correlation id is `recovery-<run>`) |
| `DEMO_READY_TIMEOUT_MS` / `DEMO_FLOW_TIMEOUT_MS` | `60000` / `30000` |

## Stop

```bash
npm run stack:down                          # keep the data
docker compose --profile app down --volumes # also delete stock and outbox
```
