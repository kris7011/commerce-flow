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
| `DEMO_RUN_ID` | the current UTC time, e.g. `20261018101500` |
| `DEMO_READY_TIMEOUT_MS` / `DEMO_FLOW_TIMEOUT_MS` | `60000` / `30000` |

## Stop

```bash
npm run stack:down                          # keep the data
docker compose --profile app down --volumes # also delete stock and outbox
```
