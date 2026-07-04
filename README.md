# CommerceFlow

CommerceFlow is a backend demo project built to explore event-driven microservice architecture in an e-commerce domain.

The purpose of the project is to model a simplified order flow where services communicate through asynchronous domain events instead of direct service-to-service calls.

This project is intentionally built step by step to make the architecture easy to understand and explain. The goal is not to build a complete webshop, but to demonstrate backend engineering principles used in scalable distributed systems.

## Current version

The first version contains two services:

- Order Service
- Payment Service

The event flow is:

```text
POST /orders
  -> Order Service creates an order
  -> Order Service publishes OrderCreated
  -> Payment Service consumes OrderCreated
  -> Payment Service publishes PaymentAuthorized
```

## Why this project exists

The project demonstrates backend concepts that are relevant for scalable e-commerce systems:

- Service boundaries
- Domain events
- Event-driven architecture
- Asynchronous communication
- RabbitMQ as message broker
- TypeScript backend services
- Correlation IDs for traceability
- Loose coupling between services

Later versions will add:

- PostgreSQL persistence
- Transactional outbox
- Idempotent consumers
- Retries
- Dead-letter queues
- Observability
- Automated tests

## Architecture overview

The first version focuses on one simple business flow:

```text
Client
  -> Order Service
  -> RabbitMQ
  -> Payment Service
  -> RabbitMQ
```

The Order Service does not call the Payment Service directly.

Instead, the Order Service publishes an `OrderCreated` event.

The Payment Service listens for that event and reacts by simulating a payment authorization. When payment is authorized, it publishes a `PaymentAuthorized` event.

This keeps the services loosely coupled.

## Services

### Order Service

The Order Service owns order creation.

Responsibilities:

- Receive order requests
- Validate incoming order data
- Create an order ID
- Calculate total order amount
- Publish an `OrderCreated` domain event

Endpoint:

```http
POST /orders
```

Health check:

```http
GET /health
```

Default port:

```text
3001
```

### Payment Service

The Payment Service owns payment authorization.

Responsibilities:

- Subscribe to `OrderCreated`
- Simulate payment authorization
- Publish `PaymentAuthorized`

Health check:

```http
GET /health
```

Default port:

```text
3002
```

## Shared libraries

The project contains shared packages for contracts and messaging.

### shared/contracts

Contains event contracts used by the services.

Current events:

- `OrderCreated`
- `PaymentAuthorized`

The contracts are shared so that services agree on the structure of events.

### shared/messaging

Contains a small RabbitMQ wrapper used by the services.

It handles:

- Connecting to RabbitMQ
- Creating the topic exchange
- Publishing events
- Subscribing to routing keys
- Acknowledging messages
- Rejecting failed messages

## Message broker

RabbitMQ is used as the message broker.

The exchange used by the project is:

```text
commerce.events
```

Exchange type:

```text
topic
```

Current routing keys:

```text
order.created
payment.authorized
```

Current queue:

```text
payment-service.order-created
```

## Correlation ID

A correlation ID is used to trace a business flow across services.

When creating an order, you can provide this header:

```http
x-correlation-id: demo-correlation-001
```

If no correlation ID is provided, the Order Service generates one automatically.

The same correlation ID is passed from:

```text
OrderCreated
  -> PaymentAuthorized
```

This makes it easier to follow logs across services.

## Requirements

You need:

- Node.js 20 or newer
- npm
- Docker Desktop

Check Node version:

```bash
node -v
```

Check npm version:

```bash
npm -v
```

Check Docker:

```bash
docker --version
```

## Running locally

Install dependencies:

```bash
npm install
```

Start RabbitMQ:

```bash
docker compose up -d
```

Start Order Service:

```bash
npm run dev:order
```

Start Payment Service in another terminal:

```bash
npm run dev:payment
```

## Test the services

Order Service health check:

```bash
curl http://localhost:3001/health
```

Expected response:

```json
{
  "status": "Healthy",
  "service": "order-service"
}
```

Payment Service health check:

```bash
curl http://localhost:3002/health
```

Expected response:

```json
{
  "status": "Healthy",
  "service": "payment-service"
}
```

## Create an order

Use this command:

```bash
curl -X POST http://localhost:3001/orders \
  -H "content-type: application/json" \
  -H "x-correlation-id: demo-correlation-001" \
  -d '{
    "customerId": "customer-1001",
    "items": [
      {
        "productId": "washing-machine-01",
        "quantity": 1,
        "unitPrice": 4999
      }
    ]
  }'
```

Expected response:

```json
{
  "orderId": "...",
  "status": "Created",
  "totalAmount": 4999,
  "correlationId": "demo-correlation-001"
}
```

## Expected logs

Order Service should log something similar to:

```text
[messaging] Connected to RabbitMQ exchange commerce.events
[order-service] Listening on port 3001
[messaging] Published event with routing key 'order.created' and correlationId 'demo-correlation-001'
[order-service] Created order '...' with correlationId 'demo-correlation-001'
```

Payment Service should log something similar to:

```text
[messaging] Connected to RabbitMQ exchange commerce.events
[messaging] Subscribed queue 'payment-service.order-created' to routing keys: order.created
[payment-service] Listening on port 3002
[payment-service] Received OrderCreated for order '...' with correlationId 'demo-correlation-001'
[messaging] Published event with routing key 'payment.authorized' and correlationId 'demo-correlation-001'
[payment-service] Authorized payment for order '...'
```

## RabbitMQ management UI

RabbitMQ management UI is available at:

```text
http://localhost:15672
```

Login:

```text
username: guest
password: guest
```

In the UI you can inspect:

- Exchanges
- Queues
- Bindings
- Published and consumed messages

## Type checking

Run TypeScript type checking:

```bash
npm run typecheck
```

## Build

Build all workspaces:

```bash
npm run build
```

## Project structure

```text
commerce-flow/
├── .gitignore
├── docker-compose.yml
├── package.json
├── tsconfig.base.json
├── README.md
├── docs/
│   └── architecture.md
├── shared/
│   ├── contracts/
│   │   ├── package.json
│   │   ├── tsconfig.json
│   │   └── src/
│   │       ├── events.ts
│   │       └── index.ts
│   └── messaging/
│       ├── package.json
│       ├── tsconfig.json
│       └── src/
│           ├── rabbitMqClient.ts
│           └── index.ts
└── services/
    ├── order-service/
    │   ├── package.json
    │   ├── tsconfig.json
    │   └── src/
    │       └── index.ts
    └── payment-service/
        ├── package.json
        ├── tsconfig.json
        └── src/
            └── index.ts
```

## Next planned steps

The next iterations will expand the system gradually.

Planned commits:

```text
Add inventory service for stock reservation
Add delivery service for delivery booking
Add notification service for customer updates
Add PostgreSQL persistence to order service
Add transactional outbox for reliable event publishing
Add idempotent event handling
Add retry and dead-letter queue handling
Add structured logging and observability
Add tests for event handlers
```

## Learning goals

This project is built to demonstrate the thinking behind event-driven backend systems.

Important architectural questions explored by the project:

- How do services communicate without direct dependencies?
- How do we model business events?
- How do we keep services loosely coupled?
- How do we trace a business process across services?
- What reliability problems appear when messages are handled asynchronously?
- Why do patterns like outbox, idempotency and dead-letter queues become important?

## Interview explanation

A short explanation of the current version:

```text
I started by building a small e-commerce order flow using Node.js, TypeScript and RabbitMQ.

The first version focuses on the basic event-driven flow: the Order Service publishes an OrderCreated event, and the Payment Service reacts to that event asynchronously.

I intentionally started simple because I wanted the commits to show the evolution of the architecture instead of pushing a finished demo in one large commit.

The next steps are to add persistence, transactional outbox, idempotent consumers, retries and dead-letter queues, because those are the patterns that become important when event-driven systems need to be reliable in production.
```