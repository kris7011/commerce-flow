# Architecture

CommerceFlow is built as a small event-driven microservice demo in an e-commerce domain.

The system is split into services based on business responsibility.

## Service boundaries

### Order Service

Owns order creation.

Responsibilities:

- Receive order requests
- Validate order input
- Create order ID
- Calculate total amount
- Publish `OrderCreated`

Order Service does not call Payment Service, Inventory Service, Delivery Service or Notification Service directly.

### Payment Service

Owns payment authorization.

Responsibilities:

- Listen to `OrderCreated`
- Simulate payment authorization
- Publish `PaymentAuthorized`

### Inventory Service

Owns stock reservation.

Responsibilities:

- Listen to `PaymentAuthorized`
- Check stock availability
- Reserve stock when available
- Publish `InventoryReserved`
- Publish `InventoryReservationFailed` when stock is insufficient

### Delivery Service

Owns delivery booking.

Responsibilities:

- Listen to `InventoryReserved`
- Simulate delivery booking
- Publish `DeliveryBooked`

### Notification Service

Owns customer notifications.

Responsibilities:

- Listen to `DeliveryBooked`
- Listen to `InventoryReservationFailed`
- Create customer-facing notification records

## Event-driven flow

The services communicate through domain events.

A service publishes that something has happened. It does not need to know who reacts to it.

Example:

```text
Order Service publishes OrderCreated.
Payment Service listens to OrderCreated.
```

This avoids direct coupling between Order Service and Payment Service.

## Why not direct HTTP calls?

A direct flow could look like this:

```text
Order Service -> Payment Service -> Inventory Service -> Delivery Service -> Notification Service
```

This makes services tightly coupled.

In CommerceFlow, services communicate through events:

```text
OrderCreated
PaymentAuthorized
InventoryReserved
DeliveryBooked
```

This allows new services to be added without changing the original publisher.

Notification Service is an example of this. It was added later and simply listens to existing events.

## Event contracts

Event contracts are stored in:

```text
shared/contracts/src/events.ts
```

This keeps event shape consistent across services.

For this demo, shared contracts are acceptable because the project is a monorepo.

In a larger distributed system, alternatives could include:

- versioned contracts
- schema registry
- published internal packages
- protobuf or Avro schemas

## Messaging abstraction

RabbitMQ logic is stored in:

```text
shared/messaging/src/rabbitMqClient.ts
```

This keeps infrastructure code out of the services.

The services do not need to know how to:

- connect to RabbitMQ
- assert exchanges
- assert queues
- bind queues
- configure dead-letter queues
- handle idempotency

They only call:

```ts
publish(...)
subscribe(...)
```

## Topic exchange

The project uses a RabbitMQ topic exchange:

```text
commerce.events
```

Topic routing keys make it possible to route events by business meaning:

```text
order.created
payment.authorized
inventory.reserved
inventory.reservation.failed
delivery.booked
```

## Correlation ID

Each event includes a `correlationId`.

The same correlation ID flows through all related events.

This makes it easier to trace one business process across several services.