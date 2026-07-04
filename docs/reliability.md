# Reliability Notes

This document explains the reliability patterns currently implemented in CommerceFlow.

## RabbitMQ connection retry

Problem:

A service may start before RabbitMQ is ready to accept connections.

Docker can report that the container is running before RabbitMQ has fully completed startup.

Solution:

`RabbitMqClient` retries the initial connection several times before failing.

This prevents services from crashing immediately during local startup.

## Dead-letter queues

Problem:

A consumer may receive a message it cannot process.

Examples:

- invalid JSON
- missing eventId
- unexpected schema
- bug in handler
- temporary downstream issue

Solution:

Each consumer queue is configured with a dead-letter exchange and dead-letter queue.

If processing fails, the message is rejected with:

```ts
nack(message, false, false)
```

This means:

```text
do not acknowledge the message
do not requeue the message
route it to the dead-letter exchange
```

Example:

```text
payment-service.order-created
  -> payment-service.order-created.dead-letter
```

This allows failed messages to be inspected later.

## Idempotent message handling

Problem:

Messages may be delivered more than once.

If the same message is processed twice, it may cause duplicate side effects.

Examples:

- payment is authorized twice
- stock is reserved twice
- delivery is booked twice
- customer receives duplicate notifications

Solution:

`RabbitMqClient` tracks processed event IDs per queue.

If the same event ID appears again on the same queue, the message is acknowledged without running the handler.

Current implementation:

```text
in-memory Map<queueName, Set<eventId>>
```

This demonstrates the concept.

Production implementation should persist processed events in a database table with a unique constraint.

## Why idempotency is per queue

The same event may be handled by multiple consumers.

For example:

```text
OrderCreated
  -> Payment Service
  -> Analytics Service
  -> Notification Service
```

It would be wrong to treat the event as globally processed after one service handles it.

Idempotency should prevent duplicate handling within the same consumer, not prevent other consumers from reacting to the same event.

## What is not implemented yet

The project does not yet implement:

- persistent idempotency
- transactional outbox
- message retry with backoff
- poison message replay tooling
- persistent service databases

These are planned improvements.