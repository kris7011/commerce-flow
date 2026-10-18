import assert from "node:assert/strict";
import {
    randomUUID
} from "node:crypto";
import test from "node:test";
import * as amqp from "amqplib";
import {
    COMMERCE_TOPOLOGY
} from "@commerce-flow/contracts";
import type {
    AppLogger
} from "@commerce-flow/logging";
import {
    RabbitMqClient
} from "@commerce-flow/messaging";
import {
    createTestVirtualHost,
    type TestVirtualHost
} from "@commerce-flow/testing";
import {
    REPLAY_COUNT_HEADER,
    REPLAYED_FROM_HEADER,
    inspectDeadLetters,
    replayDeadLetters,
    resolveQueuePair
} from "../src/deadLetters.js";

/*
 * Runs against a real RabbitMQ broker, in a dedicated virtual host
 * that holds the production topology (declared by the services' own
 * RabbitMqClient), so queue arguments and dead-letter routing are
 * exactly those of the running system.
 */

const QUEUE =
    resolveQueuePair(
        "inventory-service.payment-authorized"
    );

const silentLogger: AppLogger = {
    info: () => undefined,
    warn: () => undefined,
    error: () => undefined,
    child: () => silentLogger
};

let virtualHost: TestVirtualHost;
let connection: amqp.ChannelModel;

test.before(async () => {
    virtualHost =
        await createTestVirtualHost();

    const client =
        new RabbitMqClient(
            virtualHost.url,
            "commerce.events",
            {
                topology: COMMERCE_TOPOLOGY
            },
            {
                logger: silentLogger
            }
        );

    await client.connect();
    await client.close();

    connection =
        await amqp.connect(
            virtualHost.url
        );
});

test.after(async () => {
    await connection?.close().catch(() => undefined);
    await virtualHost?.delete();
});

test.beforeEach(async () => {
    const channel =
        await connection.createChannel();

    await channel.purgeQueue(QUEUE.queueName);
    await channel.purgeQueue(QUEUE.deadLetterQueueName);
    await channel.close();
});

interface TestEvent {
    readonly eventId: string;
    readonly eventType: "PaymentAuthorized";
    readonly correlationId: string;
    readonly data: {
        readonly orderId: string;
    };
}

function createEvent(): TestEvent {
    return {
        eventId: randomUUID(),
        eventType: "PaymentAuthorized",
        correlationId: `test-${randomUUID()}`,
        data: {
            orderId: randomUUID()
        }
    };
}

/** Puts events into the dead-letter queue the way a consumer does: by rejecting them. */
async function deadLetter(
    events: readonly TestEvent[]
): Promise<void> {
    const channel =
        await connection.createConfirmChannel();

    for (const event of events) {
        channel.sendToQueue(
            QUEUE.queueName,
            Buffer.from(JSON.stringify(event)),
            {
                persistent: true,
                contentType: "application/json",
                messageId: event.eventId,
                correlationId: event.correlationId,
                type: event.eventType
            }
        );
    }

    await channel.waitForConfirms();

    for (let index = 0; index < events.length; index += 1) {
        const message =
            await channel.get(QUEUE.queueName, { noAck: false });

        assert.ok(message, "Expected a message to reject.");
        channel.nack(message, false, false);
    }

    await channel.close();

    await waitForCount(QUEUE.deadLetterQueueName, events.length);
}

async function messageCount(
    queueName: string
): Promise<number> {
    const channel =
        await connection.createChannel();

    try {
        return (await channel.checkQueue(queueName)).messageCount;
    } finally {
        await channel.close();
    }
}

async function waitForCount(
    queueName: string,
    expected: number,
    timeoutInMs = 5_000
): Promise<void> {
    const deadline =
        Date.now() + timeoutInMs;

    while (await messageCount(queueName) !== expected) {
        if (Date.now() > deadline) {
            assert.fail(
                `${queueName} did not reach ${expected} message(s).`
            );
        }

        await new Promise(resolve => setTimeout(resolve, 50));
    }
}

test(
    "lists dead letters with their death reason and leaves them in place",
    {
        timeout: 20_000
    },
    async () => {
        const events = [createEvent(), createEvent()];

        await deadLetter(events);

        const inspection =
            await inspectDeadLetters(connection, QUEUE);

        assert.equal(inspection.total, 2);

        assert.deepEqual(
            inspection.messages.map(summary => [
                summary.messageId,
                summary.eventType,
                summary.orderId,
                summary.correlationId,
                summary.death?.reason,
                summary.death?.queue
            ]),
            events.map(event => [
                event.eventId,
                "PaymentAuthorized",
                event.data.orderId,
                event.correlationId,
                "rejected",
                QUEUE.queueName
            ])
        );

        // Listing again shows the same messages in the same order.
        const again =
            await inspectDeadLetters(connection, QUEUE);

        assert.deepEqual(
            again.messages.map(summary => summary.messageId),
            events.map(event => event.eventId)
        );

        assert.equal(await messageCount(QUEUE.deadLetterQueueName), 2);
    }
);

test(
    "replays dead letters to their original queue with their identity intact",
    {
        timeout: 20_000
    },
    async () => {
        const events = [createEvent(), createEvent()];

        await deadLetter(events);

        const result =
            await replayDeadLetters(connection, QUEUE);

        assert.deepEqual(result, {
            replayed: 2,
            remaining: 0
        });

        const channel =
            await connection.createChannel();

        try {
            for (const event of events) {
                const message =
                    await channel.get(QUEUE.queueName, { noAck: true });

                assert.ok(message, "Expected the replayed message.");
                assert.deepEqual(JSON.parse(message.content.toString("utf8")), event);
                assert.equal(message.properties.messageId, event.eventId);
                assert.equal(message.properties.correlationId, event.correlationId);
                assert.equal(message.properties.type, event.eventType);
                assert.equal(message.properties.deliveryMode, 2);

                const headers =
                    message.properties.headers ?? {};

                assert.equal(headers[REPLAYED_FROM_HEADER], QUEUE.deadLetterQueueName);
                assert.equal(headers[REPLAY_COUNT_HEADER], 1);
                assert.equal(headers["x-death"], undefined);
            }

            // Delivered to the consumer queue only, not to other queues.
            for (const queue of COMMERCE_TOPOLOGY) {
                if (queue.queueName !== QUEUE.queueName) {
                    assert.equal((await channel.checkQueue(queue.queueName)).messageCount, 0);
                }
            }
        } finally {
            await channel.close();
        }
    }
);

test(
    "replays at most --limit messages",
    {
        timeout: 20_000
    },
    async () => {
        await deadLetter([createEvent(), createEvent(), createEvent()]);

        const result =
            await replayDeadLetters(connection, QUEUE, { limit: 1 });

        assert.deepEqual(result, {
            replayed: 1,
            remaining: 2
        });

        assert.equal(await messageCount(QUEUE.queueName), 1);
    }
);

test(
    "terminates when replayed messages fail again and return to the dead-letter queue",
    {
        timeout: 20_000
    },
    async () => {
        await deadLetter([createEvent(), createEvent(), createEvent()]);

        // A consumer that still rejects everything (the cause is not fixed).
        const consumerChannel =
            await connection.createChannel();

        await consumerChannel.consume(
            QUEUE.queueName,
            message => {
                if (message) {
                    consumerChannel.nack(message, false, false);
                }
            }
        );

        try {
            const result =
                await replayDeadLetters(connection, QUEUE);

            assert.equal(result.replayed, 3);

            await waitForCount(QUEUE.deadLetterQueueName, 3);

            const inspection =
                await inspectDeadLetters(connection, QUEUE);

            assert.deepEqual(
                inspection.messages.map(summary => summary.replayCount),
                [1, 1, 1]
            );
        } finally {
            await consumerChannel.close();
        }
    }
);
