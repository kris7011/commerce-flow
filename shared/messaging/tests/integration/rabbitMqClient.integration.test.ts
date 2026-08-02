import assert from "node:assert/strict";
import {
    randomUUID
} from "node:crypto";
import test from "node:test";
import * as amqp from "amqplib";
import {
    RabbitMqClient
} from "../../src/rabbitMqClient.js";

interface IntegrationTestEvent {
    eventId: string;
    eventType: "MessagingIntegrationTestEvent";
    correlationId: string;
    occurredAt: string;
    data: {
        value: string;
    };
}

interface ReceivedMessage {
    event: IntegrationTestEvent;
    messageId: string | undefined;
    correlationId: string | undefined;
    contentType: string | undefined;
    deliveryMode: number | undefined;
}

const rabbitMqUrl =
    process.env.RABBITMQ_URL ??
    "amqp://guest:guest@localhost:5672";

test(
    "publishes and consumes an event through a real RabbitMQ broker",
    {
        timeout: 15_000
    },
    async () => {
        const uniqueId =
            randomUUID().replaceAll("-", "");

        const exchangeName =
            `commerce.tests.${uniqueId}`;

        const queueName =
            `commerce.tests.queue.${uniqueId}`;

        const routingKey =
            `commerce.tests.event.${uniqueId}`;

        const subscriber =
            new RabbitMqClient(
                rabbitMqUrl,
                exchangeName,
                {
                    maxConnectionRetries: 20,
                    retryDelayInMs: 250
                }
            );

        const publisher =
            new RabbitMqClient(
                rabbitMqUrl,
                exchangeName,
                {
                    maxConnectionRetries: 20,
                    retryDelayInMs: 250
                }
            );

        const expectedEvent:
            IntegrationTestEvent = {
                eventId: randomUUID(),
                eventType:
                    "MessagingIntegrationTestEvent",
                correlationId: randomUUID(),
                occurredAt:
                    new Date().toISOString(),
                data: {
                    value:
                        "published through RabbitMQ"
                }
            };

        let topologyWasCreated = false;

        const receivedMessagePromise =
            createReceivedMessagePromise();

        try {
            await subscriber.subscribe<
                IntegrationTestEvent
            >(
                queueName,
                [routingKey],
                async (
                    event,
                    message
                ) => {
                    receivedMessagePromise.resolve(
                        {
                            event,
                            messageId:
                                message.properties
                                    .messageId,
                            correlationId:
                                message.properties
                                    .correlationId,
                            contentType:
                                message.properties
                                    .contentType,
                            deliveryMode:
                                message.properties
                                    .deliveryMode
                        }
                    );
                }
            );

            topologyWasCreated = true;

            await publisher.publish(
                routingKey,
                expectedEvent
            );

            const receivedMessage =
                await withTimeout(
                    receivedMessagePromise.promise,
                    5_000
                );

            assert.deepEqual(
                receivedMessage.event,
                expectedEvent
            );

            assert.equal(
                receivedMessage.messageId,
                expectedEvent.eventId
            );

            assert.equal(
                receivedMessage.correlationId,
                expectedEvent.correlationId
            );

            assert.equal(
                receivedMessage.contentType,
                "application/json"
            );

            assert.equal(
                receivedMessage.deliveryMode,
                2
            );
        } finally {
            await Promise.allSettled([
                subscriber.close(),
                publisher.close()
            ]);

            if (topologyWasCreated) {
                await deleteTestTopology(
                    rabbitMqUrl,
                    exchangeName,
                    queueName
                );
            }
        }
    }
);

function createReceivedMessagePromise(): {
    promise: Promise<ReceivedMessage>;
    resolve: (
        receivedMessage: ReceivedMessage
    ) => void;
} {
    let resolvePromise:
        (
            receivedMessage: ReceivedMessage
        ) => void = () => undefined;

    const promise =
        new Promise<ReceivedMessage>(
            resolve => {
                resolvePromise = resolve;
            }
        );

    return {
        promise,
        resolve: resolvePromise
    };
}

function withTimeout<T>(
    promise: Promise<T>,
    timeoutInMs: number
): Promise<T> {
    return new Promise<T>(
        (
            resolve,
            reject
        ) => {
            const timeout =
                setTimeout(
                    () => {
                        reject(
                            new Error(
                                "Timed out while " +
                                "waiting for a " +
                                "RabbitMQ message."
                            )
                        );
                    },
                    timeoutInMs
                );

            promise.then(
                value => {
                    clearTimeout(timeout);
                    resolve(value);
                },
                error => {
                    clearTimeout(timeout);
                    reject(error);
                }
            );
        }
    );
}

async function deleteTestTopology(
    url: string,
    exchangeName: string,
    queueName: string
): Promise<void> {
    const connection =
        await amqp.connect(url);

    const channel =
        await connection.createChannel();

    try {
        await channel.deleteQueue(
            `${queueName}.dead-letter`
        );

        await channel.deleteQueue(
            queueName
        );

        await channel.deleteExchange(
            `${exchangeName}.dead-letter`
        );

        await channel.deleteExchange(
            exchangeName
        );
    } finally {
        await channel.close();
        await connection.close();
    }
}
