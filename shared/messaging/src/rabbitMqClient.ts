import * as amqp from "amqplib";
import type { Channel, ConsumeMessage } from "amqplib";

type RabbitConnection = Awaited<ReturnType<typeof amqp.connect>>;

interface RabbitMqClientOptions {
    maxConnectionRetries?: number;
    retryDelayInMs?: number;
}

interface MessageEvent {
    eventId: string;
    correlationId: string;
}

export class RabbitMqClient {
    private connection: RabbitConnection | null = null;
    private channel: Channel | null = null;
    private readonly processedEventIdsByQueueName = new Map<string, Set<string>>();

    constructor(
        private readonly url: string,
        private readonly exchangeName: string = "commerce.events",
        private readonly options: RabbitMqClientOptions = {}
    ) { }

    async connect(): Promise<void> {
        if (this.channel) {
            return;
        }

        const maxConnectionRetries = this.options.maxConnectionRetries ?? 10;
        const retryDelayInMs = this.options.retryDelayInMs ?? 2000;

        let lastError: unknown;

        for (let attempt = 1; attempt <= maxConnectionRetries; attempt++) {
            try {
                this.connection = await amqp.connect(this.url);
                this.channel = await this.connection.createChannel();

                await this.channel.assertExchange(this.exchangeName, "topic", {
                    durable: true
                });

                console.log(
                    `[messaging] Connected to RabbitMQ exchange ${this.exchangeName}`
                );

                return;
            } catch (error) {
                lastError = error;

                console.warn(
                    `[messaging] RabbitMQ connection attempt ${attempt}/${maxConnectionRetries} failed: ${getErrorMessage(error)}`
                );

                await this.close();

                if (attempt < maxConnectionRetries) {
                    await sleep(retryDelayInMs);
                }
            }
        }

        throw new Error(
            `[messaging] Could not connect to RabbitMQ after ${maxConnectionRetries} attempts. Last error: ${getErrorMessage(lastError)}`
        );
    }

    async publish<TEvent extends MessageEvent>(
        routingKey: string,
        event: TEvent
    ): Promise<void> {
        await this.connect();

        if (!this.channel) {
            throw new Error("RabbitMQ channel was not initialized.");
        }

        const body = Buffer.from(JSON.stringify(event));

        this.channel.publish(this.exchangeName, routingKey, body, {
            contentType: "application/json",
            deliveryMode: 2,
            messageId: event.eventId,
            correlationId: event.correlationId
        });

        console.log(
            `[messaging] Published event with routing key '${routingKey}' and correlationId '${event.correlationId}'`
        );
    }

    async subscribe<TEvent extends MessageEvent>(
        queueName: string,
        routingKeys: string[],
        handler: (event: TEvent, message: ConsumeMessage) => Promise<void>
    ): Promise<void> {
        await this.connect();

        if (!this.channel) {
            throw new Error("RabbitMQ channel was not initialized.");
        }

        const deadLetterExchangeName = `${this.exchangeName}.dead-letter`;
        const deadLetterQueueName = `${queueName}.dead-letter`;
        const deadLetterRoutingKey = `${queueName}.dead-letter`;

        await this.channel.assertExchange(deadLetterExchangeName, "topic", {
            durable: true
        });

        await this.channel.assertQueue(deadLetterQueueName, {
            durable: true
        });

        await this.channel.bindQueue(
            deadLetterQueueName,
            deadLetterExchangeName,
            deadLetterRoutingKey
        );

        await this.channel.assertQueue(queueName, {
            durable: true,
            arguments: {
                "x-dead-letter-exchange": deadLetterExchangeName,
                "x-dead-letter-routing-key": deadLetterRoutingKey
            }
        });

        for (const routingKey of routingKeys) {
            await this.channel.bindQueue(queueName, this.exchangeName, routingKey);
        }

        await this.channel.consume(queueName, async message => {
            if (!message) {
                return;
            }

            try {
                const event = JSON.parse(message.content.toString()) as TEvent;
                const eventId = getRequiredEventId(event);

                if (this.hasProcessedEvent(queueName, eventId)) {
                    console.warn(
                        `[messaging] Duplicate event '${eventId}' received on queue '${queueName}'. Acknowledging without reprocessing.`
                    );

                    this.channel?.ack(message);
                    return;
                }

                await handler(event, message);

                this.markEventAsProcessed(queueName, eventId);

                this.channel?.ack(message);
            } catch (error) {
                console.error(
                    `[messaging] Failed to process message from queue '${queueName}'. Moving message to dead-letter queue '${deadLetterQueueName}'.`,
                    error
                );

                this.channel?.nack(message, false, false);
            }
        });

        console.log(
            `[messaging] Subscribed queue '${queueName}' to routing keys: ${routingKeys.join(", ")}`
        );

        console.log(
            `[messaging] Dead-letter queue '${deadLetterQueueName}' configured for queue '${queueName}'`
        );
    }

    async close(): Promise<void> {
        await this.channel?.close().catch(() => undefined);
        await this.connection?.close().catch(() => undefined);

        this.channel = null;
        this.connection = null;
    }

    private hasProcessedEvent(queueName: string, eventId: string): boolean {
        const processedEventIds = this.processedEventIdsByQueueName.get(queueName);

        return processedEventIds?.has(eventId) ?? false;
    }

    private markEventAsProcessed(queueName: string, eventId: string): void {
        const existingSet = this.processedEventIdsByQueueName.get(queueName);

        if (existingSet) {
            existingSet.add(eventId);
            return;
        }

        this.processedEventIdsByQueueName.set(queueName, new Set([eventId]));
    }
}

function sleep(milliseconds: number): Promise<void> {
    return new Promise(resolve => {
        setTimeout(resolve, milliseconds);
    });
}

function getRequiredEventId(event: MessageEvent): string {
    if (typeof event.eventId !== "string" || event.eventId.length === 0) {
        throw new Error("Message does not contain a valid eventId.");
    }

    return event.eventId;
}

function getErrorMessage(error: unknown): string {
    if (error instanceof Error && error.message.length > 0) {
        return error.message;
    }

    if (typeof error === "object" && error !== null) {
        return JSON.stringify(error);
    }

    const message = String(error);

    if (message.length > 0) {
        return message;
    }

    return "Unknown connection error";
}