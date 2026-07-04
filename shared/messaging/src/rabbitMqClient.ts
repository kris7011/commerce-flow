import * as amqp from "amqplib";
import type { Channel, ConsumeMessage } from "amqplib";

type RabbitConnection = Awaited<ReturnType<typeof amqp.connect>>;

interface RabbitMqClientOptions {
    maxConnectionRetries?: number;
    retryDelayInMs?: number;
}

export class RabbitMqClient {
    private connection: RabbitConnection | null = null;
    private channel: Channel | null = null;

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

    async publish<TEvent extends { eventId: string; correlationId: string }>(
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

    async subscribe<TEvent>(
        queueName: string,
        routingKeys: string[],
        handler: (event: TEvent, message: ConsumeMessage) => Promise<void>
    ): Promise<void> {
        await this.connect();

        if (!this.channel) {
            throw new Error("RabbitMQ channel was not initialized.");
        }

        await this.channel.assertQueue(queueName, {
            durable: true
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

                await handler(event, message);

                this.channel?.ack(message);
            } catch (error) {
                console.error("[messaging] Failed to process message", error);

                this.channel?.nack(message, false, false);
            }
        });

        console.log(
            `[messaging] Subscribed queue '${queueName}' to routing keys: ${routingKeys.join(", ")}`
        );
    }

    async close(): Promise<void> {
        await this.channel?.close().catch(() => undefined);
        await this.connection?.close().catch(() => undefined);

        this.channel = null;
        this.connection = null;
    }
}

function sleep(milliseconds: number): Promise<void> {
    return new Promise(resolve => {
        setTimeout(resolve, milliseconds);
    });
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