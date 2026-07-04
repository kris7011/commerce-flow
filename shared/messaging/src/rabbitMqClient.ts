import * as amqp from "amqplib";
import type { Channel, ConsumeMessage } from "amqplib";

type RabbitConnection = Awaited<ReturnType<typeof amqp.connect>>;

export class RabbitMqClient {
    private connection: RabbitConnection | null = null;
    private channel: Channel | null = null;

    constructor(
        private readonly url: string,
        private readonly exchangeName: string = "commerce.events"
    ) { }

    async connect(): Promise<void> {
        if (this.channel) {
            return;
        }

        this.connection = await amqp.connect(this.url);
        this.channel = await this.connection.createChannel();

        await this.channel.assertExchange(this.exchangeName, "topic", {
            durable: true
        });

        console.log(`[messaging] Connected to RabbitMQ exchange ${this.exchangeName}`);
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
        await this.channel?.close();
        await this.connection?.close();

        this.channel = null;
        this.connection = null;
    }
}