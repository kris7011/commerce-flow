import * as amqp from "amqplib";
import {
    readEnvelope,
    type ObservedMessage
} from "./timeline.js";

const EXCHANGE_NAME =
    "commerce.events";

/**
 * Observes every event on the `commerce.events` exchange through a
 * temporary, exclusive queue bound to `#`. The broker deletes the queue
 * when the tap disconnects.
 *
 * Side effect to be aware of: while the tap is connected, every routing
 * key has at least one bound queue, so the services' mandatory
 * publishing cannot detect a missing binding. That is fine for a demo
 * and is why the tap is a developer tool, not part of the services.
 */
export class EventTap {
    readonly messages: ObservedMessage[] = [];

    private readonly listeners =
        new Set<() => void>();

    private constructor(
        private readonly connection: amqp.ChannelModel
    ) {
    }

    static async open(
        rabbitMqUrl: string
    ): Promise<EventTap> {
        const connection =
            await amqp.connect(
                rabbitMqUrl
            );

        try {
            const tap =
                new EventTap(
                    connection
                );

            await tap.subscribe();

            return tap;
        } catch (error) {
            await connection
                .close()
                .catch(() => undefined);

            throw error;
        }
    }

    /**
     * Resolves true as soon as the predicate holds for the messages
     * observed so far, or false once the timeout expires.
     */
    async waitFor(
        predicate: (messages: readonly ObservedMessage[]) => boolean,
        timeoutInMs: number
    ): Promise<boolean> {
        if (predicate(this.messages)) {
            return true;
        }

        return await new Promise<boolean>(resolve => {
            const listener = () => {
                if (predicate(this.messages)) {
                    finish(true);
                }
            };

            const timer =
                setTimeout(
                    () => finish(false),
                    timeoutInMs
                );

            const finish = (result: boolean) => {
                clearTimeout(timer);
                this.listeners.delete(listener);
                resolve(result);
            };

            this.listeners.add(listener);
        });
    }

    /**
     * Publishes a raw event with publisher confirms, the way the
     * services do. Used by the demo to inject a duplicate.
     */
    async publish(
        routingKey: string,
        event: {
            readonly eventId: string;
            readonly eventType: string;
            readonly correlationId: string;
        }
    ): Promise<void> {
        const channel =
            await this.connection.createConfirmChannel();

        try {
            channel.publish(
                EXCHANGE_NAME,
                routingKey,
                Buffer.from(
                    JSON.stringify(event)
                ),
                {
                    persistent: true,
                    contentType: "application/json",
                    messageId: event.eventId,
                    correlationId: event.correlationId,
                    type: event.eventType
                }
            );

            await channel.waitForConfirms();
        } finally {
            await channel.close();
        }
    }

    async close(): Promise<void> {
        await this.connection
            .close()
            .catch(() => undefined);
    }

    private async subscribe(): Promise<void> {
        const channel =
            await this.connection.createChannel();

        // Fails if the exchange does not exist yet, i.e. no service has
        // connected to this broker so far.
        await channel.checkExchange(
            EXCHANGE_NAME
        );

        const {
            queue
        } =
            await channel.assertQueue(
                "",
                {
                    exclusive: true,
                    autoDelete: true,
                    durable: false
                }
            );

        await channel.bindQueue(
            queue,
            EXCHANGE_NAME,
            "#"
        );

        await channel.consume(
            queue,
            message => {
                if (!message) {
                    return;
                }

                this.record(message);
            },
            {
                noAck: true
            }
        );
    }

    private record(
        message: amqp.ConsumeMessage
    ): void {
        let body: unknown;

        try {
            body = JSON.parse(
                message.content.toString("utf8")
            );
        } catch {
            body = undefined;
        }

        const envelope =
            readEnvelope(body) ?? {
                eventId:
                    String(message.properties.messageId ?? "unknown"),
                eventType:
                    String(message.properties.type ?? "unknown"),
                correlationId:
                    String(message.properties.correlationId ?? ""),
                orderId: undefined
            };

        this.messages.push({
            receivedAt:
                Date.now(),
            routingKey:
                message.fields.routingKey,
            envelope,
            body
        });

        for (const listener of [...this.listeners]) {
            listener();
        }
    }
}
