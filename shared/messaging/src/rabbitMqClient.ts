import * as amqp from "amqplib";
import type {
    ConsumeMessage
} from "amqplib";
import {
    createStructuredLogger,
    type AppLogger
} from "@commerce-flow/logging";

export interface RabbitMqConnection {
    createChannel():
        Promise<RabbitMqChannel>;

    onClose(
        handler: () => void
    ): void;

    close(): Promise<void>;
}

export interface RabbitMqChannel {
    assertExchange(
        exchangeName: string,
        exchangeType: string,
        options: {
            durable: boolean;
        }
    ): Promise<unknown>;

    assertQueue(
        queueName: string,
        options: {
            durable: boolean;
            arguments?:
            Record<string, unknown>;
        }
    ): Promise<unknown>;

    bindQueue(
        queueName: string,
        exchangeName: string,
        routingKey: string
    ): Promise<unknown>;

    publish(
        exchangeName: string,
        routingKey: string,
        content: Buffer,
        options: {
            contentType: string;
            deliveryMode: number;
            messageId: string;
            correlationId: string;
        }
    ): boolean;

    consume(
        queueName: string,
        onMessage: (
            message:
                ConsumeMessage | null
        ) => void | Promise<void>
    ): Promise<unknown>;

    ack(
        message: ConsumeMessage
    ): void;

    nack(
        message: ConsumeMessage,
        allUpTo?: boolean,
        requeue?: boolean
    ): void;

    onClose(
        handler: () => void
    ): void;

    close(): Promise<void>;
}

export type RabbitMqLogger =
    AppLogger;

export interface RabbitMqClientOptions {
    maxConnectionRetries?: number;
    retryDelayInMs?: number;
}

export interface RabbitMqClientDependencies {
    connect(
        url: string
    ): Promise<RabbitMqConnection>;

    sleep(
        milliseconds: number
    ): Promise<void>;

    logger: RabbitMqLogger;
}

interface MessageEvent {
    eventId: string;
    correlationId: string;
}

export class RabbitMqClient {
    private connection:
        RabbitMqConnection | null = null;

    private channel:
        RabbitMqChannel | null = null;

    private readonly processedEventIdsByQueueName =
        new Map<
            string,
            Set<string>
        >();

    private readonly connectToBroker:
        RabbitMqClientDependencies[
        "connect"
        ];

    private readonly delay:
        RabbitMqClientDependencies[
        "sleep"
        ];

    private readonly logger:
        RabbitMqLogger;

    constructor(
        private readonly url: string,
        private readonly exchangeName:
            string = "commerce.events",
        private readonly options:
            RabbitMqClientOptions = {},
        dependencies:
            Partial<
                RabbitMqClientDependencies
            > = {}
    ) {
        this.connectToBroker =
            dependencies.connect ??
            connectToRabbitMq;

        this.delay =
            dependencies.sleep ??
            sleep;

        this.logger =
            dependencies.logger ??
            createStructuredLogger(
                "messaging"
            );
    }

    isReady(): boolean {
        return (
            this.connection !== null &&
            this.channel !== null
        );
    }

    async connect(): Promise<void> {
        if (this.channel) {
            return;
        }

        if (this.connection) {
            await this.close();
        }

        const maxConnectionRetries =
            this.options
                .maxConnectionRetries ??
            10;

        const retryDelayInMs =
            this.options
                .retryDelayInMs ??
            2000;

        let lastError: unknown;

        for (
            let attempt = 1;
            attempt <=
            maxConnectionRetries;
            attempt += 1
        ) {
            try {
                const connection =
                    await this
                        .connectToBroker(
                            this.url
                        );

                this.connection =
                    connection;

                connection.onClose(
                    () => {
                        if (
                            this.connection !==
                            connection
                        ) {
                            return;
                        }

                        this.connection =
                            null;

                        this.channel =
                            null;

                        this.logger.warn(
                            "RabbitMQ connection closed",
                            {
                                exchangeName:
                                    this.exchangeName
                            }
                        );
                    }
                );

                const channel =
                    await connection
                        .createChannel();

                this.channel =
                    channel;

                channel.onClose(
                    () => {
                        if (
                            this.channel !==
                            channel
                        ) {
                            return;
                        }

                        this.channel =
                            null;

                        this.logger.warn(
                            "RabbitMQ channel closed",
                            {
                                exchangeName:
                                    this.exchangeName
                            }
                        );
                    }
                );

                await channel
                    .assertExchange(
                        this.exchangeName,
                        "topic",
                        {
                            durable: true
                        }
                    );

                this.logger.info(
                    "Connected to RabbitMQ exchange",
                    {
                        exchangeName:
                            this.exchangeName
                    }
                );

                return;
            } catch (error) {
                lastError =
                    error;

                this.logger.warn(
                    "RabbitMQ connection attempt failed",
                    {
                        attempt,
                        maxConnectionRetries,
                        errorMessage:
                            getErrorMessage(
                                error
                            )
                    }
                );

                await this.close();

                if (
                    attempt <
                    maxConnectionRetries
                ) {
                    await this.delay(
                        retryDelayInMs
                    );
                }
            }
        }

        throw new Error(
            `[messaging] Could not connect to ` +
            `RabbitMQ after ` +
            `${maxConnectionRetries} attempts. ` +
            `Last error: ` +
            `${getErrorMessage(lastError)}`
        );
    }

    async publish<
        TEvent extends MessageEvent
    >(
        routingKey: string,
        event: TEvent
    ): Promise<void> {
        await this.connect();

        if (!this.channel) {
            throw new Error(
                "RabbitMQ channel was not initialized."
            );
        }

        const body =
            Buffer.from(
                JSON.stringify(
                    event
                )
            );

        this.channel.publish(
            this.exchangeName,
            routingKey,
            body,
            {
                contentType:
                    "application/json",
                deliveryMode:
                    2,
                messageId:
                    event.eventId,
                correlationId:
                    event.correlationId
            }
        );

        this.logger.info(
            "Published event",
            {
                exchangeName:
                    this.exchangeName,
                routingKey,
                eventId:
                    event.eventId,
                correlationId:
                    event.correlationId
            }
        );
    }

    async subscribe<
        TEvent extends MessageEvent
    >(
        queueName: string,
        routingKeys: string[],
        handler: (
            event: TEvent,
            message:
                ConsumeMessage
        ) => Promise<void>
    ): Promise<void> {
        await this.connect();

        if (!this.channel) {
            throw new Error(
                "RabbitMQ channel was not initialized."
            );
        }

        const channel =
            this.channel;

        const deadLetterExchangeName =
            `${this.exchangeName}` +
            `.dead-letter`;

        const deadLetterQueueName =
            `${queueName}` +
            `.dead-letter`;

        const deadLetterRoutingKey =
            `${queueName}` +
            `.dead-letter`;

        await channel
            .assertExchange(
                deadLetterExchangeName,
                "topic",
                {
                    durable: true
                }
            );

        await channel
            .assertQueue(
                deadLetterQueueName,
                {
                    durable: true
                }
            );

        await channel
            .bindQueue(
                deadLetterQueueName,
                deadLetterExchangeName,
                deadLetterRoutingKey
            );

        await channel
            .assertQueue(
                queueName,
                {
                    durable: true,
                    arguments: {
                        "x-dead-letter-exchange":
                            deadLetterExchangeName,
                        "x-dead-letter-routing-key":
                            deadLetterRoutingKey
                    }
                }
            );

        for (
            const routingKey
            of routingKeys
        ) {
            await channel
                .bindQueue(
                    queueName,
                    this.exchangeName,
                    routingKey
                );
        }

        await channel
            .consume(
                queueName,
                async message => {
                    if (!message) {
                        return;
                    }

                    try {
                        const event =
                            JSON.parse(
                                message
                                    .content
                                    .toString()
                            ) as TEvent;

                        const eventId =
                            getRequiredEventId(
                                event
                            );

                        if (
                            this
                                .hasProcessedEvent(
                                    queueName,
                                    eventId
                                )
                        ) {
                            this.logger.warn(
                                "Duplicate event received",
                                {
                                    eventId,
                                    queueName,
                                    correlationId:
                                        event
                                            .correlationId,
                                    action:
                                        "acknowledge-without-reprocessing"
                                }
                            );

                            channel.ack(
                                message
                            );

                            return;
                        }

                        await handler(
                            event,
                            message
                        );

                        this
                            .markEventAsProcessed(
                                queueName,
                                eventId
                            );

                        channel.ack(
                            message
                        );
                    } catch (error) {
                        this.logger.error(
                            "Failed to process message",
                            error,
                            {
                                queueName,
                                deadLetterQueueName
                            }
                        );

                        channel.nack(
                            message,
                            false,
                            false
                        );
                    }
                }
            );

        this.logger.info(
            "Subscribed queue",
            {
                queueName,
                routingKeys
            }
        );

        this.logger.info(
            "Configured dead-letter queue",
            {
                queueName,
                deadLetterQueueName,
                deadLetterExchangeName,
                deadLetterRoutingKey
            }
        );
    }

    async close(): Promise<void> {
        const channel =
            this.channel;

        const connection =
            this.connection;

        this.channel =
            null;

        this.connection =
            null;

        await channel
            ?.close()
            .catch(
                () => undefined
            );

        await connection
            ?.close()
            .catch(
                () => undefined
            );
    }

    private hasProcessedEvent(
        queueName: string,
        eventId: string
    ): boolean {
        const processedEventIds =
            this
                .processedEventIdsByQueueName
                .get(
                    queueName
                );

        return (
            processedEventIds
                ?.has(
                    eventId
                ) ??
            false
        );
    }

    private markEventAsProcessed(
        queueName: string,
        eventId: string
    ): void {
        const existingSet =
            this
                .processedEventIdsByQueueName
                .get(
                    queueName
                );

        if (existingSet) {
            existingSet.add(
                eventId
            );

            return;
        }

        this
            .processedEventIdsByQueueName
            .set(
                queueName,
                new Set([
                    eventId
                ])
            );
    }
}

async function connectToRabbitMq(
    url: string
): Promise<RabbitMqConnection> {
    const connection =
        await amqp.connect(
            url
        );

    return {
        createChannel:
            async () => {
                const channel =
                    await connection
                        .createChannel();

                return {
                    assertExchange:
                        async (
                            exchangeName,
                            exchangeType,
                            options
                        ) => {
                            return await channel
                                .assertExchange(
                                    exchangeName,
                                    exchangeType,
                                    options
                                );
                        },

                    assertQueue:
                        async (
                            queueName,
                            options
                        ) => {
                            return await channel
                                .assertQueue(
                                    queueName,
                                    options
                                );
                        },

                    bindQueue:
                        async (
                            queueName,
                            exchangeName,
                            routingKey
                        ) => {
                            return await channel
                                .bindQueue(
                                    queueName,
                                    exchangeName,
                                    routingKey
                                );
                        },

                    publish:
                        (
                            exchangeName,
                            routingKey,
                            content,
                            options
                        ) => {
                            return channel
                                .publish(
                                    exchangeName,
                                    routingKey,
                                    content,
                                    options
                                );
                        },

                    consume:
                        async (
                            queueName,
                            onMessage
                        ) => {
                            return await channel
                                .consume(
                                    queueName,
                                    onMessage
                                );
                        },

                    ack:
                        message => {
                            channel.ack(
                                message
                            );
                        },

                    nack:
                        (
                            message,
                            allUpTo,
                            requeue
                        ) => {
                            channel.nack(
                                message,
                                allUpTo,
                                requeue
                            );
                        },

                    onClose:
                        handler => {
                            channel.on(
                                "close",
                                handler
                            );
                        },

                    close:
                        async () => {
                            await channel
                                .close();
                        }
                };
            },

        onClose:
            handler => {
                connection.on(
                    "close",
                    handler
                );
            },

        close:
            async () => {
                await connection
                    .close();
            }
    };
}

function sleep(
    milliseconds: number
): Promise<void> {
    return new Promise(
        resolve => {
            setTimeout(
                resolve,
                milliseconds
            );
        }
    );
}

function getRequiredEventId(
    event: MessageEvent
): string {
    if (
        typeof event.eventId !==
        "string" ||
        event.eventId.length === 0
    ) {
        throw new Error(
            "Message does not contain " +
            "a valid eventId."
        );
    }

    return event.eventId;
}

function getErrorMessage(
    error: unknown
): string {
    if (
        error instanceof Error &&
        error.message.length > 0
    ) {
        return error.message;
    }

    if (
        typeof error ===
        "object" &&
        error !== null
    ) {
        return JSON.stringify(
            error
        );
    }

    const message =
        String(
            error
        );

    if (
        message.length > 0
    ) {
        return message;
    }

    return "Unknown connection error";
}