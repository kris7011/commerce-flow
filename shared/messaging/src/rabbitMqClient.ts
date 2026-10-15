import * as amqp from "amqplib";
import type {
    ConsumeMessage
} from "amqplib";
import {
    createStructuredLogger,
    type AppLogger,
    type LogContext
} from "@commerce-flow/logging";
import {
    PermanentMessageError,
    PublishTimeoutError,
    getErrorMessage
} from "./messageErrors.js";

export interface RabbitMqConnection {
    createChannel():
        Promise<RabbitMqChannel>;

    onClose(
        handler: () => void
    ): void;

    close(): Promise<void>;
}

export interface RabbitMqPublishOptions {
    readonly contentType: string;
    readonly deliveryMode: number;
    readonly messageId: string;
    readonly correlationId: string;
    readonly type: string;
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

    prefetch(
        count: number
    ): Promise<unknown>;

    /**
     * Publishes on a confirm channel. The promise resolves only
     * after the broker has acknowledged the message and rejects
     * when the broker nacks it or the channel closes first.
     */
    publish(
        exchangeName: string,
        routingKey: string,
        content: Buffer,
        options: RabbitMqPublishOptions
    ): Promise<void>;

    consume(
        queueName: string,
        onMessage: (
            message:
                ConsumeMessage | null
        ) => void | Promise<void>
    ): Promise<{
        consumerTag: string;
    }>;

    cancel(
        consumerTag: string
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
    readonly maxConnectionRetries?: number;
    readonly retryDelayInMs?: number;

    /**
     * Upper bound for connecting (when needed) and waiting for the
     * broker confirmation of a single publication.
     */
    readonly publishTimeoutInMs?: number;

    /** Maximum number of unacknowledged deliveries per consumer. */
    readonly prefetchCount?: number;

    /** Total handler attempts for transient failures before dead-lettering. */
    readonly handlerMaxAttempts?: number;
    readonly handlerRetryDelayInMs?: number;
    readonly handlerMaxRetryDelayInMs?: number;

    /** How long close() waits for in-flight handlers to finish. */
    readonly drainTimeoutInMs?: number;

    /**
     * Size of the per-queue, in-memory cache of recently processed
     * event ids. This is only an optimisation: durable idempotency
     * is the responsibility of each handler.
     */
    readonly recentEventIdCacheSize?: number;
}

export interface RabbitMqClientDependencies {
    connect(
        url: string
    ): Promise<RabbitMqConnection>;

    sleep(
        milliseconds: number,
        signal?: AbortSignal
    ): Promise<void>;

    logger: RabbitMqLogger;
}

export interface MessageEvent {
    readonly eventId: string;
    readonly eventType: string;
    readonly correlationId: string;
}

export interface RabbitMqSubscription<
    TEvent extends MessageEvent
> {
    readonly queueName: string;
    readonly routingKeys: readonly string[];

    /**
     * Validates the decoded JSON body. Throwing marks the message
     * as permanently unprocessable and dead-letters it.
     */
    parseEvent(
        value: unknown
    ): TEvent;

    handler(
        event: TEvent,
        message: ConsumeMessage
    ): Promise<void>;

    readonly signal?: AbortSignal;
}

export interface PublishOptions {
    readonly signal?: AbortSignal;
}

interface ActiveConsumer {
    readonly channel: RabbitMqChannel;
    readonly consumerTag: string;
}

/** Development-only default broker URL (guest account on localhost). */
export const DEVELOPMENT_RABBITMQ_URL =
    "amqp://guest:guest@localhost:5672";

const DEFAULTS = {
    maxConnectionRetries: 10,
    retryDelayInMs: 2_000,
    publishTimeoutInMs: 10_000,
    prefetchCount: 10,
    handlerMaxAttempts: 5,
    handlerRetryDelayInMs: 500,
    handlerMaxRetryDelayInMs: 8_000,
    drainTimeoutInMs: 10_000,
    recentEventIdCacheSize: 10_000
} as const;

export class RabbitMqClient {
    private connection:
        RabbitMqConnection | null = null;

    private channel:
        RabbitMqChannel | null = null;

    private connectPromise:
        Promise<void> | null = null;

    private connectAbortController:
        AbortController | null = null;

    private shutdownController =
        new AbortController();

    private readonly activeConsumers:
        ActiveConsumer[] = [];

    private readonly inFlightDeliveries =
        new Set<Promise<void>>();

    private readonly recentEventIdsByQueueName =
        new Map<
            string,
            RecentEventIds
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

    async connect(
        signal?: AbortSignal
    ): Promise<void> {
        throwIfAborted(
            signal
        );

        if (this.channel) {
            return;
        }

        if (this.connectPromise) {
            await waitForPromiseOrAbort(
                this.connectPromise,
                signal
            );

            return;
        }

        const controller =
            new AbortController();

        const removeAbortForwarding =
            forwardAbortSignal(
                signal,
                controller
            );

        this.connectAbortController =
            controller;

        const connectPromise =
            this.connectWithRetry(
                controller.signal
            );

        this.connectPromise =
            connectPromise;

        try {
            await connectPromise;
        } finally {
            removeAbortForwarding();

            if (
                this.connectPromise ===
                connectPromise
            ) {
                this.connectPromise =
                    null;
            }

            if (
                this.connectAbortController ===
                controller
            ) {
                this.connectAbortController =
                    null;
            }
        }
    }

    private async connectWithRetry(
        signal: AbortSignal
    ): Promise<void> {
        throwIfAborted(
            signal
        );

        if (this.channel) {
            return;
        }

        if (this.connection) {
            await this.closeResources();
        }

        const maxConnectionRetries =
            this.options.maxConnectionRetries ??
            DEFAULTS.maxConnectionRetries;

        const retryDelayInMs =
            this.options.retryDelayInMs ??
            DEFAULTS.retryDelayInMs;

        let lastError: unknown;

        for (
            let attempt = 1;
            attempt <=
            maxConnectionRetries;
            attempt += 1
        ) {
            throwIfAborted(
                signal
            );

            try {
                const connection =
                    await this
                        .connectToBroker(
                            this.url
                        );

                this.connection =
                    connection;

                throwIfAborted(
                    signal
                );

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

                throwIfAborted(
                    signal
                );

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

                throwIfAborted(
                    signal
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

                await this
                    .closeResources();

                if (signal.aborted) {
                    throw createAbortError();
                }

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

                if (
                    attempt <
                    maxConnectionRetries
                ) {
                    await this.delay(
                        retryDelayInMs,
                        signal
                    );

                    throwIfAborted(
                        signal
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

    /**
     * Publishes an event as a persistent message and waits for the
     * broker's publisher confirmation. A resolved promise means the
     * broker has taken responsibility for the message; a rejection
     * means the outcome is unknown and the caller must retry.
     */
    async publish<
        TEvent extends MessageEvent
    >(
        routingKey: string,
        event: TEvent,
        options: PublishOptions = {}
    ): Promise<void> {
        const publishTimeoutInMs =
            this.options.publishTimeoutInMs ??
            DEFAULTS.publishTimeoutInMs;

        // A referenced timer (unlike AbortSignal.timeout) keeps the
        // timeout reliable even when nothing else holds the event
        // loop open while we wait for the confirm.
        const timeoutController =
            new AbortController();

        const timeout =
            setTimeout(
                () => {
                    timeoutController.abort();
                },
                publishTimeoutInMs
            );

        const timeoutSignal =
            timeoutController.signal;

        const signal =
            options.signal
                ? AbortSignal.any([
                    options.signal,
                    timeoutSignal
                ])
                : timeoutSignal;

        try {
            await this.connect(
                signal
            );

            const channel =
                this.channel;

            if (!channel) {
                throw new Error(
                    "RabbitMQ channel is not available."
                );
            }

            const body =
                Buffer.from(
                    JSON.stringify(
                        event
                    )
                );

            await waitForPromiseOrAbort(
                channel.publish(
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
                            event.correlationId,
                        type:
                            event.eventType
                    }
                ),
                signal
            );
        } catch (error) {
            if (
                timeoutSignal.aborted &&
                !options.signal?.aborted
            ) {
                throw new PublishTimeoutError(
                    `RabbitMQ did not confirm event ` +
                    `${event.eventId} within ` +
                    `${publishTimeoutInMs} ms.`
                );
            }

            throw new Error(
                `[messaging] Failed to publish event ${event.eventId} ` +
                `to '${routingKey}': ${getErrorMessage(error)}`,
                {
                    cause: error
                }
            );
        } finally {
            clearTimeout(
                timeout
            );
        }

        this.logger.info(
            "Published event",
            {
                exchangeName:
                    this.exchangeName,
                routingKey,
                eventId:
                    event.eventId,
                eventType:
                    event.eventType,
                correlationId:
                    event.correlationId
            }
        );
    }

    async subscribe<
        TEvent extends MessageEvent
    >(
        subscription:
            RabbitMqSubscription<TEvent>
    ): Promise<void> {
        const {
            queueName,
            routingKeys,
            signal
        } = subscription;

        await this.connect(
            signal
        );

        throwIfAborted(
            signal
        );

        const channel =
            this.channel;

        if (!channel) {
            throw new Error(
                "RabbitMQ channel was not initialized."
            );
        }

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

        throwIfAborted(
            signal
        );

        await channel
            .assertQueue(
                deadLetterQueueName,
                {
                    durable: true
                }
            );

        throwIfAborted(
            signal
        );

        await channel
            .bindQueue(
                deadLetterQueueName,
                deadLetterExchangeName,
                deadLetterRoutingKey
            );

        throwIfAborted(
            signal
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

        throwIfAborted(
            signal
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

            throwIfAborted(
                signal
            );
        }

        await channel.prefetch(
            this.options.prefetchCount ??
            DEFAULTS.prefetchCount
        );

        const {
            consumerTag
        } =
            await channel
                .consume(
                    queueName,
                    async message => {
                        if (!message) {
                            this.handleConsumerCancelledByBroker(
                                channel,
                                queueName
                            );

                            return;
                        }

                        await this.trackDelivery(
                            this.handleDelivery(
                                channel,
                                subscription,
                                deadLetterQueueName,
                                message
                            )
                        );
                    }
                );

        this.activeConsumers.push({
            channel,
            consumerTag
        });

        throwIfAborted(
            signal
        );

        this.logger.info(
            "Subscribed queue",
            {
                queueName,
                routingKeys,
                consumerTag
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

    /**
     * Stops consuming, waits (bounded) for in-flight handlers and
     * closes the channel and connection. Unacknowledged messages
     * are returned to their queues by the broker. The client can
     * be connected again afterwards.
     */
    async close(): Promise<void> {
        this.connectAbortController
            ?.abort();

        this.shutdownController
            .abort();

        await this.cancelConsumers();

        await this.waitForInFlightDeliveries();

        await this.closeResources();

        this.shutdownController =
            new AbortController();
    }

    private async handleDelivery<
        TEvent extends MessageEvent
    >(
        channel: RabbitMqChannel,
        subscription:
            RabbitMqSubscription<TEvent>,
        deadLetterQueueName: string,
        message: ConsumeMessage
    ): Promise<void> {
        const {
            queueName
        } = subscription;

        let event: TEvent;

        try {
            event =
                decodeEvent(
                    message,
                    subscription.parseEvent
                );
        } catch (error) {
            this.logger.error(
                "Rejected malformed message",
                error,
                {
                    queueName,
                    deadLetterQueueName,
                    messageId:
                        message.properties.messageId,
                    action:
                        "dead-letter"
                }
            );

            this.settle(
                channel,
                message,
                "dead-letter",
                {
                    queueName
                }
            );

            return;
        }

        const context: LogContext = {
            queueName,
            eventId:
                event.eventId,
            eventType:
                event.eventType,
            correlationId:
                event.correlationId,
            redelivered:
                message.fields.redelivered
        };

        const recentEventIds =
            this.getRecentEventIds(
                queueName
            );

        if (
            recentEventIds.has(
                event.eventId
            )
        ) {
            this.logger.warn(
                "Duplicate event received",
                {
                    ...context,
                    action:
                        "acknowledge-without-reprocessing"
                }
            );

            this.settle(
                channel,
                message,
                "ack",
                context
            );

            return;
        }

        const maxAttempts =
            this.options.handlerMaxAttempts ??
            DEFAULTS.handlerMaxAttempts;

        for (
            let attempt = 1;
            ;
            attempt += 1
        ) {
            try {
                await subscription.handler(
                    event,
                    message
                );

                recentEventIds.add(
                    event.eventId
                );

                this.settle(
                    channel,
                    message,
                    "ack",
                    context
                );

                return;
            } catch (error) {
                const permanent =
                    error instanceof
                    PermanentMessageError;

                if (
                    permanent ||
                    attempt >= maxAttempts
                ) {
                    this.logger.error(
                        "Failed to process message",
                        error,
                        {
                            ...context,
                            attempt,
                            maxAttempts,
                            permanent,
                            deadLetterQueueName,
                            action:
                                "dead-letter"
                        }
                    );

                    this.settle(
                        channel,
                        message,
                        "dead-letter",
                        context
                    );

                    return;
                }

                const retryDelayInMs =
                    this.getHandlerRetryDelay(
                        attempt
                    );

                this.logger.warn(
                    "Retrying message after handler failure",
                    {
                        ...context,
                        attempt,
                        maxAttempts,
                        retryDelayInMs,
                        errorMessage:
                            getErrorMessage(
                                error
                            )
                    }
                );

                const shutdownSignal =
                    this.shutdownController
                        .signal;

                await this.delay(
                    retryDelayInMs,
                    shutdownSignal
                );

                if (
                    shutdownSignal.aborted ||
                    this.channel !== channel
                ) {
                    // The broker redelivers unacknowledged
                    // messages once this channel is closed.
                    this.logger.warn(
                        "Abandoned message retry; " +
                        "the broker will redeliver it",
                        {
                            ...context,
                            attempt
                        }
                    );

                    return;
                }
            }
        }
    }

    private settle(
        channel: RabbitMqChannel,
        message: ConsumeMessage,
        outcome:
            | "ack"
            | "dead-letter",
        context: LogContext
    ): void {
        try {
            if (outcome === "ack") {
                channel.ack(
                    message
                );

                return;
            }

            channel.nack(
                message,
                false,
                false
            );
        } catch (error) {
            // The channel closed underneath us. The broker
            // will redeliver the message to a live consumer.
            this.logger.warn(
                "Could not settle message on a closed channel",
                {
                    ...context,
                    outcome,
                    errorMessage:
                        getErrorMessage(
                            error
                        )
                }
            );
        }
    }

    private handleConsumerCancelledByBroker(
        channel: RabbitMqChannel,
        queueName: string
    ): void {
        this.logger.warn(
            "RabbitMQ cancelled the consumer",
            {
                queueName
            }
        );

        if (this.channel !== channel) {
            return;
        }

        // Dropping the channel lets the supervisor
        // re-declare the topology and resubscribe.
        void this.closeResources();
    }

    private async trackDelivery(
        delivery: Promise<void>
    ): Promise<void> {
        const tracked =
            delivery.catch(
                error => {
                    this.logger.error(
                        "Unexpected failure while handling a message",
                        error
                    );
                }
            );

        this.inFlightDeliveries.add(
            tracked
        );

        try {
            await tracked;
        } finally {
            this.inFlightDeliveries.delete(
                tracked
            );
        }
    }

    private async cancelConsumers():
        Promise<void> {
        const consumers =
            this.activeConsumers.splice(
                0
            );

        await Promise.allSettled(
            consumers
                .filter(
                    consumer =>
                        consumer.channel ===
                        this.channel
                )
                .map(
                    consumer =>
                        consumer.channel.cancel(
                            consumer.consumerTag
                        )
                )
        );
    }

    private async waitForInFlightDeliveries():
        Promise<void> {
        if (
            this.inFlightDeliveries.size ===
            0
        ) {
            return;
        }

        const inFlightCount =
            this.inFlightDeliveries.size;

        const drainTimeoutInMs =
            this.options.drainTimeoutInMs ??
            DEFAULTS.drainTimeoutInMs;

        const timeoutController =
            new AbortController();

        let drained =
            false;

        await Promise.race([
            Promise.allSettled([
                ...this.inFlightDeliveries
            ]).then(
                () => {
                    drained =
                        true;
                }
            ),
            this.delay(
                drainTimeoutInMs,
                timeoutController.signal
            )
        ]);

        timeoutController.abort();

        if (!drained) {
            this.logger.warn(
                "Timed out waiting for in-flight messages; " +
                "the broker will redeliver them",
                {
                    inFlightCount,
                    drainTimeoutInMs
                }
            );
        }
    }

    private getHandlerRetryDelay(
        attempt: number
    ): number {
        const initialDelay =
            this.options.handlerRetryDelayInMs ??
            DEFAULTS.handlerRetryDelayInMs;

        const maxDelay =
            this.options.handlerMaxRetryDelayInMs ??
            DEFAULTS.handlerMaxRetryDelayInMs;

        return Math.min(
            initialDelay *
            2 ** (attempt - 1),
            maxDelay
        );
    }

    private getRecentEventIds(
        queueName: string
    ): RecentEventIds {
        const existing =
            this.recentEventIdsByQueueName
                .get(
                    queueName
                );

        if (existing) {
            return existing;
        }

        const created =
            new RecentEventIds(
                this.options.recentEventIdCacheSize ??
                DEFAULTS.recentEventIdCacheSize
            );

        this.recentEventIdsByQueueName
            .set(
                queueName,
                created
            );

        return created;
    }

    private async closeResources():
        Promise<void> {
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
}

/**
 * Bounded, insertion-ordered set used to skip obvious duplicate
 * deliveries without growing memory without limit.
 */
class RecentEventIds {
    private readonly eventIds =
        new Set<string>();

    constructor(
        private readonly capacity: number
    ) {
    }

    has(
        eventId: string
    ): boolean {
        return this.eventIds.has(
            eventId
        );
    }

    add(
        eventId: string
    ): void {
        if (
            this.capacity <= 0 ||
            this.eventIds.has(
                eventId
            )
        ) {
            return;
        }

        this.eventIds.add(
            eventId
        );

        if (
            this.eventIds.size >
            this.capacity
        ) {
            const oldest =
                this.eventIds
                    .values()
                    .next();

            if (!oldest.done) {
                this.eventIds.delete(
                    oldest.value
                );
            }
        }
    }
}

function decodeEvent<
    TEvent extends MessageEvent
>(
    message: ConsumeMessage,
    parseEvent: (
        value: unknown
    ) => TEvent
): TEvent {
    let body: unknown;

    try {
        body =
            JSON.parse(
                message.content
                    .toString(
                        "utf8"
                    )
            );
    } catch (error) {
        throw new PermanentMessageError(
            "Message body is not valid JSON.",
            {
                cause: error
            }
        );
    }

    try {
        return parseEvent(
            body
        );
    } catch (error) {
        throw new PermanentMessageError(
            "Message does not match the expected event contract.",
            {
                cause: error
            }
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

    // Connection-level errors are always followed by a
    // "close" event, which drives reconnection.
    connection.on(
        "error",
        () => undefined
    );

    return {
        createChannel:
            async () => {
                const channel =
                    await connection
                        .createConfirmChannel();

                // Channel errors are followed by "close";
                // without a listener they would crash the process.
                channel.on(
                    "error",
                    () => undefined
                );

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

                    prefetch:
                        async count => {
                            return await channel
                                .prefetch(
                                    count
                                );
                        },

                    publish:
                        (
                            exchangeName,
                            routingKey,
                            content,
                            options
                        ) => {
                            return new Promise<void>(
                                (
                                    resolve,
                                    reject
                                ) => {
                                    try {
                                        channel.publish(
                                            exchangeName,
                                            routingKey,
                                            content,
                                            options,
                                            (error: unknown) => {
                                                if (
                                                    error === null ||
                                                    error === undefined
                                                ) {
                                                    resolve();

                                                    return;
                                                }

                                                reject(
                                                    error instanceof Error
                                                        ? error
                                                        : new Error(
                                                            getErrorMessage(
                                                                error
                                                            )
                                                        )
                                                );
                                            }
                                        );
                                    } catch (error) {
                                        reject(
                                            error
                                        );
                                    }
                                }
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

                    cancel:
                        async consumerTag => {
                            return await channel
                                .cancel(
                                    consumerTag
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
    milliseconds: number,
    signal?: AbortSignal
): Promise<void> {
    if (!signal) {
        return new Promise(
            resolve => {
                setTimeout(
                    resolve,
                    milliseconds
                );
            }
        );
    }

    if (signal.aborted) {
        return Promise.resolve();
    }

    return new Promise(
        resolve => {
            const onAbort =
                (): void => {
                    clearTimeout(
                        timeout
                    );

                    signal
                        .removeEventListener(
                            "abort",
                            onAbort
                        );

                    resolve();
                };

            const timeout =
                setTimeout(
                    () => {
                        signal
                            .removeEventListener(
                                "abort",
                                onAbort
                            );

                        resolve();
                    },
                    milliseconds
                );

            signal.addEventListener(
                "abort",
                onAbort,
                {
                    once: true
                }
            );
        }
    );
}

function throwIfAborted(
    signal?: AbortSignal
): void {
    if (!signal?.aborted) {
        return;
    }

    throw createAbortError();
}

function createAbortError():
    Error {
    const error =
        new Error(
            "RabbitMQ initialization aborted."
        );

    error.name =
        "AbortError";

    return error;
}

function forwardAbortSignal(
    source:
        AbortSignal | undefined,
    target:
        AbortController
): () => void {
    if (!source) {
        return () =>
            undefined;
    }

    if (source.aborted) {
        target.abort();

        return () =>
            undefined;
    }

    const onAbort =
        (): void => {
            target.abort();
        };

    source.addEventListener(
        "abort",
        onAbort,
        {
            once: true
        }
    );

    return () => {
        source.removeEventListener(
            "abort",
            onAbort
        );
    };
}

function waitForPromiseOrAbort(
    promise: Promise<void>,
    signal?: AbortSignal
): Promise<void> {
    if (!signal) {
        return promise;
    }

    if (signal.aborted) {
        // Keep a late rejection from becoming unhandled.
        promise.catch(
            () => undefined
        );

        return Promise.reject(
            createAbortError()
        );
    }

    return new Promise(
        (
            resolve,
            reject
        ) => {
            const cleanup =
                (): void => {
                    signal
                        .removeEventListener(
                            "abort",
                            onAbort
                        );
                };

            const onAbort =
                (): void => {
                    cleanup();

                    reject(
                        createAbortError()
                    );
                };

            signal.addEventListener(
                "abort",
                onAbort,
                {
                    once: true
                }
            );

            promise.then(
                () => {
                    cleanup();

                    resolve();
                },
                error => {
                    cleanup();

                    reject(
                        error
                    );
                }
            );
        }
    );
}
