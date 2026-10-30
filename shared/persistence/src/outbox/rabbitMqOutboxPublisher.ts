import type {
    MessageEvent,
    PublishOptions
} from "@commerce-flow/messaging";
import type {
    OutboxMessage,
    OutboxPublisher
} from "./outbox.js";

export interface ConfirmingEventPublisher {
    publish<TEvent extends MessageEvent>(
        routingKey: string,
        event: TEvent,
        options?: PublishOptions
    ): Promise<void>;
}

type StoredEvent =
    MessageEvent &
    Readonly<Record<string, unknown>>;

/**
 * Publishes outbox rows through the confirming RabbitMQ client. The
 * stored payload is the complete event envelope written in the same
 * transaction as the business change.
 */
export class RabbitMqOutboxPublisher
    implements OutboxPublisher {
    constructor(
        private readonly eventPublisher:
            ConfirmingEventPublisher
    ) {
    }

    async publish(
        message: OutboxMessage
    ): Promise<void> {
        await this.eventPublisher.publish(
            message.routingKey,
            toStoredEvent(
                message
            )
        );
    }
}

function toStoredEvent(
    message: OutboxMessage
): StoredEvent {
    const payload =
        message.payload;

    if (
        typeof payload !== "object" ||
        payload === null ||
        Array.isArray(payload)
    ) {
        throw new Error(
            `Outbox event ${message.eventId} has a non-object payload.`
        );
    }

    const eventId =
        Reflect.get(
            payload,
            "eventId"
        );

    const eventType =
        Reflect.get(
            payload,
            "eventType"
        );

    const correlationId =
        Reflect.get(
            payload,
            "correlationId"
        );

    if (
        eventId !== message.eventId ||
        eventType !== message.eventType ||
        typeof correlationId !== "string"
    ) {
        throw new Error(
            `Outbox event ${message.eventId} has a payload ` +
            `that does not match its outbox metadata.`
        );
    }

    return {
        ...payload,
        eventId,
        eventType,
        correlationId
    };
}
