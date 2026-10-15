import {
    deriveDeterministicId,
    type OrderCreatedEvent,
    type PaymentAuthorizedEvent
} from "@commerce-flow/contracts";

export interface PaymentServiceDependencies {
    /**
     * Overrides id generation (used by tests). By default ids are
     * derived deterministically from the source event, see below.
     */
    generateId?: () => string;
}

export class PaymentService {
    private readonly generateId:
        (() => string) | undefined;

    constructor(
        dependencies: PaymentServiceDependencies = {}
    ) {
        this.generateId =
            dependencies.generateId;
    }

    /**
     * The resulting event is a pure function of the source event.
     *
     * A redelivered OrderCreated message (e.g. after a crash between
     * publish and ack) therefore re-emits a byte-identical event: the
     * same event id, the same payment id (one payment per order) and
     * the same timestamp. An event id can never stand for two
     * different outcomes, and downstream consumers can safely treat a
     * repeated id as a duplicate.
     *
     * The simulated authorization is recorded as of the order it
     * authorizes (occurredAt of the source event). A real payment
     * integration would persist its outcome and publish it through an
     * outbox instead of re-deriving it.
     */
    authorizePayment(
        sourceEvent: OrderCreatedEvent
    ): PaymentAuthorizedEvent {
        const eventId =
            this.createId(
                "payment-authorized-event",
                sourceEvent.eventId
            );

        const paymentId =
            this.createId(
                "payment",
                sourceEvent.data.orderId
            );

        return {
            eventId,
            eventType: "PaymentAuthorized",
            occurredAt: sourceEvent.occurredAt,
            correlationId: sourceEvent.correlationId,
            data: {
                orderId: sourceEvent.data.orderId,
                paymentId,
                amount: sourceEvent.data.totalAmount,
                items: sourceEvent.data.items
            }
        };
    }

    private createId(
        namespace: string,
        key: string
    ): string {
        return (
            this.generateId?.() ??
            deriveDeterministicId(
                `payment-service.${namespace}`,
                key
            )
        );
    }
}
