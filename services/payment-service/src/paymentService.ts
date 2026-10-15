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
    getCurrentTime?: () => string;
}

export class PaymentService {
    private readonly generateId:
        (() => string) | undefined;

    private readonly getCurrentTime: () => string;

    constructor(
        dependencies: PaymentServiceDependencies = {}
    ) {
        this.generateId =
            dependencies.generateId;

        this.getCurrentTime =
            dependencies.getCurrentTime ??
            (() => new Date().toISOString());
    }

    /**
     * Ids are derived from the source event: a redelivered
     * OrderCreated message yields the same PaymentAuthorized event id
     * and the same payment id (one payment per order), so downstream
     * consumers recognise the re-emitted event as a duplicate.
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
            occurredAt: this.getCurrentTime(),
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
