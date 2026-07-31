import { randomUUID } from "node:crypto";
import type {
    OrderCreatedEvent,
    PaymentAuthorizedEvent
} from "@commerce-flow/contracts";

export interface PaymentServiceDependencies {
    generateId?: () => string;
    getCurrentTime?: () => string;
}

export class PaymentService {
    private readonly generateId: () => string;
    private readonly getCurrentTime: () => string;

    constructor(
        dependencies: PaymentServiceDependencies = {}
    ) {
        this.generateId =
            dependencies.generateId ?? (() => randomUUID());

        this.getCurrentTime =
            dependencies.getCurrentTime ??
            (() => new Date().toISOString());
    }

    authorizePayment(
        sourceEvent: OrderCreatedEvent
    ): PaymentAuthorizedEvent {
        const eventId = this.generateId();
        const paymentId = this.generateId();

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
}
