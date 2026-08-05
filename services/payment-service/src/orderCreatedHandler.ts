import type {
    OrderCreatedEvent,
    PaymentAuthorizedEvent
} from "@commerce-flow/contracts";
import {
    PaymentService
} from "./paymentService.js";

export interface PaymentAuthorizedPublisher {
    publishPaymentAuthorized(
        event: PaymentAuthorizedEvent
    ): Promise<void>;
}

export interface PaymentEventLogger {
    log(message: string): void;
}

export interface OrderCreatedHandlerDependencies {
    readonly paymentService:
    PaymentService;

    readonly paymentAuthorizedPublisher:
    PaymentAuthorizedPublisher;

    readonly logger?:
    PaymentEventLogger;
}

export type OrderCreatedHandler = (
    event: OrderCreatedEvent
) => Promise<void>;

export function createOrderCreatedHandler(
    dependencies:
        OrderCreatedHandlerDependencies
): OrderCreatedHandler {
    const {
        paymentService,
        paymentAuthorizedPublisher,
        logger = console
    } = dependencies;

    return async (
        event: OrderCreatedEvent
    ): Promise<void> => {
        logger.log(
            `[payment-service] ` +
            `Received OrderCreated ` +
            `for order ` +
            `'${event.data.orderId}' ` +
            `with correlationId ` +
            `'${event.correlationId}'`
        );

        const paymentAuthorizedEvent =
            paymentService
                .authorizePayment(event);

        await paymentAuthorizedPublisher
            .publishPaymentAuthorized(
                paymentAuthorizedEvent
            );

        logger.log(
            `[payment-service] ` +
            `Authorized payment ` +
            `for order ` +
            `'${event.data.orderId}'`
        );
    };
}
