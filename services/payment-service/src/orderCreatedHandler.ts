import type {
    OrderCreatedEvent,
    PaymentAuthorizedEvent
} from "@commerce-flow/contracts";
import type {
    AppLogger
} from "@commerce-flow/logging";
import {
    PaymentService
} from "./paymentService.js";

export interface PaymentAuthorizedPublisher {
    publishPaymentAuthorized(
        event: PaymentAuthorizedEvent
    ): Promise<void>;
}

export interface OrderCreatedHandlerDependencies {
    readonly paymentService:
    PaymentService;

    readonly paymentAuthorizedPublisher:
    PaymentAuthorizedPublisher;

    readonly logger:
    AppLogger;
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
        logger
    } = dependencies;

    return async (
        event: OrderCreatedEvent
    ): Promise<void> => {
        logger.info(
            "Received OrderCreated",
            {
                eventId:
                    event.eventId,
                orderId:
                    event.data.orderId,
                correlationId:
                    event.correlationId,
                totalAmount:
                    event.data.totalAmount,
                itemCount:
                    event.data.items.length
            }
        );

        const paymentAuthorizedEvent =
            paymentService
                .authorizePayment(event);

        await paymentAuthorizedPublisher
            .publishPaymentAuthorized(
                paymentAuthorizedEvent
            );

        logger.info(
            "Authorized payment",
            {
                eventId:
                    paymentAuthorizedEvent
                        .eventId,
                orderId:
                    paymentAuthorizedEvent
                        .data.orderId,
                paymentId:
                    paymentAuthorizedEvent
                        .data.paymentId,
                correlationId:
                    paymentAuthorizedEvent
                        .correlationId,
                amount:
                    paymentAuthorizedEvent
                        .data.amount
            }
        );
    };
}