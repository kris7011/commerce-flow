import type {
    InventoryReservationFailedEvent,
    InventoryReservedEvent,
    PaymentAuthorizedEvent
} from "@commerce-flow/contracts";
import type {
    AppLogger
} from "@commerce-flow/logging";
import {
    InventoryService,
    type InventoryResultEvent
} from "./inventoryService.js";

export interface InventoryResultPublisher {
    publishInventoryResult(
        event: InventoryResultEvent
    ): Promise<void>;
}

export interface PaymentAuthorizedHandlerDependencies {
    readonly inventoryService:
    InventoryService;

    readonly inventoryResultPublisher:
    InventoryResultPublisher;

    readonly logger:
    AppLogger;
}

export type PaymentAuthorizedHandler = (
    event: PaymentAuthorizedEvent
) => Promise<void>;

export function createPaymentAuthorizedHandler(
    dependencies:
        PaymentAuthorizedHandlerDependencies
): PaymentAuthorizedHandler {
    const {
        inventoryService,
        inventoryResultPublisher,
        logger
    } = dependencies;

    return async (
        event: PaymentAuthorizedEvent
    ): Promise<void> => {
        logger.info(
            "Received PaymentAuthorized",
            {
                eventId:
                    event.eventId,
                orderId:
                    event.data.orderId,
                paymentId:
                    event.data.paymentId,
                correlationId:
                    event.correlationId,
                amount:
                    event.data.amount,
                itemCount:
                    event.data.items.length
            }
        );

        const resultEvent =
            inventoryService
                .processPaymentAuthorized(
                    event
                );

        await inventoryResultPublisher
            .publishInventoryResult(
                resultEvent
            );

        logInventoryResult(
            resultEvent,
            logger
        );
    };
}

function logInventoryResult(
    event: InventoryResultEvent,
    logger: AppLogger
): void {
    switch (event.eventType) {
        case "InventoryReserved":
            logInventoryReserved(
                event,
                logger
            );
            return;

        case "InventoryReservationFailed":
            logInventoryReservationFailed(
                event,
                logger
            );
            return;

        default:
            assertNever(event);
    }
}

function logInventoryReserved(
    event: InventoryReservedEvent,
    logger: AppLogger
): void {
    logger.info(
        "Reserved inventory",
        {
            eventId:
                event.eventId,
            orderId:
                event.data.orderId,
            reservationId:
                event.data.reservationId,
            correlationId:
                event.correlationId,
            itemCount:
                event.data.items.length
        }
    );
}

function logInventoryReservationFailed(
    event:
        InventoryReservationFailedEvent,
    logger: AppLogger
): void {
    logger.warn(
        "Inventory reservation failed",
        {
            eventId:
                event.eventId,
            orderId:
                event.data.orderId,
            correlationId:
                event.correlationId,
            reason:
                event.data.reason,
            unavailableItems:
                event.data.unavailableItems
        }
    );
}

function assertNever(
    value: never
): never {
    throw new Error(
        `Unsupported inventory result event: ` +
        `${JSON.stringify(value)}`
    );
}