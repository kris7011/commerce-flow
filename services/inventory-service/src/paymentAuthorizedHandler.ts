import type {
    InventoryReservationFailedEvent,
    InventoryReservedEvent,
    PaymentAuthorizedEvent
} from "@commerce-flow/contracts";
import {
    InventoryService,
    type InventoryResultEvent
} from "./inventoryService.js";

export interface InventoryResultPublisher {
    publishInventoryResult(
        event: InventoryResultEvent
    ): Promise<void>;
}

export interface InventoryEventLogger {
    log(message: string): void;
}

export interface PaymentAuthorizedHandlerDependencies {
    readonly inventoryService:
    InventoryService;

    readonly inventoryResultPublisher:
    InventoryResultPublisher;

    readonly logger?:
    InventoryEventLogger;
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
        logger = console
    } = dependencies;

    return async (
        event: PaymentAuthorizedEvent
    ): Promise<void> => {
        logger.log(
            `[inventory-service] ` +
            `Received PaymentAuthorized ` +
            `for order ` +
            `'${event.data.orderId}' ` +
            `with correlationId ` +
            `'${event.correlationId}'`
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
    logger: InventoryEventLogger
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
    logger: InventoryEventLogger
): void {
    logger.log(
        `[inventory-service] ` +
        `Reserved inventory ` +
        `for order ` +
        `'${event.data.orderId}'`
    );
}

function logInventoryReservationFailed(
    event:
        InventoryReservationFailedEvent,
    logger: InventoryEventLogger
): void {
    logger.log(
        `[inventory-service] ` +
        `Inventory reservation failed ` +
        `for order ` +
        `'${event.data.orderId}'`
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