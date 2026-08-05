import type {
    DeliveryBookedEvent,
    InventoryReservedEvent
} from "@commerce-flow/contracts";
import {
    DeliveryService
} from "./deliveryService.js";

export interface DeliveryBookedPublisher {
    publishDeliveryBooked(
        event: DeliveryBookedEvent
    ): Promise<void>;
}

export interface DeliveryEventLogger {
    log(message: string): void;
}

export interface InventoryReservedHandlerDependencies {
    readonly deliveryService:
    DeliveryService;

    readonly deliveryBookedPublisher:
    DeliveryBookedPublisher;

    readonly logger?:
    DeliveryEventLogger;
}

export type InventoryReservedHandler = (
    event: InventoryReservedEvent
) => Promise<void>;

export function createInventoryReservedHandler(
    dependencies:
        InventoryReservedHandlerDependencies
): InventoryReservedHandler {
    const {
        deliveryService,
        deliveryBookedPublisher,
        logger = console
    } = dependencies;

    return async (
        event: InventoryReservedEvent
    ): Promise<void> => {
        logger.log(
            `[delivery-service] ` +
            `Received InventoryReserved ` +
            `for order ` +
            `'${event.data.orderId}' ` +
            `with correlationId ` +
            `'${event.correlationId}'`
        );

        const deliveryBookedEvent =
            deliveryService
                .bookDelivery(event);

        await deliveryBookedPublisher
            .publishDeliveryBooked(
                deliveryBookedEvent
            );

        logger.log(
            `[delivery-service] ` +
            `Booked delivery ` +
            `for order ` +
            `'${event.data.orderId}'`
        );
    };
}