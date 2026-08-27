import type {
    DeliveryBookedEvent,
    InventoryReservedEvent
} from "@commerce-flow/contracts";
import type {
    AppLogger
} from "@commerce-flow/logging";
import {
    DeliveryService
} from "./deliveryService.js";

export interface DeliveryBookedPublisher {
    publishDeliveryBooked(
        event: DeliveryBookedEvent
    ): Promise<void>;
}

export interface InventoryReservedHandlerDependencies {
    readonly deliveryService:
    DeliveryService;

    readonly deliveryBookedPublisher:
    DeliveryBookedPublisher;

    readonly logger:
    AppLogger;
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
        logger
    } = dependencies;

    return async (
        event: InventoryReservedEvent
    ): Promise<void> => {
        logger.info(
            "Received InventoryReserved",
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

        const deliveryBookedEvent =
            deliveryService
                .bookDelivery(event);

        await deliveryBookedPublisher
            .publishDeliveryBooked(
                deliveryBookedEvent
            );

        logger.info(
            "Booked delivery",
            {
                eventId:
                    deliveryBookedEvent.eventId,
                orderId:
                    deliveryBookedEvent
                        .data.orderId,
                deliveryId:
                    deliveryBookedEvent
                        .data.deliveryId,
                carrier:
                    deliveryBookedEvent
                        .data.carrier,
                estimatedDeliveryDate:
                    deliveryBookedEvent
                        .data
                        .estimatedDeliveryDate,
                correlationId:
                    deliveryBookedEvent
                        .correlationId
            }
        );
    };
}