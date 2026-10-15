import type {
    InventoryReservationFailedEvent,
    InventoryReservedEvent,
    PaymentAuthorizedEvent
} from "@commerce-flow/contracts";
import type {
    AppLogger
} from "@commerce-flow/logging";
import type {
    InventoryResultEvent,
    InventoryService
} from "./inventoryService.js";

export interface PaymentAuthorizedHandlerDependencies {
    readonly inventoryService:
    Pick<InventoryService, "reserveForPayment">;

    readonly logger:
    AppLogger;

    /**
     * Called after a new outcome has been committed to the outbox,
     * typically to wake the outbox relay without waiting for a poll.
     */
    readonly onEventEnqueued?:
    () => void;
}

export type PaymentAuthorizedHandler = (
    event: PaymentAuthorizedEvent
) => Promise<void>;

/**
 * Message handler for PaymentAuthorized. It only records the decision;
 * publishing happens asynchronously through the transactional outbox,
 * so acknowledging the message never depends on RabbitMQ being able to
 * accept the resulting event at that moment.
 */
export function createPaymentAuthorizedHandler(
    dependencies:
        PaymentAuthorizedHandlerDependencies
): PaymentAuthorizedHandler {
    const {
        inventoryService,
        logger,
        onEventEnqueued
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

        const outcome =
            await inventoryService
                .reserveForPayment(
                    event
                );

        if (outcome.status === "Duplicate") {
            logger.warn(
                "Ignored duplicate PaymentAuthorized for an already processed order",
                {
                    eventId:
                        event.eventId,
                    orderId:
                        event.data.orderId,
                    correlationId:
                        event.correlationId,
                    originalSourceEventId:
                        outcome.existing.sourceEventId,
                    originalStatus:
                        outcome.existing.status,
                    originalResultEventId:
                        outcome.existing.resultEventId
                }
            );

            return;
        }

        onEventEnqueued?.();

        logInventoryResult(
            outcome.event,
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
