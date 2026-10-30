import {
    parseDeliveryBookedEvent,
    parseInventoryReservationFailedEvent,
    parseInventoryReservedEvent,
    parsePaymentAuthorizedEvent,
    type DeliveryBookedEvent,
    type InventoryReservationFailedEvent,
    type InventoryReservedEvent,
    type PaymentAuthorizedEvent
} from "@commerce-flow/contracts";
import type {
    AppLogger
} from "@commerce-flow/logging";
import type {
    OrderStore
} from "./orderStore.js";

/*
 * Consumes the events the other services publish about an order and
 * records them in the order's timeline. This is a read model: it only
 * observes the workflow and never influences it.
 */

export type OrderWorkflowEvent =
    | PaymentAuthorizedEvent
    | InventoryReservedEvent
    | InventoryReservationFailedEvent
    | DeliveryBookedEvent;

/** Throws for anything else, which dead-letters the message. */
export function parseOrderWorkflowEvent(
    value: unknown
): OrderWorkflowEvent {
    const eventType =
        typeof value === "object" && value !== null
            ? Reflect.get(
                value,
                "eventType"
            )
            : undefined;

    switch (eventType) {
        case "PaymentAuthorized":
            return parsePaymentAuthorizedEvent(
                value
            );

        case "InventoryReserved":
            return parseInventoryReservedEvent(
                value
            );

        case "InventoryReservationFailed":
            return parseInventoryReservationFailedEvent(
                value
            );

        case "DeliveryBooked":
            return parseDeliveryBookedEvent(
                value
            );

        default:
            throw new Error(
                `Unsupported event type for the order timeline: ${String(eventType)}`
            );
    }
}

export interface OrderWorkflowEventHandlerDependencies {
    readonly store:
    Pick<OrderStore, "recordWorkflowEvent">;

    readonly logger:
    AppLogger;
}

export function createOrderWorkflowEventHandler(
    dependencies:
        OrderWorkflowEventHandlerDependencies
): (event: OrderWorkflowEvent) => Promise<void> {
    const {
        store,
        logger
    } = dependencies;

    return async event => {
        const context = {
            eventId:
                event.eventId,
            eventType:
                event.eventType,
            orderId:
                event.data.orderId,
            correlationId:
                event.correlationId
        };

        const outcome =
            await store.recordWorkflowEvent({
                eventId:
                    event.eventId,
                eventType:
                    event.eventType,
                orderId:
                    event.data.orderId,
                correlationId:
                    event.correlationId,
                occurredAt:
                    event.occurredAt,
                data:
                    event.data
            });

        switch (outcome.kind) {
            case "Recorded":
                logger.info(
                    "Recorded order workflow event",
                    {
                        ...context,
                        orderStatus:
                            outcome.status
                    }
                );
                return;

            case "Duplicate":
                logger.warn(
                    "Ignored duplicate order workflow event",
                    {
                        ...context,
                        deliveries:
                            outcome.deliveries
                    }
                );
                return;

            case "UnknownOrder":
                // Retrying cannot help: an order is stored before its
                // OrderCreated event exists, so this event refers to an
                // order this database never had.
                logger.warn(
                    "Ignored workflow event for an unknown order",
                    context
                );
                return;
        }
    };
}
