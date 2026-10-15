import type {
    EventType
} from "./events.js";

/**
 * Single source of truth for how each event type is routed on the
 * `commerce.events` topic exchange.
 */
export const ROUTING_KEYS = {
    OrderCreated:
        "order.created",
    PaymentAuthorized:
        "payment.authorized",
    InventoryReserved:
        "inventory.reserved",
    InventoryReservationFailed:
        "inventory.reservation.failed",
    DeliveryBooked:
        "delivery.booked"
} as const satisfies Record<EventType, string>;

export type RoutingKey =
    (typeof ROUTING_KEYS)[EventType];

export function getRoutingKey(
    eventType: EventType
): RoutingKey {
    return ROUTING_KEYS[eventType];
}
