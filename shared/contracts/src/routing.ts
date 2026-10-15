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

export interface QueueBinding {
    readonly queueName: string;
    readonly routingKeys: readonly RoutingKey[];
}

/**
 * The consumer queues of the system and the routing keys bound to
 * them. Every service declares this whole topology idempotently when
 * it connects, so an event always has a queue to land in, whichever
 * service happens to start first.
 */
export const QUEUES = {
    paymentOrderCreated: {
        queueName:
            "payment-service.order-created",
        routingKeys: [
            ROUTING_KEYS.OrderCreated
        ]
    },
    inventoryPaymentAuthorized: {
        queueName:
            "inventory-service.payment-authorized",
        routingKeys: [
            ROUTING_KEYS.PaymentAuthorized
        ]
    },
    deliveryInventoryReserved: {
        queueName:
            "delivery-service.inventory-reserved",
        routingKeys: [
            ROUTING_KEYS.InventoryReserved
        ]
    },
    notificationCustomerEvents: {
        queueName:
            "notification-service.customer-events",
        routingKeys: [
            ROUTING_KEYS.DeliveryBooked,
            ROUTING_KEYS.InventoryReservationFailed
        ]
    }
} as const satisfies Record<string, QueueBinding>;

export const COMMERCE_TOPOLOGY:
    readonly QueueBinding[] =
    Object.values(
        QUEUES
    );
