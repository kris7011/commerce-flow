/*
 * The order's workflow status, derived from the events the other
 * services publish. Events can arrive more than once and, because the
 * timeline consumer processes messages concurrently, out of order. The
 * status therefore only moves forward: a late PaymentAuthorized never
 * overwrites DeliveryBooked, and a duplicate changes nothing.
 */

export const ORDER_WORKFLOW_EVENT_TYPES = [
    "PaymentAuthorized",
    "InventoryReserved",
    "InventoryReservationFailed",
    "DeliveryBooked"
] as const;

export type OrderWorkflowEventType =
    (typeof ORDER_WORKFLOW_EVENT_TYPES)[number];

export type OrderStatus =
    | "Created"
    | OrderWorkflowEventType;

const STAGE: Readonly<Record<OrderStatus, number>> = {
    Created: 0,
    PaymentAuthorized: 1,
    InventoryReserved: 2,
    InventoryReservationFailed: 2,
    DeliveryBooked: 3
};

export function applyWorkflowEvent(
    current: OrderStatus,
    eventType: OrderWorkflowEventType
): OrderStatus {
    // Inventory decides once per order, so reserved and failed never
    // both happen; whichever was recorded first is kept.
    if (
        current === "InventoryReservationFailed" ||
        STAGE[eventType] <= STAGE[current]
    ) {
        return current;
    }

    return eventType;
}

export type OrderOutcome =
    | "InProgress"
    | "Completed"
    | "Failed";

export function getOrderOutcome(
    status: OrderStatus
): OrderOutcome {
    switch (status) {
        case "DeliveryBooked":
            return "Completed";

        case "InventoryReservationFailed":
            return "Failed";

        case "Created":
        case "PaymentAuthorized":
        case "InventoryReserved":
            return "InProgress";
    }
}

export function isOrderStatus(
    value: string
): value is OrderStatus {
    return Object.hasOwn(
        STAGE,
        value
    );
}
