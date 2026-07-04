export type EventType =
    | "OrderCreated"
    | "PaymentAuthorized"
    | "InventoryReserved"
    | "InventoryReservationFailed";

export interface DomainEvent<TEventType extends EventType, TData> {
    eventId: string;
    eventType: TEventType;
    occurredAt: string;
    correlationId: string;
    data: TData;
}

export interface OrderItem {
    productId: string;
    quantity: number;
    unitPrice: number;
}

export interface OrderCreatedData {
    orderId: string;
    customerId: string;
    items: OrderItem[];
    totalAmount: number;
}

export type OrderCreatedEvent = DomainEvent<
    "OrderCreated",
    OrderCreatedData
>;

export interface PaymentAuthorizedData {
    orderId: string;
    paymentId: string;
    amount: number;
    items: OrderItem[];
}

export type PaymentAuthorizedEvent = DomainEvent<
    "PaymentAuthorized",
    PaymentAuthorizedData
>;

export interface InventoryReservedData {
    orderId: string;
    reservationId: string;
    items: OrderItem[];
}

export type InventoryReservedEvent = DomainEvent<
    "InventoryReserved",
    InventoryReservedData
>;

export interface InventoryReservationFailedData {
    orderId: string;
    reason: string;
    unavailableItems: {
        productId: string;
        requestedQuantity: number;
        availableQuantity: number;
    }[];
}

export type InventoryReservationFailedEvent = DomainEvent<
    "InventoryReservationFailed",
    InventoryReservationFailedData
>;

export type CommerceEvent =
    | OrderCreatedEvent
    | PaymentAuthorizedEvent
    | InventoryReservedEvent
    | InventoryReservationFailedEvent;