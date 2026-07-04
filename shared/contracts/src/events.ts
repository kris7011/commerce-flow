export type EventType = "OrderCreated" | "PaymentAuthorized";

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
}

export type PaymentAuthorizedEvent = DomainEvent<
    "PaymentAuthorized",
    PaymentAuthorizedData
>;

export type CommerceEvent = OrderCreatedEvent | PaymentAuthorizedEvent;