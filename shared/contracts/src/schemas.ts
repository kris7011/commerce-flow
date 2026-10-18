import * as z from "zod";
import type {
    DeliveryBookedEvent,
    InventoryReservationFailedEvent,
    InventoryReservedEvent,
    OrderCreatedEvent,
    OrderItem,
    PaymentAuthorizedEvent
} from "./events.js";

/*
 * Runtime schemas for the event contracts. Consumers validate every
 * incoming message before handling it, so a malformed or incompatible
 * message is rejected at the boundary (and dead-lettered) instead of
 * failing somewhere inside the business logic.
 *
 * The schemas follow the tolerant-reader principle: unknown fields
 * are ignored, which lets producers add optional fields without a
 * coordinated deployment. Breaking changes require a new event type.
 */

/**
 * Maximum length of every identifier in an event. Producers that accept
 * identifiers from outside (the Order API) must enforce it themselves,
 * otherwise they would emit events that every consumer dead-letters.
 */
export const EVENT_IDENTIFIER_MAX_LENGTH =
    200;

const identifierSchema =
    z.string()
        .min(1)
        .max(EVENT_IDENTIFIER_MAX_LENGTH);

const orderItemSchema:
    z.ZodType<OrderItem> =
    z.object({
        productId:
            identifierSchema,
        quantity:
            z.number()
                .int()
                .positive(),
        unitPrice:
            z.number()
                .nonnegative()
    });

function eventEnvelope<
    TEventType extends string,
    TData extends z.ZodType
>(
    eventType: TEventType,
    data: TData
) {
    return z.object({
        eventId:
            identifierSchema,
        eventType:
            z.literal(
                eventType
            ),
        occurredAt:
            z.iso.datetime(),
        correlationId:
            identifierSchema,
        data
    });
}

export const orderCreatedEventSchema:
    z.ZodType<OrderCreatedEvent> =
    eventEnvelope(
        "OrderCreated",
        z.object({
            orderId:
                identifierSchema,
            customerId:
                identifierSchema,
            items:
                z.array(
                    orderItemSchema
                ).min(1),
            totalAmount:
                z.number()
                    .nonnegative()
        })
    );

export const paymentAuthorizedEventSchema:
    z.ZodType<PaymentAuthorizedEvent> =
    eventEnvelope(
        "PaymentAuthorized",
        z.object({
            orderId:
                identifierSchema,
            paymentId:
                identifierSchema,
            amount:
                z.number()
                    .nonnegative(),
            items:
                z.array(
                    orderItemSchema
                ).min(1)
        })
    );

export const inventoryReservedEventSchema:
    z.ZodType<InventoryReservedEvent> =
    eventEnvelope(
        "InventoryReserved",
        z.object({
            orderId:
                identifierSchema,
            reservationId:
                identifierSchema,
            items:
                z.array(
                    orderItemSchema
                ).min(1)
        })
    );

export const inventoryReservationFailedEventSchema:
    z.ZodType<InventoryReservationFailedEvent> =
    eventEnvelope(
        "InventoryReservationFailed",
        z.object({
            orderId:
                identifierSchema,
            reason:
                z.string(),
            unavailableItems:
                z.array(
                    z.object({
                        productId:
                            identifierSchema,
                        requestedQuantity:
                            z.number()
                                .int()
                                .positive(),
                        availableQuantity:
                            z.number()
                                .int()
                                .nonnegative()
                    })
                )
        })
    );

export const deliveryBookedEventSchema:
    z.ZodType<DeliveryBookedEvent> =
    eventEnvelope(
        "DeliveryBooked",
        z.object({
            orderId:
                identifierSchema,
            deliveryId:
                identifierSchema,
            carrier:
                z.string()
                    .min(1),
            estimatedDeliveryDate:
                z.iso.date()
        })
    );

export function parseOrderCreatedEvent(
    value: unknown
): OrderCreatedEvent {
    return orderCreatedEventSchema.parse(
        value
    );
}

export function parsePaymentAuthorizedEvent(
    value: unknown
): PaymentAuthorizedEvent {
    return paymentAuthorizedEventSchema.parse(
        value
    );
}

export function parseInventoryReservedEvent(
    value: unknown
): InventoryReservedEvent {
    return inventoryReservedEventSchema.parse(
        value
    );
}

export function parseInventoryReservationFailedEvent(
    value: unknown
): InventoryReservationFailedEvent {
    return inventoryReservationFailedEventSchema.parse(
        value
    );
}

export function parseDeliveryBookedEvent(
    value: unknown
): DeliveryBookedEvent {
    return deliveryBookedEventSchema.parse(
        value
    );
}
