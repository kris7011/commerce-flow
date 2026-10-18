import * as z from "zod";
import {
    EVENT_IDENTIFIER_MAX_LENGTH
} from "@commerce-flow/contracts";
import type {
    CreateOrderRequest
} from "./orderService.js";

/*
 * The Order API is the only entry point for data from outside the
 * system, so it must never accept a request whose OrderCreated event a
 * consumer would reject: the client would get 201 Created while the
 * event went straight to a dead-letter queue. The limits below are
 * therefore at least as strict as the event contracts.
 */

/** Stock quantities are PostgreSQL INTEGER columns. */
export const MAX_ORDER_ITEM_QUANTITY =
    2_147_483_647;

/**
 * Bounds the request size and, together with the quantity limit,
 * keeps aggregated per-product quantities exact (safe integers).
 */
export const MAX_ORDER_ITEMS =
    100;

const nonBlankIdentifierSchema =
    z.string()
        .max(EVENT_IDENTIFIER_MAX_LENGTH)
        .refine(value => {
            return value.trim().length > 0;
        });

const orderItemSchema =
    z.object({
        productId:
            nonBlankIdentifierSchema,

        quantity:
            z.number()
                .int()
                .positive()
                .max(MAX_ORDER_ITEM_QUANTITY),

        unitPrice:
            z.number().nonnegative()
    });

const createOrderRequestSchema:
    z.ZodType<CreateOrderRequest> =
    z.object({
        customerId:
            nonBlankIdentifierSchema,

        items:
            z.array(
                orderItemSchema
            )
                .min(1)
                .max(MAX_ORDER_ITEMS)
    }).refine(
        request => {
            // A total that overflows to Infinity is serialised as
            // null and violates the totalAmount contract downstream.
            const totalAmount =
                request.items.reduce(
                    (sum, item) =>
                        sum +
                        item.quantity *
                        item.unitPrice,
                    0
                );

            return Number.isFinite(
                totalAmount
            );
        },
        {
            message:
                "The order total is too large.",
            path: [
                "items"
            ]
        }
    );

export function parseCreateOrderRequest(
    input: unknown
) {
    return createOrderRequestSchema
        .safeParse(input);
}
