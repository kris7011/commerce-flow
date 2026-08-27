import * as z from "zod";
import type {
    CreateOrderRequest
} from "./orderService.js";

const nonBlankStringSchema =
    z.string().refine(value => {
        return value.trim().length > 0;
    });

const orderItemSchema =
    z.object({
        productId:
            nonBlankStringSchema,

        quantity:
            z.number().positive(),

        unitPrice:
            z.number().nonnegative()
    });

const createOrderRequestSchema:
    z.ZodType<CreateOrderRequest> =
    z.object({
        customerId:
            nonBlankStringSchema,

        items:
            z.array(
                orderItemSchema
            ).min(1)
    });

export function parseCreateOrderRequest(
    input: unknown
) {
    return createOrderRequestSchema
        .safeParse(input);
}