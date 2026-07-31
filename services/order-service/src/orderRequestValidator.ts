import type { OrderItem } from "@commerce-flow/contracts";
import type { CreateOrderRequest } from "./orderService.js";

export function isCreateOrderRequest(
    input: unknown
): input is CreateOrderRequest {
    if (!isRecord(input)) {
        return false;
    }

    if (
        typeof input.customerId !== "string" ||
        input.customerId.trim().length === 0
    ) {
        return false;
    }

    if (
        !Array.isArray(input.items) ||
        input.items.length === 0
    ) {
        return false;
    }

    return input.items.every(isOrderItem);
}

function isOrderItem(input: unknown): input is OrderItem {
    if (!isRecord(input)) {
        return false;
    }

    return (
        typeof input.productId === "string" &&
        input.productId.trim().length > 0 &&
        typeof input.quantity === "number" &&
        Number.isFinite(input.quantity) &&
        input.quantity > 0 &&
        typeof input.unitPrice === "number" &&
        Number.isFinite(input.unitPrice) &&
        input.unitPrice >= 0
    );
}

function isRecord(
    input: unknown
): input is Record<string, unknown> {
    return (
        typeof input === "object" &&
        input !== null &&
        !Array.isArray(input)
    );
}
