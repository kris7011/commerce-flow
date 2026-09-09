import type {
    OrderItem
} from "@commerce-flow/contracts";
import type {
    InventoryRepository,
    InventoryReservationResult,
    UnavailableInventoryItem
} from "./inventoryRepository.js";

export class InMemoryInventoryRepository
    implements InventoryRepository {
    private readonly stockByProductId:
        Map<string, number>;

    constructor(
        initialStock:
            Readonly<Record<string, number>>
    ) {
        this.stockByProductId =
            new Map(
                Object.entries(initialStock)
            );
    }

    async tryReserve(
        items: readonly OrderItem[]
    ): Promise<InventoryReservationResult> {
        const requestedQuantityByProductId =
            aggregateQuantities(items);

        const unavailableItems:
            UnavailableInventoryItem[] = [];

        for (
            const [
                productId,
                requestedQuantity
            ] of
            requestedQuantityByProductId
        ) {
            const availableQuantity =
                this.stockByProductId
                    .get(productId) ??
                0;

            if (
                availableQuantity <
                requestedQuantity
            ) {
                unavailableItems.push({
                    productId,
                    requestedQuantity,
                    availableQuantity
                });
            }
        }

        if (
            unavailableItems.length >
            0
        ) {
            return {
                status:
                    "InsufficientStock",
                unavailableItems
            };
        }

        for (
            const [
                productId,
                requestedQuantity
            ] of
            requestedQuantityByProductId
        ) {
            const currentQuantity =
                this.stockByProductId
                    .get(productId) ??
                0;

            this.stockByProductId.set(
                productId,
                currentQuantity -
                requestedQuantity
            );
        }

        return {
            status:
                "Reserved"
        };
    }

    async getAllStock(): Promise<
        Readonly<Record<string, number>>
    > {
        return Object.fromEntries(
            this.stockByProductId
        );
    }
}

function aggregateQuantities(
    items: readonly OrderItem[]
): Map<string, number> {
    const quantityByProductId =
        new Map<string, number>();

    for (const item of items) {
        const currentQuantity =
            quantityByProductId
                .get(item.productId) ??
            0;

        quantityByProductId.set(
            item.productId,
            currentQuantity +
            item.quantity
        );
    }

    return quantityByProductId;
}