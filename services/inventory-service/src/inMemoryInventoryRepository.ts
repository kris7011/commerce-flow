import type { OrderItem } from "@commerce-flow/contracts";
import type { InventoryRepository } from "./inventoryRepository.js";

export class InMemoryInventoryRepository implements InventoryRepository {
    private readonly stockByProductId: Map<string, number>;

    constructor(initialStock: Readonly<Record<string, number>>) {
        this.stockByProductId = new Map(Object.entries(initialStock));
    }

    getAvailableQuantity(productId: string): number {
        return this.stockByProductId.get(productId) ?? 0;
    }

    reserve(items: readonly OrderItem[]): void {
        const requestedQuantityByProductId = aggregateQuantities(items);

        this.ensureStockIsAvailable(requestedQuantityByProductId);

        for (const [productId, requestedQuantity] of requestedQuantityByProductId) {
            const currentQuantity = this.getAvailableQuantity(productId);

            this.stockByProductId.set(
                productId,
                currentQuantity - requestedQuantity
            );
        }
    }

    getAllStock(): Readonly<Record<string, number>> {
        return Object.fromEntries(this.stockByProductId);
    }

    private ensureStockIsAvailable(
        requestedQuantityByProductId: ReadonlyMap<string, number>
    ): void {
        for (const [productId, requestedQuantity] of requestedQuantityByProductId) {
            const availableQuantity = this.getAvailableQuantity(productId);

            if (availableQuantity < requestedQuantity) {
                throw new Error(
                    `Cannot reserve product '${productId}'. ` +
                    `Requested quantity: ${requestedQuantity}. ` +
                    `Available quantity: ${availableQuantity}.`
                );
            }
        }
    }
}

function aggregateQuantities(
    items: readonly OrderItem[]
): Map<string, number> {
    const quantityByProductId = new Map<string, number>();

    for (const item of items) {
        const currentQuantity =
            quantityByProductId.get(item.productId) ?? 0;

        quantityByProductId.set(
            item.productId,
            currentQuantity + item.quantity
        );
    }

    return quantityByProductId;
}