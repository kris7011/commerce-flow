import type {
    OrderItem
} from "@commerce-flow/contracts";
import type {
    InventoryRepository
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

    async getAvailableQuantity(
        productId: string
    ): Promise<number> {
        return (
            this.stockByProductId
                .get(productId) ??
            0
        );
    }

    async reserve(
        items: readonly OrderItem[]
    ): Promise<void> {
        const requestedQuantityByProductId =
            aggregateQuantities(items);

        this.ensureStockIsAvailable(
            requestedQuantityByProductId
        );

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
    }

    async getAllStock(): Promise<
        Readonly<Record<string, number>>
    > {
        return Object.fromEntries(
            this.stockByProductId
        );
    }

    private ensureStockIsAvailable(
        requestedQuantityByProductId:
            ReadonlyMap<string, number>
    ): void {
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