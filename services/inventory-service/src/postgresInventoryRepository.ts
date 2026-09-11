import type {
    OrderItem
} from "@commerce-flow/contracts";
import type {
    Pool,
    PoolClient
} from "pg";
import type {
    InventoryRepository,
    InventoryReservationResult,
    UnavailableInventoryItem
} from "./inventoryRepository.js";

interface InventoryStockRow {
    productId: string;
    quantity: number;
}

export class PostgresInventoryRepository
    implements InventoryRepository {
    constructor(
        private readonly pool: Pool
    ) { }

    async tryReserve(
        items: readonly OrderItem[]
    ): Promise<InventoryReservationResult> {
        const requestedQuantityByProductId =
            aggregateQuantities(
                items
            );

        const client =
            await this.pool.connect();

        try {
            await client.query(
                "BEGIN"
            );

            const availableQuantityByProductId =
                await this.lockRequestedStock(
                    client,
                    [
                        ...requestedQuantityByProductId
                            .keys()
                    ]
                );

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
                    availableQuantityByProductId
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
                await client.query(
                    "ROLLBACK"
                );

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
                await client.query(
                    `
                        UPDATE inventory_stock
                        SET quantity =
                            quantity - $2
                        WHERE product_id = $1
                    `,
                    [
                        productId,
                        requestedQuantity
                    ]
                );
            }

            await client.query(
                "COMMIT"
            );

            return {
                status:
                    "Reserved"
            };
        } catch (error) {
            try {
                await client.query(
                    "ROLLBACK"
                );
            } catch {
                // Preserve the original error.
            }

            throw error;
        } finally {
            client.release();
        }
    }

    async getAllStock(): Promise<
        Readonly<Record<string, number>>
    > {
        const result =
            await this.pool
                .query<InventoryStockRow>(
                    `
                        SELECT
                            product_id AS "productId",
                            quantity
                        FROM inventory_stock
                        ORDER BY product_id
                    `
                );

        return Object.fromEntries(
            result.rows.map(
                row => [
                    row.productId,
                    row.quantity
                ]
            )
        );
    }

    private async lockRequestedStock(
        client: PoolClient,
        productIds:
            readonly string[]
    ): Promise<
        ReadonlyMap<string, number>
    > {
        const result =
            await client
                .query<InventoryStockRow>(
                    `
                        SELECT
                            product_id AS "productId",
                            quantity
                        FROM inventory_stock
                        WHERE product_id =
                            ANY($1::text[])
                        ORDER BY product_id
                        FOR UPDATE
                    `,
                    [
                        productIds
                    ]
                );

        return new Map(
            result.rows.map(
                row => [
                    row.productId,
                    row.quantity
                ]
            )
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
