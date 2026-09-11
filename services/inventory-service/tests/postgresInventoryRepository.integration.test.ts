import assert from "node:assert/strict";
import test from "node:test";
import {
    createInventoryDatabasePool,
    initializeInventoryDatabase
} from "../src/database.js";
import {
    PostgresInventoryRepository
} from "../src/postgresInventoryRepository.js";

const pool =
    createInventoryDatabasePool();

const repository =
    new PostgresInventoryRepository(
        pool
    );

test.before(
    async () => {
        await initializeInventoryDatabase(
            pool
        );
    }
);

test.beforeEach(
    async () => {
        await resetStock();
    }
);

test.after(
    async () => {
        await resetStock();

        await pool.end();
    }
);

test(
    "reserves available stock atomically",
    async () => {
        const result =
            await repository.tryReserve([
                {
                    productId:
                        "washing-machine-01",
                    quantity: 2,
                    unitPrice: 4999
                },
                {
                    productId:
                        "dryer-01",
                    quantity: 1,
                    unitPrice: 2999
                }
            ]);

        assert.deepEqual(
            result,
            {
                status:
                    "Reserved"
            }
        );

        const stock =
            await repository
                .getAllStock();

        assert.deepEqual(
            stock,
            {
                "dishwasher-01": 5,
                "dryer-01": 2,
                "washing-machine-01": 8
            }
        );
    }
);

test(
    "does not change any stock when one product is unavailable",
    async () => {
        const result =
            await repository.tryReserve([
                {
                    productId:
                        "washing-machine-01",
                    quantity: 2,
                    unitPrice: 4999
                },
                {
                    productId:
                        "dryer-01",
                    quantity: 4,
                    unitPrice: 2999
                }
            ]);

        assert.deepEqual(
            result,
            {
                status:
                    "InsufficientStock",
                unavailableItems: [
                    {
                        productId:
                            "dryer-01",
                        requestedQuantity:
                            4,
                        availableQuantity:
                            3
                    }
                ]
            }
        );

        const stock =
            await repository
                .getAllStock();

        assert.deepEqual(
            stock,
            {
                "dishwasher-01": 5,
                "dryer-01": 3,
                "washing-machine-01": 10
            }
        );
    }
);

test(
    "treats unknown products as zero stock",
    async () => {
        const result =
            await repository.tryReserve([
                {
                    productId:
                        "unknown-product",
                    quantity: 1,
                    unitPrice: 100
                }
            ]);

        assert.deepEqual(
            result,
            {
                status:
                    "InsufficientStock",
                unavailableItems: [
                    {
                        productId:
                            "unknown-product",
                        requestedQuantity:
                            1,
                        availableQuantity:
                            0
                    }
                ]
            }
        );
    }
);

test(
    "combines duplicate order lines before reserving",
    async () => {
        const result =
            await repository.tryReserve([
                {
                    productId:
                        "dryer-01",
                    quantity: 1,
                    unitPrice: 2999
                },
                {
                    productId:
                        "dryer-01",
                    quantity: 2,
                    unitPrice: 2999
                }
            ]);

        assert.deepEqual(
            result,
            {
                status:
                    "Reserved"
            }
        );

        const stock =
            await repository
                .getAllStock();

        assert.equal(
            stock["dryer-01"],
            0
        );
    }
);

test(
    "prevents concurrent reservations from overselling stock",
    async () => {
        await pool.query(
            `
                UPDATE inventory_stock
                SET quantity = 1
                WHERE product_id =
                    'dryer-01'
            `
        );

        const item = {
            productId:
                "dryer-01",
            quantity: 1,
            unitPrice: 2999
        };

        const results =
            await Promise.all([
                repository.tryReserve([
                    item
                ]),
                repository.tryReserve([
                    item
                ])
            ]);

        const reservedCount =
            results.filter(
                result =>
                    result.status ===
                    "Reserved"
            ).length;

        const rejectedCount =
            results.filter(
                result =>
                    result.status ===
                    "InsufficientStock"
            ).length;

        assert.equal(
            reservedCount,
            1
        );

        assert.equal(
            rejectedCount,
            1
        );

        const stock =
            await repository
                .getAllStock();

        assert.equal(
            stock["dryer-01"],
            0
        );
    }
);

async function resetStock():
    Promise<void> {
    await pool.query(
        "TRUNCATE inventory_stock"
    );

    await pool.query(
        `
            INSERT INTO inventory_stock (
                product_id,
                quantity
            )
            VALUES
                ($1, $2),
                ($3, $4),
                ($5, $6)
        `,
        [
            "washing-machine-01",
            10,
            "dishwasher-01",
            5,
            "dryer-01",
            3
        ]
    );
}
