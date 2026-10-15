import type {
    InventoryResultEvent,
    InventoryStockReader,
    InventoryTransaction,
    InventoryUnitOfWork,
    ReservationRecord
} from "./inventoryStore.js";

interface InventoryState {
    readonly stock: Map<string, number>;
    readonly reservations: Map<string, ReservationRecord>;
    readonly outbox: InventoryResultEvent[];
}

/**
 * In-memory adapter for unit tests and local experiments.
 *
 * Transactions run one at a time against a copy of the state that is
 * only swapped in when the work completes, so a throwing transaction
 * leaves no partial changes behind — the same contract the PostgreSQL
 * adapter provides.
 */
export class InMemoryInventoryStore
    implements InventoryUnitOfWork, InventoryStockReader {
    private state: InventoryState;

    private queue:
        Promise<unknown> =
        Promise.resolve();

    constructor(
        initialStock:
            Readonly<Record<string, number>>
    ) {
        this.state = {
            stock:
                new Map(
                    Object.entries(
                        initialStock
                    )
                ),
            reservations:
                new Map(),
            outbox: []
        };
    }

    async inTransaction<T>(
        work: (
            transaction: InventoryTransaction
        ) => Promise<T>
    ): Promise<T> {
        const run =
            this.queue.then(
                async () => {
                    const draft =
                        cloneState(
                            this.state
                        );

                    const result =
                        await work(
                            createTransaction(
                                draft
                            )
                        );

                    this.state =
                        draft;

                    return result;
                }
            );

        this.queue =
            run.catch(
                () => undefined
            );

        return await run;
    }

    async getAllStock(): Promise<
        Readonly<Record<string, number>>
    > {
        return Object.fromEntries(
            this.state.stock
        );
    }

    getReservations():
        readonly ReservationRecord[] {
        return [
            ...this.state.reservations.values()
        ];
    }

    getOutbox():
        readonly InventoryResultEvent[] {
        return [
            ...this.state.outbox
        ];
    }
}

function createTransaction(
    state: InventoryState
): InventoryTransaction {
    return {
        async lockOrder(
            orderId
        ) {
            return (
                state.reservations.get(
                    orderId
                ) ??
                null
            );
        },

        async lockStock(
            productIds
        ) {
            const quantities =
                new Map<string, number>();

            for (const productId of productIds) {
                const quantity =
                    state.stock.get(
                        productId
                    );

                if (quantity !== undefined) {
                    quantities.set(
                        productId,
                        quantity
                    );
                }
            }

            return quantities;
        },

        async decrementStock(
            quantityByProductId
        ) {
            for (
                const [
                    productId,
                    quantity
                ] of quantityByProductId
            ) {
                const current =
                    state.stock.get(
                        productId
                    ) ??
                    0;

                if (current < quantity) {
                    throw new Error(
                        `Stock for ${productId} would become negative.`
                    );
                }

                state.stock.set(
                    productId,
                    current - quantity
                );
            }
        },

        async recordReservation(
            record
        ) {
            if (
                state.reservations.has(
                    record.orderId
                )
            ) {
                throw new Error(
                    `A reservation for order ${record.orderId} already exists.`
                );
            }

            state.reservations.set(
                record.orderId,
                record
            );
        },

        async enqueueEvent(
            event
        ) {
            state.outbox.push(
                event
            );
        }
    };
}

function cloneState(
    state: InventoryState
): InventoryState {
    return {
        stock:
            new Map(
                state.stock
            ),
        reservations:
            new Map(
                state.reservations
            ),
        outbox: [
            ...state.outbox
        ]
    };
}
