import assert from "node:assert/strict";
import test from "node:test";
import type { OrderItem } from "@commerce-flow/contracts";
import { isCreateOrderRequest } from "../src/orderRequestValidator.js";
import {
    OrderService,
    type CreateOrderRequest
} from "../src/orderService.js";

const fixedTime = "2026-07-31T08:00:00.000Z";

test(
    "creates an order event using a supplied correlation id",
    () => {
        const service = createOrderService([
            "order-001",
            "event-001"
        ]);

        const request: CreateOrderRequest = {
            customerId: "customer-001",
            items: [
                {
                    productId: "washing-machine-01",
                    quantity: 2,
                    unitPrice: 4999
                },
                {
                    productId: "dishwasher-01",
                    quantity: 1,
                    unitPrice: 3499.95
                }
            ]
        };

        const result = service.createOrder(
            request,
            "correlation-001"
        );

        assert.deepEqual(result.response, {
            orderId: "order-001",
            status: "Created",
            totalAmount: 13497.95,
            correlationId: "correlation-001"
        });

        assert.deepEqual(result.event, {
            eventId: "event-001",
            eventType: "OrderCreated",
            occurredAt: fixedTime,
            correlationId: "correlation-001",
            data: {
                orderId: "order-001",
                customerId: "customer-001",
                items: request.items,
                totalAmount: 13497.95
            }
        });
    }
);

test(
    "generates a correlation id when none is supplied",
    () => {
        const service = createOrderService([
            "generated-correlation-id",
            "order-001",
            "event-001"
        ]);

        const result = service.createOrder({
            customerId: "customer-001",
            items: [
                {
                    productId: "dryer-01",
                    quantity: 1,
                    unitPrice: 2999
                }
            ]
        });

        assert.equal(
            result.response.correlationId,
            "generated-correlation-id"
        );

        assert.equal(
            result.event.correlationId,
            "generated-correlation-id"
        );
    }
);

test(
    "generates a correlation id for a blank header",
    () => {
        const service = createOrderService([
            "generated-correlation-id",
            "order-001",
            "event-001"
        ]);

        const result = service.createOrder(
            {
                customerId: "customer-001",
                items: [
                    {
                        productId: "dryer-01",
                        quantity: 1,
                        unitPrice: 2999
                    }
                ]
            },
            "   "
        );

        assert.equal(
            result.response.correlationId,
            "generated-correlation-id"
        );
    }
);

test(
    "rounds the total amount to two decimal places",
    () => {
        const service = createOrderService([
            "order-001",
            "event-001"
        ]);

        const result = service.createOrder(
            {
                customerId: "customer-001",
                items: [
                    {
                        productId: "test-product",
                        quantity: 1,
                        unitPrice: 12.346
                    }
                ]
            },
            "correlation-001"
        );

        assert.equal(result.response.totalAmount, 12.35);
        assert.equal(result.event.data.totalAmount, 12.35);
    }
);

test(
    "accepts a valid create order request",
    () => {
        const input: unknown = {
            customerId: "customer-001",
            items: [
                {
                    productId: "washing-machine-01",
                    quantity: 1,
                    unitPrice: 4999
                }
            ]
        };

        assert.equal(isCreateOrderRequest(input), true);
    }
);

test(
    "rejects invalid request objects",
    () => {
        const invalidRequests: unknown[] = [
            null,
            undefined,
            [],
            {},
            {
                customerId: "",
                items: [
                    validOrderItem()
                ]
            },
            {
                customerId: "   ",
                items: [
                    validOrderItem()
                ]
            },
            {
                customerId: "customer-001"
            },
            {
                customerId: "customer-001",
                items: []
            },
            {
                customerId: "customer-001",
                items: "not-an-array"
            }
        ];

        for (const invalidRequest of invalidRequests) {
            assert.equal(
                isCreateOrderRequest(invalidRequest),
                false
            );
        }
    }
);

test(
    "rejects invalid order items",
    () => {
        const invalidItems: unknown[] = [
            null,
            {},
            {
                productId: "",
                quantity: 1,
                unitPrice: 100
            },
            {
                productId: "   ",
                quantity: 1,
                unitPrice: 100
            },
            {
                productId: "product-001",
                quantity: 0,
                unitPrice: 100
            },
            {
                productId: "product-001",
                quantity: -1,
                unitPrice: 100
            },
            {
                productId: "product-001",
                quantity: Number.NaN,
                unitPrice: 100
            },
            {
                productId: "product-001",
                quantity: Number.POSITIVE_INFINITY,
                unitPrice: 100
            },
            {
                productId: "product-001",
                quantity: 1,
                unitPrice: -1
            },
            {
                productId: "product-001",
                quantity: 1,
                unitPrice: Number.NaN
            },
            {
                productId: "product-001",
                quantity: 1,
                unitPrice: Number.POSITIVE_INFINITY
            }
        ];

        for (const invalidItem of invalidItems) {
            assert.equal(
                isCreateOrderRequest({
                    customerId: "customer-001",
                    items: [invalidItem]
                }),
                false
            );
        }
    }
);

function createOrderService(
    generatedIds: string[]
): OrderService {
    let currentIndex = 0;

    return new OrderService({
        generateId: () => {
            const generatedId =
                generatedIds[currentIndex];

            if (!generatedId) {
                throw new Error(
                    "The test did not provide enough generated IDs."
                );
            }

            currentIndex += 1;

            return generatedId;
        },
        getCurrentTime: () => fixedTime
    });
}

function validOrderItem(): OrderItem {
    return {
        productId: "washing-machine-01",
        quantity: 1,
        unitPrice: 4999
    };
}
