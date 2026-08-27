import assert from "node:assert/strict";
import test from "node:test";
import type {
    OrderItem
} from "@commerce-flow/contracts";
import {
    parseCreateOrderRequest
} from "../src/orderRequestValidator.js";

test(
    "parses a valid create order request",
    () => {
        const input: unknown = {
            customerId:
                "customer-001",
            items: [
                {
                    productId:
                        "washing-machine-01",
                    quantity: 1,
                    unitPrice: 4999
                }
            ]
        };

        const result =
            parseCreateOrderRequest(
                input
            );

        assert.equal(
            result.success,
            true
        );

        if (!result.success) {
            assert.fail(
                "Expected the request " +
                "to pass validation."
            );
        }

        assert.deepEqual(
            result.data,
            input
        );
    }
);

test(
    "rejects invalid request objects",
    () => {
        const invalidRequests:
            unknown[] = [
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
                    customerId:
                        "customer-001"
                },
                {
                    customerId:
                        "customer-001",
                    items: []
                },
                {
                    customerId:
                        "customer-001",
                    items:
                        "not-an-array"
                }
            ];

        for (
            const invalidRequest
            of invalidRequests
        ) {
            const result =
                parseCreateOrderRequest(
                    invalidRequest
                );

            assert.equal(
                result.success,
                false
            );
        }
    }
);

test(
    "rejects invalid order items",
    () => {
        const invalidItems:
            unknown[] = [
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
                    productId:
                        "product-001",
                    quantity: 0,
                    unitPrice: 100
                },
                {
                    productId:
                        "product-001",
                    quantity: -1,
                    unitPrice: 100
                },
                {
                    productId:
                        "product-001",
                    quantity:
                        Number.NaN,
                    unitPrice: 100
                },
                {
                    productId:
                        "product-001",
                    quantity:
                        Number
                            .POSITIVE_INFINITY,
                    unitPrice: 100
                },
                {
                    productId:
                        "product-001",
                    quantity: 1,
                    unitPrice: -1
                },
                {
                    productId:
                        "product-001",
                    quantity: 1,
                    unitPrice:
                        Number.NaN
                },
                {
                    productId:
                        "product-001",
                    quantity: 1,
                    unitPrice:
                        Number
                            .POSITIVE_INFINITY
                }
            ];

        for (
            const invalidItem
            of invalidItems
        ) {
            const result =
                parseCreateOrderRequest({
                    customerId:
                        "customer-001",
                    items: [
                        invalidItem
                    ]
                });

            assert.equal(
                result.success,
                false
            );
        }
    }
);

function validOrderItem():
    OrderItem {
    return {
        productId:
            "washing-machine-01",
        quantity: 1,
        unitPrice: 4999
    };
}