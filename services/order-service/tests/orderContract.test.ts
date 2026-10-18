import assert from "node:assert/strict";
import test from "node:test";
import {
    EVENT_IDENTIFIER_MAX_LENGTH,
    parseOrderCreatedEvent
} from "@commerce-flow/contracts";
import {
    MAX_ORDER_ITEM_QUANTITY,
    MAX_ORDER_ITEMS,
    parseCreateOrderRequest
} from "../src/orderRequestValidator.js";
import {
    OrderService
} from "../src/orderService.js";

/*
 * Guards the boundary between the public API and the event contracts:
 * whatever the API accepts (answering 201) must produce an OrderCreated
 * event that Payment Service accepts after the wire round trip.
 * Otherwise the order would be dead-lettered right after the client was
 * told it had been created.
 */

const longestIdentifier =
    "x".repeat(
        EVENT_IDENTIFIER_MAX_LENGTH
    );

const boundaryRequests:
    Readonly<Record<string, unknown>> = {
        "longest identifiers": {
            customerId:
                longestIdentifier,
            items: [
                {
                    productId:
                        longestIdentifier,
                    quantity: 1,
                    unitPrice: 1
                }
            ]
        },
        "largest quantity and price": {
            customerId:
                "customer-001",
            items: [
                {
                    productId:
                        "product-001",
                    quantity:
                        MAX_ORDER_ITEM_QUANTITY,
                    unitPrice:
                        1e290
                }
            ]
        },
        "most items, all for one product": {
            customerId:
                "customer-001",
            items:
                Array.from(
                    {
                        length:
                            MAX_ORDER_ITEMS
                    },
                    () => ({
                        productId:
                            "product-001",
                        quantity:
                            MAX_ORDER_ITEM_QUANTITY,
                        unitPrice: 0.01
                    })
                )
        },
        "fractional prices": {
            customerId:
                "customer-001",
            items: [
                {
                    productId:
                        "product-001",
                    quantity: 3,
                    unitPrice: 0.1
                }
            ]
        }
    };

for (
    const [
        name,
        request
    ] of Object.entries(
        boundaryRequests
    )
) {
    test(
        `an accepted request produces a contract-valid event: ${name}`,
        () => {
            const validation =
                parseCreateOrderRequest(
                    request
                );

            assert.equal(
                validation.success,
                true,
                "The API should accept this request."
            );

            if (!validation.success) {
                return;
            }

            const {
                event
            } =
                new OrderService()
                    .createOrder(
                        validation.data
                    );

            const received: unknown =
                JSON.parse(
                    JSON.stringify(
                        event
                    )
                );

            assert.deepEqual(
                parseOrderCreatedEvent(
                    received
                ),
                event
            );
        }
    );
}
