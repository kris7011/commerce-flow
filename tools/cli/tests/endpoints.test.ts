import assert from "node:assert/strict";
import test from "node:test";
import {
    resolveEndpoints
} from "../src/endpoints.js";

test(
    "defaults to the ports published by npm run dev and docker compose",
    () => {
        const endpoints =
            resolveEndpoints({});

        assert.equal(endpoints.services["order-service"], "http://127.0.0.1:3001");
        assert.equal(endpoints.services["notification-service"], "http://127.0.0.1:3005");
        assert.equal(endpoints.rabbitMqUrl, "amqp://guest:guest@127.0.0.1:5672");
    }
);

test(
    "accepts overrides and strips trailing slashes",
    () => {
        const endpoints =
            resolveEndpoints({
                ORDER_SERVICE_URL: "http://orders.local:8080/",
                RABBITMQ_URL: "amqps://user:secret@broker.local/vhost"
            });

        assert.equal(endpoints.services["order-service"], "http://orders.local:8080");
        assert.equal(endpoints.rabbitMqUrl, "amqps://user:secret@broker.local/vhost");
    }
);

test(
    "rejects invalid URLs without echoing their value",
    () => {
        assert.throws(
            () => resolveEndpoints({
                RABBITMQ_URL: "not a url with secret"
            }),
            (error: unknown) =>
                error instanceof Error &&
                error.message === "RABBITMQ_URL is not a valid URL."
        );

        assert.throws(
            () => resolveEndpoints({
                ORDER_SERVICE_URL: "amqp://wrong-protocol"
            }),
            /ORDER_SERVICE_URL must use one of: http:, https:/
        );
    }
);
