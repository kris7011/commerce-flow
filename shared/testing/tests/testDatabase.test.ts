import assert from "node:assert/strict";
import test from "node:test";
import {
    assertConnectedToTestDatabase,
    assertTestDatabaseName,
    isTestDatabaseName
} from "../src/testDatabase.js";
import {
    assertTestVirtualHostName
} from "../src/testVirtualHost.js";

test(
    "recognises only generated test database names",
    () => {
        assert.equal(
            isTestDatabaseName(
                "commerce_flow_test_lz3k9a1b_0a1b2c3d"
            ),
            true
        );

        for (
            const name of [
                "commerce_flow",
                "postgres",
                "commerce_flow_test_",
                "commerce_flow_test_x",
                "commerce_flow_test_lz3k9a1b_0a1b2c3d; DROP DATABASE commerce_flow",
                "prefix_commerce_flow_test_lz3k9a1b_0a1b2c3d"
            ]
        ) {
            assert.equal(
                isTestDatabaseName(
                    name
                ),
                false,
                name
            );
        }
    }
);

test(
    "refuses destructive operations on the development database",
    () => {
        assert.throws(
            () =>
                assertTestDatabaseName(
                    "commerce_flow"
                ),
            /Refusing to run a destructive operation/
        );
    }
);

test(
    "verifies the live connection before destructive operations",
    async () => {
        const fakePool = {
            query: async () => ({
                rows: [
                    {
                        databaseName:
                            "commerce_flow"
                    }
                ]
            })
        } as unknown as Parameters<
            typeof assertConnectedToTestDatabase
        >[0];

        await assert.rejects(
            assertConnectedToTestDatabase(
                fakePool
            ),
            /Refusing/
        );
    }
);

test(
    "refuses to delete virtual hosts without the test prefix",
    () => {
        for (
            const name of [
                "/",
                "commerce-flow",
                "commerce-flow-test-",
                "commerce-flow-test-zzzzzzzz"
            ]
        ) {
            assert.throws(
                () =>
                    assertTestVirtualHostName(
                        name
                    ),
                /Refusing to delete RabbitMQ virtual host/
            );
        }

        assertTestVirtualHostName(
            "commerce-flow-test-0a1b2c3d"
        );
    }
);
