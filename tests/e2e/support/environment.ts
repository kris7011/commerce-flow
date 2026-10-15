import {
    createTestDatabase,
    createTestVirtualHost,
    type TestDatabase
} from "@commerce-flow/testing";

/*
 * Every end-to-end test file runs against its own RabbitMQ virtual host
 * and its own PostgreSQL database. Durable queues and stock left behind
 * by earlier (or failed) runs can therefore never leak into a test, and
 * the development database is never modified.
 */

export interface E2eEnvironment {
    readonly virtualHost: string;
    readonly rabbitMqUrl: string;
    readonly database: TestDatabase;

    dispose(): Promise<void>;
}

export async function createE2eEnvironment():
    Promise<E2eEnvironment> {
    const virtualHost =
        await createTestVirtualHost();

    let database:
        TestDatabase;

    try {
        database =
            await createTestDatabase();
    } catch (error) {
        await virtualHost.delete();

        throw error;
    }

    return {
        virtualHost:
            virtualHost.name,
        rabbitMqUrl:
            virtualHost.url,
        database,

        async dispose() {
            await Promise.allSettled([
                database.drop(),
                virtualHost.delete()
            ]);
        }
    };
}
