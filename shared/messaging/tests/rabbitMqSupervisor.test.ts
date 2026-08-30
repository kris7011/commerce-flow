import assert from "node:assert/strict";
import test from "node:test";
import type {
    AppLogger,
    LogContext
} from "@commerce-flow/logging";
import {
    RabbitMqSupervisor,
    type RabbitMqReadinessClient
} from "../src/rabbitMqSupervisor.js";

test(
    "initializes RabbitMQ before reporting ready",
    async () => {
        const client =
            new FakeReadinessClient(
                false
            );

        const controller =
            new AbortController();

        let initializeCalls =
            0;

        const supervisor =
            new RabbitMqSupervisor(
                client,
                async () => {
                    initializeCalls +=
                        1;

                    client.ready =
                        true;
                },
                {},
                {
                    logger:
                        silentLogger,

                    sleep:
                        async (
                            _milliseconds,
                            signal
                        ) => {
                            if (
                                !signal
                                    .aborted
                            ) {
                                controller
                                    .abort();
                            }
                        }
                }
            );

        assert.equal(
            supervisor.isReady(),
            false
        );

        await supervisor.run(
            controller.signal
        );

        assert.equal(
            initializeCalls,
            1
        );

        assert.equal(
            client.isReady(),
            true
        );

        assert.equal(
            supervisor.isReady(),
            true
        );
    }
);

test(
    "initializes once even when the RabbitMQ client is already connected",
    async () => {
        const client =
            new FakeReadinessClient(
                true
            );

        const controller =
            new AbortController();

        let initializeCalls =
            0;

        const supervisor =
            new RabbitMqSupervisor(
                client,
                async () => {
                    initializeCalls +=
                        1;
                },
                {},
                {
                    logger:
                        silentLogger,

                    sleep:
                        async () => {
                            controller
                                .abort();
                        }
                }
            );

        assert.equal(
            supervisor.isReady(),
            false
        );

        await supervisor.run(
            controller.signal
        );

        assert.equal(
            initializeCalls,
            1
        );

        assert.equal(
            supervisor.isReady(),
            true
        );
    }
);

test(
    "does not report ready while initialization is still running",
    async () => {
        const client =
            new FakeReadinessClient(
                true
            );

        const controller =
            new AbortController();

        let releaseInitialization:
            (() => void) | undefined;

        const initializationGate =
            new Promise<void>(
                resolve => {
                    releaseInitialization =
                        resolve;
                }
            );

        let initializationStarted:
            (() => void) | undefined;

        const initializationStartedGate =
            new Promise<void>(
                resolve => {
                    initializationStarted =
                        resolve;
                }
            );

        const supervisor =
            new RabbitMqSupervisor(
                client,
                async () => {
                    initializationStarted?.();

                    await initializationGate;
                },
                {},
                {
                    logger:
                        silentLogger,

                    sleep:
                        async () => {
                            controller
                                .abort();
                        }
                }
            );

        const runPromise =
            supervisor.run(
                controller.signal
            );

        await initializationStartedGate;

        assert.equal(
            client.isReady(),
            true
        );

        assert.equal(
            supervisor.isReady(),
            false
        );

        releaseInitialization?.();

        await runPromise;

        assert.equal(
            supervisor.isReady(),
            true
        );
    }
);

test(
    "reinitializes RabbitMQ after readiness is lost",
    async () => {
        const client =
            new FakeReadinessClient(
                false
            );

        const controller =
            new AbortController();

        let initializeCalls =
            0;

        let sleepCalls =
            0;

        const supervisor =
            new RabbitMqSupervisor(
                client,
                async () => {
                    initializeCalls +=
                        1;

                    client.ready =
                        true;
                },
                {},
                {
                    logger:
                        silentLogger,

                    sleep:
                        async () => {
                            sleepCalls +=
                                1;

                            if (
                                sleepCalls ===
                                1
                            ) {
                                client.ready =
                                    false;

                                return;
                            }

                            controller
                                .abort();
                        }
                }
            );

        await supervisor.run(
            controller.signal
        );

        assert.equal(
            initializeCalls,
            2
        );

        assert.equal(
            client.isReady(),
            true
        );

        assert.equal(
            supervisor.isReady(),
            true
        );
    }
);

test(
    "reports not ready immediately when the RabbitMQ client loses readiness",
    async () => {
        const client =
            new FakeReadinessClient(
                false
            );

        const controller =
            new AbortController();

        const supervisor =
            new RabbitMqSupervisor(
                client,
                async () => {
                    client.ready =
                        true;
                },
                {},
                {
                    logger:
                        silentLogger,

                    sleep:
                        async () => {
                            controller
                                .abort();
                        }
                }
            );

        await supervisor.run(
            controller.signal
        );

        assert.equal(
            supervisor.isReady(),
            true
        );

        client.ready =
            false;

        assert.equal(
            supervisor.isReady(),
            false
        );
    }
);

test(
    "retries after initialization failures",
    async () => {
        const client =
            new FakeReadinessClient(
                false
            );

        const controller =
            new AbortController();

        const logger =
            new RecordingLogger();

        let initializeCalls =
            0;

        let sleepCalls =
            0;

        const supervisor =
            new RabbitMqSupervisor(
                client,
                async () => {
                    initializeCalls +=
                        1;

                    if (
                        initializeCalls ===
                        1
                    ) {
                        throw new Error(
                            "RabbitMQ unavailable"
                        );
                    }

                    client.ready =
                        true;
                },
                {
                    retryDelayInMs:
                        25
                },
                {
                    logger,

                    sleep:
                        async (
                            milliseconds
                        ) => {
                            sleepCalls +=
                                1;

                            if (
                                sleepCalls ===
                                1
                            ) {
                                assert.equal(
                                    milliseconds,
                                    25
                                );

                                assert.equal(
                                    supervisor.isReady(),
                                    false
                                );

                                return;
                            }

                            controller
                                .abort();
                        }
                }
            );

        await supervisor.run(
            controller.signal
        );

        assert.equal(
            initializeCalls,
            2
        );

        assert.equal(
            supervisor.isReady(),
            true
        );

        assert.equal(
            logger.errorLogs.length,
            1
        );

        assert.equal(
            logger
                .errorLogs[0]
                ?.message,
            "RabbitMQ dependency initialization failed"
        );

        assert.match(
            String(
                logger
                    .errorLogs[0]
                    ?.error
            ),
            /RabbitMQ unavailable/
        );
    }
);

test(
    "stops without initializing when already aborted",
    async () => {
        const client =
            new FakeReadinessClient(
                false
            );

        const controller =
            new AbortController();

        controller.abort();

        let initializeCalls =
            0;

        const supervisor =
            new RabbitMqSupervisor(
                client,
                async () => {
                    initializeCalls +=
                        1;
                },
                {},
                {
                    logger:
                        silentLogger,

                    sleep:
                        async () =>
                            undefined
                }
            );

        await supervisor.run(
            controller.signal
        );

        assert.equal(
            initializeCalls,
            0
        );

        assert.equal(
            supervisor.isReady(),
            false
        );
    }
);

class FakeReadinessClient
    implements RabbitMqReadinessClient {
    constructor(
        public ready: boolean
    ) {
    }

    isReady(): boolean {
        return this.ready;
    }
}

class RecordingLogger
    implements AppLogger {
    readonly errorLogs: {
        message: string;
        error?: unknown;
        context?: LogContext;
    }[] = [];

    info(
        _message: string,
        _context?: LogContext
    ): void {
    }

    warn(
        _message: string,
        _context?: LogContext
    ): void {
    }

    error(
        message: string,
        error?: unknown,
        context?: LogContext
    ): void {
        this.errorLogs.push({
            message,
            error,
            context
        });
    }

    child(
        _context: LogContext
    ): AppLogger {
        return this;
    }
}

const silentLogger:
    AppLogger = {
    info(): void {
    },

    warn(): void {
    },

    error(): void {
    },

    child(): AppLogger {
        return silentLogger;
    }
};