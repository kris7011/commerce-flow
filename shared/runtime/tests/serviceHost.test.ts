import assert from "node:assert/strict";
import test from "node:test";
import type {
    AppLogger
} from "@commerce-flow/logging";
import {
    runService,
    type ProcessLike
} from "../src/serviceHost.js";

const silentLogger:
    AppLogger = {
    info: () => undefined,
    warn: () => undefined,
    error: () => undefined,
    child: () => silentLogger
};

class FakeProcess
    implements ProcessLike {
    readonly exitCodes: number[] = [];

    private markExited:
        () => void =
        () => undefined;

    readonly exited =
        new Promise<void>(
            resolve => {
                this.markExited =
                    resolve;
            }
        );

    private readonly listeners =
        new Map<string, () => void>();

    once(
        event: "SIGINT" | "SIGTERM",
        listener: () => void
    ): this {
        this.listeners.set(
            event,
            listener
        );

        return this;
    }

    private readonly errorListeners =
        new Map<string, (error: unknown) => void>();

    on(
        event: "uncaughtException" | "unhandledRejection",
        listener: (error: unknown) => void
    ): this {
        this.errorListeners.set(
            event,
            listener
        );

        return this;
    }

    emit(
        event: "SIGINT" | "SIGTERM"
    ): void {
        this.listeners.get(
            event
        )?.();
    }

    emitError(
        event: "uncaughtException" | "unhandledRejection",
        error: unknown
    ): void {
        this.errorListeners.get(
            event
        )?.(
            error
        );
    }

    exit(
        code: number
    ): void {
        this.exitCodes.push(
            code
        );

        this.markExited();
    }
}

test(
    "shuts down in order on SIGTERM: server, background tasks, resources",
    async () => {
        const fakeProcess =
            new FakeProcess();

        const steps: string[] = [];

        let taskStopped:
            () => void =
            () => undefined;

        const taskFinished =
            new Promise<void>(
                resolve => {
                    taskStopped =
                        resolve;
                }
            );

        const service =
            await runService({
                port: 0,
                requestListener:
                    (_request, response) => {
                        response.end(
                            "ok"
                        );
                    },
                logger:
                    silentLogger,
                process:
                    fakeProcess,
                start:
                    async () => {
                        steps.push(
                            "start"
                        );
                    },
                backgroundTasks: [
                    {
                        name:
                            "worker",
                        run:
                            async signal => {
                                await new Promise<void>(
                                    resolve => {
                                        signal.addEventListener(
                                            "abort",
                                            () => resolve(),
                                            {
                                                once: true
                                            }
                                        );
                                    }
                                );

                                steps.push(
                                    "task-stopped"
                                );

                                taskStopped();
                            }
                    }
                ],
                stop:
                    async () => {
                        steps.push(
                            "resources-released"
                        );
                    }
            });

        const response =
            await fetch(
                `http://127.0.0.1:${service.port}/`
            );

        assert.equal(
            await response.text(),
            "ok"
        );

        fakeProcess.emit(
            "SIGTERM"
        );

        await taskFinished;

        await service.shutdown(
            "test"
        );

        assert.deepEqual(
            steps,
            [
                "start",
                "task-stopped",
                "resources-released"
            ]
        );

        assert.deepEqual(
            fakeProcess.exitCodes,
            [
                0
            ]
        );

        await assert.rejects(
            fetch(
                `http://127.0.0.1:${service.port}/`
            )
        );
    }
);

test(
    "exits with a failure code when a background task crashes",
    async () => {
        const fakeProcess =
            new FakeProcess();

        let released =
            false;

        await runService({
                port: 0,
                requestListener:
                    (_request, response) => {
                        response.end();
                    },
                logger:
                    silentLogger,
                process:
                    fakeProcess,
                backgroundTasks: [
                    {
                        name:
                            "crashing-task",
                        run:
                            async () => {
                                throw new Error(
                                    "boom"
                                );
                            }
                    }
                ],
                stop:
                    async () => {
                        released =
                            true;
                    }
            });

        await fakeProcess.exited;

        assert.equal(
            released,
            true
        );

        assert.deepEqual(
            fakeProcess.exitCodes,
            [
                1
            ]
        );
    }
);

test(
    "releases resources and exits when startup fails",
    async () => {
        const fakeProcess =
            new FakeProcess();

        let released =
            false;

        await assert.rejects(
            runService({
                port: 0,
                requestListener:
                    (_request, response) => {
                        response.end();
                    },
                logger:
                    silentLogger,
                process:
                    fakeProcess,
                start:
                    async () => {
                        throw new Error(
                            "database unavailable"
                        );
                    },
                stop:
                    async () => {
                        released =
                            true;
                    }
            }),
            /database unavailable/
        );

        assert.equal(
            released,
            true
        );

        assert.deepEqual(
            fakeProcess.exitCodes,
            [
                1
            ]
        );
    }
);

test(
    "logs an uncaught exception and exits with a failure code",
    async () => {
        const fakeProcess =
            new FakeProcess();

        const errors: string[] = [];

        const logger:
            AppLogger = {
            ...silentLogger,
            error: message => {
                errors.push(
                    message
                );
            }
        };

        await runService({
            port: 0,
            requestListener:
                (_request, response) => {
                    response.end();
                },
            logger,
            process:
                fakeProcess
        });

        fakeProcess.emitError(
            "uncaughtException",
            new Error(
                "unexpected socket error"
            )
        );

        await fakeProcess.exited;

        assert.equal(
            errors[0],
            "Uncaught exception"
        );

        assert.deepEqual(
            fakeProcess.exitCodes,
            [
                1
            ]
        );
    }
);
