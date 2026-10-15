import {
    createServer,
    type RequestListener,
    type Server
} from "node:http";
import type {
    AddressInfo
} from "node:net";
import type {
    AppLogger
} from "@commerce-flow/logging";

/** Long-running work such as a RabbitMQ supervisor or outbox relay. */
export interface BackgroundTask {
    readonly name: string;

    run(
        signal: AbortSignal
    ): Promise<void>;
}

export interface ProcessLike {
    once(
        event: "SIGINT" | "SIGTERM",
        listener: () => void
    ): unknown;

    exit(
        code: number
    ): void;
}

export interface ServiceHostOptions {
    readonly port: number;
    readonly requestListener: RequestListener;
    readonly logger: AppLogger;

    /** Runs before the HTTP server listens, e.g. database migrations. */
    readonly start?: () => Promise<void>;

    readonly backgroundTasks?: readonly BackgroundTask[];

    /**
     * Releases resources after the server and background tasks have
     * stopped (drain consumers, close connections and pools).
     */
    readonly stop?: () => Promise<void>;

    readonly shutdownTimeoutInMs?: number;
    readonly process?: ProcessLike;
}

export interface RunningService {
    readonly port: number;

    shutdown(
        reason: string,
        exitCode?: number
    ): Promise<void>;
}

/**
 * Starts an HTTP service with background tasks and a graceful,
 * bounded shutdown on SIGINT/SIGTERM:
 *
 * 1. stop accepting HTTP connections,
 * 2. abort background tasks and wait for them to finish,
 * 3. release resources via `stop`,
 * 4. exit — forcibly if the steps above exceed the timeout.
 *
 * A background task that fails unexpectedly triggers a shutdown with a
 * non-zero exit code so the orchestrator can restart the process.
 */
export async function runService(
    options: ServiceHostOptions
): Promise<RunningService> {
    const {
        logger
    } = options;

    const processLike =
        options.process ??
        process;

    const shutdownTimeoutInMs =
        options.shutdownTimeoutInMs ??
        15_000;

    const controller =
        new AbortController();

    let server:
        Server | undefined;

    let shutdownPromise:
        Promise<void> | undefined;

    const taskPromises:
        Promise<void>[] = [];

    const shutdown =
        (
            reason: string,
            exitCode = 0
        ): Promise<void> => {
            shutdownPromise ??=
                performShutdown(
                    reason,
                    exitCode
                );

            return shutdownPromise;
        };

    async function performShutdown(
        reason: string,
        exitCode: number
    ): Promise<void> {
        logger.info(
            "Service shutting down",
            {
                reason
            }
        );

        const forceExitTimer =
            setTimeout(
                () => {
                    logger.error(
                        "Graceful shutdown timed out; forcing exit",
                        undefined,
                        {
                            shutdownTimeoutInMs
                        }
                    );

                    processLike.exit(
                        exitCode === 0
                            ? 1
                            : exitCode
                    );
                },
                shutdownTimeoutInMs
            );

        forceExitTimer.unref();

        await closeServer(
            server
        );

        controller.abort();

        await Promise.allSettled(
            taskPromises
        );

        try {
            await options.stop?.();
        } catch (error) {
            logger.error(
                "Failed to release resources during shutdown",
                error
            );
        }

        clearTimeout(
            forceExitTimer
        );

        logger.info(
            "Service stopped",
            {
                exitCode
            }
        );

        processLike.exit(
            exitCode
        );
    }

    processLike.once(
        "SIGINT",
        () => {
            void shutdown(
                "SIGINT"
            );
        }
    );

    processLike.once(
        "SIGTERM",
        () => {
            void shutdown(
                "SIGTERM"
            );
        }
    );

    try {
        await options.start?.();

        server =
            createServer(
                options.requestListener
            );

        await listen(
            server,
            options.port
        );
    } catch (error) {
        logger.error(
            "Service failed to start",
            error,
            {
                port:
                    options.port
            }
        );

        await shutdown(
            "startup failure",
            1
        );

        throw error;
    }

    const port =
        (server.address() as AddressInfo).port;

    logger.info(
        "Service listening",
        {
            port
        }
    );

    for (const task of options.backgroundTasks ?? []) {
        taskPromises.push(
            task
                .run(
                    controller.signal
                )
                .then(
                    () => {
                        if (!controller.signal.aborted) {
                            logger.error(
                                "Background task stopped unexpectedly",
                                undefined,
                                {
                                    task:
                                        task.name
                                }
                            );

                            void shutdown(
                                `${task.name} stopped`,
                                1
                            );
                        }
                    },
                    error => {
                        logger.error(
                            "Background task failed",
                            error,
                            {
                                task:
                                    task.name
                            }
                        );

                        void shutdown(
                            `${task.name} failed`,
                            1
                        );
                    }
                )
        );
    }

    return {
        port,
        shutdown
    };
}

function listen(
    server: Server,
    port: number
): Promise<void> {
    return new Promise(
        (
            resolve,
            reject
        ) => {
            const onError =
                (error: Error): void => {
                    reject(
                        error
                    );
                };

            server.once(
                "error",
                onError
            );

            server.listen(
                port,
                () => {
                    server.off(
                        "error",
                        onError
                    );

                    resolve();
                }
            );
        }
    );
}

function closeServer(
    server: Server | undefined
): Promise<void> {
    if (!server?.listening) {
        return Promise.resolve();
    }

    return new Promise(
        resolve => {
            server.close(
                () => {
                    resolve();
                }
            );

            // Keep-alive connections would otherwise hold close() open.
            server.closeIdleConnections();
        }
    );
}
