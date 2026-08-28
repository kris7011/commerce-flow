import type {
    AppLogger
} from "@commerce-flow/logging";

export interface RabbitMqReadinessClient {
    isReady(): boolean;
}

export interface RabbitMqSupervisorOptions {
    readonly retryDelayInMs?:
    number;

    readonly readyCheckIntervalInMs?:
    number;
}

export interface RabbitMqSupervisorDependencies {
    readonly logger:
    AppLogger;

    sleep(
        milliseconds: number,
        signal: AbortSignal
    ): Promise<void>;
}

export class RabbitMqSupervisor {
    private readonly retryDelayInMs:
        number;

    private readonly readyCheckIntervalInMs:
        number;

    private readonly logger:
        AppLogger;

    private readonly delay:
        RabbitMqSupervisorDependencies[
        "sleep"
        ];

    constructor(
        private readonly client:
            RabbitMqReadinessClient,
        private readonly initialize:
            () => Promise<void>,
        options:
            RabbitMqSupervisorOptions = {},
        dependencies:
            Partial<
                RabbitMqSupervisorDependencies
            > = {}
    ) {
        this.retryDelayInMs =
            options.retryDelayInMs ??
            2000;

        this.readyCheckIntervalInMs =
            options.readyCheckIntervalInMs ??
            1000;

        this.logger =
            dependencies.logger ??
            createSilentLogger();

        this.delay =
            dependencies.sleep ??
            sleep;
    }

    async run(
        signal: AbortSignal
    ): Promise<void> {
        while (!signal.aborted) {
            if (
                this.client.isReady()
            ) {
                await this.delay(
                    this.readyCheckIntervalInMs,
                    signal
                );

                continue;
            }

            try {
                await this.initialize();
            } catch (error) {
                if (signal.aborted) {
                    return;
                }

                this.logger.error(
                    "RabbitMQ dependency initialization failed",
                    error
                );
            }

            if (signal.aborted) {
                return;
            }

            await this.delay(
                this.client.isReady()
                    ? this.readyCheckIntervalInMs
                    : this.retryDelayInMs,
                signal
            );
        }
    }
}

function sleep(
    milliseconds: number,
    signal: AbortSignal
): Promise<void> {
    if (signal.aborted) {
        return Promise.resolve();
    }

    return new Promise(
        resolve => {
            const onAbort = (): void => {
                clearTimeout(
                    timeout
                );

                signal.removeEventListener(
                    "abort",
                    onAbort
                );

                resolve();
            };

            const timeout =
                setTimeout(
                    () => {
                        signal
                            .removeEventListener(
                                "abort",
                                onAbort
                            );

                        resolve();
                    },
                    milliseconds
                );

            signal.addEventListener(
                "abort",
                onAbort,
                {
                    once: true
                }
            );
        }
    );
}

function createSilentLogger():
    AppLogger {
    const logger:
        AppLogger = {
        info(): void {
            // Intentionally silent.
        },

        warn(): void {
            // Intentionally silent.
        },

        error(): void {
            // Intentionally silent.
        },

        child(): AppLogger {
            return logger;
        }
    };

    return logger;
}