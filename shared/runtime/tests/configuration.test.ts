import assert from "node:assert/strict";
import test from "node:test";
import {
    ConfigurationError,
    readInteger,
    readPort,
    readString,
    readUrl
} from "../src/configuration.js";

test(
    "uses defaults for missing or blank values",
    () => {
        assert.equal(
            readPort(
                {},
                "PORT",
                3001
            ),
            3001
        );

        assert.equal(
            readString(
                {
                    NAME: "   "
                },
                "NAME",
                "fallback"
            ),
            "fallback"
        );
    }
);

test(
    "rejects non-numeric and out-of-range integers",
    () => {
        assert.throws(
            () =>
                readPort(
                    {
                        PORT: "30O1"
                    },
                    "PORT",
                    3001
                ),
            (error: unknown) =>
                error instanceof ConfigurationError &&
                /PORT must be an integer/.test(
                    error.message
                )
        );

        assert.throws(
            () =>
                readPort(
                    {
                        PORT: "70000"
                    },
                    "PORT",
                    3001
                ),
            /between 0 and 65535/
        );

        assert.throws(
            () =>
                readInteger(
                    {
                        BATCH: "1.5"
                    },
                    "BATCH",
                    10,
                    {
                        min: 1,
                        max: 100
                    }
                ),
            /must be an integer/
        );
    }
);

test(
    "refuses development connection defaults in production",
    () => {
        assert.throws(
            () =>
                readUrl(
                    {
                        NODE_ENV: "production"
                    },
                    "DATABASE_URL",
                    "postgresql://dev:dev@localhost/app",
                    [
                        "postgresql:"
                    ]
                ),
            /DATABASE_URL must be set when NODE_ENV is 'production'/
        );

        assert.equal(
            readUrl(
                {
                    NODE_ENV: "production",
                    DATABASE_URL:
                        "postgresql://app@db.internal/app"
                },
                "DATABASE_URL",
                "postgresql://dev:dev@localhost/app",
                [
                    "postgresql:"
                ]
            ),
            "postgresql://app@db.internal/app"
        );
    }
);

test(
    "validates URL protocols without leaking credentials",
    () => {
        assert.equal(
            readUrl(
                {
                    RABBITMQ_URL:
                        "amqp://user:secret@broker:5672/vhost"
                },
                "RABBITMQ_URL",
                "amqp://localhost",
                [
                    "amqp:",
                    "amqps:"
                ]
            ),
            "amqp://user:secret@broker:5672/vhost"
        );

        assert.throws(
            () =>
                readUrl(
                    {
                        DATABASE_URL:
                            "mysql://user:secret@db/app"
                    },
                    "DATABASE_URL",
                    "postgresql://localhost/app",
                    [
                        "postgres:",
                        "postgresql:"
                    ]
                ),
            (error: unknown) => {
                assert.ok(
                    error instanceof ConfigurationError
                );

                assert.doesNotMatch(
                    error.message,
                    /secret/
                );

                return true;
            }
        );
    }
);
