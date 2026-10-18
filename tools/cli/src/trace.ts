import {
    createReadStream
} from "node:fs";
import {
    createInterface
} from "node:readline";
import {
    formatLogEntry,
    selectTrace
} from "./logTrace.js";

/*
 * Prints the log lines of one order across all services, in time order.
 *
 *   docker compose --profile app logs --no-log-prefix | npm run --silent trace -- <id>
 *   npm run --silent trace -- <id> service-logs.txt
 *
 * <id> is a correlation id (for example the x-correlation-id sent to
 * Order Service) or an order id.
 */

async function readLines(
    files: readonly string[]
): Promise<string[]> {
    const lines: string[] = [];

    const sources =
        files.length > 0
            ? files.map(file => createReadStream(file, "utf8"))
            : [process.stdin];

    for (const source of sources) {
        for await (const line of createInterface({
            input: source,
            crlfDelay: Number.POSITIVE_INFINITY
        })) {
            lines.push(line);
        }
    }

    return lines;
}

async function main(): Promise<number> {
    const [id, ...files] =
        process.argv.slice(2);

    if (!id) {
        console.error(
            "Usage: npm run --silent trace -- <correlation-or-order-id> [log files...]\n" +
            "Reads the services' JSON logs from the files or from stdin."
        );

        return 2;
    }

    const entries =
        selectTrace(
            await readLines(files),
            id
        );

    if (entries.length === 0) {
        console.error(`No log entries found for ${id}.`);
        return 1;
    }

    for (const entry of entries) {
        console.log(formatLogEntry(entry));
    }

    return 0;
}

process.exitCode =
    await main();
