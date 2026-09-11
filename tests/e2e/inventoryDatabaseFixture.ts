import {
    spawnSync
} from "node:child_process";
import {
    fileURLToPath
} from "node:url";

const repositoryRoot =
    fileURLToPath(
        new URL(
            "../../",
            import.meta.url
        )
    );

const resetInventorySql = `
    BEGIN;

    CREATE TABLE IF NOT EXISTS inventory_stock (
        product_id TEXT PRIMARY KEY,
        quantity INTEGER NOT NULL,
        CONSTRAINT inventory_stock_quantity_nonnegative
            CHECK (quantity >= 0)
    );

    TRUNCATE inventory_stock;

    INSERT INTO inventory_stock (
        product_id,
        quantity
    )
    VALUES
        ('washing-machine-01', 10),
        ('dishwasher-01', 5),
        ('dryer-01', 3);

    COMMIT;
`;

export function resetInventoryDatabase():
    void {
    const result =
        spawnSync(
            "docker",
            [
                "compose",
                "exec",
                "-T",
                "postgres",
                "psql",
                "-U",
                "commerce_flow",
                "-d",
                "commerce_flow",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                resetInventorySql
            ],
            {
                cwd:
                    repositoryRoot,
                encoding:
                    "utf8",
                windowsHide:
                    true
            }
        );

    if (result.error) {
        throw result.error;
    }

    if (
        result.status !==
        0
    ) {
        throw new Error(
            "Failed to reset Inventory " +
            "PostgreSQL test state." +
            `\nstdout:\n` +
            `${result.stdout || "(empty)"}` +
            `\nstderr:\n` +
            `${result.stderr || "(empty)"}`
        );
    }
}
