import js from "@eslint/js";
import tseslint from "typescript-eslint";

/*
 * Deliberately small, bug-focused rule set. Formatting is not enforced
 * here; the rules target defects that the type checker cannot catch,
 * above all unhandled promises (an unawaited rejection in an Express
 * handler previously crashed the Order Service).
 */
export default tseslint.config(
    {
        ignores: [
            "**/dist/**",
            "**/node_modules/**"
        ]
    },
    js.configs.recommended,
    ...tseslint.configs.recommended,
    {
        files: [
            "**/*.ts"
        ],
        languageOptions: {
            parserOptions: {
                projectService: true,
                tsconfigRootDir: import.meta.dirname
            }
        },
        rules: {
            "@typescript-eslint/no-floating-promises": [
                "error",
                {
                    // node:test schedules and awaits these itself.
                    allowForKnownSafeCalls: [
                        {
                            from: "package",
                            package: "node:test",
                            name: [
                                "test",
                                "it",
                                "describe",
                                "suite"
                            ]
                        }
                    ]
                }
            ],
            "@typescript-eslint/no-misused-promises": [
                "error",
                {
                    // Express 4 route handlers are async by design and
                    // handle their own errors.
                    checksVoidReturn: {
                        arguments: false
                    }
                }
            ],
            "@typescript-eslint/await-thenable": "error",
            "@typescript-eslint/no-explicit-any": "error",
            "@typescript-eslint/switch-exhaustiveness-check": "error",
            "@typescript-eslint/no-unused-vars": [
                "error",
                {
                    argsIgnorePattern: "^_",
                    varsIgnorePattern: "^_",
                    caughtErrors: "none"
                }
            ],
            eqeqeq: "error",
            "no-return-await": "off",
            "prefer-const": "error"
        }
    },
    {
        files: [
            "eslint.config.js"
        ],
        ...tseslint.configs.disableTypeChecked
    }
);
