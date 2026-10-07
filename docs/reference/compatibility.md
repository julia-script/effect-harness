# Effect compatibility

The package peer requirement is Effect `^4.0.1`. The implementation and examples are checked against Effect `4.0.1` and the corresponding `4.0.1` platform/provider adapters. Examples use the released `effect/workflow`, `effect/ai`, `effect/persistence`, `effect/eventlog` and `effect/cluster` module paths.

## Effect 4.0.1 patch

The full harness contract on Effect 4.0.1 requires the supplied [compatibility patch](../../patches/effect@4.0.1.patch). The package peer range does not apply that patch automatically to a consumer's installation. It changes two native behaviors:

| Behavior                                        | Patch contract                                                                                                                                  |
| ----------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| Caller-owned unknown tool settlement            | Preserves undeclared names/parameters only with allowUnknownToolCalls and disableToolCallResolution both true; declared tools retain validation |
| Cluster transaction-annotated Activity recovery | Waits for the recovered Activity definition before acquiring its transaction, allowing earlier cached Activity results to be read               |

The generic Executor opts into unknown-call preservation to commit unavailable-tool results. Unpatched Effect rejects those calls before that settlement path. The native type option permits the opt-in only for broad tool names and unknown parameter types. Strict rejection remains the default for ordinary native model calls.

The Cluster fix changes recovery ordering for transaction-annotated Activities. Those bodies and replies remain natively transactional. Built-in harness Activities use ordinary native replay and domain receipts, without transaction annotations.

## Applying the patch

Dependency patching is owned by the consuming application. Use your package manager's patch support, or `patch-package` if your package manager has no native patch command. Save the linked patch as `effect.patch` in the application root before applying it.

For npm with `patch-package`:

```sh
npm install --save-dev patch-package
git apply --directory=node_modules/effect effect.patch
npx patch-package effect
```

Run `patch-package` in the application's `postinstall` script so clean installs restore the patch. If there is already a postinstall command, retain it and append the patch step. Commit the generated file in `patches/`, the package manifest and the lockfile.

With pnpm, Yarn or Bun's native dependency patching, edit Effect inside the temporary directory or installed package prepared by that tool, apply the same patch there, and finalize it with the package manager's patch commit command. Retain its generated patch and manifest/lockfile entry. The source monorepo already records its patch for repository installs.

The patch targets 4.0.1. Applying it to another version is not a supported compatibility claim. A later dependency version needs verification of both behaviors before removing the patch.
