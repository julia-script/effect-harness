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

## Applying the patch with Bun

Save the linked patch as `effect.patch` in the consuming application's root, then use Bun's package patching:

```sh
bun patch effect@4.0.1
git apply --directory=node_modules/effect effect.patch
bun patch --commit node_modules/effect
```

Bun records patchedDependencies and a generated patch file for subsequent installs. Those application files belong with the application's lockfile. The source repository already records this patch, so its `bun install` applies it.

The patch targets 4.0.1. Applying it to another version is not a supported compatibility claim. A later dependency version needs verification of both behaviors before removing the patch.
