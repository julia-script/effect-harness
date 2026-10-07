# Effect compatibility

The package peer requirement is Effect `^4.0.1`. The implementation and examples are checked against Effect `4.0.1` and the corresponding `4.0.1` platform/provider adapters. Examples use the released `effect/workflow`, `effect/ai`, `effect/persistence`, `effect/eventlog` and `effect/cluster` module paths.

## Effect 4.0.1 patch

Unknown-tool settlement requires the supplied [AI compatibility patch](../../patches/effect@4.0.1.patch). The package peer range does not apply that patch automatically to a consumer's installation.

The generic Executor opts into `allowUnknownToolCalls` together with `disableToolCallResolution` so it can commit an unavailable-tool result for an undeclared name. The patch preserves that name and its parameters; declared tools retain native parameter validation. Unpatched Effect rejects undeclared calls before this settlement path. Strict rejection remains the default for ordinary native model calls.

## Native Workflow recovery

The harness does not patch ClusterWorkflowEngine or its transaction handling. Built-in Activities use ordinary native replay and domain receipts. Their domain commits and the native Activity replies have separate commit points; a saved domain receipt resolves the gap on recovery. See [replay and recovery](../explanation/recovery.md#the-commit-to-reply-gap).

A separate upstream recovery deadlock was reproduced with `ClusterSchema.WithTransaction` Activities and a shared SQLite client. That annotation lets a recovered Activity request acquire the connection before its Workflow has replayed far enough to register its definition. Earlier database access can then block the replay needed to release the connection. Built-in harness Activities do not use that annotation. The library does not supply a workaround for custom transaction-annotated Activities.

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
