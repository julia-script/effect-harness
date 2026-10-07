# @effect-harness/harness

Model and tool execution for Effect v4 applications. The generic harness prepares prompts, resolves named extensions and executes native Effect AI requests. Durable storage and native Workflow registration are supplied by `@effect-harness/durable`.

```sh
bun add @effect-harness/harness effect@4.0.1
```

Public subpaths include `Model`, `Agent`, `Registry`, `Extension`, `Executor`, `Tool`, `Hook`, `Invocation`, `Context`, `ToolResult`, `Env`, `NodeEnv`, `MutationLocks`, `tools` and `testing`. Root imports expose concept namespaces.

`Executor.layer` consumes Registry and Model.Catalog. Bind ordinary Toolkits with `Tool.bind`, providing handler and host Layers at construction. Invocation services remain dynamic. Replay defaults to unsafe; a safe policy must tolerate re-execution of the tool body.

Start with [the conversation tutorial](../../docs/tutorials/first-conversation.md), [tool integration](../../docs/tools.md), or [provider integration](../../docs/providers.md). [Configuration](../../docs/reference/configuration.md) records policy defaults; [packages and services](../../docs/reference/packages.md#generic-harness) lists boundaries.

Unknown-tool settlement on Effect 4.0.1 requires the [compatibility patch](../../docs/reference/compatibility.md). License attribution is retained in [NOTICE](NOTICE).
