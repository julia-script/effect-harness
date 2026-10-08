# effect-harness

Model and tool execution for Effect applications.

The harness prepares model prompts, resolves named extensions and executes native Effect AI requests. Use it to connect a model catalogue to application tools, hooks and execution policy. Use [effect-harness/durable](durable.md) for saved conversations and native Workflow executors.

## Install

```sh
npm install effect-harness effect
```

Use the equivalent `pnpm add`, `yarn add` or `bun add` command if you prefer.

## Bind a native Toolkit

Declare tools with Effect AI and supply their handlers through a Layer. The harness binding adds recovery policy to the native Toolkit:

```ts
import * as Registry from 'effect-harness/Registry'
import * as ToolRegistration from 'effect-harness/ToolRegistration'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Tool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'

const Uppercase = Tool.make('uppercase', {
  description: 'Convert text to uppercase.',
  parameters: Schema.Struct({ text: Schema.String }),
  success: Schema.String,
})
const toolkit = Toolkit.make(Uppercase)
const handlers = toolkit.toLayer({ uppercase: ({ text }) => Effect.succeed(text.toUpperCase()) })

export const Tools = Layer.unwrap(
  ToolRegistration.bind(toolkit, { uppercase: { replay: 'safe' } }).pipe(
    Effect.map((tools) => Registry.layer([{ name: 'text-tools', tools }])),
  ),
).pipe(Layer.provide(handlers))
```

Provide `Tools` and a `Model.Catalog` Layer to `Executor.layer`. Bind host services while constructing the tools; per-call Invocation services remain dynamic. This tool is safe to repeat because it is a pure transformation. Tool replay defaults to unsafe.

## Find the right API

| Modules                                                                      | Purpose                                                      |
| ---------------------------------------------------------------------------- | ------------------------------------------------------------ |
| `Model`, `Agent`, `Executor`                                                 | Model references, request configuration and execution        |
| `Registry`, `Extension`, `Hook`                                              | Selectable capabilities and lifecycle hooks                  |
| `ToolRegistration`, `Invocation`, `ToolResult`                               | Toolkit binding, progress and model-facing results           |
| `Transcript`, `PromptPreparation`, `ResponseAccumulator`                     | Transcript projection, prompt preparation and response state |
| `Env`, `NativeFiles`, `NodeEnv`, `NodeNativeFiles`, `MutationLocks`, `tools` | Environment capabilities and portable coding tools           |
| `testing`                                                                    | Adapter conformance helpers                                  |

Import concept modules through public subpaths, as above. Root imports also expose concept namespaces.

## Continue

- [First conversation tutorial](../tutorials/first-conversation.md): compose the model, tools and durable execution.
- [Register tools](../tools.md): host services, coding tools and progress reporting.
- [Connect a provider](../providers.md): supply a model catalogue.
- [Configuration reference](../reference/configuration.md): selection and execution policies.
- [Service reference](../reference/packages.md#generic-harness): Layer dependencies.

Model responses use native Effect AI validation. See [Effect compatibility](../reference/compatibility.md) for failed-response recording and retry behavior. License attribution is retained in [NOTICE](../../packages/effect-harness/NOTICE).
