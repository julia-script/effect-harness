# How to register application and coding tools

Use this guide to expose application functions to a harness conversation. Tools are ordinary Effect AI declarations and Toolkit handlers, bound into a named Registry extension.

## Bind a Toolkit

Install the generic harness:

```sh
npm install @effect-harness/harness effect
```

Define the schemas, supply the handlers and bind them when constructing the Registry:

```ts
import * as Registry from '@effect-harness/harness/Registry'
import * as ToolBinding from '@effect-harness/harness/Tool'
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
  ToolBinding.bind(toolkit, { uppercase: { replay: 'safe' } }).pipe(
    Effect.map((tools) => Registry.layer([{ name: 'text-tools', tools }])),
  ),
).pipe(Layer.provide(handlers))
```

Provide `Tools` to the harness Executor. If conversation settings restrict extensions, include `text-tools` in the selected extension names. If agent settings restrict tools, include `uppercase`. See [selection rules](reference/configuration.md#conversation-overrides).

Provide host service Layers while binding handlers. `Tool.bind` captures them for later calls. Keep per-call `Invocation` and `ToolCall` services dynamic; additional request services belong in the explicit `requestServices` argument.

## Report progress during execution

Add `Invocation.ToolCall` as a dependency to the native Tool declaration and yield that service inside its handler. Its `output`, `details` and `diagnostic` operations report distinct channels of progress. The [tutorial Toolkit](tutorials/first-conversation.md#2-bind-an-ordinary-ai-toolkit) shows the minimal binding; the API comments in `harness/Invocation` describe the reporting operations.

For richer results, provide `Tool.Metadata.project` to map the native result to model-facing content and committed metadata. Keep model content in `content`; private details and control requests remain separate. Provider media translation is described in [the provider guide](providers.md#tool-media-and-native-validation).

## Bind the portable coding tools

The Node environment adapter supplies narrow filesystem capabilities in addition to native Effect platform services. This Layer registers `read`, `write`, `edit` and `bash`:

```ts
import * as NodeServices from '@effect/platform-node/NodeServices'
import * as MutationLocks from '@effect-harness/harness/MutationLocks'
import * as NodeEnv from '@effect-harness/harness/NodeEnv'
import * as Registry from '@effect-harness/harness/Registry'
import * as CodingTools from '@effect-harness/harness/tools/CodingTools'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'

const Environment = NodeEnv.layer({ id: 'workspace', cwd: '/srv/project' })
const Locks = MutationLocks.layer

export const CodingRegistry = Layer.unwrap(
  CodingTools.make().pipe(Effect.map((extension) => Registry.layer([extension]))),
).pipe(Layer.provide(Layer.mergeAll(Environment, Locks)), Layer.provide(NodeServices.layer))
```

Install `@effect/platform-node` for this adapter. Replace `/srv/project` with the application's working directory. Share `Locks` across every runtime that writes files in the same environment namespace.

The environment is a capability boundary, not a filesystem sandbox. The host chooses access policy and which tools a conversation can select. For a remote or restricted environment, supply your own Env capabilities instead of the Node adapter.

## Choose an honest recovery policy

Replay is `unsafe` by default. Mark a tool `safe` only when repeating its body after a crash is acceptable. Pure transformations are a straightforward case; a payment, shell command or file mutation needs an application-specific decision. Built-in coding tools retain unsafe replay.

Use [replay and recovery](explanation/recovery.md#external-actions) to reason about an external action that completes before its receipt commits. The [tool policy reference](reference/configuration.md#tool-policy) lists execution and output defaults.

## Verify registration

Build the Registry and inspect Registry.snapshot. The bound tools should appear under the coding-tools extension. Check the [coding tool behavior reference](reference/configuration.md#coding-tool-behavior) when selecting read windows, shell timeouts and output limits.

For custom environment adapters, run the public harness/testing conformance helpers. Check scoped reader/watcher lifetimes and process cancellation before offering those tools to a conversation.
