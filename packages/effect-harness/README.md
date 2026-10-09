# Effect Harness

Durable AI conversations with Effect services, schema-backed state, and scoped local execution.

[effect-harness on npm](https://www.npmjs.com/package/effect-harness)

Define tools with schemas, supply their handlers through Layers, and submit input through a `Harness` client. The local runtime persists conversation history, submissions, tool checkpoints, and application documents through a `Storage` service.

## Submit and recover

Your application supplies `tools`, `ToolsLive` (their handlers), and `ModelLive` (a native Effect AI `LanguageModel`). This is the complete client program and local Layer composition:

```ts
import { Effect, Layer } from 'effect'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as Harness from 'effect-harness/Harness'
import * as Conversation from 'effect-harness/Conversation'
import * as Submission from 'effect-harness/Submission'
import * as Storage from 'effect-harness/Storage'

const HarnessLive = Harness.layerLocal({ tools }).pipe(
  Layer.provide(ToolsLive),
  Layer.provide(ModelLive),
  Layer.provide(
    Storage.layerSql.pipe(Layer.provide(SqliteClient.layer({ filename: './agent.sqlite' }))),
  ),
)

const job = {
  type: 'input',
  content: 'Fix the flaky login test',
  requestId: 'job-42',
} as const

const program = Effect.gen(function* () {
  const harness = yield* Harness.Harness
  const root = yield* harness.root
  const submission = yield* root.pipe(Conversation.submit(job))
  return yield* Submission.wait(submission)
})

const settled = await Effect.runPromise(program.pipe(Effect.provide(HarnessLive)))
```

Run the same program after a crash. It opens the same store, resumes unfinished work, and retrieves the same submission by its request ID. `Submission.wait` returns an `InputDone` or `InputUnanswered` record; a completed record identifies the committed answer entry.

The [single-file recovery example](../../apps/example/src/tour/Recovery.ts) supplies an offline model and handlers and runs without an API key.

## Declare tools, then supply handlers

Declarations describe the shared contract. Handler Layers resolve the services used by the implementation.

```ts
import { Effect, Schema } from 'effect'
import * as Tool from 'effect-harness/Tool'
import * as Toolkit from 'effect-harness/Toolkit'
import { ToolExecution } from 'effect-harness/ToolExecution'

const tools = Toolkit.make(
  Tool.make('uppercase', {
    description: 'Convert text to uppercase',
    parameters: Schema.Struct({ text: Schema.String }),
    success: Schema.String,
    replay: 'safe',
  }),
)

const ToolsLive = tools.toLayer({
  uppercase: Effect.fn('uppercase')(function* ({ text }) {
    const execution = yield* ToolExecution
    yield* execution.output('Converting text\n')
    return text.toUpperCase()
  }),
})
```

`Toolkit.merge` combines declarations. Handlers receive decoded arguments and return schema-typed values. The runtime validates and encodes results, persists progress, and supplies invocation capabilities. Tools default to unsafe replay: an interrupted action is reported to the model without being repeated. Mark a tool safe only when repeating the whole handler is safe.

See [tools and dependencies](../../docs/tools.md) and the [runnable tool definition](../../apps/example/src/Uppercase.ts).

## Fork a conversation

A fork creates a conversation with inherited history through the selected entry. Parent and child can process independent submissions concurrently.

```ts
const answered =
  yield *
  root.pipe(
    Conversation.submit({ type: 'input', content: 'Investigate the login test' }),
    Effect.flatMap(Submission.wait),
  )

if (answered._tag === 'InputDone') {
  const fork = yield * root.pipe(Conversation.fork({ at: answered.answer }))
  yield * fork.pipe(Conversation.configure({ instructions: 'Explore another approach.' }))
  const next =
    yield *
    fork.pipe(
      Conversation.submit({ type: 'input', content: 'Check the timeout' }),
      Effect.flatMap(Submission.wait),
    )
}
```

These operations run inside `Effect.gen`. The [forks example](../../apps/example/src/tour/Forks.ts) runs parent and child in parallel.

## Persist application data

`Document.define` takes a schema, scope, and initial value. `Session.commit` stages document changes and entries in one atomic transaction. Tool handlers access that transaction through `ToolExecution.commit`.

```ts
const Calls = Document.define({
  kind: 'app.calls',
  version: 1,
  scope: 'session',
  schema: Schema.Struct({ count: Schema.Natural }),
  initial: () => ({ count: 0 }),
})

const target = { scope: { _tag: 'session' } } as const

// Inside a tool handler:
const execution = yield * ToolExecution

yield *
  execution.commit((tx) =>
    Effect.gen(function* () {
      yield* Transaction.ensureDocument(tx, Calls, target)
      yield* Transaction.updateDocument(tx, Calls, target, ({ count }) => ({ count: count + 1 }))
    }),
  )

// From the application:
const snapshot = yield * root.pipe(Conversation.snapshot(Calls, target))
```

Import `Document` and `Transaction` from their `effect-harness/*` subpaths. This handler changes state, so repeating it would increment twice; use the default unsafe replay policy. The [documents example](../../apps/example/src/tour/Documents.ts) includes the full handler and composition.

## Runtime support

The library uses standard JavaScript, Web APIs, and Effect services. `Storage.layerMemory` needs no platform services. `Storage.layerSql` requires `SqlClient`; the application chooses its implementation. `Storage.layerJsonl({ filePath })` requires `FileSystem`.

Tool dependencies define their environment: a handler can use a local filesystem, an issue-tracker service, or a remote object store supplied through Layers. The library has no Bun or Node runtime imports. The example application chooses Node adapters; the portable example also runs in a browser bundle. Bun is the repository's development tool.

One active local runtime owns a store. Recovery requires the same persisted data and the resources used by the tools. [Recovery](../../docs/explanation/recovery.md) describes interrupted tool execution and its limits.

## Client and runtime

`Harness` is the application client. `HarnessRuntime` owns the local agent loop and its `Session`. `HarnessBackend` is their schema-backed data boundary. `Harness.layerLocal` composes all three; `Harness.layer` accepts a backend supplied separately. Remote transports can implement that backend contract; this package currently implements local execution.

[Composition](../../docs/explanation/composition.md) explains the boundaries. [Extensions](../../apps/example/src/tour/Extensions.ts) shows static bundles of tools, hooks, and prompt sections.

## Run the examples

```sh
bun install
bun run build
bun run --cwd apps/example start
bun run --cwd apps/example tour
bun run --cwd apps/example recovery
bun run --cwd apps/example portable
```

The examples use an offline native Effect AI model. [Example commands](../../apps/example/README.md) describe each program. [Documentation](../../docs/README.md) covers providers, storage, documents, and observation.

## Develop

```sh
bun run check
bun run test
bun run build
```

The package root exposes concept namespaces; public leaf imports such as `effect-harness/Harness`, `effect-harness/Storage`, and `effect-harness/Toolkit` select individual modules. Provider adapters use `effect-harness/provider-openai/*` and `effect-harness/provider-anthropic/*`. Internal modules are private.

Package changes use [Changesets](../../.changeset/README.md).

## Thanks

Inspired by [Pi Durable](https://github.com/earendil-works/pi)'s embedded durable conversation design. Upstream attribution is retained in [NOTICE](NOTICE).
