# Effect Harness

Durable AI conversations with Effect services, schema-backed state, and scoped local execution.

[Documentation & interactive lab](https://effect-harness-docs-docs-prod-xywgv64k67qqnujx.juliaortiz-js.workers.dev) · [effect-harness on npm](https://www.npmjs.com/package/effect-harness)

Define tools with schemas, supply their handlers through Layers, and submit input through a `Harness` client. The local runtime persists conversation history, submissions, tool checkpoints, and application documents through a `Storage` service.

## Submit and recover

Your application supplies `tools`, `ToolsLive` (their handlers), and `ModelLive` (a native Effect AI `LanguageModel`). This is the complete client program and local Layer composition:

```ts
import { Effect, Layer } from 'effect'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as SqlEventJournal from 'effect/eventlog/SqlEventJournal'
import * as Harness from 'effect-harness/Harness'
import * as Conversation from 'effect-harness/Conversation'
import * as Submission from 'effect-harness/Submission'
import * as Storage from 'effect-harness/Storage'

const HarnessLive = Harness.layerLocal({ tools }).pipe(
  Layer.provide(ToolsLive),
  Layer.provide(ModelLive),
  Layer.provide(
    Storage.layerJournal().pipe(
      Layer.provide(SqlEventJournal.layer()),
      Layer.provide(SqliteClient.layer({ filename: './agent.sqlite' })),
    ),
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

The [single-file recovery example](apps/example/src/tour/Recovery.ts) supplies an offline model and handlers and runs without an API key.

## Discover models lazily

`Provider.make` accepts a provider ID, a `models` source with independent `list` and
`get` Effects, and a `create` callback that builds an existing `Model` descriptor.
`ModelCatalog.make({ providers })` preserves each provider's literal ID, error
types, and service requirements. Construction performs no discovery.

Use `ModelCatalog.list(catalog, { provider })` for lightweight model summaries and
`ModelCatalog.get(catalog, { provider, modelId })` for capabilities, limits, and
supported reasoning controls. A direct lookup does not enumerate models. Sources
own pagination and query semantics; an upstream bulk endpoint may still require a
bulk request. `Provider.fromModels` supplies local discovery with pagination and
case-insensitive search.

`ModelCatalog.resolve(catalog, { provider, modelId, options })` validates explicit
effort and thinking controls before calling the selected provider's factory. It
preserves the factory's requirements and the returned model's inference
requirements. Provider factories translate controls and enforce any additional
protocol-specific constraints. Effort values are per-model strings; unsupported
values fail instead of being clamped. The generic catalog leaves omitted options
untouched; concrete factories choose their effective defaults before construction. Unknown capabilities stay unknown;
`ModelInfo.toDefinition` requires complete capability declarations before building
a runtime model.

`Provider.cached(provider, { capacity, timeToLive })` binds discovery services at
cache construction without fetching data. Create one cache per account and
endpoint. The cache shares concurrent requests, limits pages and model entries
separately, and does not retain failures. Its `invalidate` Effect clears both
caches. Model construction still requires its own services. Plain catalogs do not
cache or capture services implicitly.

Concrete subscription providers connect discovery to the durable adapters:

- `provider-openai-codex/Provider.make({ id, binding, clientVersion })` fetches the
  authenticated Codex account's catalog lazily. Both discovery and resolution
  require `HttpClient` and `CredentialStore`. The upstream endpoint returns its
  entire inventory; filtering and pagination happen locally. `clientVersion` is
  the explicit Codex catalog compatibility version. `catalogUrl` can override
  discovery independently of the inference endpoint. For shared bulk discovery,
  `yield* provider-openai-codex/Catalog.cached(options, { timeToLive })` creates a
  lazy account-bound snapshot source for `makeWithSource`. Its list/get calls
  share one fetch; expiration or `invalidate` refreshes the whole snapshot.
  The generic provider cache alone caches pages and model IDs separately.
- `provider-anthropic/Provider.make({ id, binding })` discovers Claude models live
  through the Models API using the account's OAuth credentials. Listing, detail
  lookup, and resolution require `HttpClient` and `CredentialStore`. Each list call
  fetches one page; detail lookup requests only the selected ID. Expired tokens
  use the existing serialized refresh flow. `Catalog.oauth(options)` exposes the
  source separately. New model IDs and advertised efforts require no bundled
  inventory update. Discovery failures propagate instead of returning a static list.
- `provider-anthropic/Catalog.apiKey({ apiKey })` supplies live API-key discovery:
  one Models API page per list call and a direct request per detail lookup.
  Queries filter only the returned page, so an empty page can still have a cursor.
  This source requires `HttpClient`; it does not authenticate OAuth inference.

Both concrete providers expose `makeWithSource(options, source)` for custom
inventories, preserving each source operation's errors and services. They choose
reasoning effort from explicit selection, provider configuration, then metadata
default, and pin the effective settings in the existing durable configuration.
Claude OAuth uses adaptive thinking when advertised. Known models with thinking
binding controls default to server-side removal of invalidated thinking blocks;
other models use ordinary replay without a model-family rejection. Set
the Claude provider's `thinkingPolicy(info)` callback to return `server-drop` or
`preserve` for per-model compatibility, or set `thinkingPolicy` directly on
`OAuthModel.make`.
The effective policy is pinned as a durable capability; changing it rejects
restoration. Server-side removal requires adaptive thinking. This compatibility
is separate from the thinking modes reported by the Models API. Unsupported selections fail
before inference. API metadata can advertise controls beyond the OAuth adapter;
its factory still enforces the adapter's restrictions. Missing capabilities must
be supplied by a custom source before resolving a runtime model.

The [model catalog example](apps/example/src/tour/ModelCatalog.ts) composes both
live providers and demonstrates an explicit application-owned offline source. Run it with
`bun apps/example/src/tour/ModelCatalog.ts` after building the library.

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

See [tools and dependencies](docs/tools.md) and the [runnable tool definition](apps/example/src/Uppercase.ts).

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

These operations run inside `Effect.gen`. The [forks example](apps/example/src/tour/Forks.ts) runs parent and child in parallel.

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

Import `Document` and `Transaction` from their `effect-harness/*` subpaths. This handler changes state, so repeating it would increment twice; use the default unsafe replay policy. The [documents example](apps/example/src/tour/Documents.ts) includes the full handler and composition.

## Runtime support

The library uses standard JavaScript, Web APIs, and Effect services. `Storage.layerMemory` needs no platform services. `Storage.layerJournal({ storeId })` requires Effect’s `EventJournal`; the application chooses its backend. Own one runtime per journal store. Each write is a complete event, and reopening rebuilds the local projection. SQLite applications supply `SqlEventJournal.layer()` and a dedicated `SqlClient` at the application edge. `Storage.layerJsonl({ filePath })` requires `FileSystem`.

Tool dependencies define their environment: a handler can use a local filesystem, an issue-tracker service, or a remote object store supplied through Layers. The library has no Bun or Node runtime imports. The example application chooses Node adapters; the portable example also runs in a browser bundle. Bun is the repository's development tool.

One active local runtime owns a store. Recovery requires the same persisted data and the resources used by the tools. [Recovery](docs/explanation/recovery.md) describes interrupted tool execution and its limits.

## Client and runtime

`Harness` is the application client. `HarnessRuntime` owns the local agent loop and its `Session`. `HarnessBackend` is their schema-backed data boundary. `Harness.layerLocal` composes all three; `Harness.layer` accepts a backend supplied separately. Remote transports can implement that backend contract; this package currently implements local execution.

[Composition](docs/explanation/composition.md) explains the boundaries. [Extensions](apps/example/src/tour/Extensions.ts) shows static bundles of tools, hooks, and prompt sections.

## Run the examples

```sh
bun install
bun run build
bun run --cwd apps/example start
bun run --cwd apps/example tour
bun run --cwd apps/example recovery
bun run --cwd apps/example portable
```

The examples use an offline native Effect AI model. [Example commands](apps/example/README.md) describe each program. [Documentation](docs/README.md) covers providers, storage, documents, and observation.

## Develop

```sh
bun run check
bun run test
bun run build
```

The package root exposes concept namespaces; public leaf imports such as `effect-harness/Harness`, `effect-harness/Storage`, and `effect-harness/Toolkit` select individual modules. Provider adapters use `effect-harness/provider-openai/*` and `effect-harness/provider-anthropic/*`. Internal modules are private.

Package changes use [Changesets](.changeset/README.md).

## Thanks

Inspired by [Pi Durable](https://github.com/earendil-works/pi)'s embedded durable conversation design. Upstream attribution is retained in [NOTICE](packages/effect-harness/NOTICE).
