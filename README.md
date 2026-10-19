# Effect Harness

An Effect-based runtime for durable AI conversations, schema-backed application state, and resumable tasks. TypeScript applications compose storage, model providers, and tool implementations through Layers; clients talk to the running harness over RPC.

The library lives in `src/`. The root package is currently private; the instructions below run from a repository checkout with Bun, not an npm installation.

## Run a local conversation

Install dependencies and build the package exports:

```sh
bun install
bun run build
```

Save the following as `local.ts` at the repository root, then run `bun local.ts`. It uses the built-in scripted `Faux` model, so it requires no API key or network access. The model requests a real tool handler and then supplies a scripted answer; it is not an inference model.

```ts
import { Console, Effect, Layer, Schema } from 'effect'
import { HarnessClient, HarnessTransport } from 'effect-harness/client'
import { Extension, HarnessServer } from 'effect-harness/harness'
import { Faux, Model } from 'effect-harness/model'
import { Storage } from 'effect-harness/storage'
import { Tool, Toolkit } from 'effect-harness/tool'

const uppercase = Tool.make('uppercase', {
  description: 'Convert text to uppercase.',
  parameters: Schema.Struct({ text: Schema.String }),
  success: Schema.String,
})
const toolkit = Toolkit.make(uppercase)
const tools = Extension.make('text', { toolkit })
const ToolsLive = toolkit.toLayer({
  uppercase: ({ text }) => Effect.succeed(text.toUpperCase()),
})

const program = Effect.gen(function* () {
  const faux = yield* Faux.make()
  yield* faux.setResponses([
    Faux.message(Faux.toolCall('uppercase', { text: 'hello' }), { stopReason: 'toolUse' }),
    Faux.message('HELLO'),
  ])

  const ServerLive = HarnessServer.layer({
    extensions: [tools],
    settings: { model: faux.ref() },
  }).pipe(Layer.provide([ToolsLive, Model.layer([faux.provider]), Storage.layerMemory]))

  // Both sides must share the same transport instance.
  const AppLive = Layer.merge(ServerLive, HarnessClient.layer).pipe(
    Layer.provide(HarnessTransport.layerMemory),
  )

  yield* Effect.gen(function* () {
    const client = yield* HarnessClient.HarnessClient
    const root = yield* client.root()
    const submission = yield* root.submit({ type: 'input', content: 'Uppercase hello.' })
    const settled = yield* submission.wait
    yield* Console.log(settled)
    const { items } = yield* root.entries({ order: 'ascending' }, 50)
    yield* Console.log(items)
  }).pipe(Effect.provide(AppLive))
})

await Effect.runPromise(Effect.scoped(program))
```

`submission.wait` returns a settled record whose `status` can be `done` or `unanswered`; inspect that result rather than assuming a model answered. The entry page contains the committed conversation, including tool calls and results. Closing the scope releases the client, server, and storage resources.

## Persistence and deployment

`Storage.layerMemory` is volatile. For local persistence, replace it with `Storage.layerJsonl('./agent-data')` and provide Effect `FileSystem` and `Path` services from your platform adapter. The argument is a directory, not a JSONL filename. `Storage.layerSql()` accepts a host-provided Effect SQL `SqlClient` using SQLite or PostgreSQL; its optional `prefix` separates tables in a shared database. A Session must be the only writer of its store.

`HarnessTransport.layerMemory` connects client and server in one process. For a separate server, use `HarnessTransport.layerWebSocketServer(path)` with an Effect HTTP router and platform HTTP server; clients use `HarnessTransport.layerWebSocket(url)`.

The [Cloudflare example](examples/cloudflare/README.md) runs the harness in a Durable Object with SQLite persistence, account management, and a WebSocket command-line client. It supports OpenAI, ChatGPT/Codex sign-in, and Anthropic accounts without changing the client/server boundary.

## Architecture and public imports

Import concept namespaces from the lowercase subpaths below. Package exports resolve to `dist/`, so rebuild after source changes.

| Subpath (`effect-harness/…`)                       | Responsibility                                                                                       |
| -------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `client`                                           | `HarnessClient`, RPC contracts, named initialization, and memory/WebSocket transports.               |
| `harness`                                          | `Harness` loop, `HarnessServer`, conversations, submissions, extensions, generation, and compaction. |
| `model`                                            | Model/provider contracts, messages, transcripts, and the scripted `Faux` model.                      |
| `tool`                                             | Schema-backed `Tool` declarations, `Toolkit` handler Layers, tool results, and execution tasks.      |
| `storage`, `storage/schema`                        | Memory/JSONL/SQL persistence and stored record schemas.                                              |
| `session`, `delta`, `task`                         | Transactional session state, document changes, and durable task scheduling.                          |
| `credentials`, `credentials/bun`                   | Credential services and Bun-specific persistence.                                                    |
| `openai`, `anthropic`                              | Provider integrations; `openai/adapter` and `anthropic/adapter` expose account adapters.             |
| `accounts`                                         | Account management and its RPC boundary.                                                             |
| `coding`, `coding/bun`, `coding/sharp`, `codemode` | Coding tools and optional platform/image/code-mode integrations.                                     |
| `cloudflare`                                       | Durable Object hosting and routing.                                                                  |

A tool declaration defines its schema; `Toolkit.toLayer` supplies implementations; an `Extension` installs tools into the harness. `HarnessServer.layer` composes the default loop and serves its RPC contract, while `HarnessClient.layer` supplies the application-facing client. The client can run independently of the loop.

## Develop and verify

From the repository root:

```sh
bun install --frozen-lockfile
bun run build
bun run check
bun run test
bun pm pack --dry-run
```

`check` checks formatting, lint, and TypeScript. Tests live alongside the implementation under `src/`. The Cloudflare example lives under `examples/cloudflare/`. There is no automated package publishing workflow for this private package.

## Thanks

Inspired by [Pi Durable](https://github.com/earendil-works/pi)'s durable conversation design. Source module comments identify the Pi components this implementation models or ports; code mode uses `@earendil-works/pi-codemode`.
