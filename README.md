# Effect Harness

An Effect-based runtime for durable AI conversations, schema-backed application state, and resumable tasks. TypeScript applications compose storage, model providers, and tool implementations through Layers; clients talk to the running harness over RPC.

The library lives in `src/` and is published as `effect-harness`. The lowercase subpaths documented here are the breaking 0.6 release API; the previously published 0.5 API uses different imports.

## Install

After the 0.6 release is published, install it with its required Effect peer:

```sh
bun add effect-harness@^0.6.0 effect@4.0.5
```

Until then, use a repository checkout and the build instructions below. Install optional peers only for the integrations you use: `@effect/ai-openai@4.0.5` for `openai`, `@effect/ai-anthropic@4.0.5` for `anthropic`, `sharp@0.34.5` for `coding/sharp`, and `alchemy@2.0.0-beta.83` plus `@effect/sql-sqlite-do@4.0.5` for `cloudflare`. Bun-specific subpaths require Bun; the portable core can also run under Node.

## Run a local conversation

Install dependencies and build the package exports:

```sh
bun install
bun run setup
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

The 0.6 cutover changes built-in kinds and keys from `pi.*` to `$*` (for example, `$user`, `$assistant`, `$agent`, and `$tool-output`). Custom input-entry kinds cannot start with `$`. Existing `pi.*` stores are not migrated automatically: use a fresh store or migrate it explicitly before opening it with this release. Update any consumers that inspect transcript kinds, task names, document keys, or tool-output discriminants.

`HarnessTransport.layerMemory` connects client and server in one process. For a separate server, use `HarnessTransport.layerWebSocketServer(path)` with an Effect HTTP router and platform HTTP server; clients use `HarnessTransport.layerWebSocket(url)`.

The [Cloudflare example](examples/cloudflare/README.md) runs the harness in a Durable Object with SQLite persistence, account management, and a WebSocket command-line client. It supports OpenAI, ChatGPT/Codex sign-in, and Anthropic accounts without changing the client/server boundary.

## Architecture and public imports

Import concept namespaces from the lowercase subpaths below. Package exports resolve to `dist/`, so rebuild after source changes.

The root import exposes `Accounts`, `Client`, `Credentials`, `Delta`, `Harness`, `Model`, `Session`, `Storage`, `Task`, and `Tool` namespaces without loading optional provider or platform integrations.

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
bun run setup
bun run build
bun run check
bun run test
bun run test:package
```

`setup` patches the local compiler and linter for Effect diagnostics; it is a developer command, not an install hook run in consumers' projects. `check` checks formatting, lint, and TypeScript. Tests live alongside the implementation under `src/`. `test:package` requires Node 24 or later, builds a real npm archive, installs it into an isolated consumer, checks its declarations, and runs a tool round under Node. CI exercises Node 24. The Cloudflare example lives under `examples/cloudflare/`.

## Thanks

Inspired by [Pi Durable](https://github.com/earendil-works/pi)'s durable conversation design. Source module comments identify the Pi components this implementation models or ports; code mode uses `@earendil-works/pi-codemode`. The distributed [NOTICE](NOTICE) retains the upstream attribution and MIT license.

## Maintainer release workflow

Releases use Changesets and npm trusted publishing, with Node 24 and Bun 1.4.2. The package stays at the published base version until the automated version PR is merged; the pending minor changeset prepares the breaking pre-1.0 release from `0.5.0` to `0.6.0`, including the built-in persisted kind/key prefix change from `pi.` to `$`.

1. Include a changeset in each PR with `bun run changeset`; use `bun run changeset --empty` for changes that intentionally do not release the package. `bun run check:changesets` checks additions against `origin/main`. CI requires this except for the same-repository `changeset-release/main` version branch, whose changesets have already been consumed.
2. Merge the change into protected `main`. After its push-triggered `CI` succeeds, `.github/workflows/release.yml` runs `changesets/action` to create or update the `changeset-release/main` version PR. Its `bun run version:packages` command updates the version and changelog, refreshes the Bun lockfile without install scripts, and formats the result.
3. Review the version PR before merging. PRs created or updated with `GITHUB_TOKEN` do not automatically trigger other workflows; a maintainer can close and reopen the PR to trigger PR CI on its latest revision. CI builds, checks, tests, and runs `bun run test:package` against a real installed tarball.
4. Merge the version PR. After the resulting main commit passes `CI`, the release workflow runs `bun run release` (`changeset publish`). The package's `prepack` script builds the distribution before npm packs it. Publication uses OIDC, requests provenance, and creates the release tag and GitHub release; no long-lived npm token is needed.

Release jobs accept only successful, same-repository `push` runs of `CI` on `main`. The tested SHA must also equal the release event's `github.sha`, because Changesets uses that SHA when resetting its version branch. Immediately before running Changesets, the workflow checks that remote `main` still matches and skips superseded runs. The checkout and Changesets base remain pinned to the tested commit, so a later branch update cannot silently substitute untested code. Release jobs are serialized.

In the npm settings for `effect-harness`, keep the GitHub Actions trusted publisher configured for repository **`julia-script/effect-harness`**, workflow filename **`release.yml`**, and environment **`npm-production`**. GitHub must have that environment configured, allow Actions to create pull requests, and protect `main` with the CI checks. These repository/npm settings are external prerequisites, not settings this checkout can provision.

This publishing-readiness change has not published anything to npm. `bun run release` performs a real publication and is reserved for the approved release workflow.
