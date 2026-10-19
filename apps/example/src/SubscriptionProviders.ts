/**
 * bun apps/example/src/SubscriptionProviders.ts <command> ./private/providers.json [instance-id-or-label]
 * Commands: add-codex, add-claude, status, authenticate-browser, authenticate-device,
 * list-models, run, recover, logout, remove. Set MODEL for run/recover; recovery adds a submission ID.
 * Set CODEX_CLIENT_VERSION when adding Codex. This example stores credentials in a separate private file.
 */
import * as Readline from 'node:readline/promises'
import { NodeRuntime, NodeServices } from '@effect/platform-node'
import * as Console from 'effect/Console'
import * as Config from 'effect/Config'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as FetchHttpClient from 'effect/http/FetchHttpClient'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Record from 'effect-harness/Record'
import * as Tool from 'effect-harness/Tool'
import * as Toolkit from 'effect-harness/Toolkit'
import * as Providers from 'effect-harness/Providers'
import * as ProviderStoreNode from 'effect-harness/ProviderStoreNode'
import { AuthError } from 'effect-harness/provider-auth'
import { CallbackServer, FileSystemCredentialStore } from 'effect-harness/provider-auth-node'
import { Provider as Codex, OAuth as CodexOAuth } from 'effect-harness/provider-openai-codex'
import { Provider as Claude, OAuth as ClaudeOAuth } from 'effect-harness/provider-anthropic'

const tools = Toolkit.make(
  Tool.make('countCharacters', {
    parameters: Schema.Struct({ text: Schema.String }),
    success: Schema.Int,
    replay: 'safe',
  }),
)
const handlers = tools.toLayer({ countCharacters: ({ text }) => Effect.succeed(text.length) })
const interaction = {
  authorize: (url: string) => Console.log(`Open this URL in your browser:\n${url}`),
  complete: Effect.acquireUseRelease(
    Effect.sync(() => Readline.createInterface({ input: process.stdin, output: process.stdout })),
    (terminal) =>
      Effect.tryPromise({
        try: (signal) =>
          terminal.question('Paste redirect URL or code#state (or finish in the browser): ', {
            signal,
          }),
        catch: () => new AuthError.AuthError({ reason: 'AccessDenied' }),
      }),
    (terminal) => Effect.sync(() => terminal.close()),
  ),
  device: ({ url, code }: { readonly url: string; readonly code: string }) =>
    Console.log(`Open ${url} and enter ${code}`),
}

const definitions = { codex: Codex.definition, claude: Claude.definition }

export const program = Effect.scoped(
  Effect.gen(function* () {
    const command = process.argv[2] ?? 'status'
    const path = process.argv[3]
    if (path === undefined)
      return yield* new AuthError.AuthError({ reason: 'InvalidConfiguration' })
    const execute = Effect.gen(function* () {
      const providers = yield* Providers.make({ definitions })
      if (command === 'status') return yield* Console.log(yield* providers.list)
      if (command === 'add-codex')
        return yield* Console.log(
          yield* providers.add({
            provider: 'codex',
            label: process.argv[4] ?? 'Codex',
            config: { clientVersion: yield* Config.String('CODEX_CLIENT_VERSION') },
          }),
        )
      if (command === 'add-claude')
        return yield* Console.log(
          yield* providers.add({
            provider: 'claude',
            label: process.argv[4] ?? 'Claude',
            config: {},
          }),
        )
      const instance = yield* providers.get(process.argv[4] ?? '')
      if (command.startsWith('authenticate-')) {
        if (instance.provider === 'codex') {
          const options = {
            interaction,
            ...(instance.config.authBaseUrl === undefined
              ? {}
              : { authBaseUrl: instance.config.authBaseUrl }),
          }
          const secrets =
            command === 'authenticate-device'
              ? yield* CodexOAuth.loginDevice(options)
              : yield* CodexOAuth.loginBrowser(options)
          return yield* Console.log(yield* providers.update(instance.id, { secrets }))
        }
        const secrets = yield* ClaudeOAuth.loginBrowser({
          interaction,
          ...(instance.config.authorizeUrl === undefined
            ? {}
            : { authorizeUrl: instance.config.authorizeUrl }),
          ...(instance.config.tokenUrl === undefined ? {} : { tokenUrl: instance.config.tokenUrl }),
        })
        return yield* Console.log(yield* providers.update(instance.id, { secrets }))
      }
      if (command === 'logout')
        return yield* Console.log(yield* providers.update(instance.id, { secrets: null }))
      if (command === 'remove') return yield* providers.remove(instance.id)
      if (command === 'list-models')
        return yield* Console.log(yield* providers.models.list({ instanceId: instance.id }))
      if (command !== 'run' && command !== 'recover')
        return yield* new AuthError.AuthError({ reason: 'InvalidConfiguration' })
      const selected = yield* providers.models.resolve({
        instanceId: instance.id,
        modelId: yield* Config.String('MODEL'),
      })
      yield* Effect.gen(function* () {
        const runtime = yield* HarnessRuntime.make({ models: [selected], tools })
        const root = yield* runtime.backend.root
        let id: Record.SubmissionId
        if (command === 'recover')
          id = yield* Schema.decodeUnknownEffect(Record.SubmissionId)(process.argv[5])
        else {
          const submission = yield* runtime.backend.submit({
            conversationId: root,
            draft: {
              type: 'input',
              content: 'Use countCharacters to count hello, then tell me the result.',
            },
          })
          id = submission.id
          yield* Console.log(
            `Submission: ${id}. Reopen with recover, the instance ID, and this submission ID.`,
          )
        }
        yield* runtime.backend.wait(id)
        const entries = yield* Session.scanEntries(runtime.session, { conversationId: root }).pipe(
          Stream.runCollect,
        )
        yield* Console.log(entries.filter((entry) => entry.kind === 'assistant'))
      }).pipe(
        Effect.provide(
          Storage.layerJsonl({ filePath: `${path}.${instance.id}.conversation.jsonl` }),
        ),
      )
    })
    yield* execute.pipe(
      Effect.provide(
        Layer.merge(
          ProviderStoreNode.layer({ path }),
          FileSystemCredentialStore.layer({ path: `${path}.credentials` }),
        ),
      ),
    )
  }),
).pipe(
  Effect.provide(
    Layer.mergeAll(FetchHttpClient.layer, NodeServices.layer, CallbackServer.layer, handlers),
  ),
)

NodeRuntime.runMain(program)
