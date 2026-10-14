/**
 * bun apps/example/src/SubscriptionProviders.ts login-codex-browser ./private/auth.json
 * Commands: login-codex-browser, login-codex-device, login-claude-browser, login-claude-copy,
 * run-codex, run-claude, recover-codex, recover-claude, claude-api-key, status, logout-codex.
 * Set MODEL explicitly. Recovery takes the printed submission id as the fourth argument.
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
import { AuthError, CredentialStore } from 'effect-harness/provider-auth'
import { CallbackServer, CredentialStore as FileStore } from 'effect-harness/provider-auth-node'
import { Model as CodexModel, OAuth as CodexOAuth } from 'effect-harness/provider-openai-codex'
import {
  AnthropicLanguageModel,
  OAuth as ClaudeOAuth,
  OAuthModel,
} from 'effect-harness/provider-anthropic'

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

export const program = Effect.scoped(
  Effect.gen(function* () {
    const command = process.argv[2] ?? 'status'
    const path = process.argv[3]
    if (path === undefined)
      return yield* new AuthError.AuthError({ reason: 'InvalidConfiguration' })
    const store = yield* FileStore.make({ path })
    const loginOptions = { profile: 'personal', interaction }
    const execute = Effect.gen(function* () {
      switch (command) {
        case 'login-codex-browser':
          return yield* Console.log(yield* CodexOAuth.loginBrowser(loginOptions))
        case 'login-codex-device':
          return yield* Console.log(yield* CodexOAuth.loginDevice(loginOptions))
        case 'login-claude-browser':
          return yield* Console.log(yield* ClaudeOAuth.loginBrowser(loginOptions))
        case 'login-claude-copy':
          return yield* Console.log(yield* ClaudeOAuth.loginCopyCode(loginOptions))
        case 'status':
          return yield* Console.log(yield* store.list)
        case 'logout-codex':
          return yield* CredentialStore.logout({ provider: 'openai-codex', profile: 'personal' })
      }
      const model = process.env.MODEL
      if (!model) return yield* new AuthError.AuthError({ reason: 'InvalidConfiguration' })
      if (command === 'claude-api-key') {
        const runtime = yield* HarnessRuntime.make({ tools }).pipe(
          Effect.provide(
            AnthropicLanguageModel.layerDefaultConfig({ model: Config.succeed(model) }),
          ),
        )
        const root = yield* runtime.backend.root
        const submission = yield* runtime.backend.submit({
          conversationId: root,
          draft: {
            type: 'input',
            content: 'Use countCharacters to count hello, then tell me the result.',
          },
        })
        return yield* Console.log(yield* runtime.backend.wait(submission.id))
      }
      if (!['run-codex', 'run-claude', 'recover-codex', 'recover-claude'].includes(command))
        return yield* new AuthError.AuthError({ reason: 'InvalidConfiguration' })
      const provider = command.endsWith('codex') ? 'openai-codex' : 'anthropic'
      const status = (yield* store.list).find(
        (value) => value.binding.provider === provider && value.binding.profile === 'personal',
      )
      if (status === undefined)
        return yield* new AuthError.AuthError({ reason: 'MissingCredential' })
      const definition = {
        ref: { provider, modelId: model },
        capabilities: { tools: true, images: false, reasoning: true, structuredOutput: false },
      }
      const selected =
        provider === 'openai-codex'
          ? yield* CodexModel.make({ model, binding: status.binding, definition })
          : yield* OAuthModel.make({
              model,
              binding: status.binding,
              definition,
            })
      // Reopening uses the live store: an expired credential refreshes at dispatch, not during decoding.
      const runtime = yield* HarnessRuntime.make({ models: [selected], tools })
      const root = yield* runtime.backend.root
      let id: Record.SubmissionId
      if (command.startsWith('recover-'))
        id = yield* Schema.decodeUnknownEffect(Record.SubmissionId)(process.argv[4])
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
          `Submission: ${id}. Reopen with recover-${provider === 'openai-codex' ? 'codex' : 'claude'} and this id.`,
        )
      }
      yield* runtime.backend.wait(id)
      const entries = yield* Session.scanEntries(runtime.session, { conversationId: root }).pipe(
        Stream.runCollect,
      )
      yield* Console.log(entries.filter((entry) => entry.kind === 'assistant'))
    })
    return yield* execute.pipe(
      Effect.provideService(CredentialStore.CredentialStore, store),
      Effect.provide(Storage.layerJsonl({ filePath: `${path}.conversation.jsonl` })),
    )
  }),
).pipe(
  Effect.provide(
    Layer.mergeAll(FetchHttpClient.layer, NodeServices.layer, CallbackServer.layer, handlers),
  ),
)

NodeRuntime.runMain(program)
