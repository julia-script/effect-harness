import { encodeCredential } from './fixtures/Credentials.js'
import * as RuntimeRecords from './fixtures/RuntimeRecords.js'
import { assert, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Redacted from 'effect/Redacted'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as HarnessRuntime from '../src/HarnessRuntime.js'
import * as Model from '../src/Model.js'
import { ModelError, ModelUnsupportedError } from '../src/ModelError.js'
import * as Prepared from '../src/PreparedRequest.js'
import * as Storage from '../src/Storage.js'
import * as Session from '../src/Session.js'
import * as Record from '../src/Record.js'
import * as Store from '../src/provider-auth/CredentialStore.js'
import * as FileSystemCredentialStore from '../src/provider-auth-node/FileSystemCredentialStore.js'
import * as Codex from '../src/provider-openai-codex/Model.js'
import * as Claude from '../src/provider-anthropic/OAuthModel.js'
import * as ClaudeLanguageModel from '../src/provider-anthropic/OAuthLanguageModel.js'
import { credentials as memoryCredentials } from './fixtures/SubscriptionProtocol.js'
import * as CodexLanguageModel from '../src/provider-openai-codex/CodexLanguageModel.js'
import { binding, codexEvents, sse, payload } from './fixtures/SubscriptionProtocol.js'

it.live(
  'cold reopen restores prepared settings, refreshes expired credentials, and preserves affinity/usage without secrets',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const directory = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped()
        const authPath = `${directory}/auth.json`
        const credentials = yield* FileSystemCredentialStore.make({ path: authPath })
        const accessToken = `header.${btoa(JSON.stringify({ 'https://api.openai.com/auth': { chatgpt_account_id: 'account-1' } }))}.SECRET-token`
        yield* Store.save(
          encodeCredential({
            type: 'oauth',
            binding,
            accessToken: Redacted.make(accessToken),
            refreshToken: Redacted.make('SECRET-refresh'),
            accountId: 'account-1',
            expiresAt: 1,
          }),
        ).pipe(Effect.provideService(Store.CredentialStore, credentials))
        const storageLayer = Storage.layerJsonl({ filePath: `${directory}/session.jsonl` })
        const storage = yield* Layer.build(Layer.fresh(storageLayer))
        const root = Record.ROOT_CONVERSATION_ID
        let refreshes = 0
        let generations = 0
        let prepared: Prepared.Snapshot | undefined
        const http = HttpClient.make((request) =>
          Effect.gen(function* () {
            if (request.url.endsWith('/oauth/token')) {
              refreshes++
              return HttpClientResponse.fromWeb(
                request,
                new Response(
                  JSON.stringify({
                    access_token: accessToken,
                    refresh_token: 'SECRET-rotated',
                    expires_in: 3600,
                  }),
                ),
              )
            }
            generations++
            const body = payload(request)
            assert.deepEqual(body.text, { verbosity: 'high' })
            assert.strictEqual(body.prompt_cache_key, 'accepted-key')
            assert.strictEqual(request.headers['session-id'], 'accepted-key')
            prepared = Option.getOrThrow(
              yield* Effect.serviceOption(Prepared.PreparedRequest),
            ).current
            return HttpClientResponse.fromWeb(request, new Response(sse(codexEvents)))
          }),
        )
        const definition = {
          ref: { provider: 'openai-codex', modelId: 'gpt-5.4' },
          capabilities: { tools: true, images: false, reasoning: true, structuredOutput: false },
        }
        const firstModel = yield* Codex.make({
          model: 'gpt-5.4',
          binding,
          definition,
          config: { verbosity: 'high', cacheKey: 'accepted-key' },
        }).pipe(
          Effect.provideService(Store.CredentialStore, credentials),
          Effect.provideService(HttpClient.HttpClient, http),
        )
        const gate = Model.make({
          ...firstModel,
          configuration: {
            ...firstModel.configuration!,
            restore: () =>
              Effect.fail(
                new ModelError({
                  reason: new ModelUnsupportedError({ message: 'restart before dispatch' }),
                }),
              ),
          },
        })
        const firstScope = yield* Scope.make()
        yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
        const first = yield* HarnessRuntime.make({ models: [gate] }).pipe(
          Effect.provideContext(storage),
          Effect.provideService(Scope.Scope, firstScope),
        )
        yield* first.backend.root
        const job = yield* first.backend.submit({
          conversationId: root,
          draft: { type: 'input', content: 'hello' },
        })
        assert.isTrue(Exit.isFailure(yield* Effect.exit(first.backend.wait(job.id))))
        const tasks = yield* Context.get(storage, Storage.Storage)
          .scanTasks({ conversationId: root, kind: 'harness.run' })
          .pipe(Stream.runCollect)
        const checkpoint = yield* RuntimeRecords.preparation(
          Context.get(storage, Storage.Storage),
          tasks[0]!.state.checkpoint,
        )
        assert.notInclude(JSON.stringify(checkpoint), 'SECRET')
        assert.strictEqual(generations, 0)
        assert.strictEqual(refreshes, 0)
        yield* Scope.close(firstScope, Exit.void)
        const reopenedStore = yield* FileSystemCredentialStore.make({ path: authPath })
        const restoredModel = yield* Codex.make({
          model: 'gpt-5.4',
          binding,
          definition,
          config: { verbosity: 'low', cache: 'none' },
        }).pipe(
          Effect.provideService(Store.CredentialStore, reopenedStore),
          Effect.provideService(HttpClient.HttpClient, http),
        )
        const secondStorage = yield* Layer.build(Layer.fresh(storageLayer))
        const second = yield* HarnessRuntime.make({ models: [restoredModel] }).pipe(
          Effect.provideContext(secondStorage),
        )
        assert.strictEqual((yield* second.backend.wait(job.id)).status, 'done')
        assert.strictEqual(refreshes, 1)
        assert.strictEqual(generations, 1)
        assert.deepEqual(prepared, checkpoint)
        const entries = yield* Session.scanEntries(second.session, { conversationId: root }).pipe(
          Stream.runCollect,
        )
        assert.notInclude(JSON.stringify(entries), 'SECRET')
        assert.include(JSON.stringify(entries), 'Olá')
        const mismatch = yield* Codex.make({
          model: 'gpt-5.4',
          binding: { ...binding, identity: 'another-login' },
          definition,
        }).pipe(
          Effect.provideService(Store.CredentialStore, reopenedStore),
          Effect.provideService(HttpClient.HttpClient, http),
        )
        const encoded = yield* firstModel.configuration!.encode(
          Context.make(CodexLanguageModel.Config, { verbosity: 'high' }),
        )
        const failure = yield* Effect.flip(mismatch.configuration!.restore(encoded))
        assert.strictEqual(failure.reason._tag, 'ModelUnsupportedError')
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
)

it.effect('Claude recovery accepts an equivalent API version and rejects protocol drift', () =>
  Effect.gen(function* () {
    const store = yield* memoryCredentials('anthropic')
    const make = (apiVersion?: string) =>
      Claude.make({
        model: 'claude-sonnet-4-6',
        binding: binding,
        ...(apiVersion === undefined ? {} : { apiVersion }),
        definition: {
          ref: { provider: 'anthropic', modelId: 'claude-sonnet-4-6' },
          capabilities: { tools: true, images: false, reasoning: true, structuredOutput: false },
        },
      }).pipe(
        Effect.provideService(Store.CredentialStore, store),
        Effect.provideService(
          HttpClient.HttpClient,
          HttpClient.make(() => Effect.die('Recovery must fail before dispatch')),
        ),
      )
    const before = yield* make()
    const equivalent = yield* make('2023-06-01')
    const changed = yield* make('2099-01-01')
    const snapshot = JSON.parse(
      JSON.stringify(
        yield* before.configuration!.encode(
          Context.make(ClaudeLanguageModel.Config, { maxTokens: 1234 }),
        ),
      ),
    )
    const restored = yield* equivalent.configuration!.restore(snapshot)
    assert.strictEqual(
      Context.getOrUndefined(restored, ClaudeLanguageModel.Config)?.maxTokens,
      1234,
    )
    assert.deepEqual(before.configuration!.binding, equivalent.configuration!.binding)
    const error = yield* Effect.flip(changed.configuration!.restore(snapshot))
    assert.strictEqual(error.reason._tag, 'ModelUnsupportedError')
    assert.notDeepEqual(before.configuration!.binding, changed.configuration!.binding)
  }),
)
