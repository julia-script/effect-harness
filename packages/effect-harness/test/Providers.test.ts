import { assert, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Fiber from 'effect/Fiber'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as Provider from '../src/Provider.js'
import * as Providers from '../src/Providers.js'
import * as ProviderStore from '../src/ProviderStore.js'
import { ProviderError } from '../src/ProviderError.js'
import type * as ModelCatalog from '../src/ModelCatalog.js'
import * as Model from '../src/Model.js'
import * as ModelInfo from '../src/ModelInfo.js'
import * as HarnessRuntime from '../src/HarnessRuntime.js'
import * as Storage from '../src/Storage.js'
import * as CredentialStore from '../src/provider-auth/CredentialStore.js'
import * as MemoryCredentials from '../src/provider-auth/MemoryCredentialStore.js'
import * as Scripted from './fixtures/ScriptedStream.js'

const metadata: ModelInfo.ModelInfo = {
  id: 'shared-model',
  name: 'Shared',
  capabilities: { tools: true, images: false, reasoning: true, structuredOutput: false },
  reasoning: {
    effort: { values: ['low', 'high'] },
    thinking: { modes: ['enabled'], budgetTokens: { min: 1024, max: 8192 } },
  },
}
const Config = Schema.Struct({
  endpoint: Schema.NonEmptyString,
  attempts: Schema.NumberFromString.check(Schema.isFinite()),
})
const Secrets = Schema.Struct({ token: Schema.Redacted(Schema.NonEmptyString) })
const Settings = Schema.Struct({ temperature: Schema.Finite })
const define = (calls: string[], constructed: ModelInfo.Options[] = []) =>
  Provider.define({
    version: 1,
    config: Config,
    secrets: Secrets,
    models: {
      list: (
        _: Provider.Context<typeof Config.Type, typeof Secrets.Type>,
        input: ModelCatalog.ListOptions,
      ) =>
        Effect.sync(() => {
          calls.push(`list:${input.cursor}`)
          return { items: [{ id: metadata.id, name: metadata.name }] }
        }),
      get: (_: Provider.Context<typeof Config.Type, typeof Secrets.Type>, id: string) =>
        Effect.sync(() => {
          calls.push(`get:${id}`)
          return metadata
        }),
    },
    createModel: Effect.fnUntraced(function* (
      context: Provider.Context<typeof Config.Type, typeof Secrets.Type>,
      resolved: Provider.Resolved,
    ) {
      constructed.push(resolved.options)
      const native = yield* Scripted.make({
        generateText: () =>
          Effect.sync(() => {
            calls.push(context.config.endpoint)
            return []
          }),
      })
      return Model.make({
        definition: yield* ModelInfo.toDefinition(resolved.info, 'test'),
        languageModel: native,
        options: Settings,
        configure: () => Effect.succeed(Context.empty()),
      })
    }),
  })
const setup = Effect.fnUntraced(function* (
  calls: string[] = [],
  constructed: ModelInfo.Options[] = [],
) {
  const publicStore = yield* ProviderStore.makeMemory
  const credentials = yield* MemoryCredentials.make
  const definitions = { test: define(calls, constructed) }
  const open = Providers.make({ definitions }).pipe(
    Effect.provideService(ProviderStore.ProviderStore, publicStore),
    Effect.provideService(CredentialStore.CredentialStore, credentials),
  )
  return { manager: yield* open, open, publicStore, credentials, definitions }
})
const input = {
  provider: 'test',
  label: 'User label',
  config: { endpoint: 'a', attempts: 2 },
} as const

it.effect(
  'adds unauthenticated instances, encodes configuration, and persists only credential references',
  () =>
    Effect.gen(function* () {
      const { manager, open, publicStore, credentials } = yield* setup()
      const first = yield* manager.add(input)
      const second = yield* manager.add({
        ...input,
        secrets: { token: Redacted.make('SECRET-token') },
      })
      assert.notEqual(first.id, second.id)
      assert.isUndefined(first.credential)
      const stored = yield* publicStore.read(second.id)
      assert.deepEqual(stored?.config, { endpoint: 'a', attempts: '2' })
      assert.notInclude(JSON.stringify(yield* publicStore.list), 'SECRET')
      assert.isDefined(second.credential)
      assert.equal((yield* credentials.list).length, 1)
      const reopened = yield* open
      assert.equal((yield* reopened.get(second.id)).config.attempts, 2)
      const updated = yield* reopened.update(first.id, {
        secrets: { token: Redacted.make('SECRET-new') },
      })
      assert.isDefined(updated.credential)
      assert.notEqual(updated.revision, first.revision)
      const renamed = yield* reopened.update(first.id, { label: 'Renamed' })
      assert.equal(renamed.revision, updated.revision)
      assert.deepEqual(renamed.credential, updated.credential)
      yield* reopened.update(first.id, { secrets: null })
      assert.isUndefined((yield* reopened.get(first.id)).credential)
      assert.equal((yield* credentials.list).length, 1)
      yield* reopened.remove(second.id)
      assert.equal((yield* credentials.list).length, 0)
    }),
)

it.effect(
  'validates config and secrets before publication, and validates stored values on reopen',
  () =>
    Effect.gen(function* () {
      const { manager, publicStore, credentials } = yield* setup()
      for (const value of [
        { ...input, config: { endpoint: '', attempts: 2 } },
        { ...input, secrets: { token: Redacted.make('') } },
      ])
        assert.equal((yield* Effect.flip(manager.add(value))).reason, 'InvalidConfiguration')
      assert.deepEqual(yield* manager.list, [])
      assert.deepEqual(yield* credentials.list, [])
      const instance = yield* manager.add(input)
      yield* publicStore.modify(instance.id, (current) =>
        Effect.succeed([
          undefined,
          { ...current!, config: { endpoint: 'a', attempts: 'invalid' } },
        ] as const),
      )
      assert.equal((yield* Effect.flip(manager.get(instance.id))).reason, 'InvalidConfiguration')
      yield* publicStore.modify(instance.id, (current) =>
        Effect.succeed([undefined, { ...current!, version: 2 }] as const),
      )
      assert.equal((yield* Effect.flip(manager.get(instance.id))).reason, 'UnsupportedVersion')
    }),
)

it.effect(
  'list/get are independent and selected model controls are validated before construction',
  () =>
    Effect.gen(function* () {
      const calls: string[] = []
      const constructed: ModelInfo.Options[] = []
      const { manager } = yield* setup(calls, constructed)
      const instance = yield* manager.add(input)
      const listing = manager.models.list({ instanceId: instance.id, cursor: 'next' })
      const get = manager.models.get({ instanceId: instance.id, modelId: metadata.id })
      assert.deepEqual(calls, [])
      yield* get
      assert.deepEqual(calls, [`get:${metadata.id}`])
      yield* listing
      assert.deepEqual(calls, [`get:${metadata.id}`, 'list:next'])
      assert.equal(
        (yield* Effect.flip(manager.models.get({ instanceId: instance.id, modelId: 'wrong' })))
          .reason,
        'invalidMetadata',
      )
      for (const options of [
        { reasoning: { effort: 'unsupported' } },
        { reasoning: { thinking: { mode: 'enabled' as const, budgetTokens: 100 } } },
      ])
        assert.equal(
          (yield* Effect.flip(
            manager.models.resolve({ instanceId: instance.id, modelId: metadata.id, options }),
          )).reason,
          'unsupportedOptions',
        )
      assert.deepEqual(constructed, [])
      const options = {
        reasoning: { effort: 'high', thinking: { mode: 'enabled' as const, budgetTokens: 2048 } },
      }
      yield* manager.models.resolve({ instanceId: instance.id, modelId: metadata.id, options })
      assert.deepEqual(constructed, [options])
    }),
)

it.effect(
  'same-model accounts select independently within one runtime and stale handles cannot dispatch',
  () =>
    Effect.gen(function* () {
      const calls: string[] = []
      const { manager } = yield* setup(calls)
      const a = yield* manager.add(input)
      const b = yield* manager.add({ ...input, config: { endpoint: 'b', attempts: 2 } })
      const one = yield* manager.models.resolve({ instanceId: a.id, modelId: metadata.id })
      const two = yield* manager.models.resolve({ instanceId: b.id, modelId: metadata.id })
      assert.notDeepEqual(one.definition.ref, two.definition.ref)
      const storage = yield* Layer.build(Storage.layerMemory)
      const runtime = yield* HarnessRuntime.make({ models: [one, two] }).pipe(
        Effect.provideContext(storage),
      )
      const root = yield* runtime.backend.root
      for (const model of [two, one]) {
        yield* runtime.backend.configure({
          conversationId: root,
          change: { model: { ref: model.definition.ref, options: { temperature: 0.2 } } },
        })
        const handle = yield* runtime.backend.submit({
          conversationId: root,
          draft: { type: 'input', content: 'hello' },
        })
        yield* runtime.backend.wait(handle.id)
      }
      assert.deepEqual(
        calls.filter((value) => value === 'a' || value === 'b'),
        ['b', 'a'],
      )
      const snapshot = yield* one.configuration!.encode(Context.empty())
      yield* manager.update(a.id, { config: { endpoint: 'changed', attempts: 2 } })
      const reopened = yield* manager.models.resolve({ instanceId: a.id, modelId: metadata.id })
      assert.equal(
        (yield* Effect.flip(reopened.configuration!.restore(snapshot))).reason._tag,
        'ModelUnsupportedError',
      )
      assert.equal(
        (yield* Effect.flip(one.languageModel.generateText({ prompt: 'stale' }))).reason._tag,
        'AuthenticationError',
      )
      yield* manager.remove(b.id)
      assert.equal(
        (yield* Effect.flip(two.languageModel.generateText({ prompt: 'removed' }))).reason._tag,
        'AuthenticationError',
      )
      assert.notInclude(calls, 'changed')
    }),
)

it.effect('replaces credentials under a new reference and rejects concurrent stale updates', () =>
  Effect.gen(function* () {
    const { manager, credentials } = yield* setup()
    const first = yield* manager.add({ ...input, secrets: { token: Redacted.make('one') } })
    const second = yield* manager.update(first.id, {
      secrets: { token: Redacted.make('two') },
      expectedRevision: first.revision,
    })
    assert.notEqual(second.credential?.credentialId, first.credential?.credentialId)
    assert.notEqual(second.credential?.identity, first.credential?.identity)
    assert.isUndefined(yield* credentials.read(first.credential!.credentialId))
    assert.equal(
      (yield* Effect.flip(
        manager.update(first.id, { config: input.config, expectedRevision: first.revision }),
      )).reason,
      'Conflict',
    )
  }),
)

for (const uncertain of [false, true]) {
  it.effect(
    `public commit ${uncertain ? 'uncertainty retains' : 'failure cleans up'} newly allocated credentials`,
    () =>
      Effect.gen(function* () {
        const { publicStore, credentials, definitions } = yield* setup()
        const failing = ProviderStore.ProviderStore.of({
          ...publicStore,
          modify: (id, f) =>
            uncertain
              ? Effect.andThen(
                  publicStore.modify(id, f),
                  Effect.fail(new ProviderError({ reason: 'StoreCommitUncertain' })),
                )
              : publicStore.modify(id, (current) =>
                  Effect.andThen(
                    f(current),
                    Effect.fail(new ProviderError({ reason: 'StoreFailure' })),
                  ),
                ),
        })
        const manager = yield* Providers.make({ definitions }).pipe(
          Effect.provideService(ProviderStore.ProviderStore, failing),
          Effect.provideService(CredentialStore.CredentialStore, credentials),
        )
        assert.equal(
          (yield* Effect.flip(
            manager.add({ ...input, secrets: { token: Redacted.make('SECRET') } }),
          )).reason,
          uncertain ? 'StoreCommitUncertain' : 'StoreFailure',
        )
        assert.equal((yield* credentials.list).length, uncertain ? 1 : 0)
        assert.equal((yield* publicStore.list).length, uncertain ? 1 : 0)
      }),
  )
}

for (const replacing of [false, true])
  it.effect(
    `interrupted ${replacing ? 'replacement retires old credentials' : 'addition publishes credentials'} before releasing the transaction`,
    () =>
      Effect.gen(function* () {
        const { publicStore, credentials, definitions, manager: publicManager } = yield* setup()
        const original = replacing
          ? yield* publicManager.add({ ...input, secrets: { token: Redacted.make('OLD') } })
          : undefined
        const entered = yield* Deferred.make<void>()
        const finish = yield* Deferred.make<void>()
        const delayed = CredentialStore.CredentialStore.of({
          ...credentials,
          modify: (key, f) =>
            credentials.modify(key, (current) =>
              Effect.gen(function* () {
                const next = yield* f(current)
                yield* Deferred.succeed(entered, undefined)
                yield* Deferred.await(finish)
                return next
              }),
            ),
        })
        const manager = yield* Providers.make({ definitions }).pipe(
          Effect.provideService(ProviderStore.ProviderStore, publicStore),
          Effect.provideService(CredentialStore.CredentialStore, delayed),
        )
        const operation = yield* Effect.forkChild(
          original === undefined
            ? manager.add({ ...input, secrets: { token: Redacted.make('SECRET') } })
            : manager.update(original.id, { secrets: { token: Redacted.make('NEW') } }),
        )
        yield* Deferred.await(entered)
        const interrupt = yield* Effect.forkChild(Fiber.interrupt(operation))
        yield* Deferred.succeed(finish, undefined)
        yield* Fiber.join(interrupt)
        const saved = yield* manager.list
        assert.equal(saved.length, 1)
        assert.equal((yield* credentials.list).length, 1)
        if (original?.credential !== undefined)
          assert.isUndefined(yield* credentials.read(original.credential.credentialId))
        assert.isDefined(yield* credentials.read(saved[0]!.credential!.credentialId))
      }),
  )
