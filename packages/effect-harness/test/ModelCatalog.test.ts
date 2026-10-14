import { assert, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Schema from 'effect/Schema'
import * as TestClock from 'effect/testing/TestClock'
import * as Provider from '../src/Provider.js'
import * as Catalog from '../src/ModelCatalog.js'
import * as ModelInfo from '../src/ModelInfo.js'
import * as Model from '../src/Model.js'
import * as ScriptedStream from './fixtures/ScriptedStream.js'

const info: ModelInfo.ModelInfo = {
  id: 'reasoner',
  name: 'Reasoner',
  capabilities: { tools: true, images: false, reasoning: true, structuredOutput: false },
  reasoning: { effort: { values: ['low', 'high', 'custom-effort'], default: 'low' } },
}
const unused = () => Effect.die('Unexpected model construction')

it.effect(
  'construction and effect creation do no discovery; listing and direct lookup are independent',
  () =>
    Effect.gen(function* () {
      const calls: string[] = []
      const provider = Provider.make({
        id: 'first',
        models: {
          list: (input) => {
            calls.push(`list:${input.cursor}`)
            return Effect.succeed({
              items: [{ id: info.id, name: info.name }],
              nextCursor: 'page-two',
            })
          },
          get: (id) => {
            calls.push(`get:${id}`)
            return Effect.succeed(info)
          },
        },
        create: unused,
      })
      const catalog = Catalog.make({
        providers: [
          provider,
          Provider.make({
            id: 'other',
            models: {
              list: () => Effect.die('Unselected provider'),
              get: () => Effect.die('Unselected provider'),
            },
            create: unused,
          }),
        ],
      })
      const page = Catalog.list(catalog, { provider: 'first', cursor: 'page-one' })
      const details = Catalog.get(catalog, { provider: 'first', modelId: info.id })
      assert.deepEqual(calls, [])
      assert.equal((yield* details).id, info.id)
      assert.deepEqual(calls, ['get:reasoner'])
      assert.equal((yield* page).nextCursor, 'page-two')
      assert.deepEqual(calls, ['get:reasoner', 'list:page-one'])
      assert.equal(
        (yield* Effect.flip(Catalog.get(catalog, { provider: 'missing', modelId: info.id })))
          .reason,
        'providerNotFound',
      )
      const duplicates = Catalog.make({ providers: [provider, provider] })
      assert.equal(
        (yield* Effect.flip(Catalog.list(duplicates, { provider: 'first' }))).reason,
        'invalidMetadata',
      )
    }),
)

it.effect(
  'validates efforts before construction and passes independent thinking controls to the factory',
  () =>
    Effect.gen(function* () {
      const received: ModelInfo.Options[] = []
      const native = yield* ScriptedStream.make({ generateText: () => Effect.succeed([]) })
      const metadata: ModelInfo.ModelInfo = {
        ...info,
        reasoning: {
          ...info.reasoning,
          thinking: {
            modes: ['enabled', 'disabled', 'adaptive'],
            budgetTokens: { min: 1024, max: 8192 },
          },
        },
      }
      const provider = Provider.make({
        id: 'personal',
        models: Provider.fromModels([metadata]),
        create: Effect.fnUntraced(function* ({ info, options }: Provider.Resolved) {
          received.push(options)
          return Model.make({
            definition: yield* ModelInfo.toDefinition(info, 'upstream'),
            languageModel: native,
            options: Schema.Struct({}),
            configure: () => Effect.succeed(Context.empty()),
          })
        }),
      })
      const bad = yield* Effect.flip(
        Provider.resolve(provider, { modelId: info.id, options: { reasoning: { effort: 'max' } } }),
      )
      assert.equal(bad.reason, 'unsupportedOptions')
      assert.deepEqual(received, [])
      const options = {
        reasoning: { effort: 'custom-effort', thinking: { mode: 'disabled' as const } },
      }
      const resolved = yield* Catalog.resolve(Catalog.make({ providers: [provider] }), {
        provider: 'personal',
        modelId: info.id,
        options,
      })
      assert.equal(resolved.definition.ref.provider, 'upstream')
      assert.deepEqual(received, [options])
      yield* Provider.resolve(provider, { modelId: info.id })
      assert.deepEqual(received[1], {})
      for (const thinking of [
        { mode: 'enabled' as const, budgetTokens: 512 },
        { mode: 'enabled' as const },
        { mode: 'enabled' as const, budgetTokens: 8193 },
        { mode: 'adaptive' as const, budgetTokens: 2048 },
      ])
        assert.equal(
          (yield* Effect.flip(ModelInfo.validateOptions(metadata, { reasoning: { thinking } })))
            .reason,
          'unsupportedOptions',
        )
      yield* ModelInfo.validateOptions(metadata, {
        reasoning: { thinking: { mode: 'enabled', budgetTokens: 2048 } },
      })
    }),
)

it.effect(
  'rejects contradictory metadata and mismatched IDs without inventing unknown capabilities',
  () =>
    Effect.gen(function* () {
      const unknown = yield* ModelInfo.decode({ id: 'new', name: 'New' })
      assert.equal(unknown.capabilities, undefined)
      assert.equal(
        (yield* Effect.flip(ModelInfo.toDefinition(unknown, 'custom'))).reason,
        'invalidMetadata',
      )
      for (const reasoning of [
        { effort: { values: ['low'], default: 'high' } },
        { effort: { values: ['low', 'low'] } },
        { thinking: { modes: ['enabled'], budgetTokens: { min: 8192, max: 1024 } } },
      ])
        assert.equal(
          (yield* Effect.flip(ModelInfo.decode({ ...info, reasoning }))).reason,
          'invalidMetadata',
        )
      const provider = Provider.make({
        id: 'custom',
        models: { list: () => Effect.succeed({ items: [] }), get: () => Effect.succeed(info) },
        create: unused,
      })
      assert.equal(
        (yield* Effect.flip(Provider.get(provider, 'different'))).reason,
        'invalidMetadata',
      )
    }),
)

it.effect('local catalogs paginate summaries and support direct lookup and search', () =>
  Effect.gen(function* () {
    const provider = Provider.make({
      id: 'local',
      models: Provider.fromModels([
        info,
        { ...info, id: 'second', name: 'Second' },
        { ...info, id: 'third', name: 'Third' },
      ]),
      create: unused,
    })
    const first = yield* Provider.list(provider, { limit: 2 })
    assert.deepEqual(first, {
      items: [
        { id: 'reasoner', name: 'Reasoner' },
        { id: 'second', name: 'Second' },
      ],
      nextCursor: '2',
    })
    assert.deepEqual(yield* Provider.list(provider, { cursor: '2', limit: 2 }), {
      items: [{ id: 'third', name: 'Third' }],
    })
    assert.deepEqual((yield* Provider.list(provider, { query: 'SECOND' })).items, [
      { id: 'second', name: 'Second' },
    ])
    assert.equal((yield* Effect.flip(Provider.get(provider, 'missing'))).reason, 'modelNotFound')
    assert.equal(
      (yield* Effect.flip(Provider.list(provider, { cursor: '-1' }))).reason,
      'unsupportedOptions',
    )
  }),
)

class Account extends Context.Service<Account, string>()('catalog/test/Account') {}

it.effect(
  'caches lazily, shares concurrent lookups, expires entries, and isolates captured account services',
  () =>
    Effect.gen(function* () {
      const calls: string[] = []
      const provider = Provider.make({
        id: 'remote',
        models: {
          list: () =>
            Effect.sync(() => {
              calls.push('list')
              return { items: [] }
            }),
          get: Effect.fnUntraced(function* (id: string) {
            const account = yield* Account
            calls.push(account)
            yield* Effect.yieldNow
            return { ...info, id, name: account }
          }),
        },
        create: unused,
      })
      const a = yield* Provider.cached(provider, { capacity: 2, timeToLive: '1 minute' }).pipe(
        Effect.provideService(Account, 'A'),
      )
      const b = yield* Provider.cached(provider, { capacity: 2, timeToLive: '1 minute' }).pipe(
        Effect.provideService(Account, 'B'),
      )
      assert.deepEqual(calls, [])
      const results = yield* Effect.all(
        [Provider.get(a, 'reasoner'), Provider.get(a, 'reasoner')],
        { concurrency: 'unbounded' },
      )
      assert.deepEqual(
        results.map((result) => result.name),
        ['A', 'A'],
      )
      assert.deepEqual(calls, ['A'])
      assert.equal(
        (yield* Provider.get(a, 'reasoner').pipe(Effect.provideService(Account, 'B'))).name,
        'A',
      )
      assert.equal((yield* Provider.get(b, 'reasoner')).name, 'B')
      yield* Provider.list(a, { query: 'same', limit: 10 })
      yield* Provider.list(a, { limit: 10, query: 'same' })
      assert.deepEqual(calls, ['A', 'B', 'list'])
      yield* TestClock.adjust('1 minute')
      yield* Provider.get(a, 'reasoner')
      yield* a.invalidate
      yield* Provider.get(a, 'reasoner')
      assert.deepEqual(calls, ['A', 'B', 'list', 'A', 'A'])
    }),
)

it.effect('source errors retain identity and failed cache lookups can recover immediately', () =>
  Effect.gen(function* () {
    const failure = { _tag: 'RemoteUnavailable' as const }
    let unavailable = true
    const provider = Provider.make({
      id: 'remote',
      models: {
        list: () => Effect.succeed({ items: [] }),
        get: () => (unavailable ? Effect.fail(failure) : Effect.succeed(info)),
      },
      create: unused,
    })
    const cached = yield* Provider.cached(provider, { capacity: 2, timeToLive: '1 minute' })
    assert.strictEqual(yield* Effect.flip(Provider.get(cached, info.id)), failure)
    unavailable = false
    assert.equal((yield* Provider.get(cached, info.id)).id, info.id)
  }),
)

it.effect('bounds the cache and retries interrupted discovery without poisoning the entry', () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>()
    let block = true
    const calls: string[] = []
    const provider = Provider.make({
      id: 'remote',
      models: {
        list: () => Effect.succeed({ items: [] }),
        get: Effect.fnUntraced(function* (id: string) {
          calls.push(id)
          if (block) {
            yield* Deferred.succeed(entered, undefined)
            return yield* Effect.never
          }
          return { ...info, id }
        }),
      },
      create: unused,
    })
    const cached = yield* Provider.cached(provider, { capacity: 1, timeToLive: '1 minute' })
    const lookup = yield* Provider.get(cached, 'first').pipe(
      Effect.forkChild({ startImmediately: true }),
    )
    yield* Deferred.await(entered)
    yield* Fiber.interrupt(lookup)
    block = false
    yield* Provider.get(cached, 'first')
    yield* Provider.get(cached, 'second')
    yield* Provider.get(cached, 'first')
    assert.deepEqual(calls, ['first', 'first', 'second', 'first'])
  }),
)
