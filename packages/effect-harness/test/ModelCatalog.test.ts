import { assert, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as TestClock from 'effect/testing/TestClock'
import * as Catalog from '../src/ModelCatalog.js'
import * as ModelInfo from '../src/ModelInfo.js'

const info: ModelInfo.ModelInfo = {
  id: 'reasoner',
  name: 'Reasoner',
  capabilities: { tools: true, images: false, reasoning: true, structuredOutput: false },
  reasoning: { effort: { values: ['low', 'high', 'custom-effort'], default: 'low' } },
}

it.effect('accepts enabled thinking without a budget when the model declares only a toggle', () =>
  Effect.gen(function* () {
    const toggle = yield* ModelInfo.decode({
      ...info,
      reasoning: { thinking: { modes: ['enabled', 'disabled'] } },
    })
    assert.deepEqual(
      yield* ModelInfo.validateOptions(toggle, { reasoning: { thinking: { mode: 'enabled' } } }),
      { reasoning: { thinking: { mode: 'enabled' } } },
    )
    assert.equal(
      (yield* Effect.flip(
        ModelInfo.validateOptions(toggle, {
          reasoning: { thinking: { mode: 'enabled', budgetTokens: 2048 } },
        }),
      )).reason,
      'unsupportedOptions',
    )
  }),
)

it.effect('rejects contradictory metadata without inventing unknown capabilities', () =>
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
  }),
)

it.effect('local catalogs paginate summaries and support direct lookup and search', () =>
  Effect.gen(function* () {
    const provider = Catalog.fromModels([
      info,
      { ...info, id: 'second', name: 'Second' },
      { ...info, id: 'third', name: 'Third' },
    ])
    const first = yield* provider.list({ limit: 2 })
    assert.deepEqual(first, {
      items: [
        { id: 'reasoner', name: 'Reasoner' },
        { id: 'second', name: 'Second' },
      ],
      nextCursor: '2',
    })
    assert.deepEqual(yield* provider.list({ cursor: '2', limit: 2 }), {
      items: [{ id: 'third', name: 'Third' }],
    })
    assert.deepEqual((yield* provider.list({ query: 'SECOND' })).items, [
      { id: 'second', name: 'Second' },
    ])
    assert.equal((yield* Effect.flip(provider.get('missing'))).reason, 'modelNotFound')
    assert.equal((yield* Effect.flip(provider.list({ cursor: '-1' }))).reason, 'unsupportedOptions')
  }),
)

class Account extends Context.Service<Account, string>()('catalog/test/Account') {}

it.effect(
  'caches lazily, shares concurrent lookups, expires entries, and isolates captured account services',
  () =>
    Effect.gen(function* () {
      const calls: string[] = []
      const provider = {
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
      }
      const a = yield* Catalog.cached(provider, { capacity: 2, timeToLive: '1 minute' }).pipe(
        Effect.provideService(Account, 'A'),
      )
      const b = yield* Catalog.cached(provider, { capacity: 2, timeToLive: '1 minute' }).pipe(
        Effect.provideService(Account, 'B'),
      )
      assert.deepEqual(calls, [])
      const results = yield* Effect.all([a.get('reasoner'), a.get('reasoner')], {
        concurrency: 'unbounded',
      })
      assert.deepEqual(
        results.map((result) => result.name),
        ['A', 'A'],
      )
      assert.deepEqual(calls, ['A'])
      assert.equal((yield* a.get('reasoner').pipe(Effect.provideService(Account, 'B'))).name, 'A')
      assert.equal((yield* b.get('reasoner')).name, 'B')
      yield* a.list({ query: 'same', limit: 10 })
      yield* a.list({ limit: 10, query: 'same' })
      assert.deepEqual(calls, ['A', 'B', 'list'])
      yield* TestClock.adjust('1 minute')
      yield* a.get('reasoner')
      yield* a.invalidate
      yield* a.get('reasoner')
      assert.deepEqual(calls, ['A', 'B', 'list', 'A', 'A'])
    }),
)

it.effect('source errors retain identity and failed cache lookups can recover immediately', () =>
  Effect.gen(function* () {
    const failure = { _tag: 'RemoteUnavailable' as const }
    let unavailable = true
    const provider = {
      list: () => Effect.succeed({ items: [] }),
      get: () => (unavailable ? Effect.fail(failure) : Effect.succeed(info)),
    }
    const cached = yield* Catalog.cached(provider, { capacity: 2, timeToLive: '1 minute' })
    assert.strictEqual(yield* Effect.flip(cached.get(info.id)), failure)
    unavailable = false
    assert.equal((yield* cached.get(info.id)).id, info.id)
  }),
)

it.effect('bounds the cache and retries interrupted discovery without poisoning the entry', () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>()
    let block = true
    const calls: string[] = []
    const provider = {
      list: () => Effect.succeed({ items: [] }),
      get: Effect.fnUntraced(function* (id: string) {
        calls.push(id)
        if (block) {
          yield* Deferred.succeed(entered, undefined)
          return yield* Effect.never
        }
        return { ...info, id }
      }),
    }
    const cached = yield* Catalog.cached(provider, { capacity: 1, timeToLive: '1 minute' })
    const lookup = yield* cached.get('first').pipe(Effect.forkChild({ startImmediately: true }))
    yield* Deferred.await(entered)
    yield* Fiber.interrupt(lookup)
    block = false
    yield* cached.get('first')
    yield* cached.get('second')
    yield* cached.get('first')
    assert.deepEqual(calls, ['first', 'first', 'second', 'first'])
  }),
)
