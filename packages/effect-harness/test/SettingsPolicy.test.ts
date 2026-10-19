import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Settings from '../src/Settings.js'
import { SettingsError } from '../src/SettingsError.js'

describe('validated live host policies', () => {
  it.effect('normalizes audited defaults and uses them when the optional service is absent', () =>
    Effect.gen(function* () {
      const service = yield* Settings.make()
      const snapshot = yield* service.get
      assert.deepStrictEqual(snapshot, Settings.defaults)
      assert.strictEqual(snapshot.revision, 0)
      assert.deepStrictEqual(snapshot.retry, {
        enabled: true,
        maxRetries: 3,
        baseDelayMs: 2000,
        maxAgentDelayMs: 60000,
      })
      assert.deepStrictEqual(snapshot.compaction, {
        enabled: true,
        reserveTokens: 16384,
        keepRecentTokens: 20000,
        backgroundTokens: 32768,
      })
      assert.deepStrictEqual(snapshot.progress, { partialIntervalMs: 100, outputIntervalMs: 100 })
      assert.strictEqual(snapshot.contextRetentionMs, 600000)
      assert.strictEqual(snapshot.maxTurns, 100)
      assert.strictEqual(snapshot.toolExecution, 'parallel')
      assert.strictEqual(snapshot.steeringMode, 'one-at-a-time')
      assert.strictEqual(snapshot.followUpMode, 'one-at-a-time')
      assert.isUndefined(snapshot.extensions)
      assert.deepStrictEqual(snapshot.stream, {})
      assert.strictEqual(yield* Settings.getOrDefault, Settings.defaults)
      assert.isTrue(Schema.is(Settings.SnapshotSchema)(snapshot))
    }),
  )

  it.effect('undefined preserves defaults on construction and current fields on updates', () =>
    Effect.gen(function* () {
      const service = yield* Settings.make({
        retry: { maxRetries: undefined },
        progress: { partialIntervalMs: undefined },
        compaction: { enabled: undefined },
        stream: { timeoutMs: undefined },
        toolExecution: undefined,
        contextRetentionMs: undefined,
        maxTurns: undefined,
      })
      assert.deepStrictEqual(yield* service.get, Settings.defaults)
      const first = yield* service.update({
        retry: { enabled: false, baseDelayMs: 17 },
        progress: { outputIntervalMs: 40 },
        compaction: { keepRecentTokens: 500 },
        contextRetentionMs: 0,
      })
      const second = yield* service.update({
        retry: { enabled: undefined, baseDelayMs: undefined, maxRetries: 0 },
        progress: { outputIntervalMs: undefined, partialIntervalMs: 0 },
        compaction: { keepRecentTokens: undefined, backgroundTokens: 0 },
        contextRetentionMs: undefined,
      })
      assert.strictEqual(first.revision, 1)
      assert.strictEqual(second.revision, 2)
      assert.deepStrictEqual(second.retry, {
        enabled: false,
        baseDelayMs: 17,
        maxRetries: 0,
        maxAgentDelayMs: 60000,
      })
      assert.deepStrictEqual(second.progress, { partialIntervalMs: 0, outputIntervalMs: 40 })
      assert.strictEqual(second.compaction.keepRecentTokens, 500)
      assert.strictEqual(second.compaction.backgroundTokens, 0)
      assert.strictEqual(second.contextRetentionMs, 0)
      const third = yield* service.update({})
      assert.strictEqual(third.revision, 3)
    }),
  )

  it.effect('replaces stream bundles and explicitly clears extension selection', () =>
    Effect.gen(function* () {
      const service = yield* Settings.make({
        extensions: ['coding'],
        stream: {
          transport: 'auto',
          timeoutMs: 0,
          maxRetries: 0,
          maxRetryDelayMs: 0,
          cacheRetention: 'long',
          deferred: { window: '24h' },
          headers: { 'x-trace': 'first' },
          metadata: { nested: ['one', { count: 1 }] },
        },
      })
      const before = yield* service.get
      assert.deepStrictEqual(before.extensions, ['coding'])
      const next = yield* service.update({ stream: { cacheRetention: 'none' }, extensions: [] })
      assert.deepStrictEqual(next.stream, { cacheRetention: 'none' })
      assert.deepStrictEqual(next.extensions, [])
      const restored = yield* service.update({ stream: null, extensions: null })
      assert.deepStrictEqual(restored.stream, {})
      assert.isFalse(Object.hasOwn(restored, 'extensions'))
      assert.deepStrictEqual(before.stream.headers, { 'x-trace': 'first' })
      const empty = yield* service.update({ stream: {} })
      assert.deepStrictEqual(empty.stream, {})
      const deferred = yield* service.update({ stream: { deferred: { window: undefined } } })
      assert.deepStrictEqual(deferred.stream, { deferred: {} })
    }),
  )

  const invalidChanges: ReadonlyArray<readonly [string, Settings.Change]> = [
    ['negative retry count', { retry: { maxRetries: -1 } }],
    ['fractional retry count', { retry: { maxRetries: 0.5 } }],
    ['unsafe retry count', { retry: { maxRetries: Number.MAX_SAFE_INTEGER + 1 } }],
    ['negative base delay', { retry: { baseDelayMs: -1 } }],
    ['nonfinite cap', { retry: { maxAgentDelayMs: Infinity } }],
    ['NaN cap', { retry: { maxAgentDelayMs: NaN } }],
    ['negative compaction reserve', { compaction: { reserveTokens: -1 } }],
    ['fractional compaction keep', { compaction: { keepRecentTokens: 0.1 } }],
    ['nonfinite background tokens', { compaction: { backgroundTokens: Infinity } }],
    ['negative partial interval', { progress: { partialIntervalMs: -1 } }],
    ['NaN output interval', { progress: { outputIntervalMs: NaN } }],
    ['nonfinite partial interval', { progress: { partialIntervalMs: -Infinity } }],
    ['negative retention', { contextRetentionMs: -1 }],
    ['nonfinite retention', { contextRetentionMs: Infinity }],
    ['zero turns', { maxTurns: 0 }],
    ['fractional turns', { maxTurns: 2.5 }],
    ['negative stream timeout', { stream: { timeoutMs: -1 } }],
    ['nonfinite stream retry delay', { stream: { maxRetryDelayMs: Infinity } }],
    ['fractional provider retry count', { stream: { maxRetries: 0.5 } }],
  ]
  for (const [label, invalid] of invalidChanges) {
    it.effect(`rejects ${label} without changing the prior snapshot`, () =>
      Effect.gen(function* () {
        const service = yield* Settings.make({ steeringMode: 'all' })
        const before = yield* service.get
        const error = yield* Effect.flip(service.update(invalid))
        assert.instanceOf(error, SettingsError)
        assert.strictEqual(error.reason, 'invalid')
        assert.strictEqual(error.operation, 'update')
        assert.strictEqual(yield* service.get, before)
        assert.strictEqual((yield* service.update({ followUpMode: 'all' })).revision, 1)
        const construction = yield* Effect.flip(Settings.make(invalid))
        assert.strictEqual(construction.operation, 'make')
      }),
    )
  }

  it.effect('rejects malformed untyped modes and environment settings', () =>
    Effect.gen(function* () {
      const service = yield* Settings.make()
      const before = yield* service.get
      // Exercise JavaScript/transport callers that do not have the Change type.
      const malformed: ReadonlyArray<unknown> = [
        { toolExecution: 'batch' },
        { steeringMode: 'one' },
        { followUpMode: 'latest' },
        { stream: { transport: 'http' } },
        { stream: { cacheRetention: 'forever' } },
        { stream: { deferred: { window: '2h' } } },
        { retry: { enabled: null } },
        { compaction: { enabled: 'yes' } },
        { extensions: [''] },
        { cwd: '/tmp' },
        { env: {} },
        { revision: 99 },
        { retry: { extra: true } },
        { stream: { metadata: { invalid: Infinity } } },
        null,
      ]
      const cycle: Record<string, unknown> = {}
      cycle.self = cycle
      const cyclic = yield* Effect.flip(
        service.update({
          stream: { metadata: cycle },
        } as Settings.Change),
      )
      assert.strictEqual(cyclic.reason, 'invalid')
      assert.strictEqual(yield* service.get, before)
      for (const invalid of malformed) {
        const error = yield* Effect.flip(service.update(invalid as Settings.Change))
        assert.strictEqual(error.reason, 'invalid')
        assert.strictEqual(yield* service.get, before)
      }
    }),
  )

  it.effect('publishes coherent concurrent patches without losing independent fields', () =>
    Effect.gen(function* () {
      const service = yield* Settings.make()
      const results = yield* Effect.all(
        Array.from({ length: 50 }, (_, index) =>
          service.update({
            progress: { partialIntervalMs: index, outputIntervalMs: index },
          }),
        ),
        { concurrency: 'unbounded' },
      )
      assert.strictEqual(new Set(results.map((snapshot) => snapshot.revision)).size, 50)
      for (const snapshot of results) {
        assert.strictEqual(snapshot.progress.partialIntervalMs, snapshot.progress.outputIntervalMs)
        assert.isTrue(Object.isFrozen(snapshot))
      }
      const patches = yield* Effect.all(
        [
          service.update({ retry: { maxRetries: 8 } }),
          service.update({ retry: { baseDelayMs: 12 } }),
          service.update({ steeringMode: 'all', followUpMode: 'all' }),
        ],
        { concurrency: 'unbounded' },
      )
      for (const snapshot of patches) {
        assert.strictEqual(snapshot.steeringMode, snapshot.followUpMode)
      }
      const final = yield* service.get
      assert.strictEqual(final.revision, 53)
      assert.strictEqual(final.retry.maxRetries, 8)
      assert.strictEqual(final.retry.baseDelayMs, 12)
      assert.strictEqual(final.steeringMode, 'all')
    }),
  )

  it.effect('copies nested caller objects and freezes every snapshot value', () =>
    Effect.gen(function* () {
      const input = {
        extensions: ['coding'],
        stream: { headers: { trace: 'first' }, metadata: { nested: [{ value: 1 }] } },
        retry: { maxRetries: 4 },
      }
      const service = yield* Settings.make(input)
      const first = yield* service.get
      input.extensions.push('other')
      input.stream.headers.trace = 'changed'
      input.stream.metadata.nested[0]!.value = 2
      input.retry.maxRetries = 9
      assert.deepStrictEqual(first.extensions, ['coding'])
      assert.deepStrictEqual(first.stream.metadata, { nested: [{ value: 1 }] })
      assert.strictEqual(first.stream.headers?.trace, 'first')
      assert.strictEqual(first.retry.maxRetries, 4)
      const second = yield* service.update(input)
      input.stream.metadata.nested[0]!.value = 3
      assert.deepStrictEqual(second.stream.metadata, { nested: [{ value: 2 }] })
      const assertFrozen = (value: unknown): void => {
        if (value !== null && typeof value === 'object') {
          assert.isTrue(Object.isFrozen(value))
          for (const child of Object.values(value)) assertFrozen(child)
        }
      }
      assertFrozen(first)
      assertFrozen(second)
      assertFrozen(Settings.defaults)
      assert.isFalse(Reflect.set(second.retry, 'maxRetries', 100))
      assert.strictEqual((yield* service.get).retry.maxRetries, 9)
    }),
  )

  it.effect('samples getters once without retaining them and rolls back copy failures', () =>
    Effect.gen(function* () {
      let reads = 0
      let delay = 10
      const change = {
        retry: {
          get baseDelayMs() {
            reads++
            return delay
          },
        },
      }
      const service = yield* Settings.make()
      const snapshot = yield* service.update(change)
      assert.strictEqual(reads, 1)
      delay = 20
      assert.strictEqual((yield* service.get).retry.baseDelayMs, 10)
      assert.strictEqual(reads, 1)
      assert.isFalse(
        Object.hasOwn(Object.getOwnPropertyDescriptor(snapshot.retry, 'baseDelayMs')!, 'get'),
      )
      const throws = {
        get retry(): Settings.Retry {
          throw new Error('caller getter failed')
        },
      }
      const error = yield* Effect.flip(service.update(throws))
      assert.strictEqual(error.reason, 'copy')
      assert.strictEqual(yield* service.get, snapshot)
      const uncloneable = { extensions: [() => 'coding'] } as unknown as Settings.Change
      assert.strictEqual((yield* Effect.flip(service.update(uncloneable))).reason, 'copy')
      assert.strictEqual(yield* service.get, snapshot)
    }),
  )

  it.effect('allocates independent references and keeps other bound services unchanged', () =>
    Effect.gen(function* () {
      class Binding extends Context.Service<Binding, { readonly cwd: string }>()(
        'test/SettingsBinding',
      ) {}
      const settingsLayer = Settings.layerDefault
      const use = Effect.gen(function* () {
        const binding = yield* Binding
        const before = yield* Settings.get
        const after = yield* Settings.update({ toolExecution: 'sequential', contextRetentionMs: 0 })
        const optional = yield* Settings.getOrDefault
        assert.strictEqual(after, optional)
        assert.strictEqual(yield* Binding, binding)
        return { before, after, cwd: binding.cwd }
      })
      const results = yield* Effect.all(
        [
          use.pipe(Effect.provide(settingsLayer), Effect.provideService(Binding, { cwd: '/left' })),
          use.pipe(
            Effect.provide(settingsLayer),
            Effect.provideService(Binding, { cwd: '/right' }),
          ),
        ],
        { concurrency: 'unbounded' },
      )
      for (const result of results) {
        assert.strictEqual(result.before.revision, 0)
        assert.strictEqual(result.after.revision, 1)
        assert.strictEqual(result.before.toolExecution, 'parallel')
      }
      assert.deepStrictEqual(
        results.map((result) => result.cwd),
        ['/left', '/right'],
      )
      assert.notStrictEqual(results[0]!.after, results[1]!.after)
      const invalidLayer = Settings.layer({ maxTurns: 0 })
      assert.instanceOf(yield* Effect.flip(Layer.build(invalidLayer)), SettingsError)
    }),
  )
})
