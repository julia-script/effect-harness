import { Config, ConfigProvider, Effect, FileSystem, Layer, Path, Ref, Scope, Exit } from 'effect'
import { assert, describe, it } from '@effect/vitest'

import * as Database from '../src/Database.ts'

const configured = (value: unknown) =>
  ConfigProvider.layer(ConfigProvider.fromUnknown(value, { preserveEmptyStrings: true }))

describe('Database', () => {
  for (const expected of ['/custom/db.sqlite', '']) {
    it.effect(
      `uses injected EXAMPLE_DB ${JSON.stringify(expected)} without allocating temporary storage`,
      () =>
        Effect.gen(function* () {
          const calls = yield* Ref.make(0)
          const fs = FileSystem.layerNoop({
            makeTempDirectoryScoped: () =>
              Ref.update(calls, (n) => n + 1).pipe(Effect.as('/unused')),
          })
          const result = yield* Database.filename.pipe(
            Effect.provide(Layer.mergeAll(fs, Path.layer, configured({ EXAMPLE_DB: expected }))),
          )
          assert.strictEqual(result, expected)
          assert.strictEqual(yield* Ref.get(calls), 0)
        }),
    )
  }

  it.effect(
    'allocates missing configuration lazily and releases temporary storage with its owner',
    () =>
      Effect.gen(function* () {
        const calls = yield* Ref.make(0)
        const active = yield* Ref.make(false)
        const owner = yield* Scope.fork(yield* Scope.Scope)
        const fs = FileSystem.layerNoop({
          makeTempDirectoryScoped: (options) =>
            Effect.acquireRelease(
              Effect.gen(function* () {
                assert.strictEqual(options?.prefix, 'effect-harness-example-')
                yield* Ref.update(calls, (n) => n + 1)
                yield* Ref.set(active, true)
                return '/temporary/database'
              }),
              () => Ref.set(active, false),
            ),
        })
        assert.strictEqual(yield* Ref.get(calls), 0)
        const result = yield* Database.filename.pipe(
          Effect.provide(Layer.mergeAll(fs, Path.layer, configured({}))),
          Scope.provide(owner),
        )
        assert.strictEqual(result, '/temporary/database/example.sqlite')
        assert.strictEqual(yield* Ref.get(calls), 1)
        assert.strictEqual(yield* Ref.get(active), true)
        yield* Scope.close(owner, Exit.void)
        assert.strictEqual(yield* Ref.get(active), false)
      }),
  )

  it.effect(
    'retains the Config failure at the application boundary without creating a database',
    () =>
      Effect.gen(function* () {
        const calls = yield* Ref.make(0)
        const fs = FileSystem.layerNoop({
          makeTempDirectoryScoped: () => Ref.update(calls, (n) => n + 1).pipe(Effect.as('/unused')),
        })
        const source = new ConfigProvider.SourceError({
          message: 'Configuration source unavailable',
        })
        const provider = ConfigProvider.layer(ConfigProvider.make(() => Effect.fail(source)))
        const error = yield* Database.filename.pipe(
          Effect.provide(Layer.mergeAll(fs, Path.layer, provider)),
          Effect.flip,
        )
        assert.ok(error instanceof Database.ConfigurationError)
        assert.strictEqual(error.message, 'Cannot read EXAMPLE_DB')
        assert.ok(error.cause instanceof Config.ConfigError)
        assert.strictEqual(error.cause.cause, source)
        assert.strictEqual(yield* Ref.get(calls), 0)
      }),
  )
})
