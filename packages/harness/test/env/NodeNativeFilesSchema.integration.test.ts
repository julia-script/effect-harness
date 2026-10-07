import * as NodeNativeFiles from '@effect-harness/harness/NodeNativeFiles'
import { assert, describe, it } from '@effect/vitest'
import { vi } from 'vitest'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Context from 'effect/Context'
import * as Path from 'effect/Path'
import * as PlatformError from 'effect/PlatformError'
import { Env, fromPlatform } from '@effect-harness/harness/Env'
import * as NodeEnv from '@effect-harness/harness/NodeEnv'
import * as NativeError from '@effect-harness/harness/env/NativeError'
import { withEnv } from '../tools/Helpers.ts'

const probe = vi.hoisted(() => ({
  path: '',
  failure: undefined as unknown,
  calls: [] as Array<string>,
}))
vi.mock('node:fs/promises', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:fs/promises')>()
  return {
    ...actual,
    statfs: (...args: Parameters<typeof actual.statfs>) => {
      probe.calls.push(String(args[0]))
      if (args[0] === probe.path) return Promise.reject(probe.failure)
      return actual.statfs(...args)
    },
  }
})

describe('NodeNativeFilesSchema', () => {
  describe('NativeSchema', () => {
    it('present native codes retain coercion and exact foreign causes across both adapters', () => {
      const foreign = { code: { toString: () => 'EACCES' }, message: 'denied' }
      const native = NodeNativeFiles.fileError(foreign, '/file')
      assert.strictEqual(native.code, 'permission_denied')
      assert.strictEqual(native.cause, foreign)
      const platform = new PlatformError.PlatformError(
        new PlatformError.SystemError({
          _tag: 'Unknown',
          module: 'fs',
          method: 'open',
          cause: foreign,
        }),
      )
      const mapped = fromPlatform(platform, '/file')
      assert.strictEqual(mapped.code, 'permission_denied')
      assert.strictEqual(mapped.cause, platform)
      assert.strictEqual(platform.reason.cause, foreign)
      assert.strictEqual(NativeError.code({ code: 17 }), '17')
      assert.strictEqual(NativeError.code({ code: undefined }), 'undefined')
      assert.strictEqual(NativeError.code({}), undefined)
      assert.strictEqual(NativeError.code(Object.create({ code: 'ENOENT' })), 'ENOENT')
      const getter = {
        get code(): never {
          throw new Error('foreign getter')
        },
      }
      const conversion = {
        code: {
          toString(): never {
            throw new Error('foreign conversion')
          },
        },
      }
      for (const opaque of [getter, conversion]) {
        assert.strictEqual(NativeError.code(opaque), undefined)
        assert.strictEqual(NodeNativeFiles.fileError(opaque, '/file').cause, opaque)
      }
    })
    // This conformance case acquires real host filesystem/process resources; their completion callbacks are independent of TestClock.
    it.live(
      'failed real statfs capability probing maps the actual cause then climbs an injected Path ancestor',
      () =>
        withEnv(
          Effect.gen(function* () {
            const original = yield* Env
            const path = yield* Path.Path
            const parents: Array<string> = []
            let coerced = 0
            const failure = {
              code: {
                toString: () => {
                  coerced++
                  return 'EACCES'
                },
              },
              message: 'injected statfs denial',
            }
            yield* Effect.sync(() => {
              probe.path = original.cwd
              probe.failure = failure
              probe.calls = []
            })
            const replacement = Path.Path.of({
              ...path,
              dirname: (value) => {
                parents.push(value)
                return path.dirname(value)
              },
            })
            const services = yield* Layer.build(
              NodeEnv.layer({
                cwd: original.cwd,
                shell: '/bin/sh',
                host: {
                  platform: 'linux',
                  cwd: original.cwd,
                  home: original.cwd,
                  searchPathDelimiter: ':',
                },
              }).pipe(Layer.provide(Layer.succeed(Path.Path, replacement))),
            )
            const env = Context.get(services, Env)
            const watch = yield* env.watch([{ path: original.cwd, recursive: true }])
            assert.strictEqual(watch.mode, 'native')
            assert.strictEqual(coerced, 1)
            assert.deepStrictEqual(probe.calls.slice(0, 2), [
              original.cwd,
              path.dirname(original.cwd),
            ])
            assert.strictEqual(parents[0], original.cwd)
          }),
        ),
    )
  })
})
