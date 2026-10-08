// effect-nit-allow P8-tests-import-public-specifiers: this same-package fixture delegates real Node resource acquisition through the null-exported native adapter; package-denial tests remain unchanged.
// effect-nit-allow P9-no-internal-cross-import: this same-package fixture delegates real Node resource acquisition through the null-exported native adapter; package-denial tests remain unchanged.
import * as NodeResourceAdapter from '../src/internal/NodeResourceAdapter.ts'

import * as NodeNativeFiles from 'effect-harness/NodeNativeFiles'

import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Layer from 'effect/Layer'

import * as Context from 'effect/Context'

import * as Path from 'effect/Path'

import * as PlatformError from 'effect/PlatformError'

import { Env, fromPlatform } from 'effect-harness/Env'

import * as NodeEnv from 'effect-harness/NodeEnv'

import * as NativeError from 'effect-harness/env/NativeError'

import { withEnv as withBaseEnv } from './EnvFixture.ts'

class NativeProbe extends Context.Service<NativeProbe, ReturnType<typeof makeState>>()(
  'test/harness/env/NodeNativeFilesSchema/NativeProbe',
) {}
const makeState = () => ({
  path: '',
  failure: undefined as unknown,
  calls: [] as Array<string>,
})
const withEnv = <A, E, R>(program: Effect.Effect<A, E, R | NativeProbe>) =>
  Effect.gen(function* () {
    const probe = yield* Effect.sync(makeState)
    const adapter = NodeResourceAdapter.NodeResourceAdapter.of({
      ...NodeResourceAdapter.native,
      statfs: new Proxy(NodeResourceAdapter.native.statfs, {
        apply: (target, receiver, args) => {
          probe.calls.push(String(args[0]))
          if (args[0] === probe.path) return Promise.reject(probe.failure)
          return Reflect.apply(target, receiver, args)
        },
      }),
    })
    return yield* withBaseEnv(program.pipe(Effect.provideService(NativeProbe, probe))).pipe(
      Effect.provideService(NodeResourceAdapter.NodeResourceAdapter, adapter),
    )
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
      assert.strictEqual(NativeError.codeOrUndefined({ code: 17 }), '17')
      assert.strictEqual(NativeError.codeOrUndefined({ code: undefined }), 'undefined')
      assert.strictEqual(NativeError.codeOrUndefined({}), undefined)
      assert.strictEqual(NativeError.codeOrUndefined(Object.create({ code: 'ENOENT' })), 'ENOENT')
      const getter = {
        get code(): never {
          throw new Error('foreign getter')
        },
      }
      const conversion = {
        code: {
          // effect-nit-allow P7-v4-data-type-naming: native string coercion invokes the fixed toString protocol slot; this hostile fixture must throw the original foreign cause to exercise the guarded boundary.
          toString(): never {
            throw new Error('foreign conversion')
          },
        },
      }
      for (const opaque of [getter, conversion]) {
        assert.strictEqual(NativeError.codeOrUndefined(opaque), undefined)
        assert.strictEqual(NodeNativeFiles.fileError(opaque, '/file').cause, opaque)
      }
    })
    // This conformance case acquires real host filesystem/process resources; their completion callbacks are independent of TestClock.
    it.live(
      'failed real statfs capability probing maps the actual cause then climbs an injected Path ancestor',
      () =>
        withEnv(
          Effect.gen(function* () {
            const probe = yield* NativeProbe
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
