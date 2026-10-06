import { assert, describe, expect, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Cause from 'effect/Cause'
import * as Context from 'effect/Context'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as NodeFileSystem from '@effect/platform-node/NodeFileSystem'
import * as Env from '../../src/Env.ts'
import * as NodeEnv from '../../src/env/Node.ts'
import * as Conformance from '../../src/testing/EnvConformance.ts'
import * as Runner from '../../src/testing/Runner.ts'
import * as Assertions from '../../src/testing/Assertions.ts'
import * as Read from '../../src/tools/Read.ts'
import * as Image from '../../src/tools/Image.ts'
import { withEnv } from './Helpers.ts'

const patched = (cwd: string, update: (env: Env.Env['Service']) => Env.Env['Service']) =>
  Layer.effect(
    Env.Env,
    Effect.gen(function* () {
      const env = Context.get(yield* Layer.build(NodeEnv.layer({ cwd })), Env.Env)
      return update(env)
    }),
  )
class AdapterSettings extends Context.Service<AdapterSettings, { readonly refuse: boolean }>()(
  'test/EnvConformance/AdapterSettings',
) {}
const fresh = (mode: 'native' | 'polling') =>
  Conformance.freshLayer((cwd) =>
    NodeEnv.layer({
      cwd,
      shell: '/bin/sh',
      watch: { mode, pollIntervalMs: 20 },
      env: { BASH_ENV: '', PATH: '/usr/bin:/bin' },
    }),
  ).pipe(Layer.provide(NodeFileSystem.layer))

for (const mode of ['native', 'polling'] as const)
  Runner.registerEnvConformance(
    {
      describe,
      test: (name, run, timeoutMs) => it.live(name, run, { timeout: timeoutMs ?? 5000 }),
    },
    `public Env conformance (${mode})`,
    fresh(mode),
    { assertions: assert },
  )

describe('public conformance adapter and scope ownership', () => {
  it.effect('preserves adapter initialization errors and required services', () =>
    Effect.gen(function* () {
      const adapter = Conformance.freshLayer((cwd) =>
        Layer.effect(
          Env.Env,
          Effect.gen(function* () {
            const settings = yield* AdapterSettings
            if (settings.refuse)
              return yield* new Env.FileError({
                reason: new Env.FileNotSupported({ message: 'adapter initialization rejected' }),
              })
            return Context.get(yield* Layer.build(NodeEnv.layer({ cwd })), Env.Env)
          }),
        ),
      ).pipe(Layer.provide(NodeFileSystem.layer))
      const program = Conformance.withEnv(
        Effect.gen(function* () {
          assert.deepStrictEqual(yield* (yield* Env.Env).listDir('.'), [])
        }),
        adapter,
      )
      const retained: AdapterSettings extends Effect.Services<typeof program> ? true : false = true
      // @ts-expect-error A required adapter service cannot disappear at the public Layer boundary.
      const erased: Effect.Services<typeof program> extends never ? true : false = true
      void retained
      void erased
      yield* program.pipe(Effect.provideService(AdapterSettings, { refuse: false }))
      const exit = yield* program.pipe(
        Effect.provideService(AdapterSettings, { refuse: true }),
        Effect.exit,
      )
      assert.isTrue(Exit.isFailure(exit))
      if (Exit.isFailure(exit))
        assert.match(String(Cause.squash(exit.cause)), /initialization rejected/)
    }),
  )
  it.live(
    'acquires a fresh empty cwd and joins Env cleanup before directory removal on failure',
    () =>
      Effect.gen(function* () {
        const directories: string[] = []
        const cleaned: string[] = []
        const adapter = Conformance.freshLayer((cwd) =>
          Layer.effect(
            Env.Env,
            Effect.gen(function* () {
              const fs = yield* FileSystem.FileSystem
              const env = Context.get(yield* Layer.build(NodeEnv.layer({ cwd })), Env.Env)
              yield* Effect.addFinalizer(() =>
                fs.exists(cwd).pipe(
                  Effect.tap((exists) =>
                    Effect.sync(() => {
                      assert.strictEqual(exists, true)
                      cleaned.push(cwd)
                    }),
                  ),
                  Effect.orDie,
                ),
              )
              return env
            }),
          ),
        )
        for (let index = 0; index < 2; index++) {
          const exit = yield* Conformance.withEnv(
            Effect.gen(function* () {
              const env = yield* Env.Env
              directories.push(env.cwd)
              assert.deepStrictEqual(yield* env.listDir('.'), [])
              yield* env.writeFile('owned', 'value')
              return yield* Effect.fail('intentional')
            }),
            adapter,
          ).pipe(Effect.exit)
          assert.isTrue(Exit.isFailure(exit))
        }
        assert.notStrictEqual(directories[0], directories[1])
        assert.deepStrictEqual(cleaned, directories)
        const fs = yield* FileSystem.FileSystem
        for (const directory of directories) assert.isFalse(yield* fs.exists(directory))
      }).pipe(Effect.provide(NodeFileSystem.layer)),
  )

  it.live(
    'registration preserves every name, timeout and real Effect; broken adapters fail conformance',
    () =>
      Effect.gen(function* () {
        const cases = Conformance.createEnvConformance({ assertions: assert })
        assert.strictEqual(cases.length, 24)
        const names: string[] = []
        const timeouts: Array<number | undefined> = []
        Runner.registerEnvConformance(
          {
            describe: (_name, suite) => suite(),
            test: (name, run, timeout) => {
              names.push(name)
              timeouts.push(timeout)
              assert.isTrue(Effect.isEffect(run()))
            },
          },
          'captured',
          fresh('polling'),
          { assertions: assert },
        )
        assert.deepStrictEqual(
          names,
          cases.map((test) => test.name),
        )
        assert.deepStrictEqual(
          timeouts,
          cases.map((test) => test.timeoutMs),
        )
        const broken = Conformance.freshLayer((cwd) =>
          patched(cwd, (env) => ({
            ...env,
            openBinaryReader: () =>
              Effect.fail(
                new Env.FileError({
                  reason: new Env.FileNotSupported({ message: 'negative control' }),
                }),
              ),
          })),
        ).pipe(Layer.provide(NodeFileSystem.layer))
        const first = cases[0]
        assert.ok(first)
        const exit = yield* Conformance.withEnv(first.run, broken).pipe(Effect.exit)
        assert.isTrue(Exit.isFailure(exit))
        if (Exit.isFailure(exit)) assert.match(String(Cause.squash(exit.cause)), /negative control/)
        const adapted = Assertions.createExpectAssertions((actual) => ({
          toBe: (expected) => assert.strictEqual(actual, expected),
          toEqual: (expected) => assert.deepStrictEqual(actual, expected),
          toBeTruthy: () => assert.ok(actual),
          toMatchObject: (expected) => {
            if (expected !== null && typeof expected === 'object')
              expect(actual).toMatchObject(expected)
            else expect(actual).toEqual(expected)
          },
          toBeGreaterThan: (expected) => assert.isAbove(Number(actual), expected),
        }))
        assert.throws(() => adapted.strictEqual('incorrect', 'expected'))
        adapted.partialDeepEqual({ expected: 1, retained: true }, { expected: 1 })
        adapted.greaterThan(2, 1)
        yield* adapted.rejects(
          Effect.fail(
            new Env.FileError({
              reason: new Env.FileUnknown({ message: 'expected adapter rejection' }),
            }),
          ),
          'adapter rejection',
        )
        assert.isTrue(
          Exit.isFailure(
            yield* adapted.rejects(Effect.succeed('unexpected'), 'failure').pipe(Effect.exit),
          ),
        )
      }),
  )

  it.effect(
    'recognized images remain Pi unsupported while animated PNG follows the text path',
    () =>
      withEnv(
        Effect.gen(function* () {
          const env = yield* Env.Env
          const png = Uint8Array.from([
            137,
            80,
            78,
            71,
            13,
            10,
            26,
            10,
            0,
            0,
            0,
            13,
            73,
            72,
            68,
            82,
            ...Array.from({ length: 17 }, () => 0),
            0,
            0,
            0,
            0,
            73,
            68,
            65,
            84,
            0,
            0,
            0,
            0,
          ])
          assert.strictEqual(Image.detectSupportedImageMimeType(png), 'image/png')
          yield* env.writeFile('image.bin', png)
          const result = yield* Read.handler({ path: 'image.bin' })
          assert.isTrue(result.isError)
          assert.strictEqual(result.diagnostics?.[0]?.kind, 'unsupported_image')
          const animated = new Uint8Array(png)
          animated.set([97, 99, 84, 76], 37)
          assert.strictEqual(Image.detectSupportedImageMimeType(animated), undefined)
          yield* env.writeFile('animated.png', animated)
          assert.isNotTrue((yield* Read.handler({ path: 'animated.png' })).isError)
        }),
      ),
  )
})
