import * as Option from 'effect/Option'
import * as Duration from 'effect/Duration'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Cause from 'effect/Cause'
import * as Context from 'effect/Context'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as NodeServices from '@effect/platform-node/NodeServices'
import * as Env from '@effect-harness/harness/Env'
import * as NodeEnv from '@effect-harness/harness/NodeEnv'
import * as Conformance from '@effect-harness/harness/testing/EnvConformance'
import * as Runner from '@effect-harness/harness/testing/Runner'
import * as Assertions from '@effect-harness/harness/testing/Assertions'
import * as Read from '@effect-harness/harness/tools/Read'
import * as Image from '@effect-harness/harness/tools/Image'
import { withEnv } from '../tools/Helpers.ts'

describe('EnvConformance', () => {
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
    ).pipe(Layer.provide(NodeServices.layer))

  for (const mode of ['native', 'polling'] as const)
    Runner.registerEnvConformance(
      {
        describe,
        test: (name, run, timeoutMs) =>
          // This conformance case acquires real host filesystem/process resources; their completion callbacks are independent of TestClock.
          it.live(name, run, {
            timeout: timeoutMs === undefined ? 5000 : Duration.toMillis(timeoutMs),
          }),
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
        ).pipe(Layer.provide(NodeServices.layer))
        const program = Conformance.withEnv(
          Effect.gen(function* () {
            assert.deepStrictEqual(yield* (yield* Env.Env).listDir('.'), [])
          }),
          adapter,
        )
        yield* program.pipe(Effect.provideService(AdapterSettings, { refuse: false }))
        const failure = yield* program.pipe(
          Effect.provideService(AdapterSettings, { refuse: true }),
          Effect.flip,
        )
        assert.instanceOf(failure, Env.FileError)
        assert.instanceOf(failure.reason, Env.FileNotSupported)
        assert.strictEqual(failure.message, 'adapter initialization rejected')
        assert.strictEqual(failure.cause, undefined)
      }),
    )
    // Real filesystem acquisition and release callbacks advance on the host loop; TestClock cannot drive them.
    it.live(
      'acquires a fresh empty cwd and joins Env cleanup before directory removal on failure',
      () =>
        Effect.gen(function* () {
          const directories: Array<string> = []
          const cleaned: Array<string> = []
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
            assert.deepStrictEqual(exit, Exit.fail('intentional'))
          }
          assert.notStrictEqual(directories[0], directories[1])
          assert.deepStrictEqual(cleaned, directories)
          const fs = yield* FileSystem.FileSystem
          for (const directory of directories) assert.isFalse(yield* fs.exists(directory))
        }).pipe(Effect.provide(NodeServices.layer)),
    )

    // This conformance case acquires real host filesystem/process resources; their completion callbacks are independent of TestClock.

    it.live(
      'registration preserves every name, timeout and real Effect; broken adapters fail conformance',
      () =>
        Effect.gen(function* () {
          const cases = Conformance.makeEnvConformance({ assertions: assert })
          assert.strictEqual(cases.length, 24)
          const names: Array<string> = []
          const timeouts: Array<Duration.Input | undefined> = []
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
          ).pipe(Layer.provide(NodeServices.layer))
          const first = cases[0]
          assert.ok(first)
          const failure = yield* Conformance.withEnv(first.run, broken).pipe(Effect.flip)
          assert.instanceOf(failure, Env.FileError)
          assert.instanceOf(failure.reason, Env.FileNotSupported)
          assert.strictEqual(failure.message, 'negative control')
          const adapted = Assertions.makeExpectAssertions((actual) => ({
            toBe: (expected) => assert.strictEqual(actual, expected),
            toEqual: (expected) => assert.deepStrictEqual(actual, expected),
            toBeTruthy: () => assert.ok(actual),
            toMatchObject: (expected) => {
              if (expected !== null && typeof expected === 'object') {
                assert.ok(actual !== null && typeof actual === 'object')
                for (const key of Reflect.ownKeys(expected))
                  assert.deepStrictEqual(Reflect.get(actual, key), Reflect.get(expected, key))
              } else assert.deepStrictEqual(actual, expected)
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
          const rejected = yield* adapted
            .rejects(Effect.succeed('unexpected'), 'failure')
            .pipe(Effect.exit)
          assert.isTrue(Exit.isFailure(rejected))
          if (Exit.isFailure(rejected)) {
            assert.isTrue(Cause.hasDies(rejected.cause))
            // This is the foreign assertion library's own error: its stack depends
            // on runner version, while type/name/message are the stable contract.
            const defect = Cause.squash(rejected.cause)
            assert.instanceOf(defect, Error)
            if (defect instanceof Error) {
              assert.strictEqual(defect.name, 'AssertionError')
              assert.match(defect.message, /expected false to (equal|be) true/)
            }
          }
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
            assert.deepStrictEqual(
              Image.detectSupportedImageMimeType(png),
              Option.some('image/png'),
            )
            yield* env.writeFile('image.bin', png)
            const result = yield* Read.handler({ path: 'image.bin' })
            assert.isTrue(result.isError)
            assert.strictEqual(result.diagnostics?.[0]?.kind, 'unsupported_image')
            const animated = new Uint8Array(png)
            animated.set([97, 99, 84, 76], 37)
            assert.isTrue(Option.isNone(Image.detectSupportedImageMimeType(animated)))
            yield* env.writeFile('animated.png', animated)
            assert.isNotTrue((yield* Read.handler({ path: 'animated.png' })).isError)
          }),
        ),
    )
  })
})
