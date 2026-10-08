import { assert, describe, it } from '@effect/vitest'
import * as ConfigProvider from 'effect/ConfigProvider'
import * as Path from 'effect/Path'
import * as PlatformError from 'effect/PlatformError'
// effect-nit-allow P8-tests-import-public-specifiers: this same-package ownership fixture exercises the null-exported shell factory seam; public NodeEnv configuration behavior is tested through its package API.
// effect-nit-allow P9-no-internal-cross-import: the deliberately private shell factory has no package export; only this direct capture/ConfigProvider boundary uses its relative implementation path.
import { makeShellResolver } from '../../src/internal/nodeEnv.ts'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as FileSystem from 'effect/FileSystem'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Model from 'effect-harness/Model'
import * as AtomicWrite from 'effect-harness/env/AtomicWrite'
import * as NodeNativeFiles from 'effect-harness/NodeNativeFiles'
import { Env, make } from 'effect-harness/Env'
import { NativeFiles } from 'effect-harness/NativeFiles'
import { withEnv } from '../EnvFixture.ts'
import * as DirectoryFixture from '../tools/DirectoryFixture.ts'

describe('ConstructionOwnership', () => {
  it.effect(
    'captured shell discovery samples the executing ConfigProvider on each resolution',
    () =>
      Effect.gen(function* () {
        const attempts: Array<string> = []
        const captured = FileSystem.makeNoop({
          access: (name) =>
            Effect.suspend(() => {
              attempts.push(name)
              return name === '/first/bash' || name === '/second/bash'
                ? Effect.void
                : Effect.fail(
                    PlatformError.systemError({
                      _tag: 'NotFound',
                      module: 'FileSystem',
                      method: 'access',
                      pathOrDescriptor: name,
                    }),
                  )
            }),
        })
        const resolve = yield* makeShellResolver({
          platform: 'linux',
          cwd: '/',
          home: '/',
          searchPathDelimiter: ':',
        }).pipe(Effect.provideService(FileSystem.FileSystem, captured), Effect.provide(Path.layer))
        assert.deepStrictEqual(attempts, [])
        for (const directory of ['/first', '/second']) {
          const shell = yield* resolve().pipe(
            Effect.provideService(
              ConfigProvider.ConfigProvider,
              ConfigProvider.fromUnknown({ PATH: directory }),
            ),
            Effect.provideService(
              FileSystem.FileSystem,
              FileSystem.makeNoop({
                access: () => Effect.die('ambient filesystem must not replace captured discovery'),
              }),
            ),
          )
          assert.strictEqual(shell.program, directory + '/bash')
        }
        assert.deepStrictEqual(attempts, ['/bin/bash', '/first/bash', '/bin/bash', '/second/bash'])
      }),
  )

  it.effect(
    'an acquired atomic writer retains its selected filesystem after context replacement',
    () =>
      withEnv(
        Effect.gen(function* () {
          const env = yield* Env
          const fs = yield* FileSystem.FileSystem
          let selected = 0
          const captured: FileSystem.FileSystem = {
            ...fs,
            rename: (source, target) =>
              Effect.suspend(() => {
                selected++
                return fs.rename(source, target)
              }),
          }
          const writer = yield* AtomicWrite.make.pipe(
            Effect.provideService(FileSystem.FileSystem, captured),
            Effect.provide(NodeNativeFiles.layer),
          )
          const replacement: FileSystem.FileSystem = {
            ...fs,
            rename: () => Effect.die('ambient filesystem must not replace captured writer'),
          }
          yield* writer(env.path.join(env.cwd, 'owned'), 'captured bytes').pipe(
            Effect.provideService(FileSystem.FileSystem, replacement),
          )
          assert.strictEqual(selected, 1)
          assert.strictEqual(yield* env.readTextFile('owned'), 'captured bytes')
          yield* writer(env.path.join(env.cwd, 'second-owned'), 'second captured bytes').pipe(
            Effect.provideService(FileSystem.FileSystem, replacement),
          )
          assert.strictEqual(selected, 2)
          assert.strictEqual(yield* env.readTextFile('second-owned'), 'second captured bytes')
        }),
      ),
  )

  it.effect(
    'Env acquisition allocates no watchers and each call owns its selected producer scope',
    () =>
      withEnv(
        Effect.gen(function* () {
          const original = yield* Env
          const native = yield* NativeFiles.pipe(Effect.provide(NodeNativeFiles.layer))
          let active = 0
          const captured: NativeFiles['Service'] = {
            ...native,
            watchDirectory: () =>
              DirectoryFixture.notifications(() =>
                Effect.acquireRelease(
                  Effect.sync(() => {
                    active++
                  }),
                  () =>
                    Effect.sync(() => {
                      active--
                    }),
                ),
              ),
          }
          const env = yield* make({ id: 'captured-watcher', cwd: original.cwd }).pipe(
            Effect.provideService(NativeFiles, captured),
          )
          assert.strictEqual(active, 0)
          const firstOwner = yield* Scope.fork(yield* Scope.Scope)
          const secondOwner = yield* Scope.fork(yield* Scope.Scope)
          const replaced: NativeFiles['Service'] = {
            ...native,
            watchDirectory: () =>
              Effect.die('ambient native producer must not replace captured one'),
          }
          const open = (owner: Scope.Closeable) =>
            env
              .watch([{ path: env.path.join(env.cwd, 'missing-target') }], { mode: 'native' })
              .pipe(Scope.provide(owner), Effect.provideService(NativeFiles, replaced))
          const first = yield* open(firstOwner)
          assert.strictEqual(first.mode, 'native')
          const perWatch = active
          assert.strictEqual(perWatch > 0, true)
          const second = yield* open(secondOwner)
          assert.strictEqual(second.mode, 'native')
          assert.strictEqual(active, perWatch * 2)
          yield* Scope.close(firstOwner, Exit.void)
          assert.strictEqual(active, perWatch)
          yield* Scope.close(secondOwner, Exit.void)
          assert.strictEqual(active, 0)
          yield* Scope.close(firstOwner, Exit.void)
          assert.strictEqual(active, 0)
        }),
      ),
  )

  it.effect(
    'catalog registrations preserve exact heterogeneous instances, last-wins and caller scopes',
    () =>
      Effect.gen(function* () {
        let released = 0
        const owner = yield* Scope.fork(yield* Scope.Scope)
        const first = yield* LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () => Stream.empty,
        })
        const second = yield* LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () => Stream.empty,
        })
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            released++
          }),
        ).pipe(Scope.provide(owner))
        const descriptor = (
          model: LanguageModel.LanguageModel,
          modelId: string,
        ): Model.Descriptor => ({
          ref: { provider: 'owned', modelId },
          model,
          contextWindow: 10000,
          maxOutputTokens: 1000,
          configure: () => Effect.succeed(Context.empty()),
        })
        const original = descriptor(first, 'same')
        const winner = descriptor(second, 'same')
        const distinct = descriptor(first, 'distinct')
        const catalog = yield* Model.Catalog.pipe(
          Effect.provide(Model.layer([original, distinct, winner])),
        )
        assert.strictEqual(yield* catalog.resolve(winner.ref), winner)
        assert.strictEqual((yield* catalog.resolve(winner.ref)).model, second)
        assert.strictEqual(yield* catalog.resolve(distinct.ref), distinct)
        assert.strictEqual(released, 0)
        const empty = yield* Model.Catalog.pipe(Effect.provide(Model.layer([])))
        const error = yield* Effect.flip(empty.resolve(winner.ref))
        assert.strictEqual(error._tag, 'ModelError')
        assert.strictEqual(error.reason._tag, 'ModelNoModelError')
        yield* Scope.close(owner, Exit.void)
        assert.strictEqual(released, 1)
      }),
  )
})
