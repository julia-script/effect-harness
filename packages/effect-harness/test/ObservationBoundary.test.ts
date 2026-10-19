import * as TemporaryStorage from './fixtures/Storage.js'
import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as Struct from 'effect/Struct'
import * as LanguageModel from 'effect/ai/LanguageModel'
import type * as Conversation from 'effect-harness/Conversation'
import * as View from 'effect-harness/ConversationView'
import * as Events from 'effect-harness/AgentEvent'
import * as Harness from 'effect-harness/Harness'
import { HarnessBackend } from 'effect-harness/HarnessBackend'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Registry from 'effect-harness/Registry'
import * as Storage from 'effect-harness/Storage'
import * as Task from 'effect-harness/Task'
import { TaskExecution } from 'effect-harness/TaskExecution'
import * as Toolkit from 'effect-harness/Toolkit'

const bun = 'Bun' in globalThis
const services = bun ? (await import('@effect/platform-bun')).BunServices.layer : NodeServices.layer

describe('authentic observation boundary', () => {
  for (const backend of ['temporary', 'jsonl'] as const)
    it.live(
      `real invocation gates targets/options before effects and joins held callbacks: ${backend}`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'observation-boundary-' })

            const layer = (() => {
              if (backend === 'temporary') return TemporaryStorage.layer

              return Storage.layerJsonl({ filePath: `${directory}/native.jsonl` })
            })()
            const context = yield* Layer.build(layer)
            const model = yield* LanguageModel.make({
              generateText: () => Effect.die('No provider'),
              streamText: () => Stream.die('No provider'),
            })
            const entered = yield* Deferred.make<Conversation.Conversation>()
            const settle = yield* Deferred.make<void>()
            const settling = yield* Deferred.make<void>()
            const parentScopeClosing = yield* Deferred.make<void>()
            const contract = {
              kind: 'observation.bound-parent',
              version: 1,
              input: Schema.Void,
              checkpoint: Schema.Struct({ phase: Schema.Literal('execute') }),
              success: Schema.Void,
              failure: Schema.String,
            } as const
            const program = Task.make({
              ...contract,
              initial: () => ({ phase: 'execute' as const }),
              phases: {
                execute: () =>
                  Effect.gen(function* () {
                    const execution = yield* TaskExecution
                    const own = yield* execution.subagent!.conversation(execution.conversationId)
                    yield* Effect.addFinalizer(() =>
                      Deferred.succeed(parentScopeClosing, undefined),
                    )
                    yield* Deferred.succeed(entered, own)
                    yield* Deferred.await(settle)
                    yield* execution.commit((tx) =>
                      execution.progress(tx, contract, {
                        status: 'completing',
                        outcome: { status: 'completed', result: undefined },
                      }),
                    )
                    yield* Deferred.succeed(settling, undefined)
                  }),
              },
            })
            const registry = yield* Registry.make
            yield* registry.install(
              Registry.withTask(
                Registry.toolkit({ id: 'boundary-parent', version: 1 }, Toolkit.make()),
                program,
              ),
            )
            const runtime = yield* HarnessRuntime.make({ registry }).pipe(
              Effect.provideContext(context),
              Effect.provideService(LanguageModel.LanguageModel, model),
            )
            const client = yield* Harness.make.pipe(
              Effect.provideService(HarnessBackend, runtime.backend),
            )
            const root = yield* client.root
            const foreign = yield* client.create()
            const ref = yield* runtime.scheduler.start(program, undefined, {
              conversationId: root.id,
            })
            const bound = yield* Deferred.await(entered)
            let optionReads = 0
            const options = {
              get capacity() {
                optionReads++
                return 2
              },
            }
            const denied = { ...bound, id: foreign.id }
            assert.isTrue(
              Exit.isFailure(yield* View.watch(denied, options).pipe(Stream.runHead, Effect.exit)),
            )
            assert.strictEqual(optionReads, 0)
            assert.strictEqual((yield* View.snapshot(bound)).value.conversation.id, root.id)
            const revoked = yield* View.watch(bound).pipe(Stream.runDrain, Effect.forkChild)
            const outer = yield* Scope.Scope
            const held = yield* Deferred.make<void>()
            const release = yield* Deferred.make<void>()
            yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined))
            let callbackClosed = 0
            let delivered = 0
            const observer = yield* View.watchWith(bound, () =>
              Effect.uninterruptible(
                Effect.gen(function* () {
                  assert.notStrictEqual(yield* Scope.Scope, outer)
                  yield* Effect.addFinalizer(() =>
                    Effect.sync(() => {
                      callbackClosed++
                    }),
                  )
                  yield* Deferred.succeed(held, undefined)
                  yield* Deferred.await(release)
                  return 'held-value'
                }),
              ),
            ).pipe(
              Stream.tap(() =>
                Effect.sync(() => {
                  delivered++
                }),
              ),
              Stream.runDrain,
              Effect.forkChild,
            )
            yield* Deferred.await(held)
            yield* Deferred.succeed(settle, undefined)
            yield* Deferred.await(settling)
            yield* Fiber.await(revoked)
            assert.isTrue(Exit.isFailure(yield* View.snapshot(bound).pipe(Effect.exit)))
            assert.isTrue((yield* runtime.scheduler.inspect(ref.id)).invoking)
            assert.strictEqual(callbackClosed, 0)
            yield* Deferred.succeed(release, undefined)
            const exit = yield* Fiber.await(observer)
            assert.isTrue(Exit.isFailure(exit))
            assert.strictEqual(callbackClosed, 1)
            assert.strictEqual(delivered, 0)
            assert.strictEqual((yield* runtime.scheduler.wait(ref)).status, 'completed')
            yield* Deferred.await(parentScopeClosing)
            let targetReads = 0
            const stale = {
              ...bound,
              get id() {
                targetReads++
                return root.id
              },
            }
            assert.isTrue(Exit.isFailure(yield* View.snapshot(stale).pipe(Effect.exit)))
            assert.isTrue(
              Exit.isFailure(yield* Events.watch(stale, options).pipe(Stream.runHead, Effect.exit)),
            )
            assert.strictEqual(targetReads, 0)
            assert.strictEqual(optionReads, 0)
          }),
        ).pipe(Effect.provide(services)),
    )

  it.effect('missing optional capabilities fail before target and option access', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const model = yield* LanguageModel.make({
          generateText: () => Effect.die('No provider'),
          streamText: () => Stream.die('No provider'),
        })
        const runtime = yield* HarnessRuntime.make().pipe(
          Effect.provideService(LanguageModel.LanguageModel, model),
        )
        const oldBackend = Struct.omit(runtime.backend, ['observations'])
        const client = yield* Harness.make.pipe(Effect.provideService(HarnessBackend, oldBackend))
        const root = yield* client.root
        let reads = 0
        const self = {
          ...root,
          get id() {
            reads++
            return root.id
          },
        }
        const options = {
          get capacity() {
            reads++
            return 2
          },
        }
        assert.strictEqual((yield* View.snapshot(self).pipe(Effect.flip))._tag, 'ObservationError')
        const view = yield* View.watch(self, options).pipe(Stream.runHead, Effect.flip)
        const events = yield* Events.watch(self, options).pipe(Stream.runHead, Effect.flip)
        assert.strictEqual(view._tag, 'ObservationError')
        assert.strictEqual(events._tag, 'ObservationError')
        assert.strictEqual(reads, 0)
      }),
    ).pipe(Effect.provide(TemporaryStorage.layer)),
  )
})
