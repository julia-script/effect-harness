import { assert, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Context from 'effect/Context'
import * as Schema from 'effect/Schema'
import * as Option from 'effect/Option'
import * as Deferred from 'effect/Deferred'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import * as Storage from 'effect-harness/Storage'
import * as Format from 'effect-harness/StorageFormat'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Registry from 'effect-harness/Registry'
import * as Task from 'effect-harness/Task'
import * as Subagent from 'effect-harness/Subagent'
import * as Toolkit from 'effect-harness/Toolkit'
import * as Wire from 'effect-harness/PromptWire'
import * as Session from 'effect-harness/Session'
import { TaskExecution } from 'effect-harness/TaskExecution'
import * as Memory from '../src/internal/Memory.js'
import * as Kernel from '../src/internal/Kernel.js'
import * as MediaRow from '../src/internal/MediaRow.js'
import * as Invocation from '../src/internal/ConversationInvocation.js'
import * as Script from './fixtures/ScriptedStream.js'

for (const work of ['input', 'write'] as const)
  it.live(
    `actual subagent ${work} replay slots share images with supervisors and replay exact requests`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const access = yield* Memory.makeAccess
          const storage = yield* Kernel.make(access)
          const context = Context.make(Storage.Storage, storage).pipe(
            Context.add(Format.StorageFormat, storage.storageFormat),
          )
          const image = Prompt.filePart({
            mediaType: 'image/png',
            data: new Uint8Array(65536).fill(123),
            fileName: 'replay.png',
          })
          const model = yield* Schema.encodeEffect(Schema.Array(Schema.toCodecJson(Wire.Message)))([
            Prompt.userMessage({ content: [image] }),
          ])
          const ready = yield* Deferred.make<void>()
          const requests: Array<Subagent.Request> = []
          const contract = {
            kind: 'media.held',
            version: 1,
            input: Schema.Void,
            checkpoint: Schema.Struct({ phase: Schema.tag('run') }),
            success: Schema.Void,
            failure: Schema.String,
          } as const
          const definition = Task.make({
            ...contract,
            initial: () => ({ phase: 'run' as const }),
            phases: {
              run: () =>
                Effect.gen(function* () {
                  const execution = yield* TaskExecution
                  for (const key of ['first', 'second']) {
                    const request: Subagent.Request = {
                      draft:
                        work === 'write'
                          ? {
                              type: 'write',
                              entry: {
                                kind: 'input',
                                model,
                                data: {
                                  opaque: { type: 'file', mediaType: 'image/png', data: null },
                                },
                              },
                            }
                          : { type: 'input', content: [image] },
                      options: { key, mode: 'background' },
                    }
                    requests.push(request)
                    const handle = yield* Subagent.spawn(execution, request)
                    assert.strictEqual(
                      (yield* Subagent.wait(execution, handle)).outcome.status,
                      'completed',
                    )
                    // Matching the saved replay request must return the same supervisor, without new work.
                    const replay = yield* Subagent.spawn(execution, request)
                    assert.strictEqual(replay.taskId, handle.taskId)
                  }
                  yield* Deferred.succeed(ready, undefined)
                  return yield* Effect.never
                }),
            },
          })
          const registry = yield* Registry.make
          yield* registry.install(
            Registry.withTask(
              Registry.toolkit({ id: 'media/held', version: 1 }, Toolkit.make()),
              definition,
            ),
          )
          let calls = 0
          const provider = yield* Script.make({
            generateText: () =>
              Effect.sync(() => {
                calls++
                return [
                  { type: 'text' as const, text: 'done' },
                  {
                    type: 'finish' as const,
                    reason: 'stop' as const,
                    usage: {
                      inputTokens: {
                        total: 1,
                        uncached: 1,
                        cacheRead: undefined,
                        cacheWrite: undefined,
                      },
                      outputTokens: { total: 1, text: 1, reasoning: undefined },
                    },
                    response: undefined,
                  },
                ]
              }),
          })
          const runtime = yield* HarnessRuntime.make({ registry }).pipe(
            Effect.provideContext(context),
            Effect.provideService(LanguageModel.LanguageModel, provider),
          )
          const parent = yield* runtime.scheduler.start(definition, undefined, {
            conversationId: yield* runtime.backend.root,
          })
          yield* Deferred.await(ready)
          const state = yield* access.snapshot
          const slots = []
          const supervisors = []
          for (const physical of state.rows.values()) {
            const row = yield* MediaRow.skeleton(physical)
            if (row._tag === 'document' && row.value.record.kind === 'harness.subagent.slots')
              slots.push(physical)
            if (row._tag === 'task' && row.value.kind === 'harness.subagent')
              supervisors.push(physical)
          }
          assert.strictEqual(slots.length, 1)
          assert.deepEqual(
            supervisors.map((row) => MediaRow.references(row).length),
            [1, 1],
          )
          assert.strictEqual(state.objects.size, 1)
          assert.strictEqual(MediaRow.references(slots[0]!).length, 2)
          assert.isBelow(JSON.stringify(slots[0]).length, 8192)
          const replay = Option.getOrThrow(
            yield* Session.snapshot(runtime.session, Invocation.index, {
              scope: { _tag: 'task', taskId: parent.id },
            }),
          )
          assert.deepEqual(
            Object.values(replay.value.slots).map((slot) => slot.request),
            requests,
          )
          assert.strictEqual(calls, work === 'input' ? 2 : 0)
        }),
      ),
  )
