import * as NodeRuntime from '@effect/platform-node/NodeRuntime'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Tool from 'effect-harness/Tool'
import { ToolExecution } from 'effect-harness/ToolExecution'
import * as Toolkit from 'effect-harness/Toolkit'

// After building: node apps/example/dist/tour/DurableToolRound.js
NodeRuntime.runMain(
  Effect.scoped(
    Effect.gen(function* () {
      const firstEntered = yield* Deferred.make<void>()
      const secondEntered = yield* Deferred.make<void>()
      const childIds: number[] = []
      const toolkit = Toolkit.make(
        Tool.makeResult('first', {
          success: Schema.String,
          executionMode: 'parallel',
          replay: 'safe',
        }),
        Tool.makeResult('second', {
          success: Schema.String,
          executionMode: 'parallel',
          replay: 'safe',
        }),
      )
      const model = yield* LanguageModel.make({
        generateText: () => Effect.die('This example uses the native stream callback'),
        streamText: () =>
          Stream.fromIterable([
            { type: 'tool-call', id: 'one', name: 'first', params: {}, providerExecuted: false },
            { type: 'tool-call', id: 'two', name: 'second', params: {}, providerExecuted: false },
            {
              type: 'finish',
              reason: 'tool-calls',
              usage: {
                inputTokens: { total: 1, uncached: 1, cacheRead: undefined, cacheWrite: undefined },
                outputTokens: { total: 1, text: 1, reasoning: undefined },
              },
              response: undefined,
            },
          ]),
      })
      const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
        Effect.provideService(LanguageModel.LanguageModel, model),
        Effect.provide(
          toolkit.toLayer({
            first: () =>
              Effect.gen(function* () {
                const access = yield* ToolExecution
                childIds.push(access.taskId)
                yield* access.output('first entered\n')
                yield* Deferred.succeed(firstEntered, undefined)
                yield* Deferred.await(secondEntered)
                return {
                  content: [Prompt.textPart({ text: 'first complete' })],
                  structuredOutput: 'private first value',
                  control: { terminate: true },
                }
              }),
            second: () =>
              Effect.gen(function* () {
                const access = yield* ToolExecution
                childIds.push(access.taskId)
                yield* Deferred.succeed(secondEntered, undefined)
                yield* Deferred.await(firstEntered)
                return {
                  content: [Prompt.textPart({ text: 'second complete' })],
                  structuredOutput: 'private second value',
                  control: { terminate: true },
                }
              }),
          }),
        ),
      )
      const root = yield* runtime.backend.root
      const submission = yield* runtime.backend.submit({
        conversationId: root,
        draft: { type: 'input', content: 'Run both tools' },
      })
      const result = yield* runtime.backend.wait(submission.id)
      const tasks = yield* Session.scanTasks(runtime.session).pipe(Stream.runCollect)
      yield* Effect.log({
        status: result.status,
        childIds,
        terminalChildren: tasks.filter(
          (task) => task.kind === 'harness.tool' && task.state.status === 'terminal',
        ).length,
      })
    }),
  ).pipe(Effect.provide(Storage.layerMemory)),
)
