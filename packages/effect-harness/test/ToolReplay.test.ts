import { assert, describe, it } from '@effect/vitest'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Extension from 'effect-harness/Extension'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Tool from 'effect-harness/Tool'
import * as Toolkit from 'effect-harness/Toolkit'

const finish = {
  type: 'finish' as const,
  reason: 'stop' as const,
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 1, text: 1, reasoning: undefined },
  },
  response: undefined,
}

describe('tool declaration resolution', () => {
  it.effect('an unsafe extension override keeps its policy through restart', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const safeDefinition = Tool.make('work', {
          parameters: Schema.Struct({ text: Schema.String }),
          success: Schema.String,
          replay: 'safe',
        })
        const unsafeDefinition = Tool.make('work', {
          parameters: Schema.Struct({ text: Schema.String }),
          success: Schema.String,
          replay: 'unsafe',
        })
        const safe = Toolkit.make(safeDefinition)
        const unsafe = Toolkit.make(unsafeDefinition)
        assert.strictEqual(
          Toolkit.make(safeDefinition, unsafeDefinition).tools.work,
          unsafeDefinition,
        )
        let safeExecutions = 0
        let unsafeExecutions = 0
        let providerPolicy: Tool.Replay | undefined
        const safeLayer = safe.toLayer({
          work: () =>
            Effect.sync(() => {
              safeExecutions++
              return 'safe'
            }),
        })
        const entered = yield* Deferred.make<void>()
        const model = yield* LanguageModel.make({
          generateText: ({ prompt, tools }) => {
            const result = prompt.content
              .flatMap((m) => (m.role === 'tool' ? m.content : []))
              .find((p) => p.type === 'tool-result')
            if (result !== undefined)
              return Effect.succeed([{ type: 'text', text: 'done' }, finish])
            const declaration = tools.find((tool) => tool.name === 'work')
            assert.strictEqual(tools.filter((tool) => tool.name === 'work').length, 1)
            assert.isDefined(declaration)
            providerPolicy = Tool.policy(declaration as Tool.Any).replay
            return Effect.succeed([
              {
                type: 'tool-call',
                id: 'call-1',
                name: 'work',
                params: { text: 'hello' },
                providerExecuted: false,
              },
              { ...finish, reason: 'tool-calls' as const },
            ])
          },
          streamText: () => Stream.empty,
        })
        const firstScope = yield* Scope.make()
        yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
        const firstExtension = Extension.make({ name: 'unsafe-ext', tools: unsafe }).pipe(
          Extension.provide(
            unsafe.toLayer({
              work: () =>
                Effect.gen(function* () {
                  unsafeExecutions++
                  yield* Deferred.succeed(entered, undefined)
                  return yield* Effect.never
                }),
            }),
          ),
        )
        const first = yield* HarnessRuntime.make({
          tools: safe,
          extensions: [firstExtension],
        }).pipe(
          Effect.provideService(LanguageModel.LanguageModel, model),
          Effect.provideService(Scope.Scope, firstScope),
          Effect.provide(safeLayer),
        )
        const root = yield* first.backend.root
        const job = yield* first.backend.submit({
          conversationId: root,
          draft: { type: 'input', content: 'go' },
        })
        yield* Deferred.await(entered)
        const tasks = yield* Stream.runCollect(Session.scanTasks(first.session, {}))
        const checkpoint = tasks.find((t) => t.state.status !== 'terminal')?.state.checkpoint
        assert.include(JSON.stringify(checkpoint), '"replay":"unsafe"')
        assert.include(JSON.stringify(checkpoint), '"started":true')
        assert.strictEqual(providerPolicy, 'unsafe')
        assert.strictEqual(safeExecutions, 0)
        assert.strictEqual(unsafeExecutions, 1)
        yield* Scope.close(firstScope, Exit.void)
        const secondExtension = Extension.make({ name: 'unsafe-ext', tools: unsafe }).pipe(
          Extension.provide(
            unsafe.toLayer({
              work: () =>
                Effect.sync(() => {
                  unsafeExecutions++
                  return 'unsafe completed'
                }),
            }),
          ),
        )
        const second = yield* HarnessRuntime.make({
          tools: safe,
          extensions: [secondExtension],
        }).pipe(
          Effect.provideService(LanguageModel.LanguageModel, model),
          Effect.provide(safeLayer),
        )
        yield* second.backend.root
        const settled = yield* second.backend.wait(job.id)
        assert.strictEqual(settled.status, 'done')
        assert.strictEqual(safeExecutions, 0)
        assert.strictEqual(
          unsafeExecutions,
          1,
          JSON.stringify({
            providerPolicy,
            checkpoint,
            safeExecutions,
            unsafeExecutions,
            settlement: settled.status,
          }),
        )
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
  )
  it.effect('a disabled overriding extension cannot expose an earlier same-name handler', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sharedDefinition = Tool.make('work', { success: Schema.String })
        const toolkit = Toolkit.make(sharedDefinition)
        let executions = 0
        let offeredNames: ReadonlyArray<string> = []
        const handlerLayer = toolkit.toLayer({
          work: () =>
            Effect.sync(() => {
              executions++
              return 'unexpected'
            }),
        })
        const first = Extension.make({ name: 'first', tools: toolkit }).pipe(
          Extension.provide(handlerLayer),
        )
        const last = Extension.make({ name: 'last', tools: toolkit }).pipe(
          Extension.provide(handlerLayer),
        )
        const model = yield* LanguageModel.make({
          generateText: ({ prompt, tools }) => {
            offeredNames = tools.map((tool) => tool.name)
            const result = prompt.content
              .flatMap((message) => (message.role === 'tool' ? message.content : []))
              .find((part) => part.type === 'tool-result')
            if (result !== undefined || tools.length === 0)
              return Effect.succeed([{ type: 'text', text: 'done' }, finish])
            return Effect.succeed([
              {
                type: 'tool-call',
                id: 'call-1',
                name: 'work',
                params: {},
                providerExecuted: false,
              },
              { ...finish, reason: 'tool-calls' as const },
            ])
          },
          streamText: () => Stream.empty,
        })
        const runtime = yield* HarnessRuntime.make({
          tools: toolkit,
          extensions: [first, last],
          agent: { extensions: ['first'] },
        }).pipe(
          Effect.provideService(LanguageModel.LanguageModel, model),
          Effect.provide(handlerLayer),
        )
        const root = yield* runtime.backend.root
        const submission = yield* runtime.backend.submit({
          conversationId: root,
          draft: { type: 'input', content: 'go' },
        })
        assert.strictEqual((yield* runtime.backend.wait(submission.id)).status, 'done')
        assert.deepEqual(offeredNames, [])
        assert.strictEqual(executions, 0)
        const entries = yield* Stream.runCollect(
          Session.scanEntries(runtime.session, { conversationId: root }),
        )
        assert.isFalse(entries.some((entry) => entry.kind === 'tool.result'))
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
  )
})
