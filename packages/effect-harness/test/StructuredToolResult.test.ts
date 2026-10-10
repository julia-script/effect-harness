import { NodeServices } from '@effect/platform-node'
import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Hook from 'effect-harness/Hook'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Tool from 'effect-harness/Tool'
import * as ToolResult from 'effect-harness/ToolResult'
import * as Toolkit from 'effect-harness/Toolkit'
import * as OpenAiResult from 'effect-harness/provider-openai/ToolResult'
import * as AnthropicResult from 'effect-harness/provider-anthropic/ToolResult'

const finish = {
  type: 'finish' as const,
  reason: 'stop' as const,
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 1, text: 1, reasoning: undefined },
  },
  response: undefined,
}
const text = (value: string) => [Prompt.textPart({ text: value })]
const encodedResult = Schema.toCodecJson(ToolResult.ResultSchema)
const toolkit = Toolkit.make(
  Tool.makeResult('work', {
    success: Schema.Struct({ count: Schema.Finite }),
    failure: Schema.Struct({ reason: Schema.NonEmptyString }),
    replay: 'safe',
  }),
)

const execute = <Tools extends Record<string, Tool.Any>>(
  tools: Toolkit.Toolkit<Tools>,
  hooks: ReadonlyArray<
    Hook.Hook<'afterTool', unknown, never> | Hook.Hook<'afterTools', unknown, never>
  > = [],
) =>
  Effect.gen(function* () {
    let observed: unknown
    const model = yield* LanguageModel.make({
      generateText: ({ prompt }) => {
        const result = prompt.content
          .flatMap((message) => (message.role === 'tool' ? message.content : []))
          .find((part) => part.type === 'tool-result')
        if (result !== undefined) {
          observed = result.result
          return Effect.succeed([{ type: 'text', text: 'done' }, finish])
        }
        return Effect.succeed([
          { type: 'tool-call', id: 'call-1', name: 'work', params: {}, providerExecuted: false },
          { ...finish, reason: 'tool-calls' as const },
        ])
      },
      streamText: () => Stream.empty,
    })
    const runtime = yield* HarnessRuntime.make({ tools, hooks }).pipe(
      Effect.provideService(LanguageModel.LanguageModel, model),
    )
    const root = yield* runtime.backend.root
    const job = yield* runtime.backend.submit({
      conversationId: root,
      draft: { type: 'input', content: 'go' },
    })
    const settled = yield* runtime.backend.wait(job.id)
    const entries = yield* Session.scanEntries(runtime.session, { conversationId: root }).pipe(
      Stream.runCollect,
    )
    return { observed, settled, persisted: entries.find((entry) => entry.kind === 'tool.result') }
  })
const memory = <A, E, R>(program: Effect.Effect<A, E, R>) =>
  Effect.scoped(program).pipe(Effect.provide(Storage.layerMemory))

describe('independent tool result channels', () => {
  it.effect(
    'hooks and durable receipts retain data while the model receives selected content',
    () =>
      memory(
        Effect.gen(function* () {
          let receipt: ToolResult.Result | undefined
          let completed: ReadonlyArray<ToolResult.Result> = []
          const hooks = [
            Hook.make({
              event: 'afterTool',
              execute: ({ result }) =>
                Effect.sync(() => {
                  receipt = result
                  return { ...result, content: text('redacted caption') }
                }),
            }),
            Hook.make({
              event: 'afterTools',
              execute: ({ results }) =>
                Effect.sync(() => {
                  completed = results
                }),
            }),
          ]
          const { observed, settled, persisted } = yield* execute(toolkit, hooks).pipe(
            Effect.provide(
              toolkit.toLayer({
                work: () =>
                  Effect.succeed({
                    content: text('private caption'),
                    structuredOutput: { count: 7 },
                    details: { ui: 'private panel' },
                  }),
              }),
            ),
          )
          assert.strictEqual(settled.status, 'done')
          assert.strictEqual(observed, 'redacted caption')
          assert.deepEqual(receipt?.structuredOutput, { count: 7 })
          assert.deepEqual(receipt?.details, { ui: 'private panel' })
          const restored = yield* Schema.decodeUnknownEffect(encodedResult)(persisted?.data)
          assert.deepEqual(restored, completed[0])
          assert.deepEqual(restored.structuredOutput, { count: 7 })
          assert.deepEqual(restored.details, { ui: 'private panel' })
          assert.deepEqual(restored.content, text('redacted caption'))
          assert.notInclude(JSON.stringify(persisted?.model), 'private panel')
          assert.notInclude(JSON.stringify(persisted?.model), 'count')
        }),
      ),
  )

  it.effect('declared failures choose independent visible, structured and UI values', () =>
    memory(
      Effect.gen(function* () {
        const { observed, settled, persisted } = yield* execute(toolkit).pipe(
          Effect.provide(
            toolkit.toLayer({
              work: () =>
                Effect.fail({
                  content: text('Please try again'),
                  structuredOutput: { reason: 'private domain reason' },
                  details: { ui: 'retry button' },
                }),
            }),
          ),
        )
        assert.strictEqual(settled.status, 'done')
        assert.strictEqual(observed, 'Please try again')
        const result = yield* Schema.decodeUnknownEffect(encodedResult)(persisted?.data)
        assert.isTrue(result.isError)
        assert.deepEqual(result.structuredOutput, { reason: 'private domain reason' })
        assert.deepEqual(result.details, { ui: 'retry button' })
      }),
    ),
  )

  for (const invalid of ['success', 'failure', 'hook'] as const) {
    it.effect(`rejects invalid ${invalid} structured output before durable publication`, () =>
      memory(
        Effect.gen(function* () {
          const hook = Hook.make({
            event: 'afterTool',
            execute: ({ result }) =>
              Effect.succeed({ ...result, structuredOutput: { count: 'bad' } }),
          })
          const { observed, settled, persisted } = yield* execute(
            toolkit,
            invalid === 'hook' ? [hook] : [],
          ).pipe(
            Effect.provide(
              toolkit.toLayer({
                work: () =>
                  invalid === 'failure'
                    ? Effect.fail({ content: text('failure'), structuredOutput: { reason: '' } })
                    : Effect.succeed({
                        content: text('ok'),
                        structuredOutput: { count: invalid === 'success' ? Number.NaN : 1 },
                      }),
              }),
            ),
          )
          assert.strictEqual(settled.status, 'unanswered')
          assert.isUndefined(observed)
          assert.isUndefined(persisted)
        }),
      ),
    )
  }

  it.effect('omission stays absent even with a declared schema', () =>
    memory(
      Effect.gen(function* () {
        const omitted = yield* Effect.scoped(execute(toolkit)).pipe(
          Effect.provide(
            toolkit.toLayer({ work: () => Effect.succeed({ content: text('only content') }) }),
          ),
        )
        assert.strictEqual(omitted.settled.status, 'done')
        const absent = yield* Schema.decodeUnknownEffect(encodedResult)(omitted.persisted?.data)
        assert.isFalse(Object.hasOwn(absent, 'structuredOutput'))
      }),
    ),
  )

  it.effect('schema-free results accept JSON null without synthesizing visible content', () =>
    memory(
      Effect.gen(function* () {
        const jsonTools = Toolkit.make(Tool.makeResult('work'))
        const fallback = yield* Effect.scoped(execute(jsonTools)).pipe(
          Effect.provide(
            jsonTools.toLayer({
              work: () => Effect.succeed({ content: [], structuredOutput: null }),
            }),
          ),
        )
        assert.strictEqual(fallback.settled.status, 'done')
        assert.strictEqual(fallback.observed, '')
        const present = yield* Schema.decodeUnknownEffect(encodedResult)(fallback.persisted?.data)
        assert.isTrue(Object.hasOwn(present, 'structuredOutput'))
        assert.isNull(present.structuredOutput)
      }),
    ),
  )

  it.effect('hooks can replace structured data and UI details', () =>
    memory(
      Effect.gen(function* () {
        const hook = Hook.make({
          event: 'afterTool',
          execute: ({ result }) =>
            Effect.succeed({
              ...result,
              structuredOutput: { count: 9 },
              details: { panel: 'replacement' },
            }),
        })
        const { settled, persisted } = yield* execute(toolkit, [hook]).pipe(
          Effect.provide(
            toolkit.toLayer({
              work: () =>
                Effect.succeed({ content: text('visible'), structuredOutput: { count: 1 } }),
            }),
          ),
        )
        assert.strictEqual(settled.status, 'done')
        const result = yield* Schema.decodeUnknownEffect(encodedResult)(persisted?.data)
        assert.deepEqual(result.structuredOutput, { count: 9 })
        assert.deepEqual(result.details, { panel: 'replacement' })
      }),
    ),
  )

  it.effect('UI details shaped like a legacy content envelope remain private', () =>
    memory(
      Effect.gen(function* () {
        const privateEnvelope = yield* Schema.encodeEffect(Schema.toCodecJson(ToolResult.Envelope))(
          { _tag: '@effect-harness/ToolContent', content: text('private UI content') },
        )
        const { observed, settled, persisted } = yield* execute(toolkit).pipe(
          Effect.provide(
            toolkit.toLayer({
              work: () =>
                Effect.succeed({
                  content: text('visible result'),
                  structuredOutput: { count: 1 },
                  details: privateEnvelope,
                }),
            }),
          ),
        )
        assert.strictEqual(settled.status, 'done')
        assert.strictEqual(observed, 'visible result')
        const result = yield* Schema.decodeUnknownEffect(encodedResult)(persisted?.data)
        assert.deepEqual(result.details, privateEnvelope)
      }),
    ),
  )

  it.effect('a hook can remove structured output and details with a full replacement', () =>
    memory(
      Effect.gen(function* () {
        const hook = Hook.make({
          event: 'afterTool',
          execute: () =>
            Effect.succeed({ content: text('redacted'), isError: false, diagnostics: [] }),
        })
        const { settled, persisted } = yield* execute(toolkit, [hook]).pipe(
          Effect.provide(
            toolkit.toLayer({
              work: () =>
                Effect.succeed({
                  content: text('private'),
                  structuredOutput: { count: 1 },
                  details: 'private',
                }),
            }),
          ),
        )
        assert.strictEqual(settled.status, 'done')
        const result = yield* Schema.decodeUnknownEffect(encodedResult)(persisted?.data)
        assert.isFalse(Object.hasOwn(result, 'structuredOutput'))
        assert.isFalse(Object.hasOwn(result, 'details'))
      }),
    ),
  )

  it.effect('codec services and transformed structured values survive JSON recovery', () =>
    memory(
      Effect.gen(function* () {
        class Codec extends Context.Service<Codec, string>()('test/StructuredToolResult/Codec') {}
        const schema = Schema.Struct({ instant: Schema.DateFromString }).pipe(
          Schema.middlewareEncoding((effect) => Effect.flatMap(Codec, () => effect)),
          Schema.middlewareDecoding((effect) => Effect.flatMap(Codec, () => effect)),
        )
        const tools = Toolkit.make(Tool.makeResult('work', { success: schema }))
        const instant = new Date('2026-01-01T00:00:00.000Z')
        const result = yield* execute(tools).pipe(
          Effect.provide(
            tools.toLayer({
              work: () => Effect.succeed({ content: text('date'), structuredOutput: { instant } }),
            }),
          ),
          Effect.provideService(Codec, 'codec'),
        )
        assert.strictEqual(result.settled.status, 'done')
        const restored = yield* Schema.decodeUnknownEffect(encodedResult)(result.persisted?.data)
        const decoded = yield* Schema.decodeUnknownEffect(Schema.toCodecJson(schema))(
          restored.structuredOutput,
        ).pipe(Effect.provideService(Codec, 'codec'))
        assert.deepEqual(decoded, { instant })
      }),
    ),
  )

  it.effect('byte media and all channels survive a committed checkpoint and JSONL reopen', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'structured-tool-result-' })
        const filePath = `${directory}/history.jsonl`
        const png = new Uint8Array([1, 2, 3])
        const visible = [
          ...text('public media'),
          Prompt.filePart({
            mediaType: 'image/png',
            data: png,
            options: {
              openai: { imageDetail: 'high' },
              anthropic: { cacheControl: { type: 'ephemeral' } },
            },
          }),
          Prompt.filePart({ mediaType: 'application/pdf', data: new Uint8Array([65, 66]) }),
        ]
        const entered = yield* Deferred.make<void>()
        const firstScope = yield* Scope.make()
        yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
        let calls = 0
        let observed: unknown
        const model = yield* LanguageModel.make({
          generateText: () =>
            Effect.succeed([
              {
                type: 'tool-call',
                id: 'call-1',
                name: 'work',
                params: {},
                providerExecuted: false,
              },
              { ...finish, reason: 'tool-calls' as const },
            ]),
          streamText: () => Stream.empty,
        })
        const handlers = toolkit.toLayer({
          work: () =>
            Effect.sync(() => {
              calls++
              return {
                content: text('before hook'),
                structuredOutput: { count: 3 },
                details: { panel: 'secret' },
              }
            }),
        })
        const hook = Hook.make({
          event: 'afterTool',
          execute: ({ result }) => Effect.succeed({ ...result, content: visible }),
        })
        const pause = Hook.make({
          event: 'afterTools',
          execute: () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
        })
        const firstStorage = yield* Layer.buildWithScope(
          Storage.layerJsonl({ filePath }),
          firstScope,
        )
        const first = yield* HarnessRuntime.make({ tools: toolkit, hooks: [hook, pause] }).pipe(
          Effect.provide(handlers),
          Effect.provideService(LanguageModel.LanguageModel, model),
          Effect.provideContext(firstStorage),
          Effect.provideService(Scope.Scope, firstScope),
        )
        const root = yield* first.backend.root
        const job = yield* first.backend.submit({
          conversationId: root,
          draft: { type: 'input', content: 'go' },
        })
        yield* Deferred.await(entered)
        yield* Scope.close(firstScope, Exit.void)
        const recoveredModel = yield* LanguageModel.make({
          generateText: ({ prompt }) => {
            const result = prompt.content
              .flatMap((message) => (message.role === 'tool' ? message.content : []))
              .find((part) => part.type === 'tool-result')
            observed = result?.result
            return Effect.succeed([{ type: 'text', text: 'done' }, finish])
          },
          streamText: () => Stream.empty,
        })
        let recoveredResults: ReadonlyArray<ToolResult.Result> = []
        const recoveredHook = Hook.make({
          event: 'afterTools',
          execute: ({ results }) =>
            Effect.sync(() => {
              recoveredResults = results
            }),
        })
        const secondStorage = yield* Layer.build(Storage.layerJsonl({ filePath }))
        const second = yield* HarnessRuntime.make({ tools: toolkit, hooks: [recoveredHook] }).pipe(
          Effect.provide(handlers),
          Effect.provideService(LanguageModel.LanguageModel, recoveredModel),
          Effect.provideContext(secondStorage),
        )
        yield* second.backend.root
        assert.strictEqual((yield* second.backend.wait(job.id)).status, 'done')
        assert.strictEqual(calls, 1)
        const entries = yield* Session.scanEntries(second.session, { conversationId: root }).pipe(
          Stream.runCollect,
        )
        const saved = entries.find((entry) => entry.kind === 'tool.result')
        const restored = yield* Schema.decodeUnknownEffect(encodedResult)(saved?.data)
        assert.deepEqual(recoveredResults, [restored])
        assert.deepEqual(restored.content, visible)
        assert.deepEqual(restored.structuredOutput, { count: 3 })
        assert.deepEqual(restored.details, { panel: 'secret' })
        const envelope = yield* Schema.decodeUnknownEffect(Schema.toCodecJson(ToolResult.Envelope))(
          observed,
        )
        assert.deepEqual(envelope.content, visible)
        assert.notInclude(JSON.stringify(observed), 'secret')
        assert.notInclude(JSON.stringify(observed), 'count')
        const openai = yield* OpenAiResult.content(envelope.content)
        assert.include(JSON.stringify(openai), 'AQID')
        const anthropic = yield* AnthropicResult.content(envelope.content)
        assert.include(JSON.stringify(anthropic), 'AQID')
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  )
})
