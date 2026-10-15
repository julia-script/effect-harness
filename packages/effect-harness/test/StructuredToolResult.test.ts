import * as ToolResultRecord from 'effect-harness/ToolResultRecord'
import * as ScriptedStream from './fixtures/ScriptedStream.js'
import { NodeServices } from '@effect/platform-node'
import { assert, describe, it } from '@effect/vitest'
import * as Clock from 'effect/Clock'
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
import * as Response from 'effect/ai/Response'
import * as Model from 'effect-harness/Model'
import * as Settings from 'effect-harness/Settings'
import * as Prompt from 'effect/ai/Prompt'
import * as ContextProjection from 'effect-harness/ContextProjection'
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
    const model = yield* ScriptedStream.make({
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
  it.effect('empty content still delivers actionable diagnostics to the model', () =>
    memory(
      Effect.gen(function* () {
        const diagnostics: ReadonlyArray<ToolResult.Diagnostic> = [
          { severity: 'warning', code: 'pagination', message: 'Rows truncated; rerun with page=2' },
          { severity: 'info', message: 'Results are ordered by date' },
          { severity: 'error', message: 'One source could not be reached' },
        ]
        const { observed, settled, persisted } = yield* execute(toolkit).pipe(
          Effect.provide(
            toolkit.toLayer({
              work: () =>
                Effect.succeed({
                  content: [],
                  structuredOutput: { count: 7 },
                  details: { panel: 'private' },
                  diagnostics,
                }),
            }),
          ),
        )
        assert.strictEqual(settled.status, 'done')
        assert.strictEqual(
          observed,
          '<harness>\n[warning] Rows truncated; rerun with page=2\n[info] Results are ordered by date\n[error] One source could not be reached\n</harness>',
        )
        const restored = yield* ToolResultRecord.read(persisted!)
        assert.deepEqual(restored.content, [])
        assert.deepEqual(restored.diagnostics, diagnostics)
        assert.deepEqual(restored.structuredOutput, { count: 7 })
        assert.deepEqual(restored.details, { panel: 'private' })
        assert.isUndefined(persisted!.model)
        const snapshot = JSON.stringify(persisted)
        for (let index = 0; index < 3; index++) {
          const messages = yield* ContextProjection.messages(persisted!)
          const part = messages
            .flatMap((message) => (message.role === 'tool' ? message.content : []))
            .find((part) => part.type === 'tool-result')
          assert.strictEqual(part?.result, observed)
          assert.strictEqual(part?.id, 'call-1')
          assert.strictEqual(part?.name, 'work')
          assert.isFalse(part?.isFailure)
        }
        assert.strictEqual(JSON.stringify(persisted), snapshot)
      }),
    ),
  )

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
          const restored = yield* ToolResultRecord.read(persisted!)
          assert.deepEqual(restored, completed[0])
          assert.deepEqual(restored.structuredOutput, { count: 7 })
          assert.deepEqual(restored.details, { ui: 'private panel' })
          assert.deepEqual(restored.content, text('redacted caption'))
          assert.notInclude(
            JSON.stringify(yield* ContextProjection.messages(persisted!)),
            'private panel',
          )
          assert.notInclude(JSON.stringify(yield* ContextProjection.messages(persisted!)), 'count')
        }),
      ),
  )

  it.live('deferred starts and polls receive the same rendered diagnostics', () =>
    memory(
      Effect.gen(function* () {
        let calls = 0
        let starts = 0
        let polls = 0
        let accepted: Prompt.Prompt | undefined
        const adapter = yield* Model.makeDeferred<Schema.String, never>({
          handle: Schema.String,
          start: (request) =>
            Effect.gen(function* () {
              starts++
              const part = request.prompt.content
                .flatMap((message) => (message.role === 'tool' ? message.content : []))
                .find((part) => part.type === 'tool-result')
              if (part === undefined)
                return {
                  _tag: 'Complete' as const,
                  response: new LanguageModel.GenerateTextResponse([
                    Response.makePart('tool-call', {
                      id: 'call-1',
                      name: 'work',
                      params: {},
                      providerExecuted: false,
                    }),
                    Response.makePart('finish', { ...finish, reason: 'tool-calls' }),
                  ]),
                }
              assert.strictEqual(
                part.result,
                '<harness>\n[warning] Rows truncated; rerun with page=2\n</harness>',
              )
              accepted = request.prompt
              return {
                _tag: 'Deferred' as const,
                handle: 'result-request',
                nextPollAt: (yield* Clock.currentTimeMillis) + 1000,
              }
            }),
          poll: (handle, _previous, request) =>
            Effect.sync(() => {
              polls++
              assert.strictEqual(handle, 'result-request')
              assert.deepEqual(request.prompt, accepted)
              return {
                _tag: 'Complete' as const,
                response: new LanguageModel.GenerateTextResponse([
                  Response.makePart('text', { text: 'done' }),
                  Response.makePart('finish', finish),
                ]),
              }
            }),
          cancel: () => Effect.void,
        })
        const native = yield* LanguageModel.make({
          generateText: () => Effect.die('Expected deferred execution'),
          streamText: () => Stream.die('Expected deferred execution'),
        })
        const model = Model.make({
          definition: {
            ref: { provider: 'custom', modelId: 'diagnostics' },
            capabilities: { tools: true, images: false, reasoning: false, structuredOutput: false },
          },
          languageModel: native,
          options: Schema.JsonObject,
          configure: () => Effect.succeed(Context.empty()),
          deferred: adapter,
        })
        const runtime = yield* HarnessRuntime.make({ tools: toolkit, models: [model] }).pipe(
          Effect.provideService(
            Settings.Settings,
            yield* Settings.make({ stream: { deferred: true } }),
          ),
          Effect.provide(
            toolkit.toLayer({
              work: () =>
                Effect.sync(() => {
                  calls++
                  return {
                    content: [],
                    structuredOutput: { count: 7 },
                    diagnostics: [
                      {
                        severity: 'warning' as const,
                        message: 'Rows truncated; rerun with page=2',
                      },
                    ],
                  }
                }),
            }),
          ),
        )
        const root = yield* runtime.backend.root
        const job = yield* runtime.backend.submit({
          conversationId: root,
          draft: { type: 'input', content: 'go' },
        })
        const settled = yield* runtime.backend.wait(job.id)
        assert.strictEqual(settled.status, 'done', JSON.stringify(settled))
        assert.strictEqual(starts, 2)
        assert.strictEqual(polls, 1)
        assert.strictEqual(calls, 1)
      }),
    ),
  )

  it.effect('public reading restores aliases and rejects noncanonical entries', () =>
    memory(
      Effect.gen(function* () {
        const content = [
          Prompt.filePart({ mediaType: 'image/png', data: new Uint8Array([1, 2, 3]) }),
        ]
        const tools = Toolkit.make(Tool.make('work', { success: ToolResult.Envelope }))
        const { persisted, settled } = yield* execute(tools).pipe(
          Effect.provide(
            tools.toLayer({
              work: () => Effect.succeed({ _tag: '@effect-harness/ToolContent' as const, content }),
            }),
          ),
        )
        assert.strictEqual(settled.status, 'done')
        const restored = yield* ToolResultRecord.read(persisted!)
        assert.deepEqual(restored.content, content)
        const details = yield* Schema.decodeUnknownEffect(Schema.toCodecJson(ToolResult.Envelope))(
          restored.details,
        )
        assert.deepEqual(details.content, content)
        assert.strictEqual(JSON.stringify(persisted!.data).split('AQID').length - 1, 1)
        const oldShape = yield* Schema.encodeEffect(Schema.toCodecJson(ToolResult.ResultSchema))(
          restored,
        )
        for (const invalid of [
          { ...persisted!, kind: 'assistant' },
          { ...persisted!, data: oldShape },
          {
            ...persisted!,
            data: { callId: 'call-1', name: 'work', explicitEnvelope: true, result: {} },
          },
        ]) {
          const error = yield* ToolResultRecord.read(invalid).pipe(Effect.flip)
          assert.isTrue(Schema.isSchemaError(error))
        }
      }),
    ),
  )

  for (const variant of ['explicit', 'options'] as const) {
    it.effect(`diagnostics preserve ${variant} text envelopes`, () =>
      memory(
        Effect.gen(function* () {
          const content = [
            Prompt.textPart({
              text: 'caption',
              options:
                variant === 'options' ? { anthropic: { cacheControl: { type: 'ephemeral' } } } : {},
            }),
          ]
          const diagnostics: ReadonlyArray<ToolResult.Diagnostic> = [
            { severity: 'warning', message: 'Try page 2' },
          ]
          const tools = Toolkit.make(Tool.make('work', { success: ToolResult.Envelope }))
          const hook = Hook.make({
            event: 'afterTool',
            execute: ({ result }) => Effect.succeed({ ...result, diagnostics }),
          })
          const { observed, settled, persisted } = yield* execute(tools, [hook]).pipe(
            Effect.provide(
              tools.toLayer({
                work: () =>
                  Effect.succeed({ _tag: '@effect-harness/ToolContent' as const, content }),
              }),
            ),
          )
          assert.strictEqual(settled.status, 'done')
          const envelope = yield* Schema.decodeUnknownEffect(
            Schema.toCodecJson(ToolResult.Envelope),
          )(observed)
          assert.deepEqual(envelope.content, [
            ...content,
            Prompt.textPart({ text: '<harness>\n[warning] Try page 2\n</harness>' }),
          ])
          const restored = yield* ToolResultRecord.read(persisted!)
          assert.deepEqual(restored.content, content)
          assert.deepEqual(restored.diagnostics, diagnostics)
        }),
      ),
    )
  }

  it.effect('model diagnostics survive the tool text retention limit', () =>
    memory(
      Effect.gen(function* () {
        const tools = Toolkit.make(
          Tool.makeResult('work', {
            outputRetention: { retain: 'head', maxBytes: 3, maxLines: 1 },
          }),
        )
        const { observed, settled, persisted } = yield* execute(tools).pipe(
          Effect.provide(
            tools.toLayer({
              work: () =>
                Effect.succeed({
                  content: text('abcdef'),
                  diagnostics: [{ severity: 'warning', message: 'Use page=2' }],
                }),
            }),
          ),
        )
        assert.strictEqual(settled.status, 'done')
        const restored = yield* ToolResultRecord.read(persisted!)
        assert.deepEqual(restored.content, text('abc'))
        assert.deepEqual(
          restored.diagnostics.map((diagnostic) => diagnostic.code),
          [undefined, 'output_truncated'],
        )
        assert.isString(observed)
        assert.include(
          observed as string,
          'abc\n<harness>\n[warning] Use page=2\n[warning] Retained 3 UTF-8 bytes',
        )
        assert.notInclude(observed as string, 'abcdef')
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
                  diagnostics: [
                    { severity: 'error', message: 'Retry after refreshing credentials' },
                  ],
                  structuredOutput: { reason: 'private domain reason' },
                  details: { ui: 'retry button' },
                }),
            }),
          ),
        )
        assert.strictEqual(settled.status, 'done')
        assert.strictEqual(
          observed,
          'Please try again\n<harness>\n[error] Retry after refreshing credentials\n</harness>',
        )
        const messages = yield* ContextProjection.messages(persisted!)
        const part = messages
          .flatMap((message) => (message.role === 'tool' ? message.content : []))
          .find((part) => part.type === 'tool-result')
        assert.isTrue(part?.isFailure)
        const result = yield* ToolResultRecord.read(persisted!)
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
        const absent = yield* ToolResultRecord.read(omitted.persisted!)
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
        const present = yield* ToolResultRecord.read(fallback.persisted!)
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
        const result = yield* ToolResultRecord.read(persisted!)
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
        const result = yield* ToolResultRecord.read(persisted!)
        assert.deepEqual(result.details, privateEnvelope)
      }),
    ),
  )

  it.effect('matching content envelopes preserve additional detail fields', () =>
    memory(
      Effect.gen(function* () {
        const content = text('shared content')
        const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(ToolResult.Envelope))({
          _tag: '@effect-harness/ToolContent',
          content,
        })
        const details = {
          ...(yield* Schema.decodeUnknownEffect(Schema.JsonObject)(encoded)),
          panel: 'private metadata',
        }
        const { observed, settled, persisted } = yield* execute(toolkit).pipe(
          Effect.provide(
            toolkit.toLayer({
              work: () => Effect.succeed({ content, structuredOutput: { count: 1 }, details }),
            }),
          ),
        )
        assert.strictEqual(settled.status, 'done')
        assert.strictEqual(observed, 'shared content')
        assert.deepStrictEqual((yield* ToolResultRecord.read(persisted!)).details, details)
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
        const result = yield* ToolResultRecord.read(persisted!)
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
        const restored = yield* ToolResultRecord.read(result.persisted!)
        const decoded = yield* Schema.decodeUnknownEffect(Schema.toCodecJson(schema))(
          restored.structuredOutput,
        ).pipe(Effect.provideService(Codec, 'codec'))
        assert.deepEqual(decoded, { instant })
      }),
    ),
  )

  it.effect('native media aliases survive JSONL checkpoint recovery', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'structured-tool-result-' })
        const filePath = `${directory}/history.jsonl`
        const diagnostics: ReadonlyArray<ToolResult.Diagnostic> = [
          { severity: 'warning', message: 'Rows truncated; rerun with page=2' },
        ]
        const rendered = Prompt.textPart({
          text: '<harness>\n[warning] Rows truncated; rerun with page=2\n</harness>',
        })
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
          Prompt.filePart({
            mediaType: 'image/png',
            data: new URL('https://example.com/aliased.png'),
          }),
        ]
        const aliasedDetails = yield* Schema.encodeEffect(Schema.toCodecJson(ToolResult.Envelope))({
          _tag: '@effect-harness/ToolContent',
          content: visible,
        })
        const entered = yield* Deferred.make<void>()
        const firstScope = yield* Scope.make()
        yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
        let calls = 0
        let observed: unknown
        const model = yield* ScriptedStream.make({
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
        })
        const handlers = toolkit.toLayer({
          work: () =>
            Effect.sync(() => {
              calls++
              return {
                content: text('before hook'),
                structuredOutput: { count: 3 },
                details: aliasedDetails,
                diagnostics,
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
        const recoveredModel = yield* ScriptedStream.make({
          generateText: ({ prompt }) => {
            const result = prompt.content
              .flatMap((message) => (message.role === 'tool' ? message.content : []))
              .find((part) => part.type === 'tool-result')
            observed = result?.result
            return Effect.succeed([{ type: 'text', text: 'done' }, finish])
          },
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
        const second = yield* HarnessRuntime.make({
          tools: toolkit,
          hooks: [recoveredHook],
        }).pipe(
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
        const restored = yield* ToolResultRecord.read(saved!)
        assert.deepEqual(recoveredResults, [restored])
        assert.deepEqual(restored.content, visible)
        assert.deepEqual(restored.structuredOutput, { count: 3 })
        assert.deepEqual(restored.details, aliasedDetails)
        assert.isFalse(Object.hasOwn(restored, 'detailsFromContent'))
        assert.strictEqual(JSON.stringify(saved!.data).split('aliased.png').length - 1, 1)
        const envelope = yield* Schema.decodeUnknownEffect(Schema.toCodecJson(ToolResult.Envelope))(
          observed,
        )
        assert.deepEqual(envelope.content, [...visible, rendered])
        assert.deepEqual(restored.diagnostics, diagnostics)
        assert.isUndefined(saved!.model)
        assert.strictEqual(
          JSON.stringify(saved!.data).split('Rows truncated; rerun with page=2').length - 1,
          1,
        )
        for (let index = 0; index < 3; index++) {
          const messages = yield* ContextProjection.messages(saved!)
          const part = messages
            .flatMap((message) => (message.role === 'tool' ? message.content : []))
            .find((part) => part.type === 'tool-result')
          assert.deepEqual(part?.result, observed)
        }
        assert.notInclude(JSON.stringify(observed), 'count')
        const openai = yield* OpenAiResult.content(envelope.content)
        assert.include(JSON.stringify(openai), 'AQID')
        assert.include(JSON.stringify(openai), 'https://example.com/aliased.png')
        const anthropic = yield* AnthropicResult.content(envelope.content)
        assert.include(JSON.stringify(anthropic), 'AQID')
        assert.include(JSON.stringify(anthropic), 'https://example.com/aliased.png')
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  )
})
