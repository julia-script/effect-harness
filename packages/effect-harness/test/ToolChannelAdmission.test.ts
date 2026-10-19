import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as Registry from '../src/Registry.js'
import * as Tool from '../src/Tool.js'
import * as Toolkit from '../src/Toolkit.js'
import * as ToolInventory from '../src/ToolInventory.js'
import * as ToolResult from '../src/ToolResult.js'
import * as Record from '../src/Record.js'
import * as ProgressPolicy from '../src/ProgressPolicy.js'
import * as OutputRetention from '../src/OutputRetention.js'
import { execution } from './fixtures/Registry.js'

class Codec extends Context.Service<Codec, string>()('test/channel-admission/Codec') {}
class HandlerService extends Context.Service<HandlerService, string>()(
  'test/channel-admission/Handler',
) {}

const call = { id: 'channel-call', name: 'work', arguments: {} } as const
const media = Prompt.filePart({
  mediaType: 'image/png',
  data: new Uint8Array([1, 2, 3]),
  options: { openai: { imageDetail: 'high' } },
})

describe('leased strict channels', () => {
  for (const caller of ['model', 'tools'] as const) {
    for (const strict of [false, true]) {
      it.effect(
        `${caller} caller validates ${strict ? 'strict' : 'optional'} data with captured codec services`,
        () =>
          Effect.scoped(
            Effect.gen(function* () {
              let codecs = 0
              let calls = 0
              const value = Schema.DateFromString.pipe(
                Schema.middlewareEncoding((effect) =>
                  Effect.flatMap(Codec, (label) => {
                    assert.strictEqual(label, 'owner')
                    codecs++
                    return effect
                  }),
                ),
                Schema.middlewareDecoding((effect) =>
                  Effect.flatMap(Codec, (label) => {
                    assert.strictEqual(label, 'owner')
                    codecs++
                    return effect
                  }),
                ),
              )
              const tool = strict
                ? Tool.makeResult('work', { success: value, resultValidation: 'strict' })
                : Tool.makeResult('work', { success: value })
              const toolkit = Toolkit.make(tool.addDependency(HandlerService))
              const registry = yield* Registry.make
              yield* registry.install(
                Registry.provide(
                  Registry.toolkit({ id: 'owner', version: 1 }, toolkit),
                  toolkit
                    .toLayer({
                      work: () =>
                        Effect.gen(function* () {
                          const label = yield* HandlerService
                          assert.strictEqual(label, 'bound')
                          calls++
                          return {
                            content: [Prompt.textPart({ text: 'public' }), media],
                            structuredOutput: new Date('2026-01-01T00:00:00.000Z'),
                            details: { panel: 'private' },
                            control: { terminate: true as const, addTools: ['work'] },
                          }
                        }),
                    })
                    .pipe(
                      Layer.provideMerge(
                        Layer.merge(
                          Layer.succeed(Codec, 'owner'),
                          Layer.succeed(HandlerService, 'bound'),
                        ),
                      ),
                    ),
                ),
              )
              const lease = yield* registry.acquire
              const result = yield* ToolInventory.invoke(lease, call, caller)
              assert.strictEqual(calls, 1)
              assert.isAtLeast(codecs, 2)
              assert.strictEqual(result.structuredOutput, '2026-01-01T00:00:00.000Z')
              assert.deepStrictEqual(result.content, [Prompt.textPart({ text: 'public' }), media])
              if (caller === 'model')
                assert.deepStrictEqual(result.control, { terminate: true, addTools: ['work'] })
              else assert.notProperty(result, 'control')
              const nested = yield* ToolInventory.invokeNested(lease, call)
              assert.notProperty(nested, 'content')
              assert.notProperty(nested, 'control')
              assert.strictEqual(nested.structuredOutput, '2026-01-01T00:00:00.000Z')
              assert.deepStrictEqual(nested.details, { panel: 'private' })
            }),
          ).pipe(
            Effect.provide(execution),
            Effect.provideService(Codec, 'ambient'),
            Effect.provideService(HandlerService, 'ambient'),
          ),
      )
    }
  }

  it.effect(
    'optional omission and declared error omission stay absent while no-schema nested results fall back',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const optional = Tool.makeResult('optional', { success: Schema.String })
          const strictError = Tool.makeResult('strictError', {
            success: Schema.String,
            resultValidation: 'strict',
          })
          const fallback = Tool.makeResult('fallback')
          const toolkit = Toolkit.make(optional, strictError, fallback)
          const registry = yield* Registry.make
          yield* registry.install(
            Registry.provide(
              Registry.toolkit({ id: 'owner', version: 1 }, toolkit),
              toolkit.toLayer({
                optional: () => Effect.succeed({ content: [] }),
                strictError: () =>
                  Effect.fail({ content: [Prompt.textPart({ text: 'declared error' })] }),
                fallback: () => Effect.succeed({ content: [media] }),
              }),
            ),
          )
          const lease = yield* registry.acquire
          assert.notProperty(
            yield* ToolInventory.invokeNested(lease, { ...call, name: 'optional' }),
            'structuredOutput',
          )
          const failed = yield* ToolInventory.invokeNested(lease, { ...call, name: 'strictError' })
          assert.isTrue(failed.isError)
          assert.notProperty(failed, 'structuredOutput')
          const fallbackResult = yield* ToolInventory.invokeNested(lease, {
            ...call,
            name: 'fallback',
          })
          assert.deepStrictEqual(fallbackResult.structuredOutput, media)
          assert.strictEqual(fallbackResult.valueKind, 'content')
        }),
      ).pipe(Effect.provide(execution)),
  )

  it.effect(
    'argument/schema denial precedes handlers; legacy value-style tools keep content/details',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          let calls = 0
          const tool = Tool.make('work', {
            parameters: Schema.Struct({ count: Schema.Int.check(Schema.isGreaterThan(0)) }),
            success: Schema.String,
          })
          const toolkit = Toolkit.make(tool)
          const registry = yield* Registry.make
          yield* registry.install(
            Registry.provide(
              Registry.toolkit({ id: 'owner', version: 1 }, toolkit),
              toolkit.toLayer({
                work: ({ count }) =>
                  Effect.sync(() => {
                    calls++
                    return String(count)
                  }),
              }),
            ),
          )
          const lease = yield* registry.acquire
          for (const caller of ['model', 'tools'] as const) {
            assert.isTrue(
              Schema.isSchemaError(
                yield* ToolInventory.invoke(
                  lease,
                  { ...call, arguments: { count: 0 } },
                  caller,
                ).pipe(Effect.flip),
              ),
            )
          }
          assert.strictEqual(calls, 0)
          const value = yield* ToolInventory.invoke(
            lease,
            { ...call, arguments: { count: 1 } },
            'model',
          )
          assert.strictEqual(value.details, '1')
          assert.deepStrictEqual(value.content, [Prompt.textPart({ text: '1' })])
          assert.notProperty(value, 'structuredOutput')
        }),
      ).pipe(Effect.provide(execution)),
  )

  it.effect(
    'legacy intents default model; policies copy supplied declarations without runtime activation',
    () =>
      Effect.gen(function* () {
        const intent = yield* Schema.decodeEffect(Tool.IntentSchema)({
          conversationId: yield* Schema.decodeEffect(Record.ConversationId)(1),
          taskId: yield* Schema.decodeEffect(Record.TaskId)(1),
          call,
          replay: 'safe',
        })
        assert.strictEqual(Tool.caller(intent), 'model')
        assert.strictEqual(Tool.caller({ ...intent, caller: 'tools' }), 'tools')
        const progress = { ...ProgressPolicy.defaultPolicy, minInterval: 0 }
        const retention = { ...OutputRetention.defaultPolicy, maxBytes: 2 }
        const callers: Tool.Caller[] = ['tools']
        const tool = Tool.make('policy', { progress, outputRetention: retention, callers })
        progress.minInterval = 1000
        retention.maxBytes = 1000
        callers.push('model')
        assert.strictEqual(Tool.progress(tool).minInterval, 0)
        assert.strictEqual(Tool.outputRetention(tool).maxBytes, 2)
        assert.deepStrictEqual(Tool.callers(tool), ['tools'])
        assert.deepStrictEqual(Tool.progress(Tool.make('default')), ProgressPolicy.defaultPolicy)
        assert.deepStrictEqual(
          Tool.outputRetention(Tool.make('default')),
          OutputRetention.defaultPolicy,
        )
        const fileShaped = {
          type: 'file',
          data: { _tag: '@effect-harness/Bytes', data: 'AQID' },
          mediaType: 'image/png',
          options: {},
        }
        const projected = ToolResult.nested(
          { content: [], isError: false, structuredOutput: fileShaped, diagnostics: [] },
          true,
        )
        const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(ToolResult.NestedSchema))(
          projected,
        )
        assert.deepStrictEqual(
          yield* Schema.decodeEffect(Schema.toCodecJson(ToolResult.NestedSchema))(encoded),
          projected,
        )
      }),
  )
})
