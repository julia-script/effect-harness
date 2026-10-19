import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Result from 'effect/Result'
import * as Prompt from 'effect/ai/Prompt'
import * as Tool from '../src/Tool.js'
import * as Toolkit from '../src/Toolkit.js'
import * as ToolControl from '../src/ToolControl.js'
import * as ToolResult from '../src/ToolResult.js'

const text = (value: string) => [Prompt.textPart({ text: value })]
const result = (control?: ToolControl.Control): ToolResult.Result => ({
  content: text('public'),
  isError: false,
  details: { ui: 'private' },
  structuredOutput: { data: 'private' },
  diagnostics: [],
  ...(control === undefined ? {} : { control }),
})

describe('final tool controls and channels', () => {
  it.effect('legacy omission stays optional; strict success and hook replacements validate', () =>
    Effect.gen(function* () {
      const optional = Tool.makeResult('optional', { success: Schema.Finite })
      const strict = Tool.makeResult('strict', {
        success: Schema.Finite,
        resultValidation: 'strict',
      })
      const missing = { content: text('public'), isError: false, diagnostics: [] }
      yield* Toolkit.validateResult(optional, missing)
      assert.isTrue(
        Schema.isSchemaError(yield* Toolkit.validateResult(strict, missing).pipe(Effect.flip)),
      )
      const strictResult = {
        ...missing,
        structuredOutput: 1,
        control: { terminate: true as const, addTools: ['next'] },
      }
      assert.deepStrictEqual(
        yield* Toolkit.validateResult(strict, strictResult, ['next']),
        strictResult,
      )
      yield* Toolkit.validateResult(strict, { ...missing, isError: true })
      assert.isTrue(
        Schema.isSchemaError(
          yield* Toolkit.validateResult(strict, { ...strictResult, structuredOutput: 'wrong' }, [
            'next',
          ]).pipe(Effect.flip),
        ),
      )
      assert.strictEqual(
        (yield* Toolkit.validateResult(strict, strictResult).pipe(Effect.flip))._tag,
        'InvalidToolControl',
      )
      for (const control of [
        { terminate: false },
        { handoff: 1 },
        { addTools: [''] },
        { unexpected: true },
      ])
        assert.isTrue(
          Schema.isSchemaError(
            yield* Toolkit.validateResult(strict, { ...strictResult, control }, ['next']).pipe(
              Effect.flip,
            ),
          ),
        )
      const ordinary = Tool.make('ordinary', { success: Schema.String })
      assert.isFalse(Tool.hasResultChannels(ordinary))
      assert.isTrue(Tool.hasResultChannels(optional))
    }),
  )

  it.effect(
    'JSON receipts retain controls/media/options; model envelope keeps private channels out',
    () =>
      Effect.gen(function* () {
        const media = Prompt.filePart({
          mediaType: 'image/png',
          data: new Uint8Array([1, 2, 3]),
          options: { openai: { imageDetail: 'high' } },
        })
        const original = {
          ...result({ terminate: true, handoff: 'continue', addTools: ['next'] }),
          content: [...text('visible'), media],
        }
        const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(ToolResult.ResultSchema))(
          original,
        )
        const restored = yield* Schema.decodeEffect(Schema.toCodecJson(ToolResult.ResultSchema))(
          JSON.parse(JSON.stringify(encoded)),
        )
        assert.deepStrictEqual(restored, original)
        const tool = Tool.makeResult('media', {
          success: Schema.Struct({ data: Schema.String }),
          resultValidation: 'strict',
        })
        assert.deepStrictEqual(yield* Toolkit.validateResult(tool, restored, ['next']), original)
        const nested = ToolResult.nested(restored, true)
        assert.notProperty(nested, 'control')
        assert.notProperty(nested, 'content')
        assert.deepStrictEqual(nested.structuredOutput, { data: 'private' })
        const model = yield* Schema.encodeEffect(Schema.toCodecJson(ToolResult.Envelope))({
          _tag: '@effect-harness/ToolContent',
          content: restored.content,
        })
        assert.notInclude(JSON.stringify(model), 'private')
        assert.notInclude(JSON.stringify(model), 'terminate')
        assert.include(JSON.stringify(model), 'high')
        assert.include(JSON.stringify(model), 'AQID')
      }),
  )

  it.effect(
    'nested content fallback handles empty/text/media/list/error and preserves explicit null',
    () =>
      Effect.gen(function* () {
        const media = Prompt.filePart({
          mediaType: 'image/png',
          data: new Uint8Array([4, 5]),
          options: { openai: { imageDetail: 'low' } },
        })
        const base = {
          isError: false,
          diagnostics: [],
          control: { terminate: true as const },
          details: { panel: 'ui' },
        }
        for (const [content, expected] of [
          [[], ''],
          [text('single'), 'single'],
          [[media], media],
          [
            [...text('first'), media],
            [...text('first'), media],
          ],
        ] as const) {
          const projected = ToolResult.nested({ ...base, content }, false)
          assert.deepStrictEqual(projected.structuredOutput, expected)
          const json = yield* Schema.encodeEffect(Schema.toCodecJson(ToolResult.NestedSchema))(
            projected,
          )
          const restored = yield* Schema.decodeEffect(Schema.toCodecJson(ToolResult.NestedSchema))(
            JSON.parse(JSON.stringify(json)),
          )
          assert.deepStrictEqual(restored, projected)
          assert.notProperty(restored, 'control')
        }
        assert.strictEqual(
          ToolResult.nested({ ...base, content: text('error'), isError: true }, false)
            .structuredOutput,
          'error',
        )
        assert.notProperty(ToolResult.nested({ ...base, content: [] }, true), 'structuredOutput')
        assert.strictEqual(
          ToolResult.nested({ ...base, content: text('fallback'), structuredOutput: null }, false)
            .structuredOutput,
          null,
        )
      }),
  )

  it.effect(
    'original order decides unanimous termination, union and last handoff for every completion permutation',
    () =>
      Effect.gen(function* () {
        const receipts: ReadonlyArray<ToolControl.Receipt> = [
          {
            callId: 'a',
            caller: 'model',
            status: 'succeeded',
            control: { terminate: true, handoff: 'first', addTools: ['x', 'y'] },
          },
          {
            callId: 'b',
            caller: 'model',
            status: 'succeeded',
            control: { terminate: true, addTools: ['y', 'z'] },
          },
          {
            callId: 'c',
            caller: 'model',
            status: 'succeeded',
            control: { terminate: true, handoff: 'last', addTools: ['x'] },
          },
        ]
        assert.deepStrictEqual(
          ToolControl.reduceRound(['a', 'b', 'c'], receipts, ['x', 'y', 'z']),
          Result.succeed({ terminate: true, addTools: ['x', 'y', 'z'], handoff: 'last' }),
        )
        for (const order of [
          [0, 1, 2],
          [0, 2, 1],
          [1, 0, 2],
          [1, 2, 0],
          [2, 0, 1],
          [2, 1, 0],
        ])
          assert.deepStrictEqual(
            yield* ToolControl.reduce(
              ['a', 'b', 'c'],
              order.map((i) => receipts[i]!),
              ['x', 'y', 'z'],
            ),
            {
              terminate: true,
              addTools: ['x', 'y', 'z'],
              handoff: 'last',
            },
          )
        for (const status of ['failed', 'cancelled', 'denied'] as const)
          assert.deepStrictEqual(
            yield* ToolControl.reduce(
              ['a', 'b', 'c'],
              receipts.map((r) => (r.callId === 'b' ? { ...r, status } : r)),
              ['x', 'y', 'z'],
            ),
            {
              terminate: false,
              addTools: ['x', 'y'],
              handoff: 'last',
            },
          )
        const nested = receipts.map((r) =>
          r.callId === 'c' ? { ...r, caller: 'tools' as const } : r,
        )
        assert.deepStrictEqual(
          yield* ToolControl.reduce(['a', 'b', 'c'], nested, ['x', 'y', 'z']),
          { terminate: false, addTools: ['x', 'y', 'z'], handoff: 'first' },
        )
        const noControl = receipts.map((r) =>
          r.callId === 'b' ? { callId: r.callId, caller: r.caller, status: r.status } : r,
        )
        assert.isFalse(
          (yield* ToolControl.reduce(['a', 'b', 'c'], noControl, ['x', 'y', 'z'])).terminate,
        )
        assert.deepStrictEqual(yield* ToolControl.reduce([], [], []), {
          terminate: false,
          addTools: [],
        })
        for (const bad of [
          receipts.slice(1),
          [...receipts, receipts[0]!],
          [
            ...receipts,
            { callId: 'other', caller: 'model' as const, status: 'succeeded' as const },
          ],
        ])
          assert.strictEqual(
            (yield* ToolControl.reduce(['a', 'b', 'c'], bad, ['x', 'y', 'z']).pipe(Effect.flip))
              .reason,
            'invalidRound',
          )
        assert.strictEqual(
          (yield* ToolControl.reduce(['a', 'a', 'c'], receipts, ['x', 'y', 'z']).pipe(Effect.flip))
            .reason,
          'invalidRound',
        )
        assert.strictEqual(
          (yield* ToolControl.reduce(['a', 'b', 'c'], receipts, ['x']).pipe(Effect.flip)).reason,
          'unavailableTool',
        )
      }),
  )
})
