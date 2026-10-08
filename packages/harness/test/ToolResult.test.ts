import { assert, describe, it } from '@effect/vitest'
import * as Cause from 'effect/Cause'
import * as Effect from 'effect/Effect'
import * as Prompt from 'effect/ai/Prompt'
import * as NativeModel from 'effect/ai/LanguageModel'
import * as AiTool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Tool from '@effect-harness/harness/Tool'
import * as ToolResult from '@effect-harness/harness/ToolResult'
import { ToolError, ToolExecution } from '@effect-harness/harness/ToolError'

describe('ToolResult', () => {
  describe('model-visible tool content', () => {
    for (const retain of ['head', 'tail'] as const) {
      it.effect(`bounds text at its original ${retain} position while retaining files`, () =>
        Effect.gen(function* () {
          const first = Prompt.filePart({ mediaType: 'image/png', data: new Uint8Array([1]) })
          const last = Prompt.filePart({
            mediaType: 'image/jpeg',
            data: new URL('https://example.invalid/picture'),
          })
          const result = Tool.boundResult(
            { content: [first, Prompt.textPart({ text: 'abcdefghijklmnop' }), last] },
            { maxBytes: 5, maxLines: 10, retain },
          )
          assert.strictEqual(result.content?.[0], first)
          assert.strictEqual(result.content?.[2], last)
          assert.strictEqual(result.content?.[1]?.type, 'text')
          const encoded = yield* ToolResult.encode({
            ...result,
            details: { secret: 'private' },
            control: { terminate: true },
          })
          assert.strictEqual(JSON.stringify(encoded).includes('private'), false)
          assert.strictEqual(JSON.stringify(encoded).includes('terminate'), false)
          const decoded = yield* ToolResult.decode(encoded)
          assert.strictEqual(decoded.content[0]?.type, 'file')
          assert.strictEqual(decoded.content[2]?.type, 'file')
          const diagnostic = decoded.content.at(-1)
          assert.ok(diagnostic?.type === 'text' && diagnostic.text.includes('<harness>'))
        }),
      )
    }
    it.effect(
      'keeps the first/last text anchors across separated blocks and appends error then truncation diagnostics',
      () =>
        Effect.gen(function* () {
          const file = Prompt.filePart({ mediaType: 'image/png', data: new Uint8Array([3]) })
          const partial = {
            content: [Prompt.textPart({ text: 'abc' }), file, Prompt.textPart({ text: 'defghi' })],
            diagnostics: [{ kind: 'existing', message: 'kept' }],
          }
          const failed = yield* Tool.settleFailure(
            Cause.fail(
              new ToolError({ reason: new ToolExecution({ name: 'tool', message: 'broken' }) }),
            ),
            partial,
          )
          assert.deepStrictEqual(
            failed.diagnostics?.map((item) => item.kind),
            ['existing', 'tool_error'],
          )
          const head = Tool.boundResult(failed, { maxBytes: 3, maxLines: 10, retain: 'head' })
          const tail = Tool.boundResult(failed, { maxBytes: 3, maxLines: 10, retain: 'tail' })
          assert.deepStrictEqual(
            head.content?.map((part) => part.type),
            ['text', 'file'],
          )
          assert.deepStrictEqual(
            tail.content?.map((part) => part.type),
            ['file', 'text'],
          )
          assert.deepStrictEqual(
            tail.diagnostics?.map((item) => item.kind),
            ['existing', 'tool_error', 'truncated'],
          )
          const visible = yield* ToolResult.decode(yield* ToolResult.encode(tail))
          const diagnostic = visible.content.at(-1)
          assert.ok(diagnostic?.type === 'text' && diagnostic.text.includes('[error]'))
        }),
    )
  })

  describe('native AI tool validation', () => {
    type BroadTool = AiTool.Tool<
      string,
      {
        parameters: Schema.Codec<unknown, unknown, never, never>
        success: Schema.Codec<unknown, unknown, never, never>
        failure: typeof Schema.Never
        failureMode: 'error'
      }
    >

    const known = AiTool.make('known', {
      parameters: Schema.Struct({ value: Schema.Finite }),
      success: Schema.String,
    })
    for (const mode of ['text', 'stream'] as const) {
      for (const scenario of ['valid', 'unknown', 'invalid-offered'] as const) {
        it.effect(
          `${mode} ${scenario} preserves native validation and never invokes a handler`,
          () =>
            Effect.gen(function* () {
              let handled = 0
              const toolkit: Toolkit.WithHandler<Record<string, BroadTool>> = {
                tools: { known },
                handle: () =>
                  Effect.sync(() => {
                    handled++
                  }).pipe(Effect.andThen(Effect.die('Resolution is disabled'))),
              }
              const part = {
                type: 'tool-call' as const,
                id: 'call',
                name: scenario === 'unknown' ? 'unoffered' : 'known',
                params: { value: scenario === 'invalid-offered' ? 'bad' : 1 },
                providerExecuted: false,
              }
              const model = yield* NativeModel.make({
                generateText: () => Effect.succeed([part]),
                streamText: () => Stream.make(part),
              })
              const options: NativeModel.GenerateTextOptions<Record<string, BroadTool>> & {
                readonly toolkit: Toolkit.WithHandler<Record<string, BroadTool>>
              } = {
                prompt: 'request',
                toolkit,
                disableToolCallResolution: true,
              }
              const result = yield* Effect.result(
                mode === 'text'
                  ? model
                      .generateText(options)
                      .pipe(Effect.map((response) => response.toolCalls.map((call) => call.name)))
                  : model.streamText(options).pipe(
                      Stream.runCollect,
                      Effect.map((parts) =>
                        parts.filter((item) => item.type === 'tool-call').map((call) => call.name),
                      ),
                    ),
              )
              assert.strictEqual(result._tag, scenario === 'valid' ? 'Success' : 'Failure')
              if (result._tag === 'Success') assert.deepStrictEqual(result.success, ['known'])
              else assert.strictEqual(result.failure.reason._tag, 'InvalidOutputError')
              assert.strictEqual(handled, 0)
              assert.deepStrictEqual(Object.keys(toolkit.tools), ['known'])
            }),
        )
      }
    }
  })
})
