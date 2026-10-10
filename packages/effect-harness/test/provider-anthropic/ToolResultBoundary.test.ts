import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Prompt from 'effect/ai/Prompt'
import * as Schema from 'effect/Schema'
import * as NativeToolResult from 'effect-harness/ToolResult'
import * as ToolResult from 'effect-harness/provider-anthropic/ToolResult'

describe('ToolResultBoundary', () => {
  it.effect('JSON envelopes distinguish text bytes from literal strings', () =>
    Effect.gen(function* () {
      const text = 'hello — café 🌱'
      const content = [
        Prompt.filePart({ mediaType: 'text/plain', data: new TextEncoder().encode(text) }),
        Prompt.filePart({ mediaType: 'text/plain', data: 'aGVsbG8=' }),
        Prompt.filePart({ mediaType: 'text/plain', data: new Uint8Array() }),
      ]
      const envelope = yield* Schema.encodeEffect(Schema.toCodecJson(NativeToolResult.Envelope))({
        _tag: '@effect-harness/ToolContent',
        content,
      })
      const translated = yield* ToolResult.request({
        payload: {
          model: 'test',
          max_tokens: 1,
          messages: [
            {
              role: 'user',
              content: [
                { type: 'tool_result', tool_use_id: 'call-1', content: JSON.stringify(envelope) },
              ],
            },
          ],
        },
      })
      const message = translated.payload.messages[0]
      assert.isDefined(message)
      if (message === undefined || typeof message.content === 'string') return assert.fail()
      const result = message.content[0]
      if (
        result?.type !== 'tool_result' ||
        result.content == null ||
        typeof result.content === 'string'
      )
        return assert.fail()
      assert.deepStrictEqual(
        result.content.map((block) => (block.type === 'document' ? block.source : block)),
        [
          { type: 'text', media_type: 'text/plain', data: text },
          { type: 'text', media_type: 'text/plain', data: 'aGVsbG8=' },
          { type: 'text', media_type: 'text/plain', data: '' },
        ],
      )
      const encodedResult = yield* Schema.encodeEffect(
        Schema.toCodecJson(NativeToolResult.ResultSchema),
      )({
        content,
        isError: false,
        diagnostics: [],
      })
      const restored = yield* Schema.decodeEffect(
        Schema.toCodecJson(NativeToolResult.ResultSchema),
      )(encodedResult)
      assert.deepStrictEqual(restored.content, content)
    }),
  )

  it.effect('URL presence selects native URL sources while absent URL preserves inline media', () =>
    Effect.gen(function* () {
      const blocks = yield* ToolResult.content([
        Prompt.filePart({ mediaType: 'image/png', data: new URL('https://fixture.invalid/image') }),
        Prompt.filePart({ mediaType: 'application/pdf', data: 'https://fixture.invalid/document' }),
        Prompt.filePart({ mediaType: 'image/png', data: new Uint8Array([1, 2, 3]) }),
        Prompt.filePart({ mediaType: 'text/plain', data: 'inline text' }),
      ])
      assert.deepStrictEqual(blocks, [
        {
          type: 'image',
          cache_control: null,
          source: { type: 'url', url: 'https://fixture.invalid/image' },
        },
        {
          type: 'document',
          cache_control: null,
          title: null,
          source: { type: 'url', url: 'https://fixture.invalid/document' },
        },
        {
          type: 'image',
          cache_control: null,
          source: { type: 'base64', media_type: 'image/png', data: 'AQID' },
        },
        {
          type: 'document',
          cache_control: null,
          title: null,
          source: { type: 'text', media_type: 'text/plain', data: 'inline text' },
        },
      ])
    }),
  )
})
