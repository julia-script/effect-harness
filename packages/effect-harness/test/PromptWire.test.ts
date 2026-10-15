import { assert, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as PromptWire from 'effect-harness/PromptWire'
import * as ToolResult from 'effect-harness/ToolResult'
import * as NativePrompt from './fixtures/NativePrompt.js'

it.effect('public codec owns native media without interpreting arbitrary JSON marker values', () =>
  Effect.gen(function* () {
    const opaque = {
      _tag: '@effect-harness/Bytes',
      data: 'literal',
      nested: { _tag: '@effect-harness/URL', href: 'not a URL' },
    }
    const source = Prompt.fromMessages([
      Prompt.userMessage({ content: NativePrompt.content(), options: { fixture: opaque } }),
      Prompt.assistantMessage({
        content: [
          Prompt.filePart({ mediaType: 'image/png', data: new Uint8Array([4, 5]) }),
          Prompt.toolCallPart({ id: 'a', name: 'json', params: opaque, providerExecuted: false }),
        ],
      }),
      Prompt.toolMessage({
        content: [
          Prompt.toolResultPart({
            id: 'a',
            name: 'json',
            result: opaque,
            isFailure: false,
            providerExecuted: false,
          }),
        ],
      }),
    ])
    const codec = Schema.fromJsonString(Schema.toCodecJson(PromptWire.Prompt))
    const encoded = yield* Schema.encodeEffect(codec)(source)
    const decoded = yield* Schema.decodeEffect(codec)(encoded)
    NativePrompt.assertFiles(decoded)
    assert.isTrue(Prompt.isPrompt(decoded))
    assert.deepEqual(decoded, source)
    const first = source.content[0]
    if (
      first?.role !== 'user' ||
      first.content[1]?.type !== 'file' ||
      !(first.content[1].data instanceof Uint8Array)
    )
      return assert.fail('fixture')
    first.content[1].data[0] = 99
    NativePrompt.assertFiles(decoded)
  }),
)

it.effect(
  'result file codec distinguishes URL objects from literal URL strings in every result channel',
  () =>
    Effect.gen(function* () {
      for (const data of [
        new Uint8Array([1, 2, 3]),
        new URL('https://example.com/image.png'),
        'https://example.com/image.png',
      ]) {
        const result: ToolResult.Result = {
          content: [Prompt.filePart({ mediaType: 'image/png', data })],
          isError: false,
          diagnostics: [],
          details: { _tag: '@effect-harness/URL', href: 'opaque' },
        }
        const resultCodec = Schema.fromJsonString(Schema.toCodecJson(ToolResult.ResultSchema))
        const decoded = yield* Schema.decodeEffect(resultCodec)(
          yield* Schema.encodeEffect(resultCodec)(result),
        )
        assert.deepEqual(decoded, result)
        assert.strictEqual(decoded.content[0]!.type, 'file')
        const file = decoded.content[0]
        if (file?.type !== 'file') return assert.fail('missing file')
        assert.strictEqual(typeof file.data, typeof data)
        assert.strictEqual(file.data.constructor, data.constructor)
        const nested = ToolResult.nested(result, false)
        const codec = Schema.fromJsonString(Schema.toCodecJson(ToolResult.NestedSchema))
        assert.deepEqual(
          yield* Schema.decodeEffect(codec)(yield* Schema.encodeEffect(codec)(nested)),
          nested,
        )
      }
    }),
)
