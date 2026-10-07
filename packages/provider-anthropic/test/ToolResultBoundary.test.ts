import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Prompt from 'effect/ai/Prompt'
import * as ToolResult from '@effect-harness/provider-anthropic/ToolResult'

describe('ToolResultBoundary', () => {
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
