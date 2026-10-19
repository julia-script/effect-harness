import { assert } from '@effect/vitest'
import * as Prompt from 'effect/ai/Prompt'

export const content = (text = 'go'): ReadonlyArray<Prompt.UserMessagePart> => [
  Prompt.textPart({ text }),
  Prompt.filePart({
    mediaType: 'image/png',
    fileName: 'bytes.png',
    data: new Uint8Array([1, 2, 3]),
    options: {
      fixture: {
        _tag: '@effect-harness/Bytes',
        data: 'literal',
        nested: { _tag: '@effect-harness/URL', href: 'opaque' },
      },
    },
  }),
  Prompt.filePart({ mediaType: 'image/png', data: new URL('https://example.com/image.png') }),
  Prompt.filePart({ mediaType: 'image/png', data: 'AQID' }),
]

export const assertFiles = (prompt: Prompt.Prompt): void => {
  const files = prompt.content.flatMap((message) =>
    message.role === 'user' ? message.content.filter((part) => part.type === 'file') : [],
  )
  assert.lengthOf(files, 3)
  assert.instanceOf(files[0]!.data, Uint8Array)
  assert.deepEqual(files[0]!.data, new Uint8Array([1, 2, 3]))
  assert.strictEqual(files[0]!.fileName, 'bytes.png')
  assert.deepEqual(files[0]!.options, {
    fixture: {
      _tag: '@effect-harness/Bytes',
      data: 'literal',
      nested: { _tag: '@effect-harness/URL', href: 'opaque' },
    },
  })
  assert.instanceOf(files[1]!.data, URL)
  assert.strictEqual(files[1]!.data.toString(), 'https://example.com/image.png')
  assert.strictEqual(files[2]!.data, 'AQID')
}
