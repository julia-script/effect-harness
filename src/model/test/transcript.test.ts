/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * Replaying a transcript's system messages, and decoding the messages entries store.
 */
import { describe, expect, test } from 'bun:test'
import { Effect, Schema } from 'effect'
import { Message, Transcript } from '../index.ts'

const tool = (name: string, description = name): Message.ToolDefinition => ({
  name,
  description,
  parameters: { type: 'object', properties: {} },
})

const user = (content: string): Message.Message => ({ role: 'user', content, timestamp: 2 })

const transcript: ReadonlyArray<Message.Message> = [
  {
    role: 'system',
    content: 'You are helpful.',
    sections: { date: 'Today is Monday.', mode: 'Plan first.' },
    toolsAdded: [tool('read'), tool('write')],
    timestamp: 1,
  },
  user('hi'),
  {
    role: 'system',
    content: [{ type: 'text', text: 'Be brief.' }],
    sections: { date: 'Today is Tuesday.', mode: null },
    toolsRemoved: [{ name: 'write' }],
    toolsAdded: [tool('read', 'read v2'), tool('search')],
    timestamp: 3,
  },
  user('again'),
]

describe('Transcript', () => {
  test('replays tool changes in order', () => {
    expect(
      Transcript.tools(transcript).map(({ name, description }) => [name, description]),
    ).toEqual([
      ['read', 'read v2'],
      ['search', 'search'],
    ])
  })

  test('replays the system prompt: content appended, sections patched', () => {
    expect(Transcript.systemMessage(transcript)).toEqual({
      role: 'system',
      content: 'You are helpful.\n\nBe brief.',
      sections: { date: 'Today is Tuesday.' },
      toolsAdded: [tool('read', 'read v2'), tool('search')],
      timestamp: 1,
    })
    expect(Transcript.systemPrompt(transcript)).toBe(
      'You are helpful.\n\nBe brief.\n\nToday is Tuesday.',
    )
    expect(Transcript.systemMessage([user('no prompt')])).toBeUndefined()
  })

  test('collapses system messages to one leading message', () => {
    const collapsed = Transcript.collapse(transcript)
    expect(collapsed.map((message) => message.role)).toEqual(['system', 'user', 'user'])
    expect(collapsed[0]).toEqual(Transcript.systemMessage(transcript))
  })
})

describe('Message', () => {
  test('decodes what entries store, and encodes it back unchanged', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const stored: Schema.Json = {
          role: 'assistant',
          content: [
            { type: 'thinking', thinking: 'hmm', thinkingSignature: 'sig' },
            { type: 'toolCall', id: 'c1', name: 'read', arguments: { path: 'a' } },
          ],
          api: 'anthropic-messages',
          provider: 'anthropic',
          model: 'claude',
          usage: {
            input: 1,
            output: 2,
            cacheRead: 0,
            cacheWrite: 0,
            totalTokens: 3,
            cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
          },
          stopReason: 'toolUse',
          timestamp: 5,
        }
        const message = yield* Schema.decodeUnknownEffect(Message.Message)(stored)
        expect(message.role).toBe('assistant')
        const encoded: Schema.Json = yield* Schema.encodeEffect(Message.Message)(message)
        expect(encoded).toEqual(stored)
        const invalid = yield* Effect.flip(
          Schema.decodeUnknownEffect(Message.Message)({ role: 'robot', timestamp: 0 }),
        )
        expect(invalid).toBeInstanceOf(Schema.SchemaError)
      }),
    ))
})
