/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * Replaying a transcript to another model: what goes back unchanged, what is left out, and what
 * is rewritten, alone and through `Model.stream`.
 */
import { describe, expect, test } from 'bun:test'
import { Effect, Ref, Stream } from 'effect'
import { Message, Model, Transcript } from '../index.ts'

const reader = (
  provider: string,
  api: string,
  id: string,
  input: Model.Info['input'] = ['text', 'image'],
) => ({
  provider,
  api,
  id,
  input,
})
const claude = reader('anthropic', 'anthropic-messages', 'claude-opus-5-5')
const sonnet = reader('anthropic', 'anthropic-messages', 'claude-sonnet-5-5')
const codex = reader('openai-codex', 'openai-codex-responses', 'gpt-6.1-sol')
const kimi = reader('openrouter', 'openrouter-chat-completions', 'moonshotai/kimi-k2')

const assistant = (
  from: ReturnType<typeof reader>,
  content: ReadonlyArray<Message.AssistantContent>,
): Message.AssistantMessage => ({
  role: 'assistant',
  content,
  api: from.api,
  provider: from.provider,
  model: from.id,
  usage: Message.emptyUsage,
  stopReason: 'toolUse',
  timestamp: 1,
})

const result = (toolCallId: string, image = false): Message.Message => ({
  role: 'toolResult',
  toolCallId,
  toolName: 'read',
  content: [
    { type: 'text', text: 'hello' },
    ...(image ? [{ type: 'image' as const, data: 'AAAA', mimeType: 'image/png' }] : []),
  ],
  isError: false,
  timestamp: 2,
})

const user = (content: string): Message.Message => ({ role: 'user', content, timestamp: 0 })

const fromKimi = assistant(kimi, [
  {
    type: 'thinking',
    thinking: 'Kimi plans.',
    thinkingSignature: '{"openrouter":{"reasoningDetails":[]}}',
  },
  { type: 'text', text: '' },
  { type: 'toolCall', id: 'functions.read:0', name: 'read', arguments: { path: 'a' } },
])
const fromClaude = assistant(claude, [
  { type: 'thinking', thinking: 'Claude plans.', thinkingSignature: '{"anthropic":{}}' },
  { type: 'text', text: 'Reading.' },
  { type: 'toolCall', id: 'toolu_01', name: 'read', arguments: { path: 'b' } },
])
const fromCodex = assistant(codex, [
  { type: 'thinking', thinking: 'Codex plans.', thinkingSignature: '{"openai":{"itemId":"rs_1"}}' },
  { type: 'text', text: 'Reading.', textSignature: '{"openai":{"itemId":"msg_1"}}' },
  { type: 'toolCall', id: 'call_1', name: 'read', arguments: { path: 'c' }, thoughtSignature: 't' },
])

describe('Transcript.replay', () => {
  test('responses the reader wrote itself go back unchanged', () => {
    const transcript = [user('go'), fromKimi, result('functions.read:0')]
    const replayed = Transcript.replay(transcript, kimi)
    expect(replayed[1]).toBe(fromKimi)
    expect(replayed[2]).toEqual(transcript[2]!)
  })

  test('another provider gets safe tool IDs, no empty text and no foreign reasoning', () => {
    const replayed = Transcript.replay([user('go'), fromKimi, result('functions.read:0')], claude)
    expect(replayed[1]).toMatchObject({
      content: [{ type: 'toolCall', id: 'functions_read_0', name: 'read' }],
    })
    expect((replayed[1] as Message.AssistantMessage).content).toHaveLength(1)
    expect(replayed[2]).toMatchObject({ toolCallId: 'functions_read_0' })
  })

  test('Claude thinking goes back to another Claude model on the same account', () => {
    const replayed = Transcript.replay([user('go'), fromClaude, result('toolu_01')], sonnet)
    expect((replayed[1] as Message.AssistantMessage).content).toEqual(fromClaude.content)
  })

  test('Codex reasoning, item IDs and thought signatures stay with Codex', () => {
    const replayed = Transcript.replay([user('go'), fromCodex, result('call_1')], claude)
    expect((replayed[1] as Message.AssistantMessage).content).toEqual([
      { type: 'text', text: 'Reading.' },
      { type: 'toolCall', id: 'call_1', name: 'read', arguments: { path: 'c' } },
    ])
  })

  test('a rewritten ID never takes one already in the transcript', () => {
    const taken = assistant(claude, [
      { type: 'toolCall', id: 'functions_read_0', name: 'read', arguments: {} },
    ])
    const replayed = Transcript.replay(
      [user('go'), taken, result('functions_read_0'), fromKimi, result('functions.read:0')],
      claude,
    )
    expect(replayed[3]).toMatchObject({ content: [{ id: 'functions_read_0_1' }] })
    expect(replayed[4]).toMatchObject({ toolCallId: 'functions_read_0_1' })
  })

  test('images become a note for a model that reads text only', () => {
    const textOnly = reader('openrouter', 'openrouter-chat-completions', 'tiny', ['text'])
    const photo: Message.Message = {
      role: 'user',
      content: [
        { type: 'text', text: 'look' },
        { type: 'image', data: 'AAAA', mimeType: 'image/png' },
      ],
      timestamp: 0,
    }
    const replayed = Transcript.replay([photo, fromClaude, result('toolu_01', true)], textOnly)
    const note = { type: 'text', text: '[An image was here; tiny does not read images.]' }
    expect(replayed[0]).toMatchObject({ content: [{ type: 'text', text: 'look' }, note] })
    expect(replayed[2]).toMatchObject({ content: [{ type: 'text', text: 'hello' }, note] })
  })

  test('Model.stream replays the transcript for the model it routes to', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const seen = yield* Ref.make<ReadonlyArray<Message.Message>>([])
        const info = Model.define('anthropic', 'anthropic-messages', { id: 'claude-opus-5-5' })
        const provider: Model.Provider = {
          name: 'anthropic',
          models: [info],
          stream: (request) =>
            Stream.unwrap(
              Effect.as(
                Ref.set(seen, request.messages),
                Stream.make({
                  type: 'done' as const,
                  message: { ...assistant(claude, []), stopReason: 'stop' as const },
                }),
              ),
            ),
        }
        const model = yield* Model.make([provider])
        yield* Model.collect(
          model.stream({
            model: { provider: 'anthropic', modelId: 'claude-opus-5-5' },
            messages: [user('go'), fromKimi, result('functions.read:0')],
          }),
          info,
        )
        expect((yield* Ref.get(seen))[2]).toMatchObject({ toolCallId: 'functions_read_0' })
      }),
    ))
})
