/**
 * The agent behind the "First agent" demo: three text tools and a scripted model. Everything
 * below runs in your browser, on the same `effect-harness` modules a server would use.
 */
import { Array, Effect, Layer, Option, pipe, Schema, String } from 'effect'
import { Extension, HarnessServer } from 'effect-harness/harness'
import type { Model, Message } from 'effect-harness/model'
import { Faux } from 'effect-harness/model'
import { Tool, Toolkit } from 'effect-harness/tool'

// 1. Declarations: names, descriptions and Schemas. No code yet.

const Uppercase = Tool.make('uppercase', {
  description: 'Convert text to uppercase.',
  parameters: Schema.Struct({ text: Schema.String }),
  success: Schema.String,
})

const Reverse = Tool.make('reverse', {
  description: 'Reverse the characters of a text.',
  parameters: Schema.Struct({ text: Schema.String }),
  success: Schema.String,
})

const CountWords = Tool.make('count_words', {
  description: 'Count the words in a text.',
  parameters: Schema.Struct({ text: Schema.String }),
  success: Schema.Finite,
})

const toolkit = Toolkit.make(Uppercase, Reverse, CountWords)

// 2. Implementations, as a Layer. They run where the loop runs: here, in this tab.

const ToolsLive = toolkit.toLayer({
  uppercase: ({ text }) => Effect.succeed(text.toUpperCase()),
  reverse: ({ text }) => Effect.succeed(Array.join(Array.reverse(String.split(text, '')), '')),
  count_words: ({ text }) =>
    Effect.succeed(Array.filter(String.split(text, /\s+/), String.isNonEmpty).length),
})

// 3. The loop: one extension, the model to ask, and the implementations.

export const loop = (model: Model.Ref) =>
  HarnessServer.layer({
    extensions: [Extension.make('wordsmith', { toolkit })],
    settings: { model },
  }).pipe(Layer.provide(ToolsLive))

// 4. The scripted model. A real provider decides for itself; this one reads keywords, so the
//    demo works offline. It sees exactly the request a real model would.

const triggers: ReadonlyArray<readonly [tool: string, words: ReadonlyArray<string>]> = [
  ['uppercase', ['shout', 'upper', 'loud', 'caps']],
  ['reverse', ['reverse', 'backward', 'flip']],
  ['count_words', ['count', 'how many', 'words']],
]

const textOf = (content: Message.UserMessage['content']): string =>
  typeof content === 'string'
    ? content
    : Array.join(
        Array.flatMap(content, (part) => (part.type === 'text' ? [part.text] : [])),
        ' ',
      )

/** The quoted part of a message, or all of it. */
const subjectOf = (text: string): string =>
  pipe(
    Option.fromNullishOr(/["“](.+?)["”]/.exec(text)?.[1]),
    Option.getOrElse(() => text),
  )

const callTools = (text: string) => {
  const lowered = String.toLowerCase(text)
  const tools = Array.flatMap(triggers, ([tool, words]) =>
    Array.some(words, (word) => String.includes(word)(lowered)) ? [tool] : [],
  )
  const subject = subjectOf(text)
  return Array.match(tools, {
    onEmpty: () =>
      Faux.message(
        'I am a scripted model with three tools. Ask me to shout, reverse or count the words of something, like: shout "hello world".',
      ),
    onNonEmpty: (names) =>
      Faux.message(
        [
          Faux.thinking(`The user wants ${Array.join(names, ' and ')} on "${subject}".`),
          ...Array.map(names, (name) => Faux.toolCall(name, { text: subject })),
        ],
        { stopReason: 'toolUse' },
      ),
  })
}

const answer = (results: ReadonlyArray<Message.ToolResultMessage>) =>
  Faux.message(
    pipe(
      results,
      Array.map(
        (result) =>
          `\`${result.toolName}\` says: ${Array.join(
            Array.flatMap(result.content, (part) => (part.type === 'text' ? [part.text] : [])),
            '',
          )}`,
      ),
      Array.join('\n'),
    ),
  )

export const respond: Faux.Respond = (request) =>
  Effect.succeed(
    pipe(
      request.messages,
      Array.reverse,
      Array.takeWhile((message) => message.role !== 'user' && message.role !== 'assistant'),
      Array.flatMap((message) => (message.role === 'toolResult' ? [message] : [])),
      Array.match({
        onNonEmpty: (results) => answer(Array.reverse(results)),
        onEmpty: () =>
          pipe(
            Array.findLast(request.messages, (message) => message.role === 'user'),
            Option.map((message) => (message.role === 'user' ? textOf(message.content) : '')),
            Option.getOrElse(() => ''),
            callTools,
          ),
      }),
    ),
  )

export const suggestions: ReadonlyArray<string> = [
  'shout "hello world"',
  'reverse "effect-harness" and count the words of it',
  'how many words are in "kill the loop at any moment"',
  'what can you do?',
]
