/**
 * A transcript as an effect/ai prompt and toolkit. System messages collapse into one leading
 * message; responses that failed, were aborted, or are still pending are left out.
 */
import { Effect, Option, Schema } from 'effect'
import { AiError, Prompt, Tool } from 'effect/ai'
import type { Toolkit } from 'effect/ai'
import type { AssistantContent, Message, ToolDefinition, ToolResultMessage } from '../../Message.ts'
import * as Transcript from '../../Transcript.ts'

/** effect/ai part options kept in a signature field, as JSON. */
const optionsOf = Schema.decodeUnknownOption(Schema.fromJsonString(Prompt.ProviderOptions))

const withOptions = <A extends object>(part: A, signature: string | undefined) => {
  const options = signature === undefined ? Option.none() : optionsOf(signature)
  return Option.isSome(options) ? { ...part, options: options.value } : part
}

const assistantPart = (block: AssistantContent): Prompt.AssistantMessagePartEncoded => {
  if (block.type === 'text')
    return withOptions({ type: 'text', text: block.text }, block.textSignature)
  if (block.type === 'thinking')
    return withOptions({ type: 'reasoning', text: block.thinking }, block.thinkingSignature)
  return { type: 'tool-call', id: block.id, name: block.name, params: block.arguments }
}

const toolResult = (message: ToolResultMessage): Prompt.ToolMessagePartEncoded => ({
  type: 'tool-result',
  id: message.toolCallId,
  name: message.toolName,
  isFailure: message.isError,
  result: Transcript.contentText(message.content),
})

const answered = new Set(['stop', 'length', 'toolUse'])

const messageOf = (message: Message): ReadonlyArray<Prompt.MessageEncoded> => {
  switch (message.role) {
    case 'system':
      return [{ role: 'system', content: Transcript.systemText(message) }]
    case 'user':
      if (typeof message.content === 'string') return [{ role: 'user', content: message.content }]
      return [
        {
          role: 'user',
          content: message.content.map((part): Prompt.UserMessagePartEncoded =>
            part.type === 'text'
              ? { type: 'text', text: part.text }
              : { type: 'file', mediaType: part.mimeType, data: part.data },
          ),
        },
      ]
    case 'assistant':
      if (!answered.has(message.stopReason) || message.content.length === 0) return []
      return [{ role: 'assistant', content: message.content.map(assistantPart) }]
    case 'toolResult':
      return [{ role: 'tool', content: [toolResult(message)] }]
    default:
      return []
  }
}

/** The prompt for `messages`; without `system`, it leaves the system message out. */
export const prompt = (
  messages: ReadonlyArray<Message>,
  system = true,
): ReadonlyArray<Prompt.MessageEncoded> =>
  Transcript.collapse(messages).flatMap((message) =>
    message.role === 'system' && !system ? [] : messageOf(message),
  )

const declare = (definition: ToolDefinition) =>
  Tool.dynamic(definition.name, {
    description: definition.description,
    parameters: definition.parameters,
  })

/** Tools declared by JSON Schema: their parameters stay as the model sent them. */
export type Tools = Record<string, ReturnType<typeof declare>>

/**
 * The transcript's current tools as a toolkit that only declares them: tool call resolution is
 * off, so our loop runs the calls, and its handler is never called.
 */
export const toolkit = (messages: ReadonlyArray<Message>): Toolkit.WithHandler<Tools> => {
  const tools: Tools = {}
  for (const definition of Transcript.tools(messages)) tools[definition.name] = declare(definition)
  return {
    tools,
    handle: (name) =>
      Effect.fail(
        AiError.make({
          module: 'LanguageModelProvider',
          method: 'handle',
          reason: new AiError.ToolNotFoundError({ toolName: name, availableTools: [] }),
        }),
      ),
  }
}
