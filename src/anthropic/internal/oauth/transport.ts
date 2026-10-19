/** Native response decoding with Claude wire names restored to application tool names. */
import { AnthropicClient } from '@effect/ai-anthropic'
import { Effect, Stream } from 'effect'
import { AiError } from 'effect/ai'
import type { Request } from '../../../model/Model.ts'
import { prepare } from './payload.ts'

const restoreStream = (
  events: Stream.Stream<AnthropicClient.MessageStreamEvent, AiError.AiError>,
  originalName: (name: string) => string,
) =>
  Stream.suspend(() => {
    let terminal = false
    return events.pipe(
      Stream.map((event) => {
        if (event.type === 'message_stop') terminal = true
        if (event.type === 'content_block_start' && event.content_block.type === 'tool_use')
          return {
            ...event,
            content_block: {
              ...event.content_block,
              name: originalName(event.content_block.name),
            },
          }
        return event
      }),
      Stream.concat(
        Stream.fromEffect(
          Effect.suspend(() =>
            terminal
              ? Effect.void
              : Effect.fail(
                  new AiError.AiError({
                    module: 'AnthropicOAuth',
                    method: 'messages',
                    reason: new AiError.UnknownError({
                      description: 'Claude stream ended before message_stop',
                    }),
                  }),
                ),
          ),
        ).pipe(Stream.drain),
      ),
    )
  })

/** Wrap the native codec and error mapper instead of maintaining a second SSE implementation. */
export const client = (
  native: AnthropicClient.Service,
  request: Request,
): AnthropicClient.Service =>
  AnthropicClient.AnthropicClient.of({
    ...native,
    createMessage: Effect.fnUntraced(function* (input) {
      const { prepared, originalName } = yield* prepare(input, request)
      const [message, response] = yield* native.createMessage(prepared)
      return [
        {
          ...message,
          content: message.content.map((block) =>
            block.type === 'tool_use'
              ? Object.assign({}, block, { name: originalName(block.name) })
              : block,
          ),
        },
        response,
      ]
    }),
    createMessageStream: Effect.fnUntraced(function* (input) {
      const { prepared, originalName } = yield* prepare(input, request)
      const [response, events] = yield* native.createMessageStream(prepared)
      return [response, restoreStream(events, originalName)]
    }),
  })
