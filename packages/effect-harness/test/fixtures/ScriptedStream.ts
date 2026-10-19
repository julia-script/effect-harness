/** Test-provider construction: the script runs once inside the native stream callback. */
import * as Effect from 'effect/Effect'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import type * as Response from 'effect/ai/Response'

export const parts = (content: ReadonlyArray<Response.PartEncoded>): Response.StreamPartEncoded[] =>
  content.flatMap((part, index): Response.StreamPartEncoded[] => {
    if (part.type !== 'text' && part.type !== 'reasoning') return [part]
    const id = `${part.type}-${index}`
    return part.type === 'text'
      ? [
          { type: 'text-start', id, metadata: part.metadata },
          { type: 'text-delta', id, delta: part.text, metadata: part.metadata },
          { type: 'text-end', id, metadata: part.metadata },
        ]
      : [
          { type: 'reasoning-start', id, metadata: part.metadata },
          { type: 'reasoning-delta', id, delta: part.text },
          { type: 'reasoning-end', id },
        ]
  })
export const make = (options: Omit<Parameters<typeof LanguageModel.make>[0], 'streamText'>) =>
  LanguageModel.make({
    ...options,
    streamText: (request) =>
      Stream.unwrap(
        options
          .generateText(request)
          .pipe(Effect.map((content) => Stream.fromIterable(parts(content)))),
      ),
  })
