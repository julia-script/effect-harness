/** Volatile storage of opaque commit frames and named payload streams. */
import * as Chunk from 'effect/Chunk'
import * as Effect from 'effect/Effect'
import * as HashMap from 'effect/HashMap'
import * as Option from 'effect/Option'
import * as Ref from 'effect/Ref'
import type { Persistence } from './Persistence.js'

export const make = Effect.gen(function* () {
  const state = yield* Ref.make({
    frames: Chunk.empty<string>(),
    streams: HashMap.empty<string, Chunk.Chunk<string>>(),
  })
  return {
    checkWrite: Effect.void,
    accept: Effect.void,
    journal: Ref.get(state).pipe(Effect.map(({ frames }) => Array.from(frames))),
    keys: Ref.get(state).pipe(Effect.map(({ streams }) => Array.from(HashMap.keys(streams)))),
    read: (key) =>
      Ref.get(state).pipe(
        Effect.map(({ streams }) =>
          Array.from(Option.getOrElse(HashMap.get(streams, key), Chunk.empty<string>)),
        ),
      ),
    append: (batch) =>
      Ref.update(state, ({ frames, streams }) => ({
        frames: Chunk.append(frames, batch.frame),
        streams: HashMap.mutate(streams, (map) => {
          for (const { key, lines } of batch.append) {
            const previous = Option.getOrElse(HashMap.get(map, key), Chunk.empty<string>)
            HashMap.set(map, key, Chunk.appendAll(previous, Chunk.fromIterable(Array.from(lines))))
          }
        }),
      })),
    replace: ({ key, lines }) =>
      Ref.update(state, ({ frames, streams }) => ({
        frames,
        streams:
          lines.length === 0
            ? HashMap.remove(streams, key)
            : HashMap.set(streams, key, Chunk.fromIterable(Array.from(lines))),
      })),
  } satisfies Persistence
})
