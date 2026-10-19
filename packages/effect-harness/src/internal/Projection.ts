import * as Effect from 'effect/Effect'
import * as HashMap from 'effect/HashMap'
import * as Ref from 'effect/Ref'
import * as Semaphore from 'effect/Semaphore'
import type { Access } from './Kernel.js'
import * as Metadata from './Metadata.js'
import * as Row from './Row.js'

/** Each layer construction owns a distinct reference and mutation semaphore. */
export const makeAccess = Effect.gen(function* () {
  const state = yield* Ref.make({
    metadata: Metadata.initialMetadata,
    rows: HashMap.empty<number, Row.Stored>(),
  })
  const lock = yield* Semaphore.make(1)
  return {
    metadata: Ref.get(state).pipe(Effect.map((state) => ({ ...state.metadata }))),
    get: (id) => Ref.get(state).pipe(Effect.map((state) => HashMap.get(state.rows, id))),
    page: (kind, after, limit, filter, order) =>
      Ref.get(state).pipe(
        Effect.map((state) =>
          Array.from(HashMap.values(state.rows))
            .filter(
              (row) =>
                row._tag === kind &&
                Row.matches(row, filter) &&
                (after === undefined ||
                  (order === 'ascending' ? Row.idOf(row) > after : Row.idOf(row) < after)),
            )
            .sort((a, b) =>
              order === 'ascending' ? Row.idOf(a) - Row.idOf(b) : Row.idOf(b) - Row.idOf(a),
            )
            .slice(0, limit),
        ),
      ),
    save: (rows, metadata) =>
      Ref.update(state, (state) => {
        const next = HashMap.mutate(state.rows, (map) => {
          for (const row of rows) HashMap.set(map, Row.idOf(row), row)
        })
        return { metadata, rows: next }
      }),
    exclusive: (effect) => lock.withPermit(effect),
  } satisfies Access
})
