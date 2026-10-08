/** Process-local atomic persistence for experiments and conformance tests. */
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import type * as Scope from 'effect/Scope'
import { Persistence, type Service, type Metadata } from '../Persistence.ts'
import * as records from './internal/records.ts'
import { detachedEffect } from '../internal/records.ts'

export const make: Effect.Effect<Service, never, Scope.Scope> = Effect.gen(function* () {
  let metadata: Metadata = { revision: 0, nextId: 2 }
  const rows = new Map<number, records.Row>()
  return yield* records.make({
    metadata: Effect.sync(() => ({ ...metadata })),
    get: (id) =>
      Effect.suspend(() => {
        const row = rows.get(id)
        return row === undefined ? Effect.succeedNone : detachedEffect(row).pipe(Effect.asSome)
      }),
    page: (kind, after, limit, filter) =>
      Effect.suspend(() =>
        detachedEffect(
          [...rows.values()]
            .filter(
              (row) =>
                row._tag === kind && records.idOf(row) > after && records.matches(row, filter),
            )
            .sort((a, b) => records.idOf(a) - records.idOf(b))
            .slice(0, limit),
        ),
      ),
    save: (batch, next) =>
      detachedEffect(batch).pipe(
        Effect.map((detached) => {
          for (const row of detached) rows.set(records.idOf(row), row)
          metadata = { ...next }
        }),
      ),
  })
})
export const layer: Layer.Layer<Persistence> = Layer.effect(Persistence, make)
