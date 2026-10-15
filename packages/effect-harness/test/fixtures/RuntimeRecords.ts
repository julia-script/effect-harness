import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import type * as Storage from 'effect-harness/Storage'
import * as Request from '../../src/internal/RequestRecord.js'

export const preparation = Effect.fnUntraced(function* (
  storage: Storage.Storage['Service'],
  checkpoint: unknown,
) {
  const { preparation: ref } = yield* Schema.decodeUnknownEffect(
    Schema.Struct({ preparation: Request.ReferenceSchema }),
  )(checkpoint)
  const record = Option.getOrThrow(
    yield* storage.findDocument({
      kind: Request.document.definition.kind,
      scope: { _tag: 'task', taskId: ref.ownerTaskId },
      key: ref.requestId,
    }),
  )
  const stored = Option.getOrThrow(yield* storage.document(record.id))
  const value = yield* Schema.decodeEffect(Request.document.definition.schema)(stored.value)
  return Request.restore(Option.getOrThrow(Option.fromUndefinedOr(value.request)))
})
