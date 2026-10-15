import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as EventJournal from 'effect/eventlog/EventJournal'

// Test-side wire decoder: assertions inspect persisted bytes through the public journal.
const Frame = Schema.fromJsonString(
  Schema.Struct({
    version: Schema.Finite,
    revision: Schema.Finite,
    previous: Schema.NullOr(Schema.String),
    metadata: Schema.Struct({
      format: Schema.Finite,
      minimumReader: Schema.optionalKey(Schema.Finite),
      nextId: Schema.Finite,
      nextSeq: Schema.Finite,
    }),
    rows: Schema.Array(Schema.Unknown),
  }),
)
export const frames = Effect.gen(function* () {
  const journal = yield* EventJournal.EventJournal
  return yield* Effect.forEach(yield* journal.entries, (entry) =>
    Schema.decodeEffect(Frame)(new TextDecoder().decode(entry.payload)).pipe(
      Effect.map((frame) => ({ ...frame, id: entry.idString })),
    ),
  )
})
export const append = (value: unknown) =>
  Effect.gen(function* () {
    const journal = yield* EventJournal.EventJournal
    return yield* journal.write({
      event: 'effect-harness/commit',
      primaryKey: 'default',
      payload: new TextEncoder().encode(JSON.stringify(value)),
      effect: (entry) => Effect.succeed(entry.idString),
    })
  })
