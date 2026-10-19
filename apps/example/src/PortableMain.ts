import * as Console from 'effect/Console'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Portable from './Portable.js'

await Effect.runPromise(
  Effect.scoped(Portable.program).pipe(
    Effect.flatMap(Schema.encodeEffect(Schema.fromJsonString(Portable.ResultSchema))),
    Effect.flatMap(Console.log),
  ),
)
