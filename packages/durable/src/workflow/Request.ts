import * as Harness from '@effect-harness/harness/Executor'
import * as Schema from 'effect/Schema'
import * as Document from '../Document.ts'

const State = Schema.Struct({
  request: Schema.toEncoded(Schema.toCodecJson(Harness.Request)),
  handle: Schema.optionalKey(Schema.Json),
})
/** Inspectable, pinned request data for recovery and cancellation of deferred provider work. */
export const RequestDoc = Document.defineUnsafe({
  kind: 'harness.model-request',
  version: 1,
  scope: 'task',
  schema: State,
  initial: (seed) => Schema.decodeUnknownSync(State)({ request: seed }),
})
