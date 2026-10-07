/**
 * Pinned model request document schema and initializer.
 *
 * @since 0.0.0
 */
import * as Executor from '@effect-harness/harness/Executor'
import * as Schema from 'effect/Schema'
import * as Document from '../Document.ts'

const State = Schema.Struct({
  request: Schema.toEncoded(Schema.toCodecJson(Executor.Request)),
  handle: Schema.optionalKey(Schema.Json),
})
/**
 * Inspectable, pinned request data for recovery and cancellation of deferred provider work.
 *
 * @category models
 * @since 0.0.0
 */
export const RequestDoc = Document.defineUnsafe({
  kind: 'harness.model-request',
  version: 1,
  scope: 'task',
  schema: State,
  // effect-review-allow P4-decode-effect-at-boundary: this synchronous document initializer may throw; Session.transaction catches its seed decoder with Effect.try before commit.
  initial: (seed) => Schema.decodeUnknownSync(State)({ request: seed }),
})
