import * as Schema from 'effect/Schema'
import type * as Session from './Session.js'

/** Expected runtime failures. Storage and document codec failures retain their own types. */
export class ExecutionError extends Schema.TaggedError<ExecutionError>()('ExecutionError', {
  reason: Schema.Literals(['invalid', 'notFound', 'unsupported', 'closed', 'revoked']),
  operation: Schema.String,
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

export type Failure = ExecutionError | Session.Failure
