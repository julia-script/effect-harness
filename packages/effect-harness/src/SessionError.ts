import * as Schema from 'effect/Schema'
import type { StorageError } from './StorageError.js'

/** Session and transaction failures; persistence failures retain their storage error. */
export class SessionError extends Schema.TaggedError<SessionError>()('SessionError', {
  reason: Schema.Literals(['invalid', 'conflict', 'notFound', 'closed', 'revoked', 'overflow']),
  operation: Schema.String,
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

/** Schema errors remain available to callers of typed document operations. */
export type Failure = SessionError | StorageError | Schema.SchemaError
