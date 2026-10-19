import * as Schema from 'effect/Schema'

/** Stable client failure that can be encoded independently of backend implementations. */
export class HarnessError extends Schema.TaggedError<HarnessError>()('HarnessError', {
  reason: Schema.Literals(['invalid', 'notFound', 'closed', 'failed', 'revoked', 'conflict']),
  operation: Schema.String,
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}
