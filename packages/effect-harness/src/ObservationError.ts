/** Errors in the optional observation protocol, separate from producer failures. */
import * as Schema from 'effect/Schema'

export class ObservationError extends Schema.TaggedError<ObservationError>()('ObservationError', {
  reason: Schema.Literals([
    'unsupported',
    'notFound',
    'closed',
    'revoked',
    'invalid',
    'continuity',
    'projection',
  ]),
  operation: Schema.String,
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}
