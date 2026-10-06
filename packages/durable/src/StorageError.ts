import * as Schema from 'effect/Schema'

/** A persistence or domain failure, with an explicit guarantee about durable effects. */
export class StorageError extends Schema.TaggedError<StorageError>()('StorageError', {
  reason: Schema.Literals([
    'invalid',
    'conflict',
    'not_found',
    'closed',
    'poisoned',
    'corrupt',
    'io',
    'read_after_write',
    'revoked',
  ]),
  certainty: Schema.Literals(['rejected', 'uncertain']),
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Unknown),
}) {}

export const rejected = (
  message: string,
  reason: StorageError['reason'] = 'invalid',
  cause?: unknown,
) =>
  new StorageError({
    reason,
    certainty: 'rejected',
    message,
    ...(cause === undefined ? {} : { cause }),
  })

export const uncertain = (message: string, cause?: unknown) =>
  new StorageError({
    reason: 'io',
    certainty: 'uncertain',
    message,
    ...(cause === undefined ? {} : { cause }),
  })
