import * as Schema from 'effect/Schema'

/** Expected validation, lifecycle, corruption and I/O failures at the storage boundary. */
export class StorageError extends Schema.TaggedError<StorageError>()('StorageError', {
  reason: Schema.Literals([
    'invalid',
    'conflict',
    'notFound',
    'corrupt',
    'io',
    'closed',
    'uncertain',
  ]),
  operation: Schema.String,
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

export const make = (
  reason: StorageError['reason'],
  operation: string,
  message: string,
  cause?: unknown,
) => new StorageError({ reason, operation, message, ...(cause === undefined ? {} : { cause }) })

/** Maps caller schema failures to the storage error channel. */
export const invalid = (operation: string) => (cause: unknown) =>
  make('invalid', operation, `Invalid ${operation} data`, cause)

/** Maps persisted schema failures to the storage error channel. */
export const corrupt = (operation: string) => (cause: unknown) =>
  make('corrupt', operation, `Invalid stored ${operation} data`, cause)
