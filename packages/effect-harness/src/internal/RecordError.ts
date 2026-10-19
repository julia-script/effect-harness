/** Private validation failures translated by the storage boundary. */
import * as Schema from 'effect/Schema'

const fields = { message: Schema.String, cause: Schema.optionalKey(Schema.Defect()) }
export class InvalidError extends Schema.TaggedError<InvalidError>()('InvalidError', fields) {}
export class ConflictError extends Schema.TaggedError<ConflictError>()('ConflictError', fields) {}
export class NotFoundError extends Schema.TaggedError<NotFoundError>()('NotFoundError', fields) {}
export class CorruptError extends Schema.TaggedError<CorruptError>()('CorruptError', fields) {}
const ReasonSchema = Schema.Union([InvalidError, ConflictError, NotFoundError, CorruptError])
export class StorageError extends Schema.TaggedError<StorageError>()('RecordError', {
  reason: ReasonSchema,
}) {
  override get message(): string {
    return this.reason.message
  }
  override get cause(): unknown {
    return this.reason.cause
  }
}
export const rejected = (
  message: string,
  Reason: new (props: {
    readonly message: string
    readonly cause?: unknown
  }) => typeof ReasonSchema.Type = InvalidError,
  cause?: unknown,
): StorageError =>
  new StorageError({ reason: new Reason({ message, ...(cause === undefined ? {} : { cause }) }) })
