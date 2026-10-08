/**
 * Structured storage failure reasons and certainty projections.
 */
import * as Schema from 'effect/Schema'
import * as Effect from 'effect/Effect'

// Defect JSON preserves message/cause chains but reconstructs generic Errors;
// native subclasses, custom properties and original stacks are not a wire contract.
const fields = { message: Schema.String, cause: Schema.optionalKey(Schema.Defect()) }

/**
 * Failure reporting invalid domain data or a rejected storage operation.
 *
 * @category errors
 */
export class InvalidError extends Schema.TaggedError<InvalidError>(
  '@effect-harness/durable/StorageError/InvalidError',
)('InvalidError', fields) {
  readonly certainty = 'rejected' as const
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Failure reporting an incompatible domain update.
 *
 * @category errors
 */
export class ConflictError extends Schema.TaggedError<ConflictError>(
  '@effect-harness/durable/StorageError/ConflictError',
)('ConflictError', fields) {
  readonly certainty = 'rejected' as const
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Failure reporting a required domain record that is absent.
 *
 * @category errors
 */
export class NotFoundError extends Schema.TaggedError<NotFoundError>(
  '@effect-harness/durable/StorageError/NotFoundError',
)('NotFoundError', fields) {
  readonly certainty = 'rejected' as const
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Failure reporting an operation admitted after Persistence or its Session was sealed.
 *
 * @category errors
 */
export class ClosedError extends Schema.TaggedError<ClosedError>(
  '@effect-harness/durable/StorageError/ClosedError',
)('ClosedError', fields) {
  readonly certainty = 'rejected' as const
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Failure reporting an operation against Persistence with an unresolved write outcome.
 *
 * @category errors
 */
export class PoisonedError extends Schema.TaggedError<PoisonedError>(
  '@effect-harness/durable/StorageError/PoisonedError',
)('PoisonedError', fields) {
  readonly certainty = 'rejected' as const
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Failure reporting invalid persisted records or journal frames.
 *
 * @category errors
 */
export class CorruptError extends Schema.TaggedError<CorruptError>(
  '@effect-harness/durable/StorageError/CorruptError',
)('CorruptError', fields) {
  readonly certainty = 'rejected' as const
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Persistence failure carrying whether its write outcome is rejected or uncertain.
 *
 * @category errors
 */
export class IoError extends Schema.TaggedError<IoError>(
  '@effect-harness/durable/StorageError/IoError',
)('IoError', {
  ...fields,
  certainty: Schema.Literals(['rejected', 'uncertain']).pipe(
    Schema.withConstructorDefault(Effect.succeed('rejected')),
  ),
}) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Failure reporting use of a transaction or draft after its callback ended.
 *
 * @category errors
 */
export class RevokedError extends Schema.TaggedError<RevokedError>(
  '@effect-harness/durable/StorageError/RevokedError',
)('RevokedError', fields) {
  readonly certainty = 'rejected' as const
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Schema for structured domain validation, lifecycle and persistence failures.
 *
 * @category schemas
 */
export const StorageErrorReason = Schema.Union([
  InvalidError,
  ConflictError,
  NotFoundError,
  ClosedError,
  PoisonedError,
  CorruptError,
  IoError,
  RevokedError,
])
/**
 * Decoded value validated by the `StorageErrorReason` schema.
 *
 * @category models
 */
export type StorageErrorReason = typeof StorageErrorReason.Type

/**
 * Domain or persistence failure with an explicit write-certainty classification.
 *
 * **Details**
 *
 * Match reason._tag to distinguish invalid input, conflicts, missing records, lifecycle
 * failures and I/O. certainty distinguishes rejected work from an outcome that may already
 * have committed.
 *
 * **Gotchas**
 *
 * No reason promises automatic retry. An uncertain outcome must be reconciled by reopening
 * storage and inspecting committed records. Serialized causes preserve diagnostics rather than
 * original native error identity or stack.
 *
 * @category errors
 */
export class StorageError extends Schema.TaggedError<StorageError>(
  '@effect-harness/durable/StorageError',
)('StorageError', { reason: StorageErrorReason }) {
  override get message(): string {
    return this.reason.message
  }
  override get cause(): unknown {
    return this.reason.cause
  }
  get certainty(): 'rejected' | 'uncertain' {
    return this.reason.certainty
  }
  /**
   * Automatic retry eligibility; no storage failure currently has a documented automatic retry policy.
   */
  get isRetryable(): boolean {
    return this.reason.isRetryable
  }
}

type ReasonConstructor = new (props: {
  readonly message: string
  readonly cause?: unknown
}) => StorageErrorReason
/**
 * Creates a rejected storage failure with its structured reason.
 *
 * @category combinators
 */
export const rejected = (
  message: string,
  Reason: ReasonConstructor = InvalidError,
  cause?: unknown,
): StorageError =>
  new StorageError({ reason: new Reason({ message, ...(cause === undefined ? {} : { cause }) }) })
/**
 * Creates a storage failure whose commit outcome is unknown.
 *
 * @category combinators
 */
export const uncertain = (message: string, cause?: unknown): StorageError =>
  new StorageError({
    reason: new IoError({
      message,
      certainty: 'uncertain',
      ...(cause === undefined ? {} : { cause }),
    }),
  })

/** Checks the decoded Invalid contract without decoding or coercing input.
 * @category guards
 */
export const isInvalid: (u: unknown) => u is InvalidError = Schema.is(Schema.toType(InvalidError))

/** Checks the decoded Conflict contract without decoding or coercing input.
 * @category guards
 */
export const isConflict: (u: unknown) => u is ConflictError = Schema.is(
  Schema.toType(ConflictError),
)

/** Checks the decoded NotFound contract without decoding or coercing input.
 * @category guards
 */
export const isNotFound: (u: unknown) => u is NotFoundError = Schema.is(
  Schema.toType(NotFoundError),
)

/** Checks the decoded Closed contract without decoding or coercing input.
 * @category guards
 */
export const isClosed: (u: unknown) => u is ClosedError = Schema.is(Schema.toType(ClosedError))

/** Checks the decoded Poisoned contract without decoding or coercing input.
 * @category guards
 */
export const isPoisoned: (u: unknown) => u is PoisonedError = Schema.is(
  Schema.toType(PoisonedError),
)

/** Checks the decoded Corrupt contract without decoding or coercing input.
 * @category guards
 */
export const isCorrupt: (u: unknown) => u is CorruptError = Schema.is(Schema.toType(CorruptError))

/** Checks the decoded Io contract without decoding or coercing input.
 * @category guards
 */
export const isIo: (u: unknown) => u is IoError = Schema.is(Schema.toType(IoError))

/** Checks the decoded Revoked contract without decoding or coercing input.
 * @category guards
 */
export const isRevoked: (u: unknown) => u is RevokedError = Schema.is(Schema.toType(RevokedError))

/** Checks the decoded StorageError contract without decoding or coercing input.
 * @category guards
 */
export const isStorageError: (u: unknown) => u is StorageError = Schema.is(
  Schema.toType(StorageError),
)

/** Checks the decoded StorageErrorReason contract without decoding or coercing input.
 * @category guards
 */
export const isStorageErrorReason: (u: unknown) => u is StorageErrorReason = Schema.is(
  Schema.toType(StorageErrorReason),
)
