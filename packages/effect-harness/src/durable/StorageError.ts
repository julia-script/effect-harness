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
export class Invalid extends Schema.TaggedError<Invalid>(
  '@effect-harness/durable/StorageError/Invalid',
)('Invalid', fields) {
  readonly certainty = 'rejected' as const
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Failure reporting an incompatible update or reused receipt fingerprint.
 *
 * @category errors
 */
export class Conflict extends Schema.TaggedError<Conflict>(
  '@effect-harness/durable/StorageError/Conflict',
)('Conflict', fields) {
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
export class NotFound extends Schema.TaggedError<NotFound>(
  '@effect-harness/durable/StorageError/NotFound',
)('NotFound', fields) {
  readonly certainty = 'rejected' as const
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Failure reporting an operation admitted after the Store or Session was sealed.
 *
 * @category errors
 */
export class Closed extends Schema.TaggedError<Closed>(
  '@effect-harness/durable/StorageError/Closed',
)('Closed', fields) {
  readonly certainty = 'rejected' as const
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Failure reporting an operation against a Store with an unresolved write outcome.
 *
 * @category errors
 */
export class Poisoned extends Schema.TaggedError<Poisoned>(
  '@effect-harness/durable/StorageError/Poisoned',
)('Poisoned', fields) {
  readonly certainty = 'rejected' as const
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Failure reporting invalid persisted state, receipts or journal frames.
 *
 * @category errors
 */
export class Corrupt extends Schema.TaggedError<Corrupt>(
  '@effect-harness/durable/StorageError/Corrupt',
)('Corrupt', fields) {
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
export class Io extends Schema.TaggedError<Io>('@effect-harness/durable/StorageError/Io')('Io', {
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
 * Failure reporting a table read after table writes in one transaction.
 *
 * @category errors
 */
export class ReadAfterWrite extends Schema.TaggedError<ReadAfterWrite>(
  '@effect-harness/durable/StorageError/ReadAfterWrite',
)('ReadAfterWrite', fields) {
  readonly certainty = 'rejected' as const
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Failure reporting use of a transaction or draft after its callback ended.
 *
 * @category errors
 */
export class Revoked extends Schema.TaggedError<Revoked>(
  '@effect-harness/durable/StorageError/Revoked',
)('Revoked', fields) {
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
  Invalid,
  Conflict,
  NotFound,
  Closed,
  Poisoned,
  Corrupt,
  Io,
  ReadAfterWrite,
  Revoked,
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
 * storage and inspecting the receipt. Serialized causes preserve diagnostics rather than
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
  /** Original discriminator for compatibility records; runtime policies match reason._tag. */
  get code(): LegacyReason {
    return reasonCodes[this.reason._tag]
  }
  /** No storage failure currently has a documented automatic retry policy. */
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
  Reason: ReasonConstructor = Invalid,
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
    reason: new Io({ message, certainty: 'uncertain', ...(cause === undefined ? {} : { cause }) }),
  })

const legacyReasons = {
  invalid: Invalid,
  conflict: Conflict,
  not_found: NotFound,
  closed: Closed,
  poisoned: Poisoned,
  corrupt: Corrupt,
  io: Io,
  read_after_write: ReadAfterWrite,
  revoked: Revoked,
} as const
const reasonCodes = {
  Invalid: 'invalid',
  Conflict: 'conflict',
  NotFound: 'not_found',
  Closed: 'closed',
  Poisoned: 'poisoned',
  Corrupt: 'corrupt',
  Io: 'io',
  ReadAfterWrite: 'read_after_write',
  Revoked: 'revoked',
} as const
/**
 * Compatibility code translated into a structured storage failure reason.
 *
 * @category models
 */
export type LegacyReason = keyof typeof legacyReasons
/**
 * Input-only adapter for older callers; the runtime reason is always structured.
 *
 * @category combinators
 */
export const rejectedLegacy = (
  message: string,
  reason: LegacyReason = 'invalid',
  cause?: unknown,
): StorageError => rejected(message, legacyReasons[reason], cause)
