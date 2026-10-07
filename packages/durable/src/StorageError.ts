/**
 * Structured storage failure reasons and certainty projections.
 *
 * @since 0.0.0
 */
import * as Schema from 'effect/Schema'
import * as Effect from 'effect/Effect'

// Defect JSON preserves message/cause chains but reconstructs generic Errors;
// native subclasses, custom properties and original stacks are not a wire contract.
const fields = { message: Schema.String, cause: Schema.optionalKey(Schema.Defect()) }

/**
 * Invalid schema.
 *
 * @category errors
 * @since 0.0.0
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
 * Conflict schema.
 *
 * @category errors
 * @since 0.0.0
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
 * NotFound schema.
 *
 * @category errors
 * @since 0.0.0
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
 * Closed schema.
 *
 * @category errors
 * @since 0.0.0
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
 * Poisoned schema.
 *
 * @category errors
 * @since 0.0.0
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
 * Corrupt schema.
 *
 * @category errors
 * @since 0.0.0
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
 * Io schema.
 *
 * @category errors
 * @since 0.0.0
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
 * ReadAfterWrite schema.
 *
 * @category errors
 * @since 0.0.0
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
 * Revoked schema.
 *
 * @category errors
 * @since 0.0.0
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
 * StorageErrorReason schema.
 *
 * @category schemas
 * @since 0.0.0
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
 * StorageErrorReason contract.
 *
 * @category models
 * @since 0.0.0
 */
export type StorageErrorReason = typeof StorageErrorReason.Type

/**
 * A domain or persistence failure. An uncertain write must be reconciled, never automatically retried.
 *
 * @category errors
 * @since 0.0.0
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
 * @since 0.0.0
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
 * @since 0.0.0
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
 * LegacyReason contract.
 *
 * @category models
 * @since 0.0.0
 */
export type LegacyReason = keyof typeof legacyReasons
/**
 * Input-only adapter for older callers; the runtime reason is always structured.
 *
 * @category combinators
 * @since 0.0.0
 */
export const rejectedLegacy = (
  message: string,
  reason: LegacyReason = 'invalid',
  cause?: unknown,
): StorageError => rejected(message, legacyReasons[reason], cause)
