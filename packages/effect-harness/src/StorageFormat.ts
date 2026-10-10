/** Deliberate activation of persisted container formats, independent of task fields. */
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import { Storage } from './Storage.js'
import type { StorageError } from './StorageError.js'

export interface Format {
  readonly formatVersion: number
  readonly minimumReaderVersion: number
}

/** This reader understands legacy containers and explicitly activated v2/v3 containers. */
export const readerVersion = 3

export class StorageFormatError extends Schema.TaggedError<StorageFormatError>()(
  'StorageFormatError',
  {
    reason: Schema.Literals(['unavailable', 'unsupported']),
    message: Schema.String,
  },
) {}

export type Failure = StorageError | StorageFormatError

/**
 * Custom adapters must supply this capability for the exact Storage service they provide.
 * inspect is read-only. requireSupportedFormat must validate, serialize with domain writes,
 * persist a monotone container/minimum-reader marker, and settle durability before returning.
 * An uncertain write must revoke further use until a cold reopen reconciles persisted bytes.
 */
export class StorageFormat extends Context.Service<
  StorageFormat,
  {
    readonly storage: Storage['Service']
    readonly inspect: Effect.Effect<Format, Failure>
    readonly requireSupportedFormat: (format: Format) => Effect.Effect<Format, Failure>
  }
>()('effect-harness/StorageFormat') {}

const capability = Effect.gen(function* () {
  const storage = yield* Storage
  const format = yield* Effect.serviceOption(StorageFormat)
  if (Option.isNone(format) || format.value.storage !== storage)
    return yield* new StorageFormatError({
      reason: 'unavailable',
      message: 'This Storage adapter must explicitly supply its matching StorageFormat capability',
    })
  return format.value
})

/** Reads the current container marker without upgrading it. */
export const inspect: Effect.Effect<Format, Failure, Storage> = capability.pipe(
  Effect.flatMap((format) => format.inspect),
)

/**
 * Explicitly enables a supported format before emitting or executing work that requires it.
 * Built-ins preserve ID/sequence counters and never downgrade. v2 persists format=2, so
 * the previous format=1 reader schema rejects on reopen before replaying tasks.
 * v3 enables compact document splices and rejects format2 readers before replay.
 *
 * This is a reopen barrier, not fencing of foreign writers that already opened the store.
 * Stop old processes before activation. JSONL requires one owning writer per file;
 * SQL coordinates current writers with its metadata row lock. Unknown outcomes require
 * closing/reopening the backend; a failed guard never authorizes new-format work.
 */
export const requireSupportedFormat = Effect.fn('StorageFormat.requireSupportedFormat')(function* (
  format: Format,
) {
  const requested = yield* validate(format)
  return yield* (yield* capability).requireSupportedFormat(requested)
})

/** Validation shared by built-in capability methods and the public guard. */
export const validate = (format: Format): Effect.Effect<Format, StorageFormatError> =>
  Schema.decodeUnknownEffect(
    Schema.Struct({
      formatVersion: Schema.Literals([1, 2, 3]),
      minimumReaderVersion: Schema.Literals([1, 2, 3]),
    }).check(Schema.makeFilter((marker) => marker.formatVersion === marker.minimumReaderVersion)),
  )(format).pipe(
    Effect.mapError(
      () =>
        new StorageFormatError({
          reason: 'unsupported',
          message: 'Only container/reader pairs 1/1, 2/2 and 3/3 are supported',
        }),
    ),
    Effect.map(({ formatVersion, minimumReaderVersion }) => ({
      formatVersion,
      minimumReaderVersion,
    })),
  )
