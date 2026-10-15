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

/** This reader accepts runtime container/minimum-reader 12/12 only. */
export const readerVersion = 12

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
 * Requires runtime wire format 12/12 before domain admission or task replay.
 * Built-in stores start with this marker and reject older nonempty containers without migration.
 * Custom adapters must establish the matching marker durably before publishing PromptWire data.
 * An uncertain write revokes further use until cold reopen; this guard does not fence foreign
 * writers already holding an open store. Stop old processes before replacing a namespace.
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
      formatVersion: Schema.Literals([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]),
      minimumReaderVersion: Schema.Literals([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]),
    }).check(Schema.makeFilter((marker) => marker.formatVersion === marker.minimumReaderVersion)),
  )(format).pipe(
    Effect.mapError(
      () =>
        new StorageFormatError({
          reason: 'unsupported',
          message:
            'Only matching requirements 1 through 12 are supported; built-in containers use 12/12',
        }),
    ),
    Effect.map(({ formatVersion, minimumReaderVersion }) => ({
      formatVersion,
      minimumReaderVersion,
    })),
  )
