/**
 * Guarded normalization of native and external duration inputs.
 */
import * as Duration from 'effect/Duration'
import { dual } from 'effect/Function'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import { AuthConfigurationError, AuthError } from './AuthError.ts'

/**
 * Parses a finite option span once and copies native representations into a trusted Duration.
 *
 * @category constructors
 */
const fromInputImpl = Effect.fnUntraced(function* (
  self: Duration.Input,
  message: string,
): Effect.fn.Return<Duration.Duration, AuthError> {
  const invalid = (cause?: unknown) =>
    new AuthError({
      reason: new AuthConfigurationError({
        message,
        ...(cause === undefined ? {} : { cause }),
      }),
    })
  if (typeof self === 'number' && !Number.isFinite(self)) return yield* invalid()
  const decoded = yield* Effect.try({
    // Native fromInput recognizes TypeId alone. Match under the same guard so
    // malformed foreign values/getters cannot escape as defects; retain exact nanos.
    try: () =>
      Option.flatMap(
        Duration.fromInput(self),
        (value) =>
          Duration.match(value, {
            onMillis: (millis) =>
              Number.isFinite(millis) ? Option.some(Duration.millis(millis)) : Option.none(),
            onNanos: (nanos) =>
              typeof nanos === 'bigint' ? Option.some(Duration.nanos(nanos)) : Option.none(),
            onInfinity: () => Option.none(),
            onNegativeInfinity: () => Option.none(),
          }) ?? Option.none(),
      ),
    catch: invalid,
  })
  return yield* Effect.fromOption(decoded, invalid)
})

/** Parses a finite duration input with a caller-owned diagnostic message.
 * @category constructors
 */
export const fromInput: {
  (message: string): (self: Duration.Input) => Effect.Effect<Duration.Duration, AuthError>
  (self: Duration.Input, message: string): Effect.Effect<Duration.Duration, AuthError>
} = dual(2, fromInputImpl)
