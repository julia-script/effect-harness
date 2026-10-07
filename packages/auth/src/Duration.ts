/**
 * Guarded normalization of native and external duration inputs.
 *
 * @since 0.0.0
 */
import * as Duration from 'effect/Duration'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import { AuthConfigurationError, AuthError } from './Credential.ts'

/**
 * Parses a finite option span once and copies native representations into a trusted Duration.
 *
 * @category constructors
 * @since 0.0.0
 */
export const fromInput = Effect.fnUntraced(function* (
  input: Duration.Input,
  message: string,
): Effect.fn.Return<Duration.Duration, AuthError> {
  const invalid = (cause?: unknown) =>
    new AuthError({
      reason: new AuthConfigurationError({
        message,
        ...(cause === undefined ? {} : { cause }),
      }),
    })
  if (typeof input === 'number' && !Number.isFinite(input)) return yield* invalid()
  const decoded = yield* Effect.try({
    // Native fromInput recognizes TypeId alone. Match under the same guard so
    // malformed foreign values/getters cannot escape as defects; retain exact nanos.
    try: () =>
      Option.flatMap(
        Duration.fromInput(input),
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
