/**
 * Typed arbitrary-value conversion and explicit unencodable display markers.
 */
import { constUndefined } from 'effect/Function'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'

/**
 * Semantic serialization error with its retained cause.
 *
 * @category errors
 */
export class SerializationError extends Schema.TaggedError<SerializationError>(
  '@effect-harness/harness/SerializationError',
)('SerializationError', {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {}

/**
 * Display and heuristic estimation retain a visible marker for values JSON cannot encode.
 *
 * @category constants
 */
export const unencodable = '[unencodable value]'

/**
 * Preserve JSON conversion failures without invoking arbitrary object coercion.
 *
 * @category combinators
 */
export const stringify = (value: unknown): Result.Result<string, SerializationError> =>
  Result.try({
    try: () => JSON.stringify(value),
    catch: (cause) => new SerializationError({ message: 'Value cannot be encoded as JSON', cause }),
  }).pipe(
    Result.flatMap((text) =>
      text === undefined
        ? Result.fail(new SerializationError({ message: 'Value has no JSON representation' }))
        : Result.succeed(text),
    ),
  )

/**
 * Best-effort display preserves literal strings and marks all failed conversions.
 *
 * @category combinators
 */
export const display = (value: unknown): string =>
  typeof value === 'string' ? value : Result.getOrElse(stringify(value), () => unencodable)

/**
 * Convert a foreign synchronous operation into an explicit serialization failure.
 *
 * @category combinators
 */
export const attempt = <A>(operation: () => A): Result.Result<A, SerializationError> =>
  Result.try({
    try: operation,
    catch: (cause) => new SerializationError({ message: 'Value cannot be rendered', cause }),
  })

/**
 * Guard property access and other foreign display callbacks as well as JSON conversion.
 *
 * @category combinators
 */
export const textOrMarker = (operation: () => string): string =>
  Result.getOrElse(attempt(operation), () => unencodable)

/**
 * Error diagnostics retain string messages; foreign getters are guarded before rendering.
 *
 * @category combinators
 */
export const errorText = (value: unknown): string =>
  textOrMarker(() => {
    if (value instanceof Error)
      return typeof value.message === 'string' ? value.message : unencodable
    return display(value)
  })

/**
 * A missing or unreadable foreign code is unclassified; never coerce its object value.
 *
 * @category combinators
 */
export const stringProperty = (value: unknown, key: string): string | undefined =>
  Result.getOrElse(
    Result.try({
      try: () => {
        if ((typeof value !== 'object' || value === null) && typeof value !== 'function')
          return undefined
        const property: unknown = Reflect.get(value, key)
        return typeof property === 'string' ? property : undefined
      },
      catch: (cause) =>
        new SerializationError({ message: 'Foreign metadata is unreadable', cause }),
    }),
    constUndefined,
  )
