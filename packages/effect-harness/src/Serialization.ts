import * as Function from 'effect/Function'
import { dual } from 'effect/Function'
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
 * Serializes a value to JSON while retaining conversion failures as SerializationError.
 *
 * @category combinators
 */
export const stringify = (self: unknown): Result.Result<string, SerializationError> =>
  Result.try({
    try: () => JSON.stringify(self),
    catch: (cause) => new SerializationError({ message: 'Value cannot be encoded as JSON', cause }),
  }).pipe(
    Result.flatMap((text) =>
      text === undefined
        ? Result.fail(new SerializationError({ message: 'Value has no JSON representation' }))
        : Result.succeed(text),
    ),
  )

/**
 * Returns literal strings or a JSON display with a marker for failed conversions.
 *
 * @category combinators
 */
export const display = (self: unknown): string =>
  typeof self === 'string'
    ? self
    : Result.getOrElse(stringify(self), Function.constant(unencodable))

/**
 * Runs a synchronous operation and retains its failure as SerializationError.
 *
 * @category combinators
 */
export const attempt = <A>(operation: () => A): Result.Result<A, SerializationError> =>
  Result.try({
    try: operation,
    catch: (cause) => new SerializationError({ message: 'Value cannot be rendered', cause }),
  })

/**
 * Returns synchronous display text or the unencodable marker when its callback fails.
 *
 * @category combinators
 */
export const textOrMarker = (operation: () => string): string =>
  Result.getOrElse(attempt(operation), Function.constant(unencodable))

/**
 * Returns guarded error text while retaining readable string messages.
 *
 * @category combinators
 */
export const errorText = (self: unknown): string =>
  textOrMarker(() => {
    if (self instanceof Error) return typeof self.message === 'string' ? self.message : unencodable
    return display(self)
  })

/**
 * A missing or unreadable foreign code is unclassified; never coerce its object value.
 *
 * @category combinators
 */
const stringPropertyImpl = (self: unknown, key: string): string | undefined =>
  Result.getOrElse(
    Result.try({
      try: () => {
        if ((typeof self !== 'object' || self === null) && typeof self !== 'function')
          return undefined
        const property: unknown = Reflect.get(self, key)
        return typeof property === 'string' ? property : undefined
      },
      catch: (cause) =>
        new SerializationError({ message: 'Foreign metadata is unreadable', cause }),
    }),
    constUndefined,
  )

/** Returns a guarded string property, or undefined when unavailable.
 * @category combinators
 */
export const stringPropertyOrUndefined: {
  (key: string): (self: unknown) => string | undefined
  (self: unknown, key: string): string | undefined
} = dual(2, stringPropertyImpl)
