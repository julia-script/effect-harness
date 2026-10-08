import * as Predicate from 'effect/Predicate'
import * as Function from 'effect/Function'
/**
 * Descriptor-based JSON validation for durable receipt values.
 */
import * as Effect from 'effect/Effect'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import type * as SchemaAST from 'effect/SchemaAST'
import * as SchemaGetter from 'effect/SchemaGetter'
import * as SchemaIssue from 'effect/SchemaIssue'

/** Descriptor checks and copying never read a source property, including Proxy get traps. */
const inspect = (
  input: unknown,
  options: SchemaAST.ParseOptions,
): Result.Result<Schema.Json, SchemaIssue.Issue> => {
  const issue = (message: string) => new SchemaIssue.InvalidValue({ message }, input, options)
  const reflected = Result.try({
    try: () => {
      const visited = new Set<object>()
      const visit = (value: unknown): Result.Result<Schema.Json, SchemaIssue.Issue> => {
        if (value === null || typeof value === 'string' || typeof value === 'boolean')
          return Result.succeed(value)
        if (typeof value === 'number' && Number.isFinite(value)) return Result.succeed(value)
        if (typeof value !== 'object' || value === null)
          return Result.fail(issue('Receipt results must be JSON or void'))
        if (visited.has(value)) return Result.fail(issue('Receipt results cannot be cyclic'))
        const array = Array.isArray(value)
        if (!array) {
          const prototype = Object.getPrototypeOf(value)
          if (prototype !== Object.prototype && prototype !== null)
            return Result.fail(issue('Receipt results cannot contain service or class instances'))
        }
        visited.add(value)
        const output: Schema.Json = array ? [] : {}
        const length = array ? Object.getOwnPropertyDescriptor(value, 'length')?.value : undefined
        if (array && (typeof length !== 'number' || !Number.isSafeInteger(length) || length < 0))
          return Result.fail(issue('Receipt arrays require a valid length'))
        let indices = 0
        for (const key of Reflect.ownKeys(value)) {
          if (array && key === 'length') continue
          const descriptor = Object.getOwnPropertyDescriptor(value, key)
          if (
            typeof key !== 'string' ||
            descriptor?.enumerable !== true ||
            !Predicate.hasProperty(descriptor, 'value')
          )
            return Result.fail(issue('Receipt results require enumerable JSON data properties'))
          if (array) {
            const index = Number(key)
            if (
              !Number.isSafeInteger(index) ||
              index < 0 ||
              index >= length ||
              String(index) !== key
            )
              return Result.fail(issue('Receipt arrays cannot have named properties'))
            indices++
          }
          const copied = visit(descriptor.value)
          if (Result.isFailure(copied)) return copied
          Object.defineProperty(output, key, {
            value: copied.success,
            enumerable: true,
            writable: true,
            configurable: true,
          })
        }
        if (array && indices !== length)
          return Result.fail(issue('Receipt arrays cannot have holes'))
        visited.delete(value)
        return Result.succeed(output)
      }
      return visit(input)
    },
    catch: () => issue('Cannot inspect receipt JSON'),
  })
  return Result.isFailure(reflected) ? Result.fail(reflected.failure) : reflected.success
}
const policy = Schema.makeFilter<unknown>((input, _ast, options) => {
  const result = inspect(input, options)
  return Result.isFailure(result) ? result.failure : undefined
})
/**
 * Policy runs on Unknown before Json; Json sees only descriptor-copied data, never source getters.
 *
 * @category schemas
 */
export const StrictReceiptJson = Schema.Unknown.check(policy).pipe(
  Schema.decodeTo(Schema.Json, {
    decode: SchemaGetter.transformEffect((input, options) =>
      Effect.fromResult(inspect(input, options)),
    ),
    encode: SchemaGetter.transform(Function.identity),
  }),
)
/**
 * Decoded value validated by the `StrictReceiptJson` schema.
 *
 * @category models
 */
export type StrictReceiptJson = typeof StrictReceiptJson.Type
