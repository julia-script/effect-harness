import * as Predicate from 'effect/Predicate'
/**
 * Token and price ledgers with explicit partial-cost metadata.
 */
import { dual } from 'effect/Function'
import * as SchemaField from './SchemaField.ts'
import * as Schema from 'effect/Schema'
import * as Record from 'effect/Record'
import type * as Response from 'effect/ai/Response'

/**
 * Schema for known USD input, output and cache costs for reported usage.
 *
 * @category schemas
 */
export const Cost = Schema.Struct({
  /** False means the numeric amounts are a partial subtotal, not a complete price. */
  known: SchemaField.optional(Schema.Boolean),
  /** A transport can report the total without reporting its price components. */
  totalKnown: SchemaField.optional(Schema.Boolean),
  input: Schema.Finite,
  output: Schema.Finite,
  cacheRead: Schema.Finite,
  cacheWrite: Schema.Finite,
  total: Schema.Finite,
})
/**
 * Schema for known token and cost measurements for one model request.
 *
 * @category schemas
 */
export const Usage = Schema.Struct({
  input: Schema.Finite,
  output: Schema.Finite,
  cacheRead: Schema.Finite,
  cacheWrite: Schema.Finite,
  totalTokens: Schema.Finite,
  cacheWrite1h: SchemaField.optional(Schema.Finite),
  reasoning: SchemaField.optional(Schema.Finite),
  cost: Cost,
})
/**
 * Known token and cost measurements for one model request.
 *
 * @category models
 */
export type Usage = typeof Usage.Type
/**
 * Schema for accumulated model and tool usage ledgers.
 *
 * @category schemas
 */
export const State = Schema.Struct({
  models: Schema.Record(Schema.String, Usage),
  tools: Schema.Record(Schema.String, Usage),
})
/**
 * Accumulated model and tool usage ledgers.
 *
 * @category models
 */
export type State = typeof State.Type
/**
 * Creates usage with zero measured counters.
 *
 * @category constructors
 */
export const make = (): Usage => ({
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 0,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
})
const copyLedger = (self: Readonly<Record<string, Usage>>): Record<string, Usage> => {
  const copy = { ...self }
  // Own ledger names are arbitrary, including __proto__; dictionaries never inherit setters.
  Object.setPrototypeOf(copy, null)
  return copy
}
/**
 * Creates an empty usage state.
 *
 * @category constructors
 */
export const makeState = (): State => ({ models: copyLedger({}), tools: copyLedger({}) })
function addImpl(self: Usage, that: Usage): Usage {
  return {
    input: self.input + that.input,
    output: self.output + that.output,
    cacheRead: self.cacheRead + that.cacheRead,
    cacheWrite: self.cacheWrite + that.cacheWrite,
    totalTokens: self.totalTokens + that.totalTokens,
    ...(self.cacheWrite1h === undefined && that.cacheWrite1h === undefined
      ? {}
      : { cacheWrite1h: (self.cacheWrite1h ?? 0) + (that.cacheWrite1h ?? 0) }),
    ...(self.reasoning === undefined && that.reasoning === undefined
      ? {}
      : { reasoning: (self.reasoning ?? 0) + (that.reasoning ?? 0) }),
    cost: {
      ...(self.cost.known === undefined && that.cost.known === undefined
        ? {}
        : { known: self.cost.known !== false && that.cost.known !== false }),
      ...(self.cost.totalKnown === undefined &&
      that.cost.totalKnown === undefined &&
      self.cost.known === undefined &&
      that.cost.known === undefined
        ? {}
        : {
            totalKnown:
              (self.cost.totalKnown ?? self.cost.known) !== false &&
              (that.cost.totalKnown ?? that.cost.known) !== false,
          }),
      input: self.cost.input + that.cost.input,
      output: self.cost.output + that.cost.output,
      cacheRead: self.cost.cacheRead + that.cost.cacheRead,
      cacheWrite: self.cost.cacheWrite + that.cost.cacheWrite,
      total: self.cost.total + that.cost.total,
    },
  }
}
/**
 * Adds token and cost counters while retaining partial-cost knowledge.
 *
 * @category combinators
 */
export const add: {
  (that: Usage): (self: Usage) => Usage
  (self: Usage, that: Usage): Usage
} = dual(2, addImpl)
function recordImpl(self: State, bucket: keyof State, key: string, usage: Usage): State {
  const old = Object.hasOwn(self[bucket], key) ? self[bucket][key] : undefined
  const totals = copyLedger(self[bucket])
  Record.assignProperty(totals, key, add(old ?? make(), usage))
  return { ...self, [bucket]: totals }
}
/**
 * Records usage under an own-key ledger entry without mutating prior state.
 *
 * @category combinators
 */
export const record: {
  (bucket: keyof State, key: string, usage: Usage): (self: State) => State
  (self: State, bucket: keyof State, key: string, usage: Usage): State
} = dual(4, recordImpl)
/**
 * Combines independent usage ledgers.
 *
 * @category combinators
 */
export function sum(self: ReadonlyArray<State>): State {
  // effect-nit-allow P1-stdlib-collection-replacements: this public record may have accessors that delete or change later own keys. Native reflective enumeration rechecks each descriptor before reading; Record.collect snapshots keys and can read a newly inherited value instead.

  let total = makeState()
  for (const state of self)
    for (const bucket of ['models', 'tools'] as const)
      for (const [key, value] of Object.entries(state[bucket]))
        total = record(total, bucket, key, value)
  return total
}
/** Cost and extended cache counters are provider metadata, not inferred prices. */
function fromResponseImpl(
  self: Response.Usage,
  extra: Partial<Pick<Usage, 'cost' | 'cacheWrite1h'>> = {},
): Usage {
  const input =
    self.inputTokens.uncached ??
    Math.max(
      0,
      (self.inputTokens.total ?? 0) -
        (self.inputTokens.cacheRead ?? 0) -
        (self.inputTokens.cacheWrite ?? 0),
    )
  const output = self.outputTokens.total ?? 0
  const cacheRead = self.inputTokens.cacheRead ?? 0
  const cacheWrite = self.inputTokens.cacheWrite ?? 0
  return {
    input,
    output,
    cacheRead,
    cacheWrite,
    totalTokens: (self.inputTokens.total ?? input + cacheRead + cacheWrite) + output,
    ...(self.outputTokens.reasoning === undefined
      ? {}
      : { reasoning: self.outputTokens.reasoning }),
    ...extra,
    cost: extra.cost ?? { ...make().cost, known: false, totalKnown: false },
  }
}
/**
 * Converts native usage while preserving explicit cache and price metadata.
 *
 * @category combinators
 */
export const fromResponse: {
  (extra?: Partial<Pick<Usage, 'cost' | 'cacheWrite1h'>>): (self: Response.Usage) => Usage
  (self: Response.Usage, extra?: Partial<Pick<Usage, 'cost' | 'cacheWrite1h'>>): Usage
} = dual(
  Predicate.mapInput(
    Predicate.and(Predicate.isObjectOrArray, Predicate.hasProperty('inputTokens')),
    (args: IArguments) => args[0],
  ),
  fromResponseImpl,
)
/**
 * Returns the measured input, cache and output token total.
 *
 * @category combinators
 */
export const contextTokens = (self: Usage): number =>
  self.input + self.cacheRead + self.cacheWrite + self.output

/**
 * Checks whether a value satisfies the decoded `Cost` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isCost: (u: unknown) => u is Cost = Schema.is(Cost)

/**
 * Checks whether a value satisfies the decoded `Usage` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isUsage: (u: unknown) => u is Usage = Schema.is(Usage)

/**
 * Checks whether a value satisfies the decoded `State` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isState: (u: unknown) => u is State = Schema.is(State)

/**
 * Known USD input, output and cache costs for reported usage.
 *
 * @category models
 */
export type Cost = typeof Cost.Type
