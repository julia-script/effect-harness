/**
 * Token and price ledgers with explicit partial-cost metadata.
 *
 * @since 0.0.0
 */
import { dual } from 'effect/Function'
import * as SchemaField from './SchemaField.ts'
import * as Schema from 'effect/Schema'
import type * as Response from 'effect/ai/Response'

/**
 * Schema for cost.
 *
 * @category schemas
 * @since 0.0.0
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
 * Schema for usage.
 *
 * @category schemas
 * @since 0.0.0
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
 * Usage usage contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Usage = typeof Usage.Type
/**
 * Schema for state.
 *
 * @category schemas
 * @since 0.0.0
 */
export const State = Schema.Struct({
  models: Schema.Record(Schema.String, Usage),
  tools: Schema.Record(Schema.String, Usage),
})
/**
 * Usage state contract.
 *
 * @category models
 * @since 0.0.0
 */
export type State = typeof State.Type
/**
 * Creates usage with zero measured counters.
 *
 * @category constructors
 * @since 0.0.0
 */
export const zero = (): Usage => ({
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 0,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
})
/**
 * Creates an empty usage state.
 *
 * @category constructors
 * @since 0.0.0
 */
export const empty = (): State => ({ models: {}, tools: {} })
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
 * @since 0.0.0
 */
export const add: {
  (that: Usage): (self: Usage) => Usage
  (self: Usage, that: Usage): Usage
} = dual(2, addImpl)
function recordImpl(self: State, bucket: keyof State, key: string, usage: Usage): State {
  const old = Object.hasOwn(self[bucket], key) ? self[bucket][key] : undefined
  const totals = { ...self[bucket] }
  Object.defineProperty(totals, key, {
    value: add(old ?? zero(), usage),
    enumerable: true,
    configurable: true,
    writable: true,
  })
  return { ...self, [bucket]: totals }
}
/**
 * Records usage under an own-key ledger entry without mutating prior state.
 *
 * @category combinators
 * @since 0.0.0
 */
export const record: {
  (bucket: keyof State, key: string, usage: Usage): (self: State) => State
  (self: State, bucket: keyof State, key: string, usage: Usage): State
} = dual(4, recordImpl)
/**
 * Combines independent usage ledgers.
 *
 * @category combinators
 * @since 0.0.0
 */
export function sum(self: ReadonlyArray<State>): State {
  let total = empty()
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
    cost: extra.cost ?? { ...zero().cost, known: false, totalKnown: false },
  }
}
/**
 * Converts native usage while preserving explicit cache and price metadata.
 *
 * @category combinators
 * @since 0.0.0
 */
export const fromResponse: {
  (extra?: Partial<Pick<Usage, 'cost' | 'cacheWrite1h'>>): (self: Response.Usage) => Usage
  (self: Response.Usage, extra?: Partial<Pick<Usage, 'cost' | 'cacheWrite1h'>>): Usage
} = dual(
  (args) => typeof args[0] === 'object' && args[0] != null && 'inputTokens' in args[0],
  fromResponseImpl,
)
/**
 * Returns the measured input, cache and output token total.
 *
 * @category combinators
 * @since 0.0.0
 */
export const contextTokens = (self: Usage): number =>
  self.input + self.cacheRead + self.cacheWrite + self.output

/**
 * Checks whether an unknown value satisfies the Cost contract.
 *
 * @category guards
 * @since 0.0.0
 */
export const isCost: (u: unknown) => u is Cost = Schema.is(Cost)

/**
 * Checks whether an unknown value satisfies the Usage contract.
 *
 * @category guards
 * @since 0.0.0
 */
export const isUsage: (u: unknown) => u is Usage = Schema.is(Usage)

/**
 * Checks whether an unknown value satisfies the State contract.
 *
 * @category guards
 * @since 0.0.0
 */
export const isState: (u: unknown) => u is State = Schema.is(State)

/**
 * Usage cost contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Cost = typeof Cost.Type
