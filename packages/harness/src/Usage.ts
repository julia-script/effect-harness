import * as SchemaField from './SchemaField.ts'
import * as Schema from 'effect/Schema'
import type * as Response from 'effect/ai/Response'

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
export type Usage = typeof Usage.Type
export const State = Schema.Struct({
  models: Schema.Record(Schema.String, Usage),
  tools: Schema.Record(Schema.String, Usage),
})
export type State = typeof State.Type
export const zero = (): Usage => ({
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 0,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
})
export const empty = (): State => ({ models: {}, tools: {} })
export function add(left: Usage, right: Usage): Usage {
  return {
    input: left.input + right.input,
    output: left.output + right.output,
    cacheRead: left.cacheRead + right.cacheRead,
    cacheWrite: left.cacheWrite + right.cacheWrite,
    totalTokens: left.totalTokens + right.totalTokens,
    ...(left.cacheWrite1h === undefined && right.cacheWrite1h === undefined
      ? {}
      : { cacheWrite1h: (left.cacheWrite1h ?? 0) + (right.cacheWrite1h ?? 0) }),
    ...(left.reasoning === undefined && right.reasoning === undefined
      ? {}
      : { reasoning: (left.reasoning ?? 0) + (right.reasoning ?? 0) }),
    cost: {
      ...(left.cost.known === undefined && right.cost.known === undefined
        ? {}
        : { known: left.cost.known !== false && right.cost.known !== false }),
      ...(left.cost.totalKnown === undefined &&
      right.cost.totalKnown === undefined &&
      left.cost.known === undefined &&
      right.cost.known === undefined
        ? {}
        : {
            totalKnown:
              (left.cost.totalKnown ?? left.cost.known) !== false &&
              (right.cost.totalKnown ?? right.cost.known) !== false,
          }),
      input: left.cost.input + right.cost.input,
      output: left.cost.output + right.cost.output,
      cacheRead: left.cost.cacheRead + right.cost.cacheRead,
      cacheWrite: left.cost.cacheWrite + right.cost.cacheWrite,
      total: left.cost.total + right.cost.total,
    },
  }
}
export function record(state: State, bucket: keyof State, key: string, usage: Usage): State {
  const old = Object.hasOwn(state[bucket], key) ? state[bucket][key] : undefined
  const totals = { ...state[bucket] }
  Object.defineProperty(totals, key, {
    value: add(old ?? zero(), usage),
    enumerable: true,
    configurable: true,
    writable: true,
  })
  return { ...state, [bucket]: totals }
}
export function sum(states: ReadonlyArray<State>): State {
  let total = empty()
  for (const state of states)
    for (const bucket of ['models', 'tools'] as const)
      for (const [key, value] of Object.entries(state[bucket]))
        total = record(total, bucket, key, value)
  return total
}
/** Cost and extended cache counters are provider metadata, not inferred prices. */
export function fromResponse(
  value: Response.Usage,
  extra: Partial<Pick<Usage, 'cost' | 'cacheWrite1h'>> = {},
): Usage {
  const input =
    value.inputTokens.uncached ??
    Math.max(
      0,
      (value.inputTokens.total ?? 0) -
        (value.inputTokens.cacheRead ?? 0) -
        (value.inputTokens.cacheWrite ?? 0),
    )
  const output = value.outputTokens.total ?? 0
  const cacheRead = value.inputTokens.cacheRead ?? 0
  const cacheWrite = value.inputTokens.cacheWrite ?? 0
  return {
    input,
    output,
    cacheRead,
    cacheWrite,
    totalTokens: (value.inputTokens.total ?? input + cacheRead + cacheWrite) + output,
    ...(value.outputTokens.reasoning === undefined
      ? {}
      : { reasoning: value.outputTokens.reasoning }),
    ...extra,
    cost: extra.cost ?? { ...zero().cost, known: false, totalKnown: false },
  }
}
export const contextTokens = (value: Usage): number =>
  value.input + value.cacheRead + value.cacheWrite + value.output

export const isCost: (input: unknown) => input is typeof Cost.Type = Schema.is(Cost)

export const isUsage: (input: unknown) => input is Usage = Schema.is(Usage)

export const isState: (input: unknown) => input is State = Schema.is(State)
