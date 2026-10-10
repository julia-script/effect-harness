/** Durable accounting facts and honest aggregates. Prices are caller declarations. */
import * as Schema from 'effect/Schema'

const count = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
const amount = Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0))
export const TokensSchema = Schema.Struct({
  input: Schema.optionalKey(count),
  inputUncached: Schema.optionalKey(count),
  cacheRead: Schema.optionalKey(count),
  cacheWrite: Schema.optionalKey(count),
  output: Schema.optionalKey(count),
  outputText: Schema.optionalKey(count),
  reasoning: Schema.optionalKey(count),
})
export type Tokens = typeof TokensSchema.Type

/** Amounts use the declared currency; no conversion or remote price lookup occurs. */
export const CostSchema = Schema.Struct({
  amount,
  currency: Schema.NonEmptyString,
  source: Schema.NonEmptyString,
})
export type Cost = typeof CostSchema.Type

/**
 * Caller-declared flat rates per total input/output token, including cached/reasoning tokens.
 * source identifies the caller's price version. Cache-tier discounts require a custom adapter.
 * A complete price needs both total counters; missing usage never produces a zero charge.
 */
export const PricingSchema = Schema.Struct({
  inputToken: amount,
  outputToken: amount,
  currency: Schema.NonEmptyString,
  source: Schema.NonEmptyString,
})
export type Pricing = typeof PricingSchema.Type
export const RecordSchema = Schema.Union([
  Schema.TaggedStruct('model', {
    model: Schema.optionalKey(
      Schema.Struct({ provider: Schema.NonEmptyString, modelId: Schema.NonEmptyString }),
    ),
    tokens: TokensSchema,
    pricing: Schema.optionalKey(PricingSchema),
    cost: Schema.optionalKey(CostSchema),
  }),
  Schema.TaggedStruct('tool', {
    name: Schema.NonEmptyString,
    cost: Schema.optionalKey(CostSchema),
  }),
])
export type Record = typeof RecordSchema.Type

/** Pricing is snapshotted in each committed record. No counter is inferred from another. */
export const model = (
  tokens: Tokens,
  ref?: { readonly provider: string; readonly modelId: string },
  pricing?: Pricing,
): Record => ({
  _tag: 'model',
  tokens,
  ...(ref === undefined ? {} : { model: ref }),
  ...(pricing === undefined ? {} : { pricing }),
  ...(pricing === undefined || tokens.input === undefined || tokens.output === undefined
    ? {}
    : {
        cost: {
          amount: tokens.input * pricing.inputToken + tokens.output * pricing.outputToken,
          currency: pricing.currency,
          source: pricing.source,
        },
      }),
})
export interface ModelTotal {
  readonly model?: { readonly provider: string; readonly modelId: string }
  readonly responses: number
  /** A counter is absent if any response in this bucket omitted it. */
  readonly tokens: Tokens
}
export interface ToolTotal {
  readonly name: string
  readonly results: number
}
export interface Summary {
  readonly models: ReadonlyArray<ModelTotal>
  readonly tools: ReadonlyArray<ToolTotal>
  /** Known subtotals only, separated by currency and source kind. */
  readonly costs: ReadonlyArray<{
    readonly currency: string
    readonly model: number
    readonly tool: number
  }>
  readonly unpricedModels: number
  readonly unpricedTools: number
  /** Older assistant/tool entries have no accounting fact. Their usage remains unknown. */
  readonly legacyRecords: number
}
const counters = [
  'input',
  'inputUncached',
  'cacheRead',
  'cacheWrite',
  'output',
  'outputText',
  'reasoning',
] as const

/**
 * Aggregate unique committed entry IDs. Inherited fork entries can be passed repeatedly safely.
 * Session.usage and Harness.usage provide atomic current queries; no context edit erases spend.
 * Failed requests without committed responses and interrupted tool outcomes have unknown spend.
 * Those external attempts cannot be reconstructed. Reset/compaction are outside this delivery.
 */
export const aggregate = (
  entries: Iterable<{ readonly id: number; readonly kind: string; readonly usage?: Record }>,
): Summary => {
  const seen = new Set<number>()
  const models = new Map<string, ModelTotal>()
  const tools = new Map<string, ToolTotal>()
  const costs = new Map<string, { currency: string; model: number; tool: number }>()
  let unpricedModels = 0
  let unpricedTools = 0
  let legacyRecords = 0
  for (const entry of entries) {
    if (seen.has(entry.id)) continue
    seen.add(entry.id)
    const record = entry.usage
    if (record === undefined) {
      if (entry.kind === 'assistant' || entry.kind === 'tool.result') legacyRecords++
      continue
    }
    if (record._tag === 'model') {
      const key = JSON.stringify(
        record.model === undefined ? null : [record.model.provider, record.model.modelId],
      )
      const old = models.get(key)
      const tokens: { -readonly [K in keyof Tokens]: Tokens[K] } = {}
      for (const counter of counters) {
        const current = record.tokens[counter]
        const previous = old?.tokens[counter]
        if (current !== undefined && (old === undefined || previous !== undefined))
          tokens[counter] = (previous ?? 0) + current
      }
      models.set(key, {
        ...(record.model === undefined ? {} : { model: record.model }),
        responses: (old?.responses ?? 0) + 1,
        tokens,
      })
      if (record.cost === undefined) unpricedModels++
    } else {
      tools.set(record.name, {
        name: record.name,
        results: (tools.get(record.name)?.results ?? 0) + 1,
      })
      if (record.cost === undefined) unpricedTools++
    }
    if (record.cost !== undefined) {
      const total = costs.get(record.cost.currency) ?? {
        currency: record.cost.currency,
        model: 0,
        tool: 0,
      }
      total[record._tag] += record.cost.amount
      costs.set(record.cost.currency, total)
    }
  }
  return {
    models: [...models.values()],
    tools: [...tools.values()],
    costs: [...costs.values()],
    unpricedModels,
    unpricedTools,
    legacyRecords,
  }
}
