/** Durable accounting facts and honest aggregates. Prices are caller declarations. */
import * as Schema from 'effect/Schema'
import * as Effect from 'effect/Effect'
import * as Identity from './Identity.js'
import type * as DurableRecord from './Record.js'
import type * as Transaction from './Transaction.js'
import type { Failure } from './SessionError.js'

const count = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
const amount = Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0))
export const TokensSchema = Schema.Struct({
  input: Schema.optionalKey(count),
  inputUncached: Schema.optionalKey(count),
  cacheRead: Schema.optionalKey(count),
  cacheWrite: Schema.optionalKey(count),
  /** Subset of cacheWrite, never an additional token count. */
  cacheWrite1h: Schema.optionalKey(count),
  output: Schema.optionalKey(count),
  outputText: Schema.optionalKey(count),
  reasoning: Schema.optionalKey(count),
  totalTokens: Schema.optionalKey(count),
})
export type Tokens = typeof TokensSchema.Type

/** Reported charge components are preserved separately; amount is the authoritative total. */
export const ComponentsSchema = Schema.Struct({
  input: Schema.optionalKey(amount),
  output: Schema.optionalKey(amount),
  cacheRead: Schema.optionalKey(amount),
  cacheWrite: Schema.optionalKey(amount),
  cacheWrite1h: Schema.optionalKey(amount),
  reasoning: Schema.optionalKey(amount),
})
export type Components = typeof ComponentsSchema.Type

/** Amounts use the declared currency; no conversion or remote price lookup occurs. */
export const CostSchema = Schema.Struct({
  amount,
  currency: Schema.NonEmptyString,
  source: Schema.NonEmptyString,
  components: Schema.optionalKey(ComponentsSchema),
})
export type Cost = typeof CostSchema.Type

/**
 * Caller-declared flat rates per total input/output token, including cached/reasoning tokens.
 * source identifies the caller's price version. Use modelTiered for declared cache-tier rates.
 * A complete price needs both total counters; missing usage never produces a zero charge.
 */
export const PricingSchema = Schema.Struct({
  inputToken: amount,
  outputToken: amount,
  currency: Schema.NonEmptyString,
  source: Schema.NonEmptyString,
})
export type Pricing = typeof PricingSchema.Type
/** Disjoint cache tiers; optional overrides split existing cacheWrite/output counters. */
export const TieredPricingSchema = Schema.Struct({
  inputUncachedToken: amount,
  cacheReadToken: amount,
  cacheWriteToken: amount,
  cacheWrite1hToken: Schema.optionalKey(amount),
  outputToken: amount,
  reasoningToken: Schema.optionalKey(amount),
  currency: Schema.NonEmptyString,
  source: Schema.NonEmptyString,
})
export type TieredPricing = typeof TieredPricingSchema.Type
export const RecordSchema = Schema.Union([
  Schema.TaggedStruct('model', {
    model: Schema.optionalKey(
      Schema.Struct({ provider: Schema.NonEmptyString, modelId: Schema.NonEmptyString }),
    ),
    tokens: TokensSchema,
    pricing: Schema.optionalKey(PricingSchema),
    tieredPricing: Schema.optionalKey(TieredPricingSchema),
    cost: Schema.optionalKey(CostSchema),
  }),
  Schema.TaggedStruct('tool', {
    name: Schema.NonEmptyString,
    tokens: Schema.optionalKey(TokensSchema),
    cost: Schema.optionalKey(CostSchema),
  }),
])
export type Record = typeof RecordSchema.Type

export const AttemptId = Schema.Int.check(
  Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }),
).pipe(Schema.brand('effect-harness/Usage/AttemptId'))
export type AttemptId = typeof AttemptId.Type
export const NestedCallId = Schema.NonEmptyString.pipe(
  Schema.brand('effect-harness/Usage/NestedCallId'),
)
export type NestedCallId = typeof NestedCallId.Type
/** A request ID identifies immutable prepared work; a changed request/config needs a new ID. */
export const ReceiptIdentitySchema = Schema.Union([
  Schema.TaggedStruct('requestAttempt', {
    requestId: Identity.RequestId,
    attempt: AttemptId,
    purpose: Schema.Literals(['generation', 'compaction']),
  }),
  Schema.TaggedStruct('nestedCall', {
    // Record depends on Usage; retain its exact wire bounds/brand without an initialization cycle.
    taskId: Schema.Int.check(
      Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }),
    ).pipe(Schema.brand('@effect-harness/durable/Record/TaskId')),
    callId: NestedCallId,
    attempt: AttemptId,
  }),
  Schema.TaggedStruct('toolCall', {
    // Keep the same durable task brand without introducing the Record initialization cycle.
    taskId: Schema.Int.check(
      Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }),
    ).pipe(Schema.brand('@effect-harness/durable/Record/TaskId')),
    callId: Schema.NonEmptyString,
    attempt: AttemptId,
  }),
])
export type ReceiptIdentity = typeof ReceiptIdentitySchema.Type
/** Terminal accounting observation, including a failed known charge without a transcript entry. */
export const OutcomeSchema = Schema.Union([
  Schema.TaggedStruct('completed', { value: Schema.Json }),
  Schema.TaggedStruct('failed', { error: Schema.Json }),
  Schema.TaggedStruct('interrupted', { reason: Schema.optionalKey(Schema.String) }),
  Schema.TaggedStruct('unknown', {}),
])
export type Outcome = typeof OutcomeSchema.Type
export const ReceiptSchema = Schema.Struct({
  conversationId: Identity.ConversationId,
  identity: ReceiptIdentitySchema,
  outcome: OutcomeSchema,
  /** Omission means unreported external usage, never zero charge. */
  usage: Schema.optionalKey(RecordSchema),
  linkedEntryId: Schema.optionalKey(Identity.EntryId),
})
export type Receipt = typeof ReceiptSchema.Type
export interface Recorded {
  readonly receipt: Receipt
  readonly inserted: boolean
}

/**
 * First writer wins inside the supplied Transaction. Equal observations are no-ops;
 * changed identity payloads conflict. A linked entry must be local and agree on usage.
 * Receipts use conversation/latest/fork-initial documents and survive task retirement.
 * Request, nested-call and compaction producers must explicitly call this at settlement.
 * Tool-call receipts reject unless the Session's matching StorageFormat capability confirms
 * matching supported container/minimum-reader protection of at least 9. This check never activates a lower-format store.
 */
export const record = (
  tx: Transaction.Transaction,
  receipt: Receipt,
): Effect.Effect<Recorded, Failure> =>
  state.pipe(Effect.flatMap((module) => module.record(tx, receipt)))

/**
 * Stage accounting and outcome writes in one isolated savepoint of the real Transaction.
 * change runs only for a new receipt; failure/interruption discards all its drafts, even
 * when caught by the outer callback. Replay returns the immutable recorded outcome.
 * Use only transaction writes in change: external side effects cannot be rolled back.
 * The callback capability is revoked afterwards; Session operations would reenter its lock.
 * Tool-call receipts enforce the same reader-9 protection as record before change runs.
 */
export const settle = <E, R>(
  tx: Transaction.Transaction,
  receipt: Receipt,
  change: (tx: Transaction.Transaction) => Effect.Effect<void, E, R>,
): Effect.Effect<Recorded, Failure | E, R> =>
  state.pipe(Effect.flatMap((module) => module.settle(tx, receipt, change)))

/** Read-only committed/staged receipts for this conversation's own work. */
export const receipts = (
  tx: Transaction.Transaction,
  conversationId: DurableRecord.ConversationId,
): Effect.Effect<ReadonlyArray<Receipt>, Failure> =>
  state.pipe(Effect.flatMap((module) => module.receipts(tx, conversationId)))

// Load the transactional boundary on use: Record's entry codec depends on this module.
const state = Effect.promise(() => import('./internal/UsageState.js'))
const receiptKey = (id: ReceiptIdentity): string =>
  id._tag === 'requestAttempt'
    ? JSON.stringify([id._tag, id.requestId, id.purpose, id.attempt])
    : JSON.stringify([id._tag, id.taskId, id.callId, id.attempt])

/**
 * Validates a declared tier price and counters. Missing split counters leave cost absent.
 * Reasoning and one-hour writes are subsets, so their overrides replace part of a tier.
 * No prices, counters or totalTokens are inferred; no remote price lookup occurs.
 */
export const modelTiered = (
  tokens: Tokens,
  ref: { readonly provider: string; readonly modelId: string } | undefined,
  pricing: TieredPricing,
): Effect.Effect<Record, Failure> =>
  state.pipe(Effect.flatMap((module) => module.modelTiered(tokens, ref, pricing)))

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
  /** Committed observations, including known failed and no-entry summarization attempts. */
  readonly responses: number
  /** A counter is absent if any response in this bucket omitted it. */
  readonly tokens: Tokens
  readonly tokenCoverage?: TokenCoverage
}
export interface ToolTotal {
  readonly name: string
  readonly results: number
  readonly tokens?: Tokens
  readonly tokenCoverage?: TokenCoverage
}
/** Known subtotals and number of omitted observations, independently for every counter. */
export type TokenCoverage = {
  readonly [K in keyof Required<Tokens>]: { readonly known: number; readonly unknown: number }
}
export interface Summary {
  readonly models: ReadonlyArray<ModelTotal>
  readonly tools: ReadonlyArray<ToolTotal>
  /** Known subtotals only, separated by currency and source kind. */
  readonly costs: ReadonlyArray<{
    readonly currency: string
    readonly model: number
    readonly tool: number
    readonly modelComponents?: Components
    readonly toolComponents?: Components
  }>
  readonly unpricedModels: number
  readonly unpricedTools: number
  /** Older assistant/tool entries have no accounting fact. Their usage remains unknown. */
  readonly legacyRecords: number
  /** No-entry terminal observations with entirely unreported usage. */
  readonly unknownAttempts?: number
}
const counters = [
  'input',
  'inputUncached',
  'cacheRead',
  'cacheWrite',
  'cacheWrite1h',
  'output',
  'outputText',
  'reasoning',
  'totalTokens',
] as const

const componentNames = [
  'input',
  'output',
  'cacheRead',
  'cacheWrite',
  'cacheWrite1h',
  'reasoning',
] as const
const addTokens = (current: Tokens, previous?: Tokens): Tokens => {
  const total: { -readonly [K in keyof Tokens]: Tokens[K] } = {}
  for (const key of counters) {
    const a = current[key]
    const b = previous?.[key]
    if (a !== undefined && (previous === undefined || b !== undefined)) total[key] = (b ?? 0) + a
  }
  return total
}
const addCoverage = (tokens: Tokens, previous?: TokenCoverage): TokenCoverage =>
  Object.fromEntries(
    counters.map((key) => [
      key,
      {
        known: (previous?.[key].known ?? 0) + (tokens[key] ?? 0),
        unknown: (previous?.[key].unknown ?? 0) + (tokens[key] === undefined ? 1 : 0),
      },
    ]),
  ) as TokenCoverage
const addComponents = (current: Components, previous: Components = {}): Components => {
  const total: { -readonly [K in keyof Components]: Components[K] } = { ...previous }
  for (const key of componentNames)
    if (current[key] !== undefined) total[key] = (total[key] ?? 0) + current[key]
  return total
}

/**
 * Aggregate unique committed entry IDs. Inherited fork entries can be passed repeatedly safely.
 * Session.usage and Harness.usage provide atomic current queries; no context edit erases spend.
 * Optional receipts cover terminal attempts without entries, deduplicated by conversation
 * and typed identity. Entry-linked receipts use the entry once. Unknown external charges
 * cannot be reconstructed; producer integration must record each known failed attempt.
 */
export const aggregate = (
  entries: Iterable<{ readonly id: number; readonly kind: string; readonly usage?: Record }>,
  receipts: Iterable<Receipt> = [],
): Summary => {
  const models = new Map<string, ModelTotal>()
  const tools = new Map<string, ToolTotal>()
  const toolCoverage = new Map<string, TokenCoverage>()
  const costs = new Map<
    string,
    {
      currency: string
      model: number
      tool: number
      modelComponents?: Components
      toolComponents?: Components
    }
  >()
  let unpricedModels = 0
  let unpricedTools = 0
  let legacyRecords = 0
  let unknownAttempts = 0
  const facts = new Map<
    number,
    { readonly id: number; readonly kind: string; readonly usage?: Record }
  >()
  for (const entry of entries) if (!facts.has(entry.id)) facts.set(entry.id, entry)
  const seenReceipts = new Set<string>()
  const noEntry: Array<Record> = []
  for (const receipt of receipts) {
    const key = JSON.stringify([receipt.conversationId, receiptKey(receipt.identity)])
    if (seenReceipts.has(key)) continue
    seenReceipts.add(key)
    if (receipt.linkedEntryId !== undefined) {
      const old = facts.get(receipt.linkedEntryId)
      if (old?.usage !== undefined) continue
      facts.set(receipt.linkedEntryId, {
        id: receipt.linkedEntryId,
        kind: old?.kind ?? 'usage.receipt',
        ...(receipt.usage === undefined ? {} : { usage: receipt.usage }),
      })
    } else if (receipt.usage === undefined) unknownAttempts++
    else noEntry.push(receipt.usage)
  }
  const records: Array<Record> = [...noEntry]
  for (const entry of facts.values()) {
    const record = entry.usage
    if (record === undefined) {
      if (entry.kind === 'assistant' || entry.kind === 'tool.result') legacyRecords++
      continue
    }
    records.push(record)
  }
  for (const record of records) {
    if (record._tag === 'model') {
      const key = JSON.stringify(
        record.model === undefined ? null : [record.model.provider, record.model.modelId],
      )
      const old = models.get(key)
      models.set(key, {
        ...(record.model === undefined ? {} : { model: record.model }),
        responses: (old?.responses ?? 0) + 1,
        tokens: addTokens(record.tokens, old?.tokens),
        tokenCoverage: addCoverage(record.tokens, old?.tokenCoverage),
      })
      if (record.cost === undefined) unpricedModels++
    } else {
      const old = tools.get(record.name)
      const coverage = addCoverage(record.tokens ?? {}, toolCoverage.get(record.name))
      toolCoverage.set(record.name, coverage)
      tools.set(record.name, {
        name: record.name,
        results: (old?.results ?? 0) + 1,
        ...(record.tokens === undefined && old?.tokens === undefined
          ? {}
          : {
              tokens: addTokens(
                record.tokens ?? {},
                old === undefined ? undefined : (old.tokens ?? {}),
              ),
              tokenCoverage: coverage,
            }),
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
      if (record.cost.components !== undefined) {
        const key = record._tag === 'model' ? 'modelComponents' : 'toolComponents'
        total[key] = addComponents(record.cost.components, total[key])
      }
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
    ...(unknownAttempts === 0 ? {} : { unknownAttempts }),
  }
}
