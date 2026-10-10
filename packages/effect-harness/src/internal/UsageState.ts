/** Transactional receipt storage, independent of transcript and task-document lifetimes. */
import * as Document from '../Document.js'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import { pipeArguments } from 'effect/Pipeable'
import * as Schema from 'effect/Schema'
import * as Semaphore from 'effect/Semaphore'
import type * as Record from '../Record.js'
import { SessionError, type Failure } from '../SessionError.js'
import * as Transaction from '../Transaction.js'
import * as Usage from '../Usage.js'
import * as Value from './DocumentValue.js'
import * as Runtime from './TransactionRuntime.js'

const document = Document.family({
  kind: 'harness.usage.receipt',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'initial',
  schema: Schema.Struct({ receipt: Schema.optionalKey(Usage.ReceiptSchema) }),
  initial: () => ({}),
})
const key = (id: Usage.ReceiptIdentity): string =>
  id._tag === 'requestAttempt'
    ? JSON.stringify([id._tag, id.requestId, id.purpose, id.attempt])
    : JSON.stringify([id._tag, id.taskId, id.callId, id.attempt])
const target = (receipt: Usage.Receipt): Document.Target => ({
  scope: { _tag: 'conversation', conversationId: receipt.conversationId },
  key: key(receipt.identity),
})
const conflict = (message: string) =>
  new SessionError({ reason: 'conflict', operation: 'usage.receipt', message })
const same = Schema.toEquivalence(Usage.ReceiptSchema)
const sameUsage = Schema.toEquivalence(Usage.RecordSchema)

const read = Effect.fnUntraced(function* (
  state: Runtime.State,
  conversationId: Record.ConversationId,
): Effect.fn.Return<ReadonlyArray<Usage.Receipt>, Failure> {
  if (Option.isNone(yield* Runtime.conversation(state, conversationId)))
    return yield* new SessionError({
      reason: 'notFound',
      operation: 'usage.receipts',
      message: 'Conversation is absent',
    })
  const members = yield* Runtime.scanDocuments(state, {
    scope: { _tag: 'conversation', conversationId },
    kind: document.definition.kind,
  })
  const receipts: Array<Usage.Receipt> = []
  for (const member of members) {
    const found = yield* Runtime.snapshot(state, document, {
      scope: member.scope,
      key: member.key ?? '',
    })
    if (Option.isNone(found)) return yield* conflict('Receipt document disappeared')
    const receipt = found.value.value.receipt
    if (receipt === undefined) continue
    if (receipt.conversationId !== conversationId || member.key !== key(receipt.identity))
      return yield* conflict('Receipt identity does not match its document address')
    receipts.push(receipt)
  }
  return receipts
})

const validateLink = Effect.fnUntraced(function* (state: Runtime.State, receipt: Usage.Receipt) {
  if (receipt.linkedEntryId === undefined) return
  const found = yield* Runtime.entry(state, receipt.linkedEntryId, {
    conversationId: receipt.conversationId,
  })
  if (Option.isNone(found) || found.value.conversationId !== receipt.conversationId)
    return yield* conflict('Linked entry must belong to this conversation')
  if (
    found.value.usage !== undefined &&
    (receipt.usage === undefined || !sameUsage(found.value.usage, receipt.usage))
  )
    return yield* conflict('Linked entry reports different usage')
  for (const old of yield* read(state, receipt.conversationId)) {
    if (old.linkedEntryId === receipt.linkedEntryId && key(old.identity) !== key(receipt.identity))
      return yield* conflict('Linked entry already has another receipt identity')
  }
})

const stage = Effect.fnUntraced(function* (
  state: Runtime.State,
  input: Usage.Receipt,
): Effect.fn.Return<Usage.Recorded, Failure> {
  const receipt = yield* Value.copy(Usage.ReceiptSchema, input)
  if (receipt.identity._tag === 'nestedCall') {
    const task = yield* Runtime.task(state, receipt.identity.taskId)
    if (Option.isNone(task) || task.value.conversationId !== receipt.conversationId)
      return yield* conflict('Nested call owner must belong to this conversation')
  }
  if (
    receipt.usage !== undefined &&
    (receipt.identity._tag === 'requestAttempt') !== (receipt.usage._tag === 'model')
  )
    return yield* conflict('Request attempts report model usage; nested calls report tool usage')
  yield* validateLink(state, receipt)
  const address = target(receipt)
  const found = yield* Runtime.snapshot(state, document, address)
  const old = Option.isSome(found) ? found.value.value.receipt : undefined
  if (old !== undefined) {
    if (!same(old, receipt))
      return yield* conflict('Accounting identity already has a different payload')
    return { receipt: old, inserted: false }
  }
  // The complete payload was validated before creating an empty family member.
  yield* Runtime.ensureDocument(state, document, address)
  yield* Runtime.setDocument(state, document, address, { receipt })
  return { receipt, inserted: true }
})

/** Reuse the Transaction overlay protocol; both document maps must alias the same cloned draft. */
const isolate = Effect.fnUntraced(function* <A, E, R>(
  state: Runtime.State,
  run: (state: Runtime.State, tx: Transaction.Transaction) => Effect.Effect<A, E, R>,
) {
  const documents = new Map(state.documents)
  const incarnations = new Map(state.incarnations)
  for (const [id, draft] of state.incarnations) {
    const copy = { ...draft }
    incarnations.set(id, copy)
    if (documents.get(draft.key) === draft) documents.set(draft.key, copy)
  }
  const child: Runtime.State = {
    ...state,
    semaphore: yield* Semaphore.make(1),
    conversations: new Map(state.conversations),
    entries: new Map(state.entries),
    tasks: new Map(state.tasks),
    submissions: new Map(state.submissions),
    documents,
    incarnations,
    retirements: new Map(state.retirements),
    copiedSources: new Set(state.copiedSources),
  }
  const tx: Transaction.Transaction = {
    [Transaction.TypeId]: Transaction.TypeId,
    pipe() {
      return pipeArguments(this, arguments)
    },
  }
  Runtime.register(tx, child)
  const result = yield* Effect.scoped(Effect.suspend(() => run(child, tx))).pipe(
    Effect.ensuring(
      Effect.sync(() => {
        child.active = false
      }),
    ),
  )
  yield* Runtime.active(state)
  // Adoption is synchronous while the original Transaction permit is held.
  const adopt = <K, V>(parent: Map<K, V>, draft: Map<K, V>) => {
    parent.clear()
    for (const [key, value] of draft) parent.set(key, value)
  }
  adopt(state.conversations, child.conversations)
  adopt(state.entries, child.entries)
  adopt(state.tasks, child.tasks)
  adopt(state.submissions, child.submissions)
  adopt(state.documents, child.documents)
  adopt(state.incarnations, child.incarnations)
  adopt(state.retirements, child.retirements)
  state.copiedSources.clear()
  for (const id of child.copiedSources) state.copiedSources.add(id)
  return result
})

export const record = (
  tx: Transaction.Transaction,
  receipt: Usage.Receipt,
): Effect.Effect<Usage.Recorded, Failure> =>
  Runtime.use(tx, (state) => isolate(state, (child) => stage(child, receipt)))

export const settle = <E, R>(
  tx: Transaction.Transaction,
  receipt: Usage.Receipt,
  change: (tx: Transaction.Transaction) => Effect.Effect<void, E, R>,
): Effect.Effect<Usage.Recorded, Failure | E, R> =>
  Runtime.use(tx, (state) =>
    isolate(state, (child, callback) =>
      Effect.gen(function* () {
        const result = yield* stage(child, receipt)
        if (result.inserted) yield* change(callback)
        // Callback may have written or changed a linked entry: validate its final staged value.
        yield* validateLink(child, result.receipt)
        return result
      }),
    ),
  )

export const receipts = (
  tx: Transaction.Transaction,
  conversationId: Record.ConversationId,
): Effect.Effect<ReadonlyArray<Usage.Receipt>, Failure> =>
  Runtime.use(tx, (state) =>
    read(state, conversationId).pipe(
      Effect.tap((receipts) =>
        Effect.forEach(receipts, (receipt) => validateLink(state, receipt), { discard: true }),
      ),
    ),
  )

export const modelTiered = Effect.fnUntraced(function* (
  input: Usage.Tokens,
  ref: { readonly provider: string; readonly modelId: string } | undefined,
  price: Usage.TieredPricing,
): Effect.fn.Return<Usage.Record, Failure> {
  const tokens = yield* Value.copy(Usage.TokensSchema, input)
  const pricing = yield* Value.copy(Usage.TieredPricingSchema, price)
  if (
    (tokens.cacheWrite1h !== undefined &&
      tokens.cacheWrite !== undefined &&
      tokens.cacheWrite1h > tokens.cacheWrite) ||
    (tokens.reasoning !== undefined &&
      tokens.output !== undefined &&
      tokens.reasoning > tokens.output)
  )
    return yield* new SessionError({
      reason: 'invalid',
      operation: 'usage.pricing',
      message: 'Reported subset exceeds its total counter',
    })
  const record: Usage.Record = {
    _tag: 'model',
    tokens,
    tieredPricing: pricing,
    ...(ref === undefined ? {} : { model: ref }),
  }
  if (
    tokens.inputUncached === undefined ||
    tokens.cacheRead === undefined ||
    tokens.cacheWrite === undefined ||
    tokens.output === undefined ||
    (pricing.cacheWrite1hToken !== undefined && tokens.cacheWrite1h === undefined) ||
    (pricing.reasoningToken !== undefined && tokens.reasoning === undefined)
  )
    return yield* Value.copy(Usage.RecordSchema, record)
  const hour = pricing.cacheWrite1hToken === undefined ? 0 : (tokens.cacheWrite1h ?? 0)
  const reasoning = pricing.reasoningToken === undefined ? 0 : (tokens.reasoning ?? 0)
  const components: Usage.Components = {
    input: tokens.inputUncached * pricing.inputUncachedToken,
    cacheRead: tokens.cacheRead * pricing.cacheReadToken,
    cacheWrite: (tokens.cacheWrite - hour) * pricing.cacheWriteToken,
    output: (tokens.output - reasoning) * pricing.outputToken,
    ...(pricing.cacheWrite1hToken === undefined
      ? {}
      : { cacheWrite1h: hour * pricing.cacheWrite1hToken }),
    ...(pricing.reasoningToken === undefined
      ? {}
      : { reasoning: reasoning * pricing.reasoningToken }),
  }
  const total = Object.values(components).reduce((sum, amount) => sum + amount, 0)
  return yield* Value.copy(Usage.RecordSchema, {
    ...record,
    cost: { amount: total, currency: pricing.currency, source: pricing.source, components },
  })
})
