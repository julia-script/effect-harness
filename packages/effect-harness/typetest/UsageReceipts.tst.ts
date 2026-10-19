import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Identity from 'effect-harness/Identity'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import type { Failure } from 'effect-harness/SessionError'
import type * as Transaction from 'effect-harness/Transaction'
import * as Usage from 'effect-harness/Usage'

class Billing extends Context.Service<Billing, { readonly source: string }>()(
  'types/UsageReceipts/Billing',
) {}
class ReceiptRejected extends Schema.TaggedError<ReceiptRejected>()('ReceiptRejected', {}) {}
declare const tx: Transaction.Transaction
declare const session: Session.Session
declare const taskId: Record.TaskId
declare const entryId: Record.EntryId
const attempt = Schema.decodeSync(Usage.AttemptId)(0)
const requestId = Schema.decodeSync(Identity.RequestId)('prepared/task-1/request-1/config-1')
const fact: Usage.Receipt = {
  conversationId: Record.ROOT_CONVERSATION_ID,
  identity: { _tag: 'requestAttempt', requestId, attempt, purpose: 'compaction' },
  outcome: { _tag: 'failed', error: { message: 'summary failed' } },
  usage: Usage.model({ input: 10 }),
}

test('identities are typed independently from entry receipts and attempt counters', () => {
  expect(fact.identity).type.toBe<Usage.ReceiptIdentity>()
  expect({
    _tag: 'nestedCall',
    taskId,
    callId: Schema.decodeSync(Usage.NestedCallId)('call'),
    attempt,
  } satisfies Usage.ReceiptIdentity).type.toBeAssignableTo<Usage.ReceiptIdentity>()
  expect({
    ...fact,
    linkedEntryId: entryId,
  } satisfies Usage.Receipt).type.toBeAssignableTo<Usage.Receipt>()
  expect(Usage.record(tx, fact)).type.toBe<Effect.Effect<Usage.Recorded, Failure>>()
  expect(Usage.record({}, fact)).type.toRaiseError()
  expect(
    Usage.record(tx, { ...fact, identity: { ...fact.identity, attempt: 0 } }),
  ).type.toRaiseError()
  expect(
    Usage.record(tx, {
      ...fact,
      identity: { _tag: 'requestAttempt', requestId: 'raw', attempt, purpose: 'generation' },
    }),
  ).type.toRaiseError()
  expect(Usage.record(tx, { ...fact, linkedEntryId: 1 })).type.toRaiseError()
  expect(
    Usage.record(tx, { ...fact, outcome: { _tag: 'completed', value: () => 42 } }),
  ).type.toRaiseError()
})

test('atomic outcome recording keeps its callback service and error visible', () => {
  expect(Usage.settle(tx, fact, () => Billing.pipe(Effect.asVoid))).type.toBe<
    Effect.Effect<Usage.Recorded, Failure, Billing>
  >()
  expect(
    Usage.settle(tx, fact, () => Billing.pipe(Effect.andThen(Effect.fail(new ReceiptRejected())))),
  ).type.toBe<Effect.Effect<Usage.Recorded, Failure | ReceiptRejected, Billing>>()
  expect(
    Effect.runPromise(Usage.settle(tx, fact, () => Billing.pipe(Effect.asVoid))),
  ).type.toRaiseError()
  expect(Usage.receipts(tx, Record.ROOT_CONVERSATION_ID)).type.toBe<
    Effect.Effect<ReadonlyArray<Usage.Receipt>, Failure>
  >()
  expect(Session.usage(session)).type.toBe<Effect.Effect<Usage.Summary, Failure>>()
})

test('optional tier and tool facts preserve legacy record and aggregate inputs', () => {
  expect(
    Usage.modelTiered(
      { inputUncached: 10, cacheWrite: 4, cacheWrite1h: 2, output: 3, reasoning: 1 },
      undefined,
      {
        inputUncachedToken: 1,
        cacheReadToken: 0.1,
        cacheWriteToken: 2,
        cacheWrite1hToken: 4,
        outputToken: 3,
        reasoningToken: 4,
        currency: 'USD',
        source: 'v1',
      },
    ),
  ).type.toBe<Effect.Effect<Usage.Record, Failure>>()
  expect({
    _tag: 'tool',
    name: 'nested',
    tokens: { cacheWrite1h: 3, totalTokens: 4 },
    cost: { amount: 1, currency: 'USD', source: 'reported', components: { reasoning: 0.3 } },
  } satisfies Usage.Record).type.toBeAssignableTo<Usage.Record>()
  expect({
    _tag: 'tool',
    name: 'legacy',
  } satisfies Usage.Record).type.toBeAssignableTo<Usage.Record>()
  expect(Usage.aggregate([], [fact])).type.toBe<Usage.Summary>()
})
