/** Actual sink receipts outlive parent task documents; execution never reads this projection. */
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Document from '../Document.js'
import * as Invocation from '../ToolInvocation.js'
import type * as Record from '../Record.js'
import * as Transaction from '../Transaction.js'
import { SessionError } from '../SessionError.js'
import * as Nested from './NestedInvocation.js'
import * as Round from './ToolRound.js'
import * as Value from './DocumentValue.js'

export const document = Document.family({
  kind: 'harness.tool.nested.receipt',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'initial',
  schema: Schema.Struct({
    receipt: Schema.optionalKey(Schema.toCodecJson(Invocation.WireReceiptSchema)),
  }),
  initial: () => ({}),
})
export const key = (receipt: Invocation.WireReceipt): string =>
  JSON.stringify([
    receipt.parentTaskId,
    receipt.key,
    receipt.taskId,
    receipt.callId,
    receipt.attempt,
  ])
export const retain = Effect.fnUntraced(function* (
  tx: Transaction.Transaction,
  conversationId: Record.ConversationId,
  input: Invocation.WireReceipt,
) {
  const receipt = yield* Value.copy(Schema.toCodecJson(Invocation.WireReceiptSchema), input)
  const conflict = () =>
    new SessionError({
      reason: 'conflict',
      operation: 'nested.retain',
      message: 'Retained receipt does not match its original native identity',
    })
  if (receipt.taskId === undefined) return yield* conflict()
  const child = yield* Transaction.task(tx, receipt.taskId)
  const parent = yield* Transaction.task(tx, receipt.parentTaskId)
  if (
    Option.isNone(child) ||
    Option.isNone(parent) ||
    child.value.conversationId !== conversationId ||
    parent.value.conversationId !== conversationId ||
    child.value.owner !== parent.value.id ||
    child.value.kind !== 'harness.tool.nested' ||
    child.value.version !== 1
  )
    return yield* conflict()
  const original = yield* Schema.decodeEffect(Schema.toCodecJson(Nested.InputSchema))(
    child.value.input,
  )
  if (
    original.parentTaskId !== receipt.parentTaskId ||
    original.key !== receipt.key ||
    original.logicalCallId !== receipt.callId ||
    original.call.name !== receipt.name
  )
    return yield* conflict()
  if (child.value.state.checkpoint !== undefined) {
    const checkpoint = yield* Schema.decodeEffect(Schema.toCodecJson(Round.CheckpointSchema))(
      child.value.state.checkpoint,
    )
    if (checkpoint.attempt !== receipt.attempt) return yield* conflict()
  }
  const target: Document.Target = {
    scope: { _tag: 'conversation', conversationId },
    key: key(receipt),
  }
  const saved = yield* Transaction.snapshot(tx, document, target)
  if (Option.isSome(saved) && saved.value.value.receipt !== undefined) {
    if (!Schema.toEquivalence(Invocation.WireReceiptSchema)(saved.value.value.receipt, receipt))
      return yield* conflict()
    return
  }
  yield* Transaction.ensureDocument(tx, document, target)
  yield* Transaction.setDocument(tx, document, target, { receipt })
})
