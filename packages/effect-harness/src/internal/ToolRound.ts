/** Private ordered tool-round facts. Every slot update reads the current transaction view. */
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Document from '../Document.js'
import * as OutputRetention from '../OutputRetention.js'
import * as Prepared from '../PreparedRequest.js'
import * as ProgressPolicy from '../ProgressPolicy.js'
import * as Record from '../Record.js'
import { SessionError } from '../SessionError.js'
import * as Tool from '../Tool.js'
import * as ToolControl from '../ToolControl.js'
import * as ToolResult from '../ToolResult.js'
import * as Transaction from '../Transaction.js'
import * as Usage from '../Usage.js'

export const InputSchema = Schema.Struct({
  ownerTaskId: Record.TaskId,
  roundKey: Schema.NonEmptyString,
  index: Schema.Natural,
  call: Tool.CallSchema,
  replay: Tool.ReplaySchema,
  preparation: Prepared.SnapshotSchema,
  retention: OutputRetention.Policy,
  progress: ProgressPolicy.Policy,
})
export type Input = typeof InputSchema.Type
export const CheckpointSchema = Schema.TaggedStruct('execute', {
  phase: Schema.tag('execute'),
  started: Schema.Boolean,
  attempt: Usage.AttemptId,
  acceptedCall: Schema.optionalKey(Tool.CallSchema),
  observedResult: Schema.optionalKey(ToolResult.ResultSchema),
  explicitEnvelope: Schema.optionalKey(Schema.Boolean),
})
export type Checkpoint = typeof CheckpointSchema.Type
export const SlotSchema = Schema.Struct({
  call: Tool.CallSchema,
  replay: Tool.ReplaySchema,
  retention: OutputRetention.Policy,
  progress: ProgressPolicy.Policy,
  taskId: Schema.optionalKey(Record.TaskId),
  result: Schema.optionalKey(ToolResult.ResultSchema),
  receipt: Schema.optionalKey(ToolControl.ReceiptSchema),
  entryId: Schema.optionalKey(Record.EntryId),
  fatal: Schema.optionalKey(Schema.Literal(true)),
})
export type Slot = typeof SlotSchema.Type
export const RoundSchema = Schema.Struct({
  ownerTaskId: Record.TaskId,
  assistant: Record.EntryId,
  mode: Schema.Literals(['parallel', 'sequential']),
  preparation: Prepared.SnapshotSchema,
  applied: Schema.Boolean,
  slots: Schema.Array(SlotSchema),
})
export type Round = typeof RoundSchema.Type
export const document = Document.family({
  kind: 'harness.tool.round',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'initial',
  schema: Schema.Struct({ round: Schema.optionalKey(Schema.toCodecJson(RoundSchema)) }),
  initial: () => ({}),
})
export const target = (conversationId: Record.ConversationId, key: string): Document.Target => ({
  scope: { _tag: 'conversation', conversationId },
  key,
})
export const progressKey = (taskId: Record.TaskId) => `tool:${taskId}`
export const invalid = (message: string) =>
  new SessionError({ reason: 'invalid', operation: 'tool.round', message })
const sameCall = Schema.toEquivalence(Tool.CallSchema)
const samePreparation = Schema.toEquivalence(Prepared.SnapshotSchema)
const sameRetention = Schema.toEquivalence(OutputRetention.Policy)
const sameProgress = Schema.toEquivalence(ProgressPolicy.Policy)

export const read = Effect.fnUntraced(function* (
  tx: Transaction.Transaction,
  conversationId: Record.ConversationId,
  key: string,
) {
  const saved = yield* Transaction.snapshot(tx, document, target(conversationId, key))
  if (Option.isNone(saved) || saved.value.value.round === undefined)
    return yield* invalid('The durable tool round is absent')
  const round = saved.value.value.round
  if (
    key !== `tool-round:${round.ownerTaskId}:${round.preparation.requestId}` ||
    round.slots.length === 0 ||
    (round.applied && round.slots.some((slot) => slot.result === undefined)) ||
    new Set(round.slots.map((slot) => slot.call.id)).size !== round.slots.length ||
    new Set(round.slots.flatMap((slot) => (slot.taskId === undefined ? [] : [slot.taskId])))
      .size !== round.slots.filter((slot) => slot.taskId !== undefined).length ||
    round.slots.some(
      (slot) =>
        (slot.result === undefined) !== (slot.receipt === undefined) ||
        (slot.result === undefined && slot.entryId !== undefined) ||
        (slot.result !== undefined &&
          slot.entryId === undefined &&
          slot.receipt?.status !== 'cancelled' &&
          slot.fatal !== true) ||
        (slot.fatal === true &&
          (slot.result?.isError !== true ||
            slot.receipt?.status !== 'failed' ||
            slot.entryId !== undefined)) ||
        (slot.receipt !== undefined && slot.receipt.callId !== slot.call.id),
    )
  )
    return yield* invalid('The durable tool round has inconsistent slot identities')
  for (const slot of round.slots) {
    if (slot.result === undefined || slot.receipt === undefined) continue
    if ((slot.receipt.status === 'succeeded') === slot.result.isError)
      return yield* invalid('The result does not match its terminal disposition')
    if (slot.fatal === true) {
      if (slot.taskId === undefined)
        return yield* invalid('A fatal tool slot has no admitted child')
      const child = yield* Transaction.task(tx, slot.taskId)
      const receipt = (yield* Usage.receipts(tx, conversationId)).find(
        (receipt) =>
          receipt.identity._tag === 'toolCall' &&
          receipt.identity.taskId === slot.taskId &&
          receipt.identity.callId === slot.call.id,
      )
      if (
        Option.isNone(child) ||
        child.value.owner !== round.ownerTaskId ||
        child.value.conversationId !== conversationId ||
        child.value.state.status !== 'terminal' ||
        !Schema.is(Schema.Struct({ status: Schema.tag('failed') }))(child.value.state.outcome) ||
        receipt?.outcome._tag !== 'failed' ||
        receipt.linkedEntryId !== undefined
      )
        return yield* invalid(
          'The fatal tool slot has no matching failed child and accounting receipt',
        )
      continue
    }
    if (slot.receipt.status === 'cancelled' && slot.entryId === undefined) continue
    if (slot.taskId === undefined || slot.entryId === undefined)
      return yield* invalid('An executed slot has no child or result entry')
    const entry = yield* Transaction.entry(tx, slot.entryId)
    if (
      Option.isNone(entry) ||
      entry.value.conversationId !== conversationId ||
      entry.value.kind !== 'tool.result' ||
      entry.value.byTaskId !== slot.taskId
    )
      return yield* invalid('The result entry does not belong to the durable child')
    const receipt = (yield* Usage.receipts(tx, conversationId)).find(
      (receipt) =>
        receipt.identity._tag === 'toolCall' &&
        receipt.identity.taskId === slot.taskId &&
        receipt.identity.callId === slot.call.id &&
        receipt.linkedEntryId === slot.entryId,
    )
    if (receipt === undefined)
      return yield* invalid('The executed slot has no linked accounting receipt')
  }
  return round
})

export const write = (
  tx: Transaction.Transaction,
  conversationId: Record.ConversationId,
  key: string,
  round: Round,
) => Transaction.setDocument(tx, document, target(conversationId, key), { round })

export const slot = Effect.fnUntraced(function* (
  tx: Transaction.Transaction,
  conversationId: Record.ConversationId,
  taskId: Record.TaskId,
  input: Input,
) {
  const round = yield* read(tx, conversationId, input.roundKey)
  const found = round.slots[input.index]
  const current = yield* Transaction.task(tx, taskId)
  if (
    round.applied ||
    round.ownerTaskId !== input.ownerTaskId ||
    found === undefined ||
    found.taskId !== taskId ||
    !sameCall(found.call, input.call) ||
    found.replay !== input.replay ||
    !samePreparation(round.preparation, input.preparation) ||
    !sameRetention(found.retention, input.retention) ||
    !sameProgress(found.progress, input.progress) ||
    Option.isNone(current) ||
    current.value.owner !== input.ownerTaskId ||
    current.value.conversationId !== conversationId
  )
    return yield* invalid('The invocation does not own this durable tool slot')
  return { round, found }
})
