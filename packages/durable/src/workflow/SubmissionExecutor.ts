import * as Identity from '../Identity.ts'
import type * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import type * as Layer from 'effect/Layer'
import * as Clock from 'effect/Clock'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as ClusterSchema from 'effect/cluster/ClusterSchema'
import * as Activity from 'effect/workflow/Activity'
import * as DurableDeferred from 'effect/workflow/DurableDeferred'
import * as Conversation from '../Conversation.ts'
import * as Document from '../Document.ts'
import * as Inbox from '../Inbox.ts'
import * as Ownership from '../Ownership.ts'
import * as Record from '../Record.ts'
import type * as Session from '../Session.ts'
import { SessionDirectory } from '../SessionDirectory.ts'
import type { StorageError } from '../StorageError.ts'
import {
  ExecutionError,
  ExecutionErrorCodec,
  Closed,
  Storage,
  InvalidState,
  RequestConflict,
  ConversationBusy,
  InvalidArguments,
} from './ExecutionError.ts'
import { Generation } from './Generation.ts'
import { Submission, Result } from './Submission.ts'
import * as Prompt from 'effect/ai/Prompt'

export const Settled = DurableDeferred.make('submission/settled/v1', {
  success: Result,
  error: ExecutionErrorCodec,
})
export const Links = Document.familyUnsafe({
  kind: 'harness.submission-links',
  version: 1,
  scope: 'session',
  schema: Schema.Struct({ executions: Schema.Array(Schema.String) }),
  initial: (): { executions: string[] } => ({ executions: [] }),
})
const Admission = Schema.Struct({
  id: Record.SubmissionId,
  notify: Schema.Array(Record.SubmissionId),
  generation: Schema.optionalKey(Generation.payloadSchema),
  receipt: Schema.optionalKey(Result),
})
export const storageError = (error: StorageError) =>
  new ExecutionError({
    reason: new (error.reason._tag === 'Closed' ? Closed : Storage)({
      message: error.message,
      detail: { reason: error.code, certainty: error.certainty },
      cause: error,
    }),
  })

/** A task record is an inspectable projection of a normal native Workflow execution. */
export const createGeneration = Effect.fnUntraced(function* (
  tx: Session.Transaction,
  sessionId: Identity.SessionId,
  conversationId: Record.ConversationId,
  inputs: ReadonlyArray<Record.SubmissionId>,
  runId?: Identity.RunId,
) {
  const taskId = yield* tx.mint(Record.TaskId)
  const payload = {
    sessionId,
    conversationId,
    taskId,
    inputs: [...inputs],
    runId: runId ?? Identity.RunId.make(JSON.stringify([sessionId, conversationId, inputs[0]])),
  }
  const executionId = yield* Generation.executionId(payload)
  const binding: Ownership.Binding = { workflow: Generation._tag, executionId, payload }
  yield* tx.write({
    type: 'task',
    value: {
      id: taskId,
      conversationId,
      kind: 'harness.generation',
      version: 1,
      input: binding,
      background: false,
      abortRequested: false,
      state: { status: 'pending' },
    },
  })
  const live = yield* tx.doc(Inbox.LiveDoc, { owner: conversationId })
  live.run = { taskId, inputs: [...inputs] }
  live.generation = { attempt: 1 }
  delete live.tools
  return payload
})

/** Repeating a deferred completion is safe. Called after the cached admission/settlement Activity physically commits. */
export const notify = Effect.fnUntraced(function* (
  session: Session.Service,
  ids: ReadonlyArray<Record.SubmissionId>,
) {
  for (const id of ids) {
    const receipt = yield* session.submission(id).pipe(Effect.mapError(storageError))
    if (receipt === undefined || (receipt.status !== 'done' && receipt.status !== 'unanswered'))
      continue
    const links = yield* session
      .snapshot(Links, { key: String(id) })
      .pipe(Effect.mapError(storageError))
    const value = yield* Schema.decodeEffect(Result)(receipt).pipe(
      Effect.mapError(
        (cause) =>
          new ExecutionError({
            reason: new InvalidState({ message: 'Invalid settled submission receipt', cause }),
          }),
      ),
    )
    for (const executionId of links?.value.executions ?? [])
      yield* DurableDeferred.succeed(Settled, {
        token: DurableDeferred.tokenFromExecutionId(Settled, { workflow: Submission, executionId }),
        value,
      })
  }
})

/** Shared admission commit for native submission and compaction Activities. */
export const admitInTransaction = Effect.fnUntraced(function* (
  tx: Session.Transaction,
  config: Conversation.Configuration['Service'],
  payload: typeof Submission.payloadSchema.Type,
  executionId: string,
): Effect.fn.Return<typeof Admission.Type, StorageError | ExecutionError> {
  const existing = yield* tx.submissionByRequest(payload.conversationId, payload.requestId)
  // Domain request identity takes precedence over the busy rule and ignores changed same-kind content.
  if (existing !== undefined) {
    if (existing.type !== payload.submission.type)
      return yield* new ExecutionError({
        reason: new RequestConflict({
          message: 'Request identity already belongs to a different submission kind',
        }),
      })
    const links = yield* tx.doc(Links, { key: String(existing.id) })
    if (!links.executions.includes(executionId)) links.executions.push(executionId)
    if (existing.status === 'done' || existing.status === 'unanswered')
      return { id: existing.id, notify: [], receipt: existing }
    return { id: existing.id, notify: [] }
  }
  if ((yield* tx.conversation(payload.conversationId)) === undefined)
    return yield* new ExecutionError({
      reason: new InvalidState({ message: 'Conversation is absent' }),
    })
  const boundary = yield* Inbox.prepare(tx, payload.conversationId, config.settings)
  const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
  if (
    payload.submission.type === 'input' &&
    payload.submission.whenBusy === 'reject' &&
    live.run !== undefined
  )
    return yield* new ExecutionError({
      reason: new ConversationBusy({ message: 'Conversation already has an active run' }),
    })
  const submission = yield* tx.createSubmission({
    conversationId: payload.conversationId,
    type: payload.submission.type,
    status: 'queued',
    requestId: payload.requestId,
  })
  const links = yield* tx.doc(Links, { key: String(submission.id) })
  links.executions.push(executionId)
  if (payload.submission.type === 'write')
    boundary.inbox.items.push({
      id: submission.id,
      mode: 'write',
      entry: yield* Document.copyEffect(payload.submission.entry),
    })
  else {
    const message = yield* Schema.encodeEffect(Schema.toCodecJson(Prompt.UserMessage))(
      payload.submission.message,
    ).pipe(
      Effect.mapError(
        (cause) =>
          new ExecutionError({
            reason: new InvalidArguments({ message: 'Input cannot be persisted', cause }),
          }),
      ),
    )
    boundary.inbox.items.push({
      id: submission.id,
      mode: payload.submission.whenBusy === 'steer' ? 'steer' : 'followUp',
      message: yield* Document.copyEffect(message),
    })
  }
  if (live.run !== undefined) return { id: submission.id, notify: [] }
  const selected = yield* Inbox.apply(tx, boundary, 'final', yield* Clock.currentTimeMillis)
  const generation =
    selected.users.length === 0
      ? undefined
      : yield* createGeneration(tx, payload.sessionId, payload.conversationId, selected.users)
  return {
    id: submission.id,
    notify: selected.settled,
    ...(generation === undefined ? {} : { generation }),
  }
})

const admit = Effect.fnUntraced(function* (
  session: Session.Service,
  config: Conversation.Configuration['Service'],
  payload: typeof Submission.payloadSchema.Type,
  executionId: string,
) {
  return yield* session
    .transaction((tx) => admitInTransaction(tx, config, payload, executionId), {
      key: `workflow/submission/admit/${executionId}`,
      fingerprint: JSON.stringify([
        payload.sessionId,
        payload.conversationId,
        payload.requestId,
        payload.submission.type,
      ]),
    })
    .pipe(Effect.mapError((error) => (error._tag === 'StorageError' ? storageError(error) : error)))
})

/** Registers the standard Submission workflow; applications provide their ordinary WorkflowEngine Layer. */
export const layer: Layer.Layer<
  never,
  never,
  Conversation.Configuration | SessionDirectory | WorkflowEngine.WorkflowEngine
> = Submission.toLayer(
  Effect.fnUntraced(function* (payload, executionId) {
    const session = yield* (yield* SessionDirectory)
      .resolve(payload.sessionId)
      .pipe(Effect.mapError(storageError))
    const config = yield* Conversation.Configuration
    if (payload.conversationId === Record.ROOT_CONVERSATION_ID)
      yield* Activity.make({
        name: 'ensure-root',
        success: Record.Conversation,
        error: ExecutionErrorCodec,
        execute: session.root().pipe(Effect.mapError(storageError)),
      }).annotate(ClusterSchema.WithTransaction, true)
    const admitted = yield* Activity.make({
      name: 'admission',
      success: Admission,
      error: ExecutionErrorCodec,
      execute: admit(session, config, payload, executionId),
    }).annotate(ClusterSchema.WithTransaction, true)
    if (admitted.generation !== undefined)
      yield* Generation.execute(admitted.generation, { discard: true })
    yield* notify(session, admitted.notify)
    if (admitted.receipt !== undefined) return admitted.receipt
    return yield* DurableDeferred.await(Settled)
  }),
)
