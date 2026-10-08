/**
 * Atomic inbox admission and generation creation.
 */
import type * as Agent from 'effect-harness/Agent'
import * as Arr from 'effect/Array'
import * as Option from 'effect/Option'
import * as Identity from '../Identity.ts'
import type * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import * as Layer from 'effect/Layer'
import * as DateTime from 'effect/DateTime'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Activity from 'effect/workflow/Activity'
import * as DurableDeferred from 'effect/workflow/DurableDeferred'
import * as Conversation from '../Conversation.ts'
import * as Document from '../Document.ts'
import * as Inbox from '../Inbox.ts'
import type * as Ownership from '../Ownership.ts'
import * as Record from '../Record.ts'
import type * as Session from '../Session.ts'
import { SessionDirectory } from '../SessionDirectory.ts'
import { rejected, type StorageError } from '../StorageError.ts'
import {
  ExecutionError,
  ClosedError,
  StorageError as WorkflowStorageError,
  InvalidStateError,
  RequestConflictError,
  ConversationBusyError,
  InvalidArgumentsError,
} from './ExecutionError.ts'
import { Generation } from './Generation.ts'
import { Submission, Result } from './Submission.ts'
import * as Prompt from 'effect/ai/Prompt'

/**
 * Submission settlement notification schema.
 *
 * @category combinators
 */
export const Settled = DurableDeferred.make('submission/settled/v1', {
  success: Result,
  error: ExecutionError,
})
/**
 * Session document family recording native executions waiting for submission settlement.
 *
 * **Details**
 *
 * Each keyed value retains execution IDs notified when the corresponding submission settles.
 *
 * @category schemas
 */
export const Links = Document.familyUnsafe({
  kind: 'harness.submission-links',
  version: 1,
  scope: 'session',
  schema: Schema.Struct({ executions: Schema.Array(Schema.String) }),
  initial: (): { executions: Array<string> } => ({ executions: [] }),
})
const Admission = Schema.Struct({
  id: Record.SubmissionId,
  notify: Schema.Array(Record.SubmissionId),
  generation: Schema.optionalKey(Generation.payloadSchema),
  receipt: Schema.optionalKey(Result),
})
/**
 * Wraps a storage failure in a workflow execution failure.
 *
 * @category combinators
 */
export const storageError = (error: StorageError): ExecutionError =>
  new ExecutionError({
    reason: new (error.reason._tag === 'ClosedError' ? ClosedError : WorkflowStorageError)({
      message: error.message,
      detail: { reason: error.reason._tag, certainty: error.certainty },
      cause: error,
    }),
  })

/** Generation creation data; tx remains the sole resource subject.
 * @category models
 */
export interface GenerationOptions {
  readonly sessionId: Identity.SessionId
  readonly conversationId: Record.ConversationId
  readonly inputs: ReadonlyArray<Record.SubmissionId>
  readonly runId?: Identity.RunId | undefined
}
/**
 * Creates a task projection and payload for an ordinary native generation Workflow.
 *
 * @category constructors
 */
export const makeGeneration = Effect.fnUntraced(function* (
  tx: Session.Transaction,
  options: GenerationOptions,
): Effect.fn.Return<typeof Generation.payloadSchema.Type, StorageError> {
  const { sessionId, conversationId, inputs, runId } = options
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
    _tag: 'task',
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

/**
 * Completes saved native deferred notifications for settled submissions.
 *
 * **Details**
 *
 * Repeated completion is safe; call after the cached admission or settlement Activity physically commits.
 *
 * Called after the cached admission/settlement Activity physically commits.
 *
 * @category combinators
 */
// effect-nit-allow B-no-service-arguments: notify is a public combinator over the exact Session self capability whose committed submission receipts and links it publishes; a shared engine may serve different session owners.
export const notify = Effect.fnUntraced(function* (
  session: Session.Session.Service,
  ids: ReadonlyArray<Record.SubmissionId>,
): Effect.fn.Return<void, ExecutionError, WorkflowEngine.WorkflowEngine> {
  const receipts = yield* Effect.forEach(
    ids,
    (id) => session.submission(id).pipe(Effect.mapError(storageError)),
    { concurrency: 16 },
  )
  const settled = Arr.flatMap(receipts, (receipt) =>
    Option.isSome(receipt) &&
    (receipt.value.status === 'done' || receipt.value.status === 'unanswered')
      ? [receipt.value]
      : [],
  )
  const linksByReceipt = yield* Effect.forEach(
    settled,
    (receipt) =>
      session.snapshot(Links, { key: String(receipt.id) }).pipe(Effect.mapError(storageError)),
    { concurrency: 16 },
  )
  // Sampling batches are independent; deferred completions retain original input order.
  for (const [index, receipt] of settled.entries()) {
    if (receipt === undefined || (receipt.status !== 'done' && receipt.status !== 'unanswered'))
      continue
    const links = linksByReceipt[index]
    const value = yield* Schema.decodeEffect(Result)(receipt).pipe(
      Effect.mapError(
        (cause) =>
          new ExecutionError({
            reason: new InvalidStateError({ message: 'Invalid settled submission receipt', cause }),
          }),
      ),
    )
    for (const executionId of links === undefined
      ? []
      : links.pipe(
          Option.map((snapshot) => snapshot.value.executions),
          Option.getOrElse(() => []),
        ))
      yield* DurableDeferred.succeed(Settled, {
        token: DurableDeferred.tokenFromExecutionId(Settled, { workflow: Submission, executionId }),
        value,
      })
  }
})

/** Resolves request replay and validates conversation presence before sampling live settings.
 * @category combinators
 */
export const existingAdmission = Effect.fnUntraced(function* (
  tx: Session.Transaction,
  payload: typeof Submission.payloadSchema.Type,
  executionId: string,
): Effect.fn.Return<Option.Option<typeof Admission.Type>, StorageError | ExecutionError> {
  const existingOption = yield* tx.submissionByRequest(payload.conversationId, payload.requestId)
  // Domain request identity takes precedence over the busy rule and ignores changed same-kind content.
  if (Option.isSome(existingOption)) {
    const existing = existingOption.value
    if (existing.type !== payload.submission._tag)
      return yield* new ExecutionError({
        reason: new RequestConflictError({
          message: 'Request identity already belongs to a different submission kind',
        }),
      })
    const links = yield* tx.doc(Links, { key: String(existing.id) })
    if (!Arr.contains(links.executions, executionId)) links.executions.push(executionId)
    if (existing.status === 'done' || existing.status === 'unanswered')
      return Option.some({ id: existing.id, notify: [], receipt: existing })
    return Option.some({ id: existing.id, notify: [] })
  }
  if (Option.isNone(yield* tx.conversation(payload.conversationId)))
    return yield* new ExecutionError({
      reason: new InvalidStateError({ message: 'Conversation is absent' }),
    })
  return Option.none<typeof Admission.Type>()
})

/**
 * Admits a native submission or compaction request in the supplied transaction.
 *
 * @category combinators
 */
export const admitInTransaction = Effect.fnUntraced(function* (
  tx: Session.Transaction,
  settings: Agent.Settings,
  payload: typeof Submission.payloadSchema.Type,
  executionId: string,
): Effect.fn.Return<typeof Admission.Type, StorageError | ExecutionError> {
  const existing = yield* existingAdmission(tx, payload, executionId)
  if (Option.isSome(existing)) return existing.value
  const boundary = yield* Inbox.prepare(tx, payload.conversationId, settings)
  const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
  if (
    payload.submission._tag === 'input' &&
    payload.submission.whenBusy === 'reject' &&
    live.run !== undefined
  )
    return yield* new ExecutionError({
      reason: new ConversationBusyError({ message: 'Conversation already has an active run' }),
    })
  const identity = { conversationId: payload.conversationId, requestId: payload.requestId }
  const submission = yield* tx.createSubmission(
    payload.submission._tag === 'input'
      ? { ...identity, _tag: 'InputQueued', type: 'input', status: 'queued' }
      : { ...identity, _tag: 'WriteQueued', type: 'write', status: 'queued' },
  )
  const links = yield* tx.doc(Links, { key: String(submission.id) })
  links.executions.push(executionId)
  if (payload.submission._tag === 'write')
    boundary.inbox.items.push({
      _tag: 'write',
      id: submission.id,
      entry: yield* Document.copyEffect(payload.submission.entry),
    })
  else {
    const message = yield* Schema.encodeEffect(Schema.toCodecJson(Prompt.UserMessage))(
      payload.submission.message,
    ).pipe(
      Effect.mapError(
        (cause) =>
          new ExecutionError({
            reason: new InvalidArgumentsError({ message: 'Input cannot be persisted', cause }),
          }),
      ),
    )
    boundary.inbox.items.push({
      _tag: 'input',
      id: submission.id,
      mode: payload.submission.whenBusy === 'steer' ? 'steer' : 'followUp',
      message: yield* Document.copyEffect(message),
    })
  }
  if (live.run !== undefined) return { id: submission.id, notify: [] }
  const selected = yield* Inbox.apply(tx, boundary, 'final', yield* DateTime.now)
  const generation = Arr.isReadonlyArrayEmpty(selected.users)
    ? undefined
    : yield* makeGeneration(tx, {
        sessionId: payload.sessionId,
        conversationId: payload.conversationId,
        inputs: selected.users,
      })
  return {
    id: submission.id,
    notify: selected.settled,
    ...(generation === undefined ? {} : { generation }),
  }
})

/**
 * Registers the standard Submission workflow; applications provide their ordinary WorkflowEngine Layer.
 *
 * @category layers
 */
export const layer: Layer.Layer<
  never,
  never,
  Conversation.Configuration | SessionDirectory | WorkflowEngine.WorkflowEngine
> = Layer.unwrap(
  Effect.gen(function* () {
    const directory = yield* SessionDirectory
    const config = yield* Conversation.Configuration
    return Submission.toLayer(
      Effect.fnUntraced(function* (payload, executionId) {
        const session = yield* directory
          .resolve(payload.sessionId)
          .pipe(Effect.mapError(storageError))
        const admit = Effect.suspend(() =>
          session
            .transaction(
              Effect.fnUntraced(function* (tx) {
                const existing = yield* existingAdmission(tx, payload, executionId)
                if (Option.isSome(existing)) return existing.value
                const settings = yield* config.settings.pipe(
                  Effect.mapError((cause) => rejected('Invalid host settings', undefined, cause)),
                )
                return yield* admitInTransaction(tx, settings, payload, executionId)
              }),
              {
                key: `workflow/submission/admit/${executionId}`,
                fingerprint: JSON.stringify([
                  payload.sessionId,
                  payload.conversationId,
                  payload.requestId,
                  payload.submission._tag,
                ]),
              },
            )
            .pipe(
              Effect.mapError((error) =>
                error._tag === 'StorageError' ? storageError(error) : error,
              ),
            ),
        )
        if (payload.conversationId === Record.ROOT_CONVERSATION_ID)
          yield* Activity.make({
            name: 'ensure-root',
            success: Record.Conversation,
            error: ExecutionError,
            execute: session.root().pipe(Effect.mapError(storageError)),
          })
        const admitted = yield* Activity.make({
          name: 'admission',
          success: Admission,
          error: ExecutionError,
          execute: admit,
        })
        if (admitted.generation !== undefined)
          yield* Generation.execute(admitted.generation, { discard: true })
        yield* notify(session, admitted.notify)
        if (admitted.receipt !== undefined) return admitted.receipt
        return yield* DurableDeferred.await(Settled)
      }),
    )
  }),
)
