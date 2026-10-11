/** Fixed native actions under one reservation. This module never exports a public Session. */
import * as Deferred from 'effect/Deferred'
import * as Cause from 'effect/Cause'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Scope from 'effect/Scope'
import type * as ObservationProtocol from '../Observation.js'
import type * as ObservationRuntime from './ObservationRuntime.js'
import type * as RemoteDocument from '../RemoteDocument.js'
import type * as DocumentBackend from './DocumentBackend.js'
import * as DocumentLifetime from './RemoteDocumentLifetime.js'
import * as Agent from '../Agent.js'
import * as Document from '../Document.js'
import * as Subagent from '../Subagent.js'
import type * as Submission from '../Submission.js'
import * as Task from '../Task.js'
import * as Record from '../Record.js'
import type * as TaskExecution from '../TaskExecution.js'
import * as Transaction from '../Transaction.js'
import type { HarnessBackendService, SubmissionRef } from '../HarnessBackend.js'
import { ResetOptionsSchema } from '../HarnessBackend.js'
import * as CompactionTask from '../CompactionTask.js'
import { HarnessError } from '../HarnessError.js'
import type { ExecutionError } from '../ExecutionError.js'
import * as Prompt from 'effect/ai/Prompt'
import * as Supervision from './TaskSupervision.js'
import * as AgentState from './AgentState.js'
import * as Host from './ConversationHost.js'
import * as Supervisor from './SubagentTask.js'
import * as Value from './DocumentValue.js'
import * as NativeTransaction from './TransactionRuntime.js'
import { protect } from './HarnessFailure.js'

export interface Control {
  readonly gate: (tx: Transaction.Transaction) => Effect.Effect<void, TaskExecution.Failure>
  readonly mutation: <A, E, R>(
    change: (tx: Transaction.Transaction) => Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | TaskExecution.Failure, R>
  readonly savepoint: <A, E, R>(
    tx: Transaction.Transaction,
    run: (derived: Transaction.Transaction) => Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | TaskExecution.Failure, R>
  readonly markConversation: (
    tx: Transaction.Transaction,
    id: Record.ConversationId,
    options?: { readonly includeBackground?: boolean },
  ) => Effect.Effect<
    {
      readonly ids: ReadonlyArray<Record.TaskId>
      readonly join: Effect.Effect<void, TaskExecution.Failure>
    },
    TaskExecution.Failure
  >
  readonly ended: Effect.Effect<void>
}
const controls = new WeakMap<TaskExecution.Access, Control>()
const compactions = new WeakMap<
  CompactionTask.Handle,
  {
    readonly backend: HarnessBackendService
    readonly id: Record.TaskId
    readonly conversationId: Record.ConversationId
  }
>()
/** Live identities retain their gated authority even after revocation. Copied DTOs have none. */
export const compactionBackend = (
  handle: CompactionTask.Handle,
): HarnessBackendService | undefined => compactions.get(handle)?.backend
interface Observation {
  readonly observe: <A, E, R>(
    run: () => Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | HarnessError, R>
}
const observations = new WeakMap<HarnessBackendService, Observation>()
export const observation = (backend: HarnessBackendService): Observation | undefined =>
  observations.get(backend)
/** First authentic registration wins; a structural Access does not acquire this capability. */
export const registerControl = (execution: TaskExecution.Access, control: Control): boolean => {
  if (controls.has(execution)) return false
  controls.set(execution, control)
  return true
}
export interface Operations {
  readonly readDocument: (
    tx: Transaction.Transaction,
    request: RemoteDocument.ReadRequest,
    at?: Record.EntryId,
  ) => Effect.Effect<Option.Option<Record.StoredDocument>, RemoteDocument.Failure>
  readonly attachDocument: (
    tx: Transaction.Transaction,
    request: RemoteDocument.ReadRequest,
    scope: Scope.Scope,
    options?: ObservationProtocol.WatchOptions,
  ) => Effect.Effect<DocumentBackend.Attachment, RemoteDocument.Failure>
  readonly attachObservation: (
    tx: Transaction.Transaction,
    id: Record.ConversationId,
    consumerScope: Scope.Scope,
    options?: ObservationProtocol.WatchOptions,
  ) => Effect.Effect<ObservationRuntime.Attachment, ObservationProtocol.Failure>
  readonly backend: () => HarnessBackendService
  readonly submit: (
    tx: Transaction.Transaction,
    id: Record.ConversationId,
    draft: Submission.Draft,
  ) => Effect.Effect<SubmissionRef, HarnessError>
  readonly configure: (
    tx: Transaction.Transaction,
    id: Record.ConversationId,
    change: Agent.Change,
  ) => Effect.Effect<void, HarnessError>
  readonly compact: (
    tx: Transaction.Transaction,
    id: Record.ConversationId,
    options?: CompactionTask.Options,
  ) => Effect.Effect<CompactionTask.Handle, HarnessError>
  readonly withdraw: (
    tx: Transaction.Transaction,
    id: Record.SubmissionId,
  ) => Effect.Effect<Submission.Withdrawal, HarnessError>
  readonly created: (id: Record.ConversationId) => Effect.Effect<void, HarnessError>
  readonly wake: Effect.Effect<void, HarnessError>
}
export const index = Document.define({
  kind: 'harness.subagent.slots',
  version: 1,
  scope: 'task',
  schema: Schema.Struct({
    slots: Schema.Record(
      Schema.String,
      Schema.Struct({
        request: Schema.toCodecJson(Subagent.RequestSchema),
        handle: Subagent.HandleSchema,
        inherited: Agent.StateSchema,
      }),
    ),
  }),
  initial: () => ({ slots: {} }),
})
const target = (taskId: Record.TaskId): Document.Target => ({ scope: { _tag: 'task', taskId } })
const sameRequest = Schema.toEquivalence(Subagent.RequestSchema)
const sameHandle = Schema.toEquivalence(Subagent.HandleSchema)
const sameAgent = Schema.toEquivalence(Agent.StateSchema)
const failure = (reason: Subagent.SubagentError['reason'], message: string) =>
  new Subagent.SubagentError({ reason, message })
const historicalAgent = (
  tx: Transaction.Transaction,
  id: Record.ConversationId,
  at: Record.EntryId,
) =>
  NativeTransaction.use(tx, (state) =>
    Effect.gen(function* () {
      const entry = yield* state.runtime.storage.entry(at, { conversationId: id })
      if (Option.isNone(entry)) return yield* failure('notFound', 'The fork entry is not visible')
      const address = yield* Value.address(
        AgentState.document,
        AgentState.target(entry.value.entry.conversationId),
      )
      const record = yield* state.runtime.storage.findDocument(address, entry.value.commitSeq)
      if (Option.isNone(record))
        return Option.none<Document.Snapshot<typeof AgentState.document.definition.schema>>()
      const stored = yield* state.runtime.storage.document(record.value.id, entry.value.commitSeq)
      if (Option.isNone(stored))
        return yield* failure('invalid', 'The historical agent has no stored content')
      return Option.some(yield* Value.snapshot(AgentState.document, stored.value))
    }),
  )
interface Epoch {
  active: boolean
  admissions: number
  drained: Deferred.Deferred<void>
  observations: number
  observationsDrained: Deferred.Deferred<void>
  readonly done: Deferred.Deferred<void>
  readonly children: Set<Record.TaskId>
}
const epoch = (): Epoch => ({
  active: true,
  admissions: 0,
  drained: Deferred.makeUnsafe(),
  observations: 0,
  observationsDrained: Deferred.makeUnsafe(),
  done: Deferred.makeUnsafe(),
  children: new Set(),
})
const closers = new WeakMap<
  Subagent.Access,
  (cancel: boolean) => Effect.Effect<void, Subagent.Failure>
>()
const stagers = new WeakMap<
  Subagent.Access,
  (tx: Transaction.Transaction) => Effect.Effect<void, Subagent.Failure>
>()
const borrowers = new WeakMap<
  Subagent.Access,
  (guard: Effect.Effect<void, ExecutionError>, restart: 'resume' | 'abandon') => Subagent.Access
>()
const initializers = new WeakMap<Subagent.Access, Effect.Effect<void, Subagent.Failure>>()
const observers = new WeakMap<
  Subagent.Access,
  (attachment: Subagent.Attachment) => Effect.Effect<Subagent.Receipt, Subagent.Failure>
>()
export const initializeOwned = (execution: TaskExecution.Access) =>
  initializers.get(execution.subagent ?? Subagent.unsupported) ??
  Effect.fail(failure('unsupported', 'The supervisor has no native reservation'))
export const observeOwned = (execution: TaskExecution.Access, attachment: Subagent.Attachment) =>
  observers.get(execution.subagent ?? Subagent.unsupported)?.(attachment) ??
  Effect.fail(failure('unsupported', 'The supervisor has no native reservation'))
export const close = (access: Subagent.Access | undefined, cancel = false) =>
  access === undefined ? Effect.void : (closers.get(access)?.(cancel) ?? Effect.void)
export const stageClose = (access: Subagent.Access | undefined, tx: Transaction.Transaction) =>
  access === undefined ? Effect.void : (stagers.get(access)?.(tx) ?? Effect.void)
export const borrow = (
  access: Subagent.Access | undefined,
  guard: Effect.Effect<void, ExecutionError>,
  restart: 'resume' | 'abandon',
): Subagent.Access =>
  access === undefined
    ? Subagent.unsupported
    : (borrowers.get(access)?.(guard, restart) ?? Subagent.unsupported)

export const make = Effect.fnUntraced(function* (
  execution: TaskExecution.Access,
  mode: 'run' | 'abort',
  definition: Supervisor.Definition,
  operations: Operations,
) {
  const control = controls.get(execution)
  if (control === undefined)
    return yield* failure('unsupported', 'The factory requires the actual reserved native Access')
  const root = epoch()
  let marked: ReadonlyArray<Record.TaskId> | undefined
  let sequence = 0
  const mark = (tx: Transaction.Transaction, children?: ReadonlySet<Record.TaskId>) =>
    Effect.gen(function* () {
      yield* control.gate(tx)
      const saved = yield* Transaction.snapshot(tx, index, target(execution.taskId))
      const admitted = new Set(
        Option.isSome(saved)
          ? Object.values(saved.value.value.slots)
              .filter((slot) => slot.handle.mode === 'foreground')
              .map((slot) => slot.handle.taskId)
          : [],
      )
      const reached: Record.TaskId[] = []
      for (const id of admitted) {
        if (children !== undefined && !children.has(id)) continue
        const record = yield* Transaction.task(tx, id)
        if (Option.isNone(record) || record.value.owner !== execution.taskId)
          return yield* failure('denied', 'Cleanup requires the actual directly owned supervisor')
        if (record.value.state.status !== 'terminal')
          yield* Transaction.requestTaskAbort(tx, id, 'caller settled')
        reached.push(id)
      }
      return reached
    })
  const inspect = (tx: Transaction.Transaction, handle: Subagent.Handle) =>
    Effect.gen(function* () {
      yield* control.gate(tx)
      const owned = yield* Value.copy(Subagent.HandleSchema, handle)
      if (
        owned.originTaskId !== execution.taskId ||
        owned.originConversationId !== execution.conversationId
      )
        return yield* failure('denied', 'The handle belongs to another caller')
      const slots = yield* Transaction.snapshot(tx, index, target(execution.taskId))
      const slot =
        Option.isSome(slots) && Object.hasOwn(slots.value.value.slots, owned.key)
          ? slots.value.value.slots[owned.key]
          : undefined
      const record = yield* Transaction.task(tx, owned.taskId)
      if (
        slot === undefined ||
        !sameHandle(slot.handle, owned) ||
        Option.isNone(record) ||
        record.value.kind !== Supervisor.kind ||
        record.value.version !== 1
      )
        return yield* failure('conflict', 'The handle does not identify its saved native admission')
      const input = yield* Task.decode(Supervisor.InputSchema, record.value.input)
      if (
        input.originTaskId !== owned.originTaskId ||
        input.originConversationId !== owned.originConversationId ||
        record.value.conversationId !== owned.originConversationId ||
        input.key !== owned.key ||
        input.mode !== owned.mode ||
        !sameRequest(
          {
            draft: input.draft,
            ...(input.options === undefined ? {} : { options: input.options }),
          },
          slot.request,
        ) ||
        !sameAgent(input.inherited, slot.inherited) ||
        (owned.mode === 'foreground' &&
          (record.value.owner !== execution.taskId || record.value.background)) ||
        (owned.mode === 'background' &&
          (record.value.owner !== undefined || !record.value.background))
      )
        return yield* failure('conflict', 'The saved supervisor has another origin or ownership')
      let attachment: Subagent.Attachment | undefined
      let receipt: Subagent.Receipt | undefined
      if (record.value.state.status === 'terminal' || record.value.state.status === 'completing') {
        const outcome = yield* Task.decode(Record.TaskOutcome, record.value.state.outcome)
        if (
          outcome.status === 'completed' ||
          (outcome.status === 'failed' && outcome.result !== undefined)
        ) {
          receipt = yield* Task.decode(Subagent.ReceiptSchema, outcome.result)
          attachment = {
            conversationId: receipt.conversationId,
            submissionId: receipt.submissionId,
          }
        }
      } else {
        const checkpoint = yield* Task.decodeCheckpoint(
          Supervisor.CheckpointSchema,
          record.value.state.checkpoint,
        )
        if (checkpoint.phase === 'observe')
          attachment = {
            conversationId: checkpoint.conversationId,
            submissionId: checkpoint.submissionId,
          }
      }
      if (attachment !== undefined) {
        const conversation = yield* Transaction.conversation(tx, attachment.conversationId)
        const submission = yield* Transaction.submission(tx, attachment.submissionId)
        if (
          Option.isNone(conversation) ||
          conversation.value.owner?.taskId !== owned.taskId ||
          Option.isNone(submission) ||
          submission.value.conversationId !== attachment.conversationId
        )
          return yield* failure(
            'conflict',
            'The saved child attachment has different ownership or submission identity',
          )
      }
      return { handle: owned, record: record.value, attachment, receipt }
    })
  const ownedTarget = (tx: Transaction.Transaction, id: Record.ConversationId) =>
    Effect.gen(function* () {
      yield* control.gate(tx)
      const conversation = yield* Transaction.conversation(tx, id)
      if (Option.isNone(conversation))
        return yield* failure('notFound', 'The conversation does not exist')
      if (id === execution.conversationId || conversation.value.owner?.taskId === execution.taskId)
        return conversation.value
      const owner = conversation.value.owner?.taskId
      if (owner !== undefined) {
        const slots = yield* Transaction.snapshot(tx, index, target(execution.taskId))
        const handle = Option.isSome(slots)
          ? Object.values(slots.value.value.slots).find((slot) => slot.handle.taskId === owner)
              ?.handle
          : undefined
        if (handle !== undefined) {
          const saved = yield* inspect(tx, handle)
          if (saved.attachment?.conversationId === id) return conversation.value
        }
      }
      return yield* failure('denied', 'The conversation is not owned by this caller')
    })
  const create = (tx: Transaction.Transaction, owner: Record.Task, input: Supervisor.Input) =>
    Effect.gen(function* () {
      yield* control.gate(tx)
      const conversation = yield* Supervision.createConversation(tx, owner, input.options)
      const state = { ...input.inherited }
      for (const key of [
        'model',
        'extensions',
        'tools',
        'modelTools',
        'toolkits',
        'instructions',
      ] as const) {
        const value = input.options?.agent?.[key]
        if (value === null) delete state[key]
        else if (value !== undefined) Object.assign(state, { [key]: value })
      }
      yield* Transaction.setDocument(
        tx,
        AgentState.document,
        AgentState.target(conversation.id),
        state,
      )
      const submission = yield* operations.submit(tx, conversation.id, input.draft)
      const attachment = { conversationId: conversation.id, submissionId: submission.id }
      yield* Transaction.putTask(tx, {
        ...owner,
        state: {
          status: 'pending',
          checkpoint: yield* Task.encodeCheckpoint(Supervisor.CheckpointSchema, {
            phase: 'observe',
            ...attachment,
          }),
        },
      })
      return attachment
    })
  const rootCheck = Effect.suspend(() =>
    root.active ? Effect.void : Effect.fail(failure('revoked', 'The caller invocation has ended')),
  )
  const makeAccess = (
    borrowed: Epoch,
    guard: Effect.Effect<void, ExecutionError>,
    defaultRestart: 'resume' | 'abandon',
  ): Subagent.Access => {
    const active = guard.pipe(
      Effect.andThen(rootCheck),
      Effect.andThen(
        Effect.suspend(() =>
          borrowed.active
            ? Effect.void
            : Effect.fail(failure('revoked', 'The borrowed caller has ended')),
        ),
      ),
    )
    const change = <A, E, R>(run: (tx: Transaction.Transaction) => Effect.Effect<A, E, R>) =>
      control.mutation((tx) =>
        active.pipe(
          Effect.andThen(run(tx)),
          Effect.tap(() => active),
        ),
      )
    const checked = change(() => Effect.void)
    const ended = Effect.raceFirst(
      control.ended,
      Effect.raceFirst(Deferred.await(root.done), Deferred.await(borrowed.done)),
    )
    const observe = <A, E, R>(run: Effect.Effect<A, E, R>) =>
      Effect.raceFirst(
        checked.pipe(
          Effect.andThen(run),
          Effect.tap(() => checked),
        ),
        ended.pipe(
          Effect.andThen(Effect.fail(failure('revoked', 'The caller observation has ended'))),
        ),
      )
    const observeDecoded = <A, E, R>(
      run: () => Effect.Effect<A, E, R>,
    ): Effect.Effect<A, E | HarnessError, R> =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const check = protect('conversation.observe', checked)
          yield* restore(check)
          const epochs = borrowed === root ? [root] : [root, borrowed]
          for (const item of epochs)
            if (item.observations++ === 0) item.observationsDrained = Deferred.makeUnsafe<void>()
          return yield* Effect.gen(function* () {
            const callback = yield* restore(
              Effect.scoped(Effect.suspend(run).pipe(Effect.tap(() => check))),
            ).pipe(Effect.forkChild({ startImmediately: true }))
            const winner = yield* restore(
              Effect.raceFirst(
                Fiber.await(callback).pipe(
                  Effect.map((exit) => ({ _tag: 'callback' as const, exit })),
                ),
                ended.pipe(Effect.as({ _tag: 'ended' as const })),
              ),
            ).pipe(Effect.exit)
            if (Exit.isSuccess(winner) && winner.value._tag === 'callback')
              return yield* winner.value.exit
            yield* Fiber.interrupt(callback)
            const joined = yield* Fiber.await(callback)
            const interruption = Exit.isFailure(winner)
              ? winner.cause
              : Cause.fail(
                  new HarnessError({
                    reason: 'failed',
                    operation: 'conversation.observe',
                    message: 'The caller observation has ended',
                    cause: failure('revoked', 'The caller observation has ended'),
                  }),
                )
            return yield* Effect.failCause(
              Cause.fromReasons<E | HarnessError>([
                ...interruption.reasons,
                ...(Exit.isFailure(joined) ? joined.cause.reasons : []),
              ]),
            )
          }).pipe(
            Effect.ensuring(
              Effect.gen(function* () {
                for (const item of epochs)
                  if (--item.observations === 0)
                    yield* Deferred.succeed(item.observationsDrained, undefined)
              }),
            ),
          )
        }),
      )
    const stream = <A, E, R>(run: Stream.Stream<A, E, R>) =>
      Stream.unwrap(checked.pipe(Effect.as(run))).pipe(
        Stream.mapEffect((value) => checked.pipe(Effect.as(value))),
        Stream.interruptWhen(ended),
      )
    const attachObservation = (
      id: Record.ConversationId,
      consumerScope: Scope.Scope,
      options?: ObservationProtocol.WatchOptions,
    ) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          let attachment: ObservationRuntime.Attachment | undefined
          return yield* restore(
            change((tx) =>
              Effect.gen(function* () {
                yield* ownedTarget(tx, id)
                attachment = yield* operations.attachObservation(tx, id, consumerScope, options)
                return attachment
              }),
            ),
          ).pipe(
            Effect.onExit((exit) =>
              Exit.isFailure(exit) && attachment !== undefined ? attachment.detach : Effect.void,
            ),
            Effect.mapError((cause): ObservationProtocol.Failure =>
              cause._tag === 'ExecutionError' ||
              cause._tag === 'SubagentError' ||
              cause._tag === 'TaskError'
                ? new HarnessError({
                    reason: 'failed',
                    operation: 'conversation.observe',
                    message: cause.message,
                    cause,
                  })
                : cause,
            ),
          )
        }),
      )
    const watchObservation = <F>(
      id: Record.ConversationId,
      options: ObservationProtocol.WatchOptions | undefined,
      select: (
        attachment: ObservationRuntime.Attachment,
      ) => Stream.Stream<F, ObservationProtocol.Failure>,
    ) =>
      stream(
        Stream.unwrap(
          Effect.gen(function* () {
            const outerScope = yield* Scope.Scope
            const attachment = yield* attachObservation(id, outerScope, options)
            return select(attachment)
          }),
        ),
      ).pipe(
        Stream.mapError((cause): ObservationProtocol.Failure =>
          cause._tag === 'ExecutionError' ||
          cause._tag === 'SubagentError' ||
          cause._tag === 'TaskError'
            ? new HarnessError({
                reason: 'failed',
                operation: 'conversation.observe',
                message: cause.message,
                cause,
              })
            : cause,
        ),
      )
    const admit = (tx: Transaction.Transaction, request: Subagent.Request, reservedKey?: string) =>
      Effect.gen(function* () {
        yield* control.gate(tx)
        yield* active
        if (mode === 'abort')
          return yield* failure('revoked', 'Abort cleanup cannot admit conversation work')
        return yield* control.savepoint(tx, (tx) =>
          Effect.gen(function* () {
            yield* control.gate(tx)
            yield* active
            const owned = yield* Value.copy(Subagent.RequestSchema, request)
            const key = reservedKey ?? owned.options?.key ?? String(++sequence)
            if (
              owned.options?.key !== undefined &&
              (key === '' || key.includes('/') || key === '__proto__' || /^[1-9][0-9]*$/.test(key))
            )
              return yield* failure('invalidKey', 'Explicit keys must be safe nonordinal segments')
            const snapshot = yield* Transaction.snapshot(tx, index, target(execution.taskId))
            const slots = Option.isSome(snapshot) ? snapshot.value.value.slots : {}
            if (Object.hasOwn(slots, key)) {
              const slot = slots[key]!
              if (!sameRequest(slot.request, owned))
                return yield* failure(
                  'conflict',
                  'The key already identifies another subagent request',
                )
              yield* inspect(tx, slot.handle)
              if (slot.handle.mode === 'foreground') {
                root.children.add(slot.handle.taskId)
                borrowed.children.add(slot.handle.taskId)
              }
              return slot.handle
            }
            const selection =
              owned.options?.fork === undefined
                ? yield* Transaction.snapshot(
                    tx,
                    AgentState.document,
                    AgentState.target(execution.conversationId),
                  )
                : yield* historicalAgent(
                    tx,
                    owned.options.fork.conversationId,
                    owned.options.fork.at,
                  )
            const inherited = yield* Value.copy(
              Agent.StateSchema,
              Option.isSome(selection) ? selection.value.value : {},
            )
            const selectedMode = owned.options?.mode ?? 'foreground'
            const restartPolicy =
              owned.options?.restartPolicy ??
              (selectedMode === 'background' ? 'resume' : defaultRestart)
            const input: Supervisor.Input = {
              originTaskId: execution.taskId,
              originConversationId: execution.conversationId,
              key,
              mode: selectedMode,
              draft: owned.draft,
              ...(owned.options === undefined ? {} : { options: owned.options }),
              inherited,
              restartPolicy,
            }
            const ref =
              selectedMode === 'foreground'
                ? yield* execution.createChild(tx, definition, input, { restartPolicy })
                : yield* execution.createBackgroundSupervisor(tx, definition, input, {
                    restartPolicy,
                  })
            const handle: Subagent.Handle = {
              originTaskId: execution.taskId,
              originConversationId: execution.conversationId,
              key,
              taskId: ref.id,
              mode: selectedMode,
            }
            if (selectedMode === 'foreground') {
              const record = yield* Transaction.task(tx, ref.id)
              if (Option.isNone(record))
                return yield* failure('notFound', 'New supervisor is unavailable')
              yield* create(tx, record.value, input)
              root.children.add(ref.id)
              borrowed.children.add(ref.id)
            }
            yield* Transaction.ensureDocument(tx, index, target(execution.taskId))
            yield* Transaction.setDocument(tx, index, target(execution.taskId), {
              slots: { ...slots, [key]: { request: owned, handle, inherited } },
            })
            yield* active
            return handle
          }),
        )
      })
    const tracked = <A, E, R>(
      check: Effect.Effect<void, Subagent.Failure>,
      run: Effect.Effect<A, E, R>,
    ) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          yield* check
          const epochs = borrowed === root ? [root] : [root, borrowed]
          for (const item of epochs)
            if (item.admissions++ === 0) item.drained = Deferred.makeUnsafe<void>()
          return yield* restore(run).pipe(
            Effect.ensuring(
              Effect.gen(function* () {
                for (const item of epochs)
                  if (--item.admissions === 0) yield* Deferred.succeed(item.drained, undefined)
              }),
            ),
          )
        }),
      )
    const admission = (request: Subagent.Request) =>
      tracked(
        checked,
        execution.commit((tx) => admit(tx, request)),
      )
    const requireNew =
      mode === 'abort'
        ? Effect.fail(failure('revoked', 'Abort cleanup cannot admit or configure work'))
        : Effect.void
    const mutateTarget = <A, E, R>(
      id: Record.ConversationId,
      run: (tx: Transaction.Transaction) => Effect.Effect<A, E, R>,
      cleanup = false,
    ) =>
      change((tx) =>
        Effect.gen(function* () {
          if (!cleanup) yield* requireNew
          yield* ownedTarget(tx, id)
          return yield* run(tx)
        }),
      )
    const addressTarget = (tx: Transaction.Transaction, address: Record.DocumentAddress) =>
      Effect.gen(function* () {
        yield* control.gate(tx)
        if (address.scope._tag === 'conversation')
          return yield* ownedTarget(tx, address.scope.conversationId).pipe(Effect.asVoid)
        if (address.scope._tag === 'task' && address.scope.taskId === execution.taskId) return
        return yield* failure('denied', 'The document address is outside the caller ownership')
      })
    const compactionValue = (handle: CompactionTask.Handle) =>
      Effect.gen(function* () {
        const associated = compactions.get(handle)
        if (
          associated !== undefined &&
          (associated.backend !== backend ||
            associated.id !== handle.id ||
            associated.conversationId !== handle.conversationId)
        )
          return yield* failure('denied', 'The live compaction handle identity has changed')
        return yield* Value.copy(CompactionTask.HandleSchema, handle)
      })
    const backend: HarnessBackendService = {
      documents: {
        snapshot: (request) =>
          observeDecoded(() =>
            protect(
              'document.remote.snapshot',
              change((tx) =>
                Effect.gen(function* () {
                  yield* addressTarget(tx, request.address)
                  return yield* operations.readDocument(tx, request)
                }),
              ),
            ),
          ),
        snapshotAsOf: (request) =>
          observeDecoded(() =>
            protect(
              'document.remote.history',
              change((tx) =>
                Effect.gen(function* () {
                  yield* addressTarget(tx, request.address)
                  return yield* operations.readDocument(tx, request, request.at)
                }),
              ),
            ),
          ),
        watch: (request, options) =>
          Stream.unwrap(
            Effect.gen(function* () {
              const scope = yield* Scope.Scope
              let attached: DocumentBackend.Attachment | undefined
              return yield* protect(
                'document.remote.watch',
                change((tx) =>
                  Effect.gen(function* () {
                    yield* addressTarget(tx, request.address)
                    attached = yield* operations.attachDocument(tx, request, scope, options)
                    return attached.stream
                  }),
                ),
              ).pipe(
                Effect.onExit((exit) =>
                  Exit.isFailure(exit) && attached !== undefined ? attached.detach : Effect.void,
                ),
              )
            }),
          ).pipe(Stream.interruptWhen(ended)),
      },
      observations: {
        conversationSnapshot: (id) =>
          Effect.scoped(
            observeDecoded(() =>
              Effect.gen(function* () {
                const scope = yield* Scope.Scope
                const attached = yield* attachObservation(id, scope)
                return attached.snapshot
              }),
            ),
          ),
        watchView: (id, options) => watchObservation(id, options, (attachment) => attachment.views),
        watchEvents: (id, options) =>
          watchObservation(id, options, (attachment) => attachment.events),
      },
      compactionTask: (handle) =>
        protect(
          'compaction.read',
          change((tx) =>
            Effect.gen(function* () {
              const value = yield* compactionValue(handle)
              yield* ownedTarget(tx, value.conversationId)
              const record = yield* Transaction.task(tx, value.id)
              if (
                Option.isNone(record) ||
                record.value.kind !== CompactionTask.kind ||
                record.value.version !== 1 ||
                record.value.conversationId !== value.conversationId
              )
                return yield* failure(
                  'conflict',
                  'The compaction handle has another task or conversation',
                )
              return record.value
            }),
          ),
        ),
      waitCompaction: (handle) =>
        protect(
          'compaction.wait',
          observe(
            Effect.gen(function* () {
              const record = yield* backend.compactionTask!(handle)
              const wait = operations.backend().waitCompaction
              if (wait === undefined)
                return yield* failure(
                  'unsupported',
                  'The backend has no native compaction observer',
                )
              return yield* wait({ id: record.id, conversationId: record.conversationId })
            }),
          ),
        ),
      abortCompaction: (handle) =>
        protect(
          'compaction.abort',
          change((tx) =>
            Effect.gen(function* () {
              const value = yield* compactionValue(handle)
              yield* ownedTarget(tx, value.conversationId)
              const record = yield* Transaction.task(tx, value.id)
              if (
                Option.isNone(record) ||
                record.value.kind !== CompactionTask.kind ||
                record.value.version !== 1 ||
                record.value.conversationId !== value.conversationId
              )
                return yield* failure(
                  'conflict',
                  'The compaction handle has another task or conversation',
                )
              yield* Transaction.requestTaskAbort(tx, value.id, 'explicit bound compaction abort')
            }),
          ).pipe(
            Effect.andThen(operations.wake),
            Effect.andThen(Effect.suspend(() => backend.waitCompaction!(handle))),
            Effect.asVoid,
          ),
        ),
      root: protect('conversation.root', checked.pipe(Effect.as(execution.conversationId))),
      create: () =>
        protect(
          'conversation.create',
          Effect.fail(failure('unsupported', 'Use supervised admission to create a conversation')),
        ),
      conversation: (id) =>
        protect(
          'conversation.open',
          change((tx) => ownedTarget(tx, id).pipe(Effect.as(Option.some(id)))),
        ),
      submit: (input) =>
        protect(
          'conversation.submit',
          checked.pipe(
            Effect.andThen(
              Effect.suspend(() =>
                mutateTarget(input.conversationId, (tx) =>
                  operations.submit(tx, input.conversationId, input.draft),
                ),
              ),
            ),
            Effect.tap(() => operations.wake),
          ),
        ),
      configure: (input) =>
        protect(
          'conversation.configure',
          checked.pipe(
            Effect.andThen(
              Effect.suspend(() =>
                mutateTarget(input.conversationId, (tx) =>
                  operations.configure(tx, input.conversationId, input.change),
                ),
              ),
            ),
          ),
        ),
      agent: (id) =>
        protect(
          'conversation.agent',
          observe(
            change((tx) => ownedTarget(tx, id)).pipe(
              Effect.andThen(operations.backend().agent(id)),
            ),
          ),
        ),
      fork: (input) =>
        protect(
          'conversation.fork',
          checked.pipe(
            Effect.andThen(
              Effect.suspend(() =>
                mutateTarget(input.conversationId, (tx) =>
                  Effect.gen(function* () {
                    const entries = yield* Transaction.scanEntries(tx, {
                      conversationId: input.conversationId,
                      order: 'descending',
                    }).pipe(Stream.runCollect)
                    const at = input.options?.at ?? entries[0]?.id
                    if (at === undefined)
                      return yield* failure('invalid', 'An empty conversation has no entry to fork')
                    const record = yield* execution.createConversation(tx, {
                      fork: { conversationId: input.conversationId, at },
                    })
                    return record.id
                  }),
                ),
              ),
            ),
            Effect.tap((id) => operations.created(id)),
          ),
        ),
      reset: (id, options = {}) =>
        protect(
          'conversation.reset',
          mutateTarget(id, (tx) =>
            Effect.gen(function* () {
              const owned = yield* Value.copy(ResetOptionsSchema, options)
              const model =
                owned.handoff === undefined
                  ? undefined
                  : yield* Schema.encodeEffect(Schema.Array(Schema.toCodecJson(Prompt.Message)))([
                      Prompt.userMessage({ content: [Prompt.textPart({ text: owned.handoff })] }),
                    ])
              return yield* operations.submit(tx, id, {
                type: 'write',
                entry: { kind: 'reset', head: 'self', ...(model === undefined ? {} : { model }) },
                ...(owned.requestId === undefined ? {} : { requestId: owned.requestId }),
              })
            }),
          ).pipe(Effect.tap(() => operations.wake)),
        ),
      compact: (id, options) =>
        protect(
          'conversation.compact',
          mutateTarget(id, (tx) => operations.compact(tx, id, options)).pipe(
            Effect.tap((handle) =>
              checked.pipe(
                Effect.andThen(
                  Effect.suspend(() => {
                    if (compactions.has(handle))
                      return Effect.fail(
                        failure('conflict', 'The compaction handle is already bound'),
                      )
                    compactions.set(handle, {
                      backend,
                      id: handle.id,
                      conversationId: handle.conversationId,
                    })
                    return Effect.void
                  }),
                ),
              ),
            ),
            Effect.tap(() => operations.wake),
          ),
        ),
      abort: (id, options) =>
        protect(
          'conversation.abort',
          mutateTarget(id, (tx) => control.markConversation(tx, id, options), true).pipe(
            Effect.flatMap((marked) => observe(marked.join)),
          ),
        ),
      read: (id) =>
        protect(
          'submission.read',
          change((tx) =>
            Effect.gen(function* () {
              const value = yield* Transaction.submission(tx, id)
              if (Option.isNone(value))
                return yield* failure('notFound', 'The submission does not exist')
              yield* ownedTarget(tx, value.value.conversationId)
              return value.value
            }),
          ),
        ),
      wait: (id) =>
        protect(
          'submission.wait',
          observe(backend.read(id).pipe(Effect.andThen(operations.backend().wait(id)))),
        ),
      withdraw: (id) =>
        protect(
          'submission.withdraw',
          change((tx) =>
            Effect.gen(function* () {
              const value = yield* Transaction.submission(tx, id)
              if (Option.isNone(value))
                return yield* failure('notFound', 'The submission does not exist')
              yield* ownedTarget(tx, value.value.conversationId)
              return yield* operations.withdraw(tx, id)
            }),
          ).pipe(Effect.tap(() => operations.wake)),
        ),
      entries: (input) =>
        stream(
          Stream.unwrap(
            change((tx) => ownedTarget(tx, input.conversationId)).pipe(
              Effect.map(() => operations.backend().entries(input)),
            ),
          ),
        ).pipe(
          Stream.mapError(
            (cause) =>
              new HarnessError({
                reason: 'failed',
                operation: 'conversation.entries',
                message: 'Bound observation failed',
                cause,
              }),
          ),
        ),
      snapshot: (address) =>
        protect(
          'conversation.snapshot',
          observe(
            change((tx) => addressTarget(tx, address)).pipe(
              Effect.andThen(Effect.suspend(() => operations.backend().snapshot(address))),
            ),
          ),
        ),
      watch: (address) =>
        stream(
          Stream.unwrap(
            change((tx) => addressTarget(tx, address)).pipe(
              Effect.map(() => operations.backend().watch(address)),
            ),
          ),
        ).pipe(
          Stream.mapError(
            (cause) =>
              new HarnessError({
                reason: 'failed',
                operation: 'conversation.watch',
                message: 'Bound observation failed',
                cause,
              }),
          ),
        ),
      usage: (id) =>
        protect(
          'conversation.usage',
          observe(
            change((tx) =>
              id === undefined
                ? Effect.fail(failure('denied', 'A bound caller must select an owned conversation'))
                : ownedTarget(tx, id),
            ).pipe(
              Effect.andThen(
                Effect.suspend(() => {
                  const usage = operations.backend().usage
                  return usage === undefined
                    ? Effect.fail(
                        new HarnessError({
                          reason: 'invalid',
                          operation: 'conversation.usage',
                          message: 'Usage is unavailable',
                        }),
                      )
                    : usage(id)
                }),
              ),
            ),
          ),
        ),
      waitForIdle: protect(
        'conversation.idle',
        checked.pipe(
          Effect.andThen(
            Effect.fail(
              failure('denied', 'Waiting for the caller itself would form an ownership cycle'),
            ),
          ),
        ),
      ),
    }
    observations.set(backend, { observe: observeDecoded })
    DocumentLifetime.registerAuthorization(backend, (request) =>
      protect(
        'document.remote.authorize',
        change((tx) => addressTarget(tx, request.address)),
      ).pipe(Effect.asVoid),
    )
    const read = (handle: Subagent.Handle) =>
      change((tx) =>
        Effect.gen(function* () {
          const saved = yield* inspect(tx, handle)
          const submission =
            saved.attachment === undefined
              ? Option.none<Submission.Record>()
              : yield* Transaction.submission(tx, saved.attachment.submissionId)
          return {
            handle: saved.handle,
            status: saved.record.state.status,
            ...(saved.attachment === undefined ? {} : { attachment: saved.attachment }),
            ...(saved.record.state.outcome === undefined
              ? {}
              : { outcome: yield* Task.decode(Record.TaskOutcome, saved.record.state.outcome) }),
            ...(Option.isNone(submission) ? {} : { submission: submission.value }),
          }
        }),
      )
    const access: Subagent.Access = {
      check: checked,
      admit: (tx, request) =>
        tracked(control.gate(tx).pipe(Effect.andThen(active)), admit(tx, request)),
      spawn: admission,
      read,
      wait: (handle) =>
        observe(
          Effect.gen(function* () {
            yield* read(handle)
            yield* execution.wait({ id: handle.taskId, definition })
            const saved = yield* change((tx) => inspect(tx, handle))
            const outcome = yield* Task.decode(Record.TaskOutcome, saved.record.state.outcome)
            return {
              handle: saved.handle,
              outcome,
              ...(saved.attachment === undefined ? {} : { attachment: saved.attachment }),
              ...(saved.receipt === undefined ? {} : { receipt: saved.receipt }),
            }
          }),
        ),
      conversation: (id) =>
        backend.conversation(id).pipe(Effect.as(Host.conversation(backend, id))),
      open: (handle) =>
        read(handle).pipe(
          Effect.flatMap((saved) =>
            saved.attachment === undefined
              ? Effect.fail(failure('pending', 'The background child has not been created'))
              : access.conversation(saved.attachment.conversationId),
          ),
        ),
      abort: (handle, options) =>
        change((tx) =>
          Effect.gen(function* () {
            const saved = yield* inspect(tx, handle)
            yield* Transaction.requestTaskAbort(tx, saved.handle.taskId, 'explicit subagent abort')
            return saved.attachment === undefined
              ? undefined
              : yield* control.markConversation(tx, saved.attachment.conversationId, options)
          }),
        ).pipe(
          Effect.flatMap((marked) => marked?.join ?? Effect.void),
          Effect.andThen(observe(execution.wait({ id: handle.taskId, definition }))),
          Effect.asVoid,
        ),
    }
    closers.set(access, (cancel) =>
      Effect.uninterruptible(
        Effect.gen(function* () {
          borrowed.active = false
          yield* Deferred.succeed(borrowed.done, undefined)
          if (borrowed.admissions > 0) yield* Deferred.await(borrowed.drained)
          let ids: ReadonlyArray<Record.TaskId> = []
          if (cancel)
            ids =
              borrowed === root && marked !== undefined
                ? marked
                : yield* execution.commit((tx) =>
                    mark(tx, borrowed === root ? undefined : borrowed.children),
                  )
          if (borrowed.observations > 0) yield* Deferred.await(borrowed.observationsDrained)
          for (const id of ids) yield* execution.wait({ id, definition })
        }),
      ),
    )
    stagers.set(access, (tx) =>
      mark(tx, borrowed === root ? undefined : borrowed.children).pipe(
        Effect.tap((ids) =>
          Effect.sync(() => {
            if (borrowed === root) marked = ids
          }),
        ),
        Effect.asVoid,
      ),
    )
    borrowers.set(access, (gate, restart) => makeAccess(epoch(), gate, restart))
    return access
  }
  const access = makeAccess(root, Effect.void, 'resume')
  initializers.set(
    access,
    execution
      .commit((tx) =>
        Effect.gen(function* () {
          yield* control.gate(tx)
          yield* rootCheck
          if (mode === 'abort')
            return yield* failure('revoked', 'Abort cleanup cannot create a child')
          const record = yield* Transaction.task(tx, execution.taskId)
          if (Option.isNone(record) || record.value.kind !== Supervisor.kind)
            return yield* failure('denied', 'Only the native supervisor can initialize its child')
          const input = yield* Task.decode(Supervisor.InputSchema, record.value.input)
          const checkpoint = yield* Task.decodeCheckpoint(
            Supervisor.CheckpointSchema,
            record.value.state.checkpoint,
          )
          if (
            input.mode !== 'background' ||
            record.value.owner !== undefined ||
            !record.value.background ||
            checkpoint.phase !== 'create'
          )
            return yield* failure(
              'denied',
              'Only an uninitialized background supervisor may create its child',
            )
          const attachment = yield* create(tx, record.value, input)
          return attachment
        }),
      )
      .pipe(
        Effect.tap((attachment) => operations.created(attachment.conversationId)),
        Effect.asVoid,
      ),
  )
  observers.set(access, (attachment) =>
    Effect.gen(function* () {
      yield* access.check
      const conversation = yield* control.mutation((tx) =>
        Effect.gen(function* () {
          const value = yield* Transaction.conversation(tx, attachment.conversationId)
          if (Option.isNone(value) || value.value.owner?.taskId !== execution.taskId)
            return yield* failure('denied', 'The supervisor does not own this conversation')
          const submission = yield* Transaction.submission(tx, attachment.submissionId)
          if (
            Option.isNone(submission) ||
            submission.value.conversationId !== attachment.conversationId
          )
            return yield* failure(
              'conflict',
              'The saved submission belongs to another conversation',
            )
          return value.value
        }),
      )
      const submission = yield* Effect.raceFirst(
        operations.backend().wait(attachment.submissionId),
        control.ended.pipe(Effect.andThen(Effect.interrupt)),
      )
      yield* access.check
      return { conversationId: conversation.id, submissionId: attachment.submissionId, submission }
    }),
  )
  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      root.active = false
      yield* Deferred.succeed(root.done, undefined)
      if (root.observations > 0) yield* Deferred.await(root.observationsDrained)
    }),
  )
  return access
})
