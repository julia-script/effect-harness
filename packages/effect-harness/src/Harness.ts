/** Scoped embedded harness: one owner, persisted phase tasks, application-owned environment. */
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Prompt from 'effect/ai/Prompt'
import * as Agent from './Agent.ts'
import * as Executor from './Executor.ts'
import type * as Conversation from './Conversation.ts'
import type * as Submission from './Submission.ts'
import type * as Record from './Record.ts'
import {
  ConflictError,
  CorruptError,
  NotFoundError,
  rejected,
  type StorageError,
} from './StorageError.ts'
import type * as Observation from './Observation.ts'
import type * as Persistence from './Persistence.ts'
import type * as Task from './Task.ts'
import * as Session from './internal/Session.ts'
import * as Scheduler from './internal/Scheduler.ts'
import * as Builtins from './internal/Builtins.ts'
import * as CompactionTask from './internal/CompactionTask.ts'
import * as State from './internal/ConversationState.ts'
import { RequestId } from './Identity.ts'
import * as CreationTask from './internal/CreationTask.ts'

/** Admission options are wire data; handles and executable task definitions remain live values. */
export const SubmitOptions = Schema.Struct({
  requestId: Schema.optionalKey(RequestId),
  mode: Schema.optionalKey(Schema.Literals(['steering', 'followUp', 'reject'])),
})
export type SubmitOptions = typeof SubmitOptions.Type
export const Input = Schema.Union([Schema.String, Schema.Array(Prompt.UserMessagePart)])
export type Input = typeof Input.Type
export type Transaction = Session.Transaction
export interface Options {
  readonly agent?: Agent.State
  readonly settings?: Agent.Settings.Input
  readonly cwd?: string
  readonly report?: (error: unknown) => Effect.Effect<void>
  readonly tasks?: ReadonlyArray<Task.BoundDefinition>
}
export interface Service {
  readonly root: Effect.Effect<Conversation.Conversation, StorageError>
  readonly create: Effect.Effect<Conversation.Conversation, StorageError>
  readonly conversation: (
    id: Record.ConversationId,
  ) => Effect.Effect<Conversation.Conversation, StorageError>
  readonly fork: (
    id: Record.ConversationId,
    at: Record.EntryId,
  ) => Effect.Effect<Conversation.Conversation, StorageError>
  readonly submit: (
    id: Record.ConversationId,
    input: string | ReadonlyArray<Prompt.UserMessagePart>,
    options?: SubmitOptions,
  ) => Effect.Effect<Submission.Submission, StorageError | Schema.SchemaError>
  readonly append: (
    id: Record.ConversationId,
    draft: Record.Entry.Draft,
  ) => Effect.Effect<Record.Entry, StorageError>
  readonly configure: (
    id: Record.ConversationId,
    change: Agent.State.Change,
  ) => Effect.Effect<void, StorageError>
  readonly withdraw: (
    id: Record.SubmissionId,
  ) => Effect.Effect<Submission.WithdrawalResult, StorageError>
  readonly submission: Session.Service['submission']
  readonly awaitSubmission: (
    id: Record.SubmissionId,
  ) => Effect.Effect<Record.Submission, StorageError>
  readonly snapshot: (
    id: Record.ConversationId,
  ) => Effect.Effect<Observation.Snapshot, StorageError>
  readonly watch: (id: Record.ConversationId) => Stream.Stream<Observation.Change, StorageError>
  readonly document: Session.Service['snapshot']
  readonly transaction: Session.Service['transaction']
  readonly spawn: Scheduler.Scheduler['create']
  readonly task: Session.Service['task']
  readonly awaitTask: Scheduler.Scheduler['await']
  readonly abortTask: Scheduler.Scheduler['abort']
  readonly abort: Scheduler.Scheduler['abortConversation']
  readonly awaitIdle: Scheduler.Scheduler['idle']
  readonly compact: (
    id: Record.ConversationId,
    instructions?: string,
  ) => Effect.Effect<Record.TaskId, StorageError | Schema.SchemaError>
  readonly installTasks: (definitions: ReadonlyArray<Task.BoundDefinition>) => Effect.Effect<void>
  readonly resume: Effect.Effect<void, StorageError>
  readonly close: Effect.Effect<void>
}
export class Harness extends Context.Service<Harness, Service>()('effect-harness/Harness') {}

/** Open within a Scope. Saved tasks start when resume is called; new submissions start immediately. */
export const make = Effect.fn('Harness.make')(function* (options: Options = {}) {
  const session = yield* Session.make
  const executor = yield* Executor.Executor
  const settings = yield* Agent.settings(options.settings ?? {})
  const builtinOptions: Builtins.Options = {
    settings,
    cwd: options.cwd ?? '.',
    report: options.report ?? (() => Effect.void),
  }
  const compaction = yield* CompactionTask.make(session, executor, builtinOptions)
  const builtins = yield* Builtins.make(session, executor, builtinOptions, compaction)
  const creation = yield* CreationTask.make(session, executor, builtinOptions)
  const creationInput = yield* creation.prepare(null)
  const createHook = (tx: Session.Transaction, conversationId: Record.ConversationId) =>
    tx.createTask({
      conversationId,
      kind: creation.name,
      version: creation.version,
      input: creationInput.input,
      background: false,
      abortRequested: false,
      state: { status: 'pending', checkpoint: creationInput.checkpoint },
    })
  const core = [builtins.turn, builtins.tool, compaction, creation]
  const scheduler = yield* Scheduler.make(session, [...core, ...(options.tasks ?? [])])
  yield* session.initialize
  const root = yield* session.root
  const existing = yield* session.snapshot(State.AgentDoc, { owner: root.id })
  if (Option.isNone(existing))
    yield* session
      .transaction(
        Effect.fnUntraced(function* (tx) {
          Object.assign(yield* tx.doc(State.AgentDoc, { owner: root.id }), options.agent ?? {})
          yield* tx.doc(State.InboxDoc, { owner: root.id })
          return yield* createHook(tx, root.id)
        }),
      )
      .pipe(Effect.flatMap(scheduler.admit))
  let service: Service
  const handle = (id: Record.ConversationId): Conversation.Conversation => ({
    id,
    harness: service,
  })
  const snapshot = Effect.fnUntraced(function* (id: Record.ConversationId) {
    const conversation = yield* session.conversation(id)
    if (Option.isNone(conversation))
      return yield* rejected(`Conversation ${id} does not exist`, NotFoundError)
    const tasks = yield* Stream.runCollect(session.scanTasks({ conversationId: id }))
    const documents = [
      ...(yield* Stream.runCollect(
        session.scanDocuments({ scope: { _tag: 'conversation', conversationId: id } }),
      )),
    ]
    for (const task of tasks)
      documents.push(
        ...(yield* Stream.runCollect(
          session.scanDocuments({ scope: { _tag: 'task', taskId: task.id } }),
        )),
      )
    const values = yield* Effect.forEach(documents, (record) => session.document(record.id))
    return {
      revision: (yield* session.metadata).revision,
      conversation: conversation.value,
      entries: yield* Stream.runCollect(session.scanEntries({ conversationId: id })),
      tasks,
      submissions: yield* Stream.runCollect(session.scanSubmissions({ conversationId: id })),
      documents: values.flatMap((value) =>
        Option.isSome(value)
          ? [{ record: value.value.record, version: value.value.version, value: value.value.value }]
          : [],
      ),
    } satisfies Observation.Snapshot
  })
  const watch = (id: Record.ConversationId): Stream.Stream<Observation.Change, StorageError> =>
    Stream.unwrap(
      Effect.gen(function* () {
        const observed = yield* session.observe(snapshot(id))
        let revision: Record.Seq | 0 = observed.revision
        const changes = observed.frames.pipe(
          Stream.mapEffect(
            Effect.fnUntraced(function* (frame): Effect.fn.Return<
              Option.Option<Observation.Change>,
              StorageError
            > {
              if (frame.seq <= revision) return Option.none()
              if (frame.seq !== revision + 1) {
                const value = yield* session.transaction(() => snapshot(id))
                revision = value.revision
                return Option.some({ _tag: 'reset', value })
              }
              revision = frame.seq
              return Option.some({ _tag: 'commit', frame })
            }),
          ),
          Stream.filter(Option.isSome),
          Stream.map((value) => value.value),
        )
        return Stream.concat(
          Stream.succeed<Observation.Change>({ _tag: 'snapshot', value: observed.snapshot }),
          changes,
        )
      }),
    )
  const close = scheduler.close.pipe(Effect.andThen(session.seal))
  yield* Effect.addFinalizer(() => close)
  service = {
    root: Effect.sync(() => handle(root.id)),
    create: session
      .transaction(
        Effect.fnUntraced(function* (tx) {
          const conversation = yield* tx.createConversation({ ownership: Session.Ownership.none() })
          Object.assign(
            yield* tx.doc(State.AgentDoc, { owner: conversation.id }),
            options.agent ?? {},
          )
          yield* tx.doc(State.InboxDoc, { owner: conversation.id })
          return {
            conversation: handle(conversation.id),
            taskId: yield* createHook(tx, conversation.id),
          }
        }),
      )
      .pipe(
        Effect.tap((created) => scheduler.admit(created.taskId)),
        Effect.map((created) => created.conversation),
      ),
    conversation: Effect.fnUntraced(function* (id) {
      if (Option.isNone(yield* session.conversation(id)))
        return yield* rejected(`Conversation ${id} does not exist`, NotFoundError)
      return handle(id)
    }),
    fork: (id, at) =>
      session
        .transaction(
          Effect.fnUntraced(function* (tx) {
            const conversation = yield* tx.forkConversation(id, at, {
              ownership: Session.Ownership.none(),
            })
            yield* tx.doc(State.InboxDoc, { owner: conversation.id })
            return {
              conversation: handle(conversation.id),
              taskId: yield* createHook(tx, conversation.id),
            }
          }),
        )
        .pipe(
          Effect.tap((created) => scheduler.admit(created.taskId)),
          Effect.map((created) => created.conversation),
        ),
    submit: Effect.fnUntraced(function* (id, input, submitOptions = {}) {
      const validated = yield* Schema.decodeEffect(SubmitOptions)(submitOptions)
      const decoded = yield* Schema.decodeEffect(Schema.toType(Input))(input)
      const model = yield* State.encodeMessages([
        Prompt.userMessage({
          content: typeof decoded === 'string' ? [Prompt.textPart({ text: decoded })] : decoded,
        }),
      ])
      const prepared = yield* builtins.turn.prepare(null)
      const admitted = yield* session.transaction(
        Effect.fnUntraced(function* (tx) {
          if (Option.isNone(yield* tx.conversation(id)))
            return yield* rejected(`Conversation ${id} does not exist`, NotFoundError)
          if (validated.requestId !== undefined) {
            const existing = yield* tx.submissionByRequest(id, validated.requestId)
            if (Option.isSome(existing)) return { submission: existing.value }
          }
          const inbox = yield* tx.doc(State.InboxDoc, { owner: id })
          if (inbox.active !== undefined) {
            const active = yield* tx.task(inbox.active)
            if (Option.isNone(active) || active.value.state.status === 'terminal') {
              for (const pending of inbox.pending)
                yield* tx.settleSubmission(pending, {
                  status: 'unanswered',
                  reason: 'Previous turn ended without an answer',
                })
              inbox.pending = []
              delete inbox.active
            }
          }
          if (validated.mode === 'reject' && inbox.active !== undefined)
            return yield* rejected(`Conversation ${id} is busy`, ConflictError)
          const submission = yield* tx.createSubmission({
            _tag: 'InputQueued',
            conversationId: id,
            type: 'input',
            status: 'queued',
            ...(validated.requestId === undefined ? {} : { requestId: validated.requestId }),
          })
          Reflect.set(inbox.queue, inbox.queue.length, {
            submissionId: submission.id,
            model,
            mode: validated.mode === 'steering' ? 'steering' : 'followUp',
          })
          const prerequisites = (yield* tx.tasks({ conversationId: id, kind: creation.name }))
            .filter((task) => task.state.status !== 'terminal')
            .map((task) => task.id)
          if (inbox.active === undefined)
            inbox.active = yield* tx.createTask({
              conversationId: id,
              kind: builtins.turn.name,
              version: builtins.turn.version,
              input: prepared.input,
              background: false,
              abortRequested: false,
              state:
                prerequisites.length === 0
                  ? { status: 'pending', checkpoint: prepared.checkpoint }
                  : {
                      status: 'waiting',
                      checkpoint: prepared.checkpoint,
                      on: prerequisites,
                      policy: 'allSettled',
                    },
            })
          return { submission, taskId: inbox.active, prerequisites }
        }),
      )
      if (admitted.taskId !== undefined) yield* scheduler.admit(admitted.taskId)
      for (const id of admitted.prerequisites ?? []) yield* scheduler.admit(id)
      return { id: admitted.submission.id, harness: service }
    }),
    append: (id, draft) => session.transaction((tx) => tx.appendEntry(id, draft)),
    configure: (id, change) =>
      session.transaction(
        Effect.fnUntraced(function* (tx) {
          if (Option.isNone(yield* tx.conversation(id)))
            return yield* rejected(`Conversation ${id} does not exist`, NotFoundError)
          const draft = yield* tx.doc(State.AgentDoc, { owner: id })
          const configured = Agent.configure(draft, change)
          for (const key of Object.keys(draft))
            if (!Object.hasOwn(configured, key)) Reflect.deleteProperty(draft, key)
          Object.assign(draft, configured)
        }),
      ),
    withdraw: (id) =>
      session.transaction(
        Effect.fn('Harness.withdraw')(function* (tx) {
          const found = yield* tx.submission(id)
          if (Option.isNone(found))
            return yield* rejected(`Submission ${id} does not exist`, NotFoundError)
          const submission = found.value
          if (submission.status === 'done' || submission.status === 'unanswered') return 'settled'
          if (submission.status === 'placed') return 'already_placed'
          if (submission.type !== 'input')
            return yield* rejected('Only queued input can be withdrawn')
          const inbox = yield* tx.doc(State.InboxDoc, { owner: submission.conversationId })
          if (!inbox.queue.some((item) => item.submissionId === id))
            return yield* rejected('Queued submission is absent from its inbox', CorruptError)
          inbox.queue = inbox.queue.filter((item) => item.submissionId !== id)
          yield* tx.settleSubmission(id, { status: 'unanswered', reason: 'aborted' })
          return 'aborted'
        }),
      ),
    submission: session.submission,
    awaitSubmission: (id) =>
      Effect.gen(function* () {
        for (;;) {
          const settled = yield* Effect.scoped(
            Effect.gen(function* () {
              const observed = yield* session.observe(session.submission(id))
              if (Option.isNone(observed.snapshot))
                return yield* rejected(`Submission ${id} does not exist`, NotFoundError)
              const value = observed.snapshot.value
              if (value.status === 'done' || value.status === 'unanswered')
                return Option.some(value)
              yield* observed.frames.pipe(Stream.take(1), Stream.runDrain)
              return Option.none<Record.Submission>()
            }),
          )
          if (Option.isSome(settled)) return settled.value
        }
      }),
    snapshot: (id) => session.transaction(() => snapshot(id)),
    watch,
    document: session.snapshot,
    transaction: session.transaction,
    spawn: scheduler.create,
    task: session.task,
    awaitTask: scheduler.await,
    abortTask: scheduler.abort,
    abort: scheduler.abortConversation,
    awaitIdle: scheduler.idle,
    compact: (id, instructions) =>
      scheduler.create(
        compaction,
        { reason: 'manual', ...(instructions === undefined ? {} : { instructions }) },
        { conversationId: id, background: true },
      ),
    installTasks: (definitions) => scheduler.install([...core, ...definitions]),
    resume: scheduler.resume,
    close,
  }
  return service
})
export const layer = (
  options: Options = {},
): Layer.Layer<
  Harness,
  StorageError | Schema.SchemaError,
  Persistence.Persistence | Executor.Executor
> => Layer.effect(Harness, make(options))
