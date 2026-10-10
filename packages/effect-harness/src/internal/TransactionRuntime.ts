/** Isolated record overlays and schema-encoded document replacements for one commit. */
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Semaphore from 'effect/Semaphore'
import * as Stream from 'effect/Stream'
import * as Struct from 'effect/Struct'
import type * as Identity from '../Identity.js'
import * as Domain from '../Record.js'
import type * as Document from '../Document.js'
import * as Record from '../Record.js'
import * as Sequence from '../Sequence.js'
import type * as Session from '../Session.js'
import { SessionError, type Failure } from '../SessionError.js'
import * as Transaction from '../Transaction.js'
import * as Value from './DocumentValue.js'
import type * as Runtime from './SessionRuntime.js'

interface Draft {
  readonly key: string
  readonly original: Option.Option<Record.StoredDocument>
  readonly record: Domain.DocumentCreate | undefined
  readonly source: { readonly id: Domain.DocumentId; readonly at: Record.DocumentPoint } | undefined
  value: Record.StoredDocument
  changed: boolean
  retired: boolean
}

export interface State {
  readonly runtime: Runtime.State
  readonly options: Session.CommitOptions
  readonly semaphore: Semaphore.Semaphore
  readonly conversations: Map<Domain.ConversationId, Domain.Conversation>
  readonly entries: Map<Domain.EntryId, Domain.Entry>
  readonly tasks: Map<Domain.TaskId, Domain.Task>
  readonly submissions: Map<Domain.SubmissionId, Domain.Submission>
  readonly documents: Map<string, Draft>
  readonly incarnations: Map<Domain.DocumentId, Draft>
  readonly copiedSources: Set<Domain.DocumentId>
  active: boolean
}

const states = new WeakMap<object, State>()
export const register = (self: object, state: State): void => {
  states.set(self, state)
}
const error = (reason: SessionError['reason'], operation: string, message: string) =>
  new SessionError({ reason, operation, message })

export const active = Effect.fnUntraced(function* (state: State) {
  if (!state.active)
    return yield* error('revoked', 'transaction.access', 'Transaction callback has ended')
})

export const use = Effect.fnUntraced(function* <A, E, R>(
  self: object,
  run: (state: State) => Effect.Effect<A, E, R>,
): Effect.fn.Return<A, E | Failure, R> {
  const state = states.get(self)
  if (state === undefined)
    return yield* error('invalid', 'transaction.access', 'Invalid Transaction instance')
  yield* active(state)
  return yield* state.semaphore.withPermit(
    Effect.gen(function* () {
      yield* active(state)
      const value = yield* run(state)
      yield* active(state)
      return value
    }),
  )
})

export const make = Effect.fnUntraced(function* (
  runtime: Runtime.State,
  options: Session.CommitOptions,
): Effect.fn.Return<State> {
  return {
    runtime,
    options,
    semaphore: yield* Semaphore.make(1),
    conversations: new Map(),
    entries: new Map(),
    tasks: new Map(),
    submissions: new Map(),
    documents: new Map(),
    incarnations: new Map(),
    copiedSources: new Set(),
    active: true,
  } satisfies State
})

export const conversation = Effect.fnUntraced(function* (state: State, id: Domain.ConversationId) {
  const staged = state.conversations.get(id)
  if (staged !== undefined) return Option.some(yield* Value.copy(Domain.Conversation, staged))
  return yield* state.runtime.storage.conversation(id)
})

export const task = Effect.fnUntraced(function* (state: State, id: Domain.TaskId) {
  const staged = state.tasks.get(id)
  if (staged !== undefined) return Option.some(yield* Value.copy(Domain.Task, staged))
  return yield* state.runtime.storage.task(id)
})

export const submission = Effect.fnUntraced(function* (state: State, id: Domain.SubmissionId) {
  const staged = state.submissions.get(id)
  if (staged !== undefined) return Option.some(yield* Value.copy(Domain.Submission, staged))
  return yield* state.runtime.storage.submission(id)
})

const ancestry = Effect.fnUntraced(function* (
  state: State,
  id: Domain.ConversationId,
  max: number,
) {
  const result: Array<{ readonly id: Domain.ConversationId; readonly max: number }> = []
  const seen = new Set<Domain.ConversationId>()
  let current = id
  let cap = max
  while (true) {
    if (seen.has(current))
      return yield* error('invalid', 'conversation.history', 'Cyclic conversation ancestry')
    seen.add(current)
    const record = yield* conversation(state, current)
    if (Option.isNone(record))
      return yield* error('notFound', 'conversation.history', 'Conversation is absent')
    result.push({ id: current, max: cap })
    if (record.value.parent === undefined) return result
    current = record.value.parent.conversationId
    cap = Math.min(cap, record.value.parent.at)
  }
})

export const entry = Effect.fnUntraced(function* (
  state: State,
  id: Domain.EntryId,
  options?: { readonly conversationId: Domain.ConversationId },
) {
  const staged = state.entries.get(id)
  const stored = staged === undefined ? yield* state.runtime.storage.entry(id) : Option.none()
  const value = staged ?? (Option.isSome(stored) ? stored.value.entry : undefined)
  if (value === undefined) return Option.none()
  if (options !== undefined) {
    const ancestors = yield* ancestry(state, options.conversationId, id)
    if (!ancestors.some((ancestor) => ancestor.id === value.conversationId && id <= ancestor.max))
      return Option.none()
  }
  return Option.some(yield* Value.copy(Domain.Entry, value))
})

export const scanEntries = Effect.fnUntraced(function* (state: State, input: Record.EntryQuery) {
  const query = yield* Schema.decodeEffect(Record.EntryQuerySchema)(input)
  const ancestors = yield* ancestry(
    state,
    query.conversationId,
    query.maxEntryId ?? Number.MAX_SAFE_INTEGER,
  )
  const committed = ancestors.find((ancestor) => !state.conversations.has(ancestor.id))
  const stored =
    committed === undefined
      ? []
      : yield* state.runtime.storage
          .scanEntries({
            ...query,
            conversationId: committed.id,
            maxEntryId: yield* Schema.decodeEffect(Domain.EntryId)(committed.max),
          })
          .pipe(Stream.runCollect)
  const staged = Array.from(state.entries.values()).filter(
    (value) =>
      value.id >= (query.minEntryId ?? 0) &&
      ancestors.some(
        (ancestor) => ancestor.id === value.conversationId && value.id <= ancestor.max,
      ),
  )
  return yield* Value.copy(
    Schema.Array(Domain.Entry),
    [...stored, ...staged].sort((a, b) =>
      query.order === 'ascending' ? a.id - b.id : b.id - a.id,
    ),
  )
})

export const scanConversations = Effect.fnUntraced(function* (
  state: State,
  input: Record.ConversationQuery = {},
) {
  const query = yield* Schema.decodeEffect(Record.ConversationQuerySchema)(input)
  const stored = yield* state.runtime.storage.scanConversations(query).pipe(Stream.runCollect)
  const staged = Array.from(state.conversations.values()).filter(
    (value) =>
      (query.ownerConversationId === undefined ||
        value.owner?.conversationId === query.ownerConversationId) &&
      (query.ownerTaskId === undefined || value.owner?.taskId === query.ownerTaskId),
  )
  return yield* Value.copy(
    Schema.Array(Domain.Conversation),
    [...stored, ...staged].sort((a, b) =>
      query.order === 'descending' ? b.id - a.id : a.id - b.id,
    ),
  )
})

export const scanTasks = Effect.fnUntraced(function* (state: State, input: Record.TaskQuery = {}) {
  const query = yield* Schema.decodeEffect(Record.TaskQuerySchema)(input)
  const stored = yield* state.runtime.storage.scanTasks(query).pipe(Stream.runCollect)
  const staged = Array.from(state.tasks.values()).filter(
    (value) =>
      (query.conversationId === undefined || value.conversationId === query.conversationId) &&
      (query.kind === undefined || value.kind === query.kind) &&
      (query.status === undefined || value.state.status === query.status) &&
      (query.background === undefined || value.background === query.background) &&
      (query.abortRequested === undefined || value.abortRequested === query.abortRequested),
  )
  return yield* Value.copy(
    Schema.Array(Domain.Task),
    [...stored.filter((value) => !state.tasks.has(value.id)), ...staged].sort((a, b) =>
      query.order === 'descending' ? b.id - a.id : a.id - b.id,
    ),
  )
})

export const scanSubmissions = Effect.fnUntraced(function* (
  state: State,
  input: Record.SubmissionQuery = {},
) {
  const query = yield* Schema.decodeEffect(Record.SubmissionQuerySchema)(input)
  const stored = yield* state.runtime.storage.scanSubmissions(query).pipe(Stream.runCollect)
  const staged = Array.from(state.submissions.values()).filter(
    (value) =>
      (query.conversationId === undefined || value.conversationId === query.conversationId) &&
      (query.status === undefined || value.status === query.status),
  )
  return yield* Value.copy(
    Schema.Array(Domain.Submission),
    [...stored.filter((value) => !state.submissions.has(value.id)), ...staged].sort((a, b) =>
      query.order === 'descending' ? b.id - a.id : a.id - b.id,
    ),
  )
})

export const submissionByRequest = Effect.fnUntraced(function* (
  state: State,
  conversationId: Domain.ConversationId,
  requestId: Identity.RequestId,
) {
  const staged = Array.from(state.submissions.values()).find(
    (value) => value.conversationId === conversationId && value.requestId === requestId,
  )
  if (staged !== undefined) return Option.some(yield* Value.copy(Domain.Submission, staged))
  return yield* state.runtime.storage.submissionByRequest(conversationId, requestId)
})

const requireConversation = Effect.fnUntraced(function* (state: State, id: Domain.ConversationId) {
  if (Option.isNone(yield* conversation(state, id)))
    return yield* error('notFound', 'conversation.access', 'Conversation is absent')
})

const liveTask = Effect.fnUntraced(function* (state: State, id: Domain.TaskId) {
  const found = yield* task(state, id)
  if (Option.isNone(found)) return yield* error('notFound', 'task.owner', 'Task is absent')
  if (
    found.value.abortRequested ||
    found.value.state.status === 'terminal' ||
    found.value.state.status === 'completing'
  )
    return yield* error('conflict', 'task.owner', 'Task cannot accept owned work')
  return found.value
})

const ownership = Effect.fnUntraced(function* (
  state: State,
  input: Transaction.ConversationOptions,
) {
  const options = yield* Schema.decodeEffect(Transaction.ConversationOptionsSchema)(input)
  if (options.ownership._tag === 'ownerless') return undefined
  const owner = yield* liveTask(state, options.ownership.taskId)
  return { conversationId: owner.conversationId, taskId: owner.id }
})

export const ensureRoot = Effect.fnUntraced(function* (state: State) {
  const found = yield* conversation(state, Domain.ROOT_CONVERSATION_ID)
  if (Option.isSome(found)) return found.value
  const value = Domain.Conversation.make({ id: Domain.ROOT_CONVERSATION_ID })
  yield* active(state)
  state.conversations.set(value.id, value)
  return yield* Value.copy(Domain.Conversation, value)
})

export const createConversation = Effect.fnUntraced(function* (
  state: State,
  options: Transaction.ConversationOptions,
) {
  const owner = yield* ownership(state, options)
  const value = yield* Value.copy(Domain.Conversation, {
    id: yield* state.runtime.storage.mintId<Domain.ConversationId>(),
    ...(owner === undefined ? {} : { owner }),
  })
  yield* active(state)
  state.conversations.set(value.id, value)
  return yield* Value.copy(Domain.Conversation, value)
})

export const appendEntry = Effect.fnUntraced(function* (
  state: State,
  conversationId: Domain.ConversationId,
  input: Transaction.EntryDraft,
) {
  yield* requireConversation(state, conversationId)
  const draft = yield* Value.copy(Transaction.EntryDraftSchema, input)
  const id = yield* state.runtime.storage.mintId<Domain.EntryId>()
  if (
    draft.head !== undefined &&
    draft.head !== 'self' &&
    Option.isNone(yield* entry(state, draft.head, { conversationId }))
  )
    return yield* error('notFound', 'entry.append', 'Context head is not visible')
  for (const edit of draft.edits ?? [])
    if (Option.isNone(yield* entry(state, edit.target, { conversationId })))
      return yield* error('notFound', 'entry.append', 'Context edit target is not visible')
  if (state.options.conversationId !== undefined && state.options.conversationId !== conversationId)
    return yield* error(
      'invalid',
      'entry.append',
      'Entry does not belong to the attributed conversation',
    )
  if (state.options.taskId !== undefined) {
    const author = yield* liveTask(state, state.options.taskId)
    if (author.conversationId !== conversationId)
      return yield* error(
        'invalid',
        'entry.append',
        'Attributed task belongs to another conversation',
      )
  }
  const value = yield* Value.copy(Domain.Entry, {
    ...Struct.omit(draft, ['head']),
    id,
    conversationId,
    ...(draft.head === undefined ? {} : { head: draft.head === 'self' ? id : draft.head }),
    ...(state.options.taskId === undefined ? {} : { byTaskId: state.options.taskId }),
  })
  yield* active(state)
  state.entries.set(id, value)
  return yield* Value.copy(Domain.Entry, value)
})

const validateTask = Effect.fnUntraced(function* (
  state: State,
  value: Domain.Task,
  creating = false,
) {
  yield* requireConversation(state, value.conversationId)
  if (value.owner !== undefined) {
    const found = yield* task(state, value.owner)
    if (Option.isNone(found)) return yield* error('notFound', 'task.write', 'Task owner is absent')
    const owner = creating ? yield* liveTask(state, value.owner) : found.value
    if (
      owner.conversationId !== value.conversationId ||
      value.background ||
      value.owner === value.id
    )
      return yield* error('invalid', 'task.write', 'Invalid task ownership')
  }
  for (const id of value.state.on ?? []) {
    if (id === value.id || Option.isNone(yield* task(state, id)))
      return yield* error('invalid', 'task.write', 'Task wait references an absent task or itself')
  }
})

export const createTask = Effect.fnUntraced(function* (
  state: State,
  input: Transaction.TaskCreate,
) {
  const value = yield* Value.copy(Domain.Task, {
    ...input,
    id: yield* state.runtime.storage.mintId<Domain.TaskId>(),
  })
  yield* validateTask(state, value, true)
  yield* active(state)
  state.tasks.set(value.id, value)
  return yield* Value.copy(Domain.Task, value)
})

const TaskIdentity = Domain.Task.mapFields(
  Struct.pick(['id', 'conversationId', 'kind', 'version', 'input', 'owner', 'background']),
)
const sameTaskIdentity = Schema.toEquivalence(TaskIdentity)
const sameTask = Schema.toEquivalence(Domain.Task)
export const putTask = Effect.fnUntraced(function* (state: State, input: Domain.Task) {
  const value = yield* Value.copy(Domain.Task, input)
  const previous = yield* task(state, value.id)
  if (Option.isNone(previous)) return yield* error('notFound', 'task.write', 'Task is absent')
  if (
    !sameTaskIdentity(previous.value, value) ||
    (previous.value.abortRequested && !value.abortRequested) ||
    (previous.value.state.status === 'terminal' && !sameTask(previous.value, value))
  )
    return yield* error(
      'conflict',
      'task.write',
      'Task identity and terminal receipts are immutable',
    )
  if (sameTask(previous.value, value)) return
  yield* validateTask(state, value)
  const retiring = value.state.status === 'terminal' ? yield* taskDocuments(state, value.id) : []
  yield* active(state)
  // Prepare every retirement before staging anything: callers may catch storage errors.
  for (const draft of retiring) {
    draft.retired = true
    if (!state.documents.has(draft.key)) state.documents.set(draft.key, draft)
    state.incarnations.set(draft.value.record.id, draft)
  }
  state.tasks.set(value.id, value)
})

const validateSubmission = Effect.fnUntraced(function* (state: State, value: Domain.Submission) {
  yield* requireConversation(state, value.conversationId)
  for (const id of [value.entry, value.answer])
    if (
      id !== undefined &&
      Option.isNone(yield* entry(state, id, { conversationId: value.conversationId }))
    )
      return yield* error('notFound', 'submission.write', 'Submission entry is not visible')
  if (value.requestId !== undefined) {
    const previous = yield* submissionByRequest(state, value.conversationId, value.requestId)
    if (Option.isSome(previous) && previous.value.id !== value.id)
      return yield* error('conflict', 'submission.write', 'Request is already admitted')
  }
})

export const createSubmission = Effect.fnUntraced(function* (
  state: State,
  input: Transaction.SubmissionCreate,
) {
  const value = yield* Value.copy(Domain.Submission, {
    ...input,
    id: yield* state.runtime.storage.mintId<Domain.SubmissionId>(),
  })
  yield* validateSubmission(state, value)
  yield* active(state)
  state.submissions.set(value.id, value)
  return yield* Value.copy(Domain.Submission, value)
})

const sameSubmission = Schema.toEquivalence(Domain.Submission)
export const putSubmission = Effect.fnUntraced(function* (state: State, input: Domain.Submission) {
  const value = yield* Value.copy(Domain.Submission, input)
  const previous = yield* submission(state, value.id)
  if (Option.isNone(previous))
    return yield* error('notFound', 'submission.write', 'Submission is absent')
  const current = previous.value
  if (sameSubmission(current, value)) return
  if (
    current.conversationId !== value.conversationId ||
    current.type !== value.type ||
    current.requestId !== value.requestId ||
    current.status === 'done' ||
    current.status === 'unanswered' ||
    (current.status === 'placed' && (value.status === 'queued' || value.status === 'placed')) ||
    (current.entry !== undefined && current.entry !== value.entry)
  )
    return yield* error(
      'conflict',
      'submission.write',
      'Invalid submission transition or changed identity',
    )
  yield* validateSubmission(state, value)
  yield* active(state)
  state.submissions.set(value.id, value)
})

const checkScope = Effect.fnUntraced(function* (state: State, scope: Domain.Scope) {
  if (scope._tag === 'conversation') yield* requireConversation(state, scope.conversationId)
  if (scope._tag === 'task') {
    const owner = yield* task(state, scope.taskId)
    if (Option.isNone(owner))
      return yield* error('notFound', 'document.scope', 'Document task is absent')
    if (owner.value.state.status === 'terminal')
      return yield* error('conflict', 'document.scope', 'Document task is terminal')
  }
})

const taskDocuments = Effect.fnUntraced(function* (state: State, taskId: Domain.TaskId) {
  const retiring = Array.from(state.incarnations.values()).filter(
    (draft) =>
      draft.value.record.scope._tag === 'task' && draft.value.record.scope.taskId === taskId,
  )
  const records = yield* state.runtime.storage
    .scanDocuments({ scope: { _tag: 'task', taskId } })
    .pipe(Stream.runCollect)
  for (const record of records) {
    if (state.incarnations.has(record.id)) continue
    const stored = yield* state.runtime.storage.document(record.id)
    if (Option.isNone(stored))
      return yield* error('invalid', 'task.write', 'Task document content is absent')
    retiring.push({
      key: yield* Value.key(record),
      original: stored,
      record: undefined,
      source: undefined,
      value: stored.value,
      changed: false,
      retired: false,
    })
  }
  return retiring
})

const load = Effect.fnUntraced(function* (state: State, address: Record.DocumentAddress) {
  const key = yield* Value.key(address)
  const previous = state.documents.get(key)
  if (previous !== undefined) return previous.retired ? Option.none<Draft>() : Option.some(previous)
  const record = yield* state.runtime.storage.findDocument(address)
  if (Option.isNone(record)) return Option.none<Draft>()
  const stored = yield* state.runtime.storage.document(record.value.id)
  if (Option.isNone(stored))
    return yield* error('invalid', 'document.load', 'Document content is absent')
  const draft: Draft = {
    key,
    original: stored,
    record: undefined,
    source: undefined,
    value: stored.value,
    changed: false,
    retired: false,
  }
  yield* active(state)
  state.documents.set(key, draft)
  state.incarnations.set(record.value.id, draft)
  return Option.some(draft)
})

export const snapshot = Effect.fnUntraced(function* <S extends Document.Codec>(
  state: State,
  document: Document.Document<S>,
  target: Document.Target,
) {
  const address = yield* Value.address(document, target)
  const draft = yield* load(state, address)
  if (Option.isNone(draft)) return Option.none<Document.Snapshot<S>>()
  return Option.some(yield* Value.snapshot(document, draft.value.value))
})

export const ensureDocument = Effect.fnUntraced(function* <S extends Document.Codec>(
  state: State,
  document: Document.Document<S>,
  target: Document.Target,
  options?: { readonly seed?: Schema.Json },
) {
  const address = yield* Value.address(document, target)
  yield* checkScope(state, address.scope)
  const found = yield* load(state, address)
  if (Option.isSome(found)) {
    const previous = found.value.value
    if (
      previous.version !== document.definition.version &&
      state.copiedSources.has(previous.record.id)
    )
      return yield* error(
        'conflict',
        'document.migrate',
        'A fork source cannot change in the copying transaction',
      )
    const migrated = yield* Value.migrate(document, previous)
    const result = yield* Value.snapshot(document, migrated)
    if (migrated.version !== previous.version) {
      yield* active(state)
      found.value.value = migrated
      found.value.changed = true
    }
    return result
  }
  const seed =
    options?.seed === undefined ? undefined : yield* Value.copy(Schema.Json, options.seed)
  const initial = yield* Effect.try({
    try: () => document.definition.initial(seed),
    catch: (cause) =>
      new SessionError({
        reason: 'invalid',
        operation: 'document.initialize',
        message: 'Document initializer failed',
        cause,
      }),
  })
  const value = yield* Value.encode(document, initial)
  const record: Domain.DocumentCreate = {
    ...address,
    id: yield* state.runtime.storage.mintId<Domain.DocumentId>(),
    ...(document.definition.scope === 'conversation'
      ? { history: document.definition.history, fork: document.definition.fork }
      : {}),
  }
  const stored = yield* Schema.decodeEffect(Record.StoredDocumentSchema)({
    record: { ...record, createdAt: Sequence.make(0) },
    version: document.definition.version,
    value,
    deltasSinceBase: 0,
  })
  const key = yield* Value.key(address)
  const draft: Draft = {
    key,
    original: Option.none(),
    record,
    source: undefined,
    value: stored,
    changed: true,
    retired: false,
  }
  yield* active(state)
  state.documents.set(key, draft)
  state.incarnations.set(record.id, draft)
  return yield* Value.snapshot(document, stored)
})

export const setDocument = Effect.fnUntraced(function* <S extends Document.Codec>(
  state: State,
  document: Document.Document<S>,
  target: Document.Target,
  value: S['Type'],
) {
  const address = yield* Value.address(document, target)
  yield* checkScope(state, address.scope)
  const found = yield* load(state, address)
  if (Option.isNone(found)) return yield* error('notFound', 'document.update', 'Document is absent')
  yield* Value.compatible(document, found.value.value)
  if (state.copiedSources.has(found.value.value.record.id))
    return yield* error(
      'conflict',
      'document.update',
      'A fork source cannot change in the copying transaction',
    )
  const encoded = yield* Value.encode(document, value)
  yield* active(state)
  found.value.value = { ...found.value.value, value: encoded, deltasSinceBase: 0 }
  found.value.changed = true
})

export const updateDocument = Effect.fnUntraced(function* <S extends Document.Codec>(
  state: State,
  document: Document.Document<S>,
  target: Document.Target,
  update: (value: S['Type']) => S['Type'],
) {
  const address = yield* Value.address(document, target)
  yield* checkScope(state, address.scope)
  const found = yield* load(state, address)
  if (Option.isNone(found)) return yield* error('notFound', 'document.update', 'Document is absent')
  if (state.copiedSources.has(found.value.value.record.id))
    return yield* error(
      'conflict',
      'document.update',
      'A fork source cannot change in the copying transaction',
    )
  const migrated = yield* Value.migrate(document, found.value.value)
  const current = yield* Value.snapshot(document, migrated)
  const value = yield* Effect.try({
    try: () => update(current.value),
    catch: (cause) =>
      new SessionError({
        reason: 'invalid',
        operation: 'document.update',
        message: 'Document replacement function failed',
        cause,
      }),
  })
  const encoded = yield* Value.encode(document, value)
  yield* Value.snapshot(document, { ...migrated, value: encoded })
  yield* active(state)
  found.value.value = { ...migrated, value: encoded, deltasSinceBase: 0 }
  found.value.changed = true
})

export const retireDocument = Effect.fnUntraced(function* <S extends Document.Codec>(
  state: State,
  document: Document.Document<S>,
  target: Document.Target,
) {
  const found = yield* load(state, yield* Value.address(document, target))
  if (Option.isNone(found)) return
  yield* Value.compatible(document, found.value.value)
  if (state.copiedSources.has(found.value.value.record.id))
    return yield* error(
      'conflict',
      'document.retire',
      'A fork source cannot retire in the copying transaction',
    )
  yield* active(state)
  found.value.retired = true
})

const sameScope = Schema.toEquivalence(Domain.Scope)
export const scanDocuments = Effect.fnUntraced(function* (
  state: State,
  input: Record.DocumentQuery,
) {
  const query = yield* Schema.decodeEffect(Record.DocumentQuerySchema)(input)
  const stored = yield* state.runtime.storage.scanDocuments(query).pipe(Stream.runCollect)
  if (query.at !== undefined && query.at !== 'current') return stored
  const staged = Array.from(state.incarnations.values())
    .filter(
      (draft) =>
        !draft.retired &&
        sameScope(draft.value.record.scope, query.scope) &&
        (query.kind === undefined || draft.value.record.kind === query.kind),
    )
    .map((draft) => draft.value.record)
  return yield* Value.copy(
    Schema.Array(Record.DocumentRecordSchema),
    [...stored.filter((record) => !state.incarnations.has(record.id)), ...staged].sort((a, b) =>
      query.order === 'descending' ? b.id - a.id : a.id - b.id,
    ),
  )
})

export const forkConversation = Effect.fnUntraced(function* (
  state: State,
  parent: Domain.ConversationId,
  at: Domain.EntryId,
  options: Transaction.ConversationOptions,
) {
  yield* requireConversation(state, parent)
  const visible = yield* entry(state, at, { conversationId: parent })
  const cutoff = yield* state.runtime.storage.entry(at)
  if (Option.isNone(visible) || Option.isNone(cutoff))
    return yield* error(
      'notFound',
      'conversation.fork',
      'Fork cutoff must be a committed visible entry',
    )
  const owner = yield* ownership(state, options)
  const id = yield* state.runtime.storage.mintId<Domain.ConversationId>()
  const historical = yield* state.runtime.storage
    .scanDocuments({
      scope: { _tag: 'conversation', conversationId: visible.value.conversationId },
      at: cutoff.value.commitSeq,
    })
    .pipe(Stream.runCollect)
  const current = yield* scanDocuments(state, {
    scope: { _tag: 'conversation', conversationId: parent },
  })
  const selected = new Map<
    string,
    { readonly record: Record.DocumentRecord; readonly at: Record.DocumentPoint }
  >()
  for (const record of historical)
    if (record.fork === 'asOf') {
      const key = yield* Value.key({
        ...record,
        scope: { _tag: 'conversation', conversationId: id },
      })
      selected.set(key, { record, at: cutoff.value.commitSeq })
    }
  for (const record of current)
    if (record.fork === 'current') {
      const key = yield* Value.key({
        ...record,
        scope: { _tag: 'conversation', conversationId: id },
      })
      if (selected.has(key))
        return yield* error('conflict', 'conversation.fork', 'Ambiguous document fork source')
      selected.set(key, { record, at: 'current' })
    }
  const copies: Array<{ readonly draft: Draft; readonly sourceId: Domain.DocumentId }> = []
  for (const [key, source] of selected) {
    const pending = state.incarnations.get(source.record.id)
    if (
      pending !== undefined &&
      (pending.changed || pending.retired || pending.record !== undefined)
    )
      return yield* error(
        'conflict',
        'conversation.fork',
        'Fork source changes in this transaction',
      )
    const stored = yield* state.runtime.storage.document(source.record.id, source.at)
    if (Option.isNone(stored))
      return yield* error('notFound', 'conversation.fork', 'Fork document is absent')
    const metadata = Struct.omit(source.record, ['createdAt', 'retiredAt'])
    const record = {
      ...metadata,
      id: yield* state.runtime.storage.mintId<Domain.DocumentId>(),
      scope: { _tag: 'conversation' as const, conversationId: id },
    }
    const value = yield* Schema.decodeEffect(Record.StoredDocumentSchema)({
      ...stored.value,
      deltasSinceBase: 0,
      record: { ...record, createdAt: Sequence.make(0) },
    })
    const draft: Draft = {
      key,
      original: Option.none(),
      record,
      source: { id: source.record.id, at: source.at },
      value,
      changed: false,
      retired: false,
    }
    copies.push({ draft, sourceId: source.record.id })
  }
  const value = yield* Value.copy(Domain.Conversation, {
    id,
    parent: { conversationId: parent, at },
    ...(owner === undefined ? {} : { owner }),
  })
  const result = yield* Value.copy(Domain.Conversation, value)
  yield* active(state)
  // Prepare the complete fork before changing the overlay: callers may catch an operation error.
  for (const { draft, sourceId } of copies) {
    state.documents.set(draft.key, draft)
    state.incarnations.set(draft.value.record.id, draft)
    state.copiedSources.add(sourceId)
  }
  state.conversations.set(id, value)
  return result
})

export const writes = Effect.fnUntraced(function* (state: State) {
  const result: Array<Record.StorageWrite> = []
  for (const value of state.conversations.values()) {
    if (value.owner !== undefined) yield* liveTask(state, value.owner.taskId)
    result.push({ _tag: 'conversation', value })
  }
  for (const value of state.entries.values()) result.push({ _tag: 'entry', value })
  for (const value of state.tasks.values()) result.push({ _tag: 'task', value })
  for (const value of state.submissions.values()) result.push({ _tag: 'submission', value })
  for (const draft of state.incarnations.values()) {
    if (draft.record !== undefined) {
      if (draft.retired) continue
      if (draft.source !== undefined && !draft.changed)
        result.push({ _tag: 'document.copy', record: draft.record, source: draft.source })
      else
        result.push({
          _tag: 'document.create',
          record: draft.record,
          content: { _tag: 'base', version: draft.value.version, value: draft.value.value },
        })
    } else if (draft.changed)
      result.push({
        _tag: 'document.change',
        id: draft.value.record.id,
        content: { _tag: 'base', version: draft.value.version, value: draft.value.value },
      })
    if (draft.retired) result.push({ _tag: 'document.retire', id: draft.value.record.id })
  }
  return yield* Value.copy(Schema.Array(Record.StorageWriteSchema), result)
})

/** Adopts values without another storage read after the atomic batch has succeeded. */
export const adopt = Effect.fnUntraced(function* (state: State, seq: Sequence.Sequence) {
  const documents = new Map<Domain.DocumentId, Record.StoredDocument | null>()
  for (const draft of state.incarnations.values()) {
    if (draft.record === undefined && !draft.changed && !draft.retired) continue
    if (draft.retired) {
      state.runtime.cache.delete(draft.key)
      if (draft.record === undefined) documents.set(draft.value.record.id, null)
    } else {
      const stored = yield* Schema.decodeEffect(Record.StoredDocumentSchema)({
        ...draft.value,
        record: {
          ...draft.value.record,
          createdAt: draft.record === undefined ? draft.value.record.createdAt : seq,
        },
      })
      state.runtime.cache.set(draft.key, Option.some(stored))
      documents.set(stored.record.id, stored)
    }
  }
  return documents
})
