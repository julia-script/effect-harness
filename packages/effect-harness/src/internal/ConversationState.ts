/** Built-in documents and native transcript projection. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Predicate from 'effect/Predicate'
import * as Stream from 'effect/Stream'
import * as AiError from 'effect/ai/AiError'
import * as Prompt from 'effect/ai/Prompt'
import * as Agent from '../Agent.ts'
import * as Document from '../Document.ts'
import * as Record from '../Record.ts'
import * as Serialization from '../Serialization.ts'
import * as Transcript from '../Transcript.ts'
import * as Usage from '../Usage.ts'
import { SystemPatch } from '../SystemPatch.ts'
import type * as Session from './Session.ts'

export const AgentDoc = Document.defineUnsafe({
  kind: 'harness.agent',
  version: 1,
  scope: 'conversation',
  history: 'rewindable',
  fork: 'asOf',
  schema: Serialization.object(Agent.State),
  initial: (): Agent.State => ({}),
  checkpointWhen: () => true,
})
const Queued = Schema.Struct({
  submissionId: Record.SubmissionId,
  model: Schema.Array(Schema.Json),
  mode: Schema.Literals(['steering', 'followUp']),
})
export const Inbox = Schema.Struct({
  queue: Schema.mutable(Schema.Array(Queued)),
  pending: Schema.mutable(Schema.Array(Record.SubmissionId)),
  active: Schema.optionalKey(Record.TaskId),
})
export const InboxDoc = Document.defineUnsafe({
  kind: 'harness.inbox',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'initial',
  schema: Serialization.object(Inbox),
  initial: (): typeof Inbox.Type => ({ queue: [], pending: [] }),
  checkpointWhen: () => true,
})
export const Progress = Schema.Struct({
  output: Schema.optionalKey(Schema.String),
  details: Schema.optionalKey(Schema.Json),
  diagnostics: Schema.optionalKey(Schema.Array(Schema.Json)),
  partial: Schema.optionalKey(Schema.Json),
})
export const ProgressDoc = Document.defineUnsafe({
  kind: 'harness.progress',
  version: 1,
  scope: 'task',
  schema: Serialization.object(Progress),
  initial: (): typeof Progress.Type => ({}),
})
export const Metadata = Schema.Struct({
  status: Schema.optionalKey(
    Schema.Literals(['stop', 'length', 'tool-calls', 'aborted', 'error', 'deferred']),
  ),
  usage: Schema.optionalKey(Usage.Usage),
  error: Schema.optionalKey(AiError.AiError),
  system: Schema.optionalKey(SystemPatch),
})
export const Data = Schema.Struct({
  harness: Metadata,
  message: Schema.optionalKey(Schema.String),
})
export const encodeMessages = (messages: ReadonlyArray<Prompt.Message>) =>
  Schema.encodeEffect(Schema.toCodecJson(Schema.Array(Prompt.Message)))(messages).pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(Schema.Json))),
  )
export const projectEntry = Effect.fnUntraced(function* (entry: Record.Entry) {
  const metadata =
    Predicate.isObject(entry.data) && Object.hasOwn(entry.data, 'harness')
      ? yield* Schema.decodeUnknownEffect(Schema.toCodecJson(Metadata))(entry.data.harness)
      : {}
  const messages = yield* Schema.decodeEffect(Schema.toCodecJson(Schema.Array(Prompt.Message)))(
    entry.model ?? [],
  )
  const edits = yield* Effect.forEach(
    entry.edits ?? [],
    (edit): Effect.Effect<Transcript.Edit, Schema.SchemaError> =>
      edit._tag === 'omit'
        ? Effect.succeed(edit)
        : Schema.decodeEffect(Schema.toCodecJson(Schema.Array(Prompt.Message)))(edit.messages).pipe(
            Effect.map((messages) => ({ ...edit, messages })),
          ),
  )
  return {
    ...metadata,
    id: entry.id,
    kind: entry.kind,
    messages,
    edits,
    ...(entry.head === undefined ? {} : { head: entry.head }),
  } satisfies Transcript.Entry
})
export const context = Effect.fnUntraced(function* (
  session: Session.Session.Service,
  conversationId: Record.ConversationId,
  at?: Record.EntryId,
) {
  const entries = yield* Stream.runCollect(
    session.scanEntries({ conversationId, ...(at === undefined ? {} : { maxEntryId: at }) }),
  )
  return Transcript.derive(
    yield* Effect.forEach(
      [...entries].sort((a, b) => a.id - b.id),
      projectEntry,
    ),
    at,
  )
})

/** Place selected input at a turn boundary without losing unselected input. */
export const place = Effect.fnUntraced(function* (
  tx: Session.Transaction,
  conversationId: Record.ConversationId,
  mode: 'all' | 'steering',
  one: boolean,
) {
  const inbox = yield* tx.doc(InboxDoc, { owner: conversationId })
  const candidates = inbox.queue.filter((item) => mode === 'all' || item.mode === 'steering')
  const selected = one ? candidates.slice(0, 1) : candidates
  for (const item of selected) {
    const entry = yield* tx.appendEntry(conversationId, { kind: 'harness.user', model: item.model })
    yield* tx.placeSubmission(item.submissionId, entry.id)
    inbox.pending.push(item.submissionId)
  }
  const selectedIds = new Set(selected.map((item) => item.submissionId))
  inbox.queue = inbox.queue.filter((item) => !selectedIds.has(item.submissionId))
  return selected.length
})
