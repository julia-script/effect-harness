/** Shared invariant checks; migration is available only at the private scheduler boundary. */
import * as Schema from 'effect/Schema'
import * as Struct from 'effect/Struct'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Record from '../Record.js'
import * as TaskPolicy from '../TaskPolicy.js'
import * as Task from '../Task.js'
import { TaskError } from '../TaskError.js'

const Node = Record.Task.mapFields(
  Struct.pick([
    'id',
    'conversationId',
    'kind',
    'owner',
    'background',
    'restartPolicy',
    'schedulerFormat',
  ]),
)
const sameNode = Schema.toEquivalence(Node)
const sameTask = Schema.toEquivalence(Record.Task)
const sameJson = Schema.toEquivalence(Schema.Json)
const sameIntent = Schema.toEquivalence(TaskPolicy.AbortIntentSchema)

export const canReplace = (
  previous: Record.Task,
  next: Record.Task,
  migration = false,
): boolean => {
  if (!sameNode(previous, next)) return false
  if (previous.state.status === 'terminal') return sameTask(previous, next)
  if (previous.state.status === 'completing') {
    if (next.state.status !== 'completing' && next.state.status !== 'terminal') return false
    if (
      previous.state.outcome === undefined ||
      next.state.outcome === undefined ||
      !sameJson(previous.state.outcome, next.state.outcome)
    )
      return false
  }
  if (previous.version !== next.version || !sameJson(previous.input, next.input)) {
    if (
      !migration ||
      next.version <= previous.version ||
      previous.state.status === 'completing' ||
      next.state.status !== previous.state.status ||
      !sameJson(previous.state.on ?? [], next.state.on ?? []) ||
      previous.state.policy !== next.state.policy ||
      !sameJson(previous.memos ?? {}, next.memos ?? {})
    )
      return false
  }
  const before = TaskPolicy.abortIntent(previous)
  const after = TaskPolicy.abortIntent(next)
  if (before !== undefined) {
    if (after === undefined) return false
    if (before._tag === 'request' && !sameIntent(before, after)) return false
  }
  return true
}

/** Live scheduler token, held by one invocation Scope and revoked on the Session line. */
export interface Invocation {
  readonly taskId: Record.TaskId
  readonly conversationId: Record.ConversationId
  readonly kind: string
  readonly version: number
  readonly mode: 'run' | 'abort'
  readonly ended: boolean
}

/** Recheck inside the serialized commit before invoking a progress/memo/domain callback. */
export const gate = Effect.fnUntraced(function* (current: Record.Task, invocation: Invocation) {
  if (
    invocation.ended ||
    current.id !== invocation.taskId ||
    current.conversationId !== invocation.conversationId ||
    current.kind !== invocation.kind ||
    current.version !== invocation.version ||
    current.state.status !== 'running' ||
    (invocation.mode === 'run' ? current.abortRequested : !current.abortRequested)
  )
    return yield* new TaskError({
      reason: 'revoked',
      operation: 'task.invocation',
      message: 'Task invocation is no longer writable',
    })
})

/** Own-property lookup preserves null values and ignores Object.prototype names. */
export const memo = (current: Record.Task, key: string): Option.Option<Schema.Json> => {
  const memos = current.memos
  if (memos === undefined || !Object.hasOwn(memos, key)) return Option.none()
  return Option.fromUndefinedOr(memos[key])
}

/**
 * Prepare first-writer-wins memo data without staging. The scheduler must call gate first,
 * then putTask with this record in the same transaction. Decoding the winning value before
 * staging also makes caught schema failure leave earlier drafts untouched.
 */
export const memoize = Effect.fnUntraced(function* <S extends Schema.Constraint>(
  current: Record.Task,
  key: string,
  schema: S,
  value: S['Type'],
): Effect.fn.Return<
  { readonly record: Record.Task; readonly value: S['Type'] },
  Schema.SchemaError,
  S['EncodingServices'] | S['DecodingServices']
> {
  const previous = memo(current, key)
  if (Option.isSome(previous))
    return { record: current, value: yield* Task.decode(schema, previous.value) }
  const encoded = yield* Task.encode(schema, value)
  const decoded = yield* Task.decode(schema, encoded)
  return { record: { ...current, memos: { ...current.memos, [key]: encoded } }, value: decoded }
})
