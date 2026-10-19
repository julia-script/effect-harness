/** Immutable request documents belong to the run, including while its tool children execute. */
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as Struct from 'effect/Struct'
import * as Document from '../Document.js'
import * as Prepared from '../PreparedRequest.js'
import * as Record from '../Record.js'
import * as Session from '../Session.js'
import * as Transaction from '../Transaction.js'
import { SessionError } from '../SessionError.js'
import * as Plan from './ContextPlan.js'
import * as SessionRuntime from './SessionRuntime.js'
import * as Value from './DocumentValue.js'

export const ReferenceSchema = Schema.Struct({
  ownerTaskId: Record.TaskId,
  requestId: Prepared.SnapshotSchema.fields.requestId,
  ordinal: Schema.Natural,
  execution: Prepared.SnapshotSchema.fields.execution,
})
export type Reference = typeof ReferenceSchema.Type
export const reference = (ownerTaskId: Record.TaskId, value: Prepared.Snapshot): Reference => ({
  ownerTaskId,
  requestId: value.requestId,
  ordinal: value.ordinal,
  ...(value.execution === undefined ? {} : { execution: value.execution }),
})
export const SnapshotSchema = Prepared.SnapshotSchema.mapFields((fields) => ({
  ...fields,
  translation: Plan.TranslationSchema.mapFields(Struct.omit(['prompt'])),
}))
export const document = Document.family({
  kind: 'harness.request',
  version: 1,
  scope: 'task',
  schema: Schema.Struct({ request: Schema.optionalKey(Schema.toCodecJson(SnapshotSchema)) }),
  initial: () => ({}),
})
export const target = (ref: Reference): Document.Target => ({
  scope: { _tag: 'task', taskId: ref.ownerTaskId },
  key: ref.requestId,
})
const invalid = () =>
  new SessionError({
    reason: 'invalid',
    operation: 'request.read',
    message: 'The immutable prepared request is absent or inconsistent',
  })
export const restore = (value: typeof SnapshotSchema.Type): Prepared.Snapshot => ({
  ...value,
  translation: { ...value.translation, prompt: value.prompt },
})
const load = Effect.fnUntraced(function* (
  ref: Reference,
  snapshot: Effect.Effect<
    Option.Option<Document.Snapshot<typeof document.definition.schema>>,
    import('../TaskError.js').Failure
  >,
) {
  const saved = yield* snapshot
  if (Option.isNone(saved) || saved.value.value.request === undefined) return yield* invalid()
  const request = saved.value.value.request
  if (
    request.requestId !== ref.requestId ||
    request.ordinal !== ref.ordinal ||
    request.execution !== ref.execution
  )
    return yield* invalid()
  return restore(request)
})
export const read = (tx: Transaction.Transaction, ref: Reference) =>
  load(ref, Transaction.snapshot(tx, document, target(ref)))
export const get = (session: Session.Session, ref: Reference) =>
  load(ref, Session.snapshot(session, document, target(ref)))
export const save = Effect.fnUntraced(function* (
  tx: Transaction.Transaction,
  owner: Record.TaskId,
  value: Prepared.Snapshot,
) {
  if (!Schema.toEquivalence(Prompt.Prompt)(value.prompt, value.translation.prompt))
    return yield* invalid()
  const ref = reference(owner, value)
  const saved = yield* Transaction.snapshot(tx, document, target(ref))
  const request = { ...value, translation: Struct.omit(value.translation, ['prompt']) }
  if (Option.isSome(saved) && saved.value.value.request !== undefined) {
    if (!Schema.toEquivalence(SnapshotSchema)(saved.value.value.request, request))
      return yield* invalid()
    return ref
  }
  yield* Transaction.ensureDocument(tx, document, target(ref))
  yield* Transaction.setDocument(tx, document, target(ref), { request })
  return ref
})
export const retire = (tx: Transaction.Transaction, ref: Reference) =>
  Transaction.retireDocument(tx, document, target(ref))

/** Scheduler inspection already owns the Session line and must not reacquire it. */
export const inspect = (session: Session.Session, ref: Reference) =>
  load(
    ref,
    Effect.gen(function* () {
      const state = yield* SessionRuntime.get(session)
      const stored = yield* SessionRuntime.load(state, yield* Value.address(document, target(ref)))
      if (Option.isNone(stored)) return Option.none()
      return Option.some(yield* Value.snapshot(document, stored.value))
    }),
  )
