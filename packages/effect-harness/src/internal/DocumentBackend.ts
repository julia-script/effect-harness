/** Version-tolerant raw mounts on Session's trusted adoption line. */
import type * as Cause from 'effect/Cause'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Queue from 'effect/Queue'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as Remote from '../RemoteDocument.js'
import * as Record from '../Record.js'
import * as Observation from '../Observation.js'
import type * as Session from '../Session.js'
import type * as Transaction from '../Transaction.js'
import { SessionError } from '../SessionError.js'
import * as Runtime from './SessionRuntime.js'
import * as NativeTransaction from './TransactionRuntime.js'
import * as Value from './DocumentValue.js'

export interface Mount {
  readonly publish: (
    documents: ReadonlyMap<Record.DocumentId, Record.StoredDocument | null>,
  ) => Effect.Effect<void>
  readonly close: (error?: Remote.Failure) => Effect.Effect<void>
}
export interface Attachment {
  readonly stream: Stream.Stream<Remote.WireFrame, Remote.Failure>
  readonly detach: Effect.Effect<void>
}
const invalid = (reason: SessionError['reason'], message: string) =>
  new SessionError({ reason, operation: 'document.remote', message })
export const selector = Effect.fnUntraced(function* (input: Remote.ReadRequest) {
  const request = yield* Value.copy(Remote.ReadRequestSchema, input)
  if (request.address.kind === 'harness.tool.nested.receipt')
    return yield* invalid(
      'invalid',
      'Private retained receipt bodies are not document transport values',
    )
  if (
    request.address.kind !== request.metadata.kind ||
    request.address.scope._tag !== request.metadata.scope ||
    request.family !== (request.address.key !== undefined)
  )
    return yield* invalid('invalid', 'Document selector does not match its definition')
  return request
})
const checked = Effect.fnUntraced(function* (
  request: Remote.ReadRequest,
  raw: Option.Option<Record.StoredDocument>,
) {
  if (Option.isNone(raw)) return raw
  const record = raw.value.record
  if (
    record.kind !== request.metadata.kind ||
    record.scope._tag !== request.metadata.scope ||
    (request.metadata.scope === 'conversation' &&
      (record.history !== request.metadata.history || record.fork !== request.metadata.fork))
  )
    return yield* invalid(
      'conflict',
      'Persisted document policies differ from the requested definition',
    )
  return Option.some(yield* Value.copy(Record.StoredDocumentSchema, raw.value))
})
export const capture = Effect.fnUntraced(function* (
  runtime: Runtime.State,
  input: Remote.ReadRequest,
  at?: Record.EntryId,
) {
  const request = yield* selector(input)
  if (at === undefined)
    return yield* checked(request, yield* Runtime.load(runtime, request.address))
  if (
    request.metadata.scope !== 'conversation' ||
    request.metadata.history !== 'rewindable' ||
    request.address.scope._tag !== 'conversation'
  )
    return yield* invalid('invalid', 'Historical reads require a rewindable conversation document')
  const entry = yield* runtime.storage.entry(at, {
    conversationId: request.address.scope.conversationId,
  })
  if (Option.isNone(entry)) return yield* invalid('notFound', 'Historical entry is not visible')
  const record = yield* runtime.storage.findDocument(
    {
      ...request.address,
      scope: { _tag: 'conversation', conversationId: entry.value.entry.conversationId },
    },
    entry.value.commitSeq,
  )
  if (Option.isNone(record)) return Option.none<Record.StoredDocument>()
  const stored = yield* runtime.storage.document(record.value.id, entry.value.commitSeq)
  if (Option.isNone(stored))
    return yield* invalid('invalid', 'Historical document has no stored content')
  return yield* checked(request, stored)
})
export const attach = Effect.fnUntraced(function* (
  runtime: Runtime.State,
  input: Remote.ReadRequest,
  scope: Scope.Scope,
  options?: Observation.WatchOptions,
): Effect.fn.Return<Attachment, Remote.Failure> {
  const request = yield* selector(input)
  const ownedOptions = yield* Value.copy(Observation.WatchOptionsSchema, options ?? {})
  const raw = yield* capture(runtime, request)
  if (Option.isNone(raw)) return yield* invalid('notFound', 'Document is absent')
  const queue = yield* Queue.dropping<Remote.WireFrame, Remote.Failure | Cause.Done>(
    ownedOptions.capacity ?? Observation.defaultCapacity,
  )
  const initial: Remote.WireFrame = {
    _tag: 'Snapshot',
    protocol: 1,
    target: request.address,
    incarnation: raw.value.record.id,
    epoch: globalThis.crypto.randomUUID(),
    revision: 0,
    value: raw.value,
  }
  const release = Runtime.pinDocument(
    runtime,
    yield* Value.key(request.address),
    initial.incarnation,
  )
  let latest: Remote.WireFrame = initial
  let revision = 0
  let taken = 0
  let active = true
  let rebasing = false
  const detach = Effect.gen(function* () {
    active = false
    release()
    runtime.documentMounts.delete(mount)
    yield* Queue.shutdown(queue)
  })
  const mount: Mount = {
    publish: (documents) =>
      Effect.gen(function* () {
        if (!active || !documents.has(initial.incarnation)) return
        const value = documents.get(initial.incarnation) ?? null
        const from = revision++
        const fields = {
          protocol: 1 as const,
          target: initial.target,
          incarnation: initial.incarnation,
          epoch: initial.epoch,
        }
        const frame: Remote.WireFrame =
          value === null
            ? { ...fields, _tag: 'Retired', from, to: revision }
            : { ...fields, _tag: 'Replace', from, to: revision, value }
        latest = frame
        if (rebasing || !(yield* Queue.offer(queue, frame))) {
          rebasing = true
          yield* Queue.clear(queue).pipe(Effect.catchCause(() => Effect.succeed([])))
          latest =
            value === null
              ? { ...fields, _tag: 'Retired', from: taken, to: taken + 1 }
              : { ...fields, _tag: 'Rebase', revision, value }
          yield* Queue.offer(queue, latest)
        }
        if (value === null) {
          active = false
          release()
          runtime.documentMounts.delete(mount)
          yield* Queue.end(queue)
        }
      }),
    close: (error) =>
      Effect.gen(function* () {
        active = false
        release()
        if (error === undefined) yield* Queue.end(queue)
        else yield* Queue.fail(queue, error)
      }),
  }
  runtime.documentMounts.add(mount)
  yield* Scope.addFinalizer(scope, detach)
  const pending = Stream.fromQueue(queue).pipe(
    Stream.tap((frame) =>
      Effect.sync(() => {
        rebasing = false
        taken = frame._tag === 'Snapshot' || frame._tag === 'Rebase' ? frame.revision : frame.to
      }),
    ),
  )
  return {
    stream: Stream.concat(Stream.succeed(initial), pending).pipe(
      Stream.mapEffect((frame) => Value.copy(Remote.WireFrameSchema, frame)),
    ),
    detach,
  }
})
export const inTransaction = <A, E>(
  session: Session.Session,
  tx: Transaction.Transaction,
  use: (runtime: Runtime.State) => Effect.Effect<A, E>,
) =>
  NativeTransaction.use(tx, (transaction) =>
    Effect.gen(function* () {
      const runtime = yield* Runtime.get(session)
      if (transaction.runtime !== runtime)
        return yield* invalid('invalid', 'Document read requires the matching native transaction')
      yield* Runtime.open(runtime)
      return yield* use(runtime)
    }),
  )
export const make = (session: Session.Session): Remote.Capabilities => ({
  snapshot: (request) =>
    Runtime.line(session, 'document.remote.snapshot', (runtime) => capture(runtime, request)),
  snapshotAsOf: (request) =>
    Runtime.line(session, 'document.remote.history', (runtime) =>
      capture(runtime, request, request.at),
    ),
  watch: (request, options) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const scope = yield* Scope.Scope
        return (yield* Runtime.line(session, 'document.remote.watch', (runtime) =>
          attach(runtime, request, scope, options),
        )).stream
      }),
    ),
})
