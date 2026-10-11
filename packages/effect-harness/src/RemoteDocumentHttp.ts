/** Scoped HTTP document transport. It does not replace non-document backend delegates. */
import * as Cause from 'effect/Cause'
import * as Clock from 'effect/Clock'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as Option from 'effect/Option'
import * as Queue from 'effect/Queue'
import * as Schema from 'effect/Schema'
import * as SchemaIssue from 'effect/SchemaIssue'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as HttpClient from 'effect/http/HttpClient'
import * as ClientRequest from 'effect/http/HttpClientRequest'
import * as ServerRequest from 'effect/http/HttpServerRequest'
import * as ServerResponse from 'effect/http/HttpServerResponse'
import * as Remote from './RemoteDocument.js'
import * as Record from './Record.js'
import * as Observation from './Observation.js'
import type { HarnessBackendService } from './HarnessBackend.js'
import { HarnessError } from './HarnessError.js'
import { SessionError } from './SessionError.js'
import { StorageError } from './StorageError.js'
import { RemoteDocumentError } from './RemoteDocumentError.js'
import * as Lifetime from './internal/RemoteDocumentLifetime.js'
import { observation } from './internal/ConversationInvocation.js'

const positive = Schema.Int.check(
  Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }),
)
export const ResourceOptionsSchema = Schema.Struct({
  maxFrameBytes: positive,
  heartbeatIntervalMs: positive,
  leaseTimeoutMs: positive,
})
export type ResourceOptions = typeof ResourceOptionsSchema.Type
export interface ClientOptions extends ResourceOptions {
  readonly endpoint: string
  readonly identity: string
}
export interface HandlerOptions extends ResourceOptions {
  readonly identity: string
}
const failure = (reason: RemoteDocumentError['reason'], message: string) =>
  new RemoteDocumentError({ reason, operation: 'remoteDocument.http', message })
const validate = (options: ResourceOptions) =>
  Schema.decodeEffect(ResourceOptionsSchema)(options).pipe(
    Effect.mapError(() => failure('invalid', 'Invalid transport resource limits')),
    Effect.filterOrFail(
      (value) => value.leaseTimeoutMs > value.heartbeatIntervalMs,
      () => failure('invalid', 'Lease timeout must exceed heartbeat interval'),
    ),
  )
const SafeFailureSchema = Schema.Union([
  Schema.Struct({ tag: Schema.Literal('SchemaError'), message: Schema.String }),
  Schema.Struct({
    tag: Schema.Literal('RemoteDocumentError'),
    reason: RemoteDocumentError.fields.reason,
    operation: Schema.String,
    message: Schema.String,
  }),
  Schema.Struct({
    tag: Schema.Literal('SessionError'),
    reason: SessionError.fields.reason,
    operation: Schema.String,
    message: Schema.String,
  }),
  Schema.Struct({
    tag: Schema.Literal('StorageError'),
    reason: StorageError.fields.reason,
    operation: Schema.String,
    message: Schema.String,
  }),
  Schema.Struct({
    tag: Schema.Literal('HarnessError'),
    reason: HarnessError.fields.reason,
    operation: Schema.String,
    message: Schema.String,
  }),
])
const ReplySchema = Schema.Union([
  Schema.TaggedStruct('Ready', {
    protocol: Schema.Literal(1),
    operationId: Schema.NonEmptyString,
    expiresInMs: positive,
  }),
  Schema.TaggedStruct('Read', {
    protocol: Schema.Literal(1),
    target: Record.DocumentAddressSchema,
    at: Schema.optionalKey(Record.EntryId),
    value: Schema.NullOr(Record.StoredDocumentSchema),
  }),
  Schema.TaggedStruct('Attached', { protocol: Schema.Literal(1) }),
  Schema.TaggedStruct('Authorized', { protocol: Schema.Literal(1) }),
  Schema.TaggedStruct('Completed', { protocol: Schema.Literal(1) }),
  Schema.TaggedStruct('AlreadyCompleted', { protocol: Schema.Literal(1) }),
  Schema.TaggedStruct('Cancelled', { protocol: Schema.Literal(1) }),
  Schema.TaggedStruct('Heartbeat', { protocol: Schema.Literal(1) }),
  Schema.TaggedStruct('Failure', { protocol: Schema.Literal(1), error: SafeFailureSchema }),
])
type Reply = typeof ReplySchema.Type
const ControlSchema = Schema.Union([
  Schema.TaggedStruct('Revoked', { protocol: Schema.Literal(1) }),
  Schema.TaggedStruct('Closed', { protocol: Schema.Literal(1) }),
  Schema.TaggedStruct('Expired', { protocol: Schema.Literal(1) }),
  Schema.TaggedStruct('Data', { protocol: Schema.Literal(1), frame: Remote.WireFrameSchema }),
  Schema.TaggedStruct('Failure', { protocol: Schema.Literal(1), error: SafeFailureSchema }),
])
type Control = typeof ControlSchema.Type
const controlFailure = (message: Exclude<Control, { readonly _tag: 'Data' }>) => {
  if (message._tag === 'Failure') return deserialize(message.error)
  if (message._tag === 'Expired') return failure('expired', 'Remote operation expired')
  if (message._tag === 'Closed') return failure('closed', 'Remote operation closed')
  return failure('revoked', 'Remote operation revoked')
}
const BeginSchema = Schema.Struct({
  protocol: Schema.Literal(1),
  kind: Schema.Literals(['Current', 'Historical', 'Watch']),
})
const SelectSchema = Schema.Struct({
  protocol: Schema.Literal(1),
  request: Remote.ReadRequestSchema,
  at: Schema.optionalKey(Record.EntryId),
  options: Schema.optionalKey(Observation.WatchOptionsSchema),
})
const ActionSchema = Schema.Struct({ protocol: Schema.Literal(1) })
const AttachSchema = Schema.Struct({
  protocol: Schema.Literal(1),
  options: Schema.optionalKey(Observation.WatchOptionsSchema),
})
const FrameRequestSchema = Schema.Struct({
  protocol: Schema.Literal(1),
  epoch: Schema.NonEmptyString,
  revision: Observation.CounterSchema,
})
const deserialize = (error: typeof SafeFailureSchema.Type): Remote.Failure => {
  switch (error.tag) {
    case 'SchemaError':
      return new Schema.SchemaError(new SchemaIssue.InvalidValue({ message: error.message }))
    case 'SessionError':
      return new SessionError(error)
    case 'StorageError':
      return new StorageError(error)
    case 'HarnessError':
      return new HarnessError(error)
    case 'RemoteDocumentError':
      return new RemoteDocumentError(error)
  }
}
const serialize = (error: Remote.Failure): typeof SafeFailureSchema.Type => {
  switch (error._tag) {
    case 'SchemaError':
      return { tag: 'SchemaError', message: 'Native document validation failed' }
    case 'RemoteDocumentError':
      return {
        tag: error._tag,
        reason: error.reason,
        operation: error.operation,
        message: error.message,
      }
    case 'SessionError':
      return {
        tag: error._tag,
        reason: error.reason,
        operation: error.operation,
        message: error.message,
      }
    case 'StorageError':
      return {
        tag: error._tag,
        reason: error.reason,
        operation: error.operation,
        message: error.message,
      }
    case 'HarnessError':
      return {
        tag: error._tag,
        reason: error.reason,
        operation: error.operation,
        message: error.message,
      }
  }
}
const encoder = new TextEncoder()
/** Byte accounting precedes text and schema decoding, including for control messages. */
const lines = <E>(
  source: Stream.Stream<Uint8Array, E>,
  limit: number,
): Stream.Stream<string, E | RemoteDocumentError> =>
  Stream.unwrap(
    Effect.sync(() => {
      let pieces: Uint8Array[] = []
      let size = 0
      const chunks = source.pipe(
        Stream.mapEffect((chunk) =>
          Effect.gen(function* () {
            const output: string[] = []
            let start = 0
            for (let i = 0; i <= chunk.byteLength; i++) {
              if (i !== chunk.byteLength && chunk[i] !== 10) continue
              const part = chunk.subarray(start, i)
              if (size + part.byteLength > limit)
                return yield* failure('oversize', 'Encoded transport frame exceeds maxFrameBytes')
              size += part.byteLength
              pieces.push(part)
              if (i !== chunk.byteLength) {
                const owned = new Uint8Array(size)
                let offset = 0
                for (const piece of pieces) {
                  owned.set(piece, offset)
                  offset += piece.byteLength
                }
                const text = yield* Effect.try({
                  try: () => new TextDecoder('utf-8', { fatal: true }).decode(owned),
                  catch: () => failure('protocol', 'Malformed UTF-8 frame'),
                })
                if (text.length === 0) return yield* failure('protocol', 'Empty transport frame')
                output.push(text)
                pieces = []
                size = 0
              }
              start = i + 1
            }
            return output
          }),
        ),
        Stream.flatMap(Stream.fromIterable),
      )
      return Stream.concat(
        chunks,
        Stream.fromEffect(
          Effect.suspend(() =>
            size === 0
              ? Effect.succeed([] as string[])
              : Effect.fail(failure('protocol', 'Truncated transport frame')),
          ),
        ).pipe(Stream.flatMap(Stream.fromIterable)),
      )
    }),
  )
const decode = <S extends Schema.Constraint>(schema: S, text: string) =>
  Schema.decodeEffect(Schema.fromJsonString(schema))(text).pipe(
    Effect.mapError(() => failure('protocol', 'Invalid transport message')),
  )
const encode = <S extends Schema.Constraint>(schema: S, value: S['Type'], limit: number) =>
  Schema.encodeEffect(Schema.fromJsonString(schema))(value).pipe(
    Effect.mapError(() => failure('protocol', 'Cannot encode transport message')),
    Effect.flatMap((text) =>
      encoder.encode(text).byteLength > limit
        ? Effect.fail(failure('oversize', 'Encoded transport frame exceeds maxFrameBytes'))
        : Effect.succeed(text + '\n'),
    ),
  )
const one = <E>(source: Stream.Stream<Uint8Array, E>, limit: number) =>
  lines(source, limit).pipe(
    Stream.take(2),
    Stream.runCollect,
    Effect.flatMap((messages) =>
      messages.length === 1
        ? Effect.succeed(messages[0]!)
        : Effect.fail(failure('protocol', 'Expected one transport message')),
    ),
  )
const tokenHeader = 'x-document-operation'
const subscriptionHeader = 'x-document-subscription'
const identityHeader = 'x-document-identity'
interface ClientOperation {
  readonly token: string
  readonly id: string
}
const CurrentOperation = Context.Reference<ClientOperation | undefined>(
  'effect-harness/RemoteDocumentOperation',
  { defaultValue: () => undefined },
)
interface ClientMount {
  readonly token: string
  readonly id: string
}

/** Captures a client and endpoint once; every materialization keeps caller codec services. */
export const make = Effect.fnUntraced(function* (
  baseBackend: HarnessBackendService,
  options: ClientOptions,
): Effect.fn.Return<
  HarnessBackendService,
  RemoteDocumentError,
  HttpClient.HttpClient | Scope.Scope
> {
  const resources = yield* validate(options)
  const identity = options.identity
  const endpoint = options.endpoint.replace(/\/$/, '')
  if (identity.length === 0 || !/^https?:\/\//.test(endpoint))
    return yield* failure('invalid', 'An HTTP endpoint and transport identity are required')
  const client = yield* HttpClient.HttpClient
  const owner = yield* Scope.Scope
  let closed = false
  yield* Scope.addFinalizer(
    owner,
    Effect.sync(() => {
      closed = true
    }),
  )
  const frames = new WeakMap<Remote.WireFrame, ClientMount>()
  const call = <S extends Schema.Constraint>(
    route: string,
    schema: S,
    body: S['Type'],
    token?: string,
    mount?: string,
  ) =>
    Effect.gen(function* () {
      if (closed) return yield* failure('closed', 'Transport scope is closed')
      const text = yield* encode(schema, body, resources.maxFrameBytes)
      let request = ClientRequest.post(`${endpoint}/documents/v1/${route}`).pipe(
        ClientRequest.setHeader(identityHeader, identity),
        ClientRequest.bodyText(text, 'application/x-ndjson'),
      )
      if (token !== undefined) request = ClientRequest.setHeader(request, tokenHeader, token)
      if (mount !== undefined) request = ClientRequest.setHeader(request, subscriptionHeader, mount)
      const response = yield* client
        .execute(request)
        .pipe(Effect.mapError(() => failure('transport', 'Document HTTP request failed')))
      if (response.status === 413)
        return yield* failure('oversize', 'Encoded transport response exceeds maxFrameBytes')
      const reply = yield* one(
        response.stream.pipe(
          Stream.mapError(() => failure('transport', 'Document HTTP response failed')),
        ),
        resources.maxFrameBytes,
      ).pipe(Effect.flatMap((text) => decode(ReplySchema, text)))
      if (reply._tag === 'Failure') return yield* deserialize(reply.error)
      return { reply, headers: response.headers }
    })
  const operation = Effect.gen(function* () {
    const current = yield* CurrentOperation
    if (current === undefined)
      return yield* failure('invalid', 'Materialization requires an authentic transport operation')
    return current
  })
  const control = (op: ClientOperation, mount?: ClientMount) =>
    Stream.unwrap(
      Effect.gen(function* () {
        let request = ClientRequest.get(`${endpoint}/documents/v1/events`).pipe(
          ClientRequest.setHeader(identityHeader, identity),
          ClientRequest.setHeader(tokenHeader, op.token),
        )
        if (mount !== undefined)
          request = ClientRequest.setHeader(request, subscriptionHeader, mount.token)
        const response = yield* client
          .execute(request)
          .pipe(Effect.mapError(() => failure('transport', 'Document stream could not open')))
        return lines(
          response.stream.pipe(
            Stream.mapError(() => failure('transport', 'Document stream disconnected')),
          ),
          resources.maxFrameBytes,
        ).pipe(Stream.mapEffect((text) => decode(ControlSchema, text)))
      }),
    )
  const observe: Lifetime.Lifetime['observe'] = (kind, run, frame) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const mount = frame === undefined ? undefined : frames.get(frame)
        if (kind === 'Frame' && (frame === undefined || mount === undefined))
          return yield* failure('invalid', 'Frame has no authentic transport mount')
        const opened =
          kind === 'Frame' && frame !== undefined && mount !== undefined
            ? yield* restore(
                call(
                  'frame',
                  FrameRequestSchema,
                  {
                    protocol: 1,
                    epoch: frame.epoch,
                    revision:
                      frame._tag === 'Snapshot' || frame._tag === 'Rebase'
                        ? frame.revision
                        : frame.to,
                  },
                  undefined,
                  mount.token,
                ),
              )
            : yield* restore(
                call('begin', BeginSchema, {
                  protocol: 1,
                  kind: kind === 'Frame' ? 'Current' : kind,
                }),
              )
        const token = opened.headers[tokenHeader]
        if (opened.reply._tag !== 'Ready' || token === undefined)
          return yield* failure('protocol', 'Missing operation handshake')
        const op = { token, id: opened.reply.operationId }
        return yield* Effect.scoped(
          Effect.gen(function* () {
            const heartbeats = yield* Effect.forever(
              Effect.sleep(resources.heartbeatIntervalMs).pipe(
                Effect.andThen(call('heartbeat', ActionSchema, { protocol: 1 }, op.token)),
              ),
            ).pipe(Effect.forkScoped)
            const revoked = control(op).pipe(
              Stream.runForEach((message) =>
                Effect.fail(
                  message._tag === 'Data'
                    ? failure('protocol', 'Unexpected operation data')
                    : controlFailure(message),
                ),
              ),
              Effect.andThen(Effect.fail(failure('closed', 'Operation control stream ended'))),
            )
            const work = Effect.scoped(Effect.suspend(run)).pipe(
              Effect.provideService(CurrentOperation, op),
            )
            const worker = yield* restore(work).pipe(Effect.forkChild({ startImmediately: true }))
            const winner = yield* restore(
              Effect.raceFirst(
                Fiber.await(worker),
                Effect.raceFirst(revoked, Fiber.join(heartbeats)),
              ),
            ).pipe(Effect.exit)
            let result: Exit.Exit<
              Effect.Success<ReturnType<typeof run>>,
              Effect.Error<ReturnType<typeof run>> | Remote.Failure
            >
            if (Exit.isSuccess(winner)) result = winner.value
            else {
              yield* Fiber.interrupt(worker)
              const joined = yield* Fiber.await(worker)
              result = Exit.failCause(
                Cause.fromReasons<Effect.Error<ReturnType<typeof run>> | Remote.Failure>([
                  ...winner.cause.reasons,
                  ...(Exit.isFailure(joined) ? joined.cause.reasons : []),
                ]),
              )
            }
            // Joined callback cleanup, including its local finalizer Cause, precedes ACK.
            const acknowledgement = yield* call(
              Exit.isSuccess(result) ? 'complete' : 'cancel',
              ActionSchema,
              { protocol: 1 },
              op.token,
            ).pipe(Effect.exit)
            if (Exit.isFailure(result)) return yield* Effect.failCause(result.cause)
            if (Exit.isFailure(acknowledgement))
              return yield* Effect.failCause(acknowledgement.cause)
            if (
              acknowledgement.value.reply._tag !== 'Completed' &&
              acknowledgement.value.reply._tag !== 'AlreadyCompleted'
            )
              return yield* failure('protocol', 'Missing successful completion acknowledgement')
            return result.value
          }),
        )
      }),
    )
  const read = (request: Remote.ReadRequest, at?: Record.EntryId) =>
    Effect.gen(function* () {
      const op = yield* operation
      const result = yield* call(
        'select',
        SelectSchema,
        { protocol: 1, request, ...(at === undefined ? {} : { at }) },
        op.token,
      )
      const reply = result.reply
      if (
        reply._tag !== 'Read' ||
        !Schema.toEquivalence(Record.DocumentAddressSchema)(reply.target, request.address) ||
        reply.at !== at
      )
        return yield* failure('protocol', 'Document reply selector does not match')
      if (
        reply.value !== null &&
        (reply.value.record.kind !== request.address.kind ||
          reply.value.record.key !== request.address.key ||
          (at === undefined &&
            !Schema.toEquivalence(Record.DocumentAddressSchema)(
              reply.value.record,
              request.address,
            )))
      )
        return yield* failure(
          'protocol',
          'Document record provenance does not match the read selector',
        )
      return reply.value === null ? Option.none<Record.StoredDocument>() : Option.some(reply.value)
    })
  const backend: HarnessBackendService = {
    ...baseBackend,
    documents: {
      snapshot: (request) => read(request),
      snapshotAsOf: (request) => read(request, request.at),
      watch: (request, options) =>
        Stream.unwrap(
          Effect.gen(function* () {
            const op = yield* operation
            const authorized = yield* call(
              'select',
              SelectSchema,
              { protocol: 1, request },
              op.token,
            )
            if (authorized.reply._tag !== 'Authorized')
              return yield* failure('protocol', 'Missing target authorization')
            const result = yield* call(
              'attach',
              AttachSchema,
              { protocol: 1, ...(options === undefined ? {} : { options }) },
              op.token,
            )
            const token = result.headers[subscriptionHeader]
            if (result.reply._tag !== 'Attached' || token === undefined)
              return yield* failure('protocol', 'Missing watch mount')
            const mount = { token, id: op.id }
            yield* Effect.addFinalizer(() =>
              call('cancel', ActionSchema, { protocol: 1 }, undefined, mount.token).pipe(
                Effect.ignore,
              ),
            )
            yield* Effect.forever(
              Effect.sleep(resources.heartbeatIntervalMs).pipe(
                Effect.andThen(
                  call('heartbeat', ActionSchema, { protocol: 1 }, undefined, mount.token),
                ),
              ),
            ).pipe(Effect.forkScoped)
            return control(op, mount).pipe(
              Stream.mapEffect((message) => {
                if (message._tag !== 'Data') return Effect.fail(controlFailure(message))
                if (
                  !Schema.toEquivalence(Record.DocumentAddressSchema)(
                    message.frame.target,
                    request.address,
                  )
                )
                  return Effect.fail(failure('protocol', 'Foreign document frame'))
                if (
                  message.frame._tag !== 'Retired' &&
                  (message.frame.incarnation !== message.frame.value.record.id ||
                    !Schema.toEquivalence(Record.DocumentAddressSchema)(
                      message.frame.value.record,
                      request.address,
                    ))
                )
                  return Effect.fail(
                    failure('protocol', 'Document frame provenance does not match its mount'),
                  )
                frames.set(message.frame, mount)
                return Effect.succeed(message.frame)
              }),
            )
          }),
        ),
    },
  }
  Lifetime.register(backend, { observe })
  return backend
})

interface ServerOperation {
  readonly token: string
  readonly id: string
  readonly kind: Lifetime.Kind
  readonly ready: Deferred.Deferred<void, Remote.Failure>
  readonly completion: Deferred.Deferred<void>
  readonly control: Queue.Queue<Control, Cause.Done>
  controlOpened: boolean
  heartbeat: number
  selected: boolean
  completed: boolean
  ended: 'revoked' | 'closed' | 'expired' | undefined
  delivery?: Deferred.Deferred<void>
  mount?: ServerMount
  request?: Remote.ReadRequest | undefined
}
interface ServerMount {
  readonly token: string
  readonly operationToken: string
  readonly operation: ServerOperation
  readonly scope: Scope.Closeable
  readonly stream: Stream.Stream<Remote.WireFrame, Remote.Failure>
  readonly frames: Map<
    number,
    { readonly frame: Remote.WireFrame; readonly done: Deferred.Deferred<void> }
  >
  readonly terminal: Deferred.Deferred<void, RemoteDocumentError>
  heartbeat: number
  opened: boolean
  ended: 'closed' | 'expired' | undefined
}
/** Creates a handler for one captured authorized backend; credentials never become DTO authority. */
export const makeHandler = Effect.fnUntraced(function* (
  backend: HarnessBackendService,
  options: HandlerOptions,
): Effect.fn.Return<
  Effect.Effect<ServerResponse.HttpServerResponse, never, ServerRequest.HttpServerRequest>,
  RemoteDocumentError,
  Scope.Scope
> {
  yield* Lifetime.validateBackend(backend)
  const resources = yield* validate(options)
  const identity = options.identity
  if (identity.length === 0) return yield* failure('invalid', 'Transport identity is required')
  const scope = yield* Scope.Scope
  // Terminate leases before closing the fibers that hold their completion barriers.
  const resourceScope = yield* Scope.fork(scope)
  const operations = new Map<string, ServerOperation>()
  const mounts = new Map<string, ServerMount>()
  let closed = false
  const complete = (op: ServerOperation) =>
    Effect.gen(function* () {
      yield* Deferred.succeed(op.completion, undefined)
      if (op.delivery !== undefined) yield* Deferred.succeed(op.delivery, undefined)
    })
  const end = (op: ServerOperation, reason: 'closed' | 'expired') =>
    Effect.gen(function* () {
      if (op.completed || op.ended === 'closed' || op.ended === 'expired') return
      op.ended = reason
      yield* Queue.offer(op.control, {
        _tag: reason === 'expired' ? 'Expired' : 'Closed',
        protocol: 1,
      })
      yield* Queue.end(op.control)
      yield* complete(op)
      if (op.mount !== undefined) yield* closeMount(op.mount, reason)
      yield* Effect.sleep(resources.leaseTimeoutMs).pipe(
        Effect.andThen(Effect.sync(() => operations.delete(op.token))),
        Effect.forkIn(resourceScope),
      )
    })
  const closeMount = (mount: ServerMount, reason: 'closed' | 'expired' = 'closed') =>
    Effect.gen(function* () {
      mount.ended ??= reason
      mounts.delete(mount.token)
      mount.frames.clear()
      yield* Deferred.fail(mount.terminal, failure(mount.ended, 'Document subscription ended'))
      yield* Scope.close(mount.scope, Exit.void)
    })
  const refreshOperation = (op: ServerOperation) =>
    Effect.gen(function* () {
      if (closed) yield* end(op, 'closed')
      else if (!op.completed && op.mount?.ended !== undefined) yield* end(op, op.mount.ended)
      else if (
        !op.completed &&
        (yield* Clock.currentTimeMillis) - op.heartbeat >= resources.leaseTimeoutMs
      )
        yield* end(op, 'expired')
    })
  const assertActive = (op: ServerOperation) =>
    refreshOperation(op).pipe(
      Effect.andThen(
        Effect.suspend(() =>
          !op.completed && op.ended === undefined
            ? Effect.void
            : Effect.fail(failure(op.ended ?? 'closed', 'Operation is no longer active')),
        ),
      ),
    )
  const assertMount = (mount: ServerMount) =>
    Effect.gen(function* () {
      yield* refreshOperation(mount.operation)
      if (mount.operation.ended !== undefined)
        return yield* failure(mount.operation.ended, 'Mount operation has ended')
      if (closed) yield* closeMount(mount)
      else if ((yield* Clock.currentTimeMillis) - mount.heartbeat >= resources.leaseTimeoutMs)
        yield* closeMount(mount, 'expired')
      if (mount.ended !== undefined)
        return yield* failure(mount.ended, 'Document subscription ended')
    })
  yield* Scope.addFinalizer(
    scope,
    Effect.gen(function* () {
      closed = true
      for (const op of operations.values()) yield* end(op, 'closed')
      for (const mount of mounts.values()) yield* closeMount(mount)
      operations.clear()
    }),
  )
  const open = Effect.fnUntraced(function* (kind: Lifetime.Kind) {
    const token = globalThis.crypto.randomUUID() + globalThis.crypto.randomUUID()
    const op: ServerOperation = {
      token,
      id: globalThis.crypto.randomUUID(),
      kind,
      ready: yield* Deferred.make<void, Remote.Failure>(),
      completion: yield* Deferred.make<void>(),
      control: yield* Queue.sliding<Control, Cause.Done>(1),
      controlOpened: false,
      heartbeat: yield* Clock.currentTimeMillis,
      selected: false,
      completed: false,
      ended: undefined,
    }
    operations.set(token, op)
    const hold = () =>
      Effect.gen(function* () {
        yield* Deferred.succeed(op.ready, undefined)
        yield* Deferred.await(op.completion).pipe(
          Effect.onInterrupt(() =>
            Effect.uninterruptible(
              Effect.gen(function* () {
                if (op.ended === undefined) {
                  op.ended = 'revoked'
                  yield* Queue.offer(op.control, { _tag: 'Revoked', protocol: 1 })
                  yield* Queue.end(op.control)
                }
                // Connected cleanup keeps heartbeats alive. Expiry still breaks an unreachable peer's hold.
                yield* Deferred.await(op.completion)
              }),
            ),
          ),
        )
      })
    const guard = observation(backend)
    const holding = guard === undefined ? Effect.scoped(hold()) : guard.observe(hold)
    yield* holding.pipe(
      Effect.catchCause((cause) =>
        Effect.gen(function* () {
          const error = Cause.findErrorOption(cause)
          yield* Deferred.fail(
            op.ready,
            Option.isSome(error) ? error.value : failure('closed', 'Native operation ended'),
          )
        }),
      ),
      Effect.forkIn(resourceScope),
    )
    const expiry = Effect.forever(
      Effect.sleep(resources.heartbeatIntervalMs).pipe(Effect.andThen(refreshOperation(op))),
    )
    yield* Effect.raceFirst(expiry, Deferred.await(op.completion)).pipe(
      Effect.forkIn(resourceScope),
    )
    yield* Deferred.await(op.ready)
    return op
  })
  const response = (reply: Reply, headers?: Record<string, string>) =>
    encode(ReplySchema, reply, resources.maxFrameBytes).pipe(
      Effect.map((text) =>
        ServerResponse.text(text, {
          contentType: 'application/x-ndjson',
          ...(headers === undefined ? {} : { headers }),
        }),
      ),
    )
  const activeResponse = (op: ServerOperation, reply: Reply, headers?: Record<string, string>) =>
    response(reply, headers).pipe(Effect.tap(() => assertActive(op)))
  const handler = Effect.gen(function* () {
    const request = yield* ServerRequest.HttpServerRequest
    if (closed) return yield* failure('closed', 'Transport handler scope is closed')
    if (request.headers[identityHeader] !== identity)
      return yield* failure('revoked', 'Transport identity is not authorized')
    const route = request.url.split('?')[0]!.split('/').at(-1)
    const token = request.headers[tokenHeader]
    const mountToken = request.headers[subscriptionHeader]
    const op = token === undefined ? undefined : operations.get(token)
    const mount = mountToken === undefined ? undefined : mounts.get(mountToken)
    const requireOperation = Effect.suspend(() =>
      op === undefined
        ? Effect.fail(failure('revoked', 'Invalid operation authority'))
        : Effect.succeed(op),
    )
    const activeOperation = requireOperation.pipe(Effect.tap(assertActive))
    const body = <S extends Schema.Constraint>(schema: S) =>
      one(
        request.stream.pipe(
          Stream.mapError(() => failure('transport', 'Request body disconnected')),
        ),
        resources.maxFrameBytes,
      ).pipe(Effect.flatMap((text) => decode(schema, text)))
    if (route === 'begin' && request.method === 'POST') {
      if (token !== undefined || mountToken !== undefined)
        return yield* failure('revoked', 'Begin cannot reuse operation or mount authority')
      const input = yield* body(BeginSchema)
      if (closed) return yield* failure('closed', 'Transport handler scope is closed')
      let method:
        | Remote.Capabilities['snapshot']
        | Remote.Capabilities['snapshotAsOf']
        | Remote.Capabilities['watch']
      if (input.kind === 'Current') method = backend.documents?.snapshot
      else if (input.kind === 'Historical') method = backend.documents?.snapshotAsOf
      else method = backend.documents?.watch
      if (method === undefined) return yield* Remote.unsupported('remoteDocument.http.begin')
      const opened = yield* open(input.kind)
      return yield* activeResponse(
        opened,
        {
          _tag: 'Ready',
          protocol: 1,
          operationId: opened.id,
          expiresInMs: resources.leaseTimeoutMs,
        },
        { [tokenHeader]: opened.token },
      )
    }
    if (route === 'frame' && request.method === 'POST') {
      if (token !== undefined)
        return yield* failure('revoked', 'Frame cannot reuse operation authority')
      if (mount === undefined || mount.ended)
        return yield* failure('closed', 'Invalid mount authority')
      yield* assertMount(mount)
      const input = yield* body(FrameRequestSchema)
      yield* assertMount(mount)
      const delivery = mount.frames.get(input.revision)
      if (delivery === undefined || delivery.frame.epoch !== input.epoch)
        return yield* failure('revoked', 'Frame is not an outstanding delivery from this mount')
      mount.frames.delete(input.revision)
      const opened = yield* open('Frame')
      opened.selected = true
      opened.delivery = delivery.done
      opened.mount = mount
      return yield* activeResponse(
        opened,
        {
          _tag: 'Ready',
          protocol: 1,
          operationId: opened.id,
          expiresInMs: resources.leaseTimeoutMs,
        },
        { [tokenHeader]: opened.token },
      ).pipe(
        Effect.tap(() => assertMount(mount)),
        Effect.onError(() => end(opened, 'closed')),
      )
    }
    if (route === 'select' && request.method === 'POST') {
      if (mountToken !== undefined)
        return yield* failure('revoked', 'Selector cannot use mount authority')
      const current = yield* activeOperation
      if (current.selected) return yield* failure('revoked', 'Operation selector is already bound')
      current.selected = true
      const input = yield* body(SelectSchema)
      yield* assertActive(current)
      if (
        current.kind === 'Frame' ||
        (current.kind === 'Historical') !== (input.at !== undefined) ||
        (current.kind !== 'Watch' && input.options !== undefined)
      )
        return yield* failure('protocol', 'Selector does not match operation kind')
      if (current.kind !== 'Watch') {
        const value =
          current.kind === 'Historical' && input.at !== undefined
            ? yield* backend.documents!.snapshotAsOf!({ ...input.request, at: input.at })
            : yield* backend.documents!.snapshot!(input.request)
        return yield* activeResponse(current, {
          _tag: 'Read',
          protocol: 1,
          target: input.request.address,
          ...(input.at === undefined ? {} : { at: input.at }),
          value: Option.getOrNull(value),
        })
      }
      if (input.options !== undefined)
        return yield* failure('protocol', 'Watch options require an authorized selector')
      yield* Lifetime.authorize(backend, input.request)
      yield* assertActive(current)
      current.request = input.request
      return yield* activeResponse(current, { _tag: 'Authorized', protocol: 1 })
    }
    if (route === 'attach' && request.method === 'POST') {
      if (mountToken !== undefined)
        return yield* failure('revoked', 'Attachment cannot use mount authority')
      const current = yield* activeOperation
      if (current.kind !== 'Watch' || current.request === undefined)
        return yield* failure('revoked', 'Watch selector is not authorized')
      const selected = current.request
      current.request = undefined
      const input = yield* body(AttachSchema)
      yield* assertActive(current)
      const mountScope = yield* Scope.fork(resourceScope)
      const stream = yield* Effect.gen(function* () {
        const pull = yield* Stream.toPull(backend.documents!.watch!(selected, input.options))
        const initial = yield* pull.pipe(
          Effect.mapError((error) =>
            Cause.isDone(error)
              ? failure('closed', 'Document mount ended before initial state')
              : error,
          ),
        )
        yield* assertActive(current)
        return Stream.concat(Stream.fromIterable(initial), Stream.fromPull(Effect.succeed(pull)))
      }).pipe(
        Effect.provideService(Scope.Scope, mountScope),
        Effect.onError(() => Scope.close(mountScope, Exit.void)),
      )
      const created: ServerMount = {
        token: globalThis.crypto.randomUUID() + globalThis.crypto.randomUUID(),
        operationToken: current.token,
        operation: current,
        scope: mountScope,
        stream,
        frames: new Map(),
        terminal: yield* Deferred.make<void, RemoteDocumentError>(),
        heartbeat: yield* Clock.currentTimeMillis,
        opened: false,
        ended: undefined,
      }
      current.mount = created
      yield* assertActive(current).pipe(Effect.onError(() => closeMount(created)))
      mounts.set(created.token, created)
      yield* Effect.gen(function* () {
        while (!created.ended) {
          yield* Effect.sleep(resources.heartbeatIntervalMs)
          if ((yield* Clock.currentTimeMillis) - created.heartbeat >= resources.leaseTimeoutMs) {
            created.ended = 'expired'
            yield* Deferred.fail(
              created.terminal,
              failure('expired', 'Document subscription expired'),
            )
            if (!created.opened) yield* closeMount(created, 'expired')
            return
          }
        }
      }).pipe(Effect.forkIn(resourceScope))
      return yield* activeResponse(
        current,
        { _tag: 'Attached', protocol: 1 },
        { [subscriptionHeader]: created.token },
      ).pipe(Effect.onError(() => closeMount(created)))
    }
    if (route === 'events' && request.method === 'GET') {
      if (mountToken !== undefined) {
        if (mount === undefined || mount.ended || mount.opened)
          return yield* failure('closed', 'Invalid or already-open mount stream')
        if (token !== mount.operationToken)
          return yield* failure('revoked', 'Stream operation does not own this mount')
        yield* assertMount(mount)
        mount.opened = true
        const data = mount.stream.pipe(
          Stream.flatMap((frame) =>
            Stream.unwrap(
              Effect.gen(function* () {
                yield* assertMount(mount)
                const revision =
                  frame._tag === 'Snapshot' || frame._tag === 'Rebase' ? frame.revision : frame.to
                const done = yield* Deferred.make<void>()
                mount.frames.set(revision, { frame, done })
                // Only this subscriber waits for its decoder; native publication still offers without waiting.
                return Stream.concat(
                  Stream.succeed({ _tag: 'Data' as const, protocol: 1 as const, frame }),
                  Stream.drain(Stream.fromEffect(Deferred.await(done))),
                )
              }),
            ),
          ),
          Stream.interruptWhen(Deferred.await(mount.terminal)),
          Stream.catch((cause) =>
            Stream.succeed({
              _tag: 'Failure' as const,
              protocol: 1 as const,
              error: serialize(cause),
            }),
          ),
        )
        const encoded = data.pipe(
          Stream.mapEffect((message) =>
            encode(ControlSchema, message, resources.maxFrameBytes).pipe(
              Effect.tap(() => (message._tag === 'Data' ? assertMount(mount) : Effect.void)),
            ),
          ),
          Stream.catch((error) =>
            Stream.fromEffect(
              encode(
                ControlSchema,
                { _tag: 'Failure', protocol: 1, error: serialize(error) },
                resources.maxFrameBytes,
              ),
            ),
          ),
        )
        return ServerResponse.stream(
          encoded.pipe(
            Stream.map((text) => encoder.encode(text)),
            Stream.ensuring(closeMount(mount)),
          ),
          { contentType: 'application/x-ndjson' },
        )
      }
      const current = yield* requireOperation
      yield* refreshOperation(current)
      if (current.controlOpened)
        return yield* failure('revoked', 'Operation control stream is already open')
      current.controlOpened = true
      return ServerResponse.stream(
        Stream.fromQueue(current.control).pipe(
          Stream.mapEffect((message) => encode(ControlSchema, message, resources.maxFrameBytes)),
          Stream.map((text) => encoder.encode(text)),
        ),
        { contentType: 'application/x-ndjson' },
      )
    }
    if (
      (route === 'heartbeat' || route === 'complete' || route === 'cancel') &&
      request.method === 'POST'
    ) {
      yield* body(ActionSchema)
      if (mountToken !== undefined) {
        if (mount === undefined || mount.ended || token !== undefined)
          return yield* failure('closed', 'Invalid mount action authority')
        if (route === 'complete')
          return yield* failure('protocol', 'A mount cannot complete an operation')
        yield* assertMount(mount)
        if (route === 'cancel') yield* closeMount(mount)
        else mount.heartbeat = yield* Clock.currentTimeMillis
        return yield* response({
          _tag: route === 'cancel' ? 'Cancelled' : 'Heartbeat',
          protocol: 1,
        })
      }
      const current = yield* requireOperation
      yield* refreshOperation(current)
      if (route === 'heartbeat') {
        if (current.completed || current.ended === 'expired' || current.ended === 'closed')
          return yield* failure(current.ended ?? 'closed', 'Operation has ended')
        current.heartbeat = yield* Clock.currentTimeMillis
        return yield* response({ _tag: 'Heartbeat', protocol: 1 })
      }
      if (current.ended === 'expired' || current.ended === 'closed')
        return yield* failure(current.ended, 'Operation has ended')
      if (current.completed) {
        if (route === 'cancel')
          return yield* failure('closed', 'Completed operation cannot be cancelled')
        return yield* response({ _tag: 'AlreadyCompleted', protocol: 1 })
      }
      if (route === 'cancel') {
        yield* end(current, 'closed')
        return yield* response({ _tag: 'Cancelled', protocol: 1 })
      }
      if (current.ended === 'revoked')
        return yield* failure('revoked', 'Revoked operation cannot complete successfully')
      current.completed = true
      yield* complete(current)
      yield* Queue.end(current.control)
      yield* Effect.sleep(resources.leaseTimeoutMs).pipe(
        Effect.andThen(Effect.sync(() => operations.delete(current.token))),
        Effect.forkIn(resourceScope),
      )
      return yield* response({ _tag: 'Completed', protocol: 1 })
    }
    return yield* failure('protocol', 'Unknown document route or method')
  }).pipe(
    Effect.catch((error) =>
      response({ _tag: 'Failure', protocol: 1, error: serialize(error) }).pipe(
        Effect.orElseSucceed(() =>
          ServerResponse.text('Transport response unavailable', { status: 413 }),
        ),
      ),
    ),
  )
  // oxlint-disable-next-line effecttsgo/return-effect-in-gen -- This scoped factory intentionally returns the captured request Effect.
  return handler
})
