import { assert, describe, it } from '@effect/vitest'
import * as NodeHttpServer from '@effect/platform-node/NodeHttpServer'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Exit from 'effect/Exit'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as HttpServer from 'effect/http/HttpServer'
import * as HttpClient from 'effect/http/HttpClient'
import * as ClientRequest from 'effect/http/HttpClientRequest'
import * as ServerResponse from 'effect/http/HttpServerResponse'
import * as Harness from 'effect-harness/Harness'
import { HarnessBackend } from 'effect-harness/HarnessBackend'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Document from 'effect-harness/Document'
import * as Remote from 'effect-harness/RemoteDocument'
import * as Http from 'effect-harness/RemoteDocumentHttp'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Transaction from 'effect-harness/Transaction'
import * as Native from '../src/internal/SessionRuntime.js'
import { SessionError } from 'effect-harness/SessionError'

const httpLayer =
  'Bun' in globalThis
    ? (await import('@effect/platform-bun/BunHttpServer')).layerTest
    : NodeHttpServer.layerTest
const resources = { maxFrameBytes: 65536, heartbeatIntervalMs: 25, leaseTimeoutMs: 1000 }
const root = Record.ROOT_CONVERSATION_ID
const target = { scope: { _tag: 'conversation', conversationId: root } } satisfies Document.Target
const old = Document.define({
  kind: 'remote.counter',
  scope: 'conversation',
  history: 'rewindable',
  fork: 'asOf',
  version: 1,
  schema: Schema.Struct({ old: Schema.Natural }),
  initial: () => ({ old: 4 }),
})
const current = Document.define({
  kind: old.definition.kind,
  scope: 'conversation',
  history: 'rewindable',
  fork: 'asOf',
  version: 3,
  schema: Schema.Struct({ count: Schema.Natural }),
  initial: () => ({ count: 0 }),
  migrations: {
    1: (value: Schema.JsonObject) =>
      Schema.decodeUnknownEffect(old.definition.schema)(value).pipe(
        Effect.map((value) => ({ count: value.old })),
      ),
  },
})
class Label extends Context.Service<Label, string>()('remote-http/Label') {}
const codec = current.definition.schema.pipe(
  Schema.decodeTo(Schema.Struct({ count: Schema.Natural, label: Schema.String }), {
    decode: SchemaGetter.transformEffect((value) =>
      Label.pipe(Effect.map((label) => ({ ...value, label }))),
    ),
    encode: SchemaGetter.transform((value) => ({ count: value.count })),
  }),
)
const serviceDocument = Document.define({
  kind: current.definition.kind,
  scope: 'conversation',
  history: 'rewindable',
  fork: 'asOf',
  version: 3,
  migrations: { 1: current.definition.migrations![1]! },
  schema: codec,
  initial: () => ({ count: 0, label: '' }),
})
const fixture = (identity = 'test-remote-owner', limits = resources) =>
  Effect.gen(function* () {
    const model = yield* LanguageModel.make({
      generateText: () => Effect.die('Provider must not run'),
      streamText: () => Stream.die('Provider must not run'),
    })
    const nativeOwner = yield* Scope.fork(yield* Scope.Scope)
    const runtime = yield* HarnessRuntime.make().pipe(
      Effect.provideService(LanguageModel.LanguageModel, model),
      Effect.provideService(Scope.Scope, nativeOwner),
    )
    const at = yield* Session.commit(runtime.session, (tx) =>
      Effect.gen(function* () {
        yield* Transaction.ensureRoot(tx)
        yield* Transaction.ensureDocument(tx, old, target)
        return yield* Transaction.appendEntry(tx, root, { kind: 'remote.cutoff' })
      }),
    )
    const serverOwner = yield* Scope.fork(yield* Scope.Scope)
    const handler = yield* Http.makeHandler(runtime.backend, { ...limits, identity }).pipe(
      Effect.provideService(Scope.Scope, serverOwner),
    )
    yield* HttpServer.serveEffect(handler).pipe(Effect.provideService(Scope.Scope, serverOwner))
    const endpoint = yield* HttpServer.addressFormattedWith(Effect.succeed)
    const backend = yield* Http.make(runtime.backend, { ...limits, endpoint, identity })
    const harness = yield* Harness.make.pipe(Effect.provideService(HarnessBackend, backend))
    return {
      runtime,
      backend,
      conversation: yield* harness.root,
      at,
      endpoint,
      serverOwner,
      nativeOwner,
    }
  })
const run = <A, E, R>(program: Effect.Effect<A, E, R>) =>
  Effect.scoped(program).pipe(Effect.provide([httpLayer, Storage.layerMemory]))

describe('real HTTP materialized documents', () => {
  it.live(
    'handler Scope closes an unacknowledged operation without waiting on its hold fiber',
    () =>
      run(
        Effect.gen(function* () {
          const { endpoint, serverOwner } = yield* fixture()
          const client = yield* HttpClient.HttpClient
          const begun = yield* client.execute(
            ClientRequest.post(`${endpoint}/documents/v1/begin`).pipe(
              ClientRequest.setHeader('x-document-identity', 'test-remote-owner'),
              ClientRequest.bodyText('{"protocol":1,"kind":"Current"}\n'),
            ),
          )
          yield* begun.text
          assert.isDefined(begun.headers['x-document-operation'])
          yield* Scope.close(serverOwner, Exit.void)
        }),
      ),
  )
  it.live('a frame body crossing mount expiry cannot mint a new operation or reopen events', () =>
    run(
      Effect.gen(function* () {
        const { endpoint, runtime } = yield* fixture('test-remote-owner', {
          ...resources,
          heartbeatIntervalMs: 15,
          leaseTimeoutMs: 200,
        })
        const client = yield* HttpClient.HttpClient
        const send = (route: string, text: string, token?: string, mount?: string) => {
          let request = ClientRequest.post(`${endpoint}/documents/v1/${route}`).pipe(
            ClientRequest.setHeader('x-document-identity', 'test-remote-owner'),
            ClientRequest.bodyText(text + '\n'),
          )
          if (token !== undefined)
            request = ClientRequest.setHeader(request, 'x-document-operation', token)
          if (mount !== undefined)
            request = ClientRequest.setHeader(request, 'x-document-subscription', mount)
          return client.execute(request)
        }
        const dto = Schema.fromJsonString(
          Schema.Struct({
            _tag: Schema.String,
            frame: Schema.optionalKey(Remote.WireFrameSchema),
            error: Schema.optionalKey(Schema.Struct({ reason: Schema.String })),
          }),
        )
        const parse = Schema.decodeEffect(dto)
        const begun = yield* send('begin', '{"protocol":1,"kind":"Watch"}')
        const token = begun.headers['x-document-operation']!
        const selection = yield* Schema.encodeEffect(
          Schema.fromJsonString(Remote.ReadRequestSchema),
        )({
          address: { ...target, kind: old.definition.kind },
          metadata: old.definition,
          family: false,
        })
        yield* send('select', `{"protocol":1,"request":${selection}}`, token).pipe(
          Effect.flatMap((response) => response.text),
        )
        const attached = yield* send('attach', '{"protocol":1}', token)
        yield* attached.text
        const mount = attached.headers['x-document-subscription']!
        yield* send('complete', '{"protocol":1}', token).pipe(
          Effect.flatMap((response) => response.text),
        )
        const events = ClientRequest.get(`${endpoint}/documents/v1/events`).pipe(
          ClientRequest.setHeader('x-document-identity', 'test-remote-owner'),
          ClientRequest.setHeader('x-document-operation', token),
          ClientRequest.setHeader('x-document-subscription', mount),
        )
        const stream = yield* client.execute(events)
        const pull = yield* stream.stream.pipe(
          Stream.decodeText(),
          Stream.splitLines,
          Stream.mapEffect((text) => parse(text)),
          Stream.rechunk(1),
          Stream.toPull,
        )
        const initial = (yield* pull)[0]!
        assert.strictEqual(initial._tag, 'Data')
        const frame = initial.frame!
        const text = yield* Schema.encodeEffect(
          Schema.fromJsonString(
            Schema.Struct({
              protocol: Schema.Literal(1),
              epoch: Schema.String,
              revision: Schema.Natural,
            }),
          ),
        )({
          protocol: 1,
          epoch: frame.epoch,
          revision:
            frame._tag === 'Snapshot' || frame._tag === 'Rebase' ? frame.revision : frame.to,
        })
        const frameOperation = yield* send('frame', text, undefined, mount)
        yield* frameOperation.text
        const frameToken = frameOperation.headers['x-document-operation']!
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined))
        const pending = yield* client
          .execute(
            ClientRequest.post(`${endpoint}/documents/v1/frame`).pipe(
              ClientRequest.setHeader('x-document-identity', 'test-remote-owner'),
              ClientRequest.setHeader('x-document-subscription', mount),
              ClientRequest.bodyStream(
                Stream.concat(
                  Stream.fromEffect(
                    Deferred.succeed(entered, undefined).pipe(
                      Effect.as(new TextEncoder().encode(text.slice(0, 12))),
                    ),
                  ),
                  Stream.fromEffect(
                    Deferred.await(release).pipe(
                      Effect.as(new TextEncoder().encode(text.slice(12) + '\n')),
                    ),
                  ),
                ),
              ),
            ),
          )
          .pipe(Effect.forkChild)
        yield* Deferred.await(entered)
        yield* Effect.sleep(100)
        assert.strictEqual(
          (yield* send('heartbeat', '{"protocol":1}', frameToken).pipe(
            Effect.flatMap((response) => response.text),
            Effect.flatMap(parse),
          ))._tag,
          'Heartbeat',
        )
        const expired = (yield* pull)[0]!
        yield* Deferred.succeed(release, undefined)
        const response = yield* Fiber.join(pending)
        const result = yield* response.text.pipe(Effect.flatMap(parse))
        const heartbeat = yield* send('heartbeat', '{"protocol":1}', frameToken).pipe(
          Effect.flatMap((response) => response.text),
          Effect.flatMap(parse),
        )
        const ack = yield* send('complete', '{"protocol":1}', frameToken).pipe(
          Effect.flatMap((response) => response.text),
          Effect.flatMap(parse),
        )
        assert.strictEqual(heartbeat.error?.reason, 'expired')
        assert.strictEqual(ack.error?.reason, 'expired')
        assert.strictEqual(expired.error?.reason, 'expired')
        assert.strictEqual(result._tag, 'Failure')
        assert.strictEqual(result.error?.reason, 'expired')
        assert.isUndefined(response.headers['x-document-operation'])
        assert.strictEqual((yield* Native.get(runtime.session)).documentMounts.size, 0)
        const reopening = yield* client.execute(events).pipe(
          Effect.flatMap((response) => response.text),
          Effect.flatMap(parse),
        )
        assert.strictEqual(reopening._tag, 'Failure')
        assert.strictEqual(reopening.error?.reason, 'closed')
      }),
    ),
  )
  for (const route of ['select', 'attach'] as const)
    for (const boundary of ['body', 'native'] as const)
      it.live(`expired ${route} rejects late ${boundary} admission and leaves no mount`, () =>
        run(
          Effect.gen(function* () {
            const limits = { ...resources, heartbeatIntervalMs: 15, leaseTimeoutMs: 200 }
            const { runtime, endpoint } = yield* fixture('test-remote-owner', limits)
            const client = yield* HttpClient.HttpClient
            const dto = Schema.Struct({
              _tag: Schema.String,
              error: Schema.optionalKey(Schema.Struct({ reason: Schema.String })),
            })
            const parse = (text: string) => Schema.decodeEffect(Schema.fromJsonString(dto))(text)
            const send = (action: string, body: string, token?: string) => {
              let request = ClientRequest.post(`${endpoint}/documents/v1/${action}`).pipe(
                ClientRequest.setHeader('x-document-identity', 'test-remote-owner'),
                ClientRequest.bodyText(body + '\n'),
              )
              if (token !== undefined)
                request = ClientRequest.setHeader(request, 'x-document-operation', token)
              return client.execute(request)
            }
            const begun = yield* send(
              'begin',
              `{"protocol":1,"kind":"${route === 'select' ? 'Current' : 'Watch'}"}`,
            )
            const token = begun.headers['x-document-operation']!
            const selector = yield* Schema.encodeEffect(
              Schema.fromJsonString(Remote.ReadRequestSchema),
            )({
              address: { ...target, kind: old.definition.kind },
              metadata: old.definition,
              family: false,
            })
            const selection = `{"protocol":1,"request":${selector}}`
            if (route === 'attach')
              assert.strictEqual(
                (yield* send('select', selection, token).pipe(
                  Effect.flatMap((response) => response.text),
                  Effect.flatMap(parse),
                ))._tag,
                'Authorized',
              )
            const entered = yield* Deferred.make<void>()
            const release = yield* Deferred.make<void>()
            yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined))
            const native = yield* Native.get(runtime.session)
            if (boundary === 'native') {
              native.cache.clear()
              const document = native.storage.document
              Object.assign(native, {
                storage: {
                  ...native.storage,
                  document: (...args: Parameters<typeof document>) =>
                    Deferred.succeed(entered, undefined).pipe(
                      Effect.andThen(Deferred.await(release)),
                      Effect.andThen(document(...args)),
                    ),
                },
              })
            }
            const text = route === 'select' ? selection : '{"protocol":1,"options":{"capacity":1}}'
            const request = ClientRequest.post(`${endpoint}/documents/v1/${route}`).pipe(
              ClientRequest.setHeader('x-document-identity', 'test-remote-owner'),
              ClientRequest.setHeader('x-document-operation', token),
            )
            const pending = yield* client
              .execute(
                boundary === 'body'
                  ? request.pipe(
                      ClientRequest.bodyStream(
                        Stream.concat(
                          Stream.fromEffect(
                            Deferred.succeed(entered, undefined).pipe(
                              Effect.as(new TextEncoder().encode(text.slice(0, 12))),
                            ),
                          ),
                          Stream.fromEffect(
                            Deferred.await(release).pipe(
                              Effect.as(new TextEncoder().encode(text.slice(12) + '\n')),
                            ),
                          ),
                        ),
                      ),
                    )
                  : request.pipe(ClientRequest.bodyText(text + '\n')),
              )
              .pipe(Effect.forkChild)
            yield* Deferred.await(entered)
            const expired = yield* client
              .execute(
                ClientRequest.get(`${endpoint}/documents/v1/events`).pipe(
                  ClientRequest.setHeader('x-document-identity', 'test-remote-owner'),
                  ClientRequest.setHeader('x-document-operation', token),
                ),
              )
              .pipe(
                Effect.flatMap((response) => response.text),
                Effect.flatMap(parse),
              )
            assert.strictEqual(expired._tag, 'Expired')
            yield* Deferred.succeed(release, undefined)
            const response = yield* Fiber.join(pending)
            const result = yield* response.text.pipe(Effect.flatMap(parse))
            assert.strictEqual(result._tag, 'Failure')
            assert.strictEqual(result.error?.reason, 'expired')
            assert.isUndefined(response.headers['x-document-operation'])
            assert.isUndefined(response.headers['x-document-subscription'])
            assert.strictEqual(native.documentMounts.size, 0)
            const ack = yield* send('complete', '{"protocol":1}', token).pipe(
              Effect.flatMap((response) => response.text),
              Effect.flatMap(parse),
            )
            assert.strictEqual(ack.error?.reason, 'expired')
          }),
        ),
      )
  it.live(
    'HTTP capture registers before a queued writer and delivers initial then replacement exactly once',
    () =>
      run(
        Effect.gen(function* () {
          const storage = yield* Storage.Storage
          let writes = 0
          const commit = storage.commit
          Object.assign(storage, {
            commit: (...args: Parameters<typeof commit>) =>
              Effect.sync(() => {
                writes++
              }).pipe(Effect.andThen(commit(...args))),
          })
          const { runtime, conversation, nativeOwner } = yield* fixture()
          const baselineWrites = writes
          const native = yield* Native.get(runtime.session)
          native.cache.clear()
          const before = yield* storage
            .scanEntries({ conversationId: root })
            .pipe(Stream.runCollect)
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined))
          const document = native.storage.document
          Object.assign(native, {
            storage: {
              ...native.storage,
              document: (...args: Parameters<typeof document>) =>
                Deferred.succeed(entered, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                  Effect.andThen(document(...args)),
                ),
            },
          })
          const initial = yield* Deferred.make<void>()
          const replaced = yield* Deferred.make<void>()
          const frames: Remote.MaterializedFrame<typeof current.definition.schema>[] = []
          const watching = yield* Remote.watchWith(conversation, current, target, (frame) =>
            Effect.gen(function* () {
              frames.push(frame)
              if (frame._tag === 'Snapshot') yield* Deferred.succeed(initial, undefined)
              else if (frame._tag === 'Replace') yield* Deferred.succeed(replaced, undefined)
            }),
          ).pipe(Stream.runDrain, Effect.forkChild)
          yield* Deferred.await(entered)
          let admitted = false
          const writer = yield* Session.commit(runtime.session, (tx) =>
            Effect.gen(function* () {
              admitted = true
              assert.strictEqual(writes, baselineWrites)
              assert.strictEqual(native.documentMounts.size, 1)
              assert.deepEqual(
                yield* storage.scanEntries({ conversationId: root }).pipe(Stream.runCollect),
                before,
              )
              yield* Transaction.setDocument(tx, old, target, { old: 11 })
            }),
          ).pipe(Effect.forkChild)
          yield* Effect.yieldNow
          assert.isFalse(admitted)
          yield* Deferred.succeed(release, undefined)
          yield* Fiber.join(writer)
          yield* Effect.raceFirst(Deferred.await(initial), Fiber.join(watching))
          yield* Effect.raceFirst(Deferred.await(replaced), Fiber.join(watching))
          yield* Scope.close(nativeOwner, Exit.void)
          yield* Fiber.join(watching)
          assert.strictEqual(writes, baselineWrites + 1)
          assert.deepEqual(
            frames.map((frame) => frame._tag),
            ['Snapshot', 'Replace'],
          )
          const first = frames[0]!
          const second = frames[1]!
          if (first._tag !== 'Retired' && second._tag !== 'Retired') {
            assert.deepEqual(first.value.value, { count: 4 })
            assert.deepEqual(second.value.value, { count: 11 })
            assert.strictEqual(first.value.storedVersion, 1)
            assert.strictEqual(second.value.storedVersion, 1)
          }
          assert.strictEqual(native.documentMounts.size, 0)
        }),
      ),
  )
  it.live('normal native Scope close drains the latest HTTP state before end', () =>
    run(
      Effect.gen(function* () {
        const { runtime, conversation, nativeOwner } = yield* fixture()
        const pull = yield* Remote.watchFrames(conversation, current, target).pipe(
          Stream.rechunk(1),
          Stream.toPull,
        )
        assert.strictEqual((yield* pull)[0]._tag, 'Snapshot')
        yield* Session.commit(runtime.session, (tx) =>
          Transaction.setDocument(tx, old, target, { old: 21 }),
        )
        yield* Scope.close(nativeOwner, Exit.void)
        const final = (yield* pull)[0]
        assert.strictEqual(final._tag, 'Replace')
        if (final._tag !== 'Retired') assert.deepEqual(final.value.value, { count: 21 })
        assert.isTrue(Exit.isFailure(yield* pull.pipe(Effect.exit)))
        assert.strictEqual((yield* Native.get(runtime.session)).documentMounts.size, 0)
      }),
    ),
  )
  it.live('native poison reaches HTTP consumers as a declared semantic failure', () =>
    run(
      Effect.gen(function* () {
        const { runtime, conversation } = yield* fixture()
        const pull = yield* Remote.watchFrames(conversation, current, target).pipe(
          Stream.rechunk(1),
          Stream.toPull,
        )
        yield* pull
        yield* Native.stop(
          yield* Native.get(runtime.session),
          new SessionError({
            reason: 'closed',
            operation: 'remote.poison-test',
            message: 'Native producer cannot continue',
          }),
        )
        const error = yield* pull.pipe(Effect.flip)
        assert.strictEqual(error._tag, 'SessionError')
        if (error._tag === 'SessionError') assert.strictEqual(error.operation, 'remote.poison-test')
        assert.strictEqual((yield* Native.get(runtime.session)).documentMounts.size, 0)
      }),
    ),
  )
  it.live(
    'captured endpoint and identity stay fixed across overlapping IDs and ambient backend changes',
    () =>
      run(
        Effect.gen(function* () {
          const a = yield* fixture()
          const connectionB = yield* Layer.build(
            Layer.fresh(Layer.merge(httpLayer, Storage.layerMemory)),
          )
          const b = yield* fixture('endpoint-b').pipe(Effect.provideContext(connectionB))
          yield* Session.commit(b.runtime.session, (tx) =>
            Transaction.setDocument(tx, old, target, { old: 19 }),
          )
          assert.strictEqual(a.conversation.id, b.conversation.id)
          assert.notStrictEqual(a.endpoint, b.endpoint)
          const captured = Option.getOrThrow(
            yield* Remote.snapshot(a.conversation, serviceDocument, target).pipe(
              Effect.provideService(HarnessBackend, b.backend),
              Effect.provideService(Label, 'original-client'),
            ),
          )
          assert.deepEqual(captured.value, { count: 4, label: 'original-client' })
          assert.deepEqual(
            Option.getOrThrow(yield* Remote.snapshot(b.conversation, current, target)).value,
            { count: 19 },
          )
        }),
      ),
  )
  it.live('an unrelated operation cannot open or act on an authorized mount', () =>
    run(
      Effect.gen(function* () {
        const { endpoint } = yield* fixture()
        const client = yield* HttpClient.HttpClient
        const send = (route: string, body: string, operation?: string, mount?: string) => {
          let request = ClientRequest.post(`${endpoint}/documents/v1/${route}`).pipe(
            ClientRequest.setHeader('x-document-identity', 'test-remote-owner'),
            ClientRequest.bodyText(body + '\n'),
          )
          if (operation !== undefined)
            request = ClientRequest.setHeader(request, 'x-document-operation', operation)
          if (mount !== undefined)
            request = ClientRequest.setHeader(request, 'x-document-subscription', mount)
          return client.execute(request)
        }
        const dto = Schema.Struct({
          _tag: Schema.String,
          error: Schema.optionalKey(Schema.Struct({ reason: Schema.String })),
        })
        const reply = (response: Effect.Success<ReturnType<typeof send>>) =>
          response.text.pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(dto))))
        const acquisition = yield* send('begin', '{"protocol":1,"kind":"Watch"}')
        const owner = acquisition.headers['x-document-operation']!
        const selector = yield* Schema.encodeEffect(
          Schema.fromJsonString(Remote.ReadRequestSchema),
        )({
          address: { ...target, kind: old.definition.kind },
          metadata: old.definition,
          family: false,
        })
        assert.strictEqual(
          (yield* send('select', `{"protocol":1,"request":${selector}}`, owner).pipe(
            Effect.flatMap(reply),
          ))._tag,
          'Authorized',
        )
        const attached = yield* send('attach', '{"protocol":1}', owner)
        const mount = attached.headers['x-document-subscription']!
        const other = (yield* send('begin', '{"protocol":1,"kind":"Current"}')).headers[
          'x-document-operation'
        ]!
        const foreignStream = yield* client.execute(
          ClientRequest.get(`${endpoint}/documents/v1/events`).pipe(
            ClientRequest.setHeader('x-document-identity', 'test-remote-owner'),
            ClientRequest.setHeader('x-document-operation', other),
            ClientRequest.setHeader('x-document-subscription', mount),
          ),
        )
        assert.strictEqual((yield* reply(foreignStream)).error?.reason, 'revoked')
        assert.strictEqual(
          (yield* send('heartbeat', '{"protocol":1}', other, mount).pipe(Effect.flatMap(reply)))
            .error?.reason,
          'closed',
        )
        assert.strictEqual(
          (yield* send(
            'frame',
            '{"protocol":1,"epoch":"invented","revision":0}',
            other,
            mount,
          ).pipe(Effect.flatMap(reply))).error?.reason,
          'revoked',
        )
        assert.strictEqual(
          (yield* send(
            'frame',
            '{"protocol":1,"epoch":"invented","revision":0}',
            undefined,
            mount,
          ).pipe(Effect.flatMap(reply))).error?.reason,
          'revoked',
        )
        yield* send('cancel', '{"protocol":1}', undefined, mount)
        yield* send('complete', '{"protocol":1}', owner)
        yield* send('complete', '{"protocol":1}', other)
      }),
    ),
  )
  it.live(
    'server shutdown interrupts an actual held HTTP callback, cleans mounts and reconnects with a new epoch',
    () =>
      run(
        Effect.gen(function* () {
          const { runtime, conversation, serverOwner } = yield* fixture()
          const entered = yield* Deferred.make<void>()
          let epoch = ''
          let cleaned = 0
          const observer = yield* Remote.watchWith(conversation, current, target, (frame) =>
            Effect.gen(function* () {
              epoch = frame.epoch
              yield* Effect.addFinalizer(() =>
                Effect.sync(() => {
                  cleaned++
                }),
              )
              yield* Deferred.succeed(entered, undefined)
              return yield* Effect.never
            }),
          ).pipe(Stream.runDrain, Effect.forkChild)
          yield* Deferred.await(entered)
          yield* Scope.close(serverOwner, Exit.void)
          const ended = yield* Fiber.await(observer)
          assert.isTrue(Exit.isFailure(ended))
          assert.strictEqual(cleaned, 1)
          assert.strictEqual((yield* Native.get(runtime.session)).documentMounts.size, 0)
          const connection = yield* Layer.build(Layer.fresh(httpLayer))
          const fresh = yield* Effect.gen(function* () {
            const handler = yield* Http.makeHandler(runtime.backend, {
              ...resources,
              identity: 'test-remote-owner',
            })
            yield* HttpServer.serveEffect(handler)
            const endpoint = yield* HttpServer.addressFormattedWith(Effect.succeed)
            const backend = yield* Http.make(runtime.backend, {
              ...resources,
              endpoint,
              identity: 'test-remote-owner',
            })
            const harness = yield* Harness.make.pipe(Effect.provideService(HarnessBackend, backend))
            return Option.getOrThrow(
              yield* Remote.watchFrames(yield* harness.root, current, target).pipe(Stream.runHead),
            )
          }).pipe(Effect.provideContext(connection))
          assert.strictEqual(fresh._tag, 'Snapshot')
          assert.notStrictEqual(fresh.epoch, epoch)
        }),
      ),
  )
  for (const kind of ['jsonl', 'sqlite'] as const)
    it.live(
      `cold ${kind} reopen preserves stored versions and visible remote history without writes`,
      () =>
        run(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'remote-reopen-' })
            const sql =
              'Bun' in globalThis
                ? (yield* Effect.promise(() => import('@effect/sql-sqlite-bun/SqliteClient'))).layer
                : (yield* Effect.promise(() => import('@effect/sql-sqlite-node/SqliteClient')))
                    .layer
            const filePath = `${directory}/${kind === 'jsonl' ? 'journal.jsonl' : 'storage.sqlite'}`
            const storageLayer =
              kind === 'jsonl'
                ? Storage.layerJsonl({ filePath })
                : Storage.layerSql.pipe(Layer.provide(sql({ filename: filePath })))
            const model = yield* LanguageModel.make({
              generateText: () => Effect.die('No provider'),
              streamText: () => Stream.die('No provider'),
            })
            const initialScope = yield* Scope.make()
            const initialContext = yield* Layer.build(storageLayer).pipe(
              Effect.provideService(Scope.Scope, initialScope),
            )
            const initial = yield* HarnessRuntime.make().pipe(
              Effect.provideContext(initialContext),
              Effect.provideService(LanguageModel.LanguageModel, model),
              Effect.provideService(Scope.Scope, initialScope),
            )
            const at = yield* Session.commit(initial.session, (tx) =>
              Effect.gen(function* () {
                yield* Transaction.ensureRoot(tx)
                yield* Transaction.ensureDocument(tx, old, target)
                return yield* Transaction.appendEntry(tx, root, { kind: 'remote.reopen' })
              }),
            )
            yield* Session.commit(initial.session, (tx) =>
              Transaction.setDocument(tx, old, target, { old: 7 }),
            )
            yield* Scope.close(initialScope, Exit.void)
            const reopened = yield* Layer.build(storageLayer)
            const runtime = yield* HarnessRuntime.make().pipe(
              Effect.provideContext(reopened),
              Effect.provideService(LanguageModel.LanguageModel, model),
            )
            const handler = yield* Http.makeHandler(runtime.backend, {
              ...resources,
              identity: 'cold-client',
            })
            yield* HttpServer.serveEffect(handler)
            const endpoint = yield* HttpServer.addressFormattedWith(Effect.succeed)
            const backend = yield* Http.make(runtime.backend, {
              ...resources,
              endpoint,
              identity: 'cold-client',
            })
            const harness = yield* Harness.make.pipe(Effect.provideService(HarnessBackend, backend))
            const conversation = yield* harness.root
            const before = yield* fs.readFile(filePath)
            const read = Option.getOrThrow(yield* Remote.snapshot(conversation, current, target))
            const historical = Option.getOrThrow(
              yield* Remote.snapshotAsOf(conversation, current, target, at.id),
            )
            assert.strictEqual(read.storedVersion, 1)
            assert.deepEqual(read.value, { count: 7 })
            assert.strictEqual(historical.storedVersion, 1)
            assert.deepEqual(historical.value, { count: 4 })
            const watched = Option.getOrThrow(
              yield* Remote.watchFrames(conversation, current, target).pipe(Stream.runHead),
            )
            assert.strictEqual(watched._tag, 'Snapshot')
            if (watched._tag !== 'Retired') assert.strictEqual(watched.value.storedVersion, 1)
            assert.deepEqual(yield* fs.readFile(filePath), before)
            assert.deepEqual(
              Option.getOrThrow(yield* Session.snapshot(runtime.session, old, target)).value,
              { old: 7 },
            )
          }),
        ),
    )
  it.live(
    'remote history respects actual visible ancestor cutoff and rejects invisible cutoffs',
    () =>
      run(
        Effect.gen(function* () {
          const { runtime, conversation, at } = yield* fixture()
          yield* Session.commit(runtime.session, (tx) =>
            Transaction.setDocument(tx, old, target, { old: 9 }),
          )
          const child = yield* Session.commit(runtime.session, (tx) =>
            Transaction.forkConversation(tx, root, at.id, { ownership: { _tag: 'ownerless' } }),
          )
          const childTarget = { scope: { _tag: 'conversation' as const, conversationId: child.id } }
          const now = Option.getOrThrow(yield* Remote.snapshot(conversation, current, childTarget))
          const past = Option.getOrThrow(
            yield* Remote.snapshotAsOf(conversation, current, childTarget, at.id),
          )
          assert.deepEqual(now.value, { count: 4 })
          assert.deepEqual(past.value, { count: 4 })
          assert.strictEqual(past.record.scope._tag, 'conversation')
          if (past.record.scope._tag === 'conversation')
            assert.strictEqual(past.record.scope.conversationId, root)
          const absent = yield* Remote.snapshotAsOf(
            conversation,
            current,
            childTarget,
            Record.EntryId.make(99999),
          ).pipe(Effect.flip)
          assert.strictEqual(absent._tag, 'SessionError')
          if (absent._tag === 'SessionError') assert.strictEqual(absent.reason, 'notFound')
        }),
      ),
  )
  it.live(
    'rejects wrong identity, expired leases and stale ACK; repeated completion cannot release another operation',
    () =>
      run(
        Effect.gen(function* () {
          const { endpoint } = yield* fixture()
          const client = yield* HttpClient.HttpClient
          const send = (route: string, token?: string, identity = 'test-remote-owner') => {
            const request = ClientRequest.post(`${endpoint}/documents/v1/${route}`).pipe(
              ClientRequest.setHeader('x-document-identity', identity),
              ClientRequest.bodyText(
                route === 'begin' ? '{"protocol":1,"kind":"Current"}\n' : '{"protocol":1}\n',
              ),
            )
            return client.execute(
              token === undefined
                ? request
                : ClientRequest.setHeader(request, 'x-document-operation', token),
            )
          }
          const dto = Schema.Struct({
            _tag: Schema.String,
            error: Schema.optionalKey(Schema.Struct({ reason: Schema.String })),
          })
          const reply = (response: Awaited<Effect.Success<ReturnType<typeof send>>>) =>
            response.text.pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(dto))))
          const denied = yield* send('begin', undefined, 'wrong-identity').pipe(
            Effect.flatMap(reply),
          )
          assert.strictEqual(denied.error?.reason, 'revoked')
          const a = yield* send('begin')
          const b = yield* send('begin')
          const tokenA = a.headers['x-document-operation']
          const tokenB = b.headers['x-document-operation']
          assert.strictEqual(
            (yield* send('complete', tokenA).pipe(Effect.flatMap(reply)))._tag,
            'Completed',
          )
          assert.strictEqual(
            (yield* send('complete', tokenA).pipe(Effect.flatMap(reply)))._tag,
            'AlreadyCompleted',
          )
          assert.strictEqual(
            (yield* send('heartbeat', tokenB).pipe(Effect.flatMap(reply)))._tag,
            'Heartbeat',
          )
          assert.strictEqual(
            (yield* send('complete', tokenB).pipe(Effect.flatMap(reply)))._tag,
            'Completed',
          )
          const cancelled = yield* send('begin')
          const cancelledToken = cancelled.headers['x-document-operation']
          assert.strictEqual(
            (yield* send('cancel', cancelledToken).pipe(Effect.flatMap(reply)))._tag,
            'Cancelled',
          )
          assert.strictEqual(
            (yield* send('complete', cancelledToken).pipe(Effect.flatMap(reply))).error?.reason,
            'closed',
          )
          const expiring = yield* send('begin')
          const expiringToken = expiring.headers['x-document-operation']
          const response = yield* client.execute(
            ClientRequest.get(`${endpoint}/documents/v1/events`).pipe(
              ClientRequest.setHeader('x-document-identity', 'test-remote-owner'),
              ClientRequest.setHeader('x-document-operation', expiringToken!),
            ),
          )
          const expired = yield* response.text.pipe(
            Effect.flatMap(
              Schema.decodeEffect(
                Schema.fromJsonString(
                  Schema.TaggedStruct('Expired', { protocol: Schema.Literal(1) }),
                ),
              ),
            ),
          )
          assert.strictEqual(expired._tag, 'Expired')
          assert.strictEqual(
            (yield* send('complete', expiringToken).pipe(Effect.flatMap(reply))).error?.reason,
            'expired',
          )
          assert.strictEqual(
            (yield* send('complete', 'not-issued').pipe(Effect.flatMap(reply))).error?.reason,
            'revoked',
          )
        }),
      ),
  )
  for (const [name, bytes, reason] of [
    ['oversize', new TextEncoder().encode('x'.repeat(300)), 'oversize'],
    ['malformed', new TextEncoder().encode('not-json\n'), 'protocol'],
    ['truncated', new TextEncoder().encode('{"protocol":1}'), 'protocol'],
    ['invalid UTF-8', new Uint8Array([255, 10]), 'protocol'],
  ] as const)
    it.live(`incrementally rejects ${name} control bytes from an actual HTTP server`, () =>
      run(
        Effect.gen(function* () {
          const model = yield* LanguageModel.make({
            generateText: () => Effect.die('No provider'),
            streamText: () => Stream.die('No provider'),
          })
          const runtime = yield* HarnessRuntime.make().pipe(
            Effect.provideService(LanguageModel.LanguageModel, model),
          )
          const chunks: Uint8Array[] = []
          for (let i = 0; i < bytes.length; i += 7) chunks.push(bytes.subarray(i, i + 7))
          yield* HttpServer.serveEffect(
            Effect.succeed(ServerResponse.stream(Stream.fromIterable(chunks))),
          )
          const endpoint = yield* HttpServer.addressFormattedWith(Effect.succeed)
          const backend = yield* Http.make(runtime.backend, {
            ...resources,
            maxFrameBytes: 256,
            endpoint,
            identity: 'malformed-server',
          })
          const harness = yield* Harness.make.pipe(Effect.provideService(HarnessBackend, backend))
          const conversation = yield* harness.root
          const error = yield* Remote.snapshot(conversation, old, target).pipe(Effect.flip)
          assert.strictEqual(error._tag, 'RemoteDocumentError')
          if (error._tag === 'RemoteDocumentError') assert.strictEqual(error.reason, reason)
        }),
      ),
    )
  it.live('closed client Scope rejects operations before reading definitions', () =>
    run(
      Effect.gen(function* () {
        const { runtime, endpoint } = yield* fixture()
        const owner = yield* Scope.make()
        const backend = yield* Http.make(runtime.backend, {
          ...resources,
          endpoint,
          identity: 'test-remote-owner',
        }).pipe(Effect.provideService(Scope.Scope, owner))
        const harness = yield* Harness.make.pipe(Effect.provideService(HarnessBackend, backend))
        const conversation = yield* harness.root
        yield* Scope.close(owner, Exit.void)
        let reads = 0
        const stale = {
          ...old,
          get definition() {
            reads++
            return old.definition
          },
        }
        const error = yield* Remote.snapshot(conversation, stale, target).pipe(Effect.flip)
        assert.strictEqual(error._tag, 'RemoteDocumentError')
        if (error._tag === 'RemoteDocumentError') assert.strictEqual(error.reason, 'closed')
        assert.strictEqual(reads, 0)
      }),
    ),
  )
  it.live('remote retirement stops one incarnation and reacquisition gets a fresh epoch', () =>
    run(
      Effect.gen(function* () {
        const { runtime, conversation } = yield* fixture()
        const pull = yield* Remote.watchFrames(conversation, current, target).pipe(
          Stream.rechunk(1),
          Stream.toPull,
        )
        const first = (yield* pull)[0]
        yield* Session.commit(runtime.session, (tx) =>
          Effect.gen(function* () {
            yield* Transaction.retireDocument(tx, old, target)
            yield* Transaction.ensureDocument(tx, old, target)
          }),
        )
        const retired = (yield* pull)[0]
        assert.strictEqual(retired._tag, 'Retired')
        assert.strictEqual(retired.incarnation, first.incarnation)
        assert.isTrue(Exit.isFailure(yield* pull.pipe(Effect.exit)))
        const fresh = Option.getOrThrow(
          yield* Remote.watchFrames(conversation, current, target).pipe(Stream.runHead),
        )
        assert.notStrictEqual(fresh.incarnation, first.incarnation)
        assert.notStrictEqual(fresh.epoch, first.epoch)
      }),
    ),
  )
  it.live('reads current/history with client codecs, original versions and no native writes', () =>
    run(
      Effect.gen(function* () {
        const { runtime, conversation, at } = yield* fixture()
        const storage = yield* Storage.Storage
        const before = yield* storage.scanEntries({ conversationId: root }).pipe(Stream.runCollect)
        const read = Option.getOrThrow(
          yield* Remote.snapshot(conversation, serviceDocument, target).pipe(
            Effect.provideService(Label, 'client'),
          ),
        )
        assert.deepEqual(read.value, { count: 4, label: 'client' })
        assert.strictEqual(read.storedVersion, 1)
        assert.strictEqual(read.effectiveVersion, 3)
        const historical = Option.getOrThrow(
          yield* Remote.snapshotAsOf(conversation, current, target, at.id),
        )
        assert.deepEqual(historical.value, { count: 4 })
        assert.strictEqual(historical.record.id, read.record.id)
        assert.deepEqual(
          Option.getOrThrow(yield* Session.snapshot(runtime.session, old, target)).value,
          { old: 4 },
        )
        assert.deepEqual(
          yield* storage.scanEntries({ conversationId: root }).pipe(Stream.runCollect),
          before,
        )
        Object.assign(read.value, { count: 999 })
        assert.deepEqual(
          Option.getOrThrow(yield* Remote.snapshot(conversation, current, target)).value,
          { count: 4 },
        )
      }),
    ),
  )
  it.live('watches upgrade metadata and subsequent updates over a genuine mounted stream', () =>
    run(
      Effect.gen(function* () {
        const { runtime, conversation } = yield* fixture()
        const pull = yield* Remote.watchFrames(conversation, current, target).pipe(
          Stream.rechunk(1),
          Stream.toPull,
        )
        const first = (yield* pull)[0]
        assert.strictEqual(first._tag, 'Snapshot')
        yield* Session.commit(runtime.session, (tx) =>
          Transaction.ensureDocument(tx, current, target),
        )
        const second = (yield* pull)[0]
        assert.strictEqual(second._tag, 'Replace')
        if (second._tag !== 'Retired') {
          assert.strictEqual(second.value.storedVersion, 3)
          assert.deepEqual(second.value.value, { count: 4 })
        }
        yield* Session.commit(runtime.session, (tx) =>
          Transaction.updateDocument(tx, current, target, () => ({ count: 9 })),
        )
        const third = (yield* pull)[0]
        if (third._tag !== 'Retired') assert.deepEqual(third.value.value, { count: 9 })
      }),
    ),
  )
  it.live(
    'held client decode does not block commits and pending overflow rebases to newest state',
    () =>
      run(
        Effect.gen(function* () {
          const { runtime, conversation } = yield* fixture()
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const gotLatest = yield* Deferred.make<void>()
          let first = true
          const fiber = yield* Remote.watchWith(
            conversation,
            current,
            target,
            (frame) =>
              Effect.gen(function* () {
                if (first) {
                  first = false
                  yield* Deferred.succeed(entered, undefined)
                  yield* Deferred.await(release)
                } else {
                  assert.strictEqual(frame._tag, 'Rebase')
                  if (frame._tag !== 'Retired') assert.deepEqual(frame.value.value, { count: 12 })
                  yield* Deferred.succeed(gotLatest, undefined)
                }
              }),
            { capacity: 1 },
          ).pipe(Stream.runDrain, Effect.forkChild)
          yield* Deferred.await(entered)
          for (let value = 5; value <= 12; value++)
            yield* Session.commit(runtime.session, (tx) =>
              Transaction.setDocument(tx, old, target, { old: value }),
            )
          yield* Deferred.succeed(release, undefined)
          yield* Deferred.await(gotLatest)
          yield* Fiber.interrupt(fiber)
          assert.strictEqual((yield* Native.get(runtime.session)).documentMounts.size, 0)
        }),
      ),
  )
})
