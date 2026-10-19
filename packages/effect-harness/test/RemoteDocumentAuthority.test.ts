import * as TemporaryStorage from './fixtures/Storage.js'
import { assert, describe, it } from '@effect/vitest'
import * as NodeHttpServer from '@effect/platform-node/NodeHttpServer'
import * as Deferred from 'effect/Deferred'
import * as Context from 'effect/Context'
import * as Cause from 'effect/Cause'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as HttpServer from 'effect/http/HttpServer'
import * as HttpClient from 'effect/http/HttpClient'
import * as ClientRequest from 'effect/http/HttpClientRequest'
import type * as Conversation from 'effect-harness/Conversation'
import * as Document from 'effect-harness/Document'
import * as Harness from 'effect-harness/Harness'
import { HarnessBackend } from 'effect-harness/HarnessBackend'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Http from 'effect-harness/RemoteDocumentHttp'
import * as Remote from 'effect-harness/RemoteDocument'
import * as Record from 'effect-harness/Record'
import * as Registry from 'effect-harness/Registry'
import * as Session from 'effect-harness/Session'

import * as Task from 'effect-harness/Task'
import { TaskExecution } from 'effect-harness/TaskExecution'
import * as Toolkit from 'effect-harness/Toolkit'
import * as Transaction from 'effect-harness/Transaction'
import { Backend } from '../src/internal/ConversationHost.js'
import * as Native from '../src/internal/SessionRuntime.js'

const httpLayer =
  'Bun' in globalThis
    ? (await import('@effect/platform-bun/BunHttpServer')).layerTest
    : NodeHttpServer.layerTest
const resources = { maxFrameBytes: 65536, heartbeatIntervalMs: 25, leaseTimeoutMs: 1000 }
const root = Record.ROOT_CONVERSATION_ID
const target = { scope: { _tag: 'conversation', conversationId: root } } satisfies Document.Target
const document = Document.define({
  kind: 'remote.authority',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'current',
  schema: Schema.Struct({ count: Schema.Natural }),
  initial: () => ({ count: 0 }),
})
class DecoderLabel extends Context.Service<DecoderLabel, string>()(
  'remote-authority/DecoderLabel',
) {}
describe('authentic remote document authority', () => {
  it.live(
    'real HTTP revocation holds native completion through callback cleanup and rejects stale getters',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const model = yield* LanguageModel.make({
            generateText: () => Effect.die('No provider'),
            streamText: () => Stream.die('No provider'),
          })
          const entered = yield* Deferred.make<Conversation.Conversation>()
          const settle = yield* Deferred.make<void>()
          const settling = yield* Deferred.make<void>()
          const contract = {
            kind: 'remote.bound-parent',
            version: 1,
            input: Schema.Void,
            checkpoint: Schema.Struct({ phase: Schema.Literal('execute') }),
            success: Schema.Void,
            failure: Schema.String,
          } as const
          const program = Task.make({
            ...contract,
            initial: () => ({ phase: 'execute' as const }),
            phases: {
              execute: () =>
                Effect.gen(function* () {
                  const execution = yield* TaskExecution
                  yield* Deferred.succeed(
                    entered,
                    yield* execution.subagent!.conversation(execution.conversationId),
                  )
                  yield* Deferred.await(settle)
                  yield* execution.commit((tx) =>
                    execution.progress(tx, contract, {
                      status: 'completing',
                      outcome: { status: 'completed', result: undefined },
                    }),
                  )
                  yield* Deferred.succeed(settling, undefined)
                }),
            },
          })
          const registry = yield* Registry.make
          yield* registry.install(
            Registry.withTask(
              Registry.toolkit({ id: 'remote-parent', version: 1 }, Toolkit.make()),
              program,
            ),
          )
          const runtime = yield* HarnessRuntime.make({ registry }).pipe(
            Effect.provideService(LanguageModel.LanguageModel, model),
          )
          const local = yield* Harness.make.pipe(
            Effect.provideService(HarnessBackend, runtime.backend),
          )
          const owner = yield* local.root
          yield* Session.commit(runtime.session, (tx) =>
            Transaction.ensureDocument(tx, document, target),
          )
          const foreign = yield* local.create()
          const ref = yield* runtime.scheduler.start(program, undefined, {
            conversationId: owner.id,
          })
          const bound = yield* Deferred.await(entered)
          const copied = yield* Http.makeHandler(
            { ...bound[Backend] },
            { ...resources, identity: 'copied-parent' },
          ).pipe(Effect.exit)
          assert.isTrue(Exit.isFailure(copied))
          if (Exit.isFailure(copied))
            assert.strictEqual(
              Option.getOrThrow(Cause.findErrorOption(copied.cause)).reason,
              'unsupported',
            )
          const copiedMethods = yield* Http.makeHandler(
            { ...bound[Backend], documents: { ...bound[Backend].documents } },
            { ...resources, identity: 'copied-parent' },
          ).pipe(Effect.exit)
          assert.isTrue(Exit.isFailure(copiedMethods))
          if (Exit.isFailure(copiedMethods))
            assert.strictEqual(
              Option.getOrThrow(Cause.findErrorOption(copiedMethods.cause)).reason,
              'unsupported',
            )
          const handler = yield* Http.makeHandler(bound[Backend], {
            ...resources,
            identity: 'authentic-parent',
          })
          yield* HttpServer.serveEffect(handler)
          const endpoint = yield* HttpServer.addressFormattedWith(Effect.succeed)
          const backend = yield* Http.make(runtime.backend, {
            ...resources,
            identity: 'authentic-parent',
            endpoint,
          })
          const remote = yield* Harness.make.pipe(Effect.provideService(HarnessBackend, backend))
          const conversation = yield* remote.root
          let optionReads = 0
          const options = {
            get capacity() {
              optionReads++
              return 1
            },
          }
          const denied = yield* Remote.watch(
            conversation,
            document,
            { scope: { _tag: 'conversation', conversationId: foreign.id } },
            options,
          ).pipe(Stream.runDrain, Effect.exit)
          assert.isTrue(Exit.isFailure(denied))
          assert.strictEqual(optionReads, 0)
          assert.isTrue(Option.isSome(yield* Remote.snapshot(conversation, document, target)))
          const held = yield* Deferred.make<void>()
          const cleanup = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined))
          const outer = yield* Scope.Scope
          const decoderEntered = yield* Deferred.make<void>()
          const decoderCleanup = yield* Deferred.make<void>()
          let decodedCleaned = 0
          const asyncCodec = document.definition.schema.pipe(
            Schema.decodeTo(Schema.Struct({ count: Schema.Natural, label: Schema.String }), {
              decode: SchemaGetter.transformEffect((value) =>
                Effect.gen(function* () {
                  assert.strictEqual(yield* DecoderLabel, 'client-context')
                  assert.notStrictEqual(yield* Scope.Scope, outer)
                  yield* Effect.addFinalizer(() =>
                    Effect.uninterruptible(
                      Deferred.succeed(decoderCleanup, undefined).pipe(
                        Effect.andThen(Deferred.await(release)),
                        Effect.andThen(
                          Effect.sync(() => {
                            decodedCleaned++
                          }),
                        ),
                      ),
                    ),
                  )
                  yield* Deferred.succeed(decoderEntered, undefined)
                  yield* Deferred.await(release)
                  return { ...value, label: 'client-context' }
                }),
              ),
              encode: SchemaGetter.transform((value) => ({ count: value.count })),
            }),
          )
          const asyncDocument = Document.define({
            kind: document.definition.kind,
            version: 1,
            scope: 'conversation',
            history: 'latest',
            fork: 'current',
            schema: asyncCodec,
            initial: () => ({ count: 0, label: '' }),
          })
          const decoder = yield* Remote.snapshot(conversation, asyncDocument, target).pipe(
            Effect.provideService(DecoderLabel, 'client-context'),
            Effect.forkChild,
          )
          yield* Deferred.await(decoderEntered)
          let deliveries = 0
          let cleaned = 0
          const cleanupDefect = { _tag: 'ClientCleanupDefect' }
          const observer = yield* Remote.watchWith(conversation, document, target, () =>
            Effect.gen(function* () {
              assert.notStrictEqual(yield* Scope.Scope, outer)
              yield* Effect.addFinalizer(() =>
                Effect.uninterruptible(
                  Deferred.succeed(cleanup, undefined).pipe(
                    Effect.andThen(Deferred.await(release)),
                    Effect.andThen(
                      Effect.sync(() => {
                        cleaned++
                      }),
                    ),
                    Effect.andThen(Effect.die(cleanupDefect)),
                  ),
                ),
              )
              yield* Deferred.succeed(held, undefined)
              return yield* Effect.never
            }),
          ).pipe(
            Stream.tap(() =>
              Effect.sync(() => {
                deliveries++
              }),
            ),
            Stream.runDrain,
            Effect.forkChild,
          )
          yield* Deferred.await(held)
          const client = yield* HttpClient.HttpClient
          const action = (route: string, token?: string) => {
            let request = ClientRequest.post(`${endpoint}/documents/v1/${route}`).pipe(
              ClientRequest.setHeader('x-document-identity', 'authentic-parent'),
              ClientRequest.bodyText(
                route === 'begin' ? '{"protocol":1,"kind":"Current"}\n' : '{"protocol":1}\n',
              ),
            )
            if (token !== undefined)
              request = ClientRequest.setHeader(request, 'x-document-operation', token)
            return client.execute(request)
          }
          const abandoned = yield* action('begin')
          const abandonedToken = abandoned.headers['x-document-operation']!
          const replySchema = Schema.fromJsonString(
            Schema.Struct({
              _tag: Schema.String,
              error: Schema.optionalKey(Schema.Struct({ reason: Schema.String })),
            }),
          )
          const revoked = yield* client
            .execute(
              ClientRequest.get(`${endpoint}/documents/v1/events`).pipe(
                ClientRequest.setHeader('x-document-identity', 'authentic-parent'),
                ClientRequest.setHeader('x-document-operation', abandonedToken),
              ),
            )
            .pipe(
              Effect.flatMap((response) => response.text),
              Effect.flatMap(Schema.decodeEffect(replySchema)),
              Effect.forkChild,
            )
          yield* Deferred.succeed(settle, undefined)
          yield* Deferred.await(settling)
          yield* Deferred.await(cleanup)
          yield* Deferred.await(decoderCleanup)
          assert.isTrue((yield* runtime.scheduler.inspect(ref.id)).invoking)
          assert.strictEqual(cleaned, 0)
          assert.strictEqual((yield* Fiber.join(revoked))._tag, 'Revoked')
          yield* Effect.sleep(resources.leaseTimeoutMs + resources.heartbeatIntervalMs * 2)
          const lateAck = yield* action('complete', abandonedToken).pipe(
            Effect.flatMap((response) => response.text),
            Effect.flatMap(Schema.decodeEffect(replySchema)),
          )
          assert.isTrue((yield* runtime.scheduler.inspect(ref.id)).invoking)
          yield* Deferred.succeed(release, undefined)
          const observationExit = yield* Fiber.await(observer)
          assert.isTrue(Exit.isFailure(observationExit))
          if (Exit.isFailure(observationExit))
            assert.isTrue(
              observationExit.cause.reasons.some(
                (reason) => reason._tag === 'Die' && reason.defect === cleanupDefect,
              ),
            )
          assert.isTrue(Exit.isFailure(yield* Fiber.await(decoder)))
          assert.strictEqual(decodedCleaned, 1)
          assert.strictEqual(cleaned, 1)
          assert.strictEqual(deliveries, 0)
          assert.strictEqual((yield* runtime.scheduler.wait(ref)).status, 'completed')
          assert.strictEqual(lateAck._tag, 'Failure')
          assert.strictEqual(lateAck.error?.reason, 'expired')
          let getters = 0
          const stale = {
            ...document,
            get definition() {
              getters++
              return document.definition
            },
          }
          assert.isTrue(
            Exit.isFailure(yield* Remote.snapshot(conversation, stale, target).pipe(Effect.exit)),
          )
          assert.strictEqual(getters, 0)
          assert.strictEqual((yield* Native.get(runtime.session)).documentMounts.size, 0)
        }),
      ).pipe(Effect.provide([httpLayer, TemporaryStorage.layer])),
  )
})
