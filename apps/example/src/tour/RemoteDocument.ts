/** A real HTTP read/watch client with no ambient HarnessRuntime or Storage requirement. */
import * as NodeHttpServer from '@effect/platform-node/NodeHttpServer'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as HttpServer from 'effect/http/HttpServer'
import * as Document from 'effect-harness/Document'
import * as Harness from 'effect-harness/Harness'
import { HarnessBackend } from 'effect-harness/HarnessBackend'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Remote from 'effect-harness/RemoteDocument'
import * as Http from 'effect-harness/RemoteDocumentHttp'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Transaction from 'effect-harness/Transaction'

const root = Record.ROOT_CONVERSATION_ID
const target = { scope: { _tag: 'conversation', conversationId: root } } satisfies Document.Target
const old = Document.define({
  kind: 'example.remote.counter',
  version: 1,
  scope: 'conversation',
  history: 'rewindable',
  fork: 'asOf',
  schema: Schema.Struct({ old: Schema.Natural }),
  initial: () => ({ old: 4 }),
})
const upgraded = Document.define({
  kind: old.definition.kind,
  version: 3,
  scope: 'conversation',
  history: 'rewindable',
  fork: 'asOf',
  schema: Schema.Struct({ count: Schema.Natural }),
  initial: () => ({ count: 0 }),
  migrations: {
    1: (value) =>
      Schema.decodeUnknownEffect(old.definition.schema)(value).pipe(
        Effect.map(({ old }) => ({ count: old })),
      ),
  },
})
export const program = Effect.scoped(
  Effect.gen(function* () {
    const model = yield* LanguageModel.make({
      generateText: () => Effect.die('Reads do not run a provider'),
      streamText: () => Stream.die('Reads do not run a provider'),
    })
    const runtime = yield* HarnessRuntime.make().pipe(
      Effect.provideService(LanguageModel.LanguageModel, model),
    )
    const at = yield* Session.commit(runtime.session, (tx) =>
      Effect.gen(function* () {
        yield* Transaction.ensureRoot(tx)
        yield* Transaction.ensureDocument(tx, old, target)
        return yield* Transaction.appendEntry(tx, root, { kind: 'example.remote.cutoff' })
      }),
    )
    // Explicit budgets apply to control messages and document frames. Silence expires a lease.
    const resources = { maxFrameBytes: 65536, heartbeatIntervalMs: 100, leaseTimeoutMs: 2000 }
    const handler = yield* Http.makeHandler(runtime.backend, {
      ...resources,
      identity: 'example-owner',
    })
    yield* HttpServer.serveEffect(handler)
    const endpoint = yield* HttpServer.addressFormattedWith(Effect.succeed)
    const backend = yield* Http.make(runtime.backend, {
      ...resources,
      endpoint,
      identity: 'example-owner',
    })
    const harness = yield* Harness.make.pipe(Effect.provideService(HarnessBackend, backend))
    const conversation = yield* harness.root
    const read = Option.getOrThrow(yield* Remote.snapshot(conversation, upgraded, target))
    const historical = Option.getOrThrow(
      yield* Remote.snapshotAsOf(conversation, upgraded, target, at.id),
    )
    const pull = yield* Remote.watchFrames(conversation, upgraded, target).pipe(
      Stream.rechunk(1),
      Stream.toPull,
    )
    const initial = (yield* pull)[0]
    yield* Session.commit(runtime.session, (tx) => Transaction.ensureDocument(tx, upgraded, target))
    const changed = (yield* pull)[0]
    yield* Session.commit(runtime.session, (tx) => Transaction.retireDocument(tx, upgraded, target))
    const retired = (yield* pull)[0]
    // Client migrations do not rewrite v1. Only the explicit acquisition above persists v3.
    // Connected cancellation joins decoder cleanup before ACK; partitions cannot prove remote cleanup.
    return {
      readVersion: read.storedVersion,
      effectiveVersion: read.effectiveVersion,
      value: read.value,
      historical: historical.value,
      frames: [initial._tag, changed._tag, retired._tag],
      changedVersion: changed._tag === 'Retired' ? undefined : changed.value.storedVersion,
    }
  }),
).pipe(Effect.provide([NodeHttpServer.layerTest, Storage.layerMemory]))
