import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import type * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import type * as Scope from 'effect/Scope'
import type * as Stream from 'effect/Stream'
import type * as HttpClient from 'effect/http/HttpClient'
import type * as Conversation from 'effect-harness/Conversation'
import * as Document from 'effect-harness/Document'
import type * as Materialization from 'effect-harness/DocumentMaterialization'
import type { HarnessBackendService } from 'effect-harness/HarnessBackend'
import * as Remote from 'effect-harness/RemoteDocument'
import * as Http from 'effect-harness/RemoteDocumentHttp'
import type { RemoteDocumentError } from 'effect-harness/RemoteDocumentError'
import type * as Record from 'effect-harness/Record'
class Decode extends Context.Service<Decode, string>()('remote-typing/Decode') {}
class Encode extends Context.Service<Encode, string>()('remote-typing/Encode') {}
class Callback extends Context.Service<Callback, number>()('remote-typing/Callback') {}
const codec = Schema.Struct({ count: Schema.Natural }).pipe(
  Schema.decodeTo(Schema.Struct({ count: Schema.Natural, label: Schema.String }), {
    decode: SchemaGetter.transformEffect((value) =>
      Decode.pipe(Effect.map((label) => ({ ...value, label }))),
    ),
    encode: SchemaGetter.transformEffect((value) => Encode.pipe(Effect.as({ count: value.count }))),
  }),
)
const document = Document.define({
  kind: 'remote.types',
  version: 1,
  scope: 'session',
  schema: codec,
  initial: () => ({ count: 0, label: '' }),
})
const target = { scope: { _tag: 'session' } } satisfies Document.Target
declare const conversation: Conversation.Conversation
declare const at: Record.EntryId
declare const backend: HarnessBackendService
declare const frame: Remote.MaterializedFrame<typeof codec>
const options = {
  endpoint: 'http://localhost:1',
  identity: 'typing',
  maxFrameBytes: 4096,
  heartbeatIntervalMs: 50,
  leaseTimeoutMs: 500,
}
test('remote read/history expose decoding only, without runtime or storage services', () => {
  expect(Remote.snapshot(conversation, document, target)).type.toBe<
    Effect.Effect<Option.Option<Materialization.Snapshot<typeof codec>>, Remote.Failure, Decode>
  >()
  expect(Remote.snapshotAsOf(conversation, document, target, at)).type.toBe<
    Effect.Effect<Option.Option<Materialization.Snapshot<typeof codec>>, Remote.Failure, Decode>
  >()
  expect(Remote.snapshot(conversation, document, target)).type.not.toBeAssignableTo<
    Effect.Effect<Option.Option<Materialization.Snapshot<typeof codec>>, Remote.Failure>
  >()
})
test('watch and reducer preserve values without an encoder requirement', () => {
  expect(Remote.watch(conversation, document, target)).type.toBe<
    Stream.Stream<Materialization.Snapshot<typeof codec>, Remote.Failure, Decode>
  >()
  expect(Remote.watchFrames(conversation, document, target)).type.toBe<
    Stream.Stream<Remote.MaterializedFrame<typeof codec>, Remote.Failure, Decode>
  >()
  expect(Remote.reduce(undefined, frame)).type.toBe<
    Effect.Effect<Remote.MaterializedState<typeof codec>, RemoteDocumentError>
  >()
  const callback = Remote.watchWith(conversation, document, target, () =>
    Callback.pipe(
      Effect.filterOrFail(
        (value) => value > 0,
        () => 'callback-error' as const,
      ),
    ),
  )
  expect(callback).type.toBe<
    Stream.Stream<number, Remote.Failure | 'callback-error', Decode | Callback>
  >()
})
test('HTTP construction captures its client and Scope; resource options are required', () => {
  expect(Http.make(backend, options)).type.toBe<
    Effect.Effect<HarnessBackendService, RemoteDocumentError, HttpClient.HttpClient | Scope.Scope>
  >()
  expect(Http.make).type.not.toBeCallableWith(backend, {
    endpoint: 'http://localhost:1',
    identity: 'typing',
  })
})
