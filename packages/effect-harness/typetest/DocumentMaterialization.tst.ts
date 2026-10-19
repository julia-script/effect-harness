import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import type * as Layer from 'effect/Layer'
import type * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import type * as Stream from 'effect/Stream'
import type * as Conversation from 'effect-harness/Conversation'
import type { HarnessError } from 'effect-harness/HarnessError'
import type * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import type * as Record from 'effect-harness/Record'
import * as Document from 'effect-harness/Document'
// Source mappings let this slice be checked before the shared build.
import * as Materialization from 'effect-harness/DocumentMaterialization'
import * as Session from 'effect-harness/Session'
import * as Client from 'effect-harness/Conversation'

class DecodeLabel extends Context.Service<DecodeLabel, string>()(
  'typetest/materialization/DecodeLabel',
) {}
class EncodeLabel extends Context.Service<EncodeLabel, string>()(
  'typetest/materialization/EncodeLabel',
) {}
const encoded = Schema.Struct({ count: Schema.Natural })
const codec = encoded.pipe(
  Schema.decodeTo(Schema.Struct({ count: Schema.Natural, label: Schema.String }), {
    decode: SchemaGetter.transformEffect(({ count }) =>
      DecodeLabel.pipe(Effect.map((label) => ({ count, label }))),
    ),
    encode: SchemaGetter.transformEffect(({ count }) => EncodeLabel.pipe(Effect.as({ count }))),
  }),
)
const document = Document.define({
  kind: 'typed.materialization',
  version: 3,
  scope: 'session',
  schema: codec,
  initial: () => ({ count: 0, label: '' }),
  migrations: { 1: Effect.succeed },
})
const target = { scope: { _tag: 'session' } } satisfies Document.Target
declare const stored: Record.StoredDocument
declare const session: Session.Session
declare const conversation: Conversation.Conversation
declare const entry: Record.EntryId
declare const runtimeLayer: Layer.Layer<HarnessRuntime.HarnessRuntime>
const snapshotSchema = Materialization.SnapshotSchema(codec)

test('detached materialization exposes decoded value and original versus effective version', () => {
  expect(Materialization.materialize(document, stored)).type.toBe<
    Effect.Effect<Materialization.Snapshot<typeof codec>, Session.Failure, DecodeLabel>
  >()
  expect<Materialization.Snapshot<typeof codec>['value']>().type.toBe<{
    readonly count: number
    readonly label: string
  }>()
  expect<Materialization.Snapshot<typeof codec>['storedVersion']>().type.toBe<number>()
  expect<Materialization.Snapshot<typeof codec>['effectiveVersion']>().type.toBe<number>()
  expect<typeof snapshotSchema.Type>().type.toBe<Materialization.Snapshot<typeof codec>>()
  expect<typeof snapshotSchema.DecodingServices>().type.toBe<DecodeLabel>()
})

test('Session reads retain only decoder requirements in both calling forms', () => {
  expect(Session.materializedSnapshot(session, document, target)).type.toBe<
    Effect.Effect<
      Option.Option<Materialization.Snapshot<typeof codec>>,
      Session.Failure,
      DecodeLabel
    >
  >()
  expect(session.pipe(Session.materializedSnapshot(document, target))).type.toBe<
    Effect.Effect<
      Option.Option<Materialization.Snapshot<typeof codec>>,
      Session.Failure,
      DecodeLabel
    >
  >()
  expect(Session.materializedSnapshotAsOf(session, document, target, entry)).type.toBe<
    Effect.Effect<
      Option.Option<Materialization.Snapshot<typeof codec>>,
      Session.Failure,
      DecodeLabel
    >
  >()
  expect(session.pipe(Session.materializedSnapshotAsOf(document, target, entry))).type.toBe<
    Effect.Effect<
      Option.Option<Materialization.Snapshot<typeof codec>>,
      Session.Failure,
      DecodeLabel
    >
  >()
  expect(Session.watchMaterialized(session, document, target)).type.toBe<
    Stream.Stream<Materialization.Snapshot<typeof codec>, Session.Failure, DecodeLabel>
  >()
  expect(session.pipe(Session.watchMaterialized(document, target))).type.toBe<
    Stream.Stream<Materialization.Snapshot<typeof codec>, Session.Failure, DecodeLabel>
  >()
  const read = Session.materializedSnapshot(session, document, target)
  expect(read).type.not.toBeAssignableTo<Effect.Effect<unknown, Session.Failure>>()
  expect(read.pipe(Effect.provideService(EncodeLabel, 'unrelated'))).type.not.toBeAssignableTo<
    Effect.Effect<unknown, Session.Failure>
  >()
  expect(read.pipe(Effect.provideService(DecodeLabel, 'decoded'))).type.toBe<
    Effect.Effect<Option.Option<Materialization.Snapshot<typeof codec>>, Session.Failure>
  >()
})

test('Conversation bridge retains the owning runtime and target decoder requirements', () => {
  expect(Client.materializedSnapshot(conversation, document, target)).type.toBe<
    Effect.Effect<
      Option.Option<Materialization.Snapshot<typeof codec>>,
      HarnessError | Session.Failure,
      HarnessRuntime.HarnessRuntime | DecodeLabel
    >
  >()
  expect(Client.materializedSnapshot(document, target)(conversation)).type.toBe<
    Effect.Effect<
      Option.Option<Materialization.Snapshot<typeof codec>>,
      HarnessError | Session.Failure,
      HarnessRuntime.HarnessRuntime | DecodeLabel
    >
  >()
  expect(Client.materializedSnapshotAsOf(conversation, document, target, entry)).type.toBe<
    Effect.Effect<
      Option.Option<Materialization.Snapshot<typeof codec>>,
      HarnessError | Session.Failure,
      HarnessRuntime.HarnessRuntime | DecodeLabel
    >
  >()
  expect(Client.watchMaterialized(conversation, document, target)).type.toBe<
    Stream.Stream<
      Materialization.Snapshot<typeof codec>,
      HarnessError | Session.Failure,
      HarnessRuntime.HarnessRuntime | DecodeLabel
    >
  >()
  const read = Client.materializedSnapshot(conversation, document, target).pipe(
    Effect.provide(runtimeLayer),
  )
  expect(read).type.toBe<
    Effect.Effect<
      Option.Option<Materialization.Snapshot<typeof codec>>,
      HarnessError | Session.Failure,
      DecodeLabel
    >
  >()
  expect(read.pipe(Effect.provideService(DecodeLabel, 'label'))).type.toBe<
    Effect.Effect<
      Option.Option<Materialization.Snapshot<typeof codec>>,
      HarnessError | Session.Failure
    >
  >()
})
