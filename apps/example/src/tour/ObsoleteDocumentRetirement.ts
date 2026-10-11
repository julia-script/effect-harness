import { NodeRuntime } from '@effect/platform-node'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import * as Document from 'effect-harness/Document'
import * as DocumentRetirement from 'effect-harness/DocumentRetirement'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Transaction from 'effect-harness/Transaction'

class LegacyLabels extends Context.Service<LegacyLabels, string>()('example/LegacyLabels') {}
const legacy = Document.define({
  kind: 'example.obsolete',
  version: 1,
  scope: 'session',
  schema: Schema.Struct({ calls: Schema.Natural }),
  initial: () => ({ calls: 2 }),
})
const current = Document.define({
  ...legacy.definition,
  version: 3,
  schema: legacy.definition.schema.pipe(
    Schema.decodeTo(Schema.Struct({ calls: Schema.Natural, label: Schema.String }), {
      decode: SchemaGetter.transformEffect(({ calls }) =>
        LegacyLabels.pipe(Effect.map((label) => ({ calls, label }))),
      ),
      encode: SchemaGetter.transform(({ calls }) => ({ calls })),
    }),
  ),
  initial: () => ({ calls: 0, label: '' }),
})
const target = { scope: { _tag: 'session' } } satisfies Document.Target

// After integration build: node apps/example/dist/tour/ObsoleteDocumentRetirement.js
const program = Effect.gen(function* () {
  const storage = yield* Storage.Storage
  const session = yield* Session.make()
  const before = yield* Session.commit(session, (tx) =>
    Transaction.ensureDocument(tx, legacy, target),
  )
  const raw = Option.getOrThrow(yield* storage.document(before.record.id))
  const selected = DocumentRetirement.target(raw)
  // The new definition has no migration; its decoding service is deliberately absent.
  // Cleanup needs only its metadata policy and verifies the live version independently.
  yield* Session.commit(session, (tx) =>
    Transaction.retireDocumentMetadata(tx, current.definition, selected),
  )
  const absent = Option.isNone(yield* storage.findDocument(selected.address))
  const contentAbsent = Option.isNone(yield* storage.document(selected.id))
  if (!absent || !contentAbsent || raw.version !== 1 || raw.value.calls !== 2)
    return yield* Effect.die('Obsolete document retirement contract failed')
  yield* Effect.log('Obsolete document retired without its decoding service', {
    absent,
    storedVersion: raw.version,
    previousCalls: raw.value.calls,
  })
}).pipe(Effect.scoped, Effect.provide(Storage.layerMemory))

NodeRuntime.runMain(program)
