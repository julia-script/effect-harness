import { NodeRuntime } from '@effect/platform-node'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Document from 'effect-harness/Document'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Transaction from 'effect-harness/Transaction'

const legacySchema = Schema.Struct({ calls: Schema.Natural })
const legacy = Document.define({
  kind: 'example.materialization',
  version: 1,
  scope: 'session',
  schema: legacySchema,
  initial: () => ({ calls: 2 }),
})
const current = Document.define({
  ...legacy.definition,
  version: 3,
  schema: Schema.Struct({ count: Schema.Natural }),
  initial: () => ({ count: 0 }),
  migrations: {
    1: (value) =>
      Schema.decodeUnknownEffect(legacySchema)(value).pipe(
        Effect.map(({ calls }) => ({ count: calls })),
      ),
  },
})
const target = { scope: { _tag: 'session' } } satisfies Document.Target

// After integration builds the public subpaths: node apps/example/dist/tour/DocumentMaterialization.js
NodeRuntime.runMain(
  Effect.gen(function* () {
    const session = yield* Session.make()
    yield* Session.commit(session, (tx) => Transaction.ensureDocument(tx, legacy, target))
    const exact = Option.getOrThrow(yield* Session.snapshot(session, legacy, target))
    yield* Effect.log('Exact stored revision', exact.version, exact.value)
    const materialized = Option.getOrThrow(
      yield* Session.materializedSnapshot(session, current, target),
    )
    yield* Effect.log('In-memory read', {
      storedVersion: materialized.storedVersion,
      effectiveVersion: materialized.effectiveVersion,
      value: materialized.value,
    })
    // The exact v1 value is still readable: the read above did not persist an upgrade.
    yield* Session.snapshot(session, legacy, target)
    const acquired = yield* Session.commit(session, (tx) =>
      Transaction.ensureDocument(tx, current, target),
    )
    yield* Effect.log('Durable acquisition', acquired.version, acquired.value)
  }).pipe(Effect.scoped, Effect.provide(Storage.layerMemory)),
)
