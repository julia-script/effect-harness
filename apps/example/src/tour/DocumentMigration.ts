import { NodeRuntime } from '@effect/platform-node'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Document from 'effect-harness/Document'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Transaction from 'effect-harness/Transaction'

const Legacy = Schema.Struct({ calls: Schema.Natural })
const old = Document.define({
  kind: 'example.migration',
  version: 1,
  scope: 'session',
  schema: Legacy,
  initial: () => ({ calls: 2 }),
})
const current = Document.define({
  kind: 'example.migration',
  version: 3,
  scope: 'session',
  schema: Schema.Struct({ count: Schema.Natural }),
  initial: () => ({ count: 0 }),
  // Version 1 maps directly to version 3. There is no implicit version 2 chain.
  migrations: {
    1: (value) =>
      Schema.decodeUnknownEffect(Legacy)(value).pipe(Effect.map(({ calls }) => ({ count: calls }))),
  },
})
const target = { scope: { _tag: 'session' } } satisfies Document.Target

// Run after building: node apps/example/dist/tour/DocumentMigration.js
const program = Effect.gen(function* () {
  yield* Effect.scoped(
    Effect.gen(function* () {
      const session = yield* Session.make()
      yield* Session.commit(session, (tx) => Transaction.ensureDocument(tx, old, target))
    }),
  )
  const session = yield* Session.make()
  const upgraded = yield* Session.commit(session, (tx) =>
    Transaction.ensureDocument(tx, current, target),
  )
  yield* Effect.log('Migrated document', upgraded.version, upgraded.value)
}).pipe(Effect.scoped, Effect.provide(Storage.layerMemory))

NodeRuntime.runMain(program)
