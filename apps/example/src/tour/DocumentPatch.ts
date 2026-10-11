import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import * as Document from 'effect-harness/Document'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as StorageFormat from 'effect-harness/StorageFormat'
import * as Transaction from 'effect-harness/Transaction'

export class DecodeLabel extends Context.Service<DecodeLabel, string>()(
  'example/DocumentPatch/DecodeLabel',
) {}
export class EncodeLabel extends Context.Service<EncodeLabel, string>()(
  'example/DocumentPatch/EncodeLabel',
) {}
const codec = Schema.Struct({
  storedCount: Schema.Natural,
  items: Schema.Array(Schema.String),
}).pipe(
  Schema.decodeTo(
    Schema.Struct({
      count: Schema.Natural,
      items: Schema.Array(Schema.String),
      label: Schema.String,
    }),
    {
      decode: SchemaGetter.transformEffect(({ storedCount, items }) =>
        DecodeLabel.pipe(Effect.map((label) => ({ count: storedCount, items, label }))),
      ),
      encode: SchemaGetter.transformEffect(({ count, items }) =>
        EncodeLabel.pipe(Effect.as({ storedCount: count, items })),
      ),
    },
  ),
)
const document = Document.define({
  kind: 'example.document.patch',
  version: 1,
  scope: 'session',
  schema: codec,
  initial: () => ({ count: 0, items: ['a', 'c'], label: 'initial' }),
  // Count excludes the candidate commit: base, delta, delta, base.
  checkpointWhen: (_value, _operations, { deltasSinceBase }) => deltasSinceBase >= 2,
})
const target = { scope: { _tag: 'session' } } satisfies Document.Target

/** Patch paths refer to encoded fields; the checkpoint predicate receives decoded values. */
export const program = Effect.scoped(
  Effect.gen(function* () {
    const session = yield* Session.make()
    yield* Session.commit(session, (tx) => Transaction.ensureDocument(tx, document, target))
    yield* Session.commit(session, (tx) =>
      tx.pipe(
        Transaction.patchDocument(document, target, [
          ['set', ['storedCount'], 1],
          ['splice', ['items'], 1, 0, ['b']],
        ]),
      ),
    )
    const first = Option.getOrThrow(yield* Session.snapshot(session, document, target))
    yield* Session.commit(session, (tx) =>
      Transaction.updateDocument(tx, document, target, (value) => ({ ...value, count: 2 })),
    )
    yield* Session.commit(session, (tx) =>
      Transaction.patchDocument(tx, document, target, [['set', ['storedCount'], 3]]),
    )
    const checkpoint = Option.getOrThrow(yield* Session.snapshot(session, document, target))
    return { first, checkpoint, format: yield* StorageFormat.inspect }
  }),
).pipe(
  Effect.provide(Storage.layerMemory),
  Effect.provideService(DecodeLabel, 'decoded with a service'),
  Effect.provideService(EncodeLabel, 'encoded with a service'),
)

import * as NodeRuntime from '@effect/platform-node/NodeRuntime'
import { pathToFileURL } from 'node:url'

// Run after the shared build: node apps/example/dist/tour/DocumentPatch.js
if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href)
  NodeRuntime.runMain(program.pipe(Effect.tap((result) => Effect.log(result))))
