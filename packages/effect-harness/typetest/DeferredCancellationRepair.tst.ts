import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import type * as Scope from 'effect/Scope'
import * as Model from 'effect-harness/Model'

class Config extends Context.Service<Config, string>()('DeferredCancellationRepair/types/Config') {}
class Decoder extends Context.Service<Decoder, string>()(
  'DeferredCancellationRepair/types/Decoder',
) {}
class Encoder extends Context.Service<Encoder, string>()(
  'DeferredCancellationRepair/types/Encoder',
) {}
const codec = Schema.String.pipe(
  Schema.decodeTo(Schema.String, {
    decode: SchemaGetter.transformEffect((value) => Decoder.pipe(Effect.as(value))),
    encode: SchemaGetter.transformEffect((value) => Encoder.pipe(Effect.as(value))),
  }),
)
const callbacks = {
  handle: codec,
  start: () => Effect.succeed({ _tag: 'Deferred' as const, handle: 'h', nextPollAt: 1 }),
  poll: () => Effect.succeed({ _tag: 'Pending' as const, nextPollAt: 2 }),
  cancel: () =>
    Effect.gen(function* () {
      yield* Config
      yield* Effect.acquireRelease(Effect.void, () => Effect.void)
    }),
}
const construction = Model.makeDeferred<typeof codec, Config | Scope.Scope>(callbacks)
declare const adapter: Model.Deferred

test('callback and handle codec requirements remain on construction; saved cancellation adds no ambient requirement', () => {
  expect(construction).type.toBe<
    Effect.Effect<Model.Deferred, never, Config | Scope.Scope | Decoder | Encoder>
  >()
  expect(adapter.cancel('h')).type.toBe<Effect.Effect<void, Model.DeferredError>>()
  expect(adapter.cancel('h', Context.make(Config, 'saved'))).type.toBe<
    Effect.Effect<void, Model.DeferredError>
  >()
  expect(adapter.cancel).type.not.toBeCallableWith('h', 'unvalidated')
  expect(Model.makeDeferred<typeof codec, never>).type.not.toBeCallableWith(callbacks)
  expect(Model.makeDeferred<typeof codec, Config | Scope.Scope>).type.not.toBeCallableWith({
    ...callbacks,
    cancel: () => Effect.fail('undeclared failure'),
  })
})
