import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import * as Model from 'effect-harness/Model'
import * as Prepared from 'effect-harness/PreparedRequest'
import type * as HarnessRuntime from 'effect-harness/HarnessRuntime'

class Encoder extends Context.Service<Encoder, number>()('prepared/types/Encoder') {}
class Decoder extends Context.Service<Decoder, number>()('prepared/types/Decoder') {}
class Owner extends Context.Service<Owner, number>()('prepared/types/Owner') {}
class NativeConfig extends Context.Service<NativeConfig, number>()('prepared/types/NativeConfig') {}
class Failure extends Schema.TaggedError<Failure>()('Failure', {}) {}
const codec = Schema.String.pipe(
  Schema.decodeTo(Schema.Finite, {
    decode: SchemaGetter.transformEffect((value) => Decoder.pipe(Effect.as(Number(value)))),
    encode: SchemaGetter.transformEffect((value) => Encoder.pipe(Effect.as(String(value)))),
  }),
)

test('configuration captures concrete callback and codec services; compiled errors remain semantic', () => {
  const acquisition = Model.makeConfiguration<typeof codec, Owner, Failure>({
    id: 'native/config',
    version: 1,
    schema: codec,
    tags: [NativeConfig],
    capture: () => Owner,
    restore: (value) => Owner.pipe(Effect.as(Context.make(NativeConfig, value))),
  })
  expect(acquisition).type.toBe<
    Effect.Effect<Model.Configuration, never, Owner | Encoder | Decoder>
  >()
  const compiled = Effect.flatMap(acquisition, (config) => config.encode(Context.empty()))
  expect(compiled).type.toBe<
    Effect.Effect<Schema.Json, Model.ModelError, Owner | Encoder | Decoder>
  >()
  expect(Effect.map(Prepared.PreparedRequest, (service) => service.current)).type.toBe<
    Effect.Effect<Prepared.Snapshot, never, Prepared.PreparedRequest>
  >()
  expect(Model.emptyConfiguration.restore(null)).type.toBe<
    Effect.Effect<Context.Context<never>, Model.ModelError>
  >()
  expect<HarnessRuntime.Options['nativeModel']>().type.toBe<Prepared.Binding | undefined>()
})
