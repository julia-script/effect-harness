import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import type * as Scope from 'effect/Scope'
import * as Task from 'effect-harness/Task'

class Backend extends Context.Service<Backend, number>()('types/TaskScopeLifetime/Backend') {}
class Host extends Context.Service<Host, number>()('types/TaskScopeLifetime/Host') {}
class External extends Context.Service<External, number>()('types/TaskScopeLifetime/External') {}
class Decoder extends Context.Service<Decoder, number>()('types/TaskScopeLifetime/Decoder') {}
class Encoder extends Context.Service<Encoder, number>()('types/TaskScopeLifetime/Encoder') {}
class BuildError extends Schema.TaggedError<BuildError>()('TaskScopeLifetimeBuildError', {}) {}
class CodecBuildError extends Schema.TaggedError<CodecBuildError>()(
  'TaskScopeLifetimeCodecBuildError',
  {},
) {}
const codec = Schema.Finite.pipe(
  Schema.decodeTo(Schema.Finite, {
    decode: SchemaGetter.transformEffect((value: number) => Decoder.pipe(Effect.as(value))),
    encode: SchemaGetter.transformEffect((value: number) => Encoder.pipe(Effect.as(value))),
  }),
)
const definition = Task.make({
  kind: 'types.scope-lifetime',
  version: 1,
  input: codec,
  checkpoint: Schema.Struct({ phase: Schema.Literal('ready') }),
  success: codec,
  failure: Schema.String,
  initial: () => ({ phase: 'ready' as const }),
  phases: {
    ready: () =>
      Effect.gen(function* () {
        yield* Backend
        yield* External
        yield* Effect.addFinalizer(() => Effect.void)
        return yield* Effect.fail('handler' as const)
      }),
  },
})
const backend = Layer.effect(
  Backend,
  Effect.gen(function* () {
    const host = yield* Host
    yield* Effect.acquireRelease(Effect.void, () => Effect.void)
    if (host < 0) return yield* new BuildError()
    return host
  }),
)
const provided = Task.provide(definition, backend)

test('scoped acquisition preserves construction errors and unprovided external/codec inputs', () => {
  expect<Task.Requirements<typeof provided>>().type.toBe<Host | External | Decoder | Encoder>()
  expect<Task.ConstructionError<typeof provided>>().type.toBe<BuildError>()
  expect<Task.Error<typeof provided>>().type.toBe<'handler'>()
  expect(provided.context).type.toBe<
    Effect.Effect<
      Context.Context<Backend | External | Decoder | Encoder>,
      BuildError,
      Host | External | Decoder | Encoder | Scope.Scope
    >
  >()
  expect(Effect.runPromise(Effect.scoped(provided.context))).type.toRaiseError()
  expect(Effect.runPromise(provided.context)).type.toRaiseError()
})

test('chained provisioning accumulates errors and discharges only supplied requirements', () => {
  const codecs = Layer.merge(
    Layer.succeed(Encoder, 1),
    Layer.effect(Decoder, Effect.fail(new CodecBuildError())),
  )
  const complete = provided.pipe(
    Task.provide(codecs),
    Task.provide(Layer.merge(Layer.succeed(Host, 1), Layer.succeed(External, 1))),
  )
  expect<Task.Requirements<typeof complete>>().type.toBe<never>()
  expect<Task.ConstructionError<typeof complete>>().type.toBe<BuildError | CodecBuildError>()
  expect(Effect.scoped(complete.context)).type.toBe<
    Effect.Effect<
      Context.Context<Backend | External | Decoder | Encoder>,
      BuildError | CodecBuildError
    >
  >()
  const unscoped = definition.pipe(Task.provide(Layer.succeed(External, 1)))
  expect<Task.Requirements<typeof unscoped>>().type.toBe<Backend | Decoder | Encoder>()
  expect<Task.ConstructionError<typeof unscoped>>().type.toBe<never>()
})
