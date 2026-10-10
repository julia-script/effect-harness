import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import type * as Scope from 'effect/Scope'
import * as Task from 'effect-harness/Task'
import { TaskExecution, type Access } from 'effect-harness/TaskExecution'
import type { Failure } from 'effect-harness/TaskError'
import type * as Transaction from 'effect-harness/Transaction'
import type * as Document from 'effect-harness/Document'
import type * as Option from 'effect/Option'
import type * as Settings from 'effect-harness/Settings'
import type { TaskError } from 'effect-harness/TaskError'

class First extends Context.Service<First, number>()('types/task/First') {}
class Second extends Context.Service<Second, number>()('types/task/Second') {}
class Abort extends Context.Service<Abort, number>()('types/task/Abort') {}
class Host extends Context.Service<Host, number>()('types/task/Host') {}
const input = Schema.Struct({ count: Schema.Finite })
const checkpoint = Schema.Union([
  Schema.Struct({ phase: Schema.Literal('first'), count: Schema.Finite }),
  Schema.Struct({ phase: Schema.Literal('second'), total: Schema.Finite }),
])
const base = {
  kind: 'types.counter',
  version: 1,
  input,
  checkpoint,
  success: Schema.Finite,
  failure: Schema.Struct({ reason: Schema.String }),
  initial: ({ count }: typeof input.Type): typeof checkpoint.Type => ({
    phase: 'first' as const,
    count,
  }),
}
const self = Task.make({
  ...base,
  phases: {
    first: ({ input: parameters, checkpoint: phase }) =>
      Effect.gen(function* () {
        expect(parameters).type.toBe<typeof input.Type>()
        expect(phase.phase).type.toBe<'first'>()
        expect(phase.count).type.toBe<number>()
        yield* First
        yield* TaskExecution
        yield* Effect.addFinalizer(() => Effect.void)
        if (parameters.count < 0) return yield* Effect.fail('negative' as const)
      }),
    second: ({ checkpoint: phase }) =>
      Effect.gen(function* () {
        expect(phase.phase).type.toBe<'second'>()
        expect(phase.total).type.toBe<number>()
        yield* Second
      }),
  },
  abort: () => Abort.pipe(Effect.asVoid),
})

test('retains each callback requirement/error and subtracts only invocation-supplied services', () => {
  expect<Task.Requirements<typeof self>>().type.toBe<First | Second | Abort>()
  expect<Task.Error<typeof self>>().type.toBe<'negative'>()
  expect(self.context).type.toBe<
    Effect.Effect<
      Context.Context<First | Second | Abort>,
      never,
      First | Second | Abort | Scope.Scope
    >
  >()
  const layer = Layer.effect(First, Host)
  const provided = Task.provide(self, layer)
  expect<Task.Requirements<typeof provided>>().type.toBe<Host | Second | Abort>()
  const partial = provided.pipe(Task.provide(Layer.succeed(Second, 1)))
  expect<Task.Requirements<typeof partial>>().type.toBe<Host | Abort>()
  const failing = Task.provide(self, Layer.effect(First, Effect.fail('setup' as const)))
  expect<Task.ConstructionError<typeof failing>>().type.toBe<'setup'>()
})

test('phase maps are complete and handlers are Effects', () => {
  expect(Task.make({ ...base, phases: { first: () => Effect.void } })).type.toRaiseError()
  expect(
    Task.make({
      ...base,
      phases: { first: () => Effect.void, second: () => Effect.void, extra: () => Effect.void },
    }),
  ).type.toRaiseError()
  expect(Task.make({ ...base, phases: self.phases, abort: () => undefined })).type.toRaiseError()
  expect(
    Task.make({ ...base, phases: { first: () => undefined, second: () => Effect.void } }),
  ).type.toRaiseError()
  expect(
    Task.make({
      ...base,
      initial: () => Effect.succeed({ phase: 'first' as const, count: 1 }),
      phases: self.phases,
    }),
  ).type.toRaiseError()
  expect(
    Task.make({
      ...base,
      phases: self.phases,
      migrate: () =>
        First.pipe(
          Effect.as({ input: { count: 1 }, checkpoint: { phase: 'first' as const, count: 1 } }),
        ),
    }),
  ).type.toRaiseError()
})

test('typed progress, outcomes and optional callbacks retain codec contracts', () => {
  const minimal = Task.make({
    ...base,
    phases: { first: () => Effect.void, second: () => Effect.void },
  })
  expect(minimal.abort).type.toBe<undefined>()
  expect(minimal.migrate).type.toBe<Task.Migration<typeof input, typeof checkpoint> | undefined>()
  expect<Task.Success<typeof self>>().type.toBe<number>()
  expect<Task.DomainFailure<typeof self>>().type.toBe<{ readonly reason: string }>()
  expect<Task.Outcome<typeof self>>().type.toBe<
    | { readonly status: 'completed'; readonly result: number }
    | {
        readonly status: 'failed'
        readonly error: { readonly message: string }
        readonly result: { readonly reason: string }
      }
    | { readonly status: 'aborted'; readonly reason?: string; readonly result?: Schema.Json }
    | { readonly status: 'orphaned'; readonly reason: string }
    | { readonly status: 'faulted'; readonly error: { readonly message: string } }
  >()
  expect(Task.initialize(self, { count: 1 })).type.toBe<
    Effect.Effect<{ input: Schema.Json; checkpoint: Schema.Json }, TaskError | Schema.SchemaError>
  >()
  expect(Task.initialize(self, { count: 'no' })).type.toRaiseError()
})

class Decoder extends Context.Service<Decoder, number>()('types/task/Decoder') {}
class Encoder extends Context.Service<Encoder, number>()('types/task/Encoder') {}

const serviceNumber = Schema.Finite.pipe(
  Schema.decodeTo(Schema.Finite, {
    decode: SchemaGetter.transformEffect((value: number) => Decoder.pipe(Effect.as(value))),
    encode: SchemaGetter.transformEffect((value: number) => Encoder.pipe(Effect.as(value))),
  }),
)
const withCodecs = Task.make({
  ...base,
  input: Schema.Struct({ count: serviceNumber }),
  success: serviceNumber,
  phases: { first: () => First.pipe(Effect.asVoid), second: () => Effect.void },
})
test('retains codec services separately from supplied invocation services', () => {
  expect<Task.CodecServices<typeof withCodecs>>().type.toBe<Decoder | Encoder>()
  expect<Task.Requirements<typeof withCodecs>>().type.toBe<Decoder | Encoder | First>()
  expect(Task.decode(serviceNumber, 1)).type.toBe<
    Effect.Effect<number, Schema.SchemaError, Decoder>
  >()
  expect(Task.encode(serviceNumber, 1)).type.toBe<
    Effect.Effect<Schema.Json, Schema.SchemaError, Encoder>
  >()
  expect(Task.initialize(withCodecs, { count: 1 })).type.toBe<
    Effect.Effect<
      { input: Schema.Json; checkpoint: Schema.Json },
      TaskError | Schema.SchemaError,
      Decoder | Encoder
    >
  >()
  const partially = Task.provide(withCodecs, Layer.succeed(First, 1))
  expect<Task.Requirements<typeof partially>>().type.toBe<Decoder | Encoder>()
  expect(Effect.runPromise(Effect.scoped(partially.context))).type.toRaiseError()
  const supplied = partially.pipe(
    Task.provide(Layer.merge(Layer.succeed(Decoder, 1), Layer.succeed(Encoder, 1))),
  )
  expect(Effect.scoped(supplied.context)).type.toBe<
    Effect.Effect<Context.Context<Decoder | Encoder | First>>
  >()
  expect(Task.decode(Task.outcomeSchema(self), { status: 'completed', result: 1 })).type.toBe<
    Effect.Effect<Task.Outcome<typeof self>, Schema.SchemaError>
  >()
  expect(Task.decode(Task.outcomeSchema(withCodecs), { status: 'completed', result: 1 })).type.toBe<
    Effect.Effect<Task.Outcome<typeof withCodecs>, Schema.SchemaError, Decoder>
  >()
})

declare const invocation: Access
declare const tx: Transaction.Transaction
declare const ref: Task.Ref<typeof self>
declare const document: Document.Document<Schema.Struct<{ readonly count: typeof serviceNumber }>>
declare const target: Document.Target

test('gated transactional callbacks and invocation operations preserve exact requirements', () => {
  expect(invocation.commit(() => Second)).type.toBe<Effect.Effect<number, Failure, Second>>()
  expect(invocation.commit(() => Effect.fail('rollback' as const))).type.toBe<
    Effect.Effect<never, 'rollback' | Failure>
  >()
  expect(
    invocation.progress(tx, self, { status: 'pending', checkpoint: { phase: 'second', total: 1 } }),
  ).type.toBe<Effect.Effect<void, Failure>>()
  expect(
    invocation.progress(tx, self, { status: 'pending', checkpoint: { phase: 'second', count: 1 } }),
  ).type.toRaiseError()
  expect(
    invocation.progress(tx, self, {
      status: 'completing',
      outcome: { status: 'completed', result: 'wrong' },
    }),
  ).type.toRaiseError()
  expect(invocation.wait(ref)).type.toBe<Effect.Effect<Task.Outcome<typeof self>, Failure>>()
  expect(invocation.outcome(ref)).type.toBe<
    Effect.Effect<Option.Option<Task.Outcome<typeof self>>, Failure>
  >()
  expect(invocation.memo('__proto__', serviceNumber)).type.toBe<
    Effect.Effect<Option.Option<number>, Failure, Decoder>
  >()
  expect(invocation.memoize(tx, 'toString', serviceNumber, 1)).type.toBe<
    Effect.Effect<number, Failure, Decoder | Encoder>
  >()
  expect(invocation.snapshot(document, target)).type.toBe<
    Effect.Effect<
      Option.Option<Document.Snapshot<typeof document.definition.schema>>,
      Failure,
      Decoder
    >
  >()
  expect(invocation.settings).type.toBe<Effect.Effect<Settings.Snapshot, Failure>>()
})

const otherNumber = Schema.Finite.pipe(
  Schema.decodeTo(Schema.Finite, {
    decode: SchemaGetter.transformEffect((value: number) => First.pipe(Effect.as(value))),
    encode: SchemaGetter.transformEffect((value: number) => Second.pipe(Effect.as(value))),
  }),
)
const allCodecs = Task.make({
  ...base,
  input: serviceNumber,
  checkpoint: Schema.Struct({ phase: Schema.Literal('work'), count: otherNumber }),
  success: serviceNumber,
  failure: otherNumber,
  initial: (count) => ({ phase: 'work' as const, count }),
  phases: { work: () => Effect.void },
})
test('tracks every codec direction while keeping mutation requirements precise', () => {
  expect<Task.CodecServices<typeof allCodecs>>().type.toBe<Decoder | Encoder | First | Second>()
  expect<Task.Requirements<typeof allCodecs>>().type.toBe<Decoder | Encoder | First | Second>()
  expect<Task.CreationServices<typeof allCodecs>>().type.toBe<Decoder | Encoder | Second>()
  expect<Task.ProgressServices<typeof allCodecs>>().type.toBe<Encoder | Second>()
  expect(invocation.createChild(tx, allCodecs, 1)).type.toBe<
    Effect.Effect<Task.Ref<typeof allCodecs>, Failure, Decoder | Encoder | Second>
  >()
  expect(
    invocation.progress(tx, allCodecs, {
      status: 'pending',
      checkpoint: { phase: 'work', count: 1 },
    }),
  ).type.toBe<Effect.Effect<void, Failure, Encoder | Second>>()
  expect(Task.encodeCheckpoint(allCodecs.checkpoint, { phase: 'work', count: 1 })).type.toBe<
    Effect.Effect<Schema.Json, TaskError | Schema.SchemaError, Second>
  >()
  expect(Task.decodeCheckpoint(allCodecs.checkpoint, { phase: 'work', count: 1 })).type.toBe<
    Effect.Effect<Task.Checkpoint<typeof allCodecs>, TaskError | Schema.SchemaError, First>
  >()
})
