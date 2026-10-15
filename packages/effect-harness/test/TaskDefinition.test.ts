import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Cause from 'effect/Cause'
import * as Exit from 'effect/Exit'
import * as SchemaGetter from 'effect/SchemaGetter'
import * as SchemaTransformation from 'effect/SchemaTransformation'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Task from '../src/Task.js'
import { TaskError } from '../src/TaskError.js'

const input = Schema.Struct({ count: Schema.Natural })
const checkpoint = Schema.Union([
  Schema.Struct({ phase: Schema.Literal('first'), count: Schema.Natural }),
  Schema.Struct({ phase: Schema.Literal('second'), total: Schema.Natural }),
])
const options = {
  kind: 'test.counter',
  version: 2,
  input,
  checkpoint,
  success: Schema.Natural,
  failure: Schema.Struct({ reason: Schema.String }),
  initial: ({ count }: typeof input.Type) => ({ phase: 'first' as const, count }),
  phases: { first: () => Effect.void, second: () => Effect.void },
}
const definition = Task.make(options)

const expectInvalid = (candidate: Task.Any) =>
  Task.validate(candidate).pipe(
    Effect.flip,
    Effect.map((error) => {
      assert.strictEqual(error._tag, 'TaskError')
      if (error._tag === 'TaskError') assert.strictEqual(error.reason, 'invalidDefinition')
    }),
  )

describe('Task definition foundation', () => {
  it.effect('validates a complete phase union without invoking application callbacks', () =>
    Effect.gen(function* () {
      let calls = 0
      const self = Task.make({
        ...options,
        initial: () => {
          calls++
          return { phase: 'first' as const, count: 1 }
        },
        phases: {
          first: () => {
            calls++
            return Effect.void
          },
          second: () => Effect.void,
        },
      })
      yield* Task.validate(self)
      assert.strictEqual(calls, 0)
    }),
  )
  it.effect('requires own handlers and rejects missing, extra and prototype-inherited phases', () =>
    Effect.gen(function* () {
      yield* expectInvalid({ ...definition, phases: { first: () => Effect.void } })
      yield* expectInvalid({
        ...definition,
        phases: { ...definition.phases, third: () => Effect.void },
      })
      const inherited = Object.setPrototypeOf(
        {},
        { first: () => Effect.void, second: () => Effect.void },
      )
      yield* expectInvalid({ ...definition, phases: inherited })
    }),
  )
  it.effect('rejects unbounded, empty and duplicate discriminators', () =>
    Effect.gen(function* () {
      yield* expectInvalid({ ...definition, checkpoint: Schema.Struct({ phase: Schema.String }) })
      yield* expectInvalid({
        ...definition,
        checkpoint: Schema.Struct({ phase: Schema.Literal('') }),
      })
      yield* expectInvalid({
        ...definition,
        checkpoint: Schema.Union([
          Schema.Struct({ phase: Schema.Literal('first'), a: Schema.String }),
          Schema.Struct({ phase: Schema.Literal('first'), b: Schema.Finite }),
        ]),
      })
    }),
  )
  for (const metadata of [
    { kind: '' },
    { version: 0 },
    { version: 1.5 },
    { version: Number.MAX_SAFE_INTEGER + 1 },
  ]) {
    it.effect(`rejects invalid metadata ${JSON.stringify(metadata)}`, () =>
      Task.validate({ ...definition, ...metadata }).pipe(
        Effect.flip,
        Effect.map((error) => assert.strictEqual(error._tag, 'SchemaError')),
      ),
    )
  }
  it.effect('owns input/checkpoint JSON and rejects invalid schema values', () =>
    Effect.gen(function* () {
      const caller = { count: 3 }
      const wire = yield* Task.initialize(definition, caller)
      caller.count = 9
      assert.deepEqual(wire, {
        input: { count: 3 },
        checkpoint: { phase: 'first' as const, count: 3 },
      })
      const supplied = { phase: 'second', total: 5 }
      const decoded = yield* Task.decode(checkpoint, supplied)
      supplied.total = 20
      assert.deepEqual(decoded, { phase: 'second', total: 5 })
      assert.strictEqual(
        (yield* Task.initialize(definition, { count: -1 }).pipe(Effect.flip))._tag,
        'SchemaError',
      )
      assert.strictEqual(
        (yield* Task.decode(checkpoint, { phase: 'missing' }).pipe(Effect.flip))._tag,
        'SchemaError',
      )
      assert.strictEqual(
        (yield* Task.encode(definition.success, -1).pipe(Effect.flip))._tag,
        'SchemaError',
      )
      assert.strictEqual(
        (yield* Task.decode(definition.failure, { reason: 1 }).pipe(Effect.flip))._tag,
        'SchemaError',
      )
      assert.strictEqual(
        (yield* Task.decode(Schema.Json, { bad: undefined }).pipe(Effect.flip))._tag,
        'SchemaError',
      )
    }),
  )
  it.effect('derives native JSON codecs for non-JSON decoded values', () =>
    Effect.gen(function* () {
      const wire = yield* Task.encode(Schema.Date, new Date('2026-10-10T00:00:00.000Z'))
      assert.strictEqual(wire, '2026-10-10T00:00:00.000Z')
      const decoded = yield* Task.decode(Schema.Date, wire)
      assert.strictEqual(decoded.toISOString(), wire)
    }),
  )
  it.effect('normalizes initializer throws and validates initializer results', () =>
    Effect.gen(function* () {
      const throwing = Task.make({
        ...options,
        initial: () => {
          throw new Error('fixture')
        },
      })
      const error = yield* Task.initialize(throwing, { count: 0 }).pipe(Effect.flip)
      assert.strictEqual(error._tag, 'TaskError')
      if (error._tag === 'TaskError') assert.strictEqual(error.reason, 'invalidCheckpoint')
      const invalid = Task.make({
        ...options,
        initial: () => ({ phase: 'first' as const, count: -1 }),
      })
      assert.strictEqual(
        (yield* Task.initialize(invalid, { count: 0 }).pipe(Effect.flip))._tag,
        'SchemaError',
      )
    }),
  )
  it.effect('blocks missing migrations and downgrades, and decodes exact versions', () =>
    Effect.gen(function* () {
      const previous = {
        version: 1,
        input: { count: 1 },
        checkpoint: { phase: 'first' as const, count: 1 },
      }
      const missing = yield* Task.migrate(definition, previous).pipe(Effect.flip)
      assert.strictEqual(missing._tag, 'TaskError')
      if (missing._tag === 'TaskError') assert.strictEqual(missing.reason, 'missingMigration')
      const newer = yield* Task.migrate(definition, { ...previous, version: 3 }).pipe(Effect.flip)
      assert.strictEqual(newer._tag, 'TaskError')
      if (newer._tag === 'TaskError') assert.strictEqual(newer.reason, 'newerVersion')
      const exact = yield* Task.migrate(definition, { ...previous, version: 2 })
      assert.deepEqual(exact, { input: previous.input, checkpoint: previous.checkpoint })
      assert.notStrictEqual(exact.input, previous.input)
      assert.strictEqual(
        (yield* Task.migrate(definition, {
          ...previous,
          version: 2,
          checkpoint: { phase: 'no' },
        }).pipe(Effect.flip))._tag,
        'SchemaError',
      )
    }),
  )
  it.effect('runs direct pure migration with owned inputs and validates both outputs', () =>
    Effect.gen(function* () {
      const previous = { version: 1, input: { count: 2 }, checkpoint: { phase: 'legacy' } }
      const self = Task.make({
        ...options,
        migrate: ({ version, input: oldInput }) => {
          assert.strictEqual(version, 1)
          assert.notStrictEqual(oldInput, previous.input)
          return Effect.succeed({
            input: { count: 4 },
            checkpoint: { phase: 'second' as const, total: 4 },
          })
        },
      })
      assert.deepEqual(yield* Task.migrate(self, previous), {
        input: { count: 4 },
        checkpoint: { phase: 'second', total: 4 },
      })
      const invalid = Task.make({
        ...options,
        migrate: () =>
          Effect.succeed({
            input: { count: -1 },
            checkpoint: { phase: 'first' as const, count: 0 },
          }),
      })
      assert.strictEqual(
        (yield* Task.migrate(invalid, previous).pipe(Effect.flip))._tag,
        'SchemaError',
      )
      const failed = Task.make({
        ...options,
        migrate: () =>
          Effect.fail(
            new TaskError({ reason: 'migrationFailed', operation: 'fixture', message: 'stop' }),
          ),
      })
      assert.strictEqual(
        (yield* Task.migrate(failed, previous).pipe(Effect.flip))._tag,
        'TaskError',
      )
    }),
  )
  it.effect(
    'captures supplied Layers once, retains external inputs and releases registration resources',
    () =>
      Effect.gen(function* () {
        class Seed extends Context.Service<Seed, number>()('test/task/Seed') {}
        class Host extends Context.Service<Host, number>()('test/task/Host') {}
        let acquired = 0
        let released = 0
        const layer = Layer.effect(
          Seed,
          Effect.gen(function* () {
            acquired++
            const seed = yield* Host
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                released++
              }),
            )
            return seed
          }),
        )
        const self = Task.make({
          ...options,
          phases: { first: () => Seed.pipe(Effect.asVoid), second: () => Effect.void },
        })
        const provided = self.pipe(Task.provide(layer))
        assert.strictEqual(acquired, 0)
        yield* Effect.scoped(
          Effect.gen(function* () {
            const captured = yield* provided.context
            yield* self.phases.first().pipe(Effect.provideContext(captured))
            yield* self.phases.first().pipe(Effect.provideContext(captured))
            assert.strictEqual(Context.get(captured, Seed), 7)
            assert.strictEqual(acquired, 1)
          }).pipe(Effect.provideService(Host, 7)),
        )
        assert.strictEqual(released, 1)
      }),
  )
  it.effect(
    'validates suspended native schema definitions and handles prototype-like phases as own keys',
    () =>
      Effect.gen(function* () {
        yield* Task.validate({ ...definition, checkpoint: Schema.suspend(() => checkpoint) })
        const names = Schema.Struct({ phase: Schema.Literals(['toString', '__proto__']) })
        const self = Task.make({
          ...options,
          checkpoint: names,
          initial: () => ({ phase: 'toString' as const }),
          phases: {
            toString: () => Effect.void,
            ['__proto__']: () => Effect.void,
          },
        })
        yield* Task.validate(self)
      }),
  )
  it.effect('validates and detaches every typed outcome channel', () =>
    Effect.gen(function* () {
      const schema = Task.outcomeSchema(definition)
      const completed = { status: 'completed' as const, result: 3 }
      assert.deepEqual(yield* Task.decode(schema, yield* Task.encode(schema, completed)), completed)
      const failed = {
        status: 'failed' as const,
        error: { message: 'domain' },
        result: { reason: 'expected' },
      }
      const decoded = yield* Task.decode(schema, failed)
      failed.result.reason = 'mutated'
      assert.deepEqual(decoded, {
        status: 'failed',
        error: { message: 'domain' },
        result: { reason: 'expected' },
      })
      for (const value of [
        { status: 'aborted' as const, reason: 'cancelled', result: { retained: true } },
        { status: 'orphaned' as const, reason: 'missing' },
        { status: 'faulted' as const, error: { message: 'defect' } },
      ])
        assert.deepEqual(yield* Task.decode(schema, value), value)
      for (const value of [
        { status: 'completed', result: -1 },
        { status: 'failed', error: { message: 'legacy without payload' } },
        { status: 'failed', error: { message: 'wrong payload' }, result: { reason: 3 } },
        { status: 'pending' },
      ])
        assert.strictEqual(
          (yield* Task.decode(schema, value).pipe(Effect.flip))._tag,
          'SchemaError',
        )
    }),
  )
  it.effect(
    'executes codecs only with their declared services and keeps codec failures typed',
    () =>
      Effect.gen(function* () {
        class Decoder extends Context.Service<Decoder, number>()('test/task/Decoder') {}
        class Encoder extends Context.Service<Encoder, number>()('test/task/Encoder') {}
        let decodes = 0
        let encodes = 0
        const number = Schema.Natural.pipe(
          Schema.decodeTo(Schema.Natural, {
            decode: SchemaGetter.transformEffect((value: number) =>
              Decoder.pipe(
                Effect.map(() => {
                  decodes++
                  return value
                }),
              ),
            ),
            encode: SchemaGetter.transformEffect((value: number) =>
              Encoder.pipe(
                Effect.map(() => {
                  encodes++
                  return value
                }),
              ),
            ),
          }),
        )
        const task = Task.make({
          ...options,
          input: Schema.Struct({ count: number }),
          success: number,
        })
        const captured = yield* Task.provide(
          task,
          Layer.merge(Layer.succeed(Decoder, 1), Layer.succeed(Encoder, 1)),
        ).context
        const result = yield* Task.initialize(task, { count: 4 }).pipe(
          Effect.provideContext(captured),
        )
        assert.deepEqual(result.input, { count: 4 })
        assert.isAbove(decodes, 0)
        assert.isAbove(encodes, 0)
        assert.strictEqual(
          (yield* Task.encode(number, -1).pipe(Effect.provideContext(captured), Effect.flip))._tag,
          'SchemaError',
        )
      }),
  )
  it.effect(
    'preserves migration defects for scheduler Cause handling while typing synchronous throws',
    () =>
      Effect.gen(function* () {
        const previous = { version: 1, input: {}, checkpoint: {} }
        const defective = Task.make({
          ...options,
          migrate: () => Effect.die('migration defect'),
        })
        const exit = yield* Task.migrate(defective, previous).pipe(Effect.exit)
        assert.isTrue(Exit.isFailure(exit))
        if (Exit.isFailure(exit)) assert.isTrue(Cause.hasDies(exit.cause))
        const throwing = Task.make({
          ...options,
          migrate: () => {
            throw new Error('callback failed')
          },
        })
        const error = yield* Task.migrate(throwing, previous).pipe(Effect.flip)
        assert.strictEqual(error._tag, 'TaskError')
        if (error._tag === 'TaskError') assert.strictEqual(error.reason, 'migrationFailed')
      }),
  )
  it.effect('preserves the existing Record checkpoint wire discriminator across codecs', () =>
    Effect.gen(function* () {
      const transformed = Schema.Struct({ phase: Schema.Literal('encoded') }).pipe(
        Schema.decodeTo(
          Schema.Struct({ phase: Schema.Literal('decoded') }),
          SchemaTransformation.transform({
            decode: () => ({ phase: 'decoded' as const }),
            encode: () => ({ phase: 'encoded' as const }),
          }),
        ),
      )
      const encoded = yield* Task.encodeCheckpoint(transformed, { phase: 'decoded' }).pipe(
        Effect.flip,
      )
      assert.strictEqual(encoded._tag, 'TaskError')
      if (encoded._tag === 'TaskError') assert.strictEqual(encoded.reason, 'invalidCheckpoint')
      const decoded = yield* Task.decodeCheckpoint(transformed, { phase: 'encoded' }).pipe(
        Effect.flip,
      )
      assert.strictEqual(decoded._tag, 'TaskError')
      assert.deepEqual(yield* Task.decodeCheckpoint(checkpoint, { phase: 'second', total: 3 }), {
        phase: 'second',
        total: 3,
      })
      assert.strictEqual(
        (yield* Task.decodeCheckpoint(checkpoint, 'second').pipe(Effect.flip))._tag,
        'SchemaError',
      )
    }),
  )
})
