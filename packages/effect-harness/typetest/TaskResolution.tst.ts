import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import type * as Record from 'effect-harness/Record'
import type { Failure } from 'effect-harness/SessionError'
import * as Task from 'effect-harness/Task'
import * as TaskResolution from 'effect-harness/TaskResolution'
import type * as Transaction from 'effect-harness/Transaction'
import type * as RegistryScope from 'effect-harness/RegistryScope'
import * as TaskRegistry from '../src/internal/TaskRegistry.js'
import * as TaskTransitions from '../src/internal/TaskTransitions.js'

class Decoder extends Context.Service<Decoder, number>()('types/resolution/Decoder') {}
class Encoder extends Context.Service<Encoder, number>()('types/resolution/Encoder') {}
const count = Schema.Natural.pipe(
  Schema.decodeTo(Schema.Natural, {
    decode: SchemaGetter.transformEffect((value: number) => Decoder.pipe(Effect.as(value))),
    encode: SchemaGetter.transformEffect((value: number) => Encoder.pipe(Effect.as(value))),
  }),
)
const input = Schema.Struct({ count })
const checkpoint = Schema.Struct({ phase: Schema.Literal('work'), count })
const definition = Task.make({
  kind: 'types.resolution',
  version: 2,
  input,
  checkpoint,
  success: Schema.Void,
  failure: Schema.String,
  initial: ({ count }) => ({ phase: 'work' as const, count }),
  phases: { work: () => Effect.void },
  migrate: () =>
    Effect.succeed({ input: { count: 1 }, checkpoint: { phase: 'work' as const, count: 1 } }),
})
declare const task: Record.Task
declare const tx: Transaction.Transaction
declare const snapshot: RegistryScope.Snapshot
declare const captured: TaskRegistry.Binding<typeof definition>

test('inspection retains decoding services and typed values without dispatch requirements', () => {
  expect(TaskResolution.inspect(definition, task)).type.toBe<
    Effect.Effect<TaskResolution.Inspection<typeof definition>, never, Decoder>
  >()
  expect(TaskRegistry.inspect({ tasks: [captured] }, task)).type.toBe<
    Effect.Effect<
      TaskResolution.Inspection<typeof definition> & {
        readonly binding: TaskRegistry.Binding<typeof definition> | undefined
      }
    >
  >()
  expect(TaskRegistry.inspect(snapshot, task)).type.toBe<
    Effect.Effect<
      TaskResolution.Inspection<Task.Any> & {
        readonly binding: TaskRegistry.Binding<Task.Any> | undefined
      }
    >
  >()
  expect<TaskResolution.DecodingServices<Task.Any>>().type.toBe<unknown>()
  const decoded = TaskResolution.inspect(definition, task).pipe(
    Effect.map((result) => {
      if (result._tag === 'compatible') {
        expect(result.input).type.toBe<typeof input.Type>()
        expect(result.checkpoint).type.toBe<typeof checkpoint.Type>()
        expect(result.checkpoint.phase).type.toBe<'work'>()
      }
      if (result._tag === 'blocked') expect(result.error).type.toBe<TaskResolution.Blocked>()
    }),
  )
  expect<Effect.Services<typeof decoded>>().type.toBe<Decoder>()
})

test('private upgrade and memo preparation preserve native codec R/E', () => {
  expect(TaskRegistry.upgrade(tx, definition, task.id)).type.toBe<
    Effect.Effect<Record.Task, Failure | TaskResolution.Blocked, Decoder | Encoder>
  >()
  expect(TaskTransitions.memoize(task, 'count', count, 1)).type.toBe<
    Effect.Effect<
      { readonly record: Record.Task; readonly value: number },
      Schema.SchemaError,
      Decoder | Encoder
    >
  >()
  expect(TaskTransitions.memoize(task, 'count', count, 'wrong')).type.toRaiseError()
})
