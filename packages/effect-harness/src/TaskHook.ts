/** Typed phase lifecycle observers compiled at registration, for native supervisor dispatch. */
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import { dual } from 'effect/Function'
import type * as Pipeable from 'effect/Pipeable'
import * as Capability from './internal/Capability.js'
import * as Record from './Record.js'
import * as Task from './Task.js'
import { HookExecution } from './HookExecution.js'
import { TaskError } from './TaskError.js'
import * as HookReducer from './HookReducer.js'

export const StageSchema = Schema.Literals([
  'beforePhase',
  'afterPhase',
  'failure',
  'abort',
  'settled',
])
export type Stage = typeof StageSchema.Type
/** The supervisor supplies its pinned pre-phase input/checkpoint even for cancellation/settlement. */
export const EventSchema = Schema.Struct({
  kind: Schema.NonEmptyString,
  phase: Schema.NonEmptyString,
  stage: StageSchema,
  taskId: Record.TaskId,
  conversationId: Record.ConversationId,
  input: Schema.Json,
  checkpoint: Schema.Json,
  detail: Schema.optionalKey(Schema.Json),
})
export type Event = typeof EventSchema.Type
export type Input<T extends Task.Contract, P extends Task.Checkpoint<T>['phase']> = Omit<
  Event,
  'input' | 'checkpoint' | 'phase'
> & {
  readonly phase: P
  readonly input: Task.Input<T>
  readonly checkpoint: Task.Checkpoint<T> & { readonly phase: P }
}
export interface Compiled {
  readonly context: Context.Context<never>
  readonly kind: string
  readonly version: number
  readonly phase: string
  readonly stage: Stage
  readonly execute: (
    event: Event,
  ) => Effect.Effect<void, HookReducer.HookFailure, HookExecution | Scope.Scope>
}
export interface Registration<R = never, E = never> extends Pipeable.Pipeable {
  readonly kind: string
  readonly version: number
  readonly phase: string
  readonly stage: Stage
  readonly acquire: Effect.Effect<Compiled, E, R | Scope.Scope>
}

/** No Task.Any executable callback cast: codecs and the concrete observer are captured together. */
export const make = <T extends Task.Contract, const P extends Task.Checkpoint<T>['phase'], E, R>(
  task: T,
  definition: {
    readonly phase: P
    readonly stage: Stage
    readonly execute: (event: Input<T, P>) => Effect.Effect<void, E, R>
  },
): Registration<
  | Exclude<R, HookExecution | Scope.Scope>
  | T['input']['DecodingServices']
  | T['checkpoint']['DecodingServices']
> => {
  const contract = Object.freeze({
    kind: task.kind,
    version: task.version,
    input: task.input,
    checkpoint: task.checkpoint,
  })
  const spec = Object.freeze({ ...definition })
  return {
    pipe: Capability.pipe,
    kind: contract.kind,
    version: contract.version,
    phase: spec.phase,
    stage: spec.stage,
    acquire: Effect.gen(function* () {
      const captured = yield* Effect.context<
        R | T['input']['DecodingServices'] | T['checkpoint']['DecodingServices']
      >()
      // Typed acquisition above owns these dynamic codec services; invocation uses only this capture.
      const inputCodec = contract.input as unknown as Schema.ConstraintCodec<
        Task.Input<T>,
        unknown,
        never,
        never
      >
      const checkpointCodec = contract.checkpoint as unknown as Schema.ConstraintCodec<
        Task.Checkpoint<T>,
        unknown,
        never,
        never
      >
      return {
        context: captured,
        kind: contract.kind,
        version: contract.version,
        phase: spec.phase,
        stage: spec.stage,
        execute: (event) =>
          Effect.gen(function* () {
            const input = yield* Task.decode(inputCodec, event.input).pipe(
              Effect.provideContext(captured),
            )
            const checkpoint = yield* Task.decodeCheckpoint(checkpointCodec, event.checkpoint).pipe(
              Effect.provideContext(captured),
            )
            if (checkpoint.phase !== spec.phase)
              return yield* new TaskError({
                reason: 'invalidCheckpoint',
                operation: 'taskHook',
                message: 'Task hook checkpoint phase does not match',
              })
            return yield* Effect.suspend(() =>
              spec.execute({
                ...event,
                phase: spec.phase,
                input,
                checkpoint: checkpoint as Task.Checkpoint<T> & { readonly phase: P },
              }),
            )
          }).pipe(
            (effect) => HookReducer.protect('task.' + spec.stage, effect),
            Effect.updateContext((input: Context.Context<HookExecution | Scope.Scope>) =>
              Context.add(
                Context.add(captured, HookExecution, Context.get(input, HookExecution)),
                Scope.Scope,
                Context.get(input, Scope.Scope),
              ),
            ),
          ),
      }
    }),
  }
}

export const provide: {
  <ROut, ELayer, RIn>(
    layer: Layer.Layer<ROut, ELayer, RIn>,
  ): <R, E>(self: Registration<R, E>) => Registration<Exclude<R, ROut> | RIn, E | ELayer>
  <R, E, ROut, ELayer, RIn>(
    self: Registration<R, E>,
    layer: Layer.Layer<ROut, ELayer, RIn>,
  ): Registration<Exclude<R, ROut> | RIn, E | ELayer>
} = dual(
  2,
  <R, E, ROut, ELayer, RIn>(
    self: Registration<R, E>,
    layer: Layer.Layer<ROut, ELayer, RIn>,
  ): Registration<Exclude<R, ROut> | RIn, E | ELayer> => ({
    ...self,
    acquire: Layer.build(Layer.fresh(layer)).pipe(
      Effect.flatMap((context) => self.acquire.pipe(Effect.provideContext(context))),
    ),
  }),
)
