/** Executable definitions only. Registration, reservation and scheduling are separate runtime work. */
import type * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import type * as Layer from 'effect/Layer'
import { dual } from 'effect/Function'
import { pipeArguments } from 'effect/Pipeable'
import type * as Pipeable from 'effect/Pipeable'
import * as Schema from 'effect/Schema'
import * as SchemaAST from 'effect/SchemaAST'
import type * as Scope from 'effect/Scope'
import type * as Record from './Record.js'
import type { TaskExecution } from './TaskExecution.js'
import { TaskError } from './TaskError.js'

export const TypeId = '~effect-harness/Task'
export type CheckpointCodec = Schema.ConstraintCodec<
  { readonly phase: string },
  unknown,
  unknown,
  unknown
>
export const MetadataSchema = Schema.Struct({
  kind: Schema.NonEmptyString,
  version: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER })),
})

export type PhaseInput<
  I extends Schema.Constraint,
  C extends CheckpointCodec,
  P extends string = C['Type']['phase'],
> = {
  readonly input: I['Type']
  readonly checkpoint: C['Type'] extends infer V
    ? V extends { readonly phase: string }
      ? P extends V['phase']
        ? V & { readonly phase: P }
        : never
      : never
    : never
}
export type Handler<I extends Schema.Constraint, C extends CheckpointCodec> = (value: {
  readonly input: I['Type']
  readonly checkpoint: C['Type']
}) => Effect.Effect<void, unknown, unknown>
export type Phases<I extends Schema.Constraint, C extends CheckpointCodec> = {
  readonly [P in C['Type']['phase']]: (
    value: PhaseInput<I, C, P>,
  ) => Effect.Effect<void, unknown, unknown>
}
type HandlerError<H> = H extends (...args: never[]) => Effect.Effect<infer _A, infer E, infer _R>
  ? E
  : never
type HandlerServices<H> = H extends (...args: never[]) => Effect.Effect<infer _A, infer _E, infer R>
  ? R
  : never
type Services<S extends Schema.Constraint> = S['DecodingServices'] | S['EncodingServices']
type External<
  I extends Schema.Constraint,
  C extends CheckpointCodec,
  S extends Schema.Constraint,
  F extends Schema.Constraint,
  H,
  B,
> =
  | Services<I>
  | Services<C>
  | Services<S>
  | Services<F>
  | Exclude<HandlerServices<H[keyof H]> | HandlerServices<B>, TaskExecution | Scope.Scope>

/** Migration maps one older wire contract directly to this version, without ambient services. */
export type Migration<I extends Schema.Constraint, C extends CheckpointCodec> = (previous: {
  readonly version: number
  readonly input: Schema.Json
  readonly checkpoint: Schema.Json
}) => Effect.Effect<
  { readonly input: I['Type']; readonly checkpoint: C['Type'] },
  TaskError | Schema.SchemaError
>

/** Schema identity may be shared with handlers without a recursive executable definition. */
export interface Contract {
  readonly kind: string
  readonly version: number
  readonly input: Schema.Constraint
  readonly checkpoint: CheckpointCodec
  readonly success: Schema.Constraint
  readonly failure: Schema.Constraint
}
export interface Any extends Contract, Pipeable.Pipeable {
  readonly [TypeId]: {
    readonly _Requirements: (_: never) => unknown
    readonly _Error: (_: never) => unknown
    readonly _ConstructionError: (_: never) => unknown
  }
  readonly initial: (input: never) => unknown
  readonly phases: Readonly<globalThis.Record<string, (value: never) => unknown>>
  readonly abort?: ((value: never) => unknown) | undefined
  readonly migrate?: (value: never) => unknown
}
export type Input<T extends Contract> = T['input']['Type']
export type Checkpoint<T extends Contract> = T['checkpoint']['Type']
export type Success<T extends Contract> = T['success']['Type']
export type DomainFailure<T extends Contract> = T['failure']['Type']
export type CodecServices<T extends Contract> =
  | Services<T['input']>
  | Services<T['checkpoint']>
  | Services<T['success']>
  | Services<T['failure']>
/** Preparing a new task validates/encodes input and encodes its initial checkpoint. */
export type CreationServices<T extends Contract> =
  | T['input']['EncodingServices']
  | T['input']['DecodingServices']
  | T['checkpoint']['EncodingServices']
/** Progress writes encode checkpoints or typed outcome payloads, without decoding input. */
export type ProgressServices<T extends Contract> =
  | T['checkpoint']['EncodingServices']
  | T['success']['EncodingServices']
  | T['failure']['EncodingServices']
export type Requirements<T extends Any> = T extends {
  readonly [TypeId]: { readonly _Requirements: (_: never) => infer R }
}
  ? R
  : never
export type Error<T extends Any> = T extends {
  readonly [TypeId]: { readonly _Error: (_: never) => infer E }
}
  ? E
  : never
export type ConstructionError<T extends Any> = T extends {
  readonly [TypeId]: { readonly _ConstructionError: (_: never) => infer E }
}
  ? E
  : never

/** A live typed reference; only its ID is persisted, never its definition or Layers. */
export interface Ref<T extends Any> {
  readonly id: Record.TaskId
  readonly definition: T
}
export type Outcome<T extends Contract> =
  | { readonly status: 'completed'; readonly result: Success<T> }
  | {
      readonly status: 'failed'
      readonly error: { readonly message: string }
      readonly result: DomainFailure<T>
    }
  | { readonly status: 'aborted'; readonly reason?: string; readonly result?: Schema.Json }
  | { readonly status: 'orphaned'; readonly reason: string }
  | { readonly status: 'faulted'; readonly error: { readonly message: string } }
/** New typed receipts require a payload for declared success/domain failure codecs. */
export const outcomeSchema = <S extends Schema.Constraint, F extends Schema.Constraint>(self: {
  readonly success: S
  readonly failure: F
}) =>
  Schema.Union([
    Schema.Struct({ status: Schema.Literal('completed'), result: self.success }),
    Schema.Struct({
      status: Schema.Literal('failed'),
      error: Schema.Struct({ message: Schema.String }),
      result: self.failure,
    }),
    Schema.Struct({
      status: Schema.Literal('aborted'),
      reason: Schema.optionalKey(Schema.String),
      result: Schema.optionalKey(Schema.Json),
    }),
    Schema.Struct({ status: Schema.Literal('orphaned'), reason: Schema.String }),
    Schema.Struct({
      status: Schema.Literal('faulted'),
      error: Schema.Struct({ message: Schema.String }),
    }),
  ])

export type NextState<T extends Contract> =
  | { readonly status: 'pending'; readonly checkpoint: Checkpoint<T> }
  | {
      readonly status: 'waiting'
      readonly checkpoint: Checkpoint<T>
      readonly on: ReadonlyArray<Record.TaskId>
      readonly policy: 'failFast' | 'allSettled'
    }
  | { readonly status: 'completing'; readonly outcome: Outcome<T> }

export interface Task<
  I extends Schema.Constraint,
  C extends CheckpointCodec,
  S extends Schema.Constraint,
  F extends Schema.Constraint,
  H extends Phases<I, C>,
  B extends Handler<I, C> | undefined = undefined,
  R = External<I, C, S, F, H, B>,
  EBuild = never,
> extends Any {
  readonly [TypeId]: {
    readonly _Requirements: (_: never) => R
    readonly _Error: (_: never) => HandlerError<H[keyof H]> | HandlerError<B>
    readonly _ConstructionError: (_: never) => EBuild
  }
  readonly input: I
  readonly checkpoint: C
  readonly success: S
  readonly failure: F
  readonly initial: (input: I['Type']) => C['Type']
  readonly phases: H
  readonly abort?: B
  readonly migrate?: Migration<I, C>
  /**
   * Build/capture once per registration Scope. Provide the captured Context to codecs and
   * handlers using Context.merge(captured, invocationContext), where invocationContext
   * contains the fresh TaskExecution and Scope services and wins on both keys.
   * Layer inputs and construction failures remain explicit; this does not start a task.
   */
  readonly context: Effect.Effect<
    Context.Context<External<I, C, S, F, H, B>>,
    EBuild,
    R | Scope.Scope
  >
}

/** Declare exhaustive literal phase handlers. Call validate before accepting a registration. */
export const make = <
  I extends Schema.Constraint,
  C extends CheckpointCodec,
  S extends Schema.Constraint,
  F extends Schema.Constraint,
  const H extends Phases<I, C>,
  B extends Handler<I, C> | undefined = undefined,
>(definition: {
  readonly kind: string
  readonly version: number
  readonly input: I
  readonly checkpoint: C
  readonly success: S
  readonly failure: F
  readonly initial: (input: I['Type']) => C['Type']
  readonly phases: H & Readonly<globalThis.Record<Exclude<keyof H, C['Type']['phase']>, never>>
  readonly abort?: B
  readonly migrate?: Migration<I, C>
}): Task<I, C, S, F, H, B> =>
  Object.freeze({
    ...definition,
    phases: Object.freeze({ ...definition.phases }),
    context: Effect.context<External<I, C, S, F, H, B>>(),
    [TypeId]: {
      _Requirements: (_: never) => _,
      _Error: (_: never) => _,
      _ConstructionError: (_: never) => _,
    },
    pipe() {
      return pipeArguments(this, arguments)
    },
  })

/** Layers are acquired in the consuming registration Scope, with native input/error tracking. */
export const provide: {
  <ROut, ELayer, RIn>(
    layer: Layer.Layer<ROut, ELayer, RIn>,
  ): <
    I extends Schema.Constraint,
    C extends CheckpointCodec,
    S extends Schema.Constraint,
    F extends Schema.Constraint,
    H extends Phases<I, C>,
    B extends Handler<I, C> | undefined,
    R,
    EBuild,
  >(
    self: Task<I, C, S, F, H, B, R, EBuild>,
  ) => Task<I, C, S, F, H, B, Exclude<R, ROut> | RIn, EBuild | ELayer>
  <
    I extends Schema.Constraint,
    C extends CheckpointCodec,
    S extends Schema.Constraint,
    F extends Schema.Constraint,
    H extends Phases<I, C>,
    B extends Handler<I, C> | undefined,
    R,
    EBuild,
    ROut,
    ELayer,
    RIn,
  >(
    self: Task<I, C, S, F, H, B, R, EBuild>,
    layer: Layer.Layer<ROut, ELayer, RIn>,
  ): Task<I, C, S, F, H, B, Exclude<R, ROut> | RIn, EBuild | ELayer>
} = dual(
  2,
  <
    I extends Schema.Constraint,
    C extends CheckpointCodec,
    S extends Schema.Constraint,
    F extends Schema.Constraint,
    H extends Phases<I, C>,
    B extends Handler<I, C> | undefined,
    R,
    EBuild,
    ROut,
    ELayer,
    RIn,
  >(
    self: Task<I, C, S, F, H, B, R, EBuild>,
    layer: Layer.Layer<ROut, ELayer, RIn>,
  ): Task<I, C, S, F, H, B, Exclude<R, ROut> | RIn, EBuild | ELayer> => ({
    ...self,
    context: self.context.pipe(Effect.provide(layer)),
    [TypeId]: {
      ...self[TypeId],
      _Requirements: (_: never) => _,
      _ConstructionError: (_: never) => _,
    },
  }),
)

const invalid = (message: string) =>
  new TaskError({ reason: 'invalidDefinition', operation: 'task.validate', message })
const phaseNames = (
  ast: SchemaAST.AST,
  seen: ReadonlySet<SchemaAST.AST> = new Set(),
): Effect.Effect<ReadonlyArray<string>, TaskError> => {
  if (seen.has(ast))
    return Effect.fail(invalid('Checkpoint discriminator cannot be recursively defined'))
  const visited = new Set([...seen, ast])
  if (ast._tag === 'Suspend')
    return Effect.suspend(() => phaseNames(SchemaAST.toType(ast.thunk()), visited))
  if (ast._tag === 'Union')
    return Effect.forEach(ast.types, (member) => phaseNames(member, visited)).pipe(
      Effect.map((parts) => parts.flat()),
    )
  if (ast._tag !== 'Objects')
    return Effect.fail(
      invalid('Checkpoint must be an object or union of objects with a literal phase'),
    )
  const field = ast.propertySignatures.find((property) => property.name === 'phase')
  if (field === undefined || SchemaAST.isOptional(field.type))
    return Effect.fail(invalid('Checkpoint requires a phase discriminator'))
  const literals = field.type._tag === 'Union' ? field.type.types : [field.type]
  if (
    !literals.every(
      (literal) =>
        literal._tag === 'Literal' &&
        typeof literal.literal === 'string' &&
        literal.literal.length > 0,
    )
  )
    return Effect.fail(invalid('Checkpoint phase must use finite nonempty string literals'))
  return Effect.succeed(
    literals.flatMap((literal) =>
      literal._tag === 'Literal' && typeof literal.literal === 'string' ? [literal.literal] : [],
    ),
  )
}

/** Validate structure without running handlers, initializers, migrations or codec services. */
export const validate = Effect.fnUntraced(function* (
  self: Any,
): Effect.fn.Return<void, TaskError | Schema.SchemaError> {
  yield* Schema.decodeEffect(MetadataSchema)(self)
  if (![self.input, self.checkpoint, self.success, self.failure].every(Schema.isSchema))
    return yield* invalid('Task codecs must be schemas')
  if (
    typeof self.initial !== 'function' ||
    (self.abort !== undefined && typeof self.abort !== 'function') ||
    (self.migrate !== undefined && typeof self.migrate !== 'function')
  )
    return yield* invalid('Task callbacks must be functions')
  const phases = yield* phaseNames(SchemaAST.toType(self.checkpoint.ast))
  if (phases.length === 0 || new Set(phases).size !== phases.length)
    return yield* invalid('Checkpoint phases must be unique')
  for (const phase of phases) {
    if (!Object.hasOwn(self.phases, phase) || typeof self.phases[phase] !== 'function')
      return yield* invalid(`Missing handler for phase ${phase}`)
  }
  if (
    Reflect.ownKeys(self.phases).some(
      (phase) => typeof phase !== 'string' || !phases.includes(phase),
    )
  )
    return yield* invalid('Phase map contains an undeclared checkpoint phase')
})

const copyJson = Effect.fnUntraced(function* (
  value: unknown,
): Effect.fn.Return<Schema.Json, Schema.SchemaError> {
  const checked = yield* Schema.decodeUnknownEffect(Schema.Json)(value)
  const codec = Schema.fromJsonString(Schema.Json)
  return yield* Schema.encodeEffect(codec)(checked).pipe(Effect.flatMap(Schema.decodeEffect(codec)))
})
/** Encode and detach through the canonical JSON boundary, retaining encoding services. */
export const encode = Effect.fnUntraced(function* <S extends Schema.Constraint>(
  schema: S,
  value: S['Type'],
): Effect.fn.Return<Schema.Json, Schema.SchemaError, S['EncodingServices']> {
  return yield* Schema.encodeEffect(Schema.toCodecJson(schema))(value).pipe(
    Effect.flatMap(copyJson),
  )
})
/** Decode from owned JSON so caller mutation cannot change decoded checkpoints or inputs. */
export const decode = Effect.fnUntraced(function* <S extends Schema.Constraint>(
  schema: S,
  value: unknown,
): Effect.fn.Return<S['Type'], Schema.SchemaError, S['DecodingServices']> {
  return yield* Schema.decodeEffect(Schema.toCodecJson(schema))(yield* copyJson(value))
})

const WireCheckpointSchema = Schema.Struct({ phase: Schema.NonEmptyString })
/** Existing Record.TaskState requires the wire discriminator to remain a JSON object field. */
export const encodeCheckpoint = Effect.fnUntraced(function* <C extends CheckpointCodec>(
  schema: C,
  value: C['Type'],
): Effect.fn.Return<Schema.Json, TaskError | Schema.SchemaError, C['EncodingServices']> {
  const wire = yield* encode(schema, value)
  const metadata = yield* Schema.decodeUnknownEffect(WireCheckpointSchema)(wire)
  if (metadata.phase !== value.phase)
    return yield* new TaskError({
      reason: 'invalidCheckpoint',
      operation: 'task.encodeCheckpoint',
      message: 'Encoded checkpoint must preserve its phase discriminator',
    })
  return wire
})
/** Validate the existing persisted discriminator before decoding the task-specific payload. */
export const decodeCheckpoint = Effect.fnUntraced(function* <C extends CheckpointCodec>(
  schema: C,
  value: unknown,
): Effect.fn.Return<C['Type'], TaskError | Schema.SchemaError, C['DecodingServices']> {
  const wire = yield* copyJson(value)
  const metadata = yield* Schema.decodeUnknownEffect(WireCheckpointSchema)(wire)
  const checkpoint = yield* decode(schema, wire)
  if (metadata.phase !== checkpoint.phase)
    return yield* new TaskError({
      reason: 'invalidCheckpoint',
      operation: 'task.decodeCheckpoint',
      message: 'Decoded checkpoint must preserve its phase discriminator',
    })
  return checkpoint
})

/** Prepare owned, validated wire values; this stages no records and invokes no scheduler. */
export const initialize = Effect.fnUntraced(function* <
  I extends Schema.Constraint,
  C extends CheckpointCodec,
  S extends Schema.Constraint,
  F extends Schema.Constraint,
  H extends Phases<I, C>,
  B extends Handler<I, C> | undefined,
  R,
  EBuild,
>(
  self: Task<I, C, S, F, H, B, R, EBuild>,
  input: I['Type'],
): Effect.fn.Return<
  { input: Schema.Json; checkpoint: Schema.Json },
  TaskError | Schema.SchemaError,
  I['EncodingServices'] | I['DecodingServices'] | C['EncodingServices']
> {
  yield* validate(self)
  const encodedInput = yield* encode(self.input, input)
  const ownedInput = yield* decode(self.input, encodedInput)
  const checkpoint = yield* Effect.try({
    try: () => self.initial(ownedInput),
    catch: (cause) =>
      new TaskError({
        reason: 'invalidCheckpoint',
        operation: 'task.initial',
        message: 'Task initializer failed',
        cause,
      }),
  })
  return { input: encodedInput, checkpoint: yield* encodeCheckpoint(self.checkpoint, checkpoint) }
})

/** Fit a nonterminal stored version; the scheduler must persist upgrades under its private gate. */
export const migrate = Effect.fnUntraced(function* <
  I extends Schema.Constraint,
  C extends CheckpointCodec,
  S extends Schema.Constraint,
  F extends Schema.Constraint,
  H extends Phases<I, C>,
  B extends Handler<I, C> | undefined,
  R,
  EBuild,
>(
  self: Task<I, C, S, F, H, B, R, EBuild>,
  previous: { readonly version: number; readonly input: unknown; readonly checkpoint: unknown },
): Effect.fn.Return<
  { input: Schema.Json; checkpoint: Schema.Json },
  TaskError | Schema.SchemaError,
  Services<I> | Services<C>
> {
  yield* validate(self)
  yield* Schema.decodeEffect(MetadataSchema)({ kind: self.kind, version: previous.version })
  if (previous.version > self.version)
    return yield* new TaskError({
      reason: 'newerVersion',
      operation: 'task.migrate',
      message: 'Stored task version is newer than its definition',
    })
  const input = yield* copyJson(previous.input)
  const checkpoint = yield* copyJson(previous.checkpoint)
  if (previous.version === self.version) {
    yield* decode(self.input, input)
    yield* decodeCheckpoint(self.checkpoint, checkpoint)
    return { input, checkpoint }
  }
  if (self.migrate === undefined)
    return yield* new TaskError({
      reason: 'missingMigration',
      operation: 'task.migrate',
      message: 'Older task requires a direct migration',
    })
  const migration = self.migrate
  const transform = yield* Effect.try({
    try: () => migration({ version: previous.version, input, checkpoint }),
    catch: (cause) =>
      new TaskError({
        reason: 'migrationFailed',
        operation: 'task.migrate',
        message: 'Task migration callback failed',
        cause,
      }),
  })
  const result = yield* transform
  return {
    input: yield* encode(self.input, result.input),
    checkpoint: yield* encodeCheckpoint(self.checkpoint, result.checkpoint),
  }
})
