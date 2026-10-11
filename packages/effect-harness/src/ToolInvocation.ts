/** Durable programmatic tool calls owned by the current task invocation. */
import * as Effect from 'effect/Effect'
import type * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Record from './Record.js'
import type * as Tool from './Tool.js'
import * as ToolContract from './Tool.js'
import * as ToolResult from './ToolResult.js'
import * as Usage from './Usage.js'
import { ExecutionError } from './ExecutionError.js'
import type * as Transaction from './Transaction.js'
import type * as TaskExecution from './TaskExecution.js'
import type * as Execution from './ExecutionError.js'
import type { RegistryError } from './RegistryError.js'
import type * as StorageFormat from './StorageFormat.js'

export interface Options {
  /** Stable within one parent. Omission reserves an ordinal; replay must preserve call order. */
  readonly key?: string
  /**
   * false buffers reports until final settlement. details/diagnostic then acknowledge local
   * acceptance only; the final receipt acknowledges persistence. The default persists running
   * progress, with details acknowledging its actual committed revision. Final-only UI reports
   * share the tool's byte budget (at least 32 bytes) and accept at most 64 diagnostics.
   */
  readonly progress?: boolean
  readonly binding?: { readonly id: string; readonly version: number }
}
export const OptionsSchema = Schema.Struct({
  key: Schema.optionalKey(Schema.String),
  progress: Schema.optionalKey(Schema.Boolean),
  binding: Schema.optionalKey(
    Schema.Struct({ id: Schema.NonEmptyString, version: Schema.Natural }),
  ),
})
export const RequestSchema = Schema.Struct({
  name: Schema.NonEmptyString,
  arguments: Schema.JsonObject,
  options: Schema.optionalKey(OptionsSchema),
})
export type Request = typeof RequestSchema.Type
export const HandleSchema = Schema.Struct({
  parentTaskId: Record.TaskId,
  conversationId: Record.ConversationId,
  key: Schema.NonEmptyString,
  taskId: Record.TaskId,
  callId: Usage.NestedCallId,
  name: Schema.NonEmptyString,
})
export type Handle = typeof HandleSchema.Type
export const StatusSchema = Schema.Literals([
  'succeeded',
  'failed',
  'denied',
  'blocked',
  'invalid',
  'interrupted',
  'abandoned',
  'aborted',
  'faulted',
  'orphaned',
  'missingResult',
])
export const WireReceiptSchema = Schema.Struct({
  parentTaskId: Record.TaskId,
  key: Schema.NonEmptyString,
  taskId: Schema.optionalKey(Record.TaskId),
  callId: Usage.NestedCallId,
  name: Schema.NonEmptyString,
  attempt: Usage.AttemptId,
  status: StatusSchema,
  result: ToolResult.NestedSchema,
  /** Ordinary Tool.make's encoded value, distinct from structured result channels. */
  legacyValue: Schema.optionalKey(Schema.Json),
})
export type WireReceipt = typeof WireReceiptSchema.Type
export type Admission =
  | { readonly _tag: 'admitted'; readonly handle: Handle }
  | { readonly _tag: 'denied'; readonly receipt: WireReceipt }

export class InvocationError extends Schema.TaggedError<InvocationError>()('InvocationError', {
  reason: Schema.Literals([
    'invalidKey',
    'conflict',
    'incompatibleContract',
    'missingResult',
    'revoked',
    'denied',
    'unsupportedArguments',
  ]),
  message: Schema.String,
}) {}
export type Failure =
  | InvocationError
  | Execution.Failure
  | TaskExecution.Failure
  | RegistryError
  | StorageFormat.Failure

export interface Access {
  readonly check: Effect.Effect<void, Failure>
  readonly admit: (
    tx: Transaction.Transaction,
    request: Request,
  ) => Effect.Effect<Admission, Failure>
  readonly result: (handle: Handle) => Effect.Effect<Option.Option<WireReceipt>, Failure>
  readonly wait: (handle: Handle) => Effect.Effect<WireReceipt, Failure>
  /**
   * Retains the exact selected schema-token owner through asynchronous encode/wait/decode.
   * Denials call onDenied before any codec; without it, they fail with reason denied.
   * Both callbacks retain their services/errors and the current invocation epoch.
   */
  readonly withTool: <A, E, R, B = never, E2 = never, R2 = never>(
    declaration: Tool.Any,
    use: () => Effect.Effect<A, E, R>,
    third?: {
      readonly options?: Options
      readonly onDenied?: (receipt: WireReceipt) => Effect.Effect<B, E2, R2>
    },
  ) => Effect.Effect<A | B, E | E2 | Failure, R | R2>
  /** Dynamic callers receive wire data, never an erased decoded generic. */
  readonly invokeTool: (
    name: string,
    args: Schema.JsonObject,
    options?: Options,
  ) => Effect.Effect<WireReceipt, Failure>
}
const unavailable = Effect.fail(
  new ExecutionError({
    reason: 'unsupported',
    operation: 'tool.invoke',
    message: 'This invocation has no native tool integration',
  }),
)
/** Standalone schedulers and direct reporting services cannot admit native tool work. */
export const unsupported: Access = {
  check: unavailable,
  admit: () => unavailable,
  result: () => unavailable,
  wait: () => unavailable,
  withTool: () => unavailable,
  invokeTool: () => unavailable,
}

type Projected<A> = A extends { readonly structuredOutput: infer V }
  ? V
  : A extends ToolResult.Output<infer V>
    ? V | undefined
    : A
type Common = Omit<WireReceipt, 'status' | 'result' | 'legacyValue'> & {
  readonly details?: Schema.Json
  readonly diagnostics: ReadonlyArray<ToolResult.Diagnostic>
  readonly spend?: Usage.Cost
}
/**
 * Declared schemas decode structured values under the consumer's services. Content fallback
 * exposes the persisted native Prompt representation: URL data roundtrips as its href string,
 * while byte data uses the existing tagged byte codec. This does not retain live object identity.
 */
export type DecodedReceipt<T extends Tool.Any> = Common &
  (
    | {
        readonly status: 'succeeded'
        readonly valueKind: 'decoded'
        readonly value: Projected<Tool.Success<T>>
      }
    | {
        readonly status: 'failed'
        readonly valueKind: 'decoded'
        readonly error: Projected<Tool.Failure<T>>
      }
    | {
        readonly status: 'succeeded' | 'failed'
        readonly valueKind: 'content'
        readonly value: ReturnType<typeof ToolResult.contentValue>
      }
    | {
        readonly status: Exclude<WireReceipt['status'], 'succeeded'>
        readonly valueKind: 'unavailable'
      }
  )

const snapshotLimits = { nodes: 4096, properties: 65536, bytes: 8 * 1024 * 1024 }
const typedArrayPrototype = Object.getPrototypeOf(Uint8Array.prototype)
const bufferOf = (value: object): ArrayBuffer =>
  Object.getOwnPropertyDescriptor(typedArrayPrototype, 'buffer')!.get!.call(value)
const offsetOf = (value: object): number =>
  Object.getOwnPropertyDescriptor(typedArrayPrototype, 'byteOffset')!.get!.call(value)
const lengthOf = (value: object): number =>
  Object.getOwnPropertyDescriptor(typedArrayPrototype, 'byteLength')!.get!.call(value)
const bufferLengthOf = (value: object): number =>
  Object.getOwnPropertyDescriptor(ArrayBuffer.prototype, 'byteLength')!.get!.call(value)

/** Neutral ownership only: schema transformations belong to the later permitted lease. */
const snapshot = (argumentsValue: unknown, options: Options | undefined) => {
  const copies = new WeakMap<object, object>()
  const pending: Array<{ readonly source: object; readonly target: object }> = []
  let nodes = 0
  let properties = 0
  let bytes = 0
  const fail = (message: string): never => {
    throw new InvocationError({ reason: 'unsupportedArguments', message })
  }
  const count = () => {
    if (++nodes > snapshotLimits.nodes) fail('Argument snapshot exceeds 4096 object/buffer nodes')
  }
  const copy = (value: unknown): unknown => {
    if (typeof value === 'function' || typeof value === 'symbol')
      return fail('Functions and symbols are outside the argument snapshot domain')
    if (typeof value !== 'object' || value === null) return value
    const existing = copies.get(value)
    if (existing !== undefined) return existing
    count()
    const prototype = Object.getPrototypeOf(value)
    let result: object
    if (prototype === Date.prototype) {
      const time = Date.prototype.getTime.call(value)
      if (!Number.isFinite(time)) return fail('Invalid Date arguments cannot be snapshotted')
      result = new Date(time)
    } else if (prototype === Uint8Array.prototype) {
      const backing: ArrayBuffer = bufferOf(value)
      if (Object.getPrototypeOf(backing) !== ArrayBuffer.prototype)
        return fail('Only ordinary ArrayBuffer-backed byte views are supported')
      if (Reflect.ownKeys(backing).length !== 0)
        return fail('Byte backing buffers with additional own properties are unsupported')
      let owned = copies.get(backing)
      if (owned === undefined) {
        count()
        const size: number = bufferLengthOf(backing)
        bytes += size
        if (bytes > snapshotLimits.bytes)
          return fail('Argument snapshot exceeds 8 MiB of byte storage')
        const buffer = new ArrayBuffer(size)
        Uint8Array.prototype.set.call(new Uint8Array(buffer), new Uint8Array(backing))
        owned = buffer
        copies.set(backing, buffer)
      }
      // This internal buffer was created above; no caller instance or private fields are restored.
      if (!(owned instanceof ArrayBuffer)) return fail('Invalid internal byte snapshot')
      result = new Uint8Array(owned, offsetOf(value), lengthOf(value))
    } else if (Array.isArray(value) && prototype === Array.prototype) result = []
    else if (prototype === Object.prototype || prototype === null) result = Object.create(prototype)
    else return fail('Custom classes and exotic objects are outside the argument snapshot domain')
    copies.set(value, result)
    pending.push({ source: value, target: result })
    return result
  }
  const detached = copy(argumentsValue)
  const detachedOptions = copy(options)
  for (let position = 0; position < pending.length; position++) {
    const { source, target } = pending[position]!
    const keys = Reflect.ownKeys(source)
    properties += keys.length
    if (properties > snapshotLimits.properties)
      fail('Argument snapshot exceeds 65536 own properties')
    for (const key of keys) {
      const descriptor = Object.getOwnPropertyDescriptor(source, key)
      if (descriptor === undefined || !Object.hasOwn(descriptor, 'value'))
        fail('Accessors are outside the argument snapshot domain')
      Object.defineProperty(target, key, { ...descriptor, value: copy(descriptor!.value) })
    }
  }
  if (detachedOptions === undefined) return { arguments: detached, options: undefined }
  if (typeof detachedOptions !== 'object' || detachedOptions === null)
    return fail('Invocation options must be a plain record')
  const own = (record: object, key: string): unknown =>
    Object.hasOwn(record, key) ? Reflect.get(record, key) : undefined
  const key = own(detachedOptions, 'key')
  const progress = own(detachedOptions, 'progress')
  const binding = own(detachedOptions, 'binding')
  if (key !== undefined && typeof key !== 'string') return fail('Invocation key must be a string')
  if (progress !== undefined && typeof progress !== 'boolean')
    return fail('Invocation progress must be a boolean')
  let ownedBinding: Options['binding']
  if (binding !== undefined) {
    if (typeof binding !== 'object' || binding === null)
      return fail('Invocation binding must be a plain record')
    const id = own(binding, 'id')
    const version = own(binding, 'version')
    if (typeof id !== 'string' || typeof version !== 'number')
      return fail('Invocation binding requires a string id and numeric version')
    ownedBinding = { id, version }
  }
  return {
    arguments: detached,
    options: {
      ...(key === undefined ? {} : { key }),
      ...(progress === undefined ? {} : { progress }),
      ...(ownedBinding === undefined ? {} : { binding: ownedBinding }),
    },
  }
}

/**
 * Concrete consumer codecs remain in R. At execution entry, arguments/options detach before
 * asynchronous work. Supported values are non-Proxy plain/null records, arrays, Date and
 * Uint8Array, with aliases/cycles retained. Limits are 4096 object/buffer nodes, 65536 own
 * properties and 8 MiB of byte storage. Accessors, custom classes, detached/invalid built-ins
 * and shared-memory bytes fail with unsupportedArguments. Proxies are outside this contract:
 * their inspection traps may run; this copier is not a security sandbox. Denial runs no schema
 * codec. Owner validation never uses the consumer's service context.
 */
export const invoke = <T extends Tool.Any>(
  execution: { readonly invocation?: Access },
  declaration: T,
  args: Tool.Parameters<T>,
  options?: Options,
): Effect.Effect<
  DecodedReceipt<T>,
  Failure,
  | T['parametersSchema']['EncodingServices']
  | T['successSchema']['DecodingServices']
  | T['failureSchema']['DecodingServices']
> =>
  Effect.suspend(() => {
    const access = execution.invocation ?? unsupported
    return Effect.try({
      try: () => snapshot(args, options),
      catch: (cause) =>
        new InvocationError({
          reason: 'unsupportedArguments',
          message:
            cause instanceof InvocationError
              ? cause.message
              : 'Argument snapshot inspection or copy failed',
        }),
    }).pipe(
      Effect.flatMap((detached) =>
        Effect.gen(function* () {
          yield* access.check
          const ownedOptions = detached.options
          return yield* access.withTool(
            declaration,
            () =>
              Effect.gen(function* () {
                const schema = declaration.parametersSchema as Schema.ConstraintCodec<
                  Tool.Parameters<T>,
                  unknown,
                  T['parametersSchema']['DecodingServices'],
                  T['parametersSchema']['EncodingServices']
                >
                const owned = yield* Schema.decodeUnknownEffect(Schema.toType(schema))(
                  detached.arguments,
                )
                const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(schema))(owned)
                const parameters = yield* Schema.decodeUnknownEffect(Schema.JsonObject)(encoded)
                yield* access.check
                const receipt = yield* access.invokeTool(declaration.name, parameters, ownedOptions)
                return yield* decodeReceipt(access, declaration, receipt)
              }),
            {
              ...(ownedOptions === undefined ? {} : { options: ownedOptions }),
              onDenied: (receipt) => decodeReceipt(access, declaration, receipt),
            },
          )
        }),
      ),
    )
  })

const decodeReceipt = <T extends Tool.Any>(
  access: Access,
  declaration: T,
  receipt: WireReceipt,
): Effect.Effect<
  DecodedReceipt<T>,
  Failure,
  T['successSchema']['DecodingServices'] | T['failureSchema']['DecodingServices']
> =>
  Effect.gen(function* () {
    yield* access.check
    const { result, legacyValue, status, ...identity } = receipt
    const { details, diagnostics, spend } = result
    const common = {
      ...identity,
      ...(details === undefined ? {} : { details }),
      diagnostics,
      ...(spend === undefined ? {} : { spend }),
    }
    if (status !== 'succeeded' && status !== 'failed')
      return { ...common, status, valueKind: 'unavailable' as const }
    if (legacyValue === undefined && result.valueKind === 'content')
      return { ...common, status, valueKind: 'content' as const, value: result.structuredOutput }
    const schema = (
      status === 'succeeded' ? declaration.successSchema : declaration.failureSchema
    ) as Schema.ConstraintCodec<
      unknown,
      unknown,
      T['successSchema']['DecodingServices'] | T['failureSchema']['DecodingServices'],
      T['successSchema']['EncodingServices'] | T['failureSchema']['EncodingServices']
    >
    const wire = ToolContract.hasResultChannels(declaration)
      ? {
          content: [],
          ...(result.structuredOutput === undefined
            ? {}
            : { structuredOutput: result.structuredOutput }),
          ...(details === undefined ? {} : { details }),
          diagnostics,
          ...(spend === undefined ? {} : { spend }),
        }
      : legacyValue
    const decoded = yield* Schema.decodeUnknownEffect(Schema.toCodecJson(schema))(wire)
    yield* access.check
    const value = ToolContract.hasResultChannels(declaration)
      ? (yield* Schema.decodeUnknownEffect(
          Schema.Struct({ structuredOutput: Schema.optionalKey(Schema.Unknown) }),
        )(decoded)).structuredOutput
      : decoded
    return status === 'succeeded'
      ? { ...common, status, valueKind: 'decoded' as const, value }
      : { ...common, status, valueKind: 'decoded' as const, error: value }
  }).pipe(
    // Each value crossed the concrete declaration's schema before this conditional type boundary.
    Effect.map((receipt) => receipt as DecodedReceipt<T>),
  )

/** Consume a child from an earlier memo/checkpoint while retaining the matching owner lease. */
export const decode = <T extends Tool.Any>(
  execution: { readonly invocation?: Access },
  declaration: T,
  handle: Handle,
): Effect.Effect<
  DecodedReceipt<T>,
  Failure,
  T['successSchema']['DecodingServices'] | T['failureSchema']['DecodingServices']
> => {
  const access = execution.invocation ?? unsupported
  return access.withTool(
    declaration,
    () =>
      access
        .wait(handle)
        .pipe(Effect.flatMap((receipt) => decodeReceipt(access, declaration, receipt))),
    { onDenied: (receipt) => decodeReceipt(access, declaration, receipt) },
  )
}
