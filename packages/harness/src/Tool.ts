/**
 * Native tool binding, validated projections and replay intent codecs.
 *
 * @since 0.0.0
 */
import * as Result from 'effect/Result'
import { dual } from 'effect/Function'
import * as Arr from 'effect/Array'
import * as Option from 'effect/Option'
import * as SchemaField from './SchemaField.ts'
import * as Cause from 'effect/Cause'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Tool from 'effect/ai/Tool'
import type * as Toolkit from 'effect/ai/Toolkit'
import type * as AiError from 'effect/ai/AiError'
import * as Prompt from 'effect/ai/Prompt'
import {
  ToolError,
  ToolUnavailable,
  ToolInvalidParameters,
  ToolInvalidResult,
  ToolExecution,
} from './ToolError.ts'
import { Invocation, ToolCall, Result as ToolResultSchema, type ToolResult } from './Invocation.ts'
import * as SystemPatch from './SystemPatch.ts'
import * as Output from './Output.ts'
import * as Hook from './Hook.ts'
import * as Serialization from './Serialization.ts'

/**
 * Tool metadata contract.
 *
 * @category models
 * @since 0.0.0
 */
export interface Metadata {
  readonly replay?: 'safe' | 'unsafe' | undefined
  readonly execution?: 'parallel' | 'sequential' | undefined
  readonly output?: Partial<Output.OutputLimits> | undefined
  readonly outputWindow?: boolean | undefined
  readonly repair?: ((args: unknown) => Effect.Effect<unknown, ToolError, Invocation>) | undefined
  readonly project?:
    | ((
        result: unknown,
        encoded: unknown,
        isFailure: boolean,
      ) => Effect.Effect<ToolResult, ToolError>)
    | undefined
}
/**
 * Annotation reference for replay, execution, output and typed projection policies.
 *
 * @category annotations
 * @since 0.0.0
 */
export const Metadata = Context.Reference<Metadata>('@effect-harness/harness/Tool/Metadata', {
  defaultValue: () => ({}),
})
/**
 * Tool native result contract.
 *
 * @category models
 * @since 0.0.0
 */
export type NativeResult = Registration.NativeResult
/**
 * Host dependencies are captured at bind time; explicitly declared request services are supplied by the executor.
 *
 * @category models
 * @since 0.0.0
 */
export interface Registration {
  readonly tool: Tool.Any
  readonly metadata: Metadata
  readonly decode: (args: unknown) => Effect.Effect<unknown, ToolError, Invocation | ToolCall>
  readonly encodeArgs: (
    args: unknown,
  ) => Effect.Effect<Schema.Json, ToolError, Invocation | ToolCall>
  readonly execute: (
    args: unknown,
    id: string,
  ) => Effect.Effect<NativeResult, ToolError, Invocation | ToolCall>
}
const error = (
  self: Tool.Any,
  Reason: typeof ToolInvalidParameters | typeof ToolInvalidResult | typeof ToolExecution,
  cause: unknown,
): ToolError =>
  new ToolError({
    reason: new Reason({ name: self.name, message: Serialization.errorText(cause), cause }),
  })
/** Bind ordinary Toolkit.toLayer handlers and codec services. Dynamic lookup erases generic tool names only at this boundary. */
type Captured<Tools extends Record<string, Tool.Any>, RequestServices = never> =
  | Tool.HandlersFor<Tools>
  | Exclude<
      | Tool.HandlerServices<Tools[keyof Tools]>
      | Tool.ParametersEncodingServices<Tools[keyof Tools]>,
      Invocation | ToolCall | RequestServices
    >
const bindImpl = Effect.fnUntraced(function* <
  Tools extends Record<string, Tool.Any>,
  RequestServices = never,
>(
  toolkit: Toolkit.Toolkit<Tools>,
  metadata: Readonly<Record<string, Metadata>> = {},
  requestServices: ReadonlyArray<Context.Key<RequestServices, unknown>> = [],
): Effect.fn.Return<Array<Registration>, never, Captured<Tools, RequestServices>> {
  const captured = yield* Effect.context<Captured<Tools, RequestServices>>()
  const registrations: Array<Registration> = []
  for (const tool of Object.values(toolkit.tools)) {
    // Native Toolkit uses tool.id as its handler Context key. The Handler interface intentionally erases schemas.
    type Services = Captured<Tools, RequestServices> | Invocation | ToolCall | RequestServices
    type Parameters = Tool.Parameters<Tools[keyof Tools]>
    type Failure =
      | Tool.Failure<Tools[keyof Tools]>
      | AiError.AiError
      | AiError.AiErrorReason
      | ToolError
    type Success = Tool.Success<Tools[keyof Tools]>
    const nativeHandler = Context.getOption(
      captured,
      Context.Service<{
        readonly context: Context.Context<never>
        readonly handler: (
          params: Parameters,
          context: Toolkit.HandlerContext<Tools[keyof Tools]>,
        ) => Effect.Effect<Success, Failure, Services>
      }>(tool.id),
    )
    const handler = Option.getOrElse(nativeHandler, () => ({
      context: Context.empty(),
      handler: () =>
        Effect.fail(
          new ToolError({
            reason: new ToolUnavailable({
              name: tool.name,
              message: 'Provider-defined tool has no local handler',
            }),
          }),
        ),
    }))
    // The dynamic registry erases heterogeneous schema types, but bind's requirements capture every codec service.
    const parameters = tool.parametersSchema as Schema.Codec<
      Parameters,
      unknown,
      Services,
      Services
    >
    const success = tool.successSchema as Schema.Codec<unknown, unknown, Services, Services>
    const failure = tool.failureSchema as Schema.Codec<unknown, unknown, Services, Services>
    const info = Object.assign(
      {},
      Context.get(tool.annotations, Metadata),
      Object.hasOwn(metadata, tool.name) ? metadata[tool.name] : undefined,
    )
    const provide = <A, E, R>(
      self: Effect.Effect<A, E, R>,
    ): Effect.Effect<A, E | ToolError, Invocation | ToolCall> =>
      Effect.flatMap(
        Effect.context<Invocation | ToolCall>(),
        (current): Effect.Effect<A, E | ToolError> => {
          for (const service of requestServices)
            if (!current.mapUnsafe.has(service.key))
              return Effect.fail(
                new ToolError({
                  reason: new ToolUnavailable({
                    name: tool.name,
                    message: `Request service ${service.key} is absent`,
                  }),
                }),
              )
          // R was erased by Tool.Any. bind's Captured requirements plus native handler context satisfy codec services;
          // request-local Invocation/ToolCall override captured implementations. Context reconstruction confines erasure here.
          return Effect.provideContext(
            self,
            Context.makeUnsafe<R>(
              Context.merge(Context.merge(handler.context, captured), current).mapUnsafe,
            ),
          )
        },
      )
    const decode = (args: unknown) =>
      provide(Schema.decodeUnknownEffect(parameters)(args)).pipe(
        Effect.mapError((cause) => error(tool, ToolInvalidParameters, cause)),
      )
    const encodeArgs = (self: unknown) =>
      provide(Schema.encodeEffect(parameters)(self as Parameters)).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Schema.Json)),
        Effect.mapError((cause) => error(tool, ToolInvalidParameters, cause)),
      )
    const execute = Effect.fnUntraced(function* (args: unknown, id: string) {
      const invocation = yield* Invocation
      const api = yield* ToolCall
      const preliminary = (self: unknown): Effect.Effect<void> =>
        provide(Schema.encodeEffect(success)(self)).pipe(
          Effect.mapError((cause) => error(tool, ToolInvalidResult, cause)),
          Effect.flatMap((encoded) =>
            project({ tool, metadata: info }, { result: self, encoded, isFailure: false }),
          ),
          Effect.flatMap((value) =>
            api.preliminary === undefined
              ? invocation.progress({
                  output:
                    value.content === undefined
                      ? undefined
                      : Arr.filterMap(value.content, (part) =>
                          part.type === 'text' ? Result.succeed(part.text) : Result.failVoid,
                        ).join(''),
                  details: value.details,
                  ...(value.diagnostics === undefined ? {} : { diagnostics: value.diagnostics }),
                })
              : api.preliminary(value),
          ),
          Effect.provideService(ToolCall, api),
          Hook.recover,
          Effect.asVoid,
          Effect.provideService(Invocation, invocation),
        )

      const native = yield* provide(
        Effect.suspend(() => handler.handler(args as Parameters, { toolCallId: id, preliminary })),
      ).pipe(
        Effect.map((result): NativeResult => ({ result, encoded: undefined, isFailure: false })),
        Effect.catch((cause) =>
          tool.failureMode === 'return'
            ? Effect.succeed({ result: cause, encoded: undefined, isFailure: true })
            : Effect.fail(error(tool, ToolExecution, cause)),
        ),
      )
      const schema = native.isFailure ? failure : success
      const encoded = yield* provide(Schema.encodeEffect(schema)(native.result)).pipe(
        Effect.mapError((cause) => error(tool, ToolInvalidResult, cause)),
      )
      return { ...native, encoded }
    })
    registrations.push({ tool, metadata: info, decode, encodeArgs, execute })
  }
  return registrations
})
/**
 * Captures host dependencies while preserving invocation-time service requirements.
 *
 * @category combinators
 * @since 0.0.0
 */
export const bind: {
  <RequestServices = never>(
    metadata?: Readonly<Record<string, Metadata>>,
    requestServices?: ReadonlyArray<Context.Key<RequestServices, unknown>>,
  ): <Tools extends Record<string, Tool.Any>>(
    self: Toolkit.Toolkit<Tools>,
  ) => Effect.Effect<Array<Registration>, never, Captured<Tools, RequestServices>>
  <Tools extends Record<string, Tool.Any>, RequestServices = never>(
    self: Toolkit.Toolkit<Tools>,
    metadata?: Readonly<Record<string, Metadata>>,
    requestServices?: ReadonlyArray<Context.Key<RequestServices, unknown>>,
  ): Effect.Effect<Array<Registration>, never, Captured<Tools, RequestServices>>
} = dual(
  (args) =>
    args[0] != null &&
    (typeof args[0] === 'object' || typeof args[0] === 'function') &&
    'toLayer' in args[0],
  bindImpl,
)
/**
 * Validates owned tool projections without throwing inside Effect.
 *
 * @category combinators
 * @since 0.0.0
 */
export const decodeResult = (name: string, value: unknown): Effect.Effect<ToolResult, ToolError> =>
  Schema.decodeUnknownEffect(ToolResultSchema)(value).pipe(
    Effect.mapError(
      (cause) =>
        new ToolError({
          reason: new ToolInvalidResult({ name, message: cause.message, cause }),
        }),
    ),
  )
/**
 * Run the selected effectful projector; native encoded fallback keeps its existing display policy.
 *
 * @category combinators
 * @since 0.0.0
 */
export const project = (
  self: Pick<Registration, 'tool' | 'metadata'>,
  native: NativeResult,
): Effect.Effect<ToolResult, ToolError> =>
  Effect.suspend(() =>
    self.metadata.project === undefined
      ? Effect.succeed(defaultProject(native.result, native.encoded, native.isFailure))
      : self.metadata.project(native.result, native.encoded, native.isFailure),
  ).pipe(
    Effect.mapError((cause) =>
      cause.reason._tag === 'ToolInvalidResult'
        ? cause
        : error(self.tool, ToolInvalidResult, cause),
    ),
  )
/**
 * Projects native encoded content with the explicit unencodable display policy.
 *
 * @category combinators
 * @since 0.0.0
 */
export function defaultProject(_result: unknown, encoded: unknown, isFailure: boolean): ToolResult {
  return {
    content: [
      Prompt.textPart({
        text: Serialization.display(encoded),
      }),
    ],
    isError: isFailure,
  }
}
/**
 * Schema for declaration.
 *
 * @category schemas
 * @since 0.0.0
 */
export function declaration(
  self: Registration,
): Effect.Effect<SystemPatch.ToolDeclaration, Schema.SchemaError> {
  return Schema.decodeUnknownEffect(SystemPatch.ToolDeclaration)({
    name: self.tool.name,
    ...(self.tool.description === undefined ? {} : { description: self.tool.description }),
    parameters: Tool.getJsonSchema(self.tool),
    ...(Tool.isProviderDefined(self.tool)
      ? {
          provider: {
            id: self.tool.id,
            name: self.tool.providerName,
            args: self.tool.args,
          },
        }
      : {}),
  })
}
/**
 * Schema for intent.
 *
 * @category schemas
 * @since 0.0.0
 */
export const Intent = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  args: Schema.Json,
  encodedArgs: SchemaField.optional(Schema.Json),
  replay: Schema.Literals(['safe', 'unsafe']),
})
/**
 * Tool intent contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Intent = typeof Intent.Type
/**
 * Schema for execution.
 *
 * @category schemas
 * @since 0.0.0
 */
export const Execution = Schema.Struct({
  outcome: Schema.Literals(['completed', 'failed', 'interrupted', 'unavailable']),
  result: ToolResultSchema,
})
/**
 * Tool execution contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Execution = typeof Execution.Type
/** Persist decoded intent separately from provider encoded params. Native transforms are not re-applied during replay. */
const makeIntentImpl = Effect.fnUntraced(function* (
  registration: Registration,
  options: makeIntent.Options,
): Effect.fn.Return<Intent, ToolError> {
  const { id, decoded, encoded } = options
  const args = yield* Schema.decodeUnknownEffect(Schema.Json)(decoded).pipe(
    Effect.mapError((cause) => error(registration.tool, ToolInvalidParameters, cause)),
  )
  return {
    id,
    name: registration.tool.name,
    args,
    ...(encoded === undefined ? {} : { encodedArgs: encoded }),
    replay: registration.metadata.replay ?? 'unsafe',
  } satisfies Intent
})
/**
 * Creates a validated replay intent from decoded and provider-encoded arguments.
 *
 * @category constructors
 * @since 0.0.0
 */
export const makeIntent: {
  (options: makeIntent.Options): (self: Registration) => Effect.Effect<Intent, ToolError>
  (self: Registration, options: makeIntent.Options): Effect.Effect<Intent, ToolError>
} = dual(2, makeIntentImpl)
/**
 * Creates the model-visible result for an interrupted intent.
 *
 * @category combinators
 * @since 0.0.0
 */
export function interruption(self: Intent): ToolResult {
  return {
    isError: true,
    content: [
      Prompt.textPart({ text: 'Tool call interrupted before a durable result was recorded.' }),
    ],
    details: { reason: 'interrupted', name: self.name },
  }
}
/**
 * Creates the model-visible result for an unavailable tool.
 *
 * @category combinators
 * @since 0.0.0
 */
export function unavailable(name: string): ToolResult {
  return {
    isError: true,
    content: [Prompt.textPart({ text: `Tool ${name} is not available` })],
    details: { reason: 'unavailable', name },
  }
}
/** Convert handler failures into failed domain results; cancellation remains cancellation. */
const settleFailureImpl = (
  self: Cause.Cause<ToolError>,
  partial: ToolResult,
): Effect.Effect<ToolResult> =>
  Cause.hasInterrupts(self)
    ? Effect.failCause(Cause.fromReasons(self.reasons.filter(Cause.isInterruptReason)))
    : Effect.succeed({
        ...partial,
        isError: true,
        diagnostics: [
          ...(partial.diagnostics ?? []),
          { kind: 'tool_error', message: Serialization.errorText(Cause.squash(self)) },
        ],
        details: {
          reason: 'execution',
          error: Serialization.errorText(Cause.squash(self)),
          partial: partial.details ?? null,
        },
      })
/**
 * Converts a caught tool cause to the terminal result while propagating interruption.
 *
 * @category combinators
 * @since 0.0.0
 */
export const settleFailure: {
  (partial: ToolResult): (self: Cause.Cause<ToolError>) => Effect.Effect<ToolResult>
  (self: Cause.Cause<ToolError>, partial: ToolResult): Effect.Effect<ToolResult>
} = dual(2, settleFailureImpl)
/** Whole-result text truncation preserves non-text parts and emits one bounded text block. */
function boundResultImpl(self: ToolResult, limits: Output.OutputLimits): ToolResult {
  const content = self.content ?? []
  const text = Arr.filterMap(content, (part) =>
    part.type === 'text' ? Result.succeed(part.text) : Result.failVoid,
  ).join('')
  const bounded = Output.boundOutput(text, limits)
  if (bounded.droppedBytes === 0 && bounded.droppedLines === 0) return self
  const anchor =
    limits.retain === 'head'
      ? Arr.findFirstIndex(content, (part) => part.type === 'text')
      : Arr.findLastIndex(content, (part) => part.type === 'text')
  return {
    ...self,
    content: Arr.filterMap(content, (part, index): Result.Result<Prompt.UserMessagePart, void> => {
      if (part.type !== 'text') return Result.succeed(part)
      return Option.contains(anchor, index)
        ? Result.succeed({ ...part, text: bounded.text })
        : Result.failVoid
    }),
    diagnostics: [
      ...(self.diagnostics ?? []),
      {
        kind: 'truncated',
        detail: { droppedBytes: bounded.droppedBytes, droppedLines: bounded.droppedLines },
      },
    ],
  }
}
/**
 * Bounds model-visible text while preserving non-text native content.
 *
 * @category combinators
 * @since 0.0.0
 */
export const boundResult: {
  (limits: Output.OutputLimits): (self: ToolResult) => ToolResult
  (self: ToolResult, limits: Output.OutputLimits): ToolResult
} = dual(2, boundResultImpl)
/**
 * Selects sequential or parallel execution from settings and tool metadata.
 *
 * @category combinators
 * @since 0.0.0
 */
export function executionMode(
  self: ReadonlyArray<Registration>,
  mode: 'parallel' | 'sequential',
): 'parallel' | 'sequential' {
  return mode === 'sequential' ||
    self.some((registration) => registration.metadata.execution === 'sequential')
    ? 'sequential'
    : 'parallel'
}

/**
 * Controls are reduced in original call order, not finish order; noncompleted/unavailable slots defeat unanimity.
 *
 * @category combinators
 * @since 0.0.0
 */
export function controls(self: ReadonlyArray<Execution>): {
  readonly terminate: boolean
  readonly reset?: { readonly note?: string | undefined } | undefined
  readonly addTools: ReadonlyArray<string>
} {
  let reset: { readonly note?: string | undefined } | undefined
  const names = new Set<string>()
  for (const execution of self) {
    if (execution.outcome !== 'completed') continue
    if (execution.result.control?.reset !== undefined) reset = execution.result.control.reset
    for (const name of execution.result.control?.addTools ?? []) names.add(name)
  }
  return {
    terminate:
      self.length > 0 &&
      self.every(
        (execution) =>
          execution.outcome === 'completed' && execution.result.control?.terminate === true,
      ),
    ...(reset === undefined ? {} : { reset }),
    addTools: [...names],
  }
}

/**
 * Returns the configured tool output limits merged with defaults.
 *
 * @category combinators
 * @since 0.0.0
 */
export function outputLimits(self: Metadata): Output.OutputLimits {
  return {
    maxBytes: self.output?.maxBytes ?? Output.defaults.maxBytes,
    maxLines: self.output?.maxLines ?? Output.defaults.maxLines,
    retain: self.output?.retain ?? Output.defaults.retain,
  }
}

/**
 * Checks whether an unknown value satisfies the Intent contract.
 *
 * @category guards
 * @since 0.0.0
 */
export const isIntent: (u: unknown) => u is Intent = Schema.is(Intent)

/**
 * Checks whether an unknown value satisfies the Execution contract.
 *
 * @category guards
 * @since 0.0.0
 */
export const isExecution: (u: unknown) => u is Execution = Schema.is(Execution)

/**
 * Type contracts owned by `makeIntent`.
 *
 * @category utility types
 * @since 0.0.0
 */
export declare namespace makeIntent {
  /**
   * Configuration accepted by makeIntent.
   *
   * @category models
   * @since 0.0.0
   */
  interface Options {
    readonly id: string
    readonly decoded: unknown
    readonly encoded?: Schema.Json | undefined
  }
}

/**
 * Type contracts owned by `Registration`.
 *
 * @category utility types
 * @since 0.0.0
 */
export declare namespace Registration {
  /**
   * Registration native result type contract.
   *
   * @category models
   * @since 0.0.0
   */
  interface NativeResult {
    readonly result: unknown
    readonly encoded: unknown
    readonly isFailure: boolean
  }
}
