import * as Predicate from 'effect/Predicate'
/**
 * Native tool binding, validated projections and replay intent codecs.
 */
import * as Record from 'effect/Record'

import * as Result from 'effect/Result'
import { dual } from 'effect/Function'
import * as Array from 'effect/Array'
import * as Option from 'effect/Option'
import * as SchemaField from './SchemaField.ts'
import * as Cause from 'effect/Cause'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as SchemaIssue from 'effect/SchemaIssue'
import * as Tool from 'effect/ai/Tool'
import type * as Toolkit from 'effect/ai/Toolkit'
import type * as AiError from 'effect/ai/AiError'
import * as Prompt from 'effect/ai/Prompt'
import {
  ToolError,
  ToolUnavailableError,
  ToolInvalidParametersError,
  ToolInvalidResultError,
  ToolExecutionError,
} from './ToolError.ts'
import {
  Invocation,
  ToolCall,
  Result as ToolResultSchema,
  type Result as InvocationResult,
} from './Invocation.ts'
import * as SystemPatch from './SystemPatch.ts'
import * as Output from './Output.ts'
import * as Hook from './Hook.ts'
import * as Serialization from './Serialization.ts'

/**
 * Policies controlling harness execution and projection of a native tool.
 *
 * **Details**
 *
 * replay defaults to unsafe. A sequential tool makes its whole batch sequential. Output
 * options override library limits. repair can normalize decoded arguments; project maps
 * native results to harness content and metadata.
 *
 * **Gotchas**
 *
 * A safe replay flag permits re-execution after an intent without a saved receipt. It does
 * not make external side effects exactly once.
 *
 * @category models
 */
export interface Metadata {
  /**
   * Recovery policy; unsafe is the default and safe permits repeating a call without a saved
   * receipt.
   */
  readonly replay?: 'safe' | 'unsafe' | undefined
  /**
   * Batch execution policy; a sequential registration makes the selected batch sequential.
   */
  readonly execution?: 'parallel' | 'sequential' | undefined
  /**
   * Optional overrides for default byte, line and head/tail output limits.
   */
  readonly output?: Partial<Output.Limits> | undefined
  /**
   * Selects bounded-window output reporting for this registration.
   */
  readonly outputWindow?: boolean | undefined
  /**
   * Normalizes decoded arguments before execution in the current Invocation context.
   */
  readonly repair?: ((args: unknown) => Effect.Effect<unknown, ToolError, Invocation>) | undefined
  /**
   * Maps a handler result and encoded representation into harness tool content and metadata.
   */
  readonly project?:
    | ((
        result: unknown,
        encoded: unknown,
        isFailure: boolean,
      ) => Effect.Effect<InvocationResult, ToolError>)
    | undefined
}
/**
 * Annotation reference for native-tool replay, execution, output and projection policies.
 *
 * **Details**
 *
 * Starts with no overrides. bind merges these annotations with metadata supplied by tool
 * name; named overrides take precedence.
 *
 * @category annotations
 */
export const Metadata = Context.Reference<Metadata>('effect-harness/ToolRegistration/Metadata', {
  defaultValue: () => ({}),
})
/**
 * Native tool declaration with captured handler, codecs and harness policies.
 *
 * **Details**
 *
 * decode validates provider arguments, encodeArgs records provider representation, and
 * execute invokes the captured handler with dynamic Invocation and ToolCall services. Use
 * bind to construct registrations.
 *
 * @category models
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
  ) => Effect.Effect<Registration.NativeResult, ToolError, Invocation | ToolCall>
}
const error = (
  self: Tool.Any,
  Reason:
    | typeof ToolInvalidParametersError
    | typeof ToolInvalidResultError
    | typeof ToolExecutionError,
  cause: unknown,
): ToolError =>
  new ToolError({
    reason: new Reason({ name: self.name, message: Serialization.errorText(cause), cause }),
  })
/** Bind ordinary Toolkit.toLayer handlers and codec services. Dynamic lookup erases generic tool names only at this boundary. */
type Captured<Tools extends Record<string, Tool.Any>, RRequestServices = never> =
  | Tool.HandlersFor<Tools>
  | Exclude<
      | Tool.HandlerServices<Tools[keyof Tools]>
      | Tool.ParametersEncodingServices<Tools[keyof Tools]>,
      Invocation | ToolCall | RRequestServices
    >
const bindImpl = Effect.fnUntraced(function* <
  Tools extends Record<string, Tool.Any>,
  RRequestServices = never,
>(
  toolkit: Toolkit.Toolkit<Tools>,
  metadata: Readonly<Record<string, Metadata>> = {},
  requestServices: ReadonlyArray<Context.Key<RRequestServices, unknown>> = [],
): Effect.fn.Return<Array<Registration>, never, Captured<Tools, RRequestServices>> {
  const captured = yield* Effect.context<Captured<Tools, RRequestServices>>()
  const registrations: Array<Registration> = []
  for (const tool of Record.values(toolkit.tools)) {
    // Native Toolkit uses tool.id as its handler Context key. The Handler interface intentionally erases schemas.
    type Services = Captured<Tools, RRequestServices> | Invocation | ToolCall | RRequestServices
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
            reason: new ToolUnavailableError({
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
    const info = {
      // effect-nit-allow P1-pipeable-data-types: metadata is an open payload; merge own enumerable keys into a plain record, discarding carrier prototypes as object-spread specifies.
      // oxlint-disable-next-line typescript/no-misused-spread -- merge metadata payload fields, not class behavior
      ...Context.get(tool.annotations, Metadata),
      // oxlint-disable-next-line typescript/no-misused-spread -- caller metadata contributes only own enumerable payload fields
      ...(Object.hasOwn(metadata, tool.name) ? metadata[tool.name] : undefined),
    }
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
                  reason: new ToolUnavailableError({
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
              Context.merge(
                Context.merge(handler.context, captured),
                Context.pick(Invocation, ToolCall, ...requestServices)(current),
              ).mapUnsafe,
            ),
          )
        },
      )
    const decode = (args: unknown) =>
      provide(Schema.decodeUnknownEffect(parameters)(args)).pipe(
        Effect.mapError((cause) => error(tool, ToolInvalidParametersError, cause)),
      )
    const encodeArgs = (self: unknown) =>
      provide(Schema.encodeEffect(parameters)(self as Parameters)).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Schema.Json)),
        Effect.mapError((cause) => error(tool, ToolInvalidParametersError, cause)),
      )
    const execute = Effect.fnUntraced(function* (args: unknown, id: string) {
      const invocation = yield* Invocation
      const api = yield* ToolCall
      const preliminary = (self: unknown): Effect.Effect<void> =>
        provide(Schema.encodeEffect(success)(self)).pipe(
          Effect.mapError((cause) => error(tool, ToolInvalidResultError, cause)),
          Effect.flatMap((encoded) =>
            project({ tool, metadata: info }, { result: self, encoded, isFailure: false }),
          ),
          Effect.flatMap((value) =>
            api.preliminary === undefined
              ? invocation.progress({
                  output:
                    value.content === undefined
                      ? undefined
                      : Array.filterMap(value.content, (part) =>
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
        Effect.map((result): Registration.NativeResult => ({
          result,
          encoded: undefined,
          isFailure: false,
        })),
        Effect.catch((cause) =>
          tool.failureMode === 'return'
            ? Effect.succeed({ result: cause, encoded: undefined, isFailure: true })
            : Effect.fail(error(tool, ToolExecutionError, cause)),
        ),
      )
      const schema = native.isFailure ? failure : success
      const encoded = yield* provide(Schema.encodeEffect(schema)(native.result)).pipe(
        Effect.mapError((cause) => error(tool, ToolInvalidResultError, cause)),
      )
      return { ...native, encoded }
    })
    registrations.push({ tool, metadata: info, decode, encodeArgs, execute })
  }
  return registrations
})
/**
 * Captures native Toolkit handlers and host services as harness registrations.
 *
 * **When to use**
 *
 * Use when an ordinary Effect AI Toolkit should participate in harness hooks, output
 * reporting and replay policy.
 *
 * **Details**
 *
 * Provide toolkit.toLayer handlers when binding. Explicit metadata overrides tool
 * annotations. Invocation and ToolCall are supplied dynamically; requestServices declares
 * any additional per-request services.
 *
 * **Gotchas**
 *
 * Missing request services fail with ToolUnavailable at invocation. replay defaults to
 * unsafe; mark safe only when repeating the external action is acceptable.
 *
 * **Example** (Capturing Toolkit handlers)
 *
 * ```ts
 * import { Registry, ToolRegistration } from 'effect-harness'
 * import { Effect, Layer, Schema } from 'effect'
 * import { Tool, Toolkit } from 'effect/ai'
 *
 * const Uppercase = Tool.make('uppercase', {
 *   description: 'Convert text to uppercase.',
 *   parameters: Schema.Struct({ text: Schema.String }),
 *   success: Schema.String,
 * })
 * const toolkit = Toolkit.make(Uppercase)
 * const handlers = toolkit.toLayer({
 *   uppercase: ({ text }) => Effect.succeed(text.toUpperCase()),
 * })
 *
 * // Pure uppercasing can be repeated before a durable receipt is saved.
 * export const registry = Layer.unwrap(
 *   Effect.gen(function* () {
 *     const tools = yield* ToolRegistration.bind(toolkit, {
 *       uppercase: { replay: 'safe' },
 *     })
 *     return Registry.layer([{ name: 'text-tools', tools }])
 *   }),
 * ).pipe(Layer.provide(handlers))
 * ```
 *
 * @category combinators
 */
export const bind: {
  <RRequestServices = never>(
    metadata?: Readonly<Record<string, Metadata>>,
    requestServices?: ReadonlyArray<Context.Key<RRequestServices, unknown>>,
  ): <Tools extends Record<string, Tool.Any>>(
    self: Toolkit.Toolkit<Tools>,
  ) => Effect.Effect<Array<Registration>, never, Captured<Tools, RRequestServices>>
  <Tools extends Record<string, Tool.Any>, RRequestServices = never>(
    self: Toolkit.Toolkit<Tools>,
    metadata?: Readonly<Record<string, Metadata>>,
    requestServices?: ReadonlyArray<Context.Key<RRequestServices, unknown>>,
  ): Effect.Effect<Array<Registration>, never, Captured<Tools, RRequestServices>>
} = dual(
  Predicate.mapInput(Predicate.hasProperty('toLayer'), (args: IArguments) => args[0]),
  bindImpl,
)
/**
 * Validates owned tool projections without throwing inside Effect.
 *
 * @category combinators
 */
export const decodeResult = (
  name: string,
  value: unknown,
): Effect.Effect<InvocationResult, ToolError> =>
  Schema.decodeUnknownEffect(ToolResultSchema)(value).pipe(
    Effect.mapError(
      (cause) =>
        new ToolError({
          reason: new ToolInvalidResultError({ name, message: cause.message, cause }),
        }),
    ),
  )
/**
 * Run the selected effectful projector; native encoded fallback keeps its existing display policy.
 *
 * @category combinators
 */
const projectImpl = (
  self: Pick<Registration, 'tool' | 'metadata'>,
  native: Registration.NativeResult,
): Effect.Effect<InvocationResult, ToolError> =>
  Effect.suspend(() =>
    self.metadata.project === undefined
      ? Effect.succeed(defaultProject(native.result, native.encoded, native.isFailure))
      : self.metadata.project(native.result, native.encoded, native.isFailure),
  ).pipe(
    Effect.mapError((cause) =>
      cause.reason._tag === 'ToolInvalidResultError'
        ? cause
        : error(self.tool, ToolInvalidResultError, cause),
    ),
  )
/**
 * Projects native encoded content with the explicit unencodable display policy.
 *
 * @category combinators
 */
export function defaultProject(
  _result: unknown,
  encoded: unknown,
  isFailure: boolean,
): InvocationResult {
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
 * Builds the model-facing declaration for a registered native tool.
 *
 * **Details**
 *
 * Extracts the native parameter JSON Schema and preserves provider-defined tool metadata.
 * Invalid declaration data fails with SchemaError.
 *
 * @category schemas
 */
export function declaration(
  self: Registration,
): Effect.Effect<SystemPatch.ToolDeclaration, Schema.SchemaError> {
  return Effect.flatMap(
    Effect.try({
      try: () => Tool.getJsonSchema(self.tool),
      catch: (cause) =>
        Schema.isSchemaError(cause)
          ? cause
          : new Schema.SchemaError(
              new SchemaIssue.InvalidValue({ message: Serialization.errorText(cause) }),
            ),
    }),
    (parameters) =>
      Schema.decodeUnknownEffect(SystemPatch.ToolDeclaration)({
        name: self.tool.name,
        ...(self.tool.description === undefined ? {} : { description: self.tool.description }),
        parameters,
        ...(Tool.isProviderDefined(self.tool)
          ? {
              provider: {
                id: self.tool.id,
                name: self.tool.providerName,
                args: self.tool.args,
              },
            }
          : {}),
      }),
  )
}
/**
 * Schema for pinned tool call identity, decoded arguments and external-action replay policy.
 *
 * @category schemas
 */
export const Intent = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  args: Schema.Json,
  encodedArgs: SchemaField.optional(Schema.Json),
  replay: Schema.Literals(['safe', 'unsafe']),
})
/**
 * Pinned tool call identity, decoded arguments and external-action replay policy.
 *
 * @category models
 */
export type Intent = typeof Intent.Type
/**
 * Schema for terminal tool outcome and projected model-visible result.
 *
 * @category schemas
 */
export const Execution = Schema.Struct({
  outcome: Schema.Literals(['completed', 'failed', 'interrupted', 'unavailable']),
  result: ToolResultSchema,
})
/**
 * Terminal tool outcome and projected model-visible result.
 *
 * @category models
 */
export type Execution = typeof Execution.Type
/** Persist decoded intent separately from provider encoded params. Native transforms are not re-applied during replay. */
const makeIntentImpl = Effect.fnUntraced(function* (
  registration: Registration,
  options: makeIntent.Options,
): Effect.fn.Return<Intent, ToolError> {
  const { id, decoded, encoded } = options
  const args = yield* Schema.decodeUnknownEffect(Schema.Json)(decoded).pipe(
    Effect.mapError((cause) => error(registration.tool, ToolInvalidParametersError, cause)),
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
 * Pins call identity, decoded arguments and replay policy for recovery.
 *
 * **Details**
 *
 * Decoded arguments must be JSON-safe. Optional encodedArgs preserves the provider
 * representation without reapplying schema transforms during replay. The registration’s
 * replay policy defaults to unsafe.
 *
 * @category constructors
 */
export const makeIntent: {
  (options: makeIntent.Options): (self: Registration) => Effect.Effect<Intent, ToolError>
  (self: Registration, options: makeIntent.Options): Effect.Effect<Intent, ToolError>
} = dual(2, makeIntentImpl)
/**
 * Creates the model-visible result for an interrupted intent.
 *
 * @category combinators
 */
export function interruption(self: Intent): InvocationResult {
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
 */
export function unavailable(name: string): InvocationResult {
  return {
    isError: true,
    content: [Prompt.textPart({ text: `Tool ${name} is not available` })],
    details: { reason: 'unavailable', name },
  }
}
/** Convert handler failures into failed domain results; cancellation remains cancellation. */
// effect-nit-allow P1-stdlib-collection-replacements: native Cause.fromReasons retains its caller array, which may be sparse. Native filtering skips missing reasons while retaining interruption order; Effect Array.filter would call isInterruptReason(undefined).
const settleFailureImpl = (
  self: Cause.Cause<ToolError>,
  partial: InvocationResult,
): Effect.Effect<InvocationResult> =>
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
 */
export const settleFailure: {
  (partial: InvocationResult): (self: Cause.Cause<ToolError>) => Effect.Effect<InvocationResult>
  (self: Cause.Cause<ToolError>, partial: InvocationResult): Effect.Effect<InvocationResult>
} = dual(2, settleFailureImpl)
/** Whole-result text truncation preserves non-text parts and emits one bounded text block. */
function boundResultImpl(self: InvocationResult, limits: Output.Limits): InvocationResult {
  const content = self.content ?? []
  const text = Array.filterMap(content, (part) =>
    part.type === 'text' ? Result.succeed(part.text) : Result.failVoid,
  ).join('')
  const bounded = Output.boundOutput(text, limits)
  if (bounded.droppedBytes === 0 && bounded.droppedLines === 0) return self
  const anchor =
    limits.retain === 'head'
      ? Array.findFirstIndex(content, (part) => part.type === 'text')
      : Array.findLastIndex(content, (part) => part.type === 'text')
  return {
    ...self,
    content: Array.filterMap(
      content,
      (part, index): Result.Result<Prompt.UserMessagePart, void> => {
        if (part.type !== 'text') return Result.succeed(part)
        return Option.contains(anchor, index)
          ? Result.succeed({ ...part, text: bounded.text })
          : Result.failVoid
      },
    ),
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
 * Bounds model-visible text while preserving non-text content.
 *
 * **Details**
 *
 * Unchanged results are returned directly. On truncation, text is combined at the selected
 * head/tail anchor and one truncation diagnostic is added; media retain their order.
 *
 * @category combinators
 */
export const boundResult: {
  (limits: Output.Limits): (self: InvocationResult) => InvocationResult
  (self: InvocationResult, limits: Output.Limits): InvocationResult
} = dual(2, boundResultImpl)
/**
 * Selects sequential or parallel execution from settings and tool metadata.
 *
 * @category combinators
 */
function executionModeImpl(
  self: ReadonlyArray<Registration>,
  mode: 'parallel' | 'sequential',
): 'parallel' | 'sequential' {
  return mode === 'sequential' ||
    self.some((registration) => registration.metadata.execution === 'sequential')
    ? 'sequential'
    : 'parallel'
}

/**
 * Reduces tool controls in original call order.
 *
 * **Details**
 *
 * The last completed reset wins; added tool names are deduplicated. Termination requires a
 * nonempty batch in which every slot completed and requested termination.
 *
 * **Gotchas**
 *
 * Failed, interrupted or unavailable slots prevent unanimous termination even if another
 * tool requests it.
 *
 * @category combinators
 */
export function controls(self: ReadonlyArray<Execution>): {
  readonly terminate: boolean
  readonly reset?: { readonly note?: string | undefined } | undefined
  readonly addTools: ReadonlyArray<string>
} {
  let reset: { readonly note?: string | undefined } | undefined
  const names: Array<string> = []
  for (const execution of self) {
    if (execution.outcome !== 'completed') continue
    if (execution.result.control?.reset !== undefined) reset = execution.result.control.reset
    for (const name of execution.result.control?.addTools ?? []) names.push(name)
  }
  return {
    terminate:
      self.length > 0 &&
      self.every(
        (execution) =>
          execution.outcome === 'completed' && execution.result.control?.terminate === true,
      ),
    ...(reset === undefined ? {} : { reset }),
    addTools: Array.dedupe(names),
  }
}

/**
 * Returns the configured tool output limits merged with defaults.
 *
 * @category combinators
 */
export function outputLimits(self: Metadata): Output.Limits {
  return {
    maxBytes: self.output?.maxBytes ?? Output.defaults.maxBytes,
    maxLines: self.output?.maxLines ?? Output.defaults.maxLines,
    retain: self.output?.retain ?? Output.defaults.retain,
  }
}

/**
 * Checks whether a value satisfies the decoded `Intent` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isIntent: (u: unknown) => u is Intent = Schema.is(Intent)

/**
 * Checks whether a value satisfies the decoded `Execution` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isExecution: (u: unknown) => u is Execution = Schema.is(Execution)

/**
 * Type-level contracts for `makeIntent`.
 *
 */
export declare namespace makeIntent {
  /**
   * Configuration accepted by makeIntent.
   *
   * @category models
   */
  interface Options {
    readonly id: string
    readonly decoded: unknown
    readonly encoded?: Schema.Json | undefined
  }
}

/**
 * Type-level contracts for `Registration`.
 *
 */
export declare namespace Registration {
  /**
   * Handler result, encoded schema value and failure-mode flag.
   *
   * @category models
   */
  interface NativeResult {
    readonly result: unknown
    readonly encoded: unknown
    readonly isFailure: boolean
  }
}

/** Runs the selected projector for a native tool result.
 * @category combinators
 */
export const project: {
  (
    native: Registration.NativeResult,
  ): (self: Pick<Registration, 'tool' | 'metadata'>) => Effect.Effect<InvocationResult, ToolError>
  (
    self: Pick<Registration, 'tool' | 'metadata'>,
    native: Registration.NativeResult,
  ): Effect.Effect<InvocationResult, ToolError>
} = dual(2, projectImpl)
/** Selects the execution policy for the registered tools.
 * @category combinators
 */
export const executionMode: {
  (
    mode: 'parallel' | 'sequential',
  ): (self: ReadonlyArray<Registration>) => 'parallel' | 'sequential'
  (self: ReadonlyArray<Registration>, mode: 'parallel' | 'sequential'): 'parallel' | 'sequential'
} = dual(2, executionModeImpl)
