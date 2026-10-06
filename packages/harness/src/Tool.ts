import * as Cause from 'effect/Cause'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as AiTool from 'effect/ai/Tool'
import type * as Toolkit from 'effect/ai/Toolkit'
import type * as AiError from 'effect/ai/AiError'
import * as Prompt from 'effect/ai/Prompt'
import {
  ToolError,
  ToolUnavailable,
  ToolInvalidParameters,
  ToolInvalidResult,
  ToolExecution,
} from './Error.ts'
import { Invocation, ToolCall, Result, type ToolResult } from './Invocation.ts'
import type * as ContextView from './Context.ts'
import * as Output from './Output.ts'
import * as Hook from './Hook.ts'
import * as Serialization from './Serialization.ts'

export interface Metadata {
  readonly replay?: 'safe' | 'unsafe' | undefined
  readonly execution?: 'parallel' | 'sequential' | undefined
  readonly output?: Partial<Output.OutputLimits> | undefined
  readonly outputWindow?: boolean | undefined
  readonly repair?: ((args: unknown) => Effect.Effect<unknown, ToolError, Invocation>) | undefined
  readonly project?:
    | ((result: unknown, encoded: unknown, isFailure: boolean) => ToolResult)
    | undefined
}
export const Metadata = Context.Reference<Metadata>('@effect-harness/harness/Tool/Metadata', {
  defaultValue: () => ({}),
})
export interface NativeResult {
  readonly result: unknown
  readonly encoded: unknown
  readonly isFailure: boolean
}
/** Host dependencies are captured at bind time; explicitly declared request services are supplied by the executor. */
export interface Registration {
  readonly tool: AiTool.Any
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
  tool: AiTool.Any,
  Reason: typeof ToolInvalidParameters | typeof ToolInvalidResult | typeof ToolExecution,
  cause: unknown,
): ToolError =>
  new ToolError({
    reason: new Reason({ name: tool.name, message: Serialization.errorText(cause), cause }),
  })
/** Bind ordinary Toolkit.toLayer handlers and codec services. Dynamic lookup erases generic tool names only at this boundary. */
type Captured<Tools extends Record<string, AiTool.Any>, RequestServices = never> =
  | AiTool.HandlersFor<Tools>
  | Exclude<
      | AiTool.HandlerServices<Tools[keyof Tools]>
      | AiTool.ParametersEncodingServices<Tools[keyof Tools]>,
      Invocation | ToolCall | RequestServices
    >
export const bind = <Tools extends Record<string, AiTool.Any>, RequestServices = never>(
  toolkit: Toolkit.Toolkit<Tools>,
  metadata: Readonly<Record<string, Metadata>> = {},
  requestServices: ReadonlyArray<Context.Key<RequestServices, unknown>> = [],
): Effect.Effect<ReadonlyArray<Registration>, never, Captured<Tools, RequestServices>> =>
  Effect.gen(function* () {
    const captured = yield* Effect.context<Captured<Tools, RequestServices>>()
    const registrations: Registration[] = []
    for (const tool of Object.values(toolkit.tools)) {
      // Native Toolkit uses tool.id as its handler Context key. The Handler interface intentionally erases schemas.
      type Services = Captured<Tools, RequestServices> | Invocation | ToolCall | RequestServices
      type Parameters = AiTool.Parameters<Tools[keyof Tools]>
      type Failure =
        | AiTool.Failure<Tools[keyof Tools]>
        | AiError.AiError
        | AiError.AiErrorReason
        | ToolError
      type Success = AiTool.Success<Tools[keyof Tools]>
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
        effect: Effect.Effect<A, E, R>,
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
              effect,
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
      const encodeArgs = (args: unknown) =>
        provide(Schema.encodeEffect(parameters)(args as Parameters)).pipe(
          Effect.flatMap(Schema.decodeUnknownEffect(Schema.Json)),
          Effect.mapError((cause) => error(tool, ToolInvalidParameters, cause)),
        )
      const execute = Effect.fnUntraced(function* (args: unknown, id: string) {
        const invocation = yield* Invocation
        const projected = info.project ?? defaultProject
        const api = yield* ToolCall
        const preliminary = (result: unknown): Effect.Effect<void> =>
          provide(Schema.encodeEffect(success)(result)).pipe(
            Effect.mapError((cause) => error(tool, ToolInvalidResult, cause)),
            Effect.map((encoded) => projected(result, encoded, false)),
            Effect.flatMap((value) =>
              api.preliminary === undefined
                ? invocation.progress({
                    output: value.content
                      ?.flatMap((part) => (part.type === 'text' ? [part.text] : []))
                      .join(''),
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
          Effect.suspend(() =>
            handler.handler(args as Parameters, { toolCallId: id, preliminary }),
          ),
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
export function declaration(registration: Registration): ContextView.ToolDeclaration {
  return {
    name: registration.tool.name,
    ...(registration.tool.description === undefined
      ? {}
      : { description: registration.tool.description }),
    parameters: AiTool.getJsonSchema(registration.tool),
    ...(AiTool.isProviderDefined(registration.tool)
      ? {
          provider: {
            id: registration.tool.id,
            name: registration.tool.providerName,
            args: registration.tool.args,
          },
        }
      : {}),
  }
}
export const Intent = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  args: Schema.Json,
  encodedArgs: Schema.optionalKey(Schema.Json),
  replay: Schema.Literals(['safe', 'unsafe']),
})
export type Intent = typeof Intent.Type
export const Execution = Schema.Struct({
  outcome: Schema.Literals(['completed', 'failed', 'interrupted', 'unavailable']),
  result: Result,
})
export type Execution = typeof Execution.Type
/** Persist decoded intent separately from provider encoded params. Native transforms are not re-applied during replay. */
export const makeIntent = Effect.fnUntraced(function* (
  registration: Registration,
  id: string,
  decoded: unknown,
  encoded?: Schema.Json,
) {
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
export function interruption(intent: Intent): ToolResult {
  return {
    isError: true,
    content: [
      Prompt.textPart({ text: 'Tool call interrupted before a durable result was recorded.' }),
    ],
    details: { reason: 'interrupted', name: intent.name },
  }
}
export function unavailable(name: string): ToolResult {
  return {
    isError: true,
    content: [Prompt.textPart({ text: `Tool ${name} is not available` })],
    details: { reason: 'unavailable', name },
  }
}
/** Convert handler failures into failed domain results; cancellation remains cancellation. */
export const settleFailure = (
  cause: Cause.Cause<ToolError>,
  partial: ToolResult,
): Effect.Effect<ToolResult> =>
  Cause.hasInterrupts(cause)
    ? Effect.failCause(Cause.fromReasons(cause.reasons.filter(Cause.isInterruptReason)))
    : Effect.succeed({
        ...partial,
        isError: true,
        diagnostics: [
          ...(partial.diagnostics ?? []),
          { kind: 'tool_error', message: Serialization.errorText(Cause.squash(cause)) },
        ],
        details: {
          reason: 'execution',
          error: Serialization.errorText(Cause.squash(cause)),
          partial: partial.details ?? null,
        },
      })
/** Whole-result text truncation preserves non-text parts and emits one bounded text block. */
export function boundResult(result: ToolResult, limits: Output.OutputLimits): ToolResult {
  const content = result.content ?? []
  const text = content.flatMap((part) => (part.type === 'text' ? [part.text] : [])).join('')
  const bounded = Output.boundOutput(text, limits)
  if (bounded.droppedBytes === 0 && bounded.droppedLines === 0) return result
  const anchor =
    limits.retain === 'head'
      ? content.findIndex((part) => part.type === 'text')
      : content.findLastIndex((part) => part.type === 'text')
  return {
    ...result,
    content: content.flatMap((part, index): ReadonlyArray<Prompt.UserMessagePart> => {
      if (part.type !== 'text') return [part]
      return index === anchor ? [{ ...part, text: bounded.text }] : []
    }),
    diagnostics: [
      ...(result.diagnostics ?? []),
      {
        kind: 'truncated',
        detail: { droppedBytes: bounded.droppedBytes, droppedLines: bounded.droppedLines },
      },
    ],
  }
}
export function executionMode(
  registrations: ReadonlyArray<Registration>,
  mode: 'parallel' | 'sequential',
): 'parallel' | 'sequential' {
  return mode === 'sequential' ||
    registrations.some((registration) => registration.metadata.execution === 'sequential')
    ? 'sequential'
    : 'parallel'
}

/** Controls are reduced in original call order, not finish order; noncompleted/unavailable slots defeat unanimity. */
export function controls(executions: ReadonlyArray<Execution>): {
  readonly terminate: boolean
  readonly reset?: { readonly note?: string }
  readonly addTools: ReadonlyArray<string>
} {
  let reset: { readonly note?: string } | undefined
  const names = new Set<string>()
  for (const execution of executions) {
    if (execution.outcome !== 'completed') continue
    if (execution.result.control?.reset !== undefined) reset = execution.result.control.reset
    for (const name of execution.result.control?.addTools ?? []) names.add(name)
  }
  return {
    terminate:
      executions.length > 0 &&
      executions.every(
        (execution) =>
          execution.outcome === 'completed' && execution.result.control?.terminate === true,
      ),
    ...(reset === undefined ? {} : { reset }),
    addTools: [...names],
  }
}

export function outputLimits(metadata: Metadata): Output.OutputLimits {
  return {
    maxBytes: metadata.output?.maxBytes ?? Output.defaults.maxBytes,
    maxLines: metadata.output?.maxLines ?? Output.defaults.maxLines,
    retain: metadata.output?.retain ?? Output.defaults.retain,
  }
}
