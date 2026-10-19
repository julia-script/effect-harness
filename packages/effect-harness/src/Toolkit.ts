/** Effect-style tool contracts resolved against independently supplied handler Layers. */
import * as Cause from 'effect/Cause'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Effectable from 'effect/Effectable'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import type * as Scope from 'effect/Scope'
import * as Prompt from 'effect/ai/Prompt'
import * as AiToolkit from 'effect/ai/Toolkit'
import * as Tool from './Tool.js'
import { ExecutionError, type Failure } from './ExecutionError.js'
import { SessionError } from './SessionError.js'
import { StorageError } from './StorageError.js'
import type { ToolExecution } from './ToolExecution.js'
import * as ToolResult from './ToolResult.js'
import * as ToolControl from './ToolControl.js'

/**
 * Validate encoded final channels after hook replacement using the exact admitted declaration.
 * Schema services remain explicit for direct consumers; registry consumers supply owner context.
 */
export const validateResult = <T extends Tool.Any>(
  tool: T,
  value: unknown,
  availableTools: ReadonlyArray<string> = [],
): Effect.Effect<
  ToolResult.Result,
  Schema.SchemaError | ToolControl.InvalidControl,
  Tool.CodecServices<T>
> =>
  Effect.gen(function* () {
    const result = yield* Schema.decodeUnknownEffect(ToolResult.ResultSchema)(value, {
      onExcessProperty: 'error',
    })
    if (result.control !== undefined) yield* ToolControl.validate(result.control, availableTools)
    if (Tool.hasResultChannels(tool)) {
      const schema = result.isError ? tool.failureSchema : tool.successSchema
      // Dynamic declarations erase only schema values; the public codec service union owns this boundary.
      const codec = Schema.toCodecJson(
        schema as unknown as Schema.ConstraintCodec<
          unknown,
          unknown,
          Tool.CodecServices<T>,
          Tool.CodecServices<T>
        >,
      )
      const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(ToolResult.ResultSchema))(
        result,
      )
      const decoded = yield* Schema.decodeEffect(codec)(encoded)
      yield* Schema.encodeEffect(codec)(decoded)
    }
    return result
  })

/** Concrete codec operations captured before declarations enter an erased registry snapshot. */
export interface BoundCodec {
  readonly validateParameters: (args: Schema.JsonObject) => Effect.Effect<void, Schema.SchemaError>
  readonly validateResult: (
    value: unknown,
    availableTools: ReadonlyArray<string>,
  ) => Effect.Effect<ToolResult.Result, Schema.SchemaError | ToolControl.InvalidControl>
}

/** Capture only this declaration's services; outer wrappers never merge delegate contexts. */
export const compileCodec = <T extends Tool.Any>(
  declaration: T,
): Effect.Effect<BoundCodec, never, Tool.CodecServices<T>> =>
  Effect.gen(function* () {
    const context = yield* Effect.context<Tool.CodecServices<T>>()
    // Only schema values cross this boundary. Concrete T keeps every codec service visible.
    const parameters = Schema.toCodecJson(
      declaration.parametersSchema as unknown as Schema.ConstraintCodec<
        unknown,
        unknown,
        Tool.CodecServices<T>,
        Tool.CodecServices<T>
      >,
    )
    return {
      validateParameters: (args) =>
        Schema.decodeEffect(parameters)(args).pipe(Effect.asVoid, Effect.setContext(context)),
      validateResult: (value, availableTools) =>
        validateResult(declaration, value, availableTools).pipe(Effect.setContext(context)),
    }
  })

export interface Handler<Name extends string> {
  readonly name: Name
  readonly _tag: 'ToolHandler'
}
interface Binding {
  readonly invoke: (
    args: Schema.JsonObject,
  ) => Effect.Effect<ToolResult.Result, Failure, ToolExecution>
}
const handlerKey = <N extends string>(name: N) =>
  Context.Service<Handler<N>, Binding>(`effect-harness/Toolkit/${name}`)
export type HandlerFor<T extends Tool.Any> = T extends Tool.Any ? Handler<T['name']> : never
export type ToolsByName<T extends ReadonlyArray<Tool.Any>> = AiToolkit.ToolsByName<T>
export type HandlersFor<Tools extends Record<string, Tool.Any>> = {
  [N in keyof Tools]: Handler<Tools[N]['name']>
}[keyof Tools]
export type CodecServices<Tools extends Record<string, Tool.Any>> = Tool.CodecServices<
  Tools[keyof Tools]
>
export type ContextServices<Tools extends Record<string, Tool.Any>> =
  | CodecServices<Tools>
  | Exclude<Tool.RequestServices<Tools[keyof Tools]>, ToolExecution | Scope.Scope>
export type HandlersFrom<Tools extends Record<string, Tool.Any>> = {
  readonly [N in keyof Tools]: (
    args: Tool.Parameters<Tools[N]>,
  ) => Effect.Effect<
    Tool.Success<Tools[N]>,
    Tool.Failure<Tools[N]> | Failure,
    ToolExecution | Scope.Scope | Tool.RequestServices<Tools[N]>
  >
}
export interface WithHandler<Tools extends Record<string, Tool.Any>> {
  readonly tools: Tools
  readonly invoke: (call: Tool.Call) => Effect.Effect<ToolResult.Result, Failure, ToolExecution>
}
export interface Toolkit<Tools extends Record<string, Tool.Any>> extends Effect.Effect<
  WithHandler<Tools>,
  never,
  HandlersFor<Tools> | ContextServices<Tools>
> {
  readonly tools: Tools
  readonly native: AiToolkit.Toolkit<Tools>
  readonly toLayer: <E = never, R = never>(
    build: HandlersFrom<Tools> | Effect.Effect<HandlersFrom<Tools>, E, R>,
  ) => Layer.Layer<HandlersFor<Tools>, E, Exclude<R, Scope.Scope> | ContextServices<Tools>>
}
export type Any = Toolkit<Record<string, Tool.Any>>

const isInfrastructureFailure = (error: unknown): error is Failure =>
  Schema.is(ExecutionError)(error) ||
  Schema.is(SessionError)(error) ||
  Schema.is(StorageError)(error) ||
  Schema.isSchemaError(error)

class ToolkitImpl<Tools extends Record<string, Tool.Any>>
  extends Effectable.Class<WithHandler<Tools>, never, HandlersFor<Tools> | ContextServices<Tools>>
  implements Toolkit<Tools>
{
  readonly tools: Tools
  readonly native: AiToolkit.Toolkit<Tools>
  readonly toLayer: Toolkit<Tools>['toLayer']
  readonly effect: Effect.Effect<
    WithHandler<Tools>,
    never,
    HandlersFor<Tools> | ContextServices<Tools>
  >
  constructor(
    native: AiToolkit.Toolkit<Tools>,
    toLayer: Toolkit<Tools>['toLayer'],
    effect: Effect.Effect<WithHandler<Tools>, never, HandlersFor<Tools> | ContextServices<Tools>>,
  ) {
    super()
    this.native = native
    this.toLayer = toLayer
    this.effect = effect
    this.tools = native.tools
  }
  override asEffect() {
    return this.effect
  }
}

const resolve = <Tools extends Record<string, Tool.Any>>(
  native: AiToolkit.Toolkit<Tools>,
): Toolkit<Tools> => {
  const tools = native.tools
  const evaluate = Effect.gen(function* () {
    const context = yield* Effect.context<HandlersFor<Tools>>()
    const bindings = new Map(
      Object.values(tools).map((tool) => [tool.name, Context.get(context, handlerKey(tool.name))]),
    )
    return {
      tools,
      invoke: (call: Tool.Call) => {
        const binding = bindings.get(call.name)
        if (binding === undefined)
          return Effect.fail(
            new ExecutionError({
              reason: 'notFound',
              operation: 'tool.invoke',
              message: `Tool ${call.name} has no handler`,
            }),
          )
        return binding.invoke(call.arguments)
      },
    }
  })
  const toLayer: Toolkit<Tools>['toLayer'] = (build) =>
    Layer.effectContext(
      Effect.gen(function* () {
        const context = yield* Effect.context<ContextServices<Tools>>()
        const handlers = Effect.isEffect(build) ? yield* build : build
        let output = Context.empty() as Context.Context<HandlersFor<Tools>>
        for (const tool of Object.values(tools)) {
          // Runtime tool names erase the mapped handler's parameter/result types. The schemas own this boundary.
          const handler = handlers[tool.name as keyof Tools] as (
            args: unknown,
          ) => Effect.Effect<unknown, Failure, ToolExecution | Scope.Scope>
          const invoke = Effect.fnUntraced(
            function* (args: Schema.JsonObject) {
              const decoded = yield* Schema.decodeEffect(
                Schema.toCodecJson(
                  tool.parametersSchema as unknown as Schema.ConstraintCodec<
                    unknown,
                    unknown,
                    never,
                    never
                  >,
                ),
              )(args)
              const exit = yield* Effect.exit(Effect.scoped(Effect.suspend(() => handler(decoded))))
              let value: unknown
              let isError = false
              if (exit._tag === 'Failure') {
                if (
                  Cause.hasInterrupts(exit.cause) ||
                  Cause.hasDies(exit.cause) ||
                  exit.cause.reasons.some(
                    (reason) => Cause.isFailReason(reason) && isInfrastructureFailure(reason.error),
                  )
                )
                  return yield* Effect.failCause(exit.cause)
                const failure = Cause.findErrorOption(exit.cause)
                if (Option.isNone(failure)) return yield* Effect.failCause(exit.cause)
                value = yield* Schema.encodeUnknownEffect(
                  Schema.toCodecJson(
                    tool.failureSchema as unknown as Schema.ConstraintCodec<
                      unknown,
                      unknown,
                      never,
                      never
                    >,
                  ),
                )(failure.value)
                isError = true
              } else
                value = yield* Schema.encodeUnknownEffect(
                  Schema.toCodecJson(
                    tool.successSchema as unknown as Schema.ConstraintCodec<
                      unknown,
                      unknown,
                      never,
                      never
                    >,
                  ),
                )(exit.value)
              const json = yield* Schema.decodeUnknownEffect(Schema.Json)(value)
              if (Tool.hasResultChannels(tool)) {
                const channels = yield* Schema.decodeUnknownEffect(Schema.JsonObject)(json)
                return yield* Schema.decodeEffect(Schema.toCodecJson(ToolResult.ResultSchema))({
                  ...channels,
                  isError,
                  diagnostics: channels.diagnostics ?? [],
                })
              }
              return {
                content: [
                  Prompt.textPart({ text: typeof json === 'string' ? json : JSON.stringify(json) }),
                ],
                isError,
                details: json,
                diagnostics: [],
              }
            },
            Effect.updateContext((input: Context.Context<ToolExecution>) =>
              Context.merge(context, input),
            ),
          )
          output = Context.add(output, handlerKey(tool.name), { invoke })
        }
        // Tool names index service identifiers; TypeScript cannot correlate the loop key with the mapped union.
        return output as Context.Context<HandlersFor<Tools>>
      }),
    )
  return new ToolkitImpl(native, toLayer, evaluate)
}
export const make = <const Tools extends ReadonlyArray<Tool.Any>>(
  ...tools: Tools
): Toolkit<ToolsByName<Tools>> => resolve(AiToolkit.make(...tools))
type Members<T> = T extends { readonly tools: infer Tools } ? Tools[keyof Tools] : never
type Merged<T extends ReadonlyArray<{ readonly tools: Readonly<Record<string, Tool.Any>> }>> = {
  readonly [
    Member in Members<T[number]> as Member extends Tool.Any ? Member['name'] : never
  ]: Member
}
export const merge = <
  const Toolkits extends ReadonlyArray<{ readonly tools: Readonly<Record<string, Tool.Any>> }>,
>(
  ...toolkits: Toolkits
): Toolkit<Merged<Toolkits>> => {
  // Native name resolution owns collisions; the mapped record preserves each input tool's schemas.
  const tools = toolkits.flatMap((toolkit) => Object.values(toolkit.tools))
  return make(...tools) as unknown as Toolkit<Merged<Toolkits>>
}
