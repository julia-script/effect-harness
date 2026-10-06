import * as Cause from 'effect/Cause'
import * as Effect from 'effect/Effect'
import type * as AiPrompt from 'effect/ai/Prompt'
import type * as Response from 'effect/ai/Response'
import type * as Context from './Context.ts'
import * as Services from 'effect/Context'
import { HookError, HookFailure } from './Error.ts'
import { Invocation, type ToolResult } from './Invocation.ts'

export interface ToolInput {
  readonly id: string
  readonly name: string
  readonly args: unknown
}
export type ToolDecision = { readonly block: string } | { readonly args: unknown }
export interface CompactInput {
  readonly reason: 'manual' | 'threshold' | 'overflow'
  readonly view: Context.View
  readonly firstKept: number
  readonly instructions?: string | undefined
}
export type CompactDecision = { readonly decline: true } | { readonly summary: string }
export interface SettledTool {
  readonly id: string
  readonly name: string
  readonly entryId: number
  readonly outcome: 'completed' | 'failed' | 'interrupted' | 'unavailable'
  readonly result: ToolResult
}
export interface Handlers<R = Invocation> {
  readonly conversationCreated?:
    | ((conversationId: number) => Effect.Effect<void, HookError, R>)
    | undefined
  readonly beforeRequest?:
    | ((prompt: AiPrompt.Prompt) => Effect.Effect<AiPrompt.Prompt | undefined, HookError, R>)
    | undefined
  readonly afterResponse?:
    | ((parts: ReadonlyArray<Response.AnyPart>) => Effect.Effect<void, HookError, R>)
    | undefined
  readonly onYield?:
    | ((
        parts: ReadonlyArray<Response.AnyPart>,
      ) => Effect.Effect<AiPrompt.UserMessage | undefined, HookError, R>)
    | undefined
  readonly beforeTool?:
    | ((input: ToolInput) => Effect.Effect<ToolDecision | undefined, HookError, R>)
    | undefined
  readonly afterTool?:
    | ((
        input: ToolInput,
        result: ToolResult,
      ) => Effect.Effect<ToolResult | undefined, HookError, R>)
    | undefined
  readonly afterTools?:
    | ((results: ReadonlyArray<SettledTool>) => Effect.Effect<void, HookError, R>)
    | undefined
  readonly beforeCompact?:
    | ((input: CompactInput) => Effect.Effect<CompactDecision | undefined, HookError, R>)
    | undefined
}
export type Operation = 'generation' | 'tool' | 'compaction' | 'conversation'
export interface Registration {
  readonly operation: Operation
  readonly handlers: Handlers
}
/** Report callback faults, but propagate cancellation rather than converting it to an omitted hook result. */
export const recover = <A, E>(
  effect: Effect.Effect<A, E, Invocation>,
): Effect.Effect<A | undefined, never, Invocation> =>
  Effect.catchCause(effect, (cause) =>
    Cause.hasInterrupts(cause)
      ? Effect.failCause(Cause.fromReasons(cause.reasons.filter(Cause.isInterruptReason)))
      : Effect.flatMap(Invocation, (invocation) =>
          Effect.as(invocation.report(Cause.squash(cause)), undefined),
        ),
  )
export const beforeRequest = Effect.fnUntraced(function* (
  handlers: ReadonlyArray<Handlers>,
  initial: AiPrompt.Prompt,
) {
  let prompt = initial
  for (const handler of handlers) {
    const callback = handler.beforeRequest
    if (callback === undefined) continue
    const next = yield* recover(Effect.suspend(() => callback.call(handler, prompt)))
    if (next !== undefined) prompt = next
  }
  return prompt
})
export const afterTool = Effect.fnUntraced(function* (
  handlers: ReadonlyArray<Handlers>,
  input: ToolInput,
  initial: ToolResult,
) {
  let result = initial
  for (const handler of handlers) {
    const callback = handler.afterTool
    if (callback === undefined) continue
    const next = yield* recover(Effect.suspend(() => callback.call(handler, input, result)))
    if (next !== undefined) result = next
  }
  return result
})
export const beforeCompact = Effect.fnUntraced(function* (
  handlers: ReadonlyArray<Handlers>,
  input: CompactInput,
) {
  for (const handler of handlers) {
    const callback = handler.beforeCompact
    if (callback === undefined) continue
    const decision = yield* recover(Effect.suspend(() => callback.call(handler, input)))
    if (decision !== undefined) return decision
  }
  return undefined
})
export const onYield = Effect.fnUntraced(function* (
  handlers: ReadonlyArray<Handlers>,
  parts: ReadonlyArray<Response.AnyPart>,
) {
  for (const handler of handlers) {
    const callback = handler.onYield
    if (callback === undefined) continue
    const message = yield* recover(Effect.suspend(() => callback.call(handler, parts)))
    if (message !== undefined) return message
  }
  return undefined
})
export const afterResponse = Effect.fnUntraced(function* (
  handlers: ReadonlyArray<Handlers>,
  parts: ReadonlyArray<Response.AnyPart>,
) {
  for (const handler of handlers)
    if (handler.afterResponse !== undefined)
      yield* recover(
        Effect.suspend(() => handler.afterResponse?.call(handler, parts) ?? Effect.void),
      )
})
export const afterTools = Effect.fnUntraced(function* (
  handlers: ReadonlyArray<Handlers>,
  results: ReadonlyArray<SettledTool>,
) {
  for (const handler of handlers)
    if (handler.afterTools !== undefined)
      yield* recover(
        Effect.suspend(() => handler.afterTools?.call(handler, results) ?? Effect.void),
      )
})
export const conversationCreated = Effect.fnUntraced(function* (
  handlers: ReadonlyArray<Handlers>,
  conversationId: number,
) {
  for (const handler of handlers)
    if (handler.conversationCreated !== undefined)
      yield* recover(
        Effect.suspend(
          () => handler.conversationCreated?.call(handler, conversationId) ?? Effect.void,
        ),
      )
})

/** Capture host dependencies and declare services supplied for each durable/native invocation. */
export const bind = <R, RequestServices = never>(
  handlers: Handlers<R>,
  requestServices: ReadonlyArray<Services.Key<RequestServices, unknown>> = [],
): Effect.Effect<Handlers, never, Exclude<R, Invocation | RequestServices>> =>
  Effect.gen(function* () {
    const captured = yield* Effect.context<Exclude<R, Invocation | RequestServices>>()
    const wrap = <Args extends unknown[], A>(
      callback: ((...args: Args) => Effect.Effect<A, HookError, R>) | undefined,
    ): ((...args: Args) => Effect.Effect<A, HookError, Invocation>) | undefined =>
      callback === undefined
        ? undefined
        : (...args) =>
            Effect.flatMap(Effect.context<Invocation>(), (current) => {
              for (const service of requestServices)
                if (!current.mapUnsafe.has(service.key))
                  return Effect.fail(
                    new HookError({
                      reason: new HookFailure({
                        message: `Request service ${service.key} is absent`,
                      }),
                    }),
                  )
              // bind's R is checked before heterogeneous callbacks enter the registry.
              // Captured host services and validated invocation services satisfy R.
              return Effect.provideContext(
                Effect.suspend(() => callback.apply(handlers, args)),
                Services.makeUnsafe<R>(Services.merge(captured, current).mapUnsafe),
              )
            })
    return {
      conversationCreated: wrap(handlers.conversationCreated),
      beforeRequest: wrap(handlers.beforeRequest),
      afterResponse: wrap(handlers.afterResponse),
      onYield: wrap(handlers.onYield),
      beforeTool: wrap(handlers.beforeTool),
      afterTool: wrap(handlers.afterTool),
      afterTools: wrap(handlers.afterTools),
      beforeCompact: wrap(handlers.beforeCompact),
    }
  })
