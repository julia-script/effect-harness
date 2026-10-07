/**
 * Ordered extension hooks with interruption-preserving recovery.
 *
 * @since 0.0.0
 */
import { dual } from 'effect/Function'
import * as Data from 'effect/Data'
import * as Option from 'effect/Option'
import * as Cause from 'effect/Cause'
import * as Effect from 'effect/Effect'
import type * as Prompt from 'effect/ai/Prompt'
import type * as Response from 'effect/ai/Response'
import type * as Context from './Context.ts'
import * as Services from 'effect/Context'
// effect-review-allow P9-namespace-alias-equals-module: effect/Context and ./Context.ts both bind Context; Services preserves the checked imported-name collision.
import { HookError, HookFailure } from './HookError.ts'
import { Invocation, type ToolResult } from './Invocation.ts'
import type { ConversationId, EntryId } from './Identity.ts'

/**
 * Hook tool input contract.
 *
 * @category models
 * @since 0.0.0
 */
export type ToolInput = Handlers.ToolInput
/**
 * Hook tool decision contract.
 *
 * @category models
 * @since 0.0.0
 */
export type ToolDecision = Data.TaggedEnum<{
  Block: { readonly block: string }
  Args: { readonly args: unknown }
}>
/**
 * Constructors and matchers for blocked tools and replacement arguments.
 *
 * @category constants
 * @since 0.0.0
 */
export const ToolDecision = Data.taggedEnum<ToolDecision>()
/**
 * Hook compact input contract.
 *
 * @category models
 * @since 0.0.0
 */
export type CompactInput = Handlers.CompactInput
/**
 * Hook compact decision contract.
 *
 * @category models
 * @since 0.0.0
 */
export type CompactDecision = Data.TaggedEnum<{
  Decline: {}
  Summary: { readonly summary: string }
}>
/**
 * Constructors and matchers for declined and supplied compaction summaries.
 *
 * @category constants
 * @since 0.0.0
 */
export const CompactDecision = Data.taggedEnum<CompactDecision>()
/**
 * Hook settled tool contract.
 *
 * @category models
 * @since 0.0.0
 */
export interface SettledTool {
  readonly id: string
  readonly name: string
  readonly entryId: EntryId
  readonly outcome: 'completed' | 'failed' | 'interrupted' | 'unavailable'
  readonly result: ToolResult
}
/**
 * Hook handlers contract.
 *
 * @category models
 * @since 0.0.0
 */
export interface Handlers<out R = Invocation> {
  readonly conversationCreated?:
    | ((conversationId: ConversationId) => Effect.Effect<void, HookError, R>)
    | undefined
  readonly beforeRequest?:
    | ((prompt: Prompt.Prompt) => Effect.Effect<Prompt.Prompt | undefined, HookError, R>)
    | undefined
  readonly afterResponse?:
    | ((parts: ReadonlyArray<Response.AnyPart>) => Effect.Effect<void, HookError, R>)
    | undefined
  readonly onYield?:
    | ((
        parts: ReadonlyArray<Response.AnyPart>,
      ) => Effect.Effect<Prompt.UserMessage | undefined, HookError, R>)
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
/**
 * Hook operation contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Operation = 'generation' | 'tool' | 'compaction' | 'conversation'
/**
 * Hook registration contract.
 *
 * @category models
 * @since 0.0.0
 */
export interface Registration {
  readonly operation: Operation
  readonly handlers: Handlers
}
/**
 * Report callback faults, but propagate cancellation rather than converting it to an omitted hook result.
 *
 * @category combinators
 * @since 0.0.0
 */
export const recover = <A, E, R>(
  self: Effect.Effect<A, E, R>,
): Effect.Effect<A | undefined, never, R | Invocation> =>
  Effect.catchCause(self, (cause) =>
    Cause.hasInterrupts(cause)
      ? Effect.failCause(Cause.fromReasons(cause.reasons.filter(Cause.isInterruptReason)))
      : Effect.flatMap(Invocation, (invocation) =>
          Effect.as(invocation.report(Cause.squash(cause)), undefined),
        ),
  )
const beforeRequestImpl = Effect.fnUntraced(function* (
  handlers: ReadonlyArray<Handlers>,
  initial: Prompt.Prompt,
): Effect.fn.Return<Prompt.Prompt, never, Invocation> {
  let prompt = initial
  for (const handler of handlers) {
    const callback = handler.beforeRequest
    if (callback === undefined) continue
    const next = yield* recover(Effect.suspend(() => callback.call(handler, prompt)))
    if (next !== undefined) prompt = next
  }
  return prompt
})
/**
 * Applies request hooks sequentially to the latest native prompt.
 *
 * @category combinators
 * @since 0.0.0
 */
export const beforeRequest: {
  (
    initial: Prompt.Prompt,
  ): (self: ReadonlyArray<Handlers>) => Effect.Effect<Prompt.Prompt, never, Invocation>
  (
    self: ReadonlyArray<Handlers>,
    initial: Prompt.Prompt,
  ): Effect.Effect<Prompt.Prompt, never, Invocation>
} = dual(2, beforeRequestImpl)
const afterToolImpl = Effect.fnUntraced(function* (
  handlers: ReadonlyArray<Handlers>,
  input: ToolInput,
  initial: ToolResult,
): Effect.fn.Return<ToolResult, never, Invocation> {
  let result = initial
  for (const handler of handlers) {
    const callback = handler.afterTool
    if (callback === undefined) continue
    const next = yield* recover(Effect.suspend(() => callback.call(handler, input, result)))
    if (next !== undefined) result = next
  }
  return result
})
/**
 * Applies tool-result hooks sequentially to the latest result.
 *
 * @category combinators
 * @since 0.0.0
 */
export const afterTool: {
  (
    input: ToolInput,
    initial: ToolResult,
  ): (self: ReadonlyArray<Handlers>) => Effect.Effect<ToolResult, never, Invocation>
  (
    self: ReadonlyArray<Handlers>,
    input: ToolInput,
    initial: ToolResult,
  ): Effect.Effect<ToolResult, never, Invocation>
} = dual(3, afterToolImpl)
const beforeCompactImpl = Effect.fnUntraced(function* (
  handlers: ReadonlyArray<Handlers>,
  input: CompactInput,
): Effect.fn.Return<Option.Option<CompactDecision>, never, Invocation> {
  for (const handler of handlers) {
    const callback = handler.beforeCompact
    if (callback === undefined) continue
    const decision = yield* recover(Effect.suspend(() => callback.call(handler, input)))
    if (decision !== undefined) return Option.some(decision)
  }
  return Option.none()
})
/**
 * Returns the first supplied compaction decision.
 *
 * @category combinators
 * @since 0.0.0
 */
export const beforeCompact: {
  (
    input: CompactInput,
  ): (
    self: ReadonlyArray<Handlers>,
  ) => Effect.Effect<Option.Option<CompactDecision>, never, Invocation>
  (
    self: ReadonlyArray<Handlers>,
    input: CompactInput,
  ): Effect.Effect<Option.Option<CompactDecision>, never, Invocation>
} = dual(2, beforeCompactImpl)
const onYieldImpl = Effect.fnUntraced(function* (
  handlers: ReadonlyArray<Handlers>,
  parts: ReadonlyArray<Response.AnyPart>,
): Effect.fn.Return<Option.Option<Prompt.UserMessage>, never, Invocation> {
  for (const handler of handlers) {
    const callback = handler.onYield
    if (callback === undefined) continue
    const message = yield* recover(Effect.suspend(() => callback.call(handler, parts)))
    if (message !== undefined) return Option.some(message)
  }
  return Option.none()
})
/**
 * Returns the first supplied native user message from yield hooks.
 *
 * @category combinators
 * @since 0.0.0
 */
export const onYield: {
  (
    parts: ReadonlyArray<Response.AnyPart>,
  ): (
    self: ReadonlyArray<Handlers>,
  ) => Effect.Effect<Option.Option<Prompt.UserMessage>, never, Invocation>
  (
    self: ReadonlyArray<Handlers>,
    parts: ReadonlyArray<Response.AnyPart>,
  ): Effect.Effect<Option.Option<Prompt.UserMessage>, never, Invocation>
} = dual(2, onYieldImpl)
const afterResponseImpl = Effect.fnUntraced(function* (
  handlers: ReadonlyArray<Handlers>,
  parts: ReadonlyArray<Response.AnyPart>,
): Effect.fn.Return<void, never, Invocation> {
  for (const handler of handlers)
    if (handler.afterResponse !== undefined)
      yield* recover(
        Effect.suspend(() => handler.afterResponse?.call(handler, parts) ?? Effect.void),
      )
})
/**
 * Runs response hooks in registration order.
 *
 * @category combinators
 * @since 0.0.0
 */
export const afterResponse: {
  (
    parts: ReadonlyArray<Response.AnyPart>,
  ): (self: ReadonlyArray<Handlers>) => Effect.Effect<void, never, Invocation>
  (
    self: ReadonlyArray<Handlers>,
    parts: ReadonlyArray<Response.AnyPart>,
  ): Effect.Effect<void, never, Invocation>
} = dual(2, afterResponseImpl)
const afterToolsImpl = Effect.fnUntraced(function* (
  handlers: ReadonlyArray<Handlers>,
  results: ReadonlyArray<SettledTool>,
): Effect.fn.Return<void, never, Invocation> {
  for (const handler of handlers)
    if (handler.afterTools !== undefined)
      yield* recover(
        Effect.suspend(() => handler.afterTools?.call(handler, results) ?? Effect.void),
      )
})
/**
 * Runs terminal tool-batch hooks in registration order.
 *
 * @category combinators
 * @since 0.0.0
 */
export const afterTools: {
  (
    results: ReadonlyArray<SettledTool>,
  ): (self: ReadonlyArray<Handlers>) => Effect.Effect<void, never, Invocation>
  (
    self: ReadonlyArray<Handlers>,
    results: ReadonlyArray<SettledTool>,
  ): Effect.Effect<void, never, Invocation>
} = dual(2, afterToolsImpl)
const conversationCreatedImpl = Effect.fnUntraced(function* (
  handlers: ReadonlyArray<Handlers>,
  conversationId: ConversationId,
): Effect.fn.Return<void, never, Invocation> {
  for (const handler of handlers)
    if (handler.conversationCreated !== undefined)
      yield* recover(
        Effect.suspend(
          () => handler.conversationCreated?.call(handler, conversationId) ?? Effect.void,
        ),
      )
})
/**
 * Runs creation hooks with the canonical conversation identity.
 *
 * @category combinators
 * @since 0.0.0
 */
export const conversationCreated: {
  (
    conversationId: ConversationId,
  ): (self: ReadonlyArray<Handlers>) => Effect.Effect<void, never, Invocation>
  (
    self: ReadonlyArray<Handlers>,
    conversationId: ConversationId,
  ): Effect.Effect<void, never, Invocation>
} = dual(2, conversationCreatedImpl)

/** Capture host dependencies and declare services supplied for each durable/native invocation. */
const bindImpl = Effect.fnUntraced(function* <R, RequestServices = never>(
  handlers: Handlers<R>,
  requestServices: ReadonlyArray<Services.Key<RequestServices, unknown>> = [],
): Effect.fn.Return<Handlers, never, Exclude<R, Invocation | RequestServices>> {
  const captured = yield* Effect.context<Exclude<R, Invocation | RequestServices>>()
  const wrap = <Args extends Array<unknown>, A>(
    self: ((...args: Args) => Effect.Effect<A, HookError, R>) | undefined,
  ): ((...args: Args) => Effect.Effect<A, HookError, Invocation>) | undefined =>
    self === undefined
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
              Effect.suspend(() => self.apply(handlers, args)),
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
/**
 * Captures host dependencies while preserving invocation-time service requirements.
 *
 * @category combinators
 * @since 0.0.0
 */
export const bind: {
  <RequestServices = never>(
    requestServices?: ReadonlyArray<Services.Key<RequestServices, unknown>>,
  ): <R>(
    self: Handlers<R>,
  ) => Effect.Effect<Handlers, never, Exclude<R, Invocation | RequestServices>>
  <R, RequestServices = never>(
    self: Handlers<R>,
    requestServices?: ReadonlyArray<Services.Key<RequestServices, unknown>>,
  ): Effect.Effect<Handlers, never, Exclude<R, Invocation | RequestServices>>
} = dual((args) => typeof args[0] === 'object' && !Array.isArray(args[0]), bindImpl)

/**
 * Type contracts owned by `Handlers`.
 *
 * @category utility types
 * @since 0.0.0
 */
export declare namespace Handlers {
  /**
   * Handlers tool input type contract.
   *
   * @category models
   * @since 0.0.0
   */
  interface ToolInput {
    readonly id: string
    readonly name: string
    readonly args: unknown
  }
  /**
   * Handlers compact input type contract.
   *
   * @category models
   * @since 0.0.0
   */
  interface CompactInput {
    readonly reason: 'manual' | 'threshold' | 'overflow'
    readonly view: Context.View
    readonly firstKept: EntryId
    readonly instructions?: string | undefined
  }
}
