/**
 * Ordered extension hooks with interruption-preserving recovery.
 */
import { dual } from 'effect/Function'
import * as Data from 'effect/Data'
import * as Predicate from 'effect/Predicate'
import * as Option from 'effect/Option'
import * as Effect from 'effect/Effect'
import type * as Prompt from 'effect/ai/Prompt'
import type * as Response from 'effect/ai/Response'
import type * as Transcript from './Transcript.ts'
import * as Context from 'effect/Context'
import { HookError, HookFailureError } from './HookError.ts'
import { Invocation, type Result } from './Invocation.ts'
import type { ConversationId, EntryId } from './Identity.ts'
/**
 * Decision to continue with arguments or block a tool.
 *
 * @category models
 */
export type ToolDecision = Data.TaggedEnum<{
  Block: { readonly block: string }
  Args: { readonly args: unknown }
}>
/**
 * Constructors and matchers for blocked tools and replacement arguments.
 *
 * @category constants
 */
export const ToolDecision = Data.taggedEnum<ToolDecision>()
/**
 * Decision to request, decline or supply a compaction summary.
 *
 * @category models
 */
export type CompactDecision = Data.TaggedEnum<{
  Decline: {}
  Summary: { readonly summary: string }
}>
/**
 * Constructors and matchers for declined and supplied compaction summaries.
 *
 * @category constants
 */
export const CompactDecision = Data.taggedEnum<CompactDecision>()
/**
 * Terminal tool result and committed entry ID in provider call order.
 *
 * **Details**
 *
 * afterTools receives these values after all batch slots settle, including unavailable
 * calls. Parallel completion does not reorder the batch.
 *
 * @category models
 */
export interface SettledTool {
  readonly id: string
  readonly name: string
  readonly entryId: EntryId
  readonly outcome: 'completed' | 'failed' | 'interrupted' | 'unavailable'
  readonly result: Result
}
/**
 * Optional callbacks around request, response, tool and compaction boundaries.
 *
 * @category models
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
    | ((input: Handlers.ToolInput) => Effect.Effect<ToolDecision | undefined, HookError, R>)
    | undefined
  readonly afterTool?:
    | ((
        input: Handlers.ToolInput,
        result: Result,
      ) => Effect.Effect<Result | undefined, HookError, R>)
    | undefined
  readonly afterTools?:
    | ((results: ReadonlyArray<SettledTool>) => Effect.Effect<void, HookError, R>)
    | undefined
  readonly beforeCompact?:
    | ((input: Handlers.CompactInput) => Effect.Effect<CompactDecision | undefined, HookError, R>)
    | undefined
}
/**
 * Effectful hook boundary with its invocation dependencies.
 *
 * @category models
 */
export type Operation = 'generation' | 'tool' | 'compaction' | 'conversation'
/**
 * Hook callbacks with captured host services and declared request services.
 *
 * @category models
 */
export interface Registration {
  readonly operation: Operation
  readonly handlers: Handlers
}
/**
 * Reports typed callback failures while propagating defects and cancellation.
 *
 * @category combinators
 */
export const recover = <A, E, R>(
  self: Effect.Effect<A, E, R>,
): Effect.Effect<A | undefined, never, R | Invocation> =>
  Effect.catch(self, (error) =>
    Effect.flatMap(Invocation, (invocation) => Effect.as(invocation.report(error), undefined)),
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
  input: Handlers.ToolInput,
  initial: Result,
): Effect.fn.Return<Result, never, Invocation> {
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
 */
export const afterTool: {
  (
    input: Handlers.ToolInput,
    initial: Result,
  ): (self: ReadonlyArray<Handlers>) => Effect.Effect<Result, never, Invocation>
  (
    self: ReadonlyArray<Handlers>,
    input: Handlers.ToolInput,
    initial: Result,
  ): Effect.Effect<Result, never, Invocation>
} = dual(3, afterToolImpl)
const beforeCompactImpl = Effect.fnUntraced(function* (
  handlers: ReadonlyArray<Handlers>,
  input: Handlers.CompactInput,
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
 */
export const beforeCompact: {
  (
    input: Handlers.CompactInput,
  ): (
    self: ReadonlyArray<Handlers>,
  ) => Effect.Effect<Option.Option<CompactDecision>, never, Invocation>
  (
    self: ReadonlyArray<Handlers>,
    input: Handlers.CompactInput,
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
const bindImpl = Effect.fnUntraced(function* <R, RRequestServices = never>(
  handlers: Handlers<R>,
  requestServices: ReadonlyArray<Context.Key<RRequestServices, unknown>> = [],
): Effect.fn.Return<Handlers, never, Exclude<R, Invocation | RRequestServices>> {
  const captured = yield* Effect.context<Exclude<R, Invocation | RRequestServices>>()
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
                    reason: new HookFailureError({
                      message: `Request service ${service.key} is absent`,
                    }),
                  }),
                )
            // bind's R is checked before heterogeneous callbacks enter the registry.
            // Captured host services and validated invocation services satisfy R.
            return Effect.provideContext(
              Effect.suspend(() => self.apply(handlers, args)),
              Context.makeUnsafe<R>(
                Context.merge(captured, Context.pick(Invocation, ...requestServices)(current))
                  .mapUnsafe,
              ),
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
 * Captures host hook services and declares dynamic request dependencies.
 *
 * **Details**
 *
 * Invocation remains request-local. requestServices lists additional services that the
 * executor must supply, such as durable Ownership.Current.
 *
 * **Gotchas**
 *
 * Conversation creation does not automatically provide task invocation services. Missing
 * declared request services fail with HookError.
 *
 * @category combinators
 */
export const bind: {
  <RRequestServices = never>(
    requestServices?: ReadonlyArray<Context.Key<RRequestServices, unknown>>,
  ): <R>(
    self: Handlers<R>,
  ) => Effect.Effect<Handlers, never, Exclude<R, Invocation | RRequestServices>>
  <R, RRequestServices = never>(
    self: Handlers<R>,
    requestServices?: ReadonlyArray<Context.Key<RRequestServices, unknown>>,
  ): Effect.Effect<Handlers, never, Exclude<R, Invocation | RRequestServices>>
} = dual(
  Predicate.mapInput(
    Predicate.or(Predicate.isNull, Predicate.isObject),
    (args: IArguments) => args[0],
  ),
  bindImpl,
)

/**
 * Type-level contracts for `Handlers`.
 *
 */
export declare namespace Handlers {
  /**
   * Tool identity and decoded arguments passed to a before-tool hook.
   *
   * @category models
   */
  interface ToolInput {
    readonly id: string
    readonly name: string
    readonly args: unknown
  }
  /**
   * History and policy supplied to a compaction hook.
   *
   * @category models
   */
  interface CompactInput {
    readonly reason: 'manual' | 'threshold' | 'overflow'
    readonly view: Transcript.View
    readonly firstKept: EntryId
    readonly instructions?: string | undefined
  }
}
