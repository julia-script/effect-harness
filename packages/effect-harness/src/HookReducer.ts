/** Event-specific hook composition. Existing Harness dispatch remains a separate legacy consumer. */
import * as Cause from 'effect/Cause'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import type * as Scope from 'effect/Scope'
import * as Hook from './Hook.js'
import * as ToolResult from './ToolResult.js'
import { ExecutionError } from './ExecutionError.js'
import { SessionError } from './SessionError.js'
import { StorageError } from './StorageError.js'
import { TaskError } from './TaskError.js'
import type { HookExecution } from './HookExecution.js'

export class HookFailure extends Schema.TaggedError<HookFailure>()('HookFailure', {
  event: Schema.String,
  reason: Schema.Literals(['callback', 'validation', 'critical']),
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

export type Composition = 'chain' | 'legacy'
export interface Callback {
  readonly event: Hook.Name
  readonly execute: (
    event: Hook.Event,
  ) => Effect.Effect<unknown, HookFailure, HookExecution | Scope.Scope>
}
export interface Options<E = never, R = never> {
  /** New dispatch defaults to chain. Legacy always passes the original event and takes the last value. */
  readonly composition?: Composition
  readonly report?: (failure: HookFailure) => Effect.Effect<void, E, R>
  /** Validate before accepting each argument replacement; never rerun transforms after durable intent. */
  readonly validateArguments?: (arguments_: Schema.JsonObject) => Effect.Effect<void, E, R>
  /** Validate schema-specific result channels/controls before accepting a replacement. */
  readonly validateResult?: (result: unknown) => Effect.Effect<void, E, R>
}

const critical = (cause: unknown): boolean =>
  Schema.is(ExecutionError)(cause) ||
  Schema.is(SessionError)(cause) ||
  Schema.is(StorageError)(cause) ||
  Schema.is(TaskError)(cause) ||
  Schema.isSchemaError(cause)

/** Ordinary callback errors/throws follow event policy; interruption and critical failures always propagate. */
export const protect = <A, E, R>(
  event: string,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, HookFailure, R> =>
  effect.pipe(
    Effect.mapError(
      (cause) =>
        new HookFailure({
          event,
          reason:
            critical(cause) || (Schema.is(HookFailure)(cause) && cause.reason === 'critical')
              ? 'critical'
              : 'callback',
          message: 'Hook callback failed',
          cause,
        }),
    ),
    Effect.catchCause((cause) => {
      if (Cause.hasInterrupts(cause)) return Effect.failCause(cause)
      const criticalFailure = cause.reasons.find(
        (reason) => Cause.isFailReason(reason) && reason.error.reason === 'critical',
      )
      if (criticalFailure !== undefined && Cause.isFailReason(criticalFailure))
        return Effect.fail(criticalFailure.error)
      return Effect.fail(
        new HookFailure({ event, reason: 'callback', message: Cause.pretty(cause), cause }),
      )
    }),
  )

const output = (
  event: Hook.Event,
  value: unknown,
): Effect.Effect<Hook.Output<Hook.Name>, Schema.SchemaError> => {
  if (value === undefined) return Effect.void
  switch (event._tag) {
    case 'beforeRequest':
      return Schema.decodeUnknownEffect(Schema.toType(Prompt.Prompt))(value)
    case 'beforeTool':
      return Schema.decodeUnknownEffect(Hook.ToolDecisionSchema)(value)
    case 'afterTool':
      return Schema.decodeUnknownEffect(Schema.toType(ToolResult.ResultSchema))(value, {
        onExcessProperty: 'error',
      })
    case 'onYield':
      return Schema.decodeUnknownEffect(Schema.toType(Hook.YieldDecisionSchema))(value)
    case 'beforeCompact':
      return Schema.decodeUnknownEffect(Hook.CompactionDecisionSchema)(value)
    default:
      return Schema.decodeUnknownEffect(Schema.Void)(value)
  }
}

export const dispatch = <N extends Hook.Name, E = never, R = never>(
  callbacks: ReadonlyArray<Callback>,
  original: Hook.Input<N>,
  options: Options<E, R> = {},
): Effect.Effect<Hook.Output<N>, HookFailure | E, HookExecution | Scope.Scope | R> =>
  Effect.gen(function* () {
    let event: Hook.Event = yield* Schema.decodeUnknownEffect(Schema.toType(Hook.EventSchema))(
      original,
    ).pipe(
      Effect.mapError(
        (cause) =>
          new HookFailure({
            event: original._tag,
            reason: 'validation',
            message: 'Invalid hook input',
            cause,
          }),
      ),
    )
    let accepted: Hook.Output<Hook.Name> = undefined
    const chain = options.composition !== 'legacy'
    for (const callback of callbacks) {
      if (callback.event !== event._tag) continue
      const invoked = yield* protect(
        event._tag,
        Effect.suspend(() => callback.execute(event)),
      ).pipe(
        Effect.matchEffect({
          onSuccess: (value) => Effect.succeed({ _tag: 'value' as const, value }),
          onFailure: (
            failure,
          ): Effect.Effect<
            { readonly _tag: 'failed'; readonly failure: HookFailure },
            E | HookFailure,
            R
          > => {
            if (failure.reason === 'critical') return Effect.fail(failure)
            const report =
              options.report === undefined ? Effect.logError(failure) : options.report(failure)
            return report.pipe(Effect.as({ _tag: 'failed' as const, failure }))
          },
        }),
      )
      if (invoked._tag === 'failed') {
        if (event._tag === 'beforeTool') {
          accepted = { _tag: 'block', message: invoked.failure.message }
          if (chain) break
        }
        continue
      }
      if (
        event._tag === 'afterTool' &&
        invoked.value !== undefined &&
        options.validateResult !== undefined
      )
        yield* options.validateResult(invoked.value)
      const value = yield* output(event, invoked.value).pipe(
        Effect.mapError(
          (cause) =>
            new HookFailure({
              event: event._tag,
              reason: 'validation',
              message: 'Invalid hook replacement',
              cause,
            }),
        ),
      )
      if (value === undefined) continue
      if (event._tag === 'beforeTool') {
        const decision = value as Hook.ToolDecision
        if (decision._tag === 'replaceArguments') {
          if (options.validateArguments !== undefined)
            yield* options.validateArguments(decision.arguments)
          accepted = decision
          if (chain) event = { ...event, call: { ...event.call, arguments: decision.arguments } }
        } else if (decision._tag === 'block') {
          accepted = decision
          if (chain) break
        } else if (!chain || accepted === undefined) accepted = decision
      } else if (event._tag === 'beforeRequest') {
        accepted = value
        if (chain) event = { ...event, prompt: value as Prompt.Prompt }
      } else if (event._tag === 'afterTool') {
        const result = value as ToolResult.Result
        accepted = result
        if (chain) event = { ...event, result }
      } else {
        accepted = value
        if (chain && (event._tag === 'onYield' || event._tag === 'beforeCompact')) break
      }
    }
    // Event-specific validation and discriminator branches above preserve N's output association.
    return accepted as Hook.Output<N>
  })
