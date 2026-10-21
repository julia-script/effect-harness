/**
 * A tool's handler behind a signature that takes encoded values, so the tool task can run a call
 * it reads from storage by name. Decoding the arguments and config, and encoding the result,
 * happen here, where the tool's types are known.
 */
import { Cause, Effect, Exit, Option, Result, Schema } from 'effect'
import { cast } from 'effect/Function'
import type * as Tool from '../Tool.ts'
import type { Handler, Provided } from '../Toolkit.ts'

/** What running a call came to. */
export type Outcome =
  /** The handler's success, encoded. */
  | { readonly type: 'success'; readonly value: unknown }
  /** The tool's declared failure, encoded. */
  | { readonly type: 'failure'; readonly value: unknown }
  | { readonly type: 'invalidArguments'; readonly message: string }
  | { readonly type: 'invalidConfig'; readonly message: string }
  /** The handler died, or returned a value its Schema rejects. */
  | { readonly type: 'broken'; readonly message: string }

/** A tool with its handler. */
export interface Runner {
  readonly tool: Tool.Any
  /**
   * Run one call on encoded arguments and config. Interruption is not an outcome: it interrupts
   * the run.
   */
  readonly run: (
    args: unknown,
    context: { readonly callId: string; readonly config: unknown },
  ) => Effect.Effect<Outcome>
}

const encoded = (
  schema: Tool.Codec,
  value: unknown,
  type: 'success' | 'failure',
  name: string,
): Effect.Effect<Outcome> =>
  Effect.match(Schema.encodeUnknownEffect(schema)(value), {
    onFailure: (error): Outcome => ({
      type: 'broken',
      message: `Tool ${name} returned a value its Schema rejects: ${error.message}`,
    }),
    onSuccess: (encodedValue): Outcome => ({ type, value: encodedValue }),
  })

/** What the handler's exit means. */
const outcome = <T extends Tool.Any>(
  tool: T,
  exit: Exit.Exit<Tool.Success<T>, Tool.Failure<T>>,
): Effect.Effect<Outcome> => {
  if (Exit.isSuccess(exit)) return encoded(tool.success, exit.value, 'success', tool.name)
  if (Cause.hasInterrupts(exit.cause)) return Effect.interrupt
  const failure = Cause.findErrorOption(exit.cause)
  if (Option.isSome(failure)) return encoded(tool.failure, failure.value, 'failure', tool.name)
  return Effect.succeed({ type: 'broken', message: Cause.pretty(exit.cause) })
}

/**
 * A handler's run with its `Provided` services left to the caller: the Harness provides them to
 * the runners it resolves, and outside a Harness a handler that reads one fails as broken.
 */
const withProvided = <A, E>(effect: Effect.Effect<A, E, Provided>) =>
  cast<Effect.Effect<A, E, Provided>, Effect.Effect<A, E>>(effect)

/** `handler` behind the encoded signature. */
export const implement = <T extends Tool.Any>(tool: T, handler: Handler<T>): Runner => ({
  tool,
  run: Effect.fnUntraced(function* (args, { callId, config }) {
    const params = yield* Effect.result(Schema.decodeUnknownEffect(tool.parameters)(args))
    if (Result.isFailure(params))
      return { type: 'invalidArguments', message: params.failure.message } as const
    const decoded = yield* Effect.result(Schema.decodeUnknownEffect(tool.config)(config))
    if (Result.isFailure(decoded))
      return { type: 'invalidConfig', message: decoded.failure.message } as const
    const exit = yield* Effect.exit(
      withProvided(handler(params.success, { callId, config: decoded.success })),
    )
    return yield* outcome(tool, exit)
  }),
})
