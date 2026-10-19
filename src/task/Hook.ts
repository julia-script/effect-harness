/**
 * Hooks, ported from pi's hook registrations (spec §7.2): typed questions a task asks before it
 * commits a decision. A task declares the hooks it asks with `kind`; an extension answers them with
 * `make`; the task finds the answers its conversation selects with `handlers` and asks each with
 * `ask`.
 *
 * Hooks run off the Session line, so a crash before the commit that uses their answer may ask them
 * again. Use the task's memos, through `Api`, for a durable first-writer-wins decision.
 */
import { Cause, Context, Effect, Option } from 'effect'
import type { Runtime } from './Task.ts'

/** What a hook may use: committed document reads and the asking task's memos. */
export type Api = Pick<
  Runtime<unknown, unknown, unknown>,
  'taskId' | 'conversationId' | 'snapshot' | 'snapshotAsOf' | 'memo' | 'readMemo'
>

/** The hooks task `task` asks, with their handler signatures `H`. */
export interface Kind<H> {
  readonly task: string
  readonly key: Context.Service<Kind<H>, Partial<H>>
}

/**
 * Declare the hooks of task `task`. Dispatch matches the task's name, so hooks keep working
 * across a reload of the task.
 */
export const kind = <H>(task: string): Kind<H> => ({
  task,
  key: Context.Service<Kind<H>, Partial<H>>(`effect-harness/task/Hook/${task}`),
})

/** Handlers of one task's hooks, as an extension registers them. */
export interface Registration {
  readonly task: string
  readonly handlers: Context.Context<never>
}

/** Register `handlers` for the hooks of `kind`. */
export const make = <H>(hooks: Kind<H>, handlers: Partial<H>): Registration => ({
  task: hooks.task,
  handlers: Context.make(hooks.key, handlers),
})

/** The handlers of `kind` among `registrations`, in order. */
export const handlers = <H>(
  registrations: Iterable<Registration>,
  hooks: Kind<H>,
): ReadonlyArray<Partial<H>> => {
  const found: Array<Partial<H>> = []
  for (const registration of registrations) {
    if (registration.task !== hooks.task) continue
    const handler = Context.getOption(registration.handlers, hooks.key)
    if (Option.isSome(handler)) found.push(handler.value)
  }
  return found
}

/** A failure's text, for a hook that turns one into an answer, such as a blocked call. */
export const failureMessage = (cause: Cause.Cause<unknown>): string => {
  const error = Cause.squash(cause)
  return error instanceof Error ? error.message : String(error)
}

/**
 * Ask one hook. A failure or defect is reported as a warning and counts as no answer, so the task
 * goes on; an interruption interrupts.
 */
export const ask = <A, E>(
  question: Effect.Effect<A | undefined, E>,
  name: string,
): Effect.Effect<Option.Option<A>> =>
  Effect.catchCause(Effect.map(question, Option.fromUndefinedOr), (cause) =>
    Cause.hasInterrupts(cause)
      ? Effect.interrupt
      : Effect.as(Effect.logWarning(`Hook ${name} failed`, cause), Option.none()),
  )
