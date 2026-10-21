import { Effect } from 'effect'
import { ReadAfterWrite } from '../session/SessionError.ts'

/**
 * A commit whose callback reads tables only before writing cannot fail with `ReadAfterWrite`;
 * if it does, that is a bug.
 */
export const readsFirst = <A, E, R>(effect: Effect.Effect<A, E | ReadAfterWrite, R>) =>
  Effect.catchIf(effect, (error) => error instanceof ReadAfterWrite, Effect.die)
