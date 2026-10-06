import { Effect } from 'effect'

export const program = Effect.log('hello from @effect-harness/durable')

if (import.meta.main) {
  Effect.runFork(program)
}
