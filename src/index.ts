import { Effect } from 'effect'

export const program = Effect.log('hello from effect-harness')

if (import.meta.main) {
  Effect.runFork(program)
}
