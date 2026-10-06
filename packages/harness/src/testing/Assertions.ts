import type { Assertions } from './EnvConformance.ts'
import * as Cause from 'effect/Cause'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'

export interface Expectation {
  readonly toBe: (expected: unknown) => void
  readonly toEqual: (expected: unknown) => void
  readonly toBeTruthy: () => void
  readonly toMatchObject: (expected: unknown) => void
  readonly toBeGreaterThan: (expected: number) => void
}
export type ExpectLike = (actual: unknown) => Expectation
export interface ConformanceAssertions extends Assertions {
  readonly deepEqual: (actual: unknown, expected: unknown) => void
  readonly partialDeepEqual: (actual: unknown, expected: unknown) => void
  readonly greaterThan: (actual: number, expected: number) => void
  readonly rejects: <A, E, R>(
    operation: Effect.Effect<A, E, R>,
    messageIncludes: string,
  ) => Effect.Effect<void, never, R>
}
/** Adapts synchronous assertions without running effects or hiding adapter failures. */
export const createExpectAssertions = (expect: ExpectLike): ConformanceAssertions => ({
  strictEqual: (actual, expected) => expect(actual).toBe(expected),
  deepStrictEqual: (actual, expected) => expect(actual).toEqual(expected),
  ok: (condition) => expect(condition).toBeTruthy(),
  deepEqual: (actual, expected) => expect(actual).toEqual(expected),
  partialDeepEqual: (actual, expected) => expect(actual).toMatchObject(expected),
  greaterThan: (actual, expected) => expect(actual).toBeGreaterThan(expected),
  rejects: (operation, messageIncludes) =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(operation)
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit))
        expect(String(Cause.squash(exit.cause)).includes(messageIncludes)).toBe(true)
    }),
})
