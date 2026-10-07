/**
 * Synchronous assertion adapters for portable environment conformance.
 */
import type { Assertions as EnvAssertions } from './EnvConformance.ts'
import * as Cause from 'effect/Cause'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'

/**
 * Assertion operations used by adapter conformance cases.
 *
 * @category models
 */
export type Expectation = makeExpectAssertions.Expectation
/**
 * Test-runner expectation factory accepted by the assertion adapter.
 *
 * @category models
 */
export type ExpectLike = makeExpectAssertions.ExpectLike
/**
 * Assertions required by environment adapter conformance cases.
 *
 * @category models
 */
export type ConformanceAssertions = makeExpectAssertions.Assertions
/**
 * Adapts synchronous assertions without running effects or hiding adapter failures.
 *
 * @category constructors
 */
export const makeExpectAssertions = (expect: ExpectLike): ConformanceAssertions => ({
  strictEqual: (actual, expected) => expect(actual).toBe(expected),
  deepStrictEqual: (actual, expected) => expect(actual).toEqual(expected),
  ok: (condition) => expect(condition).toBeTruthy(),
  deepEqual: (actual, expected) => expect(actual).toEqual(expected),
  partialDeepEqual: (actual, expected) => expect(actual).toMatchObject(expected),
  greaterThan: (actual, expected) => expect(actual).toBeGreaterThan(expected),
  rejects: Effect.fnUntraced(function* (operation, messageIncludes) {
    const exit = yield* Effect.exit(operation)
    expect(Exit.isFailure(exit)).toBe(true)
    if (Exit.isFailure(exit))
      expect(String(Cause.squash(exit.cause)).includes(messageIncludes)).toBe(true)
  }),
})

/**
 * Type-level contracts for `makeExpectAssertions`.
 *
 * @category utility types
 */
export declare namespace makeExpectAssertions {
  /**
   * Assertion operations used by adapter conformance cases.
   *
   * @category models
   */
  interface Expectation {
    readonly toBe: (expected: unknown) => void
    readonly toEqual: (expected: unknown) => void
    readonly toBeTruthy: () => void
    readonly toMatchObject: (expected: unknown) => void
    readonly toBeGreaterThan: (expected: number) => void
  }
  /**
   * Test-runner expectation factory accepted by the assertion adapter.
   *
   * @category models
   */
  type ExpectLike = (actual: unknown) => Expectation
  /**
   * Assertion functions required by shared environment conformance cases.
   *
   * @category models
   */
  interface Assertions extends EnvAssertions {
    readonly deepEqual: (actual: unknown, expected: unknown) => void
    readonly partialDeepEqual: (actual: unknown, expected: unknown) => void
    readonly greaterThan: (actual: number, expected: number) => void
    readonly rejects: <A, E, R>(
      operation: Effect.Effect<A, E, R>,
      messageIncludes: string,
    ) => Effect.Effect<void, never, R>
  }
}
