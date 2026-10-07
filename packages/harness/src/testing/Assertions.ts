/**
 * Synchronous assertion adapters for portable environment conformance.
 *
 * @since 0.0.0
 */
import type { Assertions as EnvAssertions } from './EnvConformance.ts'
import * as Cause from 'effect/Cause'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'

/**
 * Assertions expectation contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Expectation = makeExpectAssertions.Expectation
/**
 * Assertions expect like contract.
 *
 * @category models
 * @since 0.0.0
 */
export type ExpectLike = makeExpectAssertions.ExpectLike
/**
 * Assertions conformance assertions contract.
 *
 * @category models
 * @since 0.0.0
 */
export type ConformanceAssertions = makeExpectAssertions.Assertions
/**
 * Adapts synchronous assertions without running effects or hiding adapter failures.
 *
 * @category constructors
 * @since 0.0.0
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
 * Type contracts owned by makeExpectAssertions.
 *
 * @category utility types
 * @since 0.0.0
 */
export declare namespace makeExpectAssertions {
  /**
   * Type contract for makeExpectAssertions.
   *
   * @category models
   * @since 0.0.0
   */
  interface Expectation {
    readonly toBe: (expected: unknown) => void
    readonly toEqual: (expected: unknown) => void
    readonly toBeTruthy: () => void
    readonly toMatchObject: (expected: unknown) => void
    readonly toBeGreaterThan: (expected: number) => void
  }
  /**
   * Type contract for makeExpectAssertions.
   *
   * @category models
   * @since 0.0.0
   */
  type ExpectLike = (actual: unknown) => Expectation
  /**
   * Type contract for makeExpectAssertions.
   *
   * @category models
   * @since 0.0.0
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
