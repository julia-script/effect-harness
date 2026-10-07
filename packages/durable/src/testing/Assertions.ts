/**
 * Assertion adapters for storage conformance cases.
 */
import type { Assertions } from './Storage.ts'

/**
 * Assertion operations used by adapter conformance cases.
 *
 * @category models
 */
export interface Expectation {
  readonly toBe: (expected: unknown) => void
  readonly toEqual: (expected: unknown) => void
  readonly not: { readonly toBe: (expected: unknown) => void }
  readonly toThrow: (expected?: RegExp) => void
  readonly toBeTruthy: () => void
}
/**
 * Test-runner expectation factory accepted by the assertion adapter.
 *
 * @category models
 */
export type ExpectLike = (actual: unknown) => Expectation
/**
 * Adapts the synchronous assertion boundary of a Jest/Vitest compatible runner.
 *
 * @category constructors
 */
export const makeExpectAssertions = (expect: ExpectLike): Assertions => ({
  strictEqual: (actual, expected) => expect(actual).toBe(expected),
  deepStrictEqual: (actual, expected) => expect(actual).toEqual(expected),
  notStrictEqual: (actual, expected) => expect(actual).not.toBe(expected),
  throws: (evaluate, matcher) => expect(evaluate).toThrow(matcher),
  ok: (condition) => expect(condition).toBeTruthy(),
})
