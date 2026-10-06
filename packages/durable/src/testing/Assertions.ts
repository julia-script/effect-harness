import type { Assertions } from './Storage.ts'

export interface Expectation {
  readonly toBe: (expected: unknown) => void
  readonly toEqual: (expected: unknown) => void
  readonly not: { readonly toBe: (expected: unknown) => void }
  readonly toThrow: (expected?: RegExp) => void
  readonly toBeTruthy: () => void
}
export type ExpectLike = (actual: unknown) => Expectation
/** Adapts the synchronous assertion boundary of a Jest/Vitest compatible runner. */
export const createExpectAssertions = (expect: ExpectLike): Assertions => ({
  strictEqual: (actual, expected) => expect(actual).toBe(expected),
  deepStrictEqual: (actual, expected) => expect(actual).toEqual(expected),
  notStrictEqual: (actual, expected) => expect(actual).not.toBe(expected),
  throws: (evaluate, matcher) => expect(evaluate).toThrow(matcher),
  ok: (condition) => expect(condition).toBeTruthy(),
})
