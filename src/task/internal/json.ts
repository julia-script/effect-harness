import type { Schema } from 'effect'
import { cast } from 'effect/Function'

/** Structural equality of two JSON values; object key order is ignored. */
export const jsonEqual = (left: Schema.Json, right: Schema.Json): boolean => {
  if (left === right) return true
  if (typeof left !== 'object' || typeof right !== 'object' || left === null || right === null) {
    return false
  }
  if (Array.isArray(left) || Array.isArray(right)) {
    if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) return false
    const rightArray = cast<Schema.Json, ReadonlyArray<Schema.Json>>(right)
    return left.every((value: Schema.Json, index) => jsonEqual(value, rightArray[index] ?? null))
  }
  // Neither is an array: both are objects.
  const leftObject = cast<Schema.Json, Record<string, Schema.Json>>(left)
  const rightObject = cast<Schema.Json, Record<string, Schema.Json>>(right)
  const keys = Object.keys(leftObject)
  if (keys.length !== Object.keys(rightObject).length) return false
  return keys.every(
    (key) =>
      Object.hasOwn(rightObject, key) &&
      jsonEqual(leftObject[key] ?? null, rightObject[key] ?? null),
  )
}
