/** Structural equality for JSON protocol values; object key order does not affect identity. */
export function equal(left: unknown, right: unknown): boolean {
  if (Object.is(left, right)) return true
  if (left === null || right === null || typeof left !== 'object' || typeof right !== 'object')
    return false
  if (Array.isArray(left) && Array.isArray(right))
    return left.length === right.length && left.every((value, index) => equal(value, right[index]))
  if (Array.isArray(left) || Array.isArray(right)) return false
  const keys = Object.keys(left)
  return (
    keys.length === Object.keys(right).length &&
    keys.every(
      (key) => Object.hasOwn(right, key) && equal(Reflect.get(left, key), Reflect.get(right, key)),
    )
  )
}
