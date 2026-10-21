import type { Schema } from 'effect'

/**
 * Strict-JSON helpers of the tracker, ported from Chord's `json.ts`. Validation returns an
 * `Invalid` instead of throwing, so a draft can reject a placement without changing.
 */

export type Json = Schema.Json
export type JsonArray = Schema.JsonArray
export type JsonObject = Schema.JsonObject
export type Container = JsonArray | JsonObject

/** Why a value is not strict JSON. */
export class Invalid {
  readonly message: string
  constructor(message: string) {
    this.message = message
  }
}

const DATA_DESCRIPTOR: PropertyDescriptor = {
  value: undefined,
  writable: true,
  enumerable: true,
  configurable: true,
}

export const isContainer = (value: unknown): value is Container =>
  value !== null && typeof value === 'object'

export const isArray = (value: Container): value is JsonArray => Array.isArray(value)

/** Write an own data property, never through a prototype setter. */
export const defineData = (target: object, key: PropertyKey, value: Json): void => {
  DATA_DESCRIPTOR.value = value
  Object.defineProperty(target, key, DATA_DESCRIPTOR)
  DATA_DESCRIPTOR.value = undefined
}

const createObject: (prototype: null) => JsonObject = Object.create

/** An object with the same prototype kind as `base`: plain, or null-prototype. */
export const emptyLike = (base: object): JsonObject =>
  Object.getPrototypeOf(base) === null ? createObject(null) : {}

const enumerableData = (descriptor: PropertyDescriptor | undefined): boolean =>
  descriptor !== undefined && descriptor.enumerable === true && 'value' in descriptor

const copyArray = (value: Array<unknown>, active: Set<object>): Json | Invalid => {
  // Indices plus `length` only: extra, symbol, and missing keys all change the count.
  if (
    Object.getPrototypeOf(value) !== Array.prototype ||
    Reflect.ownKeys(value).length !== value.length + 1
  ) {
    return new Invalid('Value must contain strict JSON dense plain arrays')
  }
  const result: Array<Json> = []
  for (let index = 0; index < value.length; index++) {
    const descriptor = Object.getOwnPropertyDescriptor(value, index)
    if (descriptor === undefined || !enumerableData(descriptor)) {
      return new Invalid('Value must contain strict JSON enumerable indexed data properties')
    }
    const copied = copy(descriptor.value, active)
    if (copied instanceof Invalid) return copied
    result.push(copied)
  }
  return result
}

const copyObject = (value: object, active: Set<object>): Json | Invalid => {
  const prototype: unknown = Object.getPrototypeOf(value)
  if (prototype !== Object.prototype && prototype !== null) {
    return new Invalid('Value must contain strict JSON plain objects or arrays')
  }
  const result = emptyLike(value)
  for (const key of Reflect.ownKeys(value)) {
    if (typeof key === 'symbol') {
      return new Invalid('Value contains a symbol key and is not strict JSON')
    }
    const descriptor = Object.getOwnPropertyDescriptor(value, key)
    if (descriptor === undefined || !enumerableData(descriptor)) {
      return new Invalid('Value must contain strict JSON enumerable data properties')
    }
    const copied = copy(descriptor.value, active)
    if (copied instanceof Invalid) return copied
    defineData(result, key, copied)
  }
  return result
}

const copy = (value: unknown, active: Set<object>): Json | Invalid => {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return value
  if (typeof value === 'number') {
    return Number.isFinite(value)
      ? value
      : new Invalid('Value contains a non-finite number and is not strict JSON')
  }
  if (typeof value !== 'object') {
    return new Invalid(`Value contains a non-JSON ${typeof value}; expected strict JSON`)
  }
  if (active.has(value)) return new Invalid('Value contains cycles and is not strict JSON')
  active.add(value)
  const copied = Array.isArray(value) ? copyArray(value, active) : copyObject(value, active)
  active.delete(value)
  return copied
}

/** Copy a value into an alias-free strict-JSON tree owned by the caller, or say why it is not. */
export const copyJson = (value: unknown): Json | Invalid => copy(value, new Set())

const equalArrays = (left: JsonArray, right: JsonArray): boolean => {
  if (left.length !== right.length) return false
  for (let index = 0; index < left.length; index++) {
    if (!equalTrustedJson(left[index]!, right[index]!)) return false
  }
  return true
}

const equalObjects = (left: JsonObject, right: JsonObject): boolean => {
  const keys = Object.keys(left)
  if (keys.length !== Object.keys(right).length) return false
  for (const key of keys) {
    if (!Object.hasOwn(right, key) || !equalTrustedJson(left[key]!, right[key]!)) return false
  }
  return true
}

/** Deep equality of trusted strict JSON, ignoring key order and prototype. */
export const equalTrustedJson = (left: Json, right: Json): boolean => {
  if (left === right) return true
  if (!isContainer(left) || !isContainer(right)) return false
  if (isArray(left)) return isArray(right) && equalArrays(left, right)
  return !isArray(right) && equalObjects(left, right)
}
