import { arrayIndex } from './arrays.ts'
import {
  nodeBase,
  releaseStoredValue,
  replaceStoredValue,
  storedValue,
  storeValue,
} from './context.ts'
import type { OverlayNode } from './context.ts'
import { isArray } from './json.ts'
import type { Json, JsonObject } from './json.ts'

/**
 * Object overlays. The common single write and single deletion live in `writeKey` and
 * `deleteKey`; a second one moves both into a map or set.
 */

export const baseObject = (node: OverlayNode): JsonObject => {
  const base = nodeBase(node)
  return isArray(base) ? {} : base
}

export const hasObjectWrite = (node: OverlayNode, key: string): boolean =>
  node.writeKey === key || (node.writes?.has(key) ?? false)

export const setObjectWrite = (node: OverlayNode, key: string, value: Json): void => {
  const context = node.context
  if (node.writes !== undefined) {
    const reference = node.writes.get(key)
    node.writes.set(
      key,
      reference === undefined
        ? storeValue(context, value)
        : replaceStoredValue(context, reference, value),
    )
    return
  }
  if (node.writeKey === undefined || node.writeKey === key) {
    node.writeKey = key
    node.writeValue =
      node.writeValue === undefined
        ? storeValue(context, value)
        : replaceStoredValue(context, node.writeValue, value)
    return
  }
  node.writes = new Map([
    [node.writeKey, node.writeValue!],
    [key, storeValue(context, value)],
  ])
  node.writeKey = undefined
  node.writeValue = undefined
}

export const deleteObjectWrite = (node: OverlayNode, key: string): void => {
  if (node.writeKey === key) {
    node.writeKey = undefined
    releaseStoredValue(node.context, node.writeValue!)
    node.writeValue = undefined
    return
  }
  const reference = node.writes?.get(key)
  if (reference !== undefined) releaseStoredValue(node.context, reference)
  node.writes?.delete(key)
}

export const isObjectDeleted = (node: OverlayNode, key: string): boolean =>
  node.deleteKey === key || (node.deletes?.has(key) ?? false)

export const setObjectDeletion = (node: OverlayNode, key: string): void => {
  if (node.deletes !== undefined) {
    node.deletes.add(key)
    return
  }
  if (node.deleteKey === undefined || node.deleteKey === key) {
    node.deleteKey = key
    return
  }
  node.deletes = new Set([node.deleteKey, key])
  node.deleteKey = undefined
}

export const deleteObjectDeletion = (node: OverlayNode, key: string): void => {
  if (node.deleteKey === key) node.deleteKey = undefined
  else node.deletes?.delete(key)
}

export const objectHas = (node: OverlayNode, key: string): boolean => {
  if (isObjectDeleted(node, key)) return false
  return hasObjectWrite(node, key) || Object.hasOwn(nodeBase(node), key)
}

export const objectValue = (node: OverlayNode, key: string): Json => {
  if (node.writeKey === key) return storedValue(node.context, node.writeValue!)
  const reference = node.writes?.get(key)
  if (reference !== undefined) return storedValue(node.context, reference)
  return baseObject(node)[key]!
}

/** Whether the draft's keys are the base's keys, so the base can list them. */
const existingKeysOnly = (node: OverlayNode, base: JsonObject): boolean => {
  if (node.deleteKey !== undefined || node.deletes !== undefined || node.readded !== undefined) {
    return false
  }
  if (node.writeKey !== undefined && !Object.hasOwn(base, node.writeKey)) return false
  if (node.writes === undefined) return true
  for (const key of node.writes.keys()) if (!Object.hasOwn(base, key)) return false
  return true
}

/** Native key order: integer-like keys ascending, then strings in insertion order. */
const nativeOrder = (keys: ReadonlyArray<string>): Array<string> => {
  const indices: Array<number> = []
  const strings: Array<string> = []
  for (const key of keys) {
    const index = arrayIndex(key)
    if (index === undefined) strings.push(key)
    else indices.push(index)
  }
  indices.sort((left, right) => left - right)
  return [...indices.map(String), ...strings]
}

/** The draft's own keys in native order. */
export const objectKeys = (node: OverlayNode): Array<string> => {
  const base = baseObject(node)
  if (existingKeysOnly(node, base)) return Object.keys(base)
  const keys = Object.keys(base).filter(
    (key) => !isObjectDeleted(node, key) && node.readded?.has(key) !== true,
  )
  const seen = new Set(keys)
  const add = (key: string): void => {
    if (seen.has(key)) return
    keys.push(key)
    seen.add(key)
  }
  if (node.writeKey !== undefined) add(node.writeKey)
  if (node.writes !== undefined) for (const key of node.writes.keys()) add(key)
  return nativeOrder(keys)
}
