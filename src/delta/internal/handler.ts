import {
  arrayIndex,
  arrayOverlay,
  entryValueAt,
  hasEntryOverrideAt,
  located,
  setArrayIndex,
  setArrayLength,
  toArrayLength,
} from './arrays.ts'
import { clonePlacement } from './clone.ts'
import {
  createNode,
  isSettledContext,
  isWritable,
  markDirty,
  NODE,
  nodeBase,
  nodeForTarget,
  reject,
} from './context.ts'
import type { OverlayNode } from './context.ts'
import { Invalid, isArray, isContainer } from './json.ts'
import { arrayMutator } from './mutators.ts'
import {
  deleteObjectDeletion,
  deleteObjectWrite,
  hasObjectWrite,
  isObjectDeleted,
  objectHas,
  objectKeys,
  objectValue,
  setObjectDeletion,
  setObjectWrite,
} from './objects.ts'
import { arrayLength, locatePiece } from './pieces.ts'

/**
 * The proxy traps of every draft handle. Proxy traps cannot fail through an error channel, so a
 * draft never throws on its own: a write it cannot represent is rejected (the draft stays
 * unchanged and `prepare()` fails), and a handle of a settled change reads as empty and refuses
 * writes, which the engine reports as a `TypeError` in strict code.
 */

/** The live node behind a trap's target, or `undefined` once its change settled. */
const live = (target: object): OverlayNode | undefined => {
  const node = nodeForTarget(target)
  return node === undefined || isSettledContext(node.context) ? undefined : node
}

const writable = (target: object): OverlayNode | undefined => {
  const node = live(target)
  return node !== undefined && isWritable(node.context) ? node : undefined
}

export const getArrayIndex = (node: OverlayNode, index: number): unknown => {
  const overlay = arrayOverlay(node)
  if (index >= arrayLength(overlay)) return undefined
  const piece = locatePiece(overlay, index)
  const sourceIndex = located(overlay, piece)
  const value = entryValueAt(node, piece, sourceIndex)
  if (!isContainer(value)) return value
  return createNode(node.context, value, node, {
    kind: piece.kind === 'base' ? 1 : 2,
    key: sourceIndex,
    source: piece.kind === 'insert' ? piece.source : undefined,
    placed: piece.kind === 'insert' || hasEntryOverrideAt(overlay, piece, sourceIndex),
  }).proxy
}

const getArrayProperty = (node: OverlayNode, property: PropertyKey): unknown => {
  if (property === 'length') return arrayLength(arrayOverlay(node))
  const mutator = arrayMutator(property)
  if (mutator !== undefined) return mutator
  const index = arrayIndex(property)
  if (index !== undefined) return getArrayIndex(node, index)
  return Reflect.get(Array.prototype, property, node.proxy)
}

export const getProperty = (node: OverlayNode, property: PropertyKey): unknown => {
  const base = nodeBase(node)
  if (isArray(base)) return getArrayProperty(node, property)
  if (typeof property === 'symbol') return Reflect.get(base, property, node.proxy)
  const key = String(property)
  if (!objectHas(node, key)) {
    if (isObjectDeleted(node, key) || Object.hasOwn(base, key)) return undefined
    return Reflect.get(base, key, node.proxy)
  }
  const value = objectValue(node, key)
  if (!isContainer(value)) return value
  return createNode(node.context, value, node, { key, placed: hasObjectWrite(node, key) }).proxy
}

const setArrayProperty = (node: OverlayNode, property: string, supplied: unknown): boolean => {
  if (property === 'length') {
    const length = toArrayLength(supplied)
    if (length === undefined) return reject(node.context, 'Invalid array length')
    setArrayLength(node, length)
    return true
  }
  const index = arrayIndex(property)
  if (index === undefined) {
    return reject(node.context, 'Only array indices and length can be written')
  }
  if (index > arrayLength(arrayOverlay(node))) {
    return reject(node.context, 'Overlay arrays cannot contain holes')
  }
  const stored = clonePlacement(supplied)
  if (stored instanceof Invalid) return reject(node.context, stored.message)
  setArrayIndex(node, index, stored)
  return true
}

const setObjectProperty = (node: OverlayNode, key: string, supplied: unknown): boolean => {
  if (supplied === undefined) return deleteObjectProperty(node, key)
  const stored = clonePlacement(supplied)
  if (stored instanceof Invalid) return reject(node.context, stored.message)
  const wasDeleted = isObjectDeleted(node, key)
  if (!wasDeleted && !isContainer(stored) && objectValue(node, key) === stored) return true
  setObjectWrite(node, key, stored)
  if (wasDeleted && Object.hasOwn(nodeBase(node), key)) {
    node.readded ??= new Set()
    node.readded.add(key)
  }
  deleteObjectDeletion(node, key)
  markDirty(node)
  return true
}

const deleteObjectProperty = (node: OverlayNode, key: string): boolean => {
  if (!objectHas(node, key)) return true
  deleteObjectWrite(node, key)
  node.readded?.delete(key)
  setObjectDeletion(node, key)
  markDirty(node)
  return true
}

const setProperty = (node: OverlayNode, property: PropertyKey, supplied: unknown): boolean => {
  if (typeof property === 'symbol') return reject(node.context, 'Symbol writes are not supported')
  return isArray(nodeBase(node))
    ? setArrayProperty(node, String(property), supplied)
    : setObjectProperty(node, String(property), supplied)
}

const deleteProperty = (node: OverlayNode, property: PropertyKey): boolean => {
  if (typeof property === 'symbol') return reject(node.context, 'Symbol writes are not supported')
  if (isArray(nodeBase(node))) return reject(node.context, 'Overlay arrays cannot contain holes')
  return deleteObjectProperty(node, String(property))
}

const hasProperty = (node: OverlayNode, property: PropertyKey): boolean => {
  const base = nodeBase(node)
  if (!isArray(base)) {
    return typeof property === 'symbol' ? property in base : objectHas(node, String(property))
  }
  if (property === 'length') return true
  const index = arrayIndex(property)
  if (index !== undefined) return index < arrayLength(arrayOverlay(node))
  return property in Array.prototype
}

const ownKeys = (node: OverlayNode): Array<string | symbol> => {
  if (!isArray(nodeBase(node))) return objectKeys(node)
  const length = arrayLength(arrayOverlay(node))
  const keys: Array<string> = []
  for (let index = 0; index < length; index++) keys.push(String(index))
  keys.push('length')
  return keys
}

const getDescriptor = (
  node: OverlayNode,
  property: PropertyKey,
): PropertyDescriptor | undefined => {
  if (isArray(nodeBase(node))) {
    const length = arrayLength(arrayOverlay(node))
    if (property === 'length') {
      // `length` is non-configurable on the target, so the reported descriptor must be its own.
      Reflect.set(node.target, 'length', length)
      return Reflect.getOwnPropertyDescriptor(node.target, 'length')
    }
    const index = arrayIndex(property)
    if (index === undefined || index >= length) return undefined
  } else if (typeof property === 'symbol' || !objectHas(node, String(property))) return undefined
  const descriptor: PropertyDescriptor = {
    configurable: true,
    enumerable: true,
    writable: node.context.status.value === 'open',
    value: getProperty(node, property),
  }
  return descriptor
}

/** Reject an operation drafts never support, reporting it as refused. */
const refuse = (target: object, problem: string): false => {
  const node = writable(target)
  if (node !== undefined) reject(node.context, problem)
  return false
}

export const handler: ProxyHandler<object> = {
  get: (target, property) => {
    const node = live(target)
    if (property === NODE) return nodeForTarget(target)
    return node === undefined ? undefined : getProperty(node, property)
  },
  set: (target, property, value) => {
    const node = writable(target)
    return node !== undefined && setProperty(node, property, value)
  },
  deleteProperty: (target, property) => {
    const node = writable(target)
    return node !== undefined && deleteProperty(node, property)
  },
  has: (target, property) => {
    const node = live(target)
    return node !== undefined && hasProperty(node, property)
  },
  ownKeys: (target) => {
    const node = live(target)
    return node === undefined ? Reflect.ownKeys(target) : ownKeys(node)
  },
  getOwnPropertyDescriptor: (target, property) => {
    const node = live(target)
    return node === undefined
      ? Reflect.getOwnPropertyDescriptor(target, property)
      : getDescriptor(node, property)
  },
  getPrototypeOf: (target) => {
    const node = live(target)
    return Reflect.getPrototypeOf(node === undefined ? target : nodeBase(node))
  },
  defineProperty: (target) => refuse(target, 'Defining overlay properties is not supported'),
  preventExtensions: (target) => refuse(target, 'Overlays cannot be made non-extensible'),
  setPrototypeOf: (target) => refuse(target, 'Changing an overlay prototype is not supported'),
}
